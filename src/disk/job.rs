use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{cmp, fmt, fs, path, time};

use fs2;
use http_range::HttpRange;
use sha1::{Digest, Sha1};
use sstream::SStream;

use super::{BufCache, FileCache, JOB_TIME_SLICE};
use crate::buffers::Buffer;
use crate::torrent::{Info, LocIter};
use crate::util::{hash_to_id, io_err};
use crate::CONFIG;

static MP_BOUNDARY: &str = "qxyllcqgNchqyob";
const EXDEV: i32 = 18;

pub struct Location {
    /// Info file index
    pub file: usize,
    pub file_len: u64,
    /// Offset into file
    pub offset: u64,
    /// Start in the piece
    pub start: usize,
    /// end in the piece
    pub end: usize,
    /// This file should be fully allocated if possible
    pub allocate: bool,
    info: Arc<Info>,
}

pub enum Request {
    Write {
        tid: usize,
        data: Buffer,
        locations: LocIter,
        path: Option<String>,
    },
    Read {
        data: Buffer,
        locations: LocIter,
        context: Ctx,
        path: Option<String>,
    },
    Serialize {
        tid: usize,
        data: Vec<u8>,
        hash: [u8; 20],
    },
    Delete {
        tid: usize,
        hash: [u8; 20],
        files: Vec<PathBuf>,
        path: Option<String>,
        artifacts: bool,
    },
    Move {
        tid: usize,
        from: String,
        to: String,
        target: String,
    },
    Validate {
        tid: usize,
        info: Arc<Info>,
        path: Option<String>,
        idx: u32,
        invalid: Vec<u32>,
    },
    ValidatePiece {
        tid: usize,
        info: Arc<Info>,
        path: Option<String>,
        piece: u32,
    },
    WriteFile {
        data: Vec<u8>,
        path: PathBuf,
    },
    Download {
        client: SStream,
        ranges: Vec<HttpRange>,
        multipart: bool,
        file_len: u64,
        file_path: String,
        buf: Vec<u8>,
        buf_idx: usize,
    },
    FreeSpace,
    Ping,
    Shutdown,
}

pub enum Response {
    Read { context: Ctx, data: Buffer },
    ValidationComplete { tid: usize, invalid: Vec<u32> },
    PieceValidated { tid: usize, piece: u32, valid: bool },
    ValidationUpdate { tid: usize, percent: f32 },
    Moved { tid: usize, path: String },
    FreeSpace(u64),
    Error { tid: usize, err: io::Error },
}

pub struct Ctx {
    pub pid: usize,
    pub tid: usize,
    pub idx: u32,
    pub begin: u32,
    pub length: u32,
}

pub enum JobRes {
    Resp(Response),
    Update(Request, Response),
    Done,
    Paused(Request),
}

impl Request {
    pub fn write(tid: usize, data: Buffer, locations: LocIter, path: Option<String>) -> Request {
        Request::Write {
            tid,
            data,
            locations,
            path,
        }
    }

    pub fn read(context: Ctx, data: Buffer, locations: LocIter, path: Option<String>) -> Request {
        Request::Read {
            context,
            data,
            locations,
            path,
        }
    }

    pub fn serialize(tid: usize, data: Vec<u8>, hash: [u8; 20]) -> Request {
        Request::Serialize { tid, data, hash }
    }

    pub fn validate(tid: usize, info: Arc<Info>, path: Option<String>) -> Request {
        Request::Validate {
            tid,
            info,
            path,
            idx: 0,
            invalid: Vec::new(),
        }
    }

    pub fn validate_piece(
        tid: usize,
        info: Arc<Info>,
        path: Option<String>,
        piece: u32,
    ) -> Request {
        Request::ValidatePiece {
            tid,
            info,
            path,
            piece,
        }
    }

    pub fn delete(
        tid: usize,
        hash: [u8; 20],
        files: Vec<PathBuf>,
        path: Option<String>,
        artifacts: bool,
    ) -> Request {
        Request::Delete {
            tid,
            hash,
            files,
            path,
            artifacts,
        }
    }

    pub fn download(
        client: SStream,
        mut ranges: Vec<HttpRange>,
        file_path: String,
        file_len: u64,
    ) -> Request {
        let http_lines = match ranges.len() {
            0 => vec![
                format!("HTTP/1.1 200 OK"),
                format!("Accept-Ranges: bytes"),
                format!("Content-Length: {}", file_len),
                format!("Content-Type: application/octet-stream"),
                format!(
                    "Content-Disposition: attachment; filename=\"{}\"",
                    path::Path::new(&file_path)
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                ),
                format!("Connection: Close"),
                format!("\r\n"),
            ],
            1 => vec![
                format!("HTTP/1.1 206 Partial Content"),
                format!("Content-Length: {}", ranges[0].length),
                format!(
                    "Content-Range: bytes {}-{}/{}",
                    ranges[0].start,
                    ranges[0].start + ranges[0].length - 1,
                    file_len
                ),
                format!("Accept-Ranges: bytes"),
                format!("Content-Type: application/octet-stream"),
                format!("Connection: Close"),
                format!("\r\n"),
            ],
            _ => vec![
                format!("HTTP/1.1 206 Partial Content"),
                format!("Accept-Ranges: bytes"),
                format!(
                    "Content-Type: {}; boundary={}",
                    "multipart/byteranges", MP_BOUNDARY
                ),
                format!("Connection: Close"),
                // Add the first multipart boundary here manually.
                // Because the job processing code only writes boundaries
                // when ranges are complete we can either add a fake range
                // which immediately triggers this write or we can manully
                // add the boundary here since I find it less confusing.
                format!("\r\n--{}", MP_BOUNDARY),
                format!("Content-Type: application/octet-stream"),
                format!(
                    "Content-Range: bytes {}-{}/{}",
                    ranges[0].start,
                    ranges[0].start + ranges[0].length - 1,
                    file_len
                ),
                format!("\r\n"),
            ],
        };
        let buf = http_lines.join("\r\n").into_bytes();
        // Add a single range containing the single file if this is
        // a plain http request.
        if ranges.is_empty() {
            ranges = vec![HttpRange {
                start: 0,
                length: file_len,
            }];
        }
        // Because we process ranges by popping them once complete,
        // we reverse the ranges initially so that we can pop them
        // from the end cheaply.
        ranges.reverse();
        Request::Download {
            client,
            multipart: ranges.len() > 1,
            ranges,
            file_path,
            file_len,
            buf,
            buf_idx: 0,
        }
    }

    pub fn shutdown() -> Request {
        Request::Shutdown
    }

    pub fn concurrent(&self) -> bool {
        match self {
            Request::Validate { .. } => false,
            _ => true,
        }
    }

    pub fn execute(self, fc: &mut FileCache, bc: &mut BufCache) -> io::Result<JobRes> {
        let sd = &CONFIG.disk.session;
        let dd = &CONFIG.disk.directory;
        let (mut tb, mut tpb, mut tpb2) = bc.data();
        match self {
            Request::Ping => {}
            Request::FreeSpace => {
                let free_space = fs2::available_space(dd.as_str())?;
                return Ok(JobRes::Resp(Response::FreeSpace(free_space)));
            }
            Request::WriteFile { path, data } => {
                let p = tpb.get(path.iter());
                p.set_extension("temp");
                let res = fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(&p)
                    .map(|mut f| f.write(&data[..]));
                match res {
                    Ok(Ok(_)) => {
                        fs::rename(&p, &path).ok();
                    }
                    Ok(Err(e)) => {
                        error!("Failed to write disk job: {}", e);
                        fs::remove_file(&p).ok();
                    }
                    Err(e) => {
                        error!("Failed to write disk job: {}", e);
                    }
                }
            }
            Request::Write {
                data,
                locations,
                path,
                ..
            } => {
                for loc in locations {
                    let pb = tpb.get(path.as_ref().unwrap_or(dd));
                    pb.push(loc.path());
                    fc.write_file_range(
                        &pb,
                        if loc.allocate {
                            Ok(loc.file_len)
                        } else {
                            Err(loc.file_len)
                        },
                        loc.offset,
                        &data[loc.start..loc.end],
                    )?;
                    if loc.end - loc.start != 16_384 {
                        fc.flush_file(&pb);
                    }
                }
            }
            Request::Read {
                context,
                mut data,
                locations,
                path,
                ..
            } => {
                for loc in locations {
                    let pb = tpb.get(path.as_ref().unwrap_or(dd));
                    pb.push(loc.path());
                    fc.read_file_range(&pb, loc.offset, &mut data[loc.start..loc.end])?;
                }
                return Ok(JobRes::Resp(Response::read(context, data)));
            }
            Request::Move {
                tid,
                from,
                to,
                target,
            } => {
                let fp = tpb.get(&from);
                let tp = tpb2.get(&to);
                fp.push(target.clone());
                tp.push(target);
                match fs::rename(&fp, &tp) {
                    Ok(_) => {}
                    // Cross filesystem move, try to copy then delete
                    Err(ref e) if e.raw_os_error() == Some(EXDEV) => {
                        if fp.is_dir() {
                            // `fs_extra::dir::copy()` behaves like `cp -r`, so the destination
                            // path should not include `target`.
                            let tp = tp.parent().unwrap();
                            match fs_extra::dir::copy(&fp, &tp, &fs_extra::dir::CopyOptions::new())
                            {
                                Ok(_) => {
                                    fs::remove_dir_all(&fp)?;
                                }
                                Err(e) => {
                                    // There are a number of reasons this might fail, e.g. the
                                    // destination might already exist. Do not try to clean up
                                    // any state in `tp` and let the user sort it out.
                                    error!("Dir copy failed: {:?}", e);
                                    return io_err("Failed to copy directory across filesystems!");
                                }
                            }
                        } else {
                            match fs_extra::file::copy(
                                &fp,
                                &tp,
                                &fs_extra::file::CopyOptions::new(),
                            ) {
                                Ok(_) => {
                                    fs::remove_file(&fp)?;
                                }
                                Err(e) => {
                                    // There are a number of reasons this might fail, e.g. the
                                    // destination might already exist. Do not try to clean up
                                    // any state in `tp` and let the user sort it out.
                                    error!("File copy failed: {:?}", e);
                                    return io_err("Failed to copy file across filesystems!");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("FS rename failed: {:?}", e);
                        return Err(e);
                    }
                }
                return Ok(JobRes::Resp(Response::moved(tid, to)));
            }
            Request::Serialize { data, hash, .. } => {
                let temp = tpb.get(sd);
                temp.push(hash_to_id(&hash) + ".temp");
                let mut f = fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(&temp)?;
                f.write_all(&data)?;
                let actual = tpb2.get(sd);
                actual.push(hash_to_id(&hash));
                fs::rename(temp, actual)?;
            }
            Request::Delete {
                hash,
                files,
                path,
                artifacts,
                ..
            } => {
                {
                    let spb = tpb.get(sd);
                    spb.push(hash_to_id(&hash));
                    fs::remove_file(&spb).ok();
                    spb.set_extension("torrent");
                    fs::remove_file(&spb).ok();
                }

                for file in &files {
                    let pb = tpb2.get(path.as_ref().unwrap_or(dd));
                    pb.push(&file);
                    fc.remove_file(&pb);
                    if artifacts {
                        if let Err(e) = fs::remove_file(&pb) {
                            debug!("Failed to delete file: {:?}, {}", pb, e);
                        }
                    }
                }

                if let Some(p) = files.get(0) {
                    let comp = p.components().next().unwrap();
                    let dirp: &Path = comp.as_os_str().as_ref();
                    let pb = tpb.get(path.as_ref().unwrap_or(dd));
                    pb.push(&dirp);
                    fs::remove_dir(&pb).ok();
                }
            }
            Request::ValidatePiece {
                tid,
                info,
                path,
                piece,
            } => {
                let buf = tb.get(info.piece_len as usize);
                let mut ctx = Sha1::new();
                let locs = Info::piece_disk_locs(&info, piece);
                for loc in locs {
                    let pb = tpb.get(path.as_ref().unwrap_or(dd));
                    pb.push(loc.path());
                    fc.read_file_range(&pb, loc.offset, &mut buf[loc.start..loc.end])
                        .map(|_| ctx.update(&buf[loc.start..loc.end]))
                        .ok();
                }
                let digest = ctx.finalize();
                return Ok(JobRes::Resp(Response::PieceValidated {
                    tid,
                    piece,
                    valid: digest[..] == info.hashes[piece as usize][..],
                }));
            }
            Request::Validate {
                tid,
                info,
                path,
                mut idx,
                mut invalid,
            } => {
                let buf = tb.get(info.piece_len as usize);
                let start = time::Instant::now();

                while idx < info.pieces()
                    && start.elapsed() < time::Duration::from_millis(JOB_TIME_SLICE)
                {
                    let mut valid = true;
                    let mut ctx = Sha1::new();
                    let locs = Info::piece_disk_locs(&info, idx);
                    for loc in locs {
                        if !valid {
                            break;
                        }
                        let pb = tpb.get(path.as_ref().unwrap_or(dd));
                        pb.push(loc.path());
                        valid &= fc
                            .read_file_range(&pb, loc.offset, &mut buf[loc.start..loc.end])
                            .map(|_| ctx.update(&buf[loc.start..loc.end]))
                            .is_ok();
                    }
                    let digest = ctx.finalize();
                    if !valid || digest[..] != info.hashes[idx as usize][..] {
                        invalid.push(idx);
                    }

                    idx += 1;
                }
                if idx == info.pieces() {
                    return Ok(JobRes::Resp(Response::validation_complete(tid, invalid)));
                } else {
                    let pieces = info.pieces();
                    return Ok(JobRes::Update(
                        Request::Validate {
                            tid,
                            info,
                            path,
                            idx,
                            invalid,
                        },
                        Response::ValidationUpdate {
                            tid,
                            percent: idx as f32 / pieces as f32,
                        },
                    ));
                }
            }
            Request::Download {
                mut client,
                file_path,
                file_len,
                mut ranges,
                mut buf,
                mut buf_idx,
                multipart,
            } => {
                let start = time::Instant::now();
                'outer: while start.elapsed() < time::Duration::from_millis(JOB_TIME_SLICE) {
                    // First write out all remaining data in buf
                    while buf_idx != buf.len() {
                        match client.write(&buf[buf_idx..]) {
                            Ok(n) => buf_idx += n,
                            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                            Err(ref e)
                                if e.kind() == io::ErrorKind::WouldBlock
                                    || e.kind() == io::ErrorKind::TimedOut =>
                            {
                                break 'outer
                            }
                            Err(e) => return Err(e),
                        }
                    }

                    // If we've run out of ranges to write out, we're done
                    if ranges.is_empty() {
                        return Ok(JobRes::Done);
                    }
                    // Now try to read out the next chunk of the current range, updating
                    // buf and the current range appropriately
                    let cur_range = ranges.last_mut().unwrap();
                    // Either read 128 KiB or the rest of the range
                    let chunk_len = cmp::min(1024 * 128, cur_range.length) as usize;
                    buf.resize(chunk_len, 0);
                    buf_idx = 0;
                    fc.read_file_range(path::Path::new(&file_path), cur_range.start, &mut buf)?;
                    cur_range.length -= buf.len() as u64;
                    cur_range.start += buf.len() as u64;

                    // Process the next range if the current is complete
                    if cur_range.length == 0 {
                        ranges.pop();
                        // If it's multipart write out either the boundary header
                        // or the final boundary if we're done with all chunks
                        if multipart {
                            let http_lines = match ranges.last() {
                                Some(cur_range) => vec![
                                    format!("\r\n--{}", MP_BOUNDARY),
                                    format!("Content-Type: application/octet-stream"),
                                    format!(
                                        "Content-Range: bytes {}-{}/{}",
                                        cur_range.start,
                                        cur_range.start + cur_range.length - 1,
                                        file_len
                                    ),
                                    format!("\r\n"),
                                ]
                                .join("\r\n"),
                                None => format!("\r\n--{}--", MP_BOUNDARY),
                            };
                            buf.extend(http_lines.into_bytes());
                        }
                    }
                }
                return Ok(JobRes::Paused(Request::Download {
                    client,
                    file_path,
                    file_len,
                    ranges,
                    buf,
                    buf_idx,
                    multipart,
                }));
            }
            Request::Shutdown => unreachable!(),
        }
        Ok(JobRes::Done)
    }

    pub fn setup(&mut self) -> io::Result<()> {
        match *self {
            Request::Download { ref mut client, .. } => {
                client.get_stream().set_nonblocking(false)?;
                client
                    .get_stream()
                    .set_write_timeout(Some(time::Duration::from_millis(JOB_TIME_SLICE)))
            }
            _ => Ok(()),
        }
    }

    pub fn tid(&self) -> Option<usize> {
        match *self {
            Request::Read { ref context, .. } => Some(context.tid),
            Request::Serialize { tid, .. }
            | Request::Validate { tid, .. }
            | Request::ValidatePiece { tid, .. }
            | Request::Delete { tid, .. }
            | Request::Move { tid, .. }
            | Request::Write { tid, .. } => Some(tid),
            Request::WriteFile { .. }
            | Request::Download { .. }
            | Request::Shutdown
            | Request::Ping
            | Request::FreeSpace => None,
        }
    }
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "disk::Request")
    }
}

impl Location {
    pub fn new(
        file: usize,
        file_len: u64,
        offset: u64,
        start: u64,
        end: u64,
        info: Arc<Info>,
        allocate: bool,
    ) -> Location {
        Location {
            file,
            file_len,
            offset,
            start: start as usize,
            end: end as usize,
            info,
            allocate,
        }
    }

    pub fn path(&self) -> &Path {
        &self.info.files[self.file].path
    }
}

impl fmt::Debug for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "disk::Location {{ file: {}, off: {}, s: {}, e: {} }}",
            self.file, self.offset, self.start, self.end
        )
    }
}

impl Response {
    pub fn read(context: Ctx, data: Buffer) -> Response {
        Response::Read { context, data }
    }

    pub fn error(tid: usize, err: io::Error) -> Response {
        Response::Error { tid, err }
    }

    pub fn moved(tid: usize, path: String) -> Response {
        Response::Moved { tid, path }
    }

    pub fn validation_complete(tid: usize, invalid: Vec<u32>) -> Response {
        Response::ValidationComplete { tid, invalid }
    }

    pub fn tid(&self) -> usize {
        match *self {
            Response::Read { ref context, .. } => context.tid,
            Response::ValidationComplete { tid, .. }
            | Response::Moved { tid, .. }
            | Response::ValidationUpdate { tid, .. }
            | Response::PieceValidated { tid, .. }
            | Response::Error { tid, .. } => tid,
            Response::FreeSpace(_) => unreachable!(),
        }
    }
}

impl fmt::Debug for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "disk::Response")
    }
}

impl Ctx {
    pub fn new(pid: usize, tid: usize, idx: u32, begin: u32, length: u32) -> Ctx {
        Ctx {
            pid,
            tid,
            idx,
            begin,
            length,
        }
    }
}
