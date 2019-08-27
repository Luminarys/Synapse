use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{cmp, fmt, fs, path, time};

use amy;
use fs_extra;
use http_range::HttpRange;
use nix::libc;
use nix::sys::statvfs;
use openssl::sha;

use super::{BufCache, FileCache, JOB_TIME_SLICE};
use buffers::Buffer;
use socket::TSocket;
use torrent::{Info, LocIter};
use util::{awrite, hash_to_id, io_err, IOR};
use CONFIG;

static MP_BOUNDARY: &str = "qxyllcqgNchqyob";

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
        client: TSocket,
        path: String,
        range_idx: usize,
        id: usize,
        ranges: Vec<HttpRange>,
        ranged: bool,
        writing: bool,
        buf_idx: usize,
        buf_max: usize,
        buf: Box<[u8; 16_384]>,
        file_len: u64,
    },
    FreeSpace,
    Ping,
    Shutdown,
}

pub enum Response {
    Read { context: Ctx, data: Arc<Buffer> },
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
    Blocked((usize, Request)),
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
        client: TSocket,
        path: String,
        mut ranges: Vec<HttpRange>,
        mut ranged: bool,
        len: u64,
    ) -> Request {
        let lines = if ranged {
            if ranges.len() == 1 {
                ranged = false;
                vec![
                    format!("HTTP/1.1 206 Partial Content"),
                    format!("Content-Length: {}", ranges[0].length),
                    format!(
                        "Content-Range: bytes {}-{}/{}",
                        ranges[0].start,
                        ranges[0].start + ranges[0].length - 1,
                        len
                    ),
                    format!("Accept-Ranges: {}", "bytes"),
                    format!("Content-Type: {};", "application/octet-stream"),
                    format!("Connection: {}", "Close"),
                    "\r\n".to_string(),
                ]
            } else {
                vec![
                    format!("HTTP/1.1 206 Partial Content"),
                    format!("Accept-Ranges: {}", "bytes"),
                    format!(
                        "Content-Type: {}; boundary={}",
                        "multipart/byteranges", MP_BOUNDARY
                    ),
                    format!("Connection: {}", "Close"),
                    "\r\n".to_string(),
                ]
            }
        } else {
            vec![
                format!("HTTP/1.1 200 OK"),
                format!("Accept-Ranges: {}", "bytes"),
                format!("Content-Length: {}", len),
                format!("Content-Type: {}", "application/octet-stream"),
                format!(
                    "Content-Disposition: attachment; filename=\"{}\"",
                    path::Path::new(&path)
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                ),
                format!("Connection: {}", "Close"),
                "\r\n".to_string(),
            ]
        };
        let data = lines.join("\r\n");
        let mut buf = Box::new([0u8; 16_384]);
        (&mut buf[..data.len()]).copy_from_slice(data.as_bytes());
        // Hack to make sure first mutlipart range gets written
        if ranged {
            ranges.insert(
                0,
                HttpRange {
                    start: 0,
                    length: 0,
                },
            );
        }
        Request::Download {
            client,
            path,
            ranges,
            ranged,
            range_idx: 0,
            id: 0,
            writing: true,
            buf,
            buf_idx: 0,
            buf_max: data.len(),
            file_len: len,
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
                if let Ok(stat) = statvfs::statvfs(dd.as_str()) {
                    let space = stat.fragment_size() as u64 * stat.blocks_available() as u64;
                    return Ok(JobRes::Resp(Response::FreeSpace(space)));
                } else {
                    return io_err("couldn't stat fs");
                }
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
                let data = Arc::new(data);
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
                    Err(ref e) if e.raw_os_error() == Some(libc::EXDEV) => {
                        match fs_extra::dir::copy(&fp, &tp, &fs_extra::dir::CopyOptions::new()) {
                            Ok(_) => {
                                fs::remove_dir_all(&fp)?;
                            }
                            Err(e) => {
                                fs::remove_dir_all(&tp)?;
                                error!("FS copy failed: {:?}", e);
                                return io_err("Failed to copy directory across filesystems!");
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
                let mut ctx = sha::Sha1::new();
                let locs = Info::piece_disk_locs(&info, piece);
                for loc in locs {
                    let pb = tpb.get(path.as_ref().unwrap_or(dd));
                    pb.push(loc.path());
                    fc.read_file_range(&pb, loc.offset, &mut buf[loc.start..loc.end])
                        .map(|_| ctx.update(&buf[loc.start..loc.end]))
                        .ok();
                }
                let digest = ctx.finish();
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
                    let mut ctx = sha::Sha1::new();
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
                    let digest = ctx.finish();
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
                path,
                id,
                ranged,
                file_len,
                mut ranges,
                mut range_idx,
                mut writing,
                mut buf_idx,
                mut buf_max,
                mut buf,
            } => {
                let start = time::Instant::now();
                while start.elapsed() < time::Duration::from_millis(JOB_TIME_SLICE) {
                    if writing {
                        loop {
                            // Need the mod here because after the first 16 KiBs complete
                            // no will be too big
                            match awrite(&buf[buf_idx..buf_max], &mut client) {
                                IOR::Complete => {
                                    writing = false;
                                    break;
                                }
                                IOR::Incomplete(w) => buf_idx += w,
                                IOR::Blocked => {
                                    return Ok(JobRes::Blocked((
                                        id,
                                        Request::Download {
                                            client,
                                            path,
                                            range_idx,
                                            id,
                                            ranges,
                                            ranged,
                                            writing,
                                            buf_idx,
                                            buf_max,
                                            buf,
                                            file_len,
                                        },
                                    )))
                                }
                                IOR::EOF => return io_err("EOF"),
                                IOR::Err(e) => return Err(e),
                            }
                        }
                    } else if range_idx == ranges.len() {
                        // Done writing the final bit
                        return Ok(JobRes::Done);
                    } else if ranges[range_idx].length == 0 {
                        range_idx += 1;
                        // Write the closer if needed
                        if range_idx == ranges.len() {
                            if ranged {
                                let closer = format!("\r\n--{}--", MP_BOUNDARY);
                                (&mut buf[..closer.len()]).copy_from_slice(closer.as_bytes());
                                buf_idx = 0;
                                buf_max = closer.len();
                                writing = true;
                            }
                        } else {
                            let lines = vec![
                                format!("\r\n--{}", MP_BOUNDARY),
                                format!("Content-Type: {}", "application/octet-stream"),
                                // Subtract because it's inclusive
                                format!(
                                    "Content-Range: bytes {}-{}/{}",
                                    ranges[range_idx].start,
                                    ranges[range_idx].start + ranges[range_idx].length - 1,
                                    file_len
                                ),
                                "\r\n".to_string(),
                            ];
                            let data = lines.join("\r\n");
                            (&mut buf[..data.len()]).copy_from_slice(data.as_bytes());
                            buf_idx = 0;
                            buf_max = data.len();
                            writing = true;
                        }
                    } else {
                        let offset = ranges[range_idx].start;
                        let len = ranges[range_idx].length;
                        let amnt = cmp::min(len, 16_384);

                        fc.read_file_range(
                            path::Path::new(&path),
                            offset,
                            &mut buf[0..amnt as usize],
                        )?;
                        ranges[range_idx].length -= amnt;
                        ranges[range_idx].start += amnt;
                        buf_max = amnt as usize;
                        buf_idx = 0;
                        writing = true;
                    }
                }
                return Ok(JobRes::Paused(Request::Download {
                    client,
                    path,
                    range_idx,
                    id,
                    file_len,
                    ranges,
                    ranged,
                    writing,
                    buf_idx,
                    buf_max,
                    buf,
                }));
            }
            Request::Shutdown => unreachable!(),
        }
        Ok(JobRes::Done)
    }

    pub fn register(&mut self, reg: &amy::Registrar) -> io::Result<()> {
        match *self {
            Request::Download {
                ref client,
                ref mut id,
                ..
            } => {
                *id = reg.register(client, amy::Event::Write)?;
                Ok(())
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "disk::Location {{ file: {}, off: {}, s: {}, e: {} }}",
            self.file, self.offset, self.start, self.end
        )
    }
}

impl Response {
    pub fn read(context: Ctx, data: Arc<Buffer>) -> Response {
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
