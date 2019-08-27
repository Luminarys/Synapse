use std::fmt;
use std::io::{self, Write};
use std::sync::Arc;

use byteorder::{BigEndian, WriteBytesExt};

use buffers::Buffer;
use torrent::info::Info as TorrentInfo;
use torrent::Bitfield;

pub enum Message {
    // TODO: Consider moving this to the heap,
    // reduces the enum size from 48 bytes to 24,
    // memcpy of Message's ends up taking ~4% of
    // CPU time, could be worth reducing as such.
    Handshake {
        rsv: [u8; 8],
        hash: [u8; 20],
        id: [u8; 20],
    },
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    Uninterested,
    Have(u32),
    Bitfield(Bitfield),
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        length: u32,
        data: Buffer,
    },
    SharedPiece {
        index: u32,
        begin: u32,
        length: u32,
        data: Arc<Buffer>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
    Port(u16),
    Extension {
        id: u8,
        payload: Vec<u8>,
    },
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Message::Handshake { rsv, .. } => {
                write!(f, "Message::Handshake {{ extensions: {:?} }}", &rsv[..])
            }
            Message::KeepAlive => write!(f, "Message::KeepAlive"),
            Message::Choke => write!(f, "Message::Choke"),
            Message::Unchoke => write!(f, "Message::Unchoke"),
            Message::Interested => write!(f, "Message::Interested"),
            Message::Uninterested => write!(f, "Message::Uninterested"),
            Message::Have(p) => write!(f, "Message::Have({})", p),
            Message::Bitfield(_) => write!(f, "Message::Bitfield"),
            Message::Request {
                index,
                begin,
                length,
            } => write!(
                f,
                "Message::Request {{ idx: {}, begin: {}, len: {} }}",
                index, begin, length
            ),
            Message::Piece { index, begin, .. } => {
                write!(f, "Message::Piece {{ idx: {}, begin: {} }}", index, begin)
            }
            Message::SharedPiece { index, begin, .. } => {
                write!(f, "Message::SPiece {{ idx: {}, begin: {} }}", index, begin)
            }
            Message::Cancel {
                index,
                begin,
                length,
            } => write!(
                f,
                "Message::Cancel {{ idx: {}, begin: {}, len: {} }}",
                index, begin, length
            ),
            Message::Port(port) => write!(f, "Message::Port({:?})", port),
            Message::Extension { id, .. } => write!(f, "Message::Extension {{ id: {} }}", id),
        }
    }
}

impl Clone for Message {
    fn clone(&self) -> Message {
        match *self {
            Message::Handshake { rsv, hash, id } => Message::Handshake { rsv, hash, id },
            Message::KeepAlive => Message::KeepAlive,
            Message::Choke => Message::Choke,
            Message::Unchoke => Message::Unchoke,
            Message::Interested => Message::Interested,
            Message::Uninterested => Message::Uninterested,
            Message::Have(p) => Message::Have(p),
            Message::Bitfield(ref b) => Message::Bitfield(b.clone()),
            Message::Request {
                index,
                begin,
                length,
            } => Message::Request {
                index,
                begin,
                length,
            },
            Message::Piece {
                index,
                begin,
                length,
                ref data,
            } => {
                if cfg!(test) {
                    Message::Piece {
                        index,
                        begin,
                        length,
                        data: data.clone(),
                    }
                } else {
                    unreachable!("pieces should not be cloned outside of testing");
                }
            }
            Message::SharedPiece {
                index,
                begin,
                length,
                ref data,
            } => Message::SharedPiece {
                index,
                begin,
                length,
                data: data.clone(),
            },
            Message::Cancel {
                index,
                begin,
                length,
            } => Message::Cancel {
                index,
                begin,
                length,
            },
            Message::Port(port) => Message::Port(port),
            Message::Extension { id, ref payload } => Message::Extension {
                id,
                payload: payload.clone(),
            },
        }
    }
}

impl PartialEq for Message {
    fn eq(&self, other: &Message) -> bool {
        match (self, other) {
            (
                &Message::Handshake { rsv, hash, id },
                &Message::Handshake {
                    rsv: rsv_,
                    hash: hash_,
                    id: id_,
                },
            ) => rsv == rsv_ && hash == hash_ && id == id_,
            (&Message::KeepAlive, &Message::KeepAlive)
            | (&Message::Choke, &Message::Choke)
            | (&Message::Unchoke, &Message::Unchoke)
            | (&Message::Interested, &Message::Interested)
            | (&Message::Uninterested, &Message::Uninterested) => true,
            (&Message::Have(p), &Message::Have(p_)) => p == p_,
            (&Message::Port(p), &Message::Port(p_)) => p == p_,
            (
                &Message::Request {
                    index,
                    begin,
                    length,
                },
                &Message::Request {
                    index: i,
                    begin: b,
                    length: l,
                },
            )
            | (
                &Message::Piece {
                    index,
                    begin,
                    length,
                    ..
                },
                &Message::Piece {
                    index: i,
                    begin: b,
                    length: l,
                    ..
                },
            )
            | (
                &Message::Cancel {
                    index,
                    begin,
                    length,
                },
                &Message::Cancel {
                    index: i,
                    begin: b,
                    length: l,
                },
            ) => index == i && begin == b && length == l,
            (
                &Message::Extension { id, ref payload },
                &Message::Extension {
                    id: i,
                    payload: ref p,
                },
            ) => id == i && payload == p,
            _ => false,
        }
    }
}

impl Message {
    pub fn handshake(torrent: &TorrentInfo) -> Message {
        use {DHT_EXT, EXT_PROTO, PEER_ID};
        let mut rsv = [0u8; 8];
        rsv[DHT_EXT.0] |= DHT_EXT.1;
        rsv[EXT_PROTO.0] |= EXT_PROTO.1;
        Message::Handshake {
            rsv,
            hash: torrent.hash,
            id: *PEER_ID,
        }
    }

    pub fn request(idx: u32, offset: u32, len: u32) -> Message {
        Message::Request {
            index: idx,
            begin: offset,
            length: len,
        }
    }

    pub fn s_piece(index: u32, begin: u32, length: u32, data: Arc<Buffer>) -> Message {
        Message::SharedPiece {
            index,
            begin,
            data,
            length,
        }
    }

    pub fn get_handshake_data(&self) -> ([u8; 20], [u8; 20], [u8; 8]) {
        match *self {
            Message::Handshake { hash, id, rsv } => (hash, id, rsv),
            _ => unreachable!(),
        }
    }

    pub fn is_special(&self) -> bool {
        match *self {
            Message::Handshake { .. } | Message::Bitfield(_) | Message::Extension { .. } => true,
            _ => false,
        }
    }

    pub fn len(&self) -> usize {
        match *self {
            Message::Handshake { .. } => 68,
            Message::KeepAlive => 4,
            Message::Choke | Message::Unchoke | Message::Interested | Message::Uninterested => 5,
            Message::Port(_) => 7,
            Message::Have(_) => 9,
            Message::Bitfield(ref pf) => 5 + pf.bytes(),
            Message::Request { .. } | Message::Cancel { .. } => 17,
            Message::Piece { ref data, .. } => 13 + data.len(),
            Message::SharedPiece { ref data, .. } => 13 + data.len(),
            Message::Extension { ref payload, .. } => 6 + payload.len(),
        }
    }

    pub fn encode(&self, mut buf: &mut [u8]) -> io::Result<()> {
        match *self {
            Message::Handshake { rsv, hash, id } => {
                if id.len() != 20 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid Peer ID",
                    ));
                }
                buf.write_u8(19)?;
                buf.write_all("BitTorrent protocol".as_ref())?;
                buf.write_all(&rsv)?;
                buf.write_all(&hash)?;
                buf.write_all(&id)?;
            }
            Message::KeepAlive => {
                buf.write_u32::<BigEndian>(0)?;
            }
            Message::Choke => {
                buf.write_u32::<BigEndian>(1)?;
                buf.write_u8(0)?;
            }
            Message::Unchoke => {
                buf.write_u32::<BigEndian>(1)?;
                buf.write_u8(1)?;
            }
            Message::Interested => {
                buf.write_u32::<BigEndian>(1)?;
                buf.write_u8(2)?;
            }
            Message::Uninterested => {
                buf.write_u32::<BigEndian>(1)?;
                buf.write_u8(3)?;
            }
            Message::Port(p) => {
                buf.write_u32::<BigEndian>(3)?;
                buf.write_u8(0x09)?;
                buf.write_u16::<BigEndian>(p)?;
            }
            Message::Have(piece) => {
                buf.write_u32::<BigEndian>(5)?;
                buf.write_u8(4)?;
                buf.write_u32::<BigEndian>(piece)?;
            }
            Message::Bitfield(ref pf) => {
                buf.write_u32::<BigEndian>(1 + pf.bytes() as u32)?;
                buf.write_u8(5)?;
                for i in 0..pf.bytes() {
                    buf.write_u8(pf.byte_at(i as u64))?;
                }
            }
            Message::Request {
                index,
                begin,
                length,
            } => {
                buf.write_u32::<BigEndian>(13)?;
                buf.write_u8(6)?;
                buf.write_u32::<BigEndian>(index)?;
                buf.write_u32::<BigEndian>(begin)?;
                buf.write_u32::<BigEndian>(length)?;
            }
            Message::Piece {
                index,
                begin,
                length,
                ..
            }
            | Message::SharedPiece {
                index,
                begin,
                length,
                ..
            } => {
                buf.write_u32::<BigEndian>(9 + length)?;
                buf.write_u8(7)?;
                buf.write_u32::<BigEndian>(index)?;
                buf.write_u32::<BigEndian>(begin)?;
            }
            Message::Cancel {
                index,
                begin,
                length,
            } => {
                buf.write_u32::<BigEndian>(13)?;
                buf.write_u8(8)?;
                buf.write_u32::<BigEndian>(index)?;
                buf.write_u32::<BigEndian>(begin)?;
                buf.write_u32::<BigEndian>(length)?;
            }
            Message::Extension { id, ref payload } => {
                buf.write_u32::<BigEndian>(2 + payload.len() as u32)?;
                buf.write_u8(20)?;
                buf.write_u8(id)?;
                buf.write_all(payload)?;
            }
        };
        Ok(())
    }
}
