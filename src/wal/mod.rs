mod multi;
mod wal;

pub use multi::MultiWal;
use wal::Wal;

pub(crate) const DEFAULT_MAX_SEGMENT_SIZE: usize = 16777216; // 16 MiB
const MAX_SEGMENT_BUFFER_SIZE: u16 = 8192; // 8 KiB

const WAL_EXTENSION: &str = "-bp.wal";
pub(crate) const WAL_DEFAULT_ID: u64 = 0;
