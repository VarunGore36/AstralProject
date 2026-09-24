use std::path::{Path, PathBuf};

use astra_types::CaptureRecord;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const INDEX_FILE: &str = "index.json";
const CHUNK_PREFIX: &str = "chunk-";
const CHUNK_SUFFIX: &str = ".zst";
const LENGTH_PREFIX_BYTES: usize = 4;
const ZSTD_LEVEL: i32 = 3;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("record encoding error: {0}")]
    RecordEncoding(#[from] postcard::Error),
    #[error("index encoding error: {0}")]
    IndexEncoding(#[from] serde_json::Error),
    #[error("malformed chunk: {0}")]
    Malformed(String),
    #[error("integrity check failed: {0}")]
    Integrity(String),
    #[error("records per chunk must be greater than zero")]
    InvalidChunkSize,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ChunkIndexEntry {
    pub name: String,
    pub first_seq: u64,
    pub last_seq: u64,
    pub records: u64,
    pub bytes: u64,
    pub sha256: String,
}

pub struct ChunkWriter {
    directory: PathBuf,
    records_per_chunk: usize,
    chunk_seq: u64,
    body: Vec<u8>,
    records: usize,
    first_seq: Option<u64>,
    last_seq: u64,
    index: Vec<ChunkIndexEntry>,
}

impl ChunkWriter {
    pub fn open(
        directory: impl Into<PathBuf>,
        records_per_chunk: usize,
    ) -> Result<Self, StoreError> {
        if records_per_chunk == 0 {
            return Err(StoreError::InvalidChunkSize);
        }

        let directory = directory.into();
        std::fs::create_dir_all(&directory)?;
        let index = read_index(&directory)?;
        let chunk_seq = index.len() as u64;

        Ok(ChunkWriter {
            directory,
            records_per_chunk,
            chunk_seq,
            body: Vec::new(),
            records: 0,
            first_seq: None,
            last_seq: 0,
            index,
        })
    }

    pub fn append(
        &mut self,
        record: &CaptureRecord,
    ) -> Result<Option<ChunkIndexEntry>, StoreError> {
        encode_record(record, &mut self.body)?;
        self.records += 1;
        self.first_seq.get_or_insert(record.seq);
        self.last_seq = record.seq;

        if self.records >= self.records_per_chunk {
            return self.close_chunk();
        }

        Ok(None)
    }

    pub fn finish(mut self) -> Result<Vec<ChunkIndexEntry>, StoreError> {
        self.close_chunk()?;
        Ok(self.index.clone())
    }

    pub fn index(&self) -> &[ChunkIndexEntry] {
        &self.index
    }

    fn close_chunk(&mut self) -> Result<Option<ChunkIndexEntry>, StoreError> {
        if self.records == 0 {
            return Ok(None);
        }

        let compressed = zstd::stream::encode_all(&self.body[..], ZSTD_LEVEL)?;
        let chunk_seq = self.chunk_seq;
        let name = format!("{CHUNK_PREFIX}{chunk_seq:06}{CHUNK_SUFFIX}");
        let path = self.directory.join(&name);
        std::fs::write(&path, &compressed)?;

        let entry = ChunkIndexEntry {
            name,
            first_seq: self.first_seq.unwrap_or_default(),
            last_seq: self.last_seq,
            records: self.records as u64,
            bytes: compressed.len() as u64,
            sha256: sha256_hex(&compressed),
        };

        self.index.push(entry.clone());
        write_index(&self.directory, &self.index)?;

        self.chunk_seq += 1;
        self.body.clear();
        self.records = 0;
        self.first_seq = None;

        Ok(Some(entry))
    }
}

pub fn read_index(directory: &Path) -> Result<Vec<ChunkIndexEntry>, StoreError> {
    let path = directory.join(INDEX_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let body = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str(&body)?)
}

pub fn write_index(directory: &Path, index: &[ChunkIndexEntry]) -> Result<(), StoreError> {
    let body = serde_json::to_string_pretty(index)?;
    std::fs::write(directory.join(INDEX_FILE), body)?;
    Ok(())
}

pub fn read_chunk(path: &Path) -> Result<Vec<CaptureRecord>, StoreError> {
    let compressed = std::fs::read(path)?;
    let body = zstd::stream::decode_all(&compressed[..])
        .map_err(|error| StoreError::Malformed(format!("{}: {error}", path.display())))?;
    decode_records(&body, path)
}

pub fn verify_chunk(path: &Path, expected_sha256: &str) -> Result<(), StoreError> {
    let actual = sha256_hex(&std::fs::read(path)?);
    if actual != expected_sha256 {
        return Err(StoreError::Integrity(format!(
            "{}: expected {expected_sha256}, found {actual}",
            path.display()
        )));
    }
    Ok(())
}

pub fn read_all(directory: &Path) -> Result<Vec<CaptureRecord>, StoreError> {
    let index = read_index(directory)?;
    let mut records = Vec::new();
    for entry in &index {
        let path = directory.join(&entry.name);
        verify_chunk(&path, &entry.sha256)?;
        records.extend(read_chunk(&path)?);
    }
    Ok(records)
}

fn encode_record(record: &CaptureRecord, out: &mut Vec<u8>) -> Result<(), StoreError> {
    let encoded = postcard::to_stdvec(record)?;
    let length = u32::try_from(encoded.len())
        .map_err(|_| StoreError::Malformed("record exceeds the 4 byte length prefix".to_owned()))?;
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(&encoded);
    Ok(())
}

fn decode_records(body: &[u8], path: &Path) -> Result<Vec<CaptureRecord>, StoreError> {
    let mut records = Vec::new();
    let mut rest = body;

    while !rest.is_empty() {
        if rest.len() < LENGTH_PREFIX_BYTES {
            return Err(truncated(path));
        }
        let length = u32::from_le_bytes(rest[..LENGTH_PREFIX_BYTES].try_into().unwrap()) as usize;
        rest = &rest[LENGTH_PREFIX_BYTES..];
        if rest.len() < length {
            return Err(truncated(path));
        }
        let (head, tail) = rest.split_at(length);
        records.push(postcard::from_bytes(head)?);
        rest = tail;
    }

    Ok(records)
}

fn truncated(path: &Path) -> StoreError {
    StoreError::Malformed(format!("{}: truncated record stream", path.display()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use astra_types::{CaptureFlags, Channel, Instrument, MarketType, Symbol, Timestamp, Venue};

    fn temp_directory(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("astra-store-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        path
    }

    fn record(seq: u64, payload: Vec<u8>) -> CaptureRecord {
        CaptureRecord {
            seq,
            instrument: Instrument::new(
                Venue::Binance,
                MarketType::Spot,
                Symbol::new("BTC/USDT").unwrap(),
            ),
            channel: Channel::Trade,
            ts_socket: Timestamp::from_unix_nanos(1_700_000_000_000_000_000 + seq as i64),
            ts_exchange: None,
            payload,
            flags: CaptureFlags::NONE,
        }
    }

    fn sample(count: u64) -> Vec<CaptureRecord> {
        (0..count)
            .map(|seq| record(seq, vec![0x00, 0xFF, 0x80, seq as u8]))
            .collect()
    }

    fn write_all(
        directory: &Path,
        records_per_chunk: usize,
        records: &[CaptureRecord],
    ) -> Vec<ChunkIndexEntry> {
        let mut writer = ChunkWriter::open(directory, records_per_chunk).unwrap();
        for record in records {
            writer.append(record).unwrap();
        }
        writer.finish().unwrap()
    }

    #[test]
    fn records_round_trip_across_chunks() {
        let directory = temp_directory("round-trip");
        let records = sample(7);
        let index = write_all(&directory, 3, &records);

        assert_eq!(index.len(), 3);
        assert_eq!(read_all(&directory).unwrap(), records);

        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn payloads_are_preserved_byte_for_byte() {
        let directory = temp_directory("payloads");
        let records = vec![
            record(1, Vec::new()),
            record(2, vec![0xFF, 0xFE, 0x00, 0x80]),
        ];
        write_all(&directory, 8, &records);

        let read = read_all(&directory).unwrap();
        assert_eq!(read[0].payload, Vec::<u8>::new());
        assert_eq!(read[1].payload, vec![0xFF, 0xFE, 0x00, 0x80]);

        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn index_records_seq_ranges_and_hashes() {
        let directory = temp_directory("index");
        let index = write_all(&directory, 4, &sample(6));

        assert_eq!(index.len(), 2);
        assert_eq!(index[0].first_seq, 0);
        assert_eq!(index[0].last_seq, 3);
        assert_eq!(index[0].records, 4);
        assert_eq!(index[1].first_seq, 4);
        assert_eq!(index[1].last_seq, 5);
        assert_eq!(index[1].records, 2);

        for entry in &index {
            let path = directory.join(&entry.name);
            assert_eq!(path.metadata().unwrap().len(), entry.bytes);
            verify_chunk(&path, &entry.sha256).unwrap();
        }

        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn tampered_chunks_fail_verification() {
        let directory = temp_directory("tamper");
        let index = write_all(&directory, 2, &sample(2));

        let path = directory.join(&index[0].name);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        assert!(matches!(
            verify_chunk(&path, &index[0].sha256),
            Err(StoreError::Integrity(_))
        ));
        assert!(matches!(
            read_all(&directory),
            Err(StoreError::Integrity(_))
        ));

        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn writer_resumes_without_clobbering() {
        let directory = temp_directory("resume");
        write_all(&directory, 2, &sample(2));
        let index = write_all(
            &directory,
            2,
            &sample(4).into_iter().skip(2).collect::<Vec<_>>(),
        );

        assert_eq!(index.len(), 2);
        assert_eq!(index[1].name, "chunk-000001.zst");
        assert_eq!(read_all(&directory).unwrap().len(), 4);

        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn zero_sized_chunks_are_rejected() {
        let directory = temp_directory("zero");
        assert!(matches!(
            ChunkWriter::open(&directory, 0),
            Err(StoreError::InvalidChunkSize)
        ));
    }

    #[test]
    fn truncated_streams_are_rejected() {
        let path = temp_directory("truncated").join("chunk-000000.zst");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut body = Vec::new();
        encode_record(&record(1, vec![1, 2, 3]), &mut body).unwrap();
        let compressed = zstd::stream::encode_all(&body[..3], ZSTD_LEVEL).unwrap();
        std::fs::write(&path, &compressed).unwrap();

        assert!(matches!(read_chunk(&path), Err(StoreError::Malformed(_))));

        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
}
