use crate::mem_table::{MemTable, MemTableEntry};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use std::fs::File;
use std::io::Write;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Index {
    keys: Vec<String>,
    // TODO offset
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct DataBlock {
    // TODO Mimics MemTableEntry because we aren't doing compression or delta-decoding just yet.
    block: Vec<MemTableEntry>,
    hash: String,
}

impl fmt::Display for DataBlock {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, " block {:?} hash {}", self.block, self.hash)
    }
}

impl DataBlock {
    pub fn new(block_size: usize) -> DataBlock {
        DataBlock {
            block: Vec::with_capacity(block_size),
            hash: "None".to_string(),
        }
    }

    pub fn hash(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.to_string());
        let digest = format!("{:X}", hasher.finalize());
        digest
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct StaticSortedTable {
    // aka Sorted String Table
    index: Index,

    data_blocks: Vec<DataBlock>,
    block_size: usize,
}

// TODO Flush worker file, could be a new thread?
// TODO what happens if the json, file create, file write fails?
pub fn flush(mem_table: &MemTable) -> std::io::Result<MemTable> {
    let block_size = 4;

    let mut data_blocks: Vec<DataBlock> = vec![];
    for (i, entry) in mem_table.data.iter().enumerate() {
        if i % block_size == 0 {
            data_blocks.push(DataBlock::new(block_size))
        }

        let db_len = data_blocks.len();

        data_blocks[db_len - 1].block.push(entry.clone())
    }

    let mut index_keys: Vec<String> = vec![];

    for data_block in &mut data_blocks {
        data_block.hash = data_block.hash();

        let _block_size = data_block.block.len();

        index_keys.push(data_block.block[_block_size - 1].key.clone())
    }

    let sst = StaticSortedTable {
        index: Index { keys: index_keys },
        data_blocks,
        block_size,
    };

    let flush_data = serde_json::to_string(&sst)?;
    let _ = std::fs::create_dir_all("/Users/stangirala/.ferrum_store/")?;
    let mut file = File::create("/Users/stangirala/.ferrum_store/sst_0")?;

    file.write_all(flush_data.as_bytes())?;

    Ok(MemTable::new())
}

fn _load_from_file() -> std::io::Result<StaticSortedTable> {
    let contents = std::fs::read_to_string("/Users/stangirala/.ferrum_store/sst_0")?;
    let sst: StaticSortedTable = serde_json::from_str(&contents)?;
    Ok(sst)
}

#[cfg(test)]
mod tests {
    use crate::flush::{_load_from_file, flush};
    use crate::mem_table::{MemTable, MemTableEntry};
    use std::ptr::addr_of;

    #[test]
    fn test_flush() {
        let mut mem_table = MemTable::new();
        mem_table.data.push(MemTableEntry {
            e_type: "PUT".to_string(),
            key: "1".to_string(),
            value: 0,
        });

        let new_mem_table = match flush(&mem_table) {
            Ok(m) => m,
            Err(e) => {
                panic!("{:?}", e);
            }
        };

        assert_ne!(addr_of!(mem_table), addr_of!(new_mem_table));
        assert_ne!(addr_of!(mem_table.data), addr_of!(new_mem_table.data));
        assert_ne!(addr_of!(mem_table.wal), addr_of!(new_mem_table.wal));
    }

    #[test]
    fn test_sst_write() {
        let mut mem_table = MemTable::new();
        for i in 0..100 {
            mem_table.data.push(MemTableEntry {
                e_type: "PUT".to_string(),
                key: i.to_string(),
                value: i + 10,
            });
        }

        match flush(&mem_table) {
            Ok(m) => m,
            Err(e) => {
                panic!("{:?}", e);
            }
        };

        let f_sst = _load_from_file().unwrap();

        assert_eq!(f_sst.data_blocks.len(), 100 / 4);
        assert_eq!(f_sst.block_size, 4);

        let mut f_sst_data: Vec<&MemTableEntry> = vec![];
        for data_block in &f_sst.data_blocks {
            for entry in &data_block.block {
                f_sst_data.push(&entry)
            }
        }

        for (i, j) in mem_table.data.iter().zip(f_sst_data.iter()) {
            assert_eq!(*i, **j);
        }

        for (i, data_block) in f_sst.data_blocks.iter().enumerate() {
            assert_eq!(
                data_block.block[f_sst.block_size - 1].key,
                f_sst.index.keys[i]
            )
        }
    }
}
