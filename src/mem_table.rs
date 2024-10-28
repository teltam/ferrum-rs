use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MemTableEntry {
    pub e_type: String,
    pub key: String,
    pub value: u32,
}

impl fmt::Display for MemTableEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            " e_typr {} key {} value {}",
            self.e_type, self.key, self.value
        )
    }
}

impl MemTableEntry {
    fn hash(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.to_string());
        let digest = format!("{:X}", hasher.finalize());
        digest
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MemTableWALEntry {
    pub mem_table_entry: MemTableEntry,

    pub checksum: String,
}

// TODO `data` should be a skip list
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MemTable {
    pub data: Vec<MemTableEntry>,

    // Could this be MemTableEntry be a ref?
    pub wal: Vec<MemTableWALEntry>, // We need a checkmark
}

// TODO what else can e_type be other than PUT
impl MemTable {
    pub fn new() -> MemTable {
        MemTable {
            data: vec![],
            wal: vec![],
        }
    }

    pub fn get(&self, key: String) -> Option<u32> {
        for mem_table_entry in &self.data {
            if mem_table_entry.key == key {
                return Some(mem_table_entry.value);
            }
        }

        None
    }

    pub fn put(&mut self, key: String, value: u32) {
        let entry = MemTableEntry {
            e_type: "PUT".to_string(),
            key: key.clone(),
            value: value.clone(),
        };

        self.wal.push(MemTableWALEntry {
            mem_table_entry: entry.clone(),
            checksum: entry.hash(),
        });

        for (i, mem_table_entry) in self.data.iter().enumerate() {
            if mem_table_entry.key > entry.key {
                self.data.insert(i, entry);
                return;
            }
        }

        self.data.push(entry);
    }
}

#[cfg(test)]
mod tests {
    use crate::mem_table::{MemTable, MemTableEntry, MemTableWALEntry};

    #[test]
    fn test_mem_table_read_and_write() {
        let mut data: Vec<MemTableEntry> = Vec::new();
        let mut wal: Vec<MemTableWALEntry> = Vec::new();

        for i in 0..10 {
            let mem_table_entry = MemTableEntry {
                e_type: "PUT".to_string(),
                key: i.to_string(),
                value: i + 5,
            };

            data.push(mem_table_entry.clone());

            wal.push(MemTableWALEntry {
                mem_table_entry: mem_table_entry.clone(),
                checksum: mem_table_entry.hash(),
            });
        }

        let mut mem_table = MemTable { data, wal };

        assert_eq!(mem_table.get("3".to_string()).unwrap(), 8);

        assert_eq!(mem_table.get("11".to_string()), None);

        mem_table.put("21".to_string(), 26);
        mem_table.put("24".to_string(), 29);
        assert_eq!(mem_table.get("21".to_string()).unwrap(), 26);
        assert_eq!(mem_table.get("24".to_string()).unwrap(), 29);

        let keys: Vec<&String> = mem_table.data.iter().map(|i| &i.key).collect();

        assert_eq!(
            keys,
            ["0", "1", "2", "21", "24", "3", "4", "5", "6", "7", "8", "9"]
        );

        // WAL
        assert_eq!(
            mem_table.wal.get(10).unwrap().checksum,
            mem_table.data.get(3).unwrap().hash()
        );

        assert_eq!(mem_table.wal.len(), mem_table.data.len());
    }
}
