use std::fs::File;
use std::io::Write;
use crate::mem_table::MemTable;

// TODO Flush worker file, could be a new thread?
// TODO what happens if the json, file create, file write fails?
pub fn flush(mem_table: &MemTable) -> std::io::Result<MemTable> {
    let flush_data = serde_json::to_string(mem_table)?;
    let _ = std::fs::create_dir_all("/Users/stangirala/.ferrum_store/")?;
    let mut file = File::create("/Users/stangirala/.ferrum_store/sst_0")?;

    file.write_all(flush_data.as_bytes())?;

    Ok(MemTable::new())
}

fn _load_from_file() -> std::io::Result<MemTable> {
    let contents = std::fs::read_to_string("/Users/stangirala/.ferrum_store/sst_0")?;
    let mem_table = serde_json::from_str(&contents)?;
    Ok(mem_table)
}


#[cfg(test)]
mod tests {
    use std::ptr::addr_of;
    use crate::flush::{_load_from_file, flush};
    use crate::mem_table::{MemTable, MemTableEntry};

    #[test]
    fn test_flush() {
        let mut mem_table = MemTable::new();
        mem_table.data.push(MemTableEntry {
            e_type: "PUT".to_string(),
            key: "1".to_string(),
            value: 0,
        });

        let new_mem_table = match flush(&mem_table){
            Ok(m) => { m },
            Err(e) => { panic!("{:?}", e); }
        };

        assert_ne!(addr_of!(mem_table), addr_of!(new_mem_table));
        assert_ne!(addr_of!(mem_table.data), addr_of!(new_mem_table.data));
        assert_ne!(addr_of!(mem_table.wal), addr_of!(new_mem_table.wal));

        let from_file_mem_table = _load_from_file().unwrap();
        assert_eq!(from_file_mem_table, mem_table);
        assert_eq!(from_file_mem_table.data, mem_table.data);
        assert_eq!(from_file_mem_table.wal, mem_table.wal);
    }
}
