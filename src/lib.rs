mod mem_table;
mod flush;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}


// TODO what is the type of value?
// pub fn put(key: String, value: u32) {
//     todo!()
// }
//
// pub fn get(key: String) {
//     todo!()
// }
//
// pub fn delete(key: String) {
//     todo!()
// }
//
// pub fn merge(key: String, value:u32) {
//     todo!()
// }


/*
main() {
let mem_table = mem_table();


mem_table.write()
flush(mem_table);
}
 */





#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
