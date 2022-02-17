use std::env;

use eyre::{eyre, Result};
use flatfs::Flatfs;

fn main() -> Result<()> {
    let mut args = env::args();
    let iter = args.nth(1).unwrap().trim().to_lowercase();
    let path = args.next().unwrap().trim().to_string();
    let n: usize = args.next().unwrap().parse()?;

    println!("Opening {:?}", path);

    let flatfs = Flatfs::new(&path)?;
    println!("Size on disk: {} bytes", flatfs.disk_usage());

    match iter.as_str() {
        "all" => {
            for r in flatfs.iter().take(n) {
                let (key, value) = r?;
                println!("{}", key);
                println!("{:?}", value);
            }
        }
        "keys" => {
            for r in flatfs.keys().take(n) {
                let key = r?;
                println!("{}", key);
            }
        }
        "values" => {
            for r in flatfs.values().take(n) {
                let value = r?;
                println!("{:?}", value);
            }
        }
        _ => return Err(eyre!("Unsupported action: {}", iter)),
    }

    Ok(())
}
