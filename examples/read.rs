use std::env;

use eyre::Result;
use flatfs::Flatfs;

fn main() -> Result<()> {
    let mut args = env::args();
    let path = args.nth(1).unwrap();
    let n: usize = args.nth(2).unwrap().parse()?;

    println!("Opening {:?}", path);

    let flatfs = Flatfs::new(&path)?;
    println!("Size on disk: {} bytes", flatfs.disk_usage());

    // read n files serially
    for r in flatfs.iter().take(n) {
        let (key, value) = r?;
        println!("{}", key);
        println!("{:?}", value);
    }

    Ok(())
}
