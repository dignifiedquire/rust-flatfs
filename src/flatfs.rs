use std::{
    fs, io,
    path::{Path, PathBuf},
    time::Duration,
};

use eyre::{eyre, Result, WrapErr};

use crate::shard::{self, Shard};

pub struct Flatfs {
    path: PathBuf,
    shard: Shard,
}

const EXTENSION: &str = "data";

/// Timeout (in ms) for a backoff on retrying operations.
const RETRY_DELAY: u64 = 200;

/// The maximum number of retries that will be attempted.
const RETRY_ATTEMPTS: usize = 6;

impl Flatfs {
    /// Creates or opens an existing store at the provided path as the root.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::with_shard(path, Shard::default())
    }

    /// Creates or opens an existing store at the provided path as the root.
    pub fn with_shard<P: AsRef<Path>>(path: P, shard: Shard) -> Result<Self> {
        if path.as_ref().exists() && path.as_ref().join(shard::FILE_NAME).exists() {
            Self::open(path, shard)
        } else {
            Self::create(path, shard)
        }
    }

    /// Stores the given value under the given key.
    pub fn put<T: AsRef<[u8]>>(&self, key: &str, value: T) -> Result<()> {
        ensure_valid_key(key)?;
        let filepath = self.as_path(key);
        let parent_dir = filepath.parent().unwrap();

        // Make sure the sharding directory exists.
        if !parent_dir.exists() {
            if let Err(err) = retry(|| fs::create_dir(&parent_dir)) {
                // Directory got already created, that's fine.
                if err.kind() != io::ErrorKind::AlreadyExists {
                    return Err(err)
                        .wrap_err_with(|| format!("Failed to create {:?}", filepath.parent()));
                }
            }
        }

        // Write to temp location
        let temp_filepath = filepath.with_extension(".temp");
        let value = value.as_ref();
        retry(|| fs::write(&temp_filepath, value))
            .wrap_err_with(|| format!("Failed to write {:?}", temp_filepath))?;

        // Rename after successfull write
        retry(|| fs::rename(&temp_filepath, &filepath)).wrap_err_with(|| {
            format!("Failed to reaname: {:?} -> {:?}", temp_filepath, filepath)
        })?;

        Ok(())
    }

    /// Retrieves the value under the given key.
    pub fn get(&self, key: &str) -> Result<Vec<u8>> {
        ensure_valid_key(key)?;
        let filepath = self.as_path(key);

        let value = retry(|| fs::read(&filepath))
            .wrap_err_with(|| format!("Failed to read {:?}", filepath))?;

        Ok(value)
    }

    /// Retrieves the size of the value under the given key.
    pub fn get_size(&self, key: &str) -> Result<u64> {
        ensure_valid_key(key)?;
        let filepath = self.as_path(key);

        let metadata = filepath
            .metadata()
            .wrap_err_with(|| format!("Failed to read metadata for {:?}", filepath))?;

        Ok(metadata.len())
    }

    /// Deletes the value under the given key, if it doesn't exists, returns an error.
    pub fn del(&self, key: &str) -> Result<()> {
        ensure_valid_key(key)?;
        let filepath = self.as_path(key);

        retry(|| fs::remove_file(&filepath))
            .wrap_err_with(|| format!("Failed to remove {:?}", filepath))?;

        Ok(())
    }

    fn create<P: AsRef<Path>>(path: P, shard: Shard) -> Result<Self> {
        fs::create_dir_all(&path)
            .wrap_err_with(|| format!("Failed to create {:?}", path.as_ref()))?;

        shard
            .write_to_file(&path)
            .wrap_err("Failed to write shard to file")?;

        Self::open(path, shard)
    }

    fn open<P: AsRef<Path>>(path: P, shard: Shard) -> Result<Self> {
        let existing_shard = Shard::from_file(&path)?;
        if shard != existing_shard {
            return Err(eyre!(
                "Tried to open store with {:?}, found {:?}",
                shard,
                existing_shard
            ));
        }

        Ok(Flatfs {
            path: path.as_ref().to_path_buf(),
            shard,
        })
    }

    fn as_path(&self, key: &str) -> PathBuf {
        let mut p = self.path.join(self.shard.dir(key)).join(key);
        p.set_extension(EXTENSION);
        p
    }
}

fn ensure_valid_key(key: &str) -> Result<()> {
    if key.len() < 2 || !key.is_ascii() || key.contains('/') {
        return Err(eyre!("Invalid key: {:?}", key));
    }

    Ok(())
}

fn retry<T, E, F: FnMut() -> std::result::Result<T, E>>(mut f: F) -> std::result::Result<T, E> {
    use backoff::{backoff::Constant, Error};

    let mut count = 0;

    let res = backoff::retry(
        Constant::new(Duration::from_millis(RETRY_DELAY)),
        || match f() {
            Ok(res) => Ok(res),
            Err(err) => {
                count += 1;
                if count < RETRY_ATTEMPTS {
                    Err(err.into())
                } else {
                    Err(Error::Permanent(err))
                }
            }
        },
    );
    match res {
        Ok(res) => Ok(res),
        Err(err) => match err {
            Error::Permanent(err) => Err(err),
            Error::Transient { err, .. } => Err(err),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_empty() {
        let dir = tempfile::tempdir().unwrap();

        let _flatfs = Flatfs::new(dir.path()).unwrap();

        let shard_file_path = dir.path().join("SHARDING");
        assert!(shard_file_path.exists());
        assert_eq!(
            fs::read_to_string(&shard_file_path).unwrap(),
            Shard::default().to_string(),
        );
    }

    #[test]
    fn test_open_empty() {
        let dir = tempfile::tempdir().unwrap();

        {
            let _flatfs = Flatfs::with_shard(dir.path(), Shard::Prefix(2)).unwrap();
            let shard_file_path = dir.path().join("SHARDING");
            assert!(shard_file_path.exists());
            assert_eq!(
                fs::read_to_string(&shard_file_path).unwrap(),
                Shard::Prefix(2).to_string(),
            );
        }

        let _flatfs = Flatfs::with_shard(dir.path(), Shard::Prefix(2)).unwrap();
        assert!(Flatfs::new(dir.path()).is_err());
    }

    #[test]
    fn test_paths() {
        let dir = tempfile::tempdir().unwrap();
        let flatfs = Flatfs::new(dir.path()).unwrap();

        assert_eq!(flatfs.as_path("foobar"), dir.path().join("ba/foobar.data"),);
    }

    #[test]
    fn test_put_get() {
        let dir = tempfile::tempdir().unwrap();
        let flatfs = Flatfs::new(dir.path()).unwrap();

        for i in 0..10 {
            flatfs.put(&format!("foo{i}"), [i; 128]).unwrap();
        }

        for i in 0..10 {
            assert_eq!(flatfs.get(&format!("foo{i}")).unwrap(), [i; 128]);
            assert_eq!(flatfs.get_size(&format!("foo{i}")).unwrap(), 128);
        }
    }

    #[test]
    fn test_put_get_del() {
        let dir = tempfile::tempdir().unwrap();
        let flatfs = Flatfs::new(dir.path()).unwrap();

        for i in 0..10 {
            flatfs.put(&format!("foo{i}"), [i; 128]).unwrap();
        }

        for i in 0..10 {
            assert_eq!(flatfs.get(&format!("foo{i}")).unwrap(), [i; 128]);
        }

        for i in 0..5 {
            flatfs.del(&format!("foo{}", i)).unwrap();
        }

        for i in 0..10 {
            if i < 5 {
                assert!(flatfs.get(&format!("foo{i}")).is_err());
                assert!(flatfs.del(&format!("foo{i}")).is_err());
            } else {
                assert_eq!(flatfs.get(&format!("foo{i}")).unwrap(), [i; 128]);
            }
        }
    }
}
