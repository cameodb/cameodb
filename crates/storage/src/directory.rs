//! The Tantivy directory every on-disk index opens through: Tantivy's [`MmapDirectory`], with
//! its atomic replace made to wait out another process holding the file on Windows.
//!
//! Tantivy rewrites `.managed.json` before it creates each file of a new segment, and
//! `meta.json` on every commit, both by writing a temporary file and renaming it over the old
//! one. On Windows that rename fails with "Access is denied" (or a sharing violation) while any
//! other handle has the old file open without delete sharing, and the usual holder is not
//! CameoDB at all: it is the antivirus scanner opening the copy that was written a millisecond
//! earlier. Tantivy reports the failure under the name of the segment file it was about to
//! create, kills the indexing thread, and every write to the index fails until it is reopened.
//! On a Windows machine with Defender on, a first write to a new index hit it more often than
//! not.
//!
//! The holder lets go within milliseconds, so the replace is retried for a short while. On
//! every other platform a rename replaces an open file, and this is a plain [`MmapDirectory`].

use std::io;
use std::path::Path;
use std::sync::Arc;

use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    Directory, DirectoryLock, FileHandle, Lock, MmapDirectory, WatchCallback, WatchHandle, WritePtr,
};

/// [`MmapDirectory`], with [`Directory::atomic_write`] retried while Windows refuses it.
#[derive(Clone, Debug)]
pub(crate) struct IndexDirectory {
    inner: MmapDirectory,
}

impl IndexDirectory {
    pub(crate) fn open(path: &Path) -> Result<Self, tantivy::directory::error::OpenDirectoryError> {
        Ok(Self {
            inner: MmapDirectory::open(path)?,
        })
    }
}

/// Whether `error` is Windows refusing to replace a file another handle holds open.
#[cfg_attr(not(windows), allow(dead_code))]
fn held_by_another_handle(error: &io::Error) -> bool {
    /// `ERROR_SHARING_VIOLATION`, which std has no `ErrorKind` for.
    const SHARING_VIOLATION: i32 = 32;
    error.kind() == io::ErrorKind::PermissionDenied
        || error.raw_os_error() == Some(SHARING_VIOLATION)
}

/// Run `replace` until it succeeds, fails for another reason, or the holder has had about two
/// seconds to let go. A scan of a file this small takes milliseconds; the budget is for a
/// loaded machine, and past it the error is real and reported as it is.
#[cfg(windows)]
fn retry_while_held(mut replace: impl FnMut() -> io::Result<()>) -> io::Result<()> {
    use std::time::Duration;
    const PAUSES_MS: [u64; 12] = [1, 2, 5, 10, 20, 40, 80, 160, 250, 400, 500, 500];
    for pause in PAUSES_MS {
        match replace() {
            Err(e) if held_by_another_handle(&e) => {
                std::thread::sleep(Duration::from_millis(pause));
            }
            other => return other,
        }
    }
    replace()
}

#[cfg(not(windows))]
fn retry_while_held(mut replace: impl FnMut() -> io::Result<()>) -> io::Result<()> {
    replace()
}

impl Directory for IndexDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.inner.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.inner.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.inner.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        self.inner.open_write(path)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.inner.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        retry_while_held(|| self.inner.atomic_write(path, data))
    }

    fn sync_directory(&self) -> io::Result<()> {
        self.inner.sync_directory()
    }

    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        self.inner.acquire_lock(lock)
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.inner.watch(watch_callback)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_refusal_to_replace_is_retried() {
        assert!(held_by_another_handle(&io::Error::from(
            io::ErrorKind::PermissionDenied
        )));
        assert!(held_by_another_handle(&io::Error::from_raw_os_error(32)));
        assert!(!held_by_another_handle(&io::Error::from(
            io::ErrorKind::NotFound
        )));
    }

    #[test]
    fn an_index_round_trips_through_the_directory() {
        let dir = tempfile::tempdir().unwrap();
        let mut builder = tantivy::schema::Schema::builder();
        let title = builder.add_text_field("title", tantivy::schema::TEXT);
        let index = tantivy::Index::create(
            IndexDirectory::open(dir.path()).unwrap(),
            builder.build(),
            Default::default(),
        )
        .unwrap();
        let mut writer: tantivy::IndexWriter = index.writer(15_000_000).unwrap();
        writer
            .add_document(tantivy::doc!(title => "rust on windows"))
            .unwrap();
        writer.commit().unwrap();
        drop(writer);

        let reopened = tantivy::Index::open(IndexDirectory::open(dir.path()).unwrap()).unwrap();
        let searcher = reopened.reader().unwrap().searcher();
        assert_eq!(searcher.num_docs(), 1);
        assert!(dir.path().join(".managed.json").exists());
    }
}
