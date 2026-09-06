#[cfg(target_os = "macos")]
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Read;
#[cfg(target_os = "macos")]
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[cfg(not(target_os = "macos"))]
use filetime::{FileTime, set_file_times, set_symlink_file_times};
use rusqlite::{Connection, ErrorCode, OpenFlags};
#[cfg(not(target_os = "macos"))]
use std::io::Write;

use super::common::{ensure_dir, path_exists};
use super::layout::display_path;
use crate::infra::tree_digest::inventory_tree;

pub(crate) const STORAGE_APFS_CLONE: &str = "apfs-clone-v1";
pub(crate) const STORAGE_FULL_COPY: &str = "full-copy-v1";
pub(crate) const STORAGE_TAR_ARCHIVE: &str = "tar-archive-v1";

#[derive(Debug)]
pub(crate) struct PreparedTreeCheckpoint {
    source: PathBuf,
    cleanup: CheckpointCleanup,
    #[cfg(target_os = "macos")]
    cloned: bool,
    #[cfg(target_os = "macos")]
    entries: HashMap<Box<[u8]>, PreparedEntry>,
}

/// Service owners retain this guard until restart/rollback has finished. Neither a
/// failed capture nor replacing a large staged directory may delete it while offline.
#[derive(Clone, Debug)]
pub(crate) struct CheckpointCleanup(Arc<CheckpointWorkspace>);

#[derive(Debug)]
struct CheckpointWorkspace {
    root: PathBuf,
    next_retired: AtomicU64,
}

impl CheckpointCleanup {
    fn new(parent: &Path) -> Result<Self, String> {
        ensure_dir(parent)?;
        let root = tempfile::Builder::new()
            .prefix(".checkpoint-preparation-")
            .tempdir_in(parent)
            .map_err(|error| error.to_string())?
            .keep();
        Ok(Self(Arc::new(CheckpointWorkspace {
            root,
            next_retired: AtomicU64::new(0),
        })))
    }

    fn candidate(&self) -> PathBuf {
        self.0.root.join("tree")
    }

    pub(crate) fn retire(&self, path: &Path) -> Result<(), String> {
        if !path_exists(path) {
            return Ok(());
        }
        let id = self.0.next_retired.fetch_add(1, Ordering::Relaxed);
        fs::rename(path, self.0.root.join(format!("retired-{id}"))).map_err(|error| {
            format!(
                "failed to defer checkpoint cleanup; retained {}: {error}",
                display_path(path)
            )
        })
    }
}

impl Drop for CheckpointWorkspace {
    fn drop(&mut self) {
        if let Err(error) = remove_tree_if_present(&self.root) {
            eprintln!(
                "ocm: retained checkpoint preparation {}: {error}",
                display_path(&self.root)
            );
        } else if let Some(parent) = self.root.parent() {
            // Remove only an empty parent; never recursively clean a shared snapshot directory.
            let _ = fs::remove_dir(parent);
        }
    }
}

impl PreparedTreeCheckpoint {
    pub(crate) fn cleanup_guard(&self) -> CheckpointCleanup {
        self.cleanup.clone()
    }
}

#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileFingerprint {
    device: u64,
    inode: u64,
    len: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
    mode: u32,
}

#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug)]
struct PreparedEntry {
    fingerprint: Option<FileFingerprint>,
    sqlite: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqliteCheck {
    NotDatabase,
    Verified,
    Deferred,
}

pub(crate) fn default_snapshot_storage_kind() -> String {
    STORAGE_TAR_ARCHIVE.to_string()
}

pub(crate) fn create_tree_checkpoint(source: &Path, destination: &Path) -> Result<String, String> {
    let parent = destination
        .parent()
        .ok_or("checkpoint destination has no parent")?;
    let prepared = prepare_tree_checkpoint_in(source, parent)?;
    create_tree_checkpoint_from_preparation(prepared, destination)
}

#[cfg(test)]
fn prepare_tree_checkpoint(source: &Path) -> Result<PreparedTreeCheckpoint, String> {
    let parent = source.parent().ok_or("checkpoint source has no parent")?;
    prepare_tree_checkpoint_in(source, parent)
}

pub(crate) fn prepare_tree_checkpoint_in(
    source: &Path,
    parent: &Path,
) -> Result<PreparedTreeCheckpoint, String> {
    if !path_exists(source) {
        return Err(format!(
            "checkpoint source does not exist: {}",
            display_path(source)
        ));
    }

    #[cfg(target_os = "macos")]
    let entries = prepare_entries(source)?;
    #[cfg(not(target_os = "macos"))]
    preflight_sqlite_databases(source)?;

    let cleanup = CheckpointCleanup::new(parent)?;
    #[cfg(target_os = "macos")]
    let cloned = {
        // This live copy is only a seed. Fingerprints were recorded BEFORE cloning;
        // capture must reconcile everything that changed before publishing it.
        let cloned = clone_tree_checkpoint(source, &cleanup.candidate()).is_ok();
        if !cloned {
            cleanup.retire(&cleanup.candidate())?;
        }
        cloned
    };

    Ok(PreparedTreeCheckpoint {
        source: source.to_path_buf(),
        cleanup,
        #[cfg(target_os = "macos")]
        cloned,
        #[cfg(target_os = "macos")]
        entries,
    })
}

pub(crate) fn create_tree_checkpoint_from_preparation(
    prepared: PreparedTreeCheckpoint,
    destination: &Path,
) -> Result<String, String> {
    if path_exists(destination) {
        return Err(format!(
            "checkpoint destination already exists: {}",
            display_path(destination)
        ));
    }
    if let Some(parent) = destination.parent() {
        ensure_dir(parent)?;
    }

    let candidate = prepared.cleanup.candidate();
    #[cfg(target_os = "macos")]
    if prepared.cloned {
        capture_prepared_clone(
            &prepared.source,
            &candidate,
            prepared.entries,
            &prepared.cleanup,
        )?;
        sync_tree_root(&candidate)?;
        fs::rename(&candidate, destination).map_err(|error| error.to_string())?;
        return Ok(STORAGE_APFS_CLONE.to_string());
    } else {
        copyfile_tree(&prepared.source, &candidate)?;
    }

    #[cfg(not(target_os = "macos"))]
    copy_tree_preserving_metadata(&prepared.source, &candidate)?;

    verify_tree_checkpoint(&prepared.source, &candidate)?;
    verify_sqlite_databases(&candidate)?;
    sync_tree_root(&candidate)?;
    fs::rename(&candidate, destination).map_err(|error| error.to_string())?;
    Ok(STORAGE_FULL_COPY.to_string())
}

pub(crate) fn copy_tree_checkpoint(source: &Path, destination: &Path) -> Result<(), String> {
    let _ = create_tree_checkpoint(source, destination)?;
    Ok(())
}

pub(crate) fn verify_tree_checkpoint(source: &Path, destination: &Path) -> Result<(), String> {
    let source_entries = inventory_tree(source)?;
    let destination_entries = inventory_tree(destination)?;
    if source_entries != destination_entries {
        return Err(format!(
            "checkpoint verification failed: {} does not exactly match {}",
            display_path(destination),
            display_path(source)
        ));
    }
    Ok(())
}

fn verify_sqlite_databases(root: &Path) -> Result<(), String> {
    for path in regular_files(root)? {
        let _ = verify_sqlite_database(&path)?;
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn preflight_sqlite_databases(root: &Path) -> Result<(), String> {
    for path in regular_files(root)? {
        let _ = check_sqlite_database(&path)?;
    }
    Ok(())
}

fn verify_sqlite_database(path: &Path) -> Result<bool, String> {
    match check_sqlite_database(path)? {
        SqliteCheck::NotDatabase => Ok(false),
        SqliteCheck::Verified => Ok(true),
        SqliteCheck::Deferred => Err(format!(
            "checkpoint SQLite database remained busy after quiescence: {}",
            display_path(path)
        )),
    }
}

fn check_sqlite_database(path: &Path) -> Result<SqliteCheck, String> {
    let mut file = fs::File::open(path).map_err(|error| error.to_string())?;
    let mut magic = [0_u8; 16];
    if file.read_exact(&mut magic).is_err() || &magic != b"SQLite format 3\0" {
        return Ok(SqliteCheck::NotDatabase);
    }
    drop(file);
    let connection = match Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Ok(connection) => connection,
        Err(error) if sqlite_is_busy(&error) => return Ok(SqliteCheck::Deferred),
        Err(error) => {
            return Err(format!(
                "failed to open checkpoint SQLite database {}: {error}",
                display_path(path)
            ));
        }
    };
    let result: String = match connection.query_row("PRAGMA quick_check", [], |row| row.get(0)) {
        Ok(result) => result,
        Err(error) if sqlite_is_busy(&error) => return Ok(SqliteCheck::Deferred),
        Err(error) => {
            return Err(format!(
                "failed to verify checkpoint SQLite database {}: {error}",
                display_path(path)
            ));
        }
    };
    if result != "ok" {
        return Err(format!(
            "checkpoint SQLite integrity check failed for {}: {result}",
            display_path(path)
        ));
    }
    Ok(SqliteCheck::Verified)
}

fn sqlite_is_busy(error: &rusqlite::Error) -> bool {
    matches!(
        error.sqlite_error_code(),
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    )
}

#[cfg(target_os = "macos")]
fn prepare_entries(root: &Path) -> Result<HashMap<Box<[u8]>, PreparedEntry>, String> {
    let mut out = HashMap::new();
    prepare_entry_path(root, root, &mut out)?;
    Ok(out)
}

#[cfg(target_os = "macos")]
fn prepare_entry_path(
    root: &Path,
    path: &Path,
    out: &mut HashMap<Box<[u8]>, PreparedEntry>,
) -> Result<(), String> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(format!(
                "failed to inspect checkpoint source {}: {error}",
                display_path(path)
            ));
        }
    };
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        let relative = path.strip_prefix(root).map_err(|error| error.to_string())?;
        out.insert(
            relative.as_os_str().as_bytes().to_vec().into_boxed_slice(),
            PreparedEntry {
                fingerprint: Some(file_fingerprint(&metadata)),
                sqlite: false,
            },
        );
        return Ok(());
    }
    if file_type.is_file() {
        let relative = path
            .strip_prefix(root)
            .map_err(|error| error.to_string())?
            .as_os_str()
            .as_bytes()
            .to_vec()
            .into_boxed_slice();
        let before = file_fingerprint(&metadata);
        let sqlite_check = match check_sqlite_database(path) {
            Ok(check) => check,
            Err(_) if !path_exists(path) => {
                out.insert(
                    relative,
                    PreparedEntry {
                        fingerprint: None,
                        sqlite: false,
                    },
                );
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let after = match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.is_file() => Some(file_fingerprint(&metadata)),
            Ok(_) => None,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.to_string()),
        };
        out.insert(
            relative,
            PreparedEntry {
                fingerprint: after.filter(|fingerprint| {
                    *fingerprint == before && sqlite_check != SqliteCheck::Deferred
                }),
                sqlite: sqlite_check != SqliteCheck::NotDatabase,
            },
        );
        return Ok(());
    }
    if file_type.is_dir() {
        let relative = path.strip_prefix(root).map_err(|error| error.to_string())?;
        out.insert(
            relative.as_os_str().as_bytes().to_vec().into_boxed_slice(),
            PreparedEntry {
                fingerprint: Some(file_fingerprint(&metadata)),
                sqlite: false,
            },
        );
        let entries = match fs::read_dir(path) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.to_string()),
        };
        for entry in entries {
            match entry {
                Ok(entry) => prepare_entry_path(root, &entry.path(), out)?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.to_string()),
            }
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn file_fingerprint(metadata: &fs::Metadata) -> FileFingerprint {
    use std::os::unix::fs::MetadataExt;

    FileFingerprint {
        device: metadata.dev(),
        inode: metadata.ino(),
        len: metadata.len(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
        mode: metadata.mode(),
    }
}

#[cfg(target_os = "macos")]
fn capture_prepared_clone(
    source: &Path,
    destination: &Path,
    mut prepared: HashMap<Box<[u8]>, PreparedEntry>,
    cleanup: &CheckpointCleanup,
) -> Result<(), String> {
    let mut candidates = HashSet::new();
    capture_prepared_path(
        source,
        source,
        destination,
        &mut prepared,
        &mut candidates,
        cleanup,
    )?;
    for (relative, prior) in prepared {
        let relative = PathBuf::from(std::ffi::OsStr::from_bytes(&relative).to_os_string());
        // Remaining entries were deleted or changed type. Reconciliation already
        // removed them; do not traverse an old path through a replacement symlink.
        if prior.sqlite && checkpoint_regular_file(destination, &relative)? {
            candidates.insert(destination.join(&relative));
        }
        if let Some(primary) = sqlite_primary_for_sidecar(&relative) {
            candidates.insert(destination.join(primary));
        }
    }

    let mut candidates = candidates.into_iter().collect::<Vec<_>>();
    candidates.sort();
    for path in candidates {
        if checkpoint_regular_file(
            destination,
            path.strip_prefix(destination)
                .map_err(|error| error.to_string())?,
        )? {
            let _ = verify_sqlite_database(&path)?;
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn checkpoint_regular_file(root: &Path, relative: &Path) -> Result<bool, String> {
    if relative.as_os_str().is_empty() {
        return fs::symlink_metadata(root)
            .map(|metadata| metadata.is_file())
            .map_err(|error| error.to_string());
    }
    let mut path = root.to_path_buf();
    for component in relative.components() {
        path.push(component);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => return Ok(false),
            Ok(metadata) if path == root.join(relative) => return Ok(metadata.is_file()),
            Ok(metadata) if !metadata.is_dir() => return Ok(false),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(false)
}

#[cfg(target_os = "macos")]
fn capture_prepared_path(
    root: &Path,
    path: &Path,
    destination: &Path,
    prepared: &mut HashMap<Box<[u8]>, PreparedEntry>,
    sqlite_candidates: &mut HashSet<PathBuf>,
    cleanup: &CheckpointCleanup,
) -> Result<bool, String> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    let relative = path.strip_prefix(root).map_err(|error| error.to_string())?;
    let destination_path = destination.join(relative);
    let prior = prepared.remove(relative.as_os_str().as_bytes());
    let unchanged = prior
        .and_then(|entry| entry.fingerprint)
        .is_some_and(|fingerprint| fingerprint == file_fingerprint(&metadata));
    if metadata.file_type().is_symlink() {
        if unchanged {
            return Ok(false);
        }
        cleanup.retire(&destination_path)?;
        copyfile_tree(path, &destination_path)?;
        verify_changed_checkpoint_path(path, &destination_path)?;
        return Ok(true);
    }
    if metadata.is_file() {
        if !unchanged {
            // An unchanged source fingerprint spans the live clone. All other
            // files need a fresh capture; never overwrite a staged link or tree.
            cleanup.retire(&destination_path)?;
            if clone_tree_checkpoint(path, &destination_path).is_err() {
                cleanup.retire(&destination_path)?;
                copyfile_tree(path, &destination_path)?;
            }
            preserve_special_mode(&destination_path, &metadata)?;
            verify_changed_checkpoint_path(path, &destination_path)?;
            if prior.is_some_and(|entry| entry.sqlite) || has_sqlite_magic(path)? {
                sqlite_candidates.insert(destination_path.clone());
            }
            if let Some(primary) = sqlite_primary_for_sidecar(relative) {
                sqlite_candidates.insert(destination.join(primary));
            }
        }
        if unchanged {
            preserve_special_mode(&destination_path, &metadata)?;
        }
        return Ok(!unchanged);
    }
    if metadata.is_dir() {
        let destination_metadata = match fs::symlink_metadata(&destination_path) {
            Ok(metadata) => Some(metadata),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.to_string()),
        };
        let created = !destination_metadata.is_some_and(|metadata| metadata.is_dir());
        if created {
            cleanup.retire(&destination_path)?;
            fs::create_dir(&destination_path).map_err(|error| error.to_string())?;
        }
        // Cloned directories can be read-only. Restore full metadata after children.
        let made_writable = metadata.permissions().mode() & 0o700 != 0o700;
        if made_writable || !unchanged {
            fs::set_permissions(&destination_path, fs::Permissions::from_mode(0o700))
                .map_err(|error| error.to_string())?;
        }
        let mut names = HashSet::new();
        let mut children_changed = false;
        for entry in fs::read_dir(path).map_err(|error| error.to_string())? {
            let entry = entry.map_err(|error| error.to_string())?;
            names.insert(entry.file_name());
            children_changed |= capture_prepared_path(
                root,
                &entry.path(),
                destination,
                prepared,
                sqlite_candidates,
                cleanup,
            )?;
        }
        for entry in fs::read_dir(&destination_path).map_err(|error| error.to_string())? {
            let entry = entry.map_err(|error| error.to_string())?;
            if !names.contains(&entry.file_name()) {
                // Bounded rename, even for a removed dependency tree.
                cleanup.retire(&entry.path())?;
                children_changed = true;
            }
        }
        if !unchanged || made_writable || children_changed {
            copyfile_metadata(path, &destination_path)?;
        }
        return Ok(created);
    }
    Err(format!(
        "unsupported special file in checkpoint: {}",
        display_path(path)
    ))
}

#[cfg(target_os = "macos")]
fn preserve_special_mode(destination: &Path, source_metadata: &fs::Metadata) -> Result<(), String> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let mode = source_metadata.mode();
    if mode & 0o6000 == 0 {
        return Ok(());
    }
    let mut permissions = fs::symlink_metadata(destination)
        .map_err(|error| error.to_string())?
        .permissions();
    permissions.set_mode(mode);
    fs::set_permissions(destination, permissions)
        .and_then(|()| fs::File::open(destination)?.sync_all())
        .map_err(|error| {
            format!(
                "failed to preserve checkpoint mode for {}: {error}",
                display_path(destination)
            )
        })
}

#[cfg(target_os = "macos")]
fn preserve_special_modes(source: &Path, destination: &Path) -> Result<(), String> {
    let metadata = fs::symlink_metadata(source).map_err(|error| error.to_string())?;
    if metadata.file_type().is_symlink() {
        return Ok(());
    }
    if metadata.is_file() {
        return preserve_special_mode(destination, &metadata);
    }
    if metadata.is_dir() {
        for entry in fs::read_dir(source).map_err(|error| error.to_string())? {
            let entry = entry.map_err(|error| error.to_string())?;
            preserve_special_modes(&entry.path(), &destination.join(entry.file_name()))?;
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn verify_changed_checkpoint_path(source: &Path, destination: &Path) -> Result<(), String> {
    let source_exists = path_exists(source);
    let destination_exists = path_exists(destination);
    if !source_exists && !destination_exists {
        return Ok(());
    }
    if source_exists
        && destination_exists
        && inventory_tree(source)? == inventory_tree(destination)?
    {
        return Ok(());
    }
    Err(format!(
        "checkpoint verification failed: {} does not exactly match changed source {}",
        display_path(destination),
        display_path(source)
    ))
}

#[cfg(target_os = "macos")]
fn has_sqlite_magic(path: &Path) -> Result<bool, String> {
    let mut file = fs::File::open(path).map_err(|error| error.to_string())?;
    let mut magic = [0_u8; 16];
    Ok(file.read_exact(&mut magic).is_ok() && &magic == b"SQLite format 3\0")
}

#[cfg(target_os = "macos")]
fn sqlite_primary_for_sidecar(path: &Path) -> Option<PathBuf> {
    let file_name = path.file_name()?.to_str()?;
    for suffix in ["-wal", "-shm", "-journal"] {
        if let Some(primary_name) = file_name.strip_suffix(suffix)
            && !primary_name.is_empty()
        {
            return Some(path.with_file_name(primary_name));
        }
    }
    None
}

fn regular_files(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut out = Vec::new();
    collect_regular_files(root, &mut out)?;
    Ok(out)
}

fn collect_regular_files(path: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if metadata.file_type().is_symlink() {
        return Ok(());
    }
    if metadata.is_file() {
        out.push(path.to_path_buf());
        return Ok(());
    }
    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|error| error.to_string())? {
            collect_regular_files(&entry.map_err(|error| error.to_string())?.path(), out)?;
        }
    }
    Ok(())
}

fn sync_tree_root(root: &Path) -> Result<(), String> {
    #[cfg(windows)]
    {
        let _ = root;
        return Ok(());
    }
    #[cfg(not(windows))]
    fs::File::open(root)
        .and_then(|file| file.sync_all())
        .map_err(|error| format!("failed to sync checkpoint {}: {error}", display_path(root)))
}

pub(crate) fn remove_tree_if_present(path: &Path) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            make_tree_removable(path)?;
            fs::remove_dir_all(path).map_err(|error| error.to_string())
        }
        Ok(_) => fs::remove_file(path).map_err(|error| error.to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

fn make_tree_removable(path: &Path) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if metadata.file_type().is_symlink() {
        return Ok(());
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if metadata.is_dir() {
            let mut permissions = metadata.permissions();
            permissions.set_mode(permissions.mode() | 0o700);
            fs::set_permissions(path, permissions).map_err(|error| error.to_string())?;
        }
    }
    #[cfg(windows)]
    {
        let mut permissions = metadata.permissions();
        if permissions.readonly() {
            permissions.set_readonly(false);
            fs::set_permissions(path, permissions).map_err(|error| error.to_string())?;
        }
    }

    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|error| error.to_string())? {
            make_tree_removable(&entry.map_err(|error| error.to_string())?.path())?;
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
type CopyfileState = *mut libc::c_void;

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn copyfile(
        from: *const libc::c_char,
        to: *const libc::c_char,
        state: CopyfileState,
        flags: u32,
    ) -> libc::c_int;
    fn copyfile_state_alloc() -> CopyfileState;
    fn copyfile_state_free(state: CopyfileState) -> libc::c_int;
    fn copyfile_state_set(
        state: CopyfileState,
        flag: u32,
        value: *const libc::c_void,
    ) -> libc::c_int;
    fn clonefile(
        source: *const libc::c_char,
        destination: *const libc::c_char,
        flags: u32,
    ) -> libc::c_int;
}

#[cfg(target_os = "macos")]
fn clone_tree_checkpoint(source: &Path, destination: &Path) -> Result<(), String> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;

    const CLONE_NOFOLLOW: u32 = 1;
    const CLONE_ACL: u32 = 1 << 2;
    let destination_parent = destination
        .parent()
        .ok_or_else(|| "checkpoint destination has no parent".to_string())?;
    if fs::metadata(source)
        .map_err(|error| error.to_string())?
        .dev()
        != fs::metadata(destination_parent)
            .map_err(|error| error.to_string())?
            .dev()
    {
        return Err("source and checkpoint destination are on different filesystems".to_string());
    }
    let source_c =
        CString::new(source.as_os_str().as_bytes()).map_err(|error| error.to_string())?;
    let destination_c =
        CString::new(destination.as_os_str().as_bytes()).map_err(|error| error.to_string())?;
    // Directory clonefile is all-or-nothing and never falls back to byte copying.
    let result = unsafe {
        clonefile(
            source_c.as_ptr(),
            destination_c.as_ptr(),
            CLONE_ACL | CLONE_NOFOLLOW,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn copyfile_tree(source: &Path, destination: &Path) -> Result<(), String> {
    const COPYFILE_ALL: u32 = (1 << 0) | (1 << 1) | (1 << 2) | (1 << 3);
    const COPYFILE_RECURSIVE: u32 = 1 << 15;
    copyfile_with_flags(source, destination, COPYFILE_ALL | COPYFILE_RECURSIVE)?;
    preserve_special_modes(source, destination)
}

#[cfg(target_os = "macos")]
fn copyfile_metadata(source: &Path, destination: &Path) -> Result<(), String> {
    const COPYFILE_METADATA: u32 = (1 << 0) | (1 << 1) | (1 << 2);
    copyfile_with_flags(source, destination, COPYFILE_METADATA)
}

#[cfg(target_os = "macos")]
fn copyfile_with_flags(source: &Path, destination: &Path, flags: u32) -> Result<(), String> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    const COPYFILE_NOFOLLOW_SRC: u32 = 1 << 18;
    const COPYFILE_NOFOLLOW_DST: u32 = 1 << 19;
    const COPYFILE_STATE_PRESERVE_SUID: u32 = 16;

    let source_c =
        CString::new(source.as_os_str().as_bytes()).map_err(|error| error.to_string())?;
    let destination_c =
        CString::new(destination.as_os_str().as_bytes()).map_err(|error| error.to_string())?;
    let state = unsafe { copyfile_state_alloc() };
    if state.is_null() {
        return Err("failed to allocate copyfile state".to_string());
    }
    let preserve_suid = 1_u32;
    let state_result = unsafe {
        copyfile_state_set(
            state,
            COPYFILE_STATE_PRESERVE_SUID,
            std::ptr::from_ref(&preserve_suid).cast(),
        )
    };
    if state_result != 0 {
        unsafe {
            copyfile_state_free(state);
        }
        return Err("failed to configure copyfile mode preservation".to_string());
    }
    let result = unsafe {
        copyfile(
            source_c.as_ptr(),
            destination_c.as_ptr(),
            state,
            flags | COPYFILE_NOFOLLOW_SRC | COPYFILE_NOFOLLOW_DST,
        )
    };
    unsafe {
        copyfile_state_free(state);
    }
    if result != 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn copy_tree_preserving_metadata(source: &Path, destination: &Path) -> Result<(), String> {
    copy_path_preserving_metadata(source, destination)
}

#[cfg(not(target_os = "macos"))]
fn copy_path_preserving_metadata(source: &Path, destination: &Path) -> Result<(), String> {
    let metadata = fs::symlink_metadata(source).map_err(|error| error.to_string())?;
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        let target = fs::read_link(source).map_err(|error| error.to_string())?;
        if let Some(parent) = destination.parent() {
            ensure_dir(parent)?;
        }
        #[cfg(unix)]
        std::os::unix::fs::symlink(target, destination).map_err(|error| error.to_string())?;
        #[cfg(windows)]
        std::os::windows::fs::symlink_file(&target, destination)
            .or_else(|_| std::os::windows::fs::symlink_dir(target, destination))
            .map_err(|error| error.to_string())?;
        preserve_metadata(source, destination, &metadata, true)?;
        return Ok(());
    }
    if file_type.is_dir() {
        ensure_dir(destination)?;
        let entries = fs::read_dir(source)
            .map_err(|error| error.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        for entry in entries {
            copy_path_preserving_metadata(&entry.path(), &destination.join(entry.file_name()))?;
        }
        preserve_metadata(source, destination, &metadata, false)?;
        return Ok(());
    }
    if file_type.is_file() {
        if let Some(parent) = destination.parent() {
            ensure_dir(parent)?;
        }
        let mut input = fs::File::open(source).map_err(|error| error.to_string())?;
        let mut output = fs::File::create(destination).map_err(|error| error.to_string())?;
        std::io::copy(&mut input, &mut output).map_err(|error| error.to_string())?;
        output.flush().map_err(|error| error.to_string())?;
        output.sync_all().map_err(|error| error.to_string())?;
        preserve_metadata(source, destination, &metadata, false)?;
        return Ok(());
    }
    Err(format!(
        "unsupported special file in checkpoint: {}",
        display_path(source)
    ))
}

#[cfg(not(target_os = "macos"))]
fn preserve_metadata(
    source: &Path,
    destination: &Path,
    metadata: &fs::Metadata,
    symlink: bool,
) -> Result<(), String> {
    if !symlink {
        fs::set_permissions(destination, metadata.permissions())
            .map_err(|error| error.to_string())?;
    }
    let accessed = FileTime::from_last_access_time(metadata);
    let modified = FileTime::from_last_modification_time(metadata);
    if symlink {
        set_symlink_file_times(destination, accessed, modified)
            .map_err(|error| error.to_string())?;
    } else {
        set_file_times(destination, accessed, modified).map_err(|error| error.to_string())?;
    }
    #[cfg(unix)]
    for name in xattr::list(source).map_err(|error| error.to_string())? {
        if let Some(value) = xattr::get(source, &name).map_err(|error| error.to_string())? {
            xattr::set(destination, &name, &value).map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    #[cfg(target_os = "macos")]
    use super::{
        STORAGE_APFS_CLONE, copyfile_tree, create_tree_checkpoint_from_preparation,
        prepare_tree_checkpoint, sqlite_primary_for_sidecar, verify_tree_checkpoint,
    };
    use crate::infra::tree_digest::inventory_tree;

    #[test]
    fn checkpoint_inventory_ignores_directory_allocation_lengths() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir_all(root.path().join("nested")).unwrap();
        fs::write(root.path().join("nested/value.txt"), "value\n").unwrap();

        let inventory = inventory_tree(root.path()).unwrap();
        assert_eq!(inventory[Path::new("")].len, None);
        assert_eq!(inventory[Path::new("nested")].len, None);
        assert_eq!(inventory[Path::new("nested/value.txt")].len, Some(6));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn apfs_checkpoint_atomically_clones_the_complete_hierarchy() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let source = tempfile::tempdir().unwrap();
        fs::create_dir_all(source.path().join("generated/node_modules/pkg")).unwrap();
        fs::write(source.path().join("empty"), []).unwrap();
        fs::write(
            source.path().join("generated/node_modules/pkg/index.js"),
            "module.exports = 1;\n",
        )
        .unwrap();
        fs::write(source.path().join("privileged-tool"), "#!/bin/sh\n").unwrap();
        fs::set_permissions(
            source.path().join("privileged-tool"),
            fs::Permissions::from_mode(0o4755),
        )
        .unwrap();
        let destination_parent = tempfile::tempdir().unwrap();
        let destination = destination_parent.path().join("checkpoint");

        let prepared = prepare_tree_checkpoint(source.path()).unwrap();
        let storage = create_tree_checkpoint_from_preparation(prepared, &destination).unwrap();

        assert_eq!(storage, STORAGE_APFS_CLONE);
        verify_tree_checkpoint(source.path(), &destination).unwrap();
        assert_eq!(
            fs::metadata(destination.join("privileged-tool"))
                .unwrap()
                .mode()
                & 0o7777,
            0o4755
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn prepared_checkpoint_rechecks_same_size_sqlite_mutation() {
        let source = tempfile::tempdir().unwrap();
        let database_path = source.path().join("state.sqlite");
        let database = rusqlite::Connection::open(&database_path).unwrap();
        database
            .execute_batch(
                "CREATE TABLE state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO state VALUES ('sentinel', 'before');",
            )
            .unwrap();
        drop(database);
        let prepared = prepare_tree_checkpoint(source.path()).unwrap();
        let metadata = fs::metadata(&database_path).unwrap();
        let original_len = metadata.len() as usize;
        let modified = filetime::FileTime::from_last_modification_time(&metadata);
        let mut corrupt = vec![0_u8; original_len];
        corrupt[..16].copy_from_slice(b"SQLite format 3\0");
        fs::write(&database_path, corrupt).unwrap();
        filetime::set_file_mtime(&database_path, modified).unwrap();

        let destination_parent = tempfile::tempdir().unwrap();
        let destination = destination_parent.path().join("checkpoint");
        let error = create_tree_checkpoint_from_preparation(prepared, &destination).unwrap_err();

        assert!(error.contains("SQLite"), "{error}");
        assert!(!destination.exists());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn prepared_checkpoint_defers_busy_sqlite_until_capture() {
        let source = tempfile::tempdir().unwrap();
        let database_path = source.path().join("state.sqlite");
        let database = rusqlite::Connection::open(&database_path).unwrap();
        database
            .execute_batch(
                "PRAGMA journal_mode=DELETE;
                 CREATE TABLE state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO state VALUES ('sentinel', 'before');
                 BEGIN EXCLUSIVE;",
            )
            .unwrap();

        let prepared = prepare_tree_checkpoint(source.path()).unwrap();
        database.execute_batch("ROLLBACK").unwrap();
        drop(database);
        let destination_parent = tempfile::tempdir().unwrap();
        let destination = destination_parent.path().join("checkpoint");

        let storage = create_tree_checkpoint_from_preparation(prepared, &destination).unwrap();

        assert_eq!(storage, STORAGE_APFS_CLONE);
        verify_tree_checkpoint(source.path(), &destination).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn prepared_checkpoint_reconciles_live_changes_without_recopying_unchanged_files() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};

        let source = tempfile::tempdir().unwrap();
        let destination_parent = tempfile::tempdir().unwrap();
        let destination = destination_parent.path().join("checkpoint");
        fs::write(source.path().join("unchanged"), "bulk data").unwrap();
        fs::write(source.path().join("changed"), "before").unwrap();
        fs::write(source.path().join("file-to-dir"), "old").unwrap();
        fs::create_dir_all(source.path().join("removed/node_modules/pkg")).unwrap();
        fs::write(
            source.path().join("removed/node_modules/pkg/data"),
            "dependency",
        )
        .unwrap();
        fs::create_dir(source.path().join("dir-to-link")).unwrap();
        fs::write(source.path().join("dir-to-link/state.sqlite-wal"), "old").unwrap();
        symlink("unchanged", source.path().join("link")).unwrap();
        symlink("unchanged", source.path().join("unchanged-link")).unwrap();
        xattr::set(source.path(), "user.ocm-removed", b"old").unwrap();

        let prepared = prepare_tree_checkpoint(source.path()).unwrap();
        assert!(prepared.cloned);
        let cleanup = prepared.cleanup_guard();
        let unchanged_inode = fs::metadata(cleanup.candidate().join("unchanged"))
            .unwrap()
            .ino();
        let unchanged_link_inode = fs::symlink_metadata(cleanup.candidate().join("unchanged-link"))
            .unwrap()
            .ino();
        let modified = filetime::FileTime::from_last_modification_time(
            &fs::metadata(source.path().join("changed")).unwrap(),
        );
        fs::write(source.path().join("changed"), "after!").unwrap();
        filetime::set_file_mtime(source.path().join("changed"), modified).unwrap();
        fs::remove_dir_all(source.path().join("removed")).unwrap();
        fs::remove_file(source.path().join("file-to-dir")).unwrap();
        fs::create_dir(source.path().join("file-to-dir")).unwrap();
        fs::write(source.path().join("file-to-dir/new"), "new").unwrap();
        fs::remove_dir_all(source.path().join("dir-to-link")).unwrap();
        symlink(destination_parent.path(), source.path().join("dir-to-link")).unwrap();
        fs::remove_file(source.path().join("link")).unwrap();
        symlink("missing", source.path().join("link")).unwrap();
        xattr::remove(source.path(), "user.ocm-removed").unwrap();
        xattr::set(source.path(), "user.ocm-added", b"new").unwrap();
        fs::set_permissions(source.path(), fs::Permissions::from_mode(0o750)).unwrap();

        create_tree_checkpoint_from_preparation(prepared, &destination).unwrap();

        verify_tree_checkpoint(source.path(), &destination).unwrap();
        assert_eq!(
            fs::metadata(destination.join("unchanged")).unwrap().ino(),
            unchanged_inode
        );
        assert_eq!(
            fs::symlink_metadata(destination.join("unchanged-link"))
                .unwrap()
                .ino(),
            unchanged_link_inode
        );
        assert_eq!(xattr::get(&destination, "user.ocm-removed").unwrap(), None);
        assert_eq!(
            xattr::get(&destination, "user.ocm-added").unwrap(),
            Some(b"new".to_vec())
        );
        let retired = fs::read_dir(&cleanup.0.root).unwrap().count();
        assert!(
            retired >= 4,
            "displaced trees must survive until the service owner releases cleanup"
        );
        let cleanup_root = cleanup.0.root.clone();
        drop(cleanup);
        assert!(!cleanup_root.exists());
        assert!(destination.exists());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn failed_capture_defers_tree_cleanup_until_service_owner_releases_guard() {
        for force_full_copy in [false, true] {
            let source = tempfile::tempdir().unwrap();
            fs::write(source.path().join("bulk"), "unchanged").unwrap();
            let mut prepared = prepare_tree_checkpoint(source.path()).unwrap();
            let cleanup = prepared.cleanup_guard();
            if force_full_copy {
                cleanup.retire(&cleanup.candidate()).unwrap();
                prepared.cloned = false;
            }
            fs::write(
                source.path().join("late.sqlite"),
                b"SQLite format 3\0not a database",
            )
            .unwrap();
            let destination_parent = tempfile::tempdir().unwrap();
            let destination = destination_parent.path().join("checkpoint");

            let error =
                create_tree_checkpoint_from_preparation(prepared, &destination).unwrap_err();

            assert!(error.contains("SQLite"), "{error}");
            assert!(!destination.exists());
            assert!(cleanup.candidate().join("bulk").exists());
            let cleanup_root = cleanup.0.root.clone();
            drop(cleanup);
            assert!(!cleanup_root.exists());
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn prepared_checkpoint_reconciles_wal_commits_and_sidecar_removal() {
        let source = tempfile::tempdir().unwrap();
        let database_path = source.path().join("state.sqlite");
        let database = rusqlite::Connection::open(&database_path).unwrap();
        database.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE state(value TEXT); INSERT INTO state VALUES ('before');").unwrap();
        let prepared = prepare_tree_checkpoint(source.path()).unwrap();
        database
            .execute_batch("INSERT INTO state VALUES ('after');")
            .unwrap();
        let destination_parent = tempfile::tempdir().unwrap();
        let destination = destination_parent.path().join("checkpoint");
        create_tree_checkpoint_from_preparation(prepared, &destination).unwrap();
        let captured = rusqlite::Connection::open(destination.join("state.sqlite")).unwrap();
        let count: i64 = captured
            .query_row("SELECT count(*) FROM state", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
        drop(captured);

        let prepared = prepare_tree_checkpoint(source.path()).unwrap();
        drop(database);
        assert!(!source.path().join("state.sqlite-wal").exists());
        let destination = destination_parent
            .path()
            .join("checkpoint-without-sidecars");
        create_tree_checkpoint_from_preparation(prepared, &destination).unwrap();
        // SQLite's read-only verification may create fresh empty WAL/SHM files;
        // it must not replay the stale sidecars from the live preparation.
        assert_eq!(
            fs::read(&database_path).unwrap(),
            fs::read(destination.join("state.sqlite")).unwrap()
        );
        let captured = rusqlite::Connection::open(destination.join("state.sqlite")).unwrap();
        let count: i64 = captured
            .query_row("SELECT count(*) FROM state", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn sqlite_sidecars_revalidate_the_primary_database() {
        assert_eq!(
            sqlite_primary_for_sidecar(Path::new("state/openclaw.sqlite-wal")),
            Some(Path::new("state/openclaw.sqlite").to_path_buf())
        );
        assert_eq!(
            sqlite_primary_for_sidecar(Path::new("state/openclaw.sqlite-shm")),
            Some(Path::new("state/openclaw.sqlite").to_path_buf())
        );
        assert_eq!(
            sqlite_primary_for_sidecar(Path::new("state/openclaw.sqlite-journal")),
            Some(Path::new("state/openclaw.sqlite").to_path_buf())
        );
        assert_eq!(
            sqlite_primary_for_sidecar(Path::new("state/openclaw.sqlite")),
            None
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_full_copy_preserves_symlinks() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};

        let source = tempfile::tempdir().unwrap();
        fs::write(source.path().join("state.txt"), "before\n").unwrap();
        fs::write(source.path().join("privileged-tool"), "#!/bin/sh\n").unwrap();
        fs::set_permissions(
            source.path().join("privileged-tool"),
            fs::Permissions::from_mode(0o4755),
        )
        .unwrap();
        symlink("state.txt", source.path().join("current")).unwrap();
        let destination_parent = tempfile::tempdir().unwrap();
        let destination = destination_parent.path().join("checkpoint");

        copyfile_tree(source.path(), &destination).unwrap();

        assert!(
            fs::symlink_metadata(destination.join("current"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            fs::read_link(destination.join("current")).unwrap(),
            Path::new("state.txt")
        );
        assert_eq!(
            fs::metadata(destination.join("privileged-tool"))
                .unwrap()
                .mode()
                & 0o7777,
            0o4755
        );
        verify_tree_checkpoint(source.path(), &destination).unwrap();
    }
}
