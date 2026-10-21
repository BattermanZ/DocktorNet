use std::path::{Component, Path, PathBuf};

/// Where the database directory lives, read from `/proc/self/mountinfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placement {
    /// On a mount other than `/`; carries the mount point.
    Mounted(PathBuf),
    /// On the container's own root filesystem, lost when it is recreated.
    RootFilesystem,
}

/// What the startup check reads from the process's surroundings.
#[derive(Debug, Clone, Default)]
pub struct Environment {
    pub in_container: bool,
    /// `/proc/self/mountinfo`, or `None` when it could not be read.
    pub mountinfo: Option<String>,
    pub uid: Option<u32>,
}

impl Environment {
    /// Read the running process's surroundings.  Docker writes `/.dockerenv`
    /// and Podman `/run/.containerenv`.
    pub fn detect() -> Self {
        let uid = std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| {
                status
                    .lines()
                    .find_map(|line| line.strip_prefix("Uid:"))
                    .and_then(|ids| ids.split_whitespace().nth(1))
                    .and_then(|effective| effective.parse().ok())
            });
        Self {
            in_container: Path::new("/.dockerenv").exists()
                || Path::new("/run/.containerenv").exists(),
            mountinfo: std::fs::read_to_string("/proc/self/mountinfo").ok(),
            uid,
        }
    }
}

/// The database file after the startup check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbLocation {
    /// Absolute, with symlinks in its existing directories resolved.
    pub path: PathBuf,
    /// `None` when mount detection failed.
    pub placement: Option<Placement>,
}

#[derive(Debug, thiserror::Error)]
pub enum LocationError {
    #[error(
        "database {path} is on the container's own filesystem and is lost when the container is recreated; mount a volume at {dir}, or set DOCKTORNET_ALLOW_EPHEMERAL_DB=true to accept that"
    )]
    Ephemeral { path: PathBuf, dir: PathBuf },
    #[error(
        "database directory {dir} cannot be created or written by uid {uid}: {cause}; give the directory to that uid, as `install -d -o 65532 -g 65532` does in docs/deployment.md"
    )]
    Unwritable {
        dir: PathBuf,
        uid: String,
        cause: std::io::Error,
    },
    #[error(
        "database file {path} cannot be written by uid {uid}: {cause}; give it to that uid, for example with `chown 65532:65532`"
    )]
    UnwritableFile {
        path: PathBuf,
        uid: String,
        cause: std::io::Error,
    },
}

/// Check where `db_path` (absolute) lives before the database is opened:
/// inside a container, refuse the container's own filesystem unless
/// `allow_ephemeral`; anywhere, create the directory and prove it writable.
pub fn prepare(
    db_path: &Path,
    allow_ephemeral: bool,
    env: &Environment,
) -> Result<DbLocation, LocationError> {
    let dir = resolve_dir(db_path.parent().unwrap_or(Path::new("/")));
    let path = match db_path.file_name() {
        Some(name) => dir.join(name),
        None => db_path.to_path_buf(),
    };

    // A directory that does not exist yet lands on the mount holding its
    // nearest existing ancestor, which resolve_dir has already resolved.
    let found = env
        .mountinfo
        .as_deref()
        .and_then(|mountinfo| placement(mountinfo, &dir));
    if env.in_container && found.is_none() {
        tracing::debug!(dir = %dir.display(), "could not find the mount holding the database directory; skipping the ephemeral storage check");
    }
    if env.in_container && found == Some(Placement::RootFilesystem) {
        if !allow_ephemeral {
            return Err(LocationError::Ephemeral { path, dir });
        }
        tracing::warn!(
            path = %path.display(),
            "DOCKTORNET_ALLOW_EPHEMERAL_DB is set and the database is on the container's own filesystem; it will be lost when the container is recreated"
        );
    }

    let uid = env
        .uid
        .map_or_else(|| "unknown".to_string(), |uid| uid.to_string());
    let unwritable = |cause| LocationError::Unwritable {
        dir: dir.clone(),
        uid: uid.clone(),
        cause,
    };
    std::fs::create_dir_all(&dir).map_err(unwritable)?;
    // SQLite writes its -wal and -shm files beside the database, so the
    // directory itself must be writable, not only the file.
    let probe = dir.join(".docktornet-write-check");
    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&probe)
        .map_err(unwritable)?;
    std::fs::remove_file(&probe).map_err(unwritable)?;

    // A writable directory does not make the files already in it writable,
    // for instance after a run as root left them root-owned.
    for suffix in ["", "-wal", "-shm"] {
        let mut file = path.clone().into_os_string();
        file.push(suffix);
        let file = PathBuf::from(file);
        if !file.exists() {
            continue;
        }
        if let Err(cause) = std::fs::OpenOptions::new().write(true).open(&file) {
            return Err(LocationError::UnwritableFile {
                path: file,
                uid: uid.clone(),
                cause,
            });
        }
    }

    Ok(DbLocation {
        path,
        placement: found,
    })
}

/// `dir` made absolute with symlinks resolved, as far as it exists.  The part
/// that does not exist yet is appended as written.
fn resolve_dir(dir: &Path) -> PathBuf {
    for ancestor in dir.ancestors() {
        let Ok(mut resolved) = ancestor.canonicalize() else {
            continue;
        };
        let tail = dir.strip_prefix(ancestor).unwrap_or(Path::new(""));
        for component in tail.components() {
            match component {
                Component::ParentDir => {
                    resolved.pop();
                }
                Component::Normal(name) => resolved.push(name),
                _ => {}
            }
        }
        return resolved;
    }
    dir.to_path_buf()
}

/// The placement of `dir` given the text of `/proc/self/mountinfo`: the mount
/// whose mount point is the longest prefix of `dir`.  `None` when no line
/// parses or no mount holds `dir`.
pub fn placement(mountinfo: &str, dir: &Path) -> Option<Placement> {
    // Path::starts_with compares whole components, so /data does not hold
    // /database.
    let mount_point = mountinfo
        .lines()
        .filter_map(|line| line.split(' ').nth(4))
        .map(decode_mount_point)
        .filter(|mount_point| dir.starts_with(mount_point))
        .max_by_key(|mount_point| mount_point.components().count())?;
    if mount_point == Path::new("/") {
        Some(Placement::RootFilesystem)
    } else {
        Some(Placement::Mounted(mount_point))
    }
}

/// The kernel writes space, tab, newline and backslash in a mount point as
/// three-digit octal escapes (`\040` for a space).
fn decode_mount_point(field: &str) -> PathBuf {
    use std::os::unix::ffi::OsStringExt;

    let bytes = field.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let escape = bytes
            .get(i + 1..i + 4)
            .filter(|digits| bytes[i] == b'\\' && digits.iter().all(|d| (b'0'..=b'7').contains(d)));
        match escape {
            Some(digits) => {
                let value = digits
                    .iter()
                    .fold(0u32, |acc, d| acc * 8 + u32::from(d - b'0'));
                decoded.push(value as u8);
                i += 4;
            }
            None => {
                decoded.push(bytes[i]);
                i += 1;
            }
        }
    }
    PathBuf::from(std::ffi::OsString::from_vec(decoded))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOT: &str = "1005 900 0:52 / / rw,relatime master:1 - overlay overlay rw,lowerdir=/x\n";

    fn line(mount_point: &str) -> String {
        format!("1010 1005 259:2 /srv/vol {mount_point} rw,relatime - ext4 /dev/nvme0n1p2 rw\n")
    }

    #[test]
    fn a_path_under_a_data_bind_mount_is_on_a_mount() {
        let mountinfo = format!("{ROOT}{}", line("/data"));
        assert_eq!(
            placement(&mountinfo, Path::new("/data")),
            Some(Placement::Mounted(PathBuf::from("/data")))
        );
    }

    #[test]
    fn a_path_under_app_with_only_root_mounted_is_ephemeral() {
        let mountinfo = format!("{ROOT}{}", line("/data"));
        assert_eq!(
            placement(&mountinfo, Path::new("/app")),
            Some(Placement::RootFilesystem)
        );
    }

    #[test]
    fn the_longest_mount_point_prefix_wins() {
        let mountinfo = format!("{ROOT}{}{}", line("/data/sub"), line("/data"));
        assert_eq!(
            placement(&mountinfo, Path::new("/data/sub/db")),
            Some(Placement::Mounted(PathBuf::from("/data/sub")))
        );
        assert_eq!(
            placement(&mountinfo, Path::new("/data/subway")),
            Some(Placement::Mounted(PathBuf::from("/data")))
        );
    }

    #[test]
    fn escaped_characters_in_a_mount_point_are_decoded() {
        let mountinfo = format!("{ROOT}{}", line(r"/my\040data"));
        assert_eq!(
            placement(&mountinfo, Path::new("/my data/db")),
            Some(Placement::Mounted(PathBuf::from("/my data")))
        );
    }

    fn container(mountinfo: Option<&str>) -> Environment {
        Environment {
            in_container: true,
            mountinfo: mountinfo.map(str::to_string),
            uid: Some(65532),
        }
    }

    #[test]
    fn a_container_database_on_the_root_filesystem_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("docktornet.db");
        let err = prepare(&db, false, &container(Some(ROOT))).unwrap_err();
        let dir = dir.path().canonicalize().unwrap();
        assert_eq!(
            err.to_string(),
            format!(
                "database {} is on the container's own filesystem and is lost when the container is recreated; mount a volume at {}, or set DOCKTORNET_ALLOW_EPHEMERAL_DB=true to accept that",
                dir.join("docktornet.db").display(),
                dir.display()
            )
        );
    }

    #[test]
    fn the_override_accepts_a_container_database_on_the_root_filesystem() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("docktornet.db");
        let location = prepare(&db, true, &container(Some(ROOT))).unwrap();
        assert_eq!(location.placement, Some(Placement::RootFilesystem));
    }

    #[test]
    fn the_root_filesystem_is_accepted_outside_a_container() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("docktornet.db");
        let env = Environment {
            in_container: false,
            ..container(Some(ROOT))
        };
        let location = prepare(&db, false, &env).unwrap();
        assert_eq!(location.placement, Some(Placement::RootFilesystem));
    }

    #[test]
    fn failed_mount_detection_never_blocks_startup() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("docktornet.db");
        let location = prepare(&db, false, &container(None)).unwrap();
        assert_eq!(location.placement, None);
        let location = prepare(&db, false, &container(Some("garbage"))).unwrap();
        assert_eq!(location.placement, None);
    }

    #[test]
    fn a_database_on_a_mount_reports_its_resolved_path_and_mount_point() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().canonicalize().unwrap();
        let mountinfo = format!("{ROOT}{}", line(real.to_str().unwrap()));
        let db = dir.path().join("nested/./docktornet.db");
        let location = prepare(&db, false, &container(Some(&mountinfo))).unwrap();
        assert_eq!(location.path, real.join("nested/docktornet.db"));
        assert_eq!(location.placement, Some(Placement::Mounted(real.clone())));
        assert!(real.join("nested").is_dir(), "the directory is created");
    }

    #[test]
    fn an_unwritable_directory_is_refused_with_the_directory_and_uid() {
        use std::os::unix::fs::PermissionsExt;

        // Root writes through any mode bits, so the refusal cannot show.
        if Environment::detect().uid == Some(0) {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let locked = dir.path().join("locked");
        std::fs::create_dir(&locked).unwrap();
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555)).unwrap();

        let env = Environment {
            in_container: false,
            mountinfo: None,
            uid: Some(4242),
        };
        let err = prepare(&locked.join("docktornet.db"), false, &env).unwrap_err();
        let message = err.to_string();
        let locked = locked.canonicalize().unwrap();
        assert!(
            message.starts_with(&format!(
                "database directory {} cannot be created or written by uid 4242: ",
                locked.display()
            )),
            "{message}"
        );
        assert!(
            message.contains("install -d -o 65532 -g 65532"),
            "{message}"
        );

        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[test]
    fn an_unwritable_existing_database_file_is_refused_with_the_file_and_uid() {
        use std::os::unix::fs::PermissionsExt;

        if Environment::detect().uid == Some(0) {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let wal = dir.path().join("docktornet.db-wal");
        std::fs::write(&wal, b"").unwrap();
        std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(0o444)).unwrap();

        let env = Environment {
            in_container: false,
            mountinfo: None,
            uid: Some(4242),
        };
        let err = prepare(&dir.path().join("docktornet.db"), false, &env).unwrap_err();
        let message = err.to_string();
        assert!(
            message.starts_with(&format!(
                "database file {} cannot be written by uid 4242: ",
                wal.canonicalize().unwrap().display()
            )),
            "{message}"
        );
    }

    #[test]
    fn unparseable_mountinfo_has_no_placement() {
        assert_eq!(placement("garbage", Path::new("/data")), None);
        assert_eq!(placement("", Path::new("/data")), None);
    }
}
