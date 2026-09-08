#![cfg(unix)]
mod support;

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use rusqlite::Connection;
use serde_json::Value;
use support::{TestDir, ocm_env, run_ocm, stderr, stdout, write_executable_script, write_text};

struct Fixture {
    root: TestDir,
    env: BTreeMap<String, String>,
    state: PathBuf,
    project: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = TestDir::new("upgrade-independent-project");
        let env = ocm_env(&root);
        for (name, version) in [("old", "2026.9.1"), ("new", "2026.9.2")] {
            let executable = root.child(name);
            write_executable_script(
                &executable,
                &format!(
                    r#"#!/bin/sh
case "$1 $2" in
  '--version ') echo '{version}';;
  'config validate') echo 'Config valid';;
  'doctor --lint') echo '{{"ok":true,"checksRun":1,"checksSkipped":0,"findings":[]}}';;
  'update finalize')
    : > "$OCM_PROOF_READY"
    n=0
    while [ ! -f "$OCM_PROOF_RELEASE" ]; do
      n=$((n+1)); [ "$n" -lt 200 ] || exit 92
      sleep 0.025
    done
    if [ "$OCM_PROOF_FAIL" = 1 ]; then echo 'partial migration failed' >&2; exit 23; fi
    echo '{{"status":"ok","mode":"finalize","postUpdate":{{"doctor":{{"status":"ok"}},"plugins":{{"status":"ok"}}}}}}';;
  'gateway status') echo '{{"rpc":{{"ok":true}}}}';;
  *) echo "unexpected fixture invocation $*" >&2; exit 91;;
esac
"#
                ),
            );
            let result = run_ocm(
                root.path(),
                &env,
                &[
                    "runtime",
                    "add",
                    name,
                    "--path",
                    executable.to_str().unwrap(),
                ],
            );
            assert!(result.status.success(), "{}", stderr(&result));
        }
        let result = run_ocm(
            root.path(),
            &env,
            &["env", "create", "demo", "--runtime", "old"],
        );
        assert!(result.status.success(), "{}", stderr(&result));
        let state = root.child("ocm-home/envs/demo/.openclaw");
        let project = state.join("workspace/projects/example");
        write_text(&state.join("openclaw.json"), "{}\n");
        write_text(&project.join("code"), "before\n");
        write_text(&project.join("deleted"), "before\n");
        write_text(
            &project.join("node_modules/package/index.js"),
            "dependency\n",
        );
        symlink("code", project.join("link")).unwrap();
        write_text(&state.join("workspace/.openclaw/legacy-state"), "legacy\n");
        write_text(&state.join("workspace/IDENTITY.md"), "identity\n");
        write_text(
            &state.join("unknown/node_modules/payload"),
            "runtime-before\n",
        );
        write_text(&state.join("unknown/regular.sock"), "ordinary file\n");
        write_text(&state.join("credentials/synthetic"), "fixture-only\n");
        fs::set_permissions(
            state.join("credentials/synthetic"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let db = Connection::open(state.join("arbitrary.data")).unwrap();
        db.execute_batch(
            "CREATE TABLE durable(value TEXT); INSERT INTO durable VALUES ('before');",
        )
        .unwrap();
        drop(db);
        let fixture = Self {
            root,
            env,
            state,
            project,
        };
        let result = fixture.run(&[
            "env",
            "set-independent-paths",
            "demo",
            ".openclaw/workspace/projects",
        ]);
        assert!(result.status.success(), "{}", stderr(&result));
        fixture
    }

    fn run(&self, args: &[&str]) -> std::process::Output {
        run_ocm(self.root.path(), &self.env, args)
    }

    fn edit_project(&self) -> fs::Metadata {
        write_text(&self.project.join("code"), "current developer work\n");
        fs::set_permissions(self.project.join("code"), fs::Permissions::from_mode(0o600)).unwrap();
        fs::remove_file(self.project.join("deleted")).unwrap();
        write_text(&self.project.join("created"), "new work\n");
        fs::remove_file(self.project.join("link")).unwrap();
        symlink("created", self.project.join("link")).unwrap();
        fs::metadata(self.project.join("code")).unwrap()
    }

    fn assert_project(&self, expected: &fs::Metadata) {
        assert_eq!(
            fs::read_to_string(self.project.join("code")).unwrap(),
            "current developer work\n"
        );
        let current = fs::metadata(self.project.join("code")).unwrap();
        assert_eq!(
            (
                current.ino(),
                current.mode(),
                current.mtime(),
                current.mtime_nsec()
            ),
            (
                expected.ino(),
                expected.mode(),
                expected.mtime(),
                expected.mtime_nsec()
            )
        );
        assert!(!self.project.join("deleted").exists());
        assert_eq!(
            fs::read_to_string(self.project.join("created")).unwrap(),
            "new work\n"
        );
        assert_eq!(
            fs::read_link(self.project.join("link")).unwrap(),
            PathBuf::from("created")
        );
    }
}

#[test]
fn independent_project_survives_success_failure_interrupt_and_explicit_rollback() {
    for mode in ["success", "failure", "interrupt"] {
        let fixture = Fixture::new();
        let ready = fixture.root.child("ready");
        let release = fixture.root.child("release");
        let mut child = Command::new(env!("CARGO_BIN_EXE_ocm"))
            .args(["upgrade", "demo", "--runtime", "new", "--json"])
            .current_dir(fixture.root.path())
            .env_clear()
            .envs(&fixture.env)
            .env("OCM_PROOF_READY", &ready)
            .env("OCM_PROOF_RELEASE", &release)
            .env("OCM_PROOF_FAIL", if mode == "failure" { "1" } else { "0" })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        while !ready.exists() && Instant::now() < deadline && child.try_wait().unwrap().is_none() {
            sleep(Duration::from_millis(10));
        }
        if !ready.exists() {
            write_text(&release, "release");
            let output = child.wait_with_output().unwrap();
            panic!(
                "finalizer not reached: {} {}",
                stdout(&output),
                stderr(&output)
            );
        }
        let expected = fixture.edit_project();
        let db = Connection::open(fixture.state.join("arbitrary.data")).unwrap();
        db.execute_batch(
            "ALTER TABLE durable ADD COLUMN migrated TEXT; UPDATE durable SET value='after';",
        )
        .unwrap();
        drop(db);
        write_text(
            &fixture.state.join("openclaw.json"),
            "{\"migration\":\"partial\"}\n",
        );
        write_text(
            &fixture.state.join("unknown/node_modules/payload"),
            "runtime-after\n",
        );
        fs::remove_file(fixture.state.join("workspace/.openclaw/legacy-state")).unwrap();
        write_text(&fixture.state.join("migration-receipt"), "partial\n");
        if mode == "interrupt" {
            // This child belongs exclusively to this fixture.
            assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
        }
        write_text(&release, "release");
        let output = child.wait_with_output().unwrap();
        assert_eq!(
            output.status.success(),
            mode == "success",
            "{} {}",
            stdout(&output),
            stderr(&output)
        );
        fixture.assert_project(&expected);
        let receipt: Value = serde_json::from_str(&stdout(&output)).unwrap();
        if mode == "success" {
            let rollback = fixture.run(&["upgrade", "rollback", "demo", "--json"]);
            assert!(
                rollback.status.success(),
                "{} {}",
                stdout(&rollback),
                stderr(&rollback)
            );
        } else {
            assert_eq!(receipt["rollback"], "restored");
        }
        fixture.assert_project(&expected);
        assert_eq!(
            fs::read_to_string(fixture.state.join("openclaw.json")).unwrap(),
            "{}\n"
        );
        assert_eq!(
            fs::read_to_string(fixture.state.join("unknown/node_modules/payload")).unwrap(),
            "runtime-before\n"
        );
        assert_eq!(
            fs::read_to_string(fixture.state.join("unknown/regular.sock")).unwrap(),
            "ordinary file\n"
        );
        assert_eq!(
            fs::metadata(fixture.state.join("credentials/synthetic"))
                .unwrap()
                .mode()
                & 0o777,
            0o600
        );
        assert!(
            fixture
                .state
                .join("workspace/.openclaw/legacy-state")
                .exists()
        );
        assert!(!fixture.state.join("migration-receipt").exists());
        let db = Connection::open(fixture.state.join("arbitrary.data")).unwrap();
        assert_eq!(
            db.query_row("SELECT value FROM durable", [], |row| row
                .get::<_, String>(0))
                .unwrap(),
            "before"
        );
        assert!(db.prepare("SELECT migrated FROM durable").is_err());
        let snapshot = fixture.run(&[
            "env",
            "snapshot",
            "show",
            "demo",
            receipt["snapshotId"].as_str().unwrap(),
            "--json",
        ]);
        assert!(snapshot.status.success(), "{}", stderr(&snapshot));
        let snapshot: Value = serde_json::from_str(&stdout(&snapshot)).unwrap();
        assert!(
            !PathBuf::from(snapshot["archivePath"].as_str().unwrap())
                .join(".openclaw/workspace/projects")
                .exists()
        );
    }
}

#[test]
fn scope_refuses_configuration_workspace_and_path_escape_exclusions() {
    let fixture = Fixture::new();
    write_text(&fixture.state.join("workspace/projects/include.json"), "{}");
    write_text(
        &fixture.state.join("openclaw.json"),
        "{\"$include\":\"workspace/projects/include.json\"}",
    );
    for path in [
        ".openclaw/workspace",
        ".openclaw",
        "../escape",
        "/absolute",
        ".openclaw/credentials",
        ".openclaw/workspace/IDENTITY.md",
        ".openclaw/workspace/projects",
    ] {
        let result = fixture.run(&["env", "set-independent-paths", "demo", path]);
        assert!(
            !result.status.success(),
            "unsafe exclusion accepted: {path}"
        );
    }
    write_text(&fixture.state.join("openclaw.json"), "{}");
    symlink("projects", fixture.state.join("workspace/alias")).unwrap();
    let result = fixture.run(&[
        "env",
        "set-independent-paths",
        "demo",
        ".openclaw/workspace/alias/example",
    ]);
    assert!(!result.status.success(), "symlink ancestor accepted");
    fs::remove_file(fixture.state.join("openclaw.json")).unwrap();
    symlink(
        "workspace/projects/include.json",
        fixture.state.join("openclaw.json"),
    )
    .unwrap();
    let result = fixture.run(&[
        "env",
        "set-independent-paths",
        "demo",
        ".openclaw/workspace/projects",
    ]);
    assert!(
        !result.status.success(),
        "symlinked configuration target excluded"
    );
}

#[test]
fn frozen_scope_and_invalid_metadata_never_fall_back_to_whole_root_restore() {
    let fixture = Fixture::new();
    let ready = fixture.root.child("ready");
    let release = fixture.root.child("release");
    write_text(&release, "continue");
    let output = Command::new(env!("CARGO_BIN_EXE_ocm"))
        .args(["upgrade", "demo", "--runtime", "new", "--json"])
        .current_dir(fixture.root.path())
        .env_clear()
        .envs(&fixture.env)
        .env("OCM_PROOF_READY", ready)
        .env("OCM_PROOF_RELEASE", release)
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", stderr(&output));
    let receipt: Value = serde_json::from_str(&stdout(&output)).unwrap();
    let id = receipt["snapshotId"].as_str().unwrap();
    let show = fixture.run(&["env", "snapshot", "show", "demo", id, "--json"]);
    let snapshot: Value = serde_json::from_str(&stdout(&show)).unwrap();
    let archive = PathBuf::from(snapshot["archivePath"].as_str().unwrap());
    let snapshot_meta = archive.parent().unwrap().join(format!("{id}.json"));
    let original = fs::read(&snapshot_meta).unwrap();
    let expected = fixture.edit_project();
    let clear = fixture.run(&["env", "set-independent-paths", "demo", "none"]);
    assert!(clear.status.success(), "{}", stderr(&clear));
    let mut invalid: Value = serde_json::from_slice(&original).unwrap();
    invalid.as_object_mut().unwrap().remove("upgradeScope");
    fs::write(&snapshot_meta, serde_json::to_vec(&invalid).unwrap()).unwrap();
    let restore = fixture.run(&["env", "snapshot", "restore", "demo", id]);
    assert!(!restore.status.success(), "missing scope was accepted");
    fixture.assert_project(&expected);
    fs::write(&snapshot_meta, original).unwrap();
    // An artifact with an undeclared captured copy must also be refused.
    write_text(
        &archive.join(".openclaw/workspace/projects/injected"),
        "unsafe",
    );
    let restore = fixture.run(&["env", "snapshot", "restore", "demo", id]);
    assert!(
        !restore.status.success(),
        "conflicting artifact was accepted"
    );
    fixture.assert_project(&expected);
    fs::remove_dir_all(archive.join(".openclaw/workspace/projects")).unwrap();
    let restore = fixture.run(&["env", "snapshot", "restore", "demo", id]);
    assert!(restore.status.success(), "{}", stderr(&restore));
    fixture.assert_project(&expected);
}

#[test]
fn full_backup_still_rewinds_declared_independent_content() {
    let fixture = Fixture::new();
    let snapshot = fixture.run(&["env", "snapshot", "create", "demo", "--json"]);
    assert!(snapshot.status.success(), "{}", stderr(&snapshot));
    let snapshot: Value = serde_json::from_str(&stdout(&snapshot)).unwrap();
    assert!(snapshot.get("upgradeScope").is_none());
    fixture.edit_project();
    let restore = fixture.run(&[
        "env",
        "snapshot",
        "restore",
        "demo",
        snapshot["id"].as_str().unwrap(),
    ]);
    assert!(restore.status.success(), "{}", stderr(&restore));
    assert_eq!(
        fs::read_to_string(fixture.project.join("code")).unwrap(),
        "before\n"
    );
    assert!(fixture.project.join("deleted").exists());
    assert!(!fixture.project.join("created").exists());
}
