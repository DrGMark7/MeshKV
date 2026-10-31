//! Phase 1 gate checks: object semantics on real files, no network. Correctness only, not a benchmark.
use rdma_kv_store::*;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

fn tmpdir(tag: &str) -> PathBuf {
    let n = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let p = std::env::temp_dir().join(format!("rkv-test-{}-{}-{}", tag, std::process::id(), n));
    fs::create_dir_all(&p).unwrap();
    p
}

fn payload(n: usize, seed: u8) -> Vec<u8> {
    (0..n).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed)).collect()
}

fn store(root: &PathBuf) -> Arc<FileStore> {
    FileStore::open(root, StoreConfig { quota_bytes: 64 << 20, max_object_bytes: 8 << 20, fsync: true, deny_prefixes: vec![] }).unwrap()
}

fn find_obj_files(root: &PathBuf) -> Vec<PathBuf> {
    let mut v = vec![];
    fn walk(p: &std::path::Path, v: &mut Vec<PathBuf>) {
        for e in fs::read_dir(p).unwrap() {
            let e = e.unwrap();
            if e.file_type().unwrap().is_dir() {
                walk(&e.path(), v)
            } else if e.file_name().to_string_lossy().ends_with(".obj") {
                v.push(e.path())
            }
        }
    }
    walk(root, &mut v);
    v
}

#[test]
fn put_get_1mib_is_byte_identical_and_a_real_file_exists() {
    let root = tmpdir("basic");
    let s = store(&root);
    let data = payload(1 << 20, 7);
    assert!(s.put("t1", "prefix/abc", "fp-1", "raw", &data).unwrap());
    let files = find_obj_files(&root);
    assert_eq!(files.len(), 1, "exactly one real object file");
    assert!(fs::metadata(&files[0]).unwrap().len() > (1 << 20));
    let (meta, got) = s.get("t1", "prefix/abc").unwrap().unwrap();
    assert_eq!(got, data);
    assert_eq!(meta.fingerprint, "fp-1");
    assert!(s.get("t1", "missing").unwrap().is_none(), "absent key is a MISS, not an error");
    assert!(s.get("other-ns", "prefix/abc").unwrap().is_none(), "namespaces are separate");
    fs::remove_dir_all(&root).unwrap();
}

#[test]
fn duplicate_same_content_is_idempotent_and_different_content_conflicts() {
    let root = tmpdir("dup");
    let s = store(&root);
    let d1 = payload(4096, 1);
    assert!(s.put("t1", "k", "fp", "raw", &d1).unwrap());
    assert!(!s.put("t1", "k", "fp", "raw", &d1).unwrap(), "same key + same checksum = idempotent success");
    let d2 = payload(4096, 2);
    match s.put("t1", "k", "fp", "raw", &d2) {
        Err(KvError::Conflict(_)) => {}
        other => panic!("expected CONFLICT, got {:?}", other.map(|_| ())),
    }
    match s.put("t1", "k", "fp-other", "raw", &d1) {
        Err(KvError::Conflict(_)) => {}
        other => panic!("different metadata must conflict, got {:?}", other.map(|_| ())),
    }
    assert_eq!(s.get("t1", "k").unwrap().unwrap().1, d1, "original object untouched");
    fs::remove_dir_all(&root).unwrap();
}

#[test]
fn corrupt_and_truncated_files_never_return_bytes() {
    let root = tmpdir("corrupt");
    let s = store(&root);
    let d = payload(1 << 20, 3);
    s.put("t1", "a", "fp", "raw", &d).unwrap();
    s.put("t1", "b", "fp", "raw", &d).unwrap();
    let files = find_obj_files(&root);
    assert_eq!(files.len(), 2);
    // flip one payload byte in file 0, truncate file 1
    let mut b = fs::read(&files[0]).unwrap();
    let last = b.len() - 10;
    b[last] ^= 0xff;
    fs::write(&files[0], &b).unwrap();
    let len = fs::metadata(&files[1]).unwrap().len();
    fs::OpenOptions::new().write(true).open(&files[1]).unwrap().set_len(len - 100).unwrap();
    for k in ["a", "b"] {
        match s.get("t1", k) {
            Err(KvError::ChecksumMismatch(_)) => {}
            other => panic!("key {k}: expected CHECKSUM_MISMATCH, got {:?}", other.map(|o| o.is_some())),
        }
    }
    fs::remove_dir_all(&root).unwrap();
}

#[test]
fn restart_keeps_objects_and_cleans_orphan_tmp_files() {
    let root = tmpdir("restart");
    let d = payload(2 << 20, 9);
    {
        let s = store(&root);
        s.put("t1", "persist", "fp", "raw", &d).unwrap();
        // an interrupted PUT: transaction dropped without commit leaves nothing published
        let meta = ObjectMeta::for_payload("fp", "raw", &d);
        match s.begin_put("t1", "partial", meta).unwrap() {
            PutOutcome::Txn(mut t) => {
                t.write(&d[..1000]).unwrap();
            }
            _ => panic!(),
        }
        assert!(s.get("t1", "partial").unwrap().is_none(), "partial object must not be visible");
        assert_eq!(s.stats().reserved_bytes, 0, "dropped txn releases its reservation");
    }
    // simulate a crash leftover
    let leftover = fs::read_dir(root.join("t1")).unwrap().next().unwrap().unwrap().path().join(".tmp-999-0-deadbeef");
    fs::write(&leftover, b"junk").unwrap();
    let s2 = store(&root);
    assert!(!leftover.exists(), "orphan tmp removed at startup");
    let (_, got) = s2.get("t1", "persist").unwrap().unwrap();
    assert_eq!(got, d, "object survives daemon restart");
    assert_eq!(s2.stats().objects, 1);
    fs::remove_dir_all(&root).unwrap();
}

#[test]
fn bad_names_quota_size_and_single_owner_are_enforced() {
    let root = tmpdir("limits");
    let s = store(&root);
    for ns in ["", ".", "..", "../x", "a/b", ".hidden"] {
        assert!(matches!(s.put(ns, "k", "fp", "raw", b"x"), Err(KvError::BadRequest(_))), "ns {ns:?}");
    }
    assert!(matches!(s.put("t1", "", "fp", "raw", b"x"), Err(KvError::BadRequest(_))));
    // keys with path-like content are fine: they are hashed, never used as paths
    assert!(s.put("t1", "../../etc/passwd", "fp", "raw", b"x").unwrap());
    assert!(!root.parent().unwrap().join("etc").exists());
    // too large
    let big = payload(9 << 20, 1);
    assert!(matches!(s.put("t1", "big", "fp", "raw", &big), Err(KvError::TooLarge(_))));
    // quota (64 MiB): 7 x 8 MiB fits ~ 56 MiB, 9th does not
    let mut ok = 0;
    let mut quota_hit = false;
    for i in 0..10 {
        match s.put("t1", &format!("q{i}"), "fp", "raw", &payload(8 << 20, i as u8)) {
            Ok(_) => ok += 1,
            Err(KvError::QuotaExceeded(_)) => {
                quota_hit = true;
                break;
            }
            Err(e) => panic!("{e}"),
        }
    }
    assert!(quota_hit && ok >= 6, "quota enforced after {ok} objects");
    // a second daemon on the same root is refused
    match FileStore::open(&root, StoreConfig { deny_prefixes: vec![], ..StoreConfig::default() }) {
        Err(KvError::Unavailable(_)) => {}
        other => panic!("expected UNAVAILABLE, got {:?}", other.map(|_| ())),
    }
    // protected prefix is refused
    assert!(matches!(FileStore::open(std::path::Path::new("/srv/llm-models/x"), StoreConfig::default()), Err(KvError::BadRequest(_))));
    fs::remove_dir_all(&root).unwrap();
}

#[test]
fn declared_checksum_mismatch_is_rejected_and_publishes_nothing() {
    let root = tmpdir("badsum");
    let s = store(&root);
    let d = payload(100_000, 5);
    let mut meta = ObjectMeta::for_payload("fp", "raw", &d);
    meta.sha256 = "0".repeat(64);
    match s.begin_put("t1", "x", meta).unwrap() {
        PutOutcome::Txn(mut t) => {
            t.write(&d).unwrap();
            assert!(matches!(t.commit(), Err(KvError::ChecksumMismatch(_))));
        }
        _ => panic!(),
    }
    assert!(s.get("t1", "x").unwrap().is_none());
    assert_eq!(find_obj_files(&root).len(), 0);
    assert_eq!(s.stats().reserved_bytes, 0);
    fs::remove_dir_all(&root).unwrap();
}
