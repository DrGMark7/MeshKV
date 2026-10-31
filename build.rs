// Links the pinned NIXL C API. NIXL_PREFIX points to the install tree of the tested revision
// (default: the tree built by the 2026-10-29 experiment).
fn main() {
    let prefix = std::env::var("NIXL_PREFIX").unwrap_or_else(|_| "/home/hpcnc/rdma-kv-experiments/build/nixl-install".to_string());
    let lib = format!("{}/lib/x86_64-linux-gnu", prefix);
    println!("cargo:rustc-link-search=native={}", lib);
    println!("cargo:rustc-link-lib=dylib=nixl_capi");
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib);
    println!("cargo:rerun-if-env-changed=NIXL_PREFIX");
    println!("cargo:rerun-if-changed=build.rs");
}
