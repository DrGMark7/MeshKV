//! Thin safe wrapper over the pinned NIXL C API (`libnixl_capi.so`, revision 2653767b80f0).
//! All `unsafe` code of the crate lives here. DRAM only: no CUDA address is ever registered.
//!
//! Lifecycle: create agent -> create UCX backend -> register DRAM -> exchange metadata (done by the control
//! channel) -> transfer (WRITE/READ) with explicit completion polling -> deregister -> destroy.
//! NIXL may deadlock when called concurrently (see upstream bindings README), so every FFI call goes through
//! one process-wide mutex; completion polling takes the lock per call, not per transfer.
use crate::error::{KvError, Result};
use std::ffi::{c_char, c_void, CStr, CString};
use std::sync::Mutex;
use std::time::{Duration, Instant};

type Status = i32;
const OK: Status = 0;
const IN_PROG: Status = 1;
const MEM_DRAM: i32 = 0;
const XFER_READ: i32 = 0;
const XFER_WRITE: i32 = 1;

#[repr(C)]
struct Opaque {
    _p: [u8; 0],
}
type Agent = *mut Opaque;
type Backend = *mut Opaque;
type Params = *mut Opaque;
type MemList = *mut Opaque;
type RegDl = *mut Opaque;
type XferDl = *mut Opaque;
type Req = *mut Opaque;
type OptArgs = *mut Opaque;

extern "C" {
    fn nixl_capi_create_agent(name: *const c_char, agent: *mut Agent) -> Status;
    fn nixl_capi_destroy_agent(agent: Agent) -> Status;
    fn nixl_capi_get_local_md(agent: Agent, data: *mut *mut c_void, len: *mut usize) -> Status;
    fn nixl_capi_load_remote_md(agent: Agent, data: *const c_void, len: usize, name: *mut *mut c_char) -> Status;
    fn nixl_capi_invalidate_remote_md(agent: Agent, name: *const c_char) -> Status;
    fn nixl_capi_get_plugin_params(agent: Agent, plugin: *const c_char, mems: *mut MemList, params: *mut Params) -> Status;
    fn nixl_capi_destroy_mem_list(l: MemList) -> Status;
    fn nixl_capi_destroy_params(p: Params) -> Status;
    fn nixl_capi_create_backend(agent: Agent, plugin: *const c_char, params: Params, backend: *mut Backend) -> Status;
    fn nixl_capi_create_reg_dlist(mem_type: i32, dl: *mut RegDl) -> Status;
    fn nixl_capi_reg_dlist_add_desc(dl: RegDl, addr: usize, len: usize, dev: u64, md: *const c_void, md_len: usize) -> Status;
    fn nixl_capi_destroy_reg_dlist(dl: RegDl) -> Status;
    fn nixl_capi_register_mem(agent: Agent, dl: RegDl, opt: OptArgs) -> Status;
    fn nixl_capi_deregister_mem(agent: Agent, dl: RegDl, opt: OptArgs) -> Status;
    fn nixl_capi_create_xfer_dlist(mem_type: i32, dl: *mut XferDl) -> Status;
    fn nixl_capi_xfer_dlist_add_desc(dl: XferDl, addr: usize, len: usize, dev: u64) -> Status;
    fn nixl_capi_destroy_xfer_dlist(dl: XferDl) -> Status;
    fn nixl_capi_create_xfer_req(agent: Agent, op: i32, local: XferDl, remote: XferDl, remote_agent: *const c_char, req: *mut Req, opt: OptArgs) -> Status;
    fn nixl_capi_post_xfer_req(agent: Agent, req: Req, opt: OptArgs) -> Status;
    fn nixl_capi_get_xfer_status(agent: Agent, req: Req) -> Status;
    fn nixl_capi_release_xfer_req(agent: Agent, req: Req) -> Status;
    fn nixl_capi_destroy_xfer_req(req: Req) -> Status;
}

static NIXL_LOCK: Mutex<()> = Mutex::new(());

fn chk(what: &str, st: Status) -> Result<()> {
    if st == OK {
        Ok(())
    } else {
        Err(KvError::Unavailable(format!("nixl {what} failed (status {st})")))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    /// initiator pushes local memory into remote memory
    Write,
    /// initiator pulls remote memory into local memory
    Read,
}

pub struct NixlAgent {
    handle: Agent,
    pub name: String,
}
// All access is serialized by NIXL_LOCK.
unsafe impl Send for NixlAgent {}
unsafe impl Sync for NixlAgent {}

impl NixlAgent {
    /// Create an agent with the UCX backend (CPU DRAM). UCX_* and NIXL_PLUGIN_DIR must already be set in the env.
    pub fn new(name: &str) -> Result<NixlAgent> {
        let _g = NIXL_LOCK.lock().unwrap();
        let cname = CString::new(name).map_err(|_| KvError::BadRequest("agent name".into()))?;
        let mut agent: Agent = std::ptr::null_mut();
        // SAFETY: out-pointer is valid, name is NUL-terminated.
        chk("create_agent", unsafe { nixl_capi_create_agent(cname.as_ptr(), &mut agent) })?;
        let a = NixlAgent { handle: agent, name: name.to_string() };
        let plugin = CString::new("UCX").unwrap();
        let mut mems: MemList = std::ptr::null_mut();
        let mut params: Params = std::ptr::null_mut();
        // SAFETY: handles come from the calls above.
        unsafe {
            chk("get_plugin_params(UCX)", nixl_capi_get_plugin_params(a.handle, plugin.as_ptr(), &mut mems, &mut params))?;
            let mut backend: Backend = std::ptr::null_mut();
            let st = nixl_capi_create_backend(a.handle, plugin.as_ptr(), params, &mut backend);
            nixl_capi_destroy_mem_list(mems);
            nixl_capi_destroy_params(params);
            chk("create_backend(UCX)", st)?;
        }
        Ok(a)
    }

    /// Local metadata blob (contains registered memory); send to the peer over the control channel.
    pub fn local_metadata(&self) -> Result<Vec<u8>> {
        let _g = NIXL_LOCK.lock().unwrap();
        let mut data: *mut c_void = std::ptr::null_mut();
        let mut len = 0usize;
        // SAFETY: NIXL returns a malloc'd blob that we copy and free.
        unsafe {
            chk("get_local_md", nixl_capi_get_local_md(self.handle, &mut data, &mut len))?;
            let v = std::slice::from_raw_parts(data as *const u8, len).to_vec();
            libc::free(data);
            Ok(v)
        }
    }

    /// Load a peer's metadata; returns the peer's agent name.
    pub fn load_remote(&self, md: &[u8]) -> Result<String> {
        let _g = NIXL_LOCK.lock().unwrap();
        let mut name: *mut c_char = std::ptr::null_mut();
        // SAFETY: md slice valid for len; name is strdup'd by NIXL and freed here.
        unsafe {
            chk("load_remote_md", nixl_capi_load_remote_md(self.handle, md.as_ptr() as *const c_void, md.len(), &mut name))?;
            let s = CStr::from_ptr(name).to_string_lossy().to_string();
            libc::free(name as *mut c_void);
            Ok(s)
        }
    }

    pub fn invalidate_remote(&self, name: &str) {
        let _g = NIXL_LOCK.lock().unwrap();
        if let Ok(c) = CString::new(name) {
            // SAFETY: valid handle + C string. Failure (unknown peer) is harmless.
            unsafe {
                nixl_capi_invalidate_remote_md(self.handle, c.as_ptr());
            }
        }
    }

    /// One-sided transfer between a registered local range and a registered remote range, polled to completion.
    /// On `Timeout` the request is intentionally leaked and the caller must quarantine both buffers.
    pub fn transfer(&self, op: Op, remote_agent: &str, local_addr: usize, remote_addr: usize, len: usize, deadline: Duration) -> Result<()> {
        if len == 0 {
            return Ok(());
        }
        let rname = CString::new(remote_agent).map_err(|_| KvError::BadRequest("remote agent name".into()))?;
        let (mut ldl, mut rdl): (XferDl, XferDl) = (std::ptr::null_mut(), std::ptr::null_mut());
        let mut req: Req = std::ptr::null_mut();
        let start = Instant::now();
        {
            let _g = NIXL_LOCK.lock().unwrap();
            // SAFETY: creating descriptor lists and a request from valid handles; cleaned up below.
            unsafe {
                chk("create_xfer_dlist", nixl_capi_create_xfer_dlist(MEM_DRAM, &mut ldl))?;
                chk("create_xfer_dlist", nixl_capi_create_xfer_dlist(MEM_DRAM, &mut rdl))?;
                let r = (|| -> Result<Status> {
                    chk("add_desc", nixl_capi_xfer_dlist_add_desc(ldl, local_addr, len, 0))?;
                    chk("add_desc", nixl_capi_xfer_dlist_add_desc(rdl, remote_addr, len, 0))?;
                    let o = if op == Op::Write { XFER_WRITE } else { XFER_READ };
                    chk("create_xfer_req", nixl_capi_create_xfer_req(self.handle, o, ldl, rdl, rname.as_ptr(), &mut req, std::ptr::null_mut()))?;
                    let st = nixl_capi_post_xfer_req(self.handle, req, std::ptr::null_mut());
                    if st != OK && st != IN_PROG {
                        nixl_capi_release_xfer_req(self.handle, req);
                        nixl_capi_destroy_xfer_req(req);
                        req = std::ptr::null_mut();
                        return Err(KvError::Unavailable(format!("nixl post_xfer_req failed (status {st})")));
                    }
                    Ok(st)
                })();
                match r {
                    Ok(OK) => {
                        nixl_capi_release_xfer_req(self.handle, req);
                        nixl_capi_destroy_xfer_req(req);
                        nixl_capi_destroy_xfer_dlist(ldl);
                        nixl_capi_destroy_xfer_dlist(rdl);
                        return Ok(());
                    }
                    Ok(_) => {}
                    Err(e) => {
                        nixl_capi_destroy_xfer_dlist(ldl);
                        nixl_capi_destroy_xfer_dlist(rdl);
                        return Err(e);
                    }
                }
            }
        }
        // poll to completion
        let mut spins = 0u32;
        loop {
            let st = {
                let _g = NIXL_LOCK.lock().unwrap();
                // SAFETY: valid handles; the request is still owned by us.
                unsafe { nixl_capi_get_xfer_status(self.handle, req) }
            };
            if st == OK {
                let _g = NIXL_LOCK.lock().unwrap();
                // SAFETY: completed request, release then destroy as the upstream bindings do.
                unsafe {
                    nixl_capi_release_xfer_req(self.handle, req);
                    nixl_capi_destroy_xfer_req(req);
                    nixl_capi_destroy_xfer_dlist(ldl);
                    nixl_capi_destroy_xfer_dlist(rdl);
                }
                return Ok(());
            }
            if st != IN_PROG {
                let _g = NIXL_LOCK.lock().unwrap();
                // SAFETY: failed request is no longer in flight.
                unsafe {
                    nixl_capi_release_xfer_req(self.handle, req);
                    nixl_capi_destroy_xfer_req(req);
                    nixl_capi_destroy_xfer_dlist(ldl);
                    nixl_capi_destroy_xfer_dlist(rdl);
                }
                return Err(KvError::Unavailable(format!("nixl transfer failed (status {st})")));
            }
            if start.elapsed() > deadline {
                // Completion is uncertain: leak request + lists on purpose (never free memory that may still be in use).
                return Err(KvError::Timeout(format!("nixl transfer exceeded {:?}", deadline)));
            }
            spins += 1;
            if spins < 2000 {
                std::thread::yield_now();
            } else {
                std::thread::sleep(Duration::from_micros(50));
            }
        }
    }

    fn register(&self, addr: usize, len: usize) -> Result<RegDl> {
        let _g = NIXL_LOCK.lock().unwrap();
        let mut dl: RegDl = std::ptr::null_mut();
        // SAFETY: descriptor list built from a live allocation owned by `RegisteredBuf`.
        unsafe {
            chk("create_reg_dlist", nixl_capi_create_reg_dlist(MEM_DRAM, &mut dl))?;
            chk("reg_dlist_add_desc", nixl_capi_reg_dlist_add_desc(dl, addr, len, 0, std::ptr::null(), 0))?;
            let st = nixl_capi_register_mem(self.handle, dl, std::ptr::null_mut());
            if st != OK {
                nixl_capi_destroy_reg_dlist(dl);
                return Err(KvError::Unavailable(format!("nixl register_mem failed (status {st}); check ulimit -l / size")));
            }
        }
        Ok(dl)
    }

    fn deregister(&self, dl: RegDl) {
        let _g = NIXL_LOCK.lock().unwrap();
        // SAFETY: dl was registered by `register` on this agent.
        unsafe {
            nixl_capi_deregister_mem(self.handle, dl, std::ptr::null_mut());
            nixl_capi_destroy_reg_dlist(dl);
        }
    }
}

impl Drop for NixlAgent {
    fn drop(&mut self) {
        let _g = NIXL_LOCK.lock().unwrap();
        // SAFETY: last owner.
        unsafe {
            nixl_capi_destroy_agent(self.handle);
        }
    }
}

/// A page-aligned, zero-initialised DRAM region registered with an agent (bounded staging/pool buffer).
pub struct RegisteredBuf {
    ptr: *mut u8,
    len: usize,
    layout: std::alloc::Layout,
    agent: std::sync::Arc<NixlAgent>,
    dl: RegDl,
}
unsafe impl Send for RegisteredBuf {}
unsafe impl Sync for RegisteredBuf {}

impl RegisteredBuf {
    pub fn new(agent: &std::sync::Arc<NixlAgent>, len: usize) -> Result<RegisteredBuf> {
        let layout = std::alloc::Layout::from_size_align(len, 4096).map_err(|e| KvError::BadRequest(e.to_string()))?;
        // SAFETY: non-zero size layout.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(KvError::Io(format!("cannot allocate {len} bytes")));
        }
        match agent.register(ptr as usize, len) {
            Ok(dl) => Ok(RegisteredBuf { ptr, len, layout, agent: agent.clone(), dl }),
            Err(e) => {
                // SAFETY: allocated above, never registered.
                unsafe { std::alloc::dealloc(ptr, layout) };
                Err(e)
            }
        }
    }
    pub fn addr(&self) -> usize {
        self.ptr as usize
    }
    pub fn len(&self) -> usize {
        self.len
    }
    /// # Safety
    /// Caller must guarantee exclusive logical ownership of `[off, off+n)` (slot discipline) and that no RDMA
    /// transfer touching the range is still in flight.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn slice_mut(&self, off: usize, n: usize) -> &mut [u8] {
        assert!(off + n <= self.len);
        std::slice::from_raw_parts_mut(self.ptr.add(off), n)
    }
    /// # Safety
    /// Same contract as `slice_mut`.
    pub unsafe fn slice(&self, off: usize, n: usize) -> &[u8] {
        assert!(off + n <= self.len);
        std::slice::from_raw_parts(self.ptr.add(off), n)
    }
}

impl Drop for RegisteredBuf {
    fn drop(&mut self) {
        self.agent.deregister(self.dl);
        // SAFETY: allocated with this layout in `new`; deregistered above.
        unsafe { std::alloc::dealloc(self.ptr, self.layout) };
    }
}
