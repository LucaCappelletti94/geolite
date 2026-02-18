//! `geolite-sqlite` — SQLite loadable extension built on `geolite-core`.
//!
//! # Loading the extension (native only)
//! ```sql
//! SELECT load_extension('./target/release/libgeolite_sqlite');
//! ```
//!
//! # Programmatic registration
//! ```rust,no_run
//! # #[cfg(not(target_arch = "wasm32"))]
//! # fn example() {
//! # // Safety: db must be a valid open sqlite3 handle.
//! # unsafe {
//! # let db: *mut libsqlite3_sys::sqlite3 = std::ptr::null_mut();
//! let rc = geolite_sqlite::register_functions(db);
//! assert_eq!(rc, 0); // SQLITE_OK
//! # }
//! # }
//! ```

mod ffi;
mod sqlite_compat;
pub use ffi::register_functions;
