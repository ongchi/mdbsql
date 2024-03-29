use std::env;
use std::path::PathBuf;

fn main() {
    let library = pkg_config::probe_library("libmdbsql").unwrap_or_else(|e| panic!("{}", e));

    let bindings = bindgen::Builder::default()
        .clang_args(
            library
                .include_paths
                .iter()
                .map(|path| format!("-I{}", path.to_string_lossy())),
        )
        .clang_args(
            library
                .link_paths
                .iter()
                .map(|path| format!("-L{}", path.to_string_lossy())),
        )
        .clang_arg("-D HAVE_GLIB=1")
        .header("src/wrapper.h")
        .allowlist_function(r"mdb_.*")
        .allowlist_type(r".*Mdb.*")
        .allowlist_var(r".*MDB.*")
        .blocklist_type(r".*GList")
        .blocklist_type(r".*GPtrArray")
        .blocklist_type(r".*FILE")
        .blocklist_type(r".*__sFILEX")
        .blocklist_type(r".*__sbuf")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .trust_clang_mangling(false)
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
