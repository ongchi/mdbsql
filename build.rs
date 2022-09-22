use std::env;
use std::path::PathBuf;

fn main() {
    if env::var("DOCS_RS").is_err() {
        println!("cargo:rustc-link-lib=dylib=mdb");
        println!("cargo:rustc-link-lib=dylib=mdbsql");
    }

    let library = pkg_config::probe_library("glib-2.0").unwrap_or_else(|e| panic!("{}", e));

    let bindings = bindgen::Builder::default()
        .clang_args(
            library
                .include_paths
                .iter()
                .map(|path| format!("-I{}", path.to_string_lossy())),
        )
        .header("src/wrapper.h")
        .allowlist_file(r".*mdbtools\.h")
        .allowlist_file(r".*mdbsql\.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .rustfmt_bindings(true)
        .trust_clang_mangling(false)
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
