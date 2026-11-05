use std::env;
use std::path::PathBuf;

fn main() {
    #[cfg(feature = "bundled")]
    let (clang_args, version) = build_bundled();
    #[cfg(not(feature = "bundled"))]
    let (clang_args, version) = probe_system();

    generate_bindings(clang_args);

    // Exposed to dependent build scripts as DEP_MDBSQL_VERSION
    println!("cargo:version={}", version);
}

#[cfg(not(feature = "bundled"))]
fn probe_system() -> (Vec<String>, String) {
    let library = pkg_config::probe_library("libmdbsql").unwrap_or_else(|e| panic!("{}", e));

    let include_args = library
        .include_paths
        .iter()
        .map(|path| format!("-I{}", path.to_string_lossy()));
    let link_args = library
        .link_paths
        .iter()
        .map(|path| format!("-L{}", path.to_string_lossy()));

    (include_args.chain(link_args).collect(), library.version)
}

#[cfg(feature = "bundled")]
fn build_bundled() -> (Vec<String>, String) {
    let glib = pkg_config::Config::new()
        .atleast_version("2.68")
        .probe("glib-2.0")
        .unwrap_or_else(|e| panic!("bundled libmdbsql requires GLib: {}", e));

    let vendor_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap()).join("mdbtools");
    let vendor_include = vendor_dir.join("include");
    let generated_include = PathBuf::from(env::var("OUT_DIR").unwrap()).join("include");
    std::fs::create_dir_all(&generated_include).unwrap();

    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    let is_bsd_libc = matches!(target_os.as_str(), "macos" | "ios" | "freebsd" | "dragonfly");

    // Substitutions that mdbtools' configure script performs on mdbtools.h.in
    let header_template = std::fs::read_to_string(vendor_include.join("mdbtools.h.in")).unwrap();
    let header = header_template
        .replace("@HAVE_ICONV_H@", "1")
        .replace("@HAVE_XLOCALE_H@", if is_bsd_libc { "1" } else { "0" })
        .replace("@GLIB_INCLUDE_HEADER@", "glib.h")
        .replace("@LOCALE_T@", "locale_t");
    std::fs::write(generated_include.join("mdbtools.h"), header).unwrap();

    let mut build = cc::Build::new();
    build
        .include(&generated_include)
        .include(&vendor_include)
        .includes(&glib.include_paths)
        .define("TLS", "_Thread_local")
        .define("HAVE_GLIB", "1")
        .define("HAVE_ICONV", "1")
        .define("ICONV_CONST", "")
        .define("HAVE_STRPTIME", "1")
        .define("HAVE_FMEMOPEN", "1")
        .define("HAVE_GMTIME_R", "1")
        .warnings(false);
    if is_bsd_libc {
        build.define("HAVE_REALLOCF", "1");
    }
    if target_os == "linux" {
        // Exposes strptime() in glibc headers
        build.define("_GNU_SOURCE", None);
    }

    let libmdb_sources = [
        "backend.c",
        "catalog.c",
        "data.c",
        "dump.c",
        "file.c",
        "iconv.c",
        "index.c",
        "like.c",
        "map.c",
        "money.c",
        "options.c",
        "props.c",
        "rc4.c",
        "sargs.c",
        "stats.c",
        "table.c",
        "version.c",
        "worktable.c",
        "write.c",
    ];
    let sql_sources = ["mdbsql.c", "parser.c", "lexer.c"];
    build
        .files(libmdb_sources.iter().map(|file| vendor_dir.join("src/libmdb").join(file)))
        .files(sql_sources.iter().map(|file| vendor_dir.join("src/sql").join(file)))
        .compile("mdbsql");

    if matches!(target_os.as_str(), "macos" | "ios") {
        println!("cargo:rustc-link-lib=iconv");
    }
    println!("cargo:rerun-if-changed={}", vendor_dir.display());

    let include_args = [&generated_include, &vendor_include]
        .into_iter()
        .chain(&glib.include_paths)
        .map(|path| format!("-I{}", path.to_string_lossy()))
        .collect();

    (include_args, read_vendored_version(&vendor_include))
}

#[cfg(feature = "bundled")]
fn read_vendored_version(vendor_include: &std::path::Path) -> String {
    let version_header = std::fs::read_to_string(vendor_include.join("mdbver.h")).unwrap();
    version_header
        .lines()
        .find_map(|line| line.strip_prefix("#define MDB_VERSION_NO "))
        .map(|version| version.trim().trim_matches('"').to_string())
        .expect("MDB_VERSION_NO not found in vendored mdbver.h")
}

fn generate_bindings(clang_args: Vec<String>) {
    let bindings = bindgen::Builder::default()
        .clang_args(clang_args)
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
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .trust_clang_mangling(false)
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
