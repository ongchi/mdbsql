use std::env;
use std::path::PathBuf;

fn main() {
    #[cfg(feature = "bundled")]
    let version = {
        let clang_args = build_bundled();
        write_bundled_bindings(clang_args);
        read_vendored_version()
    };
    #[cfg(not(feature = "bundled"))]
    let version = {
        let (clang_args, version) = probe_system();
        generate_bindings(clang_args);
        version
    };

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
fn build_bundled() -> Vec<String> {
    let vendor_dir = vendor_dir();
    let vendor_include = vendor_dir.join("include");
    let generated_include = PathBuf::from(env::var("OUT_DIR").unwrap()).join("include");
    std::fs::create_dir_all(&generated_include).unwrap();

    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    let is_bsd_libc = matches!(
        target_os.as_str(),
        "macos" | "ios" | "freebsd" | "dragonfly"
    );
    // MinGW has no iconv; mdbtools uses the Windows locale API instead
    let is_windows = target_os == "windows";

    // Substitutions that mdbtools' configure script performs on mdbtools.h.in
    let header_template = std::fs::read_to_string(vendor_include.join("mdbtools.h.in")).unwrap();
    let header = header_template
        .replace("@HAVE_ICONV_H@", if is_windows { "0" } else { "1" })
        .replace("@HAVE_XLOCALE_H@", if is_bsd_libc { "1" } else { "0" })
        .replace("@GLIB_INCLUDE_HEADER@", "mdbfakeglib.h")
        .replace(
            "@LOCALE_T@",
            if is_windows { "_locale_t" } else { "locale_t" },
        );
    std::fs::write(generated_include.join("mdbtools.h"), header).unwrap();

    let mut build = cc::Build::new();
    build
        .include(&generated_include)
        .include(&vendor_include)
        .define("TLS", "_Thread_local")
        .warnings(false);
    if !is_windows {
        build
            .define("HAVE_ICONV", "1")
            .define("ICONV_CONST", "")
            .define("HAVE_STRPTIME", "1")
            .define("HAVE_FMEMOPEN", "1")
            .define("HAVE_GMTIME_R", "1");
    }
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
        "fakeglib.c",
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
        .files(
            libmdb_sources
                .iter()
                .map(|file| vendor_dir.join("src/libmdb").join(file)),
        )
        .files(
            sql_sources
                .iter()
                .map(|file| vendor_dir.join("src/sql").join(file)),
        )
        .compile("mdbsql");

    if matches!(target_os.as_str(), "macos" | "ios") {
        println!("cargo:rustc-link-lib=iconv");
    }
    println!("cargo:rerun-if-changed={}", vendor_dir.display());

    [&generated_include, &vendor_include]
        .into_iter()
        .map(|path| format!("-I{}", path.to_string_lossy()))
        .collect()
}

#[cfg(feature = "bundled")]
fn vendor_dir() -> PathBuf {
    PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap()).join("mdbtools")
}

#[cfg(all(feature = "bundled", feature = "buildtime_bindgen"))]
fn write_bundled_bindings(clang_args: Vec<String>) {
    generate_bindings(clang_args);
}

#[cfg(all(feature = "bundled", not(feature = "buildtime_bindgen")))]
fn write_bundled_bindings(_clang_args: Vec<String>) {
    let pregenerated = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
        .join("src")
        .join(pregenerated_bindings_name());
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("bindings.rs");
    std::fs::copy(&pregenerated, out_path).expect("Couldn't copy pre-generated bindings!");
    println!("cargo:rerun-if-changed={}", pregenerated.display());
}

/// Pre-generated bindings are only valid for the target families they were generated for
#[cfg(all(feature = "bundled", not(feature = "buildtime_bindgen")))]
fn pregenerated_bindings_name() -> &'static str {
    let target_family = env::var("CARGO_CFG_TARGET_FAMILY").unwrap();
    let pointer_width = env::var("CARGO_CFG_TARGET_POINTER_WIDTH").unwrap();

    if target_family.split(',').any(|family| family == "unix") && pointer_width == "64" {
        return "bindgen_bundled_unix.rs";
    }
    if env::var("TARGET").unwrap() == "x86_64-pc-windows-gnu" {
        return "bindgen_bundled_windows_gnu.rs";
    }
    panic!(
        "No pre-generated libmdb bindings for target {}. Enable the `buildtime_bindgen` feature.",
        env::var("TARGET").unwrap()
    );
}

#[cfg(feature = "bundled")]
fn read_vendored_version() -> String {
    let version_header =
        std::fs::read_to_string(vendor_dir().join("include").join("mdbver.h")).unwrap();
    version_header
        .lines()
        .find_map(|line| line.strip_prefix("#define MDB_VERSION_NO "))
        .map(|version| version.trim().trim_matches('"').to_string())
        .expect("MDB_VERSION_NO not found in vendored mdbver.h")
}

#[cfg(any(not(feature = "bundled"), feature = "buildtime_bindgen"))]
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
        // Target-specific system types, defined in lib.rs so that
        // pre-generated bindings work across targets
        .blocklist_type(r"tm")
        .blocklist_type(r"off_t")
        .blocklist_type(r"fpos_t")
        .blocklist_type(r"iconv_t")
        .blocklist_type(r"__.*")
        .blocklist_type(r"_IO_.*")
        .blocklist_type(r"_iobuf")
        .blocklist_type(r"off(32|64)_t")
        .blocklist_item(r"MDBTOOLS_H_.*")
        // Same output whether or not rustfmt is installed
        .formatter(bindgen::Formatter::Prettyplease)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .trust_clang_mangling(false)
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
