use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let Some(cmd) = args.next() else {
        print_usage();
        return Err("missing command".to_string());
    };

    match cmd.as_str() {
        "precommit" => {
            let mut full = false;
            for arg in args {
                match arg.as_str() {
                    "--full" => full = true,
                    "--ci" => {}
                    _ => return Err(format!("unknown precommit flag: {arg}")),
                }
            }
            precommit(full)
        }
        "install-hooks" => install_hooks(),
        "help" | "--help" | "-h" => {
            print_usage();
            Ok(())
        }
        _ => {
            print_usage();
            Err(format!("unknown command: {cmd}"))
        }
    }
}

fn print_usage() {
    eprintln!("xtask commands:");
    eprintln!("  precommit [--full] [--ci]");
    eprintln!("  install-hooks");
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask must live inside workspace")
        .to_path_buf()
}

fn precommit(full: bool) -> Result<(), String> {
    let root = repo_root();
    let mut steps: Vec<Vec<&str>> = vec![
        vec!["cargo", "fmt", "--all", "--", "--check"],
        vec![
            "cargo",
            "clippy",
            "-p",
            "geolite-core",
            "-p",
            "geolite-sqlite",
            "-p",
            "geolite-diesel",
            "--features",
            "sqlite",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
        vec!["cargo", "test", "--workspace"],
        vec![
            "cargo",
            "test",
            "-p",
            "geolite-diesel",
            "--features",
            "sqlite",
        ],
    ];

    if full {
        steps.extend([
            vec![
                "cargo",
                "test",
                "-p",
                "geolite-diesel",
                "--features",
                "postgres",
                "--test",
                "postgres_integration",
            ],
            vec![
                "cargo",
                "test",
                "-p",
                "geolite-sqlite",
                "--target",
                "wasm32-unknown-unknown",
                "--test",
                "wasm",
            ],
            vec![
                "cargo",
                "test",
                "-p",
                "geolite-diesel",
                "--features",
                "sqlite",
                "--target",
                "wasm32-unknown-unknown",
                "--test",
                "wasm_integration",
            ],
        ]);
    }

    for step in steps {
        run_step(&root, &step)?;
    }
    Ok(())
}

fn run_step(cwd: &Path, args: &[&str]) -> Result<(), String> {
    let (bin, rest) = args
        .split_first()
        .ok_or_else(|| "empty command step".to_string())?;
    eprintln!("+ {}", args.join(" "));

    let status = Command::new(bin)
        .args(rest)
        .current_dir(cwd)
        .status()
        .map_err(io_err)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("command failed: {}", args.join(" ")))
    }
}

fn install_hooks() -> Result<(), String> {
    let root = repo_root();
    let hook_path = root.join(".git/hooks/pre-commit");
    let script = format!(
        "#!/usr/bin/env sh\nset -eu\ncd \"{}\"\ncargo run --quiet -p xtask -- precommit\n",
        root.display()
    );

    if let Some(parent) = hook_path.parent() {
        fs::create_dir_all(parent).map_err(io_err)?;
    }
    fs::write(&hook_path, script).map_err(io_err)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&hook_path).map_err(io_err)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&hook_path, perms).map_err(io_err)?;
    }

    println!("installed pre-commit hook at {}", hook_path.display());
    Ok(())
}

fn io_err(e: io::Error) -> String {
    e.to_string()
}
