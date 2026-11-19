// Embed the short git SHA at compile time so `build_info` can expose it as a
// Prometheus label without any runtime overhead.
//
// Resolution order:
// 1. NOZZL_BUILD_GIT_SHA env var — set via Docker build-arg so Docker builds
//    have the SHA even though .git/ is not in the build context.
// 2. `git rev-parse --short HEAD` — works in local dev where .git/ is present.
// 3. "unknown" fallback — harmless; metric is still emitted, just unlabelled.
fn main() {
    // Tell cargo to re-run this script when the injected SHA changes (Docker
    // rebuilds with a new commit) or when HEAD advances locally.
    println!("cargo:rerun-if-env-changed=NOZZL_BUILD_GIT_SHA");
    println!("cargo:rerun-if-changed=../../.git/HEAD");

    let sha = std::env::var("NOZZL_BUILD_GIT_SHA")
        .ok()
        .filter(|s| !s.is_empty() && s != "unknown")
        .unwrap_or_else(|| {
            std::process::Command::new("git")
                .args(["rev-parse", "--short", "HEAD"])
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "unknown".to_string())
        });
    println!("cargo:rustc-env=NOZZL_GIT_SHA={sha}");
}
