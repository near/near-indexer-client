# Contributing

Use a current stable Rust toolchain with rustfmt and Clippy. No minimum supported
Rust version is promised yet; the initial local checks used Rust 1.96.0. Keep
Cargo.lock committed so CI and package verification use the reviewed dependency
resolution.

Run the same checks as CI:

```sh
cargo fmt --all -- --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
cargo package --locked
```

Tests use deterministic local RPC responses and captured payload fixtures; no
live node or credentials are needed. When changing the wire format, retain the
explicit-null/absent-field regression cases and verify against the intended
nearcore build. The polling example can be run against a configured development
node for a separate live check.

Keep the client stateless and the API small. Consumers own persistence, retries,
and rollback effects. Describe changes to checkpoint or ancestry semantics in
the changelog, and add a regression test for behavior changes.

Use semantic branch prefixes (`feat/`, `fix/`, `docs/`, or `chore/`) and
Conventional Commit titles. Contributions are licensed under MIT OR Apache-2.0.
