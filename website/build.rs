fn main() {
    // Tell cargo to re-run this build script (and thus recompile) whenever
    // any migration file changes. sqlx::migrate!() and refinery::embed_migrations!
    // both embed SQL at compile time, but cargo doesn't automatically track
    // those files as dependencies.
    println!("cargo:rerun-if-changed=migrations/");
    println!("cargo:rerun-if-changed=clickhouse_migrations/");
}
