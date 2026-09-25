//! What `get_optimal_memory_budget` reads off disk, and the size classes it maps that to.
//!
//! The budget it returns is the tantivy arena an index's writer is built with, and — through
//! `should_commit_writer` — how many operations accumulate before the next commit. It is
//! measured by walking the index directory, which is why `commit_index` no longer does it per
//! commit and a TTL governs it instead (M0-g): these tests pin the answer, not its cadence.
//!
//! This file used to also test `StorageConfig::get_bulk_operation_budget`, which documented
//! bulk writes receiving 1.5× and 2× arenas. They never did: a writer's
//! `memory_budget_per_thread` is fixed when the writer is built, and nothing in the bulk path
//! ever called the function. Being `pub` on a library type, no dead-code lint saw it, which is
//! how L17's sweep missed it. It has been deleted rather than wired up, because making the
//! documented behaviour real means rebuilding a writer per batch, and inflating writer arenas
//! is the opposite direction from M1 — which is about bounding them (M0-h).

use std::path::Path;
use storage::StorageConfig;
use tempfile::TempDir;

/// Create a sparse file of the given logical size.
///
/// Budget sizing reads `metadata.len()`, which reports the logical length, so there is no
/// reason to actually write hundreds of megabytes of zeros to disk for these assertions.
fn create_sized_file(path: &Path, len_bytes: u64) {
    let file = std::fs::File::create(path).expect("Failed to create file");
    file.set_len(len_bytes).expect("Failed to size file");
}

#[test]
fn test_optimal_memory_budget_by_index_size() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    let config = StorageConfig {
        max_open_indexes: 0,
        shard_path: temp_dir.path().to_path_buf(),

        // Memory Budget Configuration
        indexer_memory_budget: 64 * 1024 * 1024,
        indexer_memory_min_mb: 32,
        indexer_memory_max_mb: 512,
        total_memory_limit_bytes: 4 * 1024 * 1024 * 1024,
        memory_pressure_threshold_percent: 80,

        // Thread Configuration
        indexer_num_threads: 1,
        merge_num_threads: 2,

        // Other Configuration
        default_batch_size: 1000,
        wal_sync: true,
        commit_interval_ms: 0,
        query: Default::default(),
    };

    // Test with non-existent index (should return min budget)
    let non_existent = temp_dir.path().join("non_existent");
    let budget_new = config.get_optimal_memory_budget(&non_existent, None);
    let min_budget = config.indexer_memory_min_mb * 1024 * 1024;
    assert_eq!(
        budget_new, min_budget,
        "New index should use minimum budget"
    );

    // Test with small index (create a small file)
    let small_index = temp_dir.path().join("small_index");
    create_sized_file(&small_index, 50 * 1024 * 1024);
    let budget_small = config.get_optimal_memory_budget(&small_index, None);
    assert_eq!(
        budget_small, min_budget,
        "Small index (<100MB) should use minimum budget"
    );

    // Test with medium index
    let medium_index = temp_dir.path().join("medium_index");
    create_sized_file(&medium_index, 300 * 1024 * 1024);
    let budget_medium = config.get_optimal_memory_budget(&medium_index, None);
    let default_budget = config.indexer_memory_budget;
    assert_eq!(
        budget_medium, default_budget,
        "Medium index (101-500MB) should use default budget"
    );

    println!("✅ Optimal memory budget by index size works correctly!");
    println!("   New index:    {}MB", budget_new / (1024 * 1024));
    println!("   Small (50MB): {}MB", budget_small / (1024 * 1024));
    println!("   Medium (300MB): {}MB", budget_medium / (1024 * 1024));
}

/// Budgets must be derived from the *contents* of an index directory.
///
/// Production always passes `<shard>/indices/<name>`, a directory. Sizing it with
/// `metadata(dir).len()` reports the directory entry itself (a couple of KB), which pinned
/// every index to the minimum budget and flattened the whole size ladder to a no-op.
#[test]
fn test_optimal_memory_budget_sums_directory_contents() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    let config = StorageConfig {
        max_open_indexes: 0,
        shard_path: temp_dir.path().to_path_buf(),
        indexer_memory_budget: 64 * 1024 * 1024,
        indexer_memory_min_mb: 32,
        indexer_memory_max_mb: 512,
        total_memory_limit_bytes: 4 * 1024 * 1024 * 1024,
        memory_pressure_threshold_percent: 80,
        indexer_num_threads: 1,
        merge_num_threads: 2,
        default_batch_size: 1000,
        wal_sync: true,
        commit_interval_ms: 0,
        query: Default::default(),
    };

    let min_budget = config.indexer_memory_min_mb * 1024 * 1024;

    // An empty index directory is a new index: minimum budget.
    let empty_index = temp_dir.path().join("empty_index");
    std::fs::create_dir_all(&empty_index).expect("Failed to create empty index directory");
    assert_eq!(
        config.get_optimal_memory_budget(&empty_index, None),
        min_budget,
        "Empty index directory should use minimum budget"
    );

    // 300MB spread across several segment files, the way Tantivy actually lays out an
    // index, must land in the 101-500MB tier rather than being read as a ~KB directory.
    let medium_index = temp_dir.path().join("medium_index");
    std::fs::create_dir_all(&medium_index).expect("Failed to create medium index directory");
    for segment in 0..3 {
        create_sized_file(
            &medium_index.join(format!("segment_{segment}.store")),
            100 * 1024 * 1024,
        );
    }
    assert_eq!(
        config.get_optimal_memory_budget(&medium_index, None),
        config.indexer_memory_budget,
        "300MB of segment files should use the default budget tier"
    );

    // Field-count scaling applies on top of the size tier.
    assert_eq!(
        config.get_optimal_memory_budget(&medium_index, Some(120)),
        (config.indexer_memory_budget as f64 * 1.5) as usize,
        "Schemas with >100 indexed fields should scale the size-based budget by 1.5x"
    );
}
