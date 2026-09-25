//! Unit tests for the configuration model and its override layer.

use super::*;

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use super::*;

    fn cli(args: &[&str]) -> CliOverrides {
        CliOverrides::parse(args.iter().map(|arg| arg.to_string())).expect("parse")
    }

    #[test]
    fn cli_parses_both_value_syntaxes() {
        let parsed = cli(&["--http-port", "1234", "--node-label=alpha"]);
        assert_eq!(parsed.value_for("--http-port"), Some("1234"));
        assert_eq!(parsed.value_for("--node-label"), Some("alpha"));
    }

    #[test]
    fn cli_parses_config_path_including_short_form() {
        assert_eq!(
            cli(&["--config", "a.toml"]).config_path.as_deref(),
            Some("a.toml")
        );
        assert_eq!(
            cli(&["-c", "b.toml"]).config_path.as_deref(),
            Some("b.toml")
        );
        assert_eq!(
            cli(&["--config=c.yaml"]).config_path.as_deref(),
            Some("c.yaml")
        );
    }

    /// A bare switch must not eat the argument after it — `--cluster-enabled --http-port 1`
    /// once would have consumed `--http-port` as the switch's value.
    #[test]
    fn cli_switch_defaults_to_true_without_consuming_the_next_argument() {
        let parsed = cli(&["--cluster-enabled", "--http-port", "1234"]);
        assert_eq!(parsed.value_for("--cluster-enabled"), Some("true"));
        assert_eq!(parsed.value_for("--http-port"), Some("1234"));

        let explicit = cli(&["--cluster-enabled=false"]);
        assert_eq!(explicit.value_for("--cluster-enabled"), Some("false"));
    }

    #[test]
    fn cli_last_occurrence_of_a_flag_wins() {
        let parsed = cli(&["--http-port", "1", "--http-port", "2"]);
        assert_eq!(parsed.value_for("--http-port"), Some("2"));
    }

    /// The bug this whole path exists to prevent: an unrecognized option used to be ignored,
    /// so the server booted on a configuration nobody asked for.
    #[test]
    fn cli_rejects_unknown_options_and_missing_values() {
        let unknown = CliOverrides::parse(["--nope".to_string()]).unwrap_err();
        assert!(unknown.to_string().contains("Unknown option: --nope"));

        let missing = CliOverrides::parse(["--http-port".to_string()]).unwrap_err();
        assert!(missing.to_string().contains("--http-port requires a value"));

        let no_path = CliOverrides::parse(["--config".to_string()]).unwrap_err();
        assert!(no_path.to_string().contains("--config requires a path"));
    }

    #[test]
    fn every_override_is_reachable_from_both_layers() {
        for entry in OVERRIDES {
            assert!(
                entry.flag.starts_with("--"),
                "{} must be a long flag",
                entry.flag
            );
            assert!(
                entry.env.starts_with("CAMEODB_"),
                "{} must be a CAMEODB_ variable",
                entry.env
            );
            assert_eq!(
                entry.placeholder.is_empty(),
                entry.kind == FlagKind::Switch,
                "{} must have a placeholder iff it takes a value",
                entry.flag
            );
            assert!(
                cli_help().contains(entry.flag),
                "{} is missing from --help",
                entry.flag
            );
        }
    }

    /// Precedence, on one setting, through all four layers at once.
    #[test]
    fn command_line_beats_environment_beats_file_beats_default() {
        let dir = std::env::temp_dir().join(format!("cameodb-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let file = dir.join("cameodb.toml");
        // Config files are deserialized strictly, so write a complete one.
        let mut on_disk = CameoDbConfig::default();
        on_disk.network.http.port = 7001;
        std::fs::write(&file, toml::to_string_pretty(&on_disk).expect("serialize"))
            .expect("write config");
        let path = file.to_str().expect("utf-8 path").to_string();

        // Defaults only.
        let config = CameoDbConfig::load_with_cli(&CliOverrides::default()).expect("defaults");
        assert_eq!(config.network.http.port, 9480);

        // File over defaults.
        let from_file =
            CameoDbConfig::load_with_cli(&cli(&["--config", &path])).expect("file config");
        assert_eq!(from_file.network.http.port, 7001);

        unsafe { std::env::set_var("CAMEODB_HTTP_PORT", "7002") };

        // Environment over file.
        let from_env =
            CameoDbConfig::load_with_cli(&cli(&["--config", &path])).expect("env override");
        assert_eq!(from_env.network.http.port, 7002);

        // Command line over environment.
        let from_cli =
            CameoDbConfig::load_with_cli(&cli(&["--config", &path, "--http-port", "7003"]))
                .expect("cli override");
        assert_eq!(from_cli.network.http.port, 7003);

        unsafe { std::env::remove_var("CAMEODB_HTTP_PORT") };
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A setting reaching the config struct is not the same as it reaching the code that
    /// acts on it. The supervisor read `CAMEODB_SUPERVISOR_TIMEOUT_SECS` from the
    /// environment directly, so this field was populated and then ignored — the file and the
    /// `--supervisor-timeout-secs` flag did nothing, and only the env var appeared to work.
    #[test]
    fn the_supervisor_timeout_is_configurable_from_the_file() {
        let config: CameoDbConfig = toml::from_str(
            "[search]\n\
             supervisor_timeout_secs = 30\n",
        )
        .expect("partial config");

        assert_eq!(config.search.supervisor_timeout_secs, 30);
        assert_eq!(
            CameoDbConfig::default().search.supervisor_timeout_secs,
            5,
            "the documented default and the code's default have to agree"
        );
    }

    /// The pre-`[limits]` file every existing deployment still has on disk. It must resolve
    /// to exactly what the grouped spelling resolves to — an upgrade that quietly reverts a
    /// 420MB record ceiling to 64 is a node that starts and then refuses the writes it was
    /// configured for.
    #[test]
    fn the_pre_limits_spellings_are_adopted_into_limits() {
        let old = CameoDbConfig::parse_config_content(
            r#"
max_record_size_mb = 420

[network.http]
max_body_size_mb = 512

[search]
total_memory_limit_mb = 96000

[security.limits]
max_response_bytes = 16777216
"#,
            "old.toml",
        )
        .expect("the old shape still parses");

        assert_eq!(old.limits.max_record_size_mb, 420);
        assert_eq!(old.limits.max_body_size_mb, 512);
        assert_eq!(old.limits.total_memory_limit_mb, 96000);
        assert_eq!(old.limits.max_response_bytes, Some(16777216));

        // Consumed, so nothing downstream reads them and no dump writes them back out.
        assert_eq!(old.max_record_size_mb, None);
        assert_eq!(old.network.http.max_body_size_mb, None);
        assert_eq!(old.search.total_memory_limit_mb, None);
        assert_eq!(old.security.limits.max_response_bytes, None);
    }

    #[test]
    fn limits_wins_where_both_spellings_are_present() {
        let config = CameoDbConfig::parse_config_content(
            r#"
max_record_size_mb = 420

[limits]
max_record_size_mb = 128
"#,
            "both.toml",
        )
        .expect("parse");

        assert_eq!(config.limits.max_record_size_mb, 128);
    }

    /// `[security.limits]` refuses unknown keys, so the moved setting has to be a field there
    /// rather than something the unknown-key sweep can warn about: without it, an operator
    /// upgrading with the old spelling gets a node that will not start.
    #[test]
    fn the_moved_mcp_response_ceiling_does_not_stop_the_node() {
        let config = CameoDbConfig::parse_config_content(
            "[security.limits]\nmax_search_limit = 10000\nmax_response_bytes = 900\n",
            "old.toml",
        )
        .expect("the old shape still parses");

        assert_eq!(config.limits.max_response_bytes, Some(900));
        assert_eq!(config.security.limits.max_search_limit, 10000);
    }

    /// A moved setting is a known key, not a typo — the sweep must stay quiet about it, or
    /// every upgraded deployment reads "Ignoring" next to a value that was in fact applied.
    #[test]
    fn the_moved_spellings_are_not_reported_as_unknown() {
        let content = r#"
max_record_size_mb = 420

[network.http]
max_body_size_mb = 512

[search]
total_memory_limit_mb = 96000

[security.limits]
max_response_bytes = 16777216
"#;
        assert_eq!(unrecognized_keys(content), Vec::<String>::new());
    }

    /// Every config file this repository ships, relative to `crates/server`.
    const SHIPPED_CONFIGS: &[&str] = &[
        "cameodb.toml",
        "../../cameodb.example.toml",
        "../../docker/cameodb-docker.toml",
    ];

    fn shipped(path: &str) -> String {
        let full = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(path);
        std::fs::read_to_string(&full).unwrap_or_else(|e| panic!("{}: {e}", full.display()))
    }

    /// A shipped config that does not parse, or that names a setting no field claims, is a
    /// typo nobody notices until a deployment silently runs on defaults.
    #[test]
    fn every_shipped_config_parses_with_no_unrecognized_keys() {
        for path in SHIPPED_CONFIGS {
            let content = shipped(path);
            toml::from_str::<CameoDbConfig>(&content)
                .unwrap_or_else(|e| panic!("{path} does not parse: {e}"));
            assert_eq!(
                unrecognized_keys(&content),
                Vec::<String>::new(),
                "{path} names settings no field claims"
            );
        }
    }

    /// The affinity flags went missing from every shipped config once already, which is how
    /// they stayed unmeasured for a whole phase. A flag that is off by default and absent
    /// from the file is a flag nobody knows exists.
    #[test]
    fn every_shipped_config_states_the_affinity_flags() {
        for path in SHIPPED_CONFIGS {
            let content = shipped(path);
            for flag in [
                "writer_core_affinity",
                "shard_affine_dispatch",
                "worker_core_affinity",
            ] {
                assert!(
                    content
                        .lines()
                        .any(|line| line.trim_start().starts_with(flag)),
                    "{path} never sets {flag}"
                );
            }
        }
    }

    /// Both measured as regressions on 2026-08-09 and again on 2026-08-10 after workers
    /// gained per-operation concurrency — the change that was supposed to redeem them. See
    /// ROADMAP "Worker concurrency, measured".
    #[test]
    fn the_measured_affinity_flags_default_off() {
        let storage = CameoDbConfig::default().storage;
        assert!(!storage.shard_affine_dispatch);
        assert!(!storage.worker_core_affinity);
        assert!(
            storage.writer_core_affinity,
            "writer pinning measured neutral and is what the others align to"
        );
    }

    /// The long-standing contract: name only what you are changing.
    #[test]
    fn partial_config_files_keep_defaults_for_everything_omitted() {
        let config: CameoDbConfig = toml::from_str(
            "[network.http]\n\
             port = 7001\n",
        )
        .expect("a partial config must parse");

        assert_eq!(config.network.http.port, 7001, "the named setting applies");
        // Neighbours in the same table, sibling sections, and whole missing sections.
        assert_eq!(config.network.http.bind_address, "127.0.0.1");
        assert_eq!(config.network.cluster.cluster_port, 9580);
        assert_eq!(
            config.storage.data_paths,
            vec![PathBuf::from("./data/cameodb")]
        );
        assert_eq!(config.search.indexer_memory_min_mb, 64);
        assert_eq!(config.limits.max_record_size_mb, 64);
        assert!(config.validate().is_ok());
    }

    /// An empty file is the degenerate partial config and must behave like no file at all.
    #[test]
    fn empty_config_file_is_all_defaults() {
        let config: CameoDbConfig = toml::from_str("").expect("empty config must parse");
        let defaults = CameoDbConfig::default();
        assert_eq!(config.network.http.port, defaults.network.http.port);
        assert_eq!(config.storage.data_paths, defaults.storage.data_paths);
    }

    /// Partial files make a typo indistinguishable from an omission, so typos get reported.
    #[test]
    fn unknown_keys_are_reported_and_known_ones_are_not() {
        let unknown = unrecognized_keys(
            "[network.http]\n\
             port = 7001\n\
             prot = 7002\n\n\
             [storrage]\n\
             data_paths = [\"/tmp/x\"]\n",
        );
        assert_eq!(unknown, vec!["network.http.prot", "storrage"]);

        let sample = CameoDbConfig::generate_sample_config().expect("sample");
        assert!(
            unrecognized_keys(&sample).is_empty(),
            "the generated sample must not report against its own schema"
        );
    }

    /// A named config file that cannot be read must fail the boot, not fall back to defaults.
    #[test]
    fn explicitly_named_config_file_must_exist() {
        let err = CameoDbConfig::load_with_cli(&cli(&["--config", "/nonexistent/cameodb.toml"]))
            .unwrap_err();
        assert!(err.to_string().contains("Failed to read config file"));
    }

    #[test]
    fn test_default_configuration() {
        let config = CameoDbConfig::default();
        assert_eq!(config.network.http.port, 9480);
        // Loopback by default: a fresh node is not reachable off-box until asked.
        assert_eq!(config.network.http.bind_address, "127.0.0.1");
        assert_eq!(config.search.indexer_memory_min_mb, 64);
        assert_eq!(config.search.indexer_memory_max_mb, 512);
        assert_eq!(config.storage.default_batch_size, 1000);
        assert_eq!(config.limits.max_record_size_mb, 64);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_memory_validation() {
        let mut config = CameoDbConfig::default();

        // Test invalid memory range
        config.search.indexer_memory_min_mb = 600;
        config.search.indexer_memory_max_mb = 512;
        assert!(config.validate().is_err());

        // Test memory too small (below new floor of 16)
        config.search.indexer_memory_min_mb = 8;
        config.search.indexer_memory_max_mb = 512;
        assert!(config.validate().is_err());

        // Test memory at new floor (should be valid)
        config.search.indexer_memory_min_mb = 16;
        config.search.indexer_memory_max_mb = 512;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_storage_validation() {
        let mut config = CameoDbConfig::default();

        // Test empty data paths
        config.storage.data_paths.clear();
        assert!(config.validate().is_err());

        // Test invalid disk threshold
        config.storage.data_paths = vec![PathBuf::from("./data")];
        config.storage.disk_usage_threshold_percent = 150;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_sample_config_generation() {
        let sample = CameoDbConfig::generate_sample_config().unwrap();
        assert!(sample.contains("port = 9480"));
        assert!(sample.contains("indexer_memory_min_mb = 64"));
        assert!(sample.contains("default_batch_size = 1000"));
        assert!(sample.contains("max_record_size_mb = 64"));
        assert!(sample.contains("data_paths"));
    }

    #[test]
    fn test_default_batch_size_setting() {
        // Set a specific batch size via environment variable
        unsafe {
            std::env::set_var("CAMEODB_STORAGE_DEFAULT_BATCH_SIZE", "750");
        }

        // Load config and verify the value
        let config = CameoDbConfig::load_with_cli(&CliOverrides::default()).unwrap();
        assert_eq!(config.storage.default_batch_size, 750);

        // Clean up
        unsafe {
            std::env::remove_var("CAMEODB_STORAGE_DEFAULT_BATCH_SIZE");
        }

        println!("✅ Batch size configuration works!");
    }

    /// The search ceiling and the default that fills in for an absent limit must not
    /// contradict each other.
    ///
    /// A search naming no limit is filled in with the default *after* the ceiling has been
    /// checked, so a default above the ceiling runs past a bound the MCP tools advertise —
    /// and nothing in the response says so. Refused at startup rather than clamped, because
    /// clamping would mean the number an operator wrote is not the number that runs.
    #[test]
    fn a_default_search_limit_above_the_ceiling_is_refused() {
        let mut config = CameoDbConfig::default();
        config.security.limits.max_search_limit = 500;

        config.search.default_search_limit = 500;
        assert!(
            config.validate().is_ok(),
            "a default equal to the ceiling is within it"
        );

        config.search.default_search_limit = 501;
        let err = config
            .validate()
            .expect_err("a default above the ceiling was accepted")
            .to_string();
        assert!(
            err.contains("501") && err.contains("500"),
            "the refusal does not name both numbers: {err}"
        );
    }

    /// A response ceiling of zero would describe every response as truncated rather than
    /// refusing any, since one hit always survives the trim.
    #[test]
    fn a_response_ceiling_of_zero_is_refused() {
        let config = CameoDbConfig {
            limits: LimitsConfig {
                max_response_bytes: Some(0),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(
            config.validate().is_err(),
            "a response ceiling of zero was accepted"
        );
    }

    /// Zero is a misconfiguration rather than "no ceiling".
    ///
    /// Read as unlimited it would invert the meaning of the number, and read literally it
    /// refuses every search. An operator who wants a high ceiling writes a high number.
    #[test]
    fn a_ceiling_of_zero_is_refused() {
        let config = CameoDbConfig {
            security: crate::auth::SecurityConfig {
                limits: crate::ratelimit::McpLimitsConfig {
                    max_search_limit: 0,
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(config.validate().is_err(), "a ceiling of zero was accepted");
    }

    /// A fan-out bound of zero is a misconfiguration for the same reason a ceiling of zero is.
    #[test]
    fn a_fan_out_bound_of_zero_is_refused() {
        let config = CameoDbConfig {
            security: crate::auth::SecurityConfig {
                limits: crate::ratelimit::McpLimitsConfig {
                    max_federated_indexes: 0,
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(
            config.validate().is_err(),
            "a fan-out bound of zero was accepted"
        );
    }

    /// Implicit creation is on unless an operator turns it off — an upgrade must not start
    /// refusing writes it used to serve.
    ///
    /// Both spellings of "unset" are pinned: the field absent from the file, and the
    /// programmatic default, because the two took different code paths and a default that
    /// quietly flips behaviour is exactly the trap the manual `Default` impl exists against.
    #[test]
    fn implicit_index_creation_defaults_on_and_reads_false() {
        let config = CameoDbConfig::parse_config_content(
            "[security]\nimplicit_index_creation = false\n",
            "gated.toml",
        )
        .expect("parse");
        assert!(!config.security.implicit_index_creation);

        let config = CameoDbConfig::parse_config_content("", "empty.toml").expect("parse");
        assert!(
            config.security.implicit_index_creation,
            "an absent key must not close the gate"
        );
        assert!(
            crate::auth::SecurityConfig::default().implicit_index_creation,
            "the programmatic default must match the file default"
        );
    }

    /// `[mcp]` is optional, and the defaults are the ones documented on the section.
    ///
    /// The numbers are asserted rather than compared to the `default_*` functions, because
    /// those would agree with themselves whatever they returned. Two in particular are worth
    /// pinning: the idle timeout is the thing an operator reaches for after a paused agent was
    /// handed a 404, and both transports are on by default so an upgrade strands nobody.
    #[test]
    fn the_mcp_section_defaults_are_the_documented_ones() {
        let config = CameoDbConfig::default();
        assert_eq!(config.mcp, McpConfig::default());
        assert!(config.mcp.enabled);
        assert_eq!(config.mcp.session_idle_timeout_secs, 1800);
        assert_eq!(config.mcp.max_sessions, 1024);
        assert_eq!(config.mcp.sse_keepalive_secs, 15);
        assert!(config.mcp.legacy_sse_enabled);
        assert!(config.validate().is_ok());

        // And a file naming one setting gets the defaults for the rest, rather than zeros.
        let partial: CameoDbConfig =
            toml::from_str("[mcp]\nlegacy_sse_enabled = false\n").expect("parse [mcp]");
        assert!(!partial.mcp.legacy_sse_enabled);
        assert_eq!(partial.mcp.session_idle_timeout_secs, 1800);
        assert!(partial.mcp.enabled);
    }

    /// The seconds an operator wrote are the durations the transport runs with.
    ///
    /// The conversion is the only place the two representations meet, and a transposition here
    /// would be invisible: sessions would expire on the keep-alive interval and streams would
    /// be written to every half hour, both of which look like the transport merely misbehaving.
    #[test]
    fn the_mcp_section_converts_to_the_transport_it_configures() {
        let config = McpConfig {
            session_idle_timeout_secs: 900,
            max_sessions: 64,
            sse_keepalive_secs: 5,
            legacy_sse_enabled: false,
            max_in_flight_per_session: 12,
            enabled: true,
        };
        let transport = config.transport();
        assert_eq!(
            transport.session_idle_timeout,
            std::time::Duration::from_secs(900)
        );
        assert_eq!(transport.max_sessions, 64);
        assert_eq!(transport.sse_keepalive, std::time::Duration::from_secs(5));
        assert!(!transport.legacy_sse_enabled);
        assert_eq!(transport.max_in_flight_per_session, 12);
    }

    /// Each of these numbers means something else at zero, and none of the meanings is "off".
    #[test]
    fn a_zero_in_the_mcp_section_is_refused() {
        for (label, mcp) in [
            (
                "session_idle_timeout_secs",
                McpConfig {
                    session_idle_timeout_secs: 0,
                    ..Default::default()
                },
            ),
            (
                "max_sessions",
                McpConfig {
                    max_sessions: 0,
                    ..Default::default()
                },
            ),
            (
                "sse_keepalive_secs",
                McpConfig {
                    sse_keepalive_secs: 0,
                    ..Default::default()
                },
            ),
            (
                "max_in_flight_per_session",
                McpConfig {
                    max_in_flight_per_session: 0,
                    ..Default::default()
                },
            ),
        ] {
            let config = CameoDbConfig {
                mcp,
                ..Default::default()
            };
            let err = config
                .validate()
                .expect_err(&format!("{label} of zero was accepted"))
                .to_string();
            assert!(
                err.contains(label),
                "the refusal does not name the setting: {err}"
            );
        }
    }

    /// A keep-alive that fires no more often than a session expires cannot hold one open.
    ///
    /// The two settings look independent and are not: the whole point of registering an open
    /// stream as proof of life is that it is written to inside the window that would otherwise
    /// sweep the session.
    #[test]
    fn a_keepalive_slower_than_the_session_timeout_is_refused() {
        let config = CameoDbConfig {
            mcp: McpConfig {
                session_idle_timeout_secs: 60,
                sse_keepalive_secs: 60,
                ..Default::default()
            },
            ..Default::default()
        };
        let err = config
            .validate()
            .expect_err("a keep-alive at the timeout was accepted")
            .to_string();
        assert!(
            err.contains("sse_keepalive_secs") && err.contains("session_idle_timeout_secs"),
            "the refusal does not name both settings: {err}"
        );

        let workable = CameoDbConfig {
            mcp: McpConfig {
                session_idle_timeout_secs: 60,
                sse_keepalive_secs: 15,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(workable.validate().is_ok());
    }

    /// The response ceiling follows the node's message size unless an operator names one.
    ///
    /// A deployment that raises `max_record_size_mb` for large documents must not then find its
    /// searches over those documents trimmed by a bound nobody moved — which is what a fixed
    /// default would do.
    #[test]
    fn the_response_ceiling_follows_the_message_size() {
        let config = CameoDbConfig::default();
        assert_eq!(
            config.effective_max_response_bytes(),
            config.effective_max_body_size_mb() * 1024 * 1024
        );

        let larger = CameoDbConfig {
            limits: LimitsConfig {
                max_record_size_mb: 512,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(
            larger.effective_max_response_bytes() > config.effective_max_response_bytes(),
            "raising the record size should raise the response ceiling with it"
        );
        assert_eq!(
            larger.effective_max_response_bytes(),
            (512 + 64) * 1024 * 1024
        );

        // And an explicit setting wins, for callers whose context is smaller than the node's
        // message size.
        let capped = CameoDbConfig {
            limits: LimitsConfig {
                max_response_bytes: Some(64 * 1024),
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(capped.effective_max_response_bytes(), 64 * 1024);
    }

    /// An absent `[security.limits]` is a bounded deployment, not an unbounded one.
    #[test]
    fn the_ceiling_defaults_to_a_number_rather_than_to_nothing() {
        let config: CameoDbConfig = toml::from_str("").expect("empty config must parse");
        assert_eq!(
            config.security.limits.max_search_limit,
            cameodb_mcp::DEFAULT_MAX_SEARCH_LIMIT
        );
        assert!(config.validate().is_ok());
        // And a config built in code agrees with a config parsed from nothing.
        assert_eq!(
            CameoDbConfig::default().security.limits.max_search_limit,
            config.security.limits.max_search_limit
        );
    }

    /// The prefix floor defaults to two, whether the section is absent or built in code, and an
    /// operator's `0` is read as "expand every prefix" rather than refused.
    #[test]
    fn the_prefix_floor_defaults_to_two_and_zero_turns_it_off() {
        let config: CameoDbConfig = toml::from_str("").expect("empty config must parse");
        assert_eq!(
            config.security.limits.min_prefix_length,
            crate::ratelimit::DEFAULT_MIN_PREFIX_LENGTH
        );
        assert_eq!(crate::ratelimit::DEFAULT_MIN_PREFIX_LENGTH, 2);
        assert_eq!(
            CameoDbConfig::default().security.limits.min_prefix_length,
            config.security.limits.min_prefix_length
        );

        let off = CameoDbConfig::parse_config_content(
            "[security.limits]\nmin_prefix_length = 0\n",
            "off.toml",
        )
        .expect("zero is a valid floor");
        assert_eq!(off.security.limits.min_prefix_length, 0);
        assert!(off.validate().is_ok());
    }

    /// Expanding a bare `pre*` across the default fields is opt-in, and reaches storage's policy.
    #[test]
    fn unqualified_prefix_expansion_is_off_until_enabled() {
        let config: CameoDbConfig = toml::from_str("").expect("empty config must parse");
        assert!(!config.security.limits.expand_unqualified_prefix);
        assert_eq!(
            config.security.limits.query_policy(),
            storage::QueryPolicy {
                min_prefix_length: 2,
                expand_unqualified_prefix: false,
            }
        );

        let on = CameoDbConfig::parse_config_content(
            "[security.limits]\nexpand_unqualified_prefix = true\n",
            "on.toml",
        )
        .expect("parses");
        assert!(on.security.limits.query_policy().expand_unqualified_prefix);
    }

    #[test]
    fn test_derived_limits_defaults() {
        let config = CameoDbConfig::default();
        // HTTP body: max_record_size_mb + 64 = 64 + 64 = 128
        assert_eq!(config.effective_max_body_size_mb(), 128);
        // Remote message: 64 MB + 25% overhead = 80 MB in bytes
        assert_eq!(
            config.effective_remote_message_size_bytes(),
            64 * 1024 * 1024 + 64 * 1024 * 1024 / 4
        );
        // Timeout: max(60, 64/10) = max(60, 6) = 60
        assert_eq!(config.effective_request_timeout_secs(), 60);
    }

    #[test]
    fn test_derived_limits_large_record() {
        let config = CameoDbConfig {
            limits: LimitsConfig {
                max_record_size_mb: 2048,
                ..Default::default()
            },
            ..Default::default()
        };
        // HTTP body: 2048 + 64 = 2112
        assert_eq!(config.effective_max_body_size_mb(), 2112);
        // Remote message: 2048 MB + 25% overhead = 2560 MB in bytes
        assert_eq!(
            config.effective_remote_message_size_bytes(),
            2048 * 1024 * 1024 + 2048 * 1024 * 1024 / 4
        );
        // Timeout: max(60, 2048/10) = max(60, 204) = 204
        assert_eq!(config.effective_request_timeout_secs(), 204);
    }

    #[test]
    fn test_explicit_body_size_override() {
        let mut config = CameoDbConfig::default();
        config.limits.max_body_size_mb = 100;
        // Explicit override wins
        assert_eq!(config.effective_max_body_size_mb(), 100);
    }

    #[test]
    fn test_explicit_timeout_override() {
        let mut config = CameoDbConfig::default();
        config.network.http.request_timeout_secs = Some(120);
        assert_eq!(config.effective_request_timeout_secs(), 120);
    }

    /// The regression this type change exists for.
    ///
    /// 30 was the previous default, and the accessor recognised an explicit value by its
    /// *difference* from that default — so this, the one value three shipped example configs
    /// actually wrote, silently resolved to 60.
    #[test]
    fn test_timeout_equal_to_the_old_default_is_honoured() {
        let mut config = CameoDbConfig::default();
        config.network.http.request_timeout_secs = Some(30);
        assert_eq!(config.effective_request_timeout_secs(), 30);

        config.network.cluster.messaging.request_timeout_secs = Some(30);
        assert_eq!(config.effective_remote_timeout_secs(), 30);
    }

    /// Absence is a distinct answer from any value, in both file formats.
    #[test]
    fn test_unset_timeout_derives_in_toml_and_yaml() {
        let toml: CameoDbConfig = toml::from_str("[network.http]\nport = 9480\n").unwrap();
        assert_eq!(toml.network.http.request_timeout_secs, None);
        assert_eq!(toml.effective_request_timeout_secs(), 60);

        let yaml: CameoDbConfig =
            serde_saphyr::from_str("network:\n  http:\n    port: 9480\n").unwrap();
        assert_eq!(yaml.network.http.request_timeout_secs, None);
        assert_eq!(yaml.effective_request_timeout_secs(), 60);
    }

    /// The unknown-key sweep builds its schema by serializing the default config, so a key
    /// that serializes away becomes "unknown" and warns on every start. `Option` must
    /// serialize as null, not vanish — which is why neither field carries
    /// `skip_serializing_if`.
    #[test]
    fn test_unset_timeout_still_appears_in_the_serialized_schema() {
        let schema = serde_json::to_value(CameoDbConfig::default()).unwrap();
        assert!(schema["network"]["http"]["request_timeout_secs"].is_null());
        assert!(schema["network"]["cluster"]["messaging"]["request_timeout_secs"].is_null());

        let file = "[network.http]\nrequest_timeout_secs = 30\n";
        assert!(
            unrecognized_keys(file).is_empty(),
            "a written timeout must not be reported as an unknown setting"
        );
    }

    /// Unset, the remote deadline follows HTTP rather than the field's own former default —
    /// the invariant `RouterActor` broke by reading the raw field.
    #[test]
    fn test_remote_timeout_follows_http_when_unset() {
        let mut config = CameoDbConfig::default();
        assert_eq!(config.network.cluster.messaging.request_timeout_secs, None);
        assert_eq!(config.effective_remote_timeout_secs(), 60);

        config.network.http.request_timeout_secs = Some(120);
        assert_eq!(config.effective_remote_timeout_secs(), 120);

        config.limits.max_record_size_mb = 2048;
        config.network.http.request_timeout_secs = None;
        assert_eq!(config.effective_remote_timeout_secs(), 204);
    }

    #[test]
    fn test_timeout_floor_tracks_record_size() {
        let mut config = CameoDbConfig::default();
        assert_eq!(config.timeout_floor_secs(), 6); // 64MB / 10

        config.limits.max_record_size_mb = 2048;
        assert_eq!(config.timeout_floor_secs(), 204);

        // Below the floor is warned about, never refused: the node must stay configurable for
        // a search-only deployment that wants a short timeout.
        config.network.http.request_timeout_secs = Some(1);
        assert!(config.validate().is_ok());
        assert_eq!(config.effective_request_timeout_secs(), 1);
    }

    #[test]
    fn test_zero_timeout_is_refused_on_both_paths() {
        let mut config = CameoDbConfig::default();
        config.network.http.request_timeout_secs = Some(0);
        assert!(config.validate().is_err());

        let mut config = CameoDbConfig::default();
        config.network.cluster.messaging.request_timeout_secs = Some(0);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_zero_record_size_fails_validation() {
        let config = CameoDbConfig {
            limits: LimitsConfig {
                max_record_size_mb: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    /// A PSK that round-trips is the only kind the swarm can start with, and `validate()`
    /// now shares this code path, so a config that validates is one that will boot.
    #[test]
    fn psk_hex_round_trips_through_load() {
        let mut config = CameoDbConfig::default();
        config.network.cluster.psk =
            Some("00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff".to_string());
        let psk = config
            .network
            .cluster
            .load_psk()
            .expect("valid psk")
            .expect("psk present");
        assert_eq!(psk.bytes()[0], 0x00);
        assert_eq!(psk.bytes()[1], 0x11);
        assert_eq!(psk.bytes()[31], 0xff);
    }

    #[test]
    fn psk_rejects_wrong_length_and_non_hex() {
        let mut config = CameoDbConfig::default();
        for bad in ["abc", &"a".repeat(63), &"a".repeat(65), &"z".repeat(64)] {
            config.network.cluster.psk = Some(bad.to_string());
            assert!(
                config.network.cluster.load_psk().is_err(),
                "'{}' must be rejected",
                bad
            );
            // validate() must agree — it is the same code path, and that is the point.
            assert!(config.validate().is_err(), "validate accepted '{}'", bad);
        }
    }

    #[test]
    fn psk_is_trimmed_so_a_file_with_a_trailing_newline_works() {
        let dir = std::env::temp_dir().join(format!("cameodb-psk-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("cluster.psk");
        std::fs::write(&path, format!("{}\n", "ab".repeat(32))).expect("write psk");

        let mut config = CameoDbConfig::default();
        config.network.cluster.psk_file = Some(path.clone());
        let psk = config
            .network
            .cluster
            .load_psk()
            .expect("valid psk file")
            .expect("psk present");
        assert_eq!(psk.bytes(), [0xab; 32]);
        std::fs::remove_file(&path).ok();
    }

    /// The secret must not reach a log or a config dump by any ordinary route.
    #[test]
    fn psk_is_not_printable_or_serializable() {
        let secret = "ab".repeat(32);
        let mut config = CameoDbConfig::default();
        config.network.cluster.psk = Some(secret.clone());

        let serialized = toml::to_string_pretty(&config).expect("serialize");
        assert!(
            !serialized.contains(&secret),
            "psk leaked into serialized config"
        );

        let psk = config.network.cluster.load_psk().unwrap().unwrap();
        let debug = format!("{:?}", psk);
        assert!(!debug.contains(&secret), "psk leaked via Debug: {}", debug);
        assert!(debug.contains("redacted"), "{}", debug);

        let cluster_debug = format!("{:?}", config.network.cluster);
        assert!(
            !cluster_debug.contains(&secret),
            "psk leaked via ClusterConfig Debug: {}",
            cluster_debug
        );
        assert!(cluster_debug.contains("redacted"), "{}", cluster_debug);

        let config_debug = format!("{:?}", config);
        assert!(
            !config_debug.contains(&secret),
            "psk leaked via CameoDbConfig Debug: {}",
            config_debug
        );
    }

    /// The open-index cap follows the memory budget unless it is set outright.
    ///
    /// Two numbers that must stay in step, so only one of them is written down. The clamps are
    /// the part worth pinning: without the floor a small node caps itself at one or two indexes
    /// and evicts on nearly every request, and without the ceiling a large budget derives a cap
    /// in the thousands — bounding the megabytes while leaving the three OS threads per open
    /// index to be what takes the node down.
    #[test]
    fn the_open_index_cap_follows_the_memory_budget_until_it_is_set() {
        let mut config = CameoDbConfig::default();
        config.limits.total_memory_limit_mb = 2048;
        config.search.indexer_memory_min_mb = 64;
        assert_eq!(
            config.effective_max_open_indexes(),
            32,
            "one smallest-size arena per open index is what the budget divides into"
        );

        // A budget too small to derive a workable cap from still gets the floor.
        config.limits.total_memory_limit_mb = 64;
        assert_eq!(config.effective_max_open_indexes(), 8, "the floor holds");

        // And a large one does not derive a cap the thread count could not survive.
        config.limits.total_memory_limit_mb = 1024 * 1024;
        assert_eq!(
            config.effective_max_open_indexes(),
            256,
            "the ceiling holds"
        );

        // Set outright, it is taken as written — including past the derived ceiling.
        config.limits.max_open_indexes = 4000;
        assert_eq!(
            config.effective_max_open_indexes(),
            4000,
            "an operator who names the number means it"
        );
    }

    /// A rejected PSK must not appear in the reason it was rejected.
    ///
    /// The refusal in `load_psk` is the one place a malformed secret would otherwise reach a
    /// log: it is raised before anything else looks at the value, and it is the only thing the
    /// operator sees. The code says so in a comment; this is what holds it to it. Both shapes
    /// of refusal are covered, because they are two different messages — a value of the wrong
    /// length, and one of the right length that is not hex.
    #[test]
    fn a_refused_psk_does_not_appear_in_its_own_error() {
        for bad in [
            // Wrong length, and long enough that a message echoing it would be obvious.
            "sekrit-cluster-key-do-not-log-this".to_string(),
            // Right length, wrong alphabet: the other arm of the same check.
            "z".repeat(64),
        ] {
            let mut config = CameoDbConfig::default();
            config.network.cluster.psk = Some(bad.clone());

            let err = config
                .network
                .cluster
                .load_psk()
                .expect_err("a malformed psk must be refused");
            let message = format!("{err:#}");
            assert!(
                !message.contains(&bad),
                "a refused psk leaked into its own error: {message}"
            );
            assert!(
                message.contains("64 hex characters"),
                "the refusal should still say what a valid key looks like: {message}"
            );
        }
    }

    /// pnet disables QUIC, so a QUIC address alongside a PSK can never connect. Catching
    /// it here beats a dial-time warning nobody reads.
    #[test]
    fn psk_with_quic_addresses_is_rejected() {
        let mut config = CameoDbConfig::default();
        config.network.cluster.psk = Some("ab".repeat(32));
        config.network.cluster.seed_nodes = vec!["/ip4/10.0.0.5/udp/9580/quic-v1".to_string()];
        let err = config.validate().expect_err("quic + psk must be rejected");
        assert!(err.to_string().contains("QUIC"), "{}", err);

        config.network.cluster.seed_nodes = vec!["/ip4/10.0.0.5/tcp/9580".to_string()];
        assert!(config.validate().is_ok(), "tcp seed must be accepted");
    }

    /// TLS config is loaded before the banner now, but validation still has to reject the
    /// half-configured cases up front.
    #[test]
    fn tls_requires_both_files_and_they_must_exist() {
        let mut config = CameoDbConfig::default();
        config.network.http.tls.enabled = true;
        assert!(config.validate().is_err(), "no cert/key configured");

        config.network.http.tls.cert_file = Some(PathBuf::from("/nonexistent/cert.pem"));
        assert!(config.validate().is_err(), "no key configured");

        config.network.http.tls.key_file = Some(PathBuf::from("/nonexistent/key.pem"));
        let err = config
            .validate()
            .expect_err("missing files must be rejected");
        assert!(err.to_string().contains("not found"), "{}", err);
    }

    #[test]
    fn wildcard_cors_is_rejected_outside_dev() {
        let mut config = CameoDbConfig::default();
        config.node.profile = Some(crate::posture::Profile::Internal);
        config.network.http.bind_address = "0.0.0.0".to_string();
        config.network.http.cors_allowed_origins = vec!["*".to_string()];
        let err = config.validate().expect_err("wildcard must be rejected");
        assert!(err.to_string().contains("cors"), "{}", err);
    }

    /// An empty origin list used to be a config error, which pushed operators towards
    /// "*". It is now the default and must validate.
    #[test]
    fn empty_cors_validates() {
        let mut config = CameoDbConfig::default();
        config.network.http.cors_allowed_origins = vec![];
        assert!(config.validate().is_ok());
    }

    #[test]
    fn profile_flag_and_env_are_parsed() {
        let parsed = cli(&["--profile", "external"]);
        let mut config = CameoDbConfig::default();
        config = CameoDbConfig::apply_overrides(config, &parsed).expect("apply");
        assert_eq!(config.node.profile, Some(crate::posture::Profile::External));

        assert!(cli_help().contains("--profile"));
        assert!(CliOverrides::parse(["--profile", "nonsense"].map(String::from)).is_ok());
        let bad = cli(&["--profile", "nonsense"]);
        assert!(CameoDbConfig::apply_overrides(CameoDbConfig::default(), &bad).is_err());
    }
}
