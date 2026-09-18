//! The interactive session: connection state, the rustyline completer, command
//! dispatch and the usage table the help text is built from.

use super::*;
use crate::sdk::{CameoClient, ClientAuth, Credential, ListIndexesResponse, TlsTrust, origin_of};
use anyhow::{Context, Result, anyhow};
use reqwest::Url;
use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Editor, Helper, error::ReadlineError};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

// Only import colored on non-Windows platforms
#[cfg(not(target_os = "windows"))]
use colored::Colorize;

#[derive(Debug, Clone)]
pub(crate) struct FieldInfo {
    pub(crate) name: String,
    pub(crate) field_type: String,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexMetadata {
    pub(crate) fields: Vec<FieldInfo>,
}

#[derive(Debug)]
pub(crate) struct InteractiveSession {
    pub(crate) current_url: String,
    pub(crate) client: CameoClient,
    pub(crate) index_cache: Arc<RwLock<HashMap<String, IndexMetadata>>>,
    /// Carried so that `connect <target>` mid-session keeps the trust settings the
    /// session was started with instead of silently re-enabling verification.
    pub(crate) trust: TlsTrust,
    /// The key the session was started with, kept whole across reconnects.
    pub(crate) auth: ClientAuth,
    /// The origin that key was given for. `connect` elsewhere drops it; `connect` back
    /// restores it, which is why the credential is kept rather than discarded outright.
    pub(crate) key_origin: String,
}

impl InteractiveSession {
    pub(crate) fn new_with_trust(
        initial_url: String,
        trust: TlsTrust,
        auth: ClientAuth,
    ) -> Result<Self> {
        let client = CameoClient::new_with_options(&initial_url, trust, auth.clone())?;
        Ok(Self {
            key_origin: origin_of(&initial_url),
            current_url: initial_url,
            client,
            index_cache: Arc::new(RwLock::new(HashMap::new())),
            trust,
            auth,
        })
    }

    pub(crate) fn reconnect(&mut self, target: &str) -> Result<()> {
        let normalized = normalize_connect_target(target)?;
        let mut auth = self.auth.clone();

        // A key authenticates you to one node. Carrying it to whatever host is typed next
        // would hand it to that host, which is how a mistyped `connect` becomes a leak.
        if auth.credential.is_some() && origin_of(&normalized) != self.key_origin {
            auth.credential = None;
            println!(
                "🔑 Key not sent to {} — it is bound to {}. Start the client with --api-key \
                 against that origin to authenticate there.",
                origin_of(&normalized),
                self.key_origin
            );
        }

        self.client = CameoClient::new_with_options(&normalized, self.trust, auth)?;
        self.current_url = normalized;
        self.clear_index_cache();
        Ok(())
    }

    pub(crate) fn client(&self) -> &CameoClient {
        &self.client
    }

    /// Present a key to the origin this session is connected to.
    ///
    /// Without this, `connect` to another origin dropped the key — correctly — and the only
    /// way back was restarting the client. The new key is bound to the current origin, the
    /// same as one passed on the command line.
    pub(crate) fn set_credential(&mut self, credential: Option<Credential>) -> Result<()> {
        let auth = ClientAuth {
            credential,
            allow_plaintext: self.auth.allow_plaintext,
        };
        self.client = CameoClient::new_with_options(&self.current_url, self.trust, auth.clone())?;
        self.key_origin = origin_of(&self.current_url);
        self.auth = auth;
        self.clear_index_cache();
        Ok(())
    }

    /// The key in use for the current origin, if any.
    pub(crate) fn key_id(&self) -> Option<String> {
        self.client.key_id()
    }

    pub(crate) fn index_cache_handle(&self) -> Arc<RwLock<HashMap<String, IndexMetadata>>> {
        Arc::clone(&self.index_cache)
    }

    pub(crate) fn clear_index_cache(&self) {
        if let Ok(mut cache) = self.index_cache.write() {
            cache.clear();
        }
    }

    pub(crate) async fn refresh_index_cache(&self) {
        if let Ok(indexes) = self.client.list_indexes(false).await {
            self.update_index_cache(&indexes).await;
        }
    }

    pub(crate) async fn update_index_cache(&self, response: &ListIndexesResponse) {
        let mut cache_updates = HashMap::new();

        // The listing already describes every field, so this no longer fetches a schema per
        // index. It used to, on top of the one the caller had just made for the same reason —
        // so opening a REPL against a catalogue of N indexes cost 1 + 2N requests.
        for idx in &response.indexes {
            let fields = idx
                .fields
                .iter()
                .filter_map(|field| {
                    Some(FieldInfo {
                        name: field.get("name")?.as_str()?.to_string(),
                        field_type: field
                            .get("type")
                            .and_then(|t| t.as_str())
                            .unwrap_or("text")
                            .to_string(),
                    })
                })
                .collect();

            cache_updates.insert(idx.name.clone(), IndexMetadata { fields });
        }

        // Update cache in one operation
        if let Ok(mut cache) = self.index_cache.write() {
            *cache = cache_updates;
        }
    }

    pub(crate) fn prompt(&self) -> String {
        let host = self.display_host();

        // Plain prompt on Windows to avoid ANSI cursor issues; colored Bash-style elsewhere
        #[cfg(target_os = "windows")]
        {
            format!("cameodb@{} ▶ ", host)
        }

        #[cfg(not(target_os = "windows"))]
        {
            format!(
                "{}{}{} ▶ ",
                "cameodb".bold().cyan(),
                "@".white(),
                host.bold().green()
            )
        }
    }

    pub(crate) fn origin(&self) -> String {
        origin_of(&self.current_url)
    }

    pub(crate) fn display_host(&self) -> String {
        Url::parse(&self.current_url)
            .ok()
            .and_then(|u| u.host_str().map(|h| h.to_string()))
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "unknown".to_string())
    }
}

/// The token in front of the one under the cursor, which is the last of `tokens`.
pub(crate) fn preceding_token<'a>(tokens: &[&'a str]) -> Option<&'a str> {
    let len = tokens.len();
    (len >= 2).then(|| tokens[len - 2])
}

/// A replacement that closes its own token: a field's colon, a projection's comma, a directory's
/// slash, or a space the suggestion carries itself.
pub(crate) fn is_self_terminating(replacement: &str) -> bool {
    replacement.ends_with([' ', ':', ',', '/'])
}

/// The prefix every replacement shares, which is what rustyline splices into the line when the
/// candidates are ambiguous.
pub(crate) fn shared_prefix(pairs: &[Pair]) -> &str {
    let Some(first) = pairs.first() else {
        return "";
    };

    let mut shared = first.replacement.as_str();
    for pair in &pairs[1..] {
        let mut len = shared
            .bytes()
            .zip(pair.replacement.bytes())
            .take_while(|(a, b)| a == b)
            .count();
        while !shared.is_char_boundary(len) {
            len -= 1;
        }
        shared = &shared[..len];
    }
    shared
}

#[derive(Clone, Debug)]
pub(crate) struct IndexCompleter {
    pub(crate) cache: Arc<RwLock<HashMap<String, IndexMetadata>>>,
}

impl IndexCompleter {
    pub(crate) fn new(cache: Arc<RwLock<HashMap<String, IndexMetadata>>>) -> Self {
        Self { cache }
    }

    pub(crate) fn friendly_type_label(&self, raw: &str) -> String {
        let normalized = raw.to_lowercase();
        match normalized.as_str() {
            "boolean" => "true/false".to_string(),
            "integer" | "i64" | "int" | "number" | "u64" => "numeric".to_string(),
            "float" | "f64" | "double" => "decimal".to_string(),
            "text" | "string" => "text".to_string(),
            "exact" => "exact".to_string(),
            _ => normalized,
        }
    }

    pub(crate) fn split_field_modifier<'a>(&self, token: &'a str) -> (&'a str, &'a str) {
        let mut split_idx = 0;
        for (idx, ch) in token.char_indices() {
            if matches!(ch, '+' | '-' | '!') {
                split_idx = idx + ch.len_utf8();
            } else {
                break;
            }
        }
        token.split_at(split_idx)
    }

    pub(crate) fn index_suggestions(&self, prefix: &str) -> Vec<Pair> {
        if let Ok(cache) = self.cache.read() {
            cache
                .keys()
                .filter(|name| name.starts_with(prefix))
                .map(|name| Pair {
                    display: name.clone(),
                    replacement: name.clone(),
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    pub(crate) fn field_suggestions(&self, index: &str, prefix: &str) -> Vec<Pair> {
        if let Ok(cache) = self.cache.read()
            && let Some(metadata) = cache.get(index)
        {
            let (_, clean_prefix) = self.split_field_modifier(prefix);
            return metadata
                .fields
                .iter()
                .filter(|field| field.name.starts_with(clean_prefix))
                .map(|field| {
                    let label = self.friendly_type_label(&field.field_type);
                    let (modifier, _) = self.split_field_modifier(prefix);
                    Pair {
                        display: format!("{}: [{}]", field.name, label),
                        replacement: format!("{}{}:", modifier, field.name),
                    }
                })
                .collect();
        }
        Vec::new()
    }

    /// Fields for a `return` clause, each completed with its trailing comma so that successive
    /// completions build one comma-separated list. A list without them is query text, not a
    /// projection.
    pub(crate) fn return_field_suggestions(&self, index: &str, prefix: &str) -> Vec<Pair> {
        if let Ok(cache) = self.cache.read()
            && let Some(metadata) = cache.get(index)
        {
            let clean_prefix = prefix.trim_end_matches(',');
            return metadata
                .fields
                .iter()
                .filter(|field| field.name.starts_with(clean_prefix))
                .map(|field| Pair {
                    display: field.name.clone(),
                    replacement: format!("{},", field.name),
                })
                .collect();
        }
        Vec::new()
    }

    pub(crate) fn sort_field_suggestions(&self, index: &str, prefix: &str) -> Vec<Pair> {
        if let Ok(cache) = self.cache.read()
            && let Some(metadata) = cache.get(index)
        {
            // Extract field name if prefix contains ':'
            let (field_prefix, has_colon) = if let Some(colon_pos) = prefix.find(':') {
                (&prefix[..colon_pos], true)
            } else {
                (prefix, false)
            };

            // Filter for sortable fields: FAST numeric/date fields and text/string fields
            // (text/string are sorted alphabetically post-fetch, no FAST flag required).
            let sortable_fields: Vec<_> = metadata
                .fields
                .iter()
                .filter(|field| {
                    let ft = field.field_type.to_lowercase();
                    field.name.starts_with(field_prefix)
                        && (ft.contains("u64")
                            || ft.contains("i64")
                            || ft.contains("f64")
                            || ft.contains("date")
                            || ft.contains("text")
                            || ft.contains("string"))
                })
                .collect();

            if has_colon {
                // User typed "field:", suggest :asc and :desc
                let field_name = field_prefix;
                vec![
                    Pair {
                        display: format!("{}:desc", field_name),
                        replacement: format!("{}:desc", field_name),
                    },
                    Pair {
                        display: format!("{}:asc", field_name),
                        replacement: format!("{}:asc", field_name),
                    },
                ]
            } else {
                // Suggest sortable field names with :asc suffix (default)
                sortable_fields
                    .iter()
                    .flat_map(|field| {
                        vec![
                            Pair {
                                display: format!("{}:asc (default)", field.name),
                                replacement: format!("{}:asc", field.name),
                            },
                            Pair {
                                display: format!("{}:desc", field.name),
                                replacement: format!("{}:desc", field.name),
                            },
                        ]
                    })
                    .collect()
            }
        } else {
            Vec::new()
        }
    }

    pub(crate) fn field_type_hint(&self, index: &str, field: &str) -> Option<String> {
        let (_, clean_field) = self.split_field_modifier(field);
        if let Ok(cache) = self.cache.read()
            && let Some(metadata) = cache.get(index)
            && let Some(info) = metadata.fields.iter().find(|f| f.name == clean_field)
        {
            let label = self.friendly_type_label(&info.field_type);
            return Some(format!("[{}]", label));
        }
        None
    }

    pub(crate) fn command_suggestions(&self, prefix: &str) -> Vec<Pair> {
        let commands = vec![
            "health", "list", "search", "schema", "data", "delete", "admin", "connect", "conn",
            "exit", "quit", "help", "key",
        ];
        commands
            .into_iter()
            .filter(|cmd| cmd.starts_with(prefix))
            .map(|cmd| Pair {
                display: cmd.to_string(),
                replacement: cmd.to_string(),
            })
            .collect()
    }

    pub(crate) fn expand_dir_part(&self, dir_part: &str) -> PathBuf {
        if let Some(stripped) = dir_part.strip_prefix("~/")
            && let Some(home) = dirs::home_dir()
        {
            return home.join(stripped);
        }

        if dir_part.is_empty() {
            PathBuf::from(".")
        } else {
            PathBuf::from(dir_part)
        }
    }

    pub(crate) fn file_path_suggestions(&self, prefix: &str) -> Vec<Pair> {
        // Split prefix into directory part (with trailing slash) and file prefix
        let (dir_part, file_prefix) = if prefix.ends_with('/') {
            (prefix.to_string(), "".to_string())
        } else if let Some(pos) = prefix.rfind('/') {
            (prefix[..=pos].to_string(), prefix[pos + 1..].to_string())
        } else {
            ("".to_string(), prefix.to_string())
        };

        let fs_dir = self.expand_dir_part(&dir_part);
        let mut pairs = Vec::new();

        if let Ok(entries) = fs::read_dir(&fs_dir) {
            for entry in entries.flatten() {
                let file_name = entry.file_name();
                let name = file_name.to_string_lossy();
                if !name.starts_with(&file_prefix) {
                    continue;
                }

                let mut replacement = format!("{}{}", dir_part, name);
                let mut display = name.to_string();
                if let Ok(md) = entry.metadata()
                    && md.is_dir()
                {
                    replacement.push('/');
                    display.push('/');
                }

                pairs.push(Pair {
                    display,
                    replacement,
                });
            }
        }

        pairs
    }

    /// A flag is offered while what is typed is still a prefix of it, and only until it is on the
    /// line.
    pub(crate) fn flag_suggestion(
        &self,
        flag: &str,
        display: &str,
        prefix: &str,
        tokens: &[&str],
    ) -> Option<Pair> {
        (!tokens.contains(&flag) && flag.starts_with(prefix)).then(|| Pair {
            display: display.to_string(),
            replacement: flag.to_string(),
        })
    }

    /// The value of `--delimiter`, which is its own token.
    pub(crate) fn delimiter_value_suggestions(&self, prefix: &str) -> Vec<Pair> {
        ["detect", "comma", "tab", "semicolon"]
            .into_iter()
            .filter(|opt| opt.starts_with(prefix))
            .map(|opt| Pair {
                display: opt.to_string(),
                replacement: opt.to_string(),
            })
            .collect()
    }

    /// Flags for `schema detect|load`, offered once its positional arguments are in.
    pub(crate) fn schema_flag_suggestions(&self, current: &str, tokens: &[&str]) -> Vec<Pair> {
        if preceding_token(tokens) == Some("--delimiter") {
            return self.delimiter_value_suggestions(current);
        }
        self.flag_suggestion(
            "--delimiter",
            "--delimiter <detect|comma|tab|semicolon>",
            current,
            tokens,
        )
        .into_iter()
        .collect()
    }

    /// Flags for `data load`, offered once its positional arguments are in.
    pub(crate) fn data_flag_suggestions(&self, current: &str, tokens: &[&str]) -> Vec<Pair> {
        match preceding_token(tokens) {
            Some("--delimiter") => return self.delimiter_value_suggestions(current),
            // A document count: nothing to complete.
            Some("--batch-size") => return Vec::new(),
            _ => {}
        }

        let mut suggestions: Vec<Pair> = self
            .flag_suggestion(
                "--delimiter",
                "--delimiter <detect|comma|tab|semicolon>",
                current,
                tokens,
            )
            .into_iter()
            .collect();

        // The count that follows completes to nothing, which is invisible to the lookahead in
        // `terminate_completed_words`, so the flag carries its own space.
        if !tokens.contains(&"--batch-size") && "--batch-size".starts_with(current) {
            suggestions.push(Pair {
                display: "--batch-size <n>".to_string(),
                replacement: "--batch-size ".to_string(),
            });
        }

        suggestions
    }

    pub(crate) fn delete_flag_suggestions(&self, prefix: &str, tokens: &[&str]) -> Vec<Pair> {
        ["--id", "--ids-file", "--routing-key", "--delete-schema"]
            .into_iter()
            .filter_map(|flag| self.flag_suggestion(flag, flag, prefix, tokens))
            .collect()
    }

    pub(crate) fn force_flag_suggestions(&self, prefix: &str, tokens: &[&str]) -> Vec<Pair> {
        self.flag_suggestion("--force", "--force", prefix, tokens)
            .into_iter()
            .collect()
    }

    pub(crate) fn admin_subcommand_suggestions(&self, prefix: &str) -> Vec<Pair> {
        let subcommands = vec!["memory", "index", "workers"];
        subcommands
            .into_iter()
            .filter(|sub| sub.starts_with(prefix))
            .map(|sub| Pair {
                display: sub.to_string(),
                replacement: sub.to_string(),
            })
            .collect()
    }

    pub(crate) fn admin_memory_operation_suggestions(&self, prefix: &str) -> Vec<Pair> {
        let operations = vec!["stats", "purge"];
        operations
            .into_iter()
            .filter(|op| op.starts_with(prefix))
            .map(|op| Pair {
                display: op.to_string(),
                replacement: op.to_string(),
            })
            .collect()
    }

    pub(crate) fn admin_index_operation_suggestions(&self, prefix: &str) -> Vec<Pair> {
        let operations = vec!["commit", "evict-writer"];
        operations
            .into_iter()
            .filter(|op| op.starts_with(prefix))
            .map(|op| Pair {
                display: op.to_string(),
                replacement: op.to_string(),
            })
            .collect()
    }

    pub(crate) fn list_subcommand_suggestions(&self, prefix: &str) -> Vec<Pair> {
        let subcommands = vec!["indexes", "index"];
        subcommands
            .into_iter()
            .filter(|sub| sub.starts_with(prefix))
            .map(|sub| Pair {
                display: sub.to_string(),
                replacement: sub.to_string(),
            })
            .collect()
    }

    pub(crate) fn extended_flag_suggestions(&self, current: &str, tokens: &[&str]) -> Vec<Pair> {
        let has_extended = tokens.iter().any(|t| *t == "--extended" || *t == "-e");
        let has_data_size = tokens.contains(&"--data-size");
        let mut suggestions = Vec::new();

        if !has_extended && "--extended".starts_with(current) {
            suggestions.push(Pair {
                display: "--extended".to_string(),
                replacement: "--extended".to_string(),
            });
        }
        if !has_extended && "-e".starts_with(current) {
            suggestions.push(Pair {
                display: "-e".to_string(),
                replacement: "-e".to_string(),
            });
        }
        if !has_data_size && "--data-size".starts_with(current) {
            suggestions.push(Pair {
                display: "--data-size".to_string(),
                replacement: "--data-size".to_string(),
            });
        }
        suggestions
    }

    pub(crate) fn schema_subcommand_suggestions(&self, prefix: &str) -> Vec<Pair> {
        let subcommands = vec!["detect", "load"];
        subcommands
            .into_iter()
            .filter(|sub| sub.starts_with(prefix))
            .map(|sub| Pair {
                display: sub.to_string(),
                replacement: sub.to_string(),
            })
            .collect()
    }

    /// `key` also takes the key itself, which is not something to complete. `file` leads, as it
    /// does in the help text: it is the form that keeps the key out of the history file.
    pub(crate) fn key_subcommand_suggestions(&self, prefix: &str) -> Vec<Pair> {
        let subcommands = vec!["file", "show", "clear"];
        subcommands
            .into_iter()
            .filter(|sub| sub.starts_with(prefix))
            .map(|sub| Pair {
                display: sub.to_string(),
                replacement: sub.to_string(),
            })
            .collect()
    }

    pub(crate) fn data_subcommand_suggestions(&self, prefix: &str) -> Vec<Pair> {
        let subcommands = vec!["load"];
        subcommands
            .into_iter()
            .filter(|sub| sub.starts_with(prefix))
            .map(|sub| Pair {
                display: sub.to_string(),
                replacement: sub.to_string(),
            })
            .collect()
    }

    /// Ends a completed word with a space, so that one Tab settles the word and the next moves on
    /// to the position after it. A word that ends the command keeps its bare form, since a space
    /// there is only something to delete.
    ///
    /// Which of the two a word is comes from asking `complete_tokens` what an empty token in the
    /// next position would offer. `complete_tokens` holds no terminator logic of its own, which is
    /// what keeps that one level deep.
    pub(crate) fn terminate_completed_words(&self, tokens: &[&str], suggestions: &mut [Pair]) {
        // Past the command word and its subcommand, `complete_tokens` branches on position rather
        // than on the word under the cursor, so a single probe answers for the whole set. Nearer
        // the front of the line the word does decide — `admin workers` ends the command where
        // `admin memory` does not — and each candidate has to be probed on its own.
        let word_decides_grammar = tokens.len() <= 2;
        let mut answer_for_position = None;

        for pair in suggestions.iter_mut() {
            if is_self_terminating(&pair.replacement) {
                continue;
            }

            let continues = if word_decides_grammar {
                self.completes_after(tokens, &pair.replacement)
            } else {
                *answer_for_position
                    .get_or_insert_with(|| self.completes_after(tokens, &pair.replacement))
            };

            if continues {
                pair.replacement.push(' ');
            }
        }
    }

    /// Whether the position after `settled` has anything to offer.
    pub(crate) fn completes_after(&self, tokens: &[&str], settled: &str) -> bool {
        let Some((_, leading)) = tokens.split_last() else {
            return false;
        };

        let mut lookahead: Vec<&str> = leading.to_vec();
        lookahead.push(settled);

        // A free-form argument — a host to connect to — completes to nothing, yet the command word
        // in front of it is still not the end of the line.
        if lookahead.len() == 1 && matches!(settled, "connect" | "conn") {
            return true;
        }

        lookahead.push("");
        self.complete_tokens(&lookahead, "")
            .is_some_and(|next| !next.is_empty())
    }

    pub(crate) fn complete_tokens(&self, tokens: &[&str], current: &str) -> Option<Vec<Pair>> {
        if tokens.is_empty() {
            return None;
        }

        match tokens[0] {
            // Complete main commands when first token or partial command
            _cmd if tokens.len() == 1 => {
                let suggestions = self.command_suggestions(current);
                Some(suggestions)
            }
            // Complete subcommands for 'list'
            "list" if tokens.len() == 2 => {
                let suggestions = self.list_subcommand_suggestions(current);
                Some(suggestions)
            }
            // `list indexes` takes no positional argument, so every token after it is a flag.
            "list" if tokens[1] == "indexes" => {
                let suggestions = self.extended_flag_suggestions(current, tokens);
                Some(suggestions)
            }
            "list" if tokens.len() == 3 && tokens[1] == "index" => {
                let suggestions = self.index_suggestions(current);
                Some(suggestions)
            }
            // Past the index name only a flag is valid.
            "list" if tokens[1] == "index" => {
                let suggestions = self.extended_flag_suggestions(current, tokens);
                Some(suggestions)
            }
            // Complete schema subcommands and index for schema load
            "schema" if tokens.len() == 2 => {
                let suggestions = self.schema_subcommand_suggestions(current);
                Some(suggestions)
            }
            "schema" if tokens.len() == 3 && tokens[1] == "load" => {
                let suggestions = self.index_suggestions(current);
                Some(suggestions)
            }
            "schema"
                if (tokens.len() == 3 && tokens[1] == "detect")
                    || (tokens.len() == 4 && tokens[1] == "load") =>
            {
                let suggestions = self.file_path_suggestions(current);
                Some(suggestions)
            }
            "schema" if matches!(tokens[1], "detect" | "load") => {
                let suggestions = self.schema_flag_suggestions(current, tokens);
                Some(suggestions)
            }
            // Complete index name for 'search'
            "search" if tokens.len() == 2 => {
                let suggestions = self.index_suggestions(current);
                Some(suggestions)
            }
            // Complete field names in search query
            "search" if tokens.len() >= 3 => {
                let index = tokens[1];

                // Check if we're after a 'return' keyword to suggest fields
                let query_tokens = &tokens[2..];
                let after_return = query_tokens.iter().rposition(|t| *t == "return");

                if let Some(return_pos) = after_return {
                    // We're after 'return', suggest fields (comma-separated)
                    let after_return_tokens = &query_tokens[return_pos + 1..];

                    // Check if current token is after 'limit' or 'sort' keyword
                    let after_limit = after_return_tokens.contains(&"limit");
                    let after_sort = after_return_tokens.contains(&"sort");

                    if !after_limit && !after_sort {
                        let suggestions = self.return_field_suggestions(index, current);
                        return Some(suggestions);
                    }
                }

                // Check if we're after a 'sort' keyword to suggest sortable fields
                let after_sort = query_tokens.iter().rposition(|t| *t == "sort");

                if let Some(sort_pos) = after_sort {
                    // We're after 'sort', suggest sortable fields with :asc/:desc suffix
                    let after_sort_tokens = &query_tokens[sort_pos + 1..];

                    // Check if current token is after 'return' or 'limit' keyword
                    let after_return_kw = after_sort_tokens.contains(&"return");
                    let after_limit_kw = after_sort_tokens.contains(&"limit");

                    if !after_return_kw && !after_limit_kw {
                        let suggestions = self.sort_field_suggestions(index, current);
                        return Some(suggestions);
                    }
                }

                // A modifier run has to leave query text in front of it, so a keyword is offered
                // only once the query opens with something that is not one. The last of
                // `query_tokens` is the token under the cursor, empty when it follows a space.
                let query_written = query_tokens[..query_tokens.len() - 1]
                    .first()
                    .is_some_and(|token| !matches!(*token, "return" | "limit" | "sort"));

                if query_written
                    && (current.is_empty()
                        || "return".starts_with(current)
                        || "limit".starts_with(current)
                        || "sort".starts_with(current))
                {
                    let mut suggestions = Vec::new();

                    // Only suggest 'return' if not already in query
                    if !query_tokens.contains(&"return") && "return".starts_with(current) {
                        suggestions.push(Pair {
                            display: "return <fields>".to_string(),
                            replacement: "return ".to_string(),
                        });
                    }

                    // Only suggest 'limit' if not already in query
                    if !query_tokens.contains(&"limit") && "limit".starts_with(current) {
                        suggestions.push(Pair {
                            display: "limit <n>".to_string(),
                            replacement: "limit ".to_string(),
                        });
                    }

                    // Only suggest 'sort' if not already in query
                    if !query_tokens.contains(&"sort") && "sort".starts_with(current) {
                        suggestions.push(Pair {
                            display: "sort <field:order>".to_string(),
                            replacement: "sort ".to_string(),
                        });
                    }

                    if current.is_empty() {
                        suggestions.extend(self.field_suggestions(index, current));
                    }

                    if !suggestions.is_empty() {
                        return Some(suggestions);
                    }
                }

                // Default: suggest field names for query construction
                let suggestions = self.field_suggestions(index, current);
                Some(suggestions)
            }
            // Complete data subcommands and index for data load
            "data" if tokens.len() == 2 => {
                let suggestions = self.data_subcommand_suggestions(current);
                Some(suggestions)
            }
            "data" if tokens.len() == 3 && tokens[1] == "load" => {
                let suggestions = self.index_suggestions(current);
                Some(suggestions)
            }
            "data" if tokens.len() == 4 && tokens[1] == "load" => {
                let suggestions = self.file_path_suggestions(current);
                Some(suggestions)
            }
            "data" if tokens[1] == "load" => {
                let suggestions = self.data_flag_suggestions(current, tokens);
                Some(suggestions)
            }
            // Complete index name for delete
            "delete" if tokens.len() == 2 => {
                let suggestions = self.index_suggestions(current);
                Some(suggestions)
            }
            "delete" => {
                let suggestions = self.delete_flag_suggestions(current, tokens);
                Some(suggestions)
            }
            // Admin subcommands
            "admin" if tokens.len() == 2 => {
                let suggestions = self.admin_subcommand_suggestions(current);
                Some(suggestions)
            }
            "admin" if tokens.len() == 3 && tokens[1] == "memory" => {
                let suggestions = self.admin_memory_operation_suggestions(current);
                Some(suggestions)
            }
            // Only a purge takes a flag; `admin memory stats` ends there.
            "admin" if tokens[1] == "memory" && tokens[2] == "purge" => {
                let suggestions = self.force_flag_suggestions(current, tokens);
                Some(suggestions)
            }
            "admin" if tokens.len() == 3 && tokens[1] == "index" => {
                let suggestions = self.index_suggestions(current);
                Some(suggestions)
            }
            "admin" if tokens.len() == 4 && tokens[1] == "index" => {
                let suggestions = self.admin_index_operation_suggestions(current);
                Some(suggestions)
            }
            "key" if tokens.len() == 2 => {
                let suggestions = self.key_subcommand_suggestions(current);
                Some(suggestions)
            }
            "key" if tokens.len() == 3 && tokens[1] == "file" => {
                let suggestions = self.file_path_suggestions(current);
                Some(suggestions)
            }
            _ => None,
        }
    }
}

impl Validator for IndexCompleter {
    fn validate(&self, _ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
        Ok(ValidationResult::Valid(None))
    }
}

impl Helper for IndexCompleter {}

impl Highlighter for IndexCompleter {}

impl Hinter for IndexCompleter {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, _ctx: &rustyline::Context<'_>) -> Option<String> {
        let prefix = &line[..pos];
        let mut parts = prefix.split_whitespace();
        let command = parts.next()?;

        match command {
            "list" => {
                let subcommand = parts.next()?;
                let tail = parts.collect::<Vec<_>>();
                let has_extended = tail.iter().any(|t| *t == "--extended" || *t == "-e");

                if (subcommand == "index" || subcommand == "indexes")
                    && let Some(current) = tail.last()
                {
                    if !has_extended && "--extended".starts_with(current) {
                        return Some("extended".strip_prefix(current).unwrap_or("").to_string());
                    }
                    if !has_extended && "-e".starts_with(current) {
                        return Some("e".strip_prefix(current).unwrap_or("").to_string());
                    }
                }
                None
            }
            "search" => {
                let index = parts.next()?;
                let tail = parts.collect::<Vec<_>>();

                // Check if user is typing a field value
                if let Some(current) = tail.last() {
                    // Check if we're after the 'sort' keyword — sort values are asc/desc,
                    // not field values, so field type hints are irrelevant.
                    let after_sort = tail.contains(&"sort");

                    if let Some((field, value_prefix)) = current.split_once(':')
                        && !after_sort
                    {
                        let trimmed_value = value_prefix
                            .trim_start_matches(&['>', '<', '=', '!'][..])
                            .trim();
                        if trimmed_value.is_empty() {
                            return self
                                .field_type_hint(index, field)
                                .map(|hint| format!(" {}", hint));
                        }
                    }

                    // Provide hints for 'return' and 'limit' keywords
                    let has_return = tail.contains(&"return");
                    let has_limit = tail.contains(&"limit");

                    // If typing 'r', hint 'return'
                    if current.eq_ignore_ascii_case("r") && !has_return {
                        return Some("eturn ".to_string());
                    }

                    // If typing 'l', hint 'limit'
                    if current.eq_ignore_ascii_case("l") && !has_limit {
                        return Some("imit ".to_string());
                    }

                    // If typing 'ret', hint 'return'
                    if "return".starts_with(&current.to_lowercase())
                        && current.len() < "return".len()
                        && !has_return
                    {
                        return Some(format!("{} ", &"return"[current.len()..]));
                    }

                    // If typing 'lim', hint 'limit'
                    if "limit".starts_with(&current.to_lowercase())
                        && current.len() < "limit".len()
                        && !has_limit
                    {
                        return Some(format!("{} ", &"limit"[current.len()..]));
                    }
                }

                None
            }
            "admin" => {
                let subcommand = parts.next()?;
                match subcommand {
                    "memory" => {
                        let op = parts.next();
                        if op.is_none() {
                            return Some(" <stats|purge>".to_string());
                        }
                        if op == Some("purge") {
                            let has_force = parts.any(|p| p == "--force");
                            if !has_force {
                                return Some(" [--force]".to_string());
                            }
                        }
                        None
                    }
                    "index" => {
                        let index = parts.next();
                        if index.is_none() {
                            return Some(" <index>".to_string());
                        }
                        let op = parts.next();
                        if op.is_none() {
                            return Some(" <commit|evict-writer>".to_string());
                        }
                        None
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

impl Completer for IndexCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let prefix = &line[..pos];

        // Find the byte offset where the token under the cursor begins.  This is
        // the position that rustyline should start replacing from.  Scanning
        // backwards from the cursor is more robust than `pos - token.len()`
        // because it tolerates any leading whitespace (tabs, multiple spaces,
        // etc.) and never confuses a trailing-whitespace insertion point with
        // the previous token.
        let start = prefix
            .char_indices()
            .rev()
            .find(|(_, c)| c.is_whitespace())
            .map(|(i, c)| i + c.len_utf8())
            .unwrap_or(0);

        let current = &prefix[start..pos];

        let mut parts: Vec<&str> = prefix.split_whitespace().collect();
        if prefix.chars().last().is_some_and(|c| c.is_whitespace()) {
            parts.push("");
        }

        // Handle empty/whitespace-only line case
        if parts.is_empty() || (parts.len() == 1 && parts[0].is_empty()) {
            let mut suggestions = self.command_suggestions("");
            self.terminate_completed_words(&[""], &mut suggestions);
            return Ok((start, suggestions));
        }

        let Some(mut suggestions) = self.complete_tokens(&parts, current) else {
            return Ok((pos, Vec::new()));
        };

        self.terminate_completed_words(&parts, &mut suggestions);
        elect_settled_word(current, &mut suggestions);
        Ok((start, suggestions))
    }
}

/// A finished word is not ambiguous just because a longer word shares its prefix: `index` is a
/// target in its own right next to `indexes`. rustyline splices only the shared prefix of the
/// candidates, which leaves such a word open and its Tab with nothing to do. Electing the finished
/// word gives that Tab the space that closes it.
pub(crate) fn elect_settled_word(current: &str, suggestions: &mut Vec<Pair>) {
    if suggestions.len() < 2 || shared_prefix(suggestions).len() > current.len() {
        return;
    }

    // Only a word this run terminated: a set that goes nowhere keeps every candidate.
    if let Some(settled) = suggestions
        .iter()
        .position(|pair| pair.replacement.strip_suffix(' ') == Some(current))
    {
        suggestions.swap(0, settled);
        suggestions.truncate(1);
    }
}

pub(crate) async fn run_interactive_shell(
    initial_url: String,
    trust: TlsTrust,
    auth: ClientAuth,
) -> Result<()> {
    println!("🛠️  CameoDB interactive client. Type 'help' for supported commands, 'exit' to quit.");
    match &auth.credential {
        // Which key, so a session against a node with several keys is not a guess. The
        // fingerprint matches what the node logs when it accepts the key.
        Some(credential) => println!(
            "🔑 Authenticating to {} with key {}.\n",
            origin_of(&initial_url),
            credential.key_id()
        ),
        None => println!(),
    }

    let session = InteractiveSession::new_with_trust(initial_url, trust, auth)?;
    let history_path = history_file_path()?;
    let handle = tokio::runtime::Handle::current();
    session.refresh_index_cache().await;

    tokio::task::spawn_blocking(move || interactive_loop(session, history_path, handle))
        .await
        .map_err(|e| anyhow!("Interactive shell join failed: {}", e))??;

    println!("Goodbye!");
    Ok(())
}

/// The interactive grammar, one string per command.
///
/// `interactive_help` prints the table below and every dispatch arm quotes these same
/// strings in its `Usage:` errors — the help can therefore never drift from what the
/// parser accepts, which is how the hand-written copy it replaces had already drifted.
mod usage {
    pub(super) const HEALTH: &str = "health";
    pub(super) const LIST_INDEXES: &str = "list indexes [--extended] [--data-size]";
    pub(super) const LIST_INDEX: &str = "list index <name> [--extended] [--data-size]";
    pub(super) const SEARCH: &str = "search <index> <query> [limit N]";
    pub(super) const SCHEMA: &str = "schema <detect|load> ...";
    pub(super) const SCHEMA_DETECT: &str = "schema detect <file> [--delimiter <delim>]";
    pub(super) const SCHEMA_LOAD: &str = "schema load <index> <file> [--delimiter <delim>]";
    pub(super) const DATA_LOAD: &str =
        "data load <index> <file> [--delimiter <delim>] [--batch-size <n>]";
    pub(super) const DELETE_INDEX: &str = "delete <index> [--delete-schema]";
    pub(super) const DELETE_DOCS: &str =
        "delete <index> (--id <ID[,ID...]> | --ids-file <path>) [--routing-key <KEY>]";
    pub(super) const ADMIN: &str = "admin <memory|index|workers> ...";
    pub(super) const ADMIN_MEMORY: &str = "admin memory <stats|purge>";
    pub(super) const ADMIN_MEMORY_STATS: &str = "admin memory stats";
    pub(super) const ADMIN_MEMORY_PURGE: &str = "admin memory purge [--force]";
    pub(super) const ADMIN_INDEX: &str = "admin index <name> <commit|evict-writer>";
    pub(super) const ADMIN_INDEX_COMMIT: &str = "admin index <name> commit";
    pub(super) const ADMIN_INDEX_EVICT: &str = "admin index <name> evict-writer";
    pub(super) const ADMIN_WORKERS: &str = "admin workers";
    pub(super) const CONNECT: &str = "connect <host[:port]>";
    pub(super) const KEY_FILE: &str = "key file <path>";
    pub(super) const KEY_INLINE: &str = "key <api-key>";
    pub(super) const KEY_MANAGE: &str = "key show | key clear";
    pub(super) const QUIT: &str = "exit | quit | \\q";
}

/// The lines `help` prints, in order — the same strings the dispatch arms quote.
const INTERACTIVE_USAGE: &[&str] = &[
    usage::HEALTH,
    usage::LIST_INDEXES,
    usage::LIST_INDEX,
    usage::SEARCH,
    usage::SCHEMA_DETECT,
    usage::SCHEMA_LOAD,
    usage::DATA_LOAD,
    usage::DELETE_INDEX,
    usage::DELETE_DOCS,
    usage::ADMIN_MEMORY_STATS,
    usage::ADMIN_MEMORY_PURGE,
    usage::ADMIN_INDEX_COMMIT,
    usage::ADMIN_INDEX_EVICT,
    usage::ADMIN_WORKERS,
    usage::CONNECT,
    usage::KEY_FILE,
    usage::KEY_INLINE,
    usage::KEY_MANAGE,
    usage::QUIT,
];

pub(crate) fn interactive_help() -> String {
    format!(
        "Available commands:\n  {}\n\nSupported source formats for schema/data commands:\n  \
         CSV, TSV, semicolon-delimited CSV, JSON object, JSON array, JSONL/NDJSON",
        INTERACTIVE_USAGE.join("\n  ")
    )
}

fn interactive_loop(
    mut session: InteractiveSession,
    history_path: PathBuf,
    handle: tokio::runtime::Handle,
) -> Result<()> {
    let completer = IndexCompleter::new(session.index_cache_handle());

    // Configure editor with platform-specific settings
    let config = if cfg!(target_os = "windows") {
        // Windows: use simpler config to avoid cursor positioning issues
        rustyline::Config::builder()
            .auto_add_history(true)
            .history_ignore_space(true)
            .completion_type(rustyline::CompletionType::List)
            .edit_mode(rustyline::EditMode::Emacs)
            .build()
    } else {
        // Unix/Linux/macOS: full featured config
        rustyline::Config::builder()
            .auto_add_history(true)
            .history_ignore_space(true)
            .completion_type(rustyline::CompletionType::List)
            .edit_mode(rustyline::EditMode::Emacs)
            .build()
    };

    let mut editor = Editor::with_config(config).context("Failed to initialize line editor")?;
    editor.set_helper(Some(completer));
    if history_path.exists() {
        let _ = editor.load_history(&history_path);
    }

    loop {
        let line = match editor.readline(&session.prompt()) {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => {
                println!();
                continue;
            }
            Err(ReadlineError::Eof) => break,
            Err(err) => return Err(anyhow!("Input error: {}", err)),
        };

        let input = line.trim().to_string();
        if input.is_empty() {
            continue;
        }

        if matches!(input.as_str(), "exit" | "quit" | "\\q") {
            break;
        }

        if matches!(input.as_str(), "help" | "\\h") {
            println!("{}", interactive_help());
            continue;
        }

        let _ = editor.add_history_entry(line.as_str());

        if let Err(err) = handle.block_on(dispatch_interactive_command(
            &mut session,
            &mut editor,
            &input,
        )) {
            eprintln!("⚠️  {}", err);
        }
    }

    editor
        .save_history(&history_path)
        .or_else(|_| editor.append_history(&history_path))
        .ok();

    Ok(())
}

pub(crate) fn history_file_path() -> Result<PathBuf> {
    let mut path = dirs::home_dir().context("Unable to determine home directory")?;
    path.push(".cameodb");
    fs::create_dir_all(&path).context("Failed to create CameoDB config directory")?;
    path.push("client_history");
    Ok(path)
}

pub(crate) async fn dispatch_interactive_command(
    session: &mut InteractiveSession,
    editor: &mut Editor<IndexCompleter, rustyline::history::DefaultHistory>,
    input: &str,
) -> Result<()> {
    let mut parts = input.split_whitespace();
    let command = parts.next().unwrap_or_default();

    match command {
        "health" => {
            let health = session.client().health().await?;
            print_json(&health)?;
        }
        "list" => {
            let resource = parts.next().unwrap_or("indexes");
            match resource {
                "indexes" => {
                    let remaining: Vec<&str> = parts.collect();
                    let extended = remaining.iter().any(|s| *s == "--extended" || *s == "-e");
                    let data_size = remaining.contains(&"--data-size");
                    if let Some(result) = handle_list_command(
                        session.client(),
                        ListResource::Indexes,
                        None,
                        data_size,
                        extended,
                    )
                    .await?
                    {
                        session.update_index_cache(&result).await;
                    }
                }
                "index" => {
                    let name = parts
                        .next()
                        .ok_or_else(|| anyhow!("Usage: {}", usage::LIST_INDEX))?;
                    let remaining: Vec<&str> = parts.collect();
                    let extended = remaining.iter().any(|s| *s == "--extended" || *s == "-e");
                    let data_size = remaining.contains(&"--data-size");
                    if let Some(result) = handle_list_command(
                        session.client(),
                        ListResource::Index,
                        Some(name.to_string()),
                        data_size,
                        extended,
                    )
                    .await?
                    {
                        session.update_index_cache(&result).await;
                    }
                }
                "--extended" | "-e" => {
                    let remaining: Vec<&str> = parts.collect();
                    let data_size = remaining.contains(&"--data-size");
                    if let Some(result) = handle_list_command(
                        session.client(),
                        ListResource::Indexes,
                        None,
                        data_size,
                        true,
                    )
                    .await?
                    {
                        session.update_index_cache(&result).await;
                    }
                }
                other => {
                    return Err(anyhow!(
                        "Unknown list target '{}'. Use 'list indexes' or 'list index <name>'.",
                        other
                    ));
                }
            }
        }
        "search" => {
            let index = parts
                .next()
                .ok_or_else(|| anyhow!("Usage: {}", usage::SEARCH))?;
            let query: Vec<&str> = parts.collect();
            let query = query.join(" ");
            if query.trim().is_empty() {
                return Err(anyhow!("Usage: {}", usage::SEARCH));
            }

            // Inline `return`, `limit` and `sort` are read by the server, which owns the one
            // definition of where a modifier run may appear.
            let results = session
                .client()
                .search(index, &query, None, None, None, None)
                .await?;
            print_json(&results)?;
        }
        "schema" => {
            let sub = parts
                .next()
                .ok_or_else(|| anyhow!("Usage: {}", usage::SCHEMA))?;

            match sub {
                "detect" => {
                    let remaining: Vec<&str> = parts.collect();
                    let (delimiter, positional) = parse_delimiter_arg(&remaining)?;
                    let file = positional
                        .first()
                        .copied()
                        .ok_or_else(|| anyhow!("Usage: {}", usage::SCHEMA_DETECT))?;

                    let schema_json =
                        detect_schema_from_source(session.client(), file, delimiter).await?;
                    print_json(&schema_json)?;
                }
                "load" => {
                    let remaining: Vec<&str> = parts.collect();
                    let (delimiter, positional) = parse_delimiter_arg(&remaining)?;
                    let index = positional
                        .first()
                        .copied()
                        .ok_or_else(|| anyhow!("Usage: {}", usage::SCHEMA_LOAD))?;
                    let file = positional
                        .get(1)
                        .copied()
                        .ok_or_else(|| anyhow!("Usage: {}", usage::SCHEMA_LOAD))?;

                    let schema_json =
                        load_schema_from_source(session.client(), file, delimiter).await?;
                    session
                        .client()
                        .put_index_config(index, &schema_json)
                        .await?;
                    println!("Schema applied to index '{}'", index);
                    session.refresh_index_cache().await;
                }
                other => {
                    return Err(anyhow!(
                        "Unknown schema operation '{}'. Use 'schema detect' or 'schema load'.",
                        other
                    ));
                }
            }
        }
        "data" => {
            let sub = parts
                .next()
                .ok_or_else(|| anyhow!("Usage: {}", usage::DATA_LOAD))?;

            match sub {
                "load" => {
                    let remaining: Vec<&str> = parts.collect();
                    let (delimiter, positional_after_delim) = parse_delimiter_arg(&remaining)?;
                    let (batch_size, positional) =
                        parse_batch_size_arg(&positional_after_delim, DEFAULT_BATCH_SIZE)?;

                    let index = positional
                        .first()
                        .copied()
                        .ok_or_else(|| anyhow!("Usage: {}", usage::DATA_LOAD))?;
                    let file = positional
                        .get(1)
                        .copied()
                        .ok_or_else(|| anyhow!("Usage: {}", usage::DATA_LOAD))?;

                    load_data_from_source(session.client(), index, file, delimiter, batch_size)
                        .await?;
                }
                other => {
                    return Err(anyhow!(
                        "Unknown data operation '{}'. Use 'data load'.",
                        other
                    ));
                }
            }
        }
        "delete" => {
            let index = parts.next().ok_or_else(|| {
                anyhow!("Usage: {} | {}", usage::DELETE_INDEX, usage::DELETE_DOCS)
            })?;

            // Flags, in one pass: --id, --ids-file and --routing-key take values, the rest are
            // switches. The `--id` values are kept raw and handed to `collect_delete_ids`, which
            // is what splits them and what decides whether any ids were named at all — the same
            // function the command line uses, so neither the comma rule nor the refusal below
            // can drift between the two entry points.
            let rest: Vec<&str> = parts.collect();
            let mut raw_ids: Vec<String> = Vec::new();
            let mut ids_file: Option<String> = None;
            let mut routing_key: Option<String> = None;
            let mut delete_schema = false;
            let mut cursor = rest.iter();
            while let Some(token) = cursor.next() {
                match *token {
                    "--delete-schema" => delete_schema = true,
                    "--id" => {
                        let value = cursor
                            .next()
                            .ok_or_else(|| anyhow!("--id needs a document id"))?;
                        raw_ids.push((*value).to_string());
                    }
                    "--ids-file" => {
                        let value = cursor
                            .next()
                            .ok_or_else(|| anyhow!("--ids-file needs a path"))?;
                        ids_file = Some((*value).to_string());
                    }
                    "--routing-key" => {
                        let value = cursor
                            .next()
                            .ok_or_else(|| anyhow!("--routing-key needs a value"))?;
                        routing_key = Some((*value).to_string());
                    }
                    other => return Err(anyhow!("Unknown option '{}' for delete", other)),
                }
            }

            // Naming documents deletes those; naming none at all still means the index, which is
            // what this command has always meant and what the confirmation below is guarding.
            if let Some(named) = collect_delete_ids(&raw_ids, ids_file.as_deref())? {
                if delete_schema {
                    return Err(anyhow!(
                        "--delete-schema deletes the index, which is not what naming documents \
                         asks for; drop one of the two"
                    ));
                }
                let result =
                    delete_named_documents(session.client(), index, &named, routing_key.as_deref())
                        .await?;
                print_json(&result)?;
                return Ok(());
            }

            // Use rustyline for confirmation to avoid stdin conflicts in interactive mode
            let prompt = format!("Delete index \"{}\"? [yes/NO]: ", index);
            match editor.readline(&prompt) {
                Ok(answer) => {
                    let confirmed = answer.trim().eq_ignore_ascii_case("yes");
                    if !confirmed {
                        println!("Aborted delete.");
                        return Ok(());
                    }
                }
                Err(ReadlineError::Interrupted | ReadlineError::Eof) => {
                    println!("\nAborted delete.");
                    return Ok(());
                }
                Err(err) => return Err(anyhow!("Failed to read confirmation: {}", err)),
            }

            let result = session.client().delete_index(index, delete_schema).await?;
            print_json(&result)?;
            session.refresh_index_cache().await;
        }
        "key" => {
            let rest = parts.collect::<Vec<_>>().join(" ");
            let (subcommand, argument) = match rest.split_once(char::is_whitespace) {
                Some((head, tail)) => (head.trim(), tail.trim()),
                None => (rest.trim(), ""),
            };
            match subcommand {
                "" | "show" => match session.key_id() {
                    Some(key_id) => println!("🔑 Using key {key_id} for {}", session.origin()),
                    None => println!("No key in use for {}", session.origin()),
                },
                "clear" => {
                    session.set_credential(None)?;
                    println!("Key cleared.");
                }
                "file" => {
                    if argument.is_empty() {
                        return Err(anyhow!("Usage: {}", usage::KEY_FILE));
                    }
                    let credential = Credential::from_file(Path::new(argument))?;
                    let key_id = credential.key_id();
                    session.set_credential(Some(credential))?;
                    println!("🔑 Using key {key_id} for {}", session.origin());
                    session.refresh_index_cache().await;
                }
                // The key itself, typed at a prompt. It lands in the shell history file, which
                // is why `key file <path>` is offered first in the help text.
                candidate => {
                    let credential = Credential::parse(candidate)?;
                    let key_id = credential.key_id();
                    session.set_credential(Some(credential))?;
                    println!("🔑 Using key {key_id} for {}", session.origin());
                    session.refresh_index_cache().await;
                }
            }
        }
        "connect" | "conn" => {
            let target = parts.collect::<Vec<_>>().join(" ");
            if target.is_empty() {
                return Err(anyhow!("Usage: {}", usage::CONNECT));
            }
            session.reconnect(&target)?;
            println!("Connected to {}", session.display_host());
            session.refresh_index_cache().await;
        }
        "admin" => {
            let subcommand = parts
                .next()
                .ok_or_else(|| anyhow!("Usage: {}", usage::ADMIN))?;
            match subcommand {
                "memory" => {
                    let op = parts
                        .next()
                        .ok_or_else(|| anyhow!("Usage: {}", usage::ADMIN_MEMORY))?;
                    match op {
                        "stats" => {
                            let result = session.client().admin_memory_stats().await?;
                            print_json(&result)?;
                        }
                        "purge" => {
                            let force = parts.any(|p| p == "--force");
                            let result = session.client().admin_memory_purge(force).await?;
                            print_json(&result)?;
                        }
                        other => {
                            return Err(anyhow!(
                                "Unknown memory operation '{}'. Use 'stats' or 'purge'.",
                                other
                            ));
                        }
                    }
                }
                "index" => {
                    let index = parts
                        .next()
                        .ok_or_else(|| anyhow!("Usage: {}", usage::ADMIN_INDEX))?;
                    let op = parts
                        .next()
                        .ok_or_else(|| anyhow!("Usage: {}", usage::ADMIN_INDEX))?;
                    match op {
                        "commit" => {
                            let result = session.client().admin_index_commit(index).await?;
                            print_json(&result)?;
                        }
                        "evict-writer" => {
                            let result = session.client().admin_index_evict_writer(index).await?;
                            print_json(&result)?;
                        }
                        other => {
                            return Err(anyhow!(
                                "Unknown index operation '{}'. Use 'commit' or 'evict-writer'.",
                                other
                            ));
                        }
                    }
                }
                "workers" => {
                    let result = session.client().admin_worker_stats().await?;
                    print_json(&result)?;
                }
                other => {
                    return Err(anyhow!(
                        "Unknown admin subcommand '{}'. Use 'memory', 'index', or 'workers'.",
                        other
                    ));
                }
            }
        }
        other => {
            return Err(anyhow!(
                "Unknown command '{}'. Type 'help' for the supported commands.",
                other
            ));
        }
    }

    Ok(())
}
