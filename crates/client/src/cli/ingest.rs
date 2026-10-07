//! The ingest pipeline: source and compression detection, the JSON stream parsers,
//! schema detection, and the CSV/JSON loaders behind `schema` and `data` commands.

use super::*;
use crate::sdk::CameoClient;
use anyhow::{Context, Result, anyhow};
use flate2::read::GzDecoder;
use reqwest::Url;
use serde_json::Map as JsonMap;
use serde_json::Value as JsonValue;
use serde_json::json;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::io::{BufRead, BufReader, Cursor, Read};
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use storage::{FieldDef, IndexSchema, TantivyFieldType};

// Only import colored on non-Windows platforms

pub(crate) const DEFAULT_BATCH_SIZE: usize = 4000;
pub(crate) const SOURCE_SNIFF_BYTES: usize = 64 * 1024;

/// Simple progress spinner for long-running operations
pub(crate) struct ProgressSpinner {
    pub(crate) active: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) handle: Option<thread::JoinHandle<()>>,
}

impl ProgressSpinner {
    /// A spinner on stderr, and only when stderr is a terminal. On stdout its frames led the
    /// output, so `schema detect data.csv > schema.json` saved a file `schema load` could not read.
    pub(crate) fn new() -> Self {
        use std::io::IsTerminal;
        let active = Arc::new(std::sync::atomic::AtomicBool::new(true));
        if !std::io::stderr().is_terminal() {
            return Self {
                active,
                handle: None,
            };
        }
        let active_clone = active.clone();

        let handle = thread::spawn(move || {
            // Use different spinner characters based on platform
            let spinner_chars: Vec<char> = if cfg!(target_os = "windows") {
                // Windows PowerShell/Command Prompt compatible characters
                vec!['|', '/', '-', '\\']
            } else {
                // Unix-like systems - Unicode braille characters
                vec!['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
            };
            let mut i = 0;

            while active_clone.load(std::sync::atomic::Ordering::Relaxed) {
                eprint!("\r{} ", spinner_chars[i % spinner_chars.len()]);
                std::io::Write::flush(&mut std::io::stderr()).ok();
                i += 1;
                // Use slightly slower timing on Windows for better visibility
                let sleep_ms = if cfg!(target_os = "windows") {
                    150
                } else {
                    100
                };
                thread::sleep(Duration::from_millis(sleep_ms));
            }
            // Clear the spinner character when done, leaving cursor at start of line
            eprint!("\r");
            std::io::Write::flush(&mut std::io::stderr()).ok();
        });

        Self {
            active,
            handle: Some(handle),
        }
    }

    pub(crate) fn stop(&mut self) {
        // Signal the thread to stop
        self.active
            .store(false, std::sync::atomic::Ordering::Relaxed);

        // Wait for the thread to finish
        if let Some(handle) = self.handle.take() {
            handle.join().ok();
        }
    }
}

impl Drop for ProgressSpinner {
    fn drop(&mut self) {
        self.stop();
    }
}

pub(crate) fn parse_header_with_hint(raw: &str) -> (String, Option<TantivyFieldType>) {
    let mut parts = raw.splitn(2, '.');
    let name = parts.next().unwrap_or("").to_string();
    let hint = parts.next().and_then(map_type_hint);
    (name, hint)
}

pub(crate) fn map_type_hint(hint: &str) -> Option<TantivyFieldType> {
    match hint.to_lowercase().as_str() {
        "text" | "string" => Some(TantivyFieldType::Text),
        // No dedicated Exact variant; use String (untokenized) for exact semantics
        "exact" => Some(TantivyFieldType::String),
        "numeric" | "number" | "int" | "i64" | "integer" | "u64" => Some(TantivyFieldType::I64),
        "decimal" | "float" | "double" | "f64" => Some(TantivyFieldType::F64),
        "date" => Some(TantivyFieldType::Date),
        "timestamp" => Some(TantivyFieldType::Date),
        "bool" | "boolean" | "true" | "false" => Some(TantivyFieldType::Boolean),
        "ip" => Some(TantivyFieldType::Ip),
        "json" => Some(TantivyFieldType::Json),
        _ => None,
    }
}

pub(crate) fn parse_delimiter_arg<'a>(args: &'a [&'a str]) -> Result<(Delimiter, Vec<&'a str>)> {
    let mut delimiter = Delimiter::Detect;
    let mut remaining = Vec::new();
    let mut iter = args.iter().peekable();

    while let Some(&arg) = iter.next() {
        if arg == "--delimiter" {
            let value = iter
                .next()
                .copied()
                .ok_or_else(|| anyhow!("Missing value for --delimiter"))?;
            delimiter = match value {
                "detect" => Delimiter::Detect,
                "comma" => Delimiter::Comma,
                "tab" => Delimiter::Tab,
                "semicolon" => Delimiter::Semicolon,
                other => {
                    return Err(anyhow!(
                        "Invalid delimiter '{}'. Use detect|comma|tab|semicolon.",
                        other
                    ));
                }
            };
        } else {
            remaining.push(arg);
        }
    }

    Ok((delimiter, remaining))
}

pub(crate) fn parse_batch_size_arg<'a>(
    args: &'a [&'a str],
    default: usize,
) -> Result<(usize, Vec<&'a str>)> {
    let mut batch_size = default;
    let mut remaining = Vec::new();
    let mut iter = args.iter().peekable();

    while let Some(&arg) = iter.next() {
        if arg == "--batch-size" {
            let value = iter
                .next()
                .copied()
                .ok_or_else(|| anyhow!("Missing value for --batch-size"))?;
            batch_size = value
                .parse::<usize>()
                .map_err(|_| anyhow!("Invalid batch size '{}': expected number", value))?;
        } else {
            remaining.push(arg);
        }
    }

    Ok((batch_size, remaining))
}

/// `--parallel <n>`, and the arguments left: 1 unless given, and refused outside 1 to
/// [`MAX_PARALLEL`].
pub(crate) fn parse_parallel_arg<'a>(args: &'a [&'a str]) -> Result<(usize, Vec<&'a str>)> {
    let mut parallel = 1;
    let mut remaining = Vec::new();
    let mut iter = args.iter();
    while let Some(&arg) = iter.next() {
        if arg == "--parallel" {
            let value = iter
                .next()
                .copied()
                .ok_or_else(|| anyhow!("Missing value for --parallel"))?;
            parallel = check_parallel(
                value
                    .parse::<usize>()
                    .map_err(|_| anyhow!("Invalid --parallel '{}': expected number", value))?,
            )?;
        } else {
            remaining.push(arg);
        }
    }
    Ok((parallel, remaining))
}

/// `--parallel`, refused outside 1 to [`MAX_PARALLEL`].
pub(crate) fn check_parallel(parallel: usize) -> Result<usize> {
    if (1..=MAX_PARALLEL).contains(&parallel) {
        Ok(parallel)
    } else {
        Err(anyhow!(
            "--parallel takes 1 to {MAX_PARALLEL}: half of the requests a node takes at once, \
             so a load leaves room for searches"
        ))
    }
}

/// How a load sends its rows: how many to a batch, and how many batches at once.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LoadPace {
    pub(crate) batch_size: usize,
    pub(crate) parallel: usize,
}

/// `--id <COLUMN[,COLUMN...]>`, and the arguments left.
pub(crate) fn parse_id_arg<'a>(args: &'a [&'a str]) -> Result<(Option<IdSpec>, Vec<&'a str>)> {
    let mut id = None;
    let mut remaining = Vec::new();
    let mut iter = args.iter();
    while let Some(&arg) = iter.next() {
        if arg == "--id" {
            let value = iter
                .next()
                .copied()
                .ok_or_else(|| anyhow!("Missing value for --id"))?;
            id = Some(IdSpec::parse(value)?);
        } else {
            remaining.push(arg);
        }
    }
    Ok((id, remaining))
}

/// Whether a bare flag is among the arguments, and the arguments without it.
pub(crate) fn take_flag<'a>(args: &[&'a str], flag: &str) -> (bool, Vec<&'a str>) {
    let present = args.contains(&flag);
    (
        present,
        args.iter().copied().filter(|a| *a != flag).collect(),
    )
}

/// Everything a scan settles about a source before a schema is built or a row is sent.
pub(crate) struct SourceAnalysis {
    pub(crate) format: SourceFormat,
    /// The source's bytes, for the load to read again from the start; `None` for a remote JSON
    /// source, which is streamed instead.
    pub(crate) data: Option<SourceData>,
    /// The CSV delimiter; `None` for JSON.
    pub(crate) delimiter: Option<u8>,
    /// The CSV header with its type hints; empty for JSON.
    pub(crate) headers: Vec<(String, Option<TantivyFieldType>)>,
    pub(crate) profiler: Profiler,
    pub(crate) summary: ScanSummary,
    /// The field each column becomes, in the profiler's column order. A header hint wins.
    pub(crate) choices: Vec<FieldChoice>,
    pub(crate) id: IdChoice,
}

impl SourceAnalysis {
    pub(crate) fn new(
        format: SourceFormat,
        data: Option<SourceData>,
        delimiter: Option<u8>,
        headers: Vec<(String, Option<TantivyFieldType>)>,
        scan: (Profiler, ScanSummary),
        ids: &IdOptions<'_>,
    ) -> Result<Self> {
        let (profiler, summary) = scan;
        let mut choices = profiler.choices();
        for (choice, (_, hint)) in choices.iter_mut().zip(&headers) {
            if let Some(hint) = hint {
                choice.field_type = hint.clone();
                choice.category = false;
                choice.tokenizer = None;
                choice.note = Some("declared in the header".to_string());
            }
        }
        let id = profiler.choose_id(ids.explicit, ids.recorded)?;
        Ok(Self {
            format,
            data,
            delimiter,
            headers,
            profiler,
            summary,
            choices,
            id,
        })
    }

    /// The shadow field that keeps a single id column's name: a CSV's `ID` and `SHA256`
    /// lowercased, any other name as written. `None` for the column named `id`, and for a
    /// composite id — its columns stay fields of their own, returned apart, not as the id.
    pub(crate) fn shadow_name(&self) -> Option<String> {
        let name = self.id.spec.single()?;
        let shadow = if self.delimiter.is_some() && is_canonical_id_name(name) {
            name.to_lowercase()
        } else {
            name.to_string()
        };
        (shadow != "id").then_some(shadow)
    }

    /// Each column's position among the profiler's columns, for the id.
    pub(crate) fn id_columns(&self) -> Vec<usize> {
        self.id
            .spec
            .columns()
            .iter()
            .map(|name| {
                self.profiler
                    .index_of(name)
                    .expect("the id names columns the scan saw")
            })
            .collect()
    }

    /// How the load reads each column: the type the index's field declares, the date order the
    /// scan found, and whether its values are lists.
    pub(crate) fn shapes(
        &self,
        field_types: &HashMap<String, TantivyFieldType>,
    ) -> Vec<ColumnShape> {
        self.profiler
            .columns
            .iter()
            .zip(&self.choices)
            .map(|(column, choice)| {
                let field_type = field_types.get(&column.name).cloned();
                let dates = match field_type {
                    Some(TantivyFieldType::Date) => choice.dates,
                    _ => DateOrders::default(),
                };
                ColumnShape {
                    field_type,
                    dates,
                    list: choice.list,
                }
            })
            .collect()
    }

    pub(crate) fn report(&self, source: &str) -> String {
        render_report(source, &self.summary, &self.profiler, &self.id)
    }
}

/// How the id is to be chosen: the columns `--id` named, and the id the index already records.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct IdOptions<'a> {
    pub(crate) explicit: Option<&'a IdSpec>,
    pub(crate) recorded: Option<&'a IdSpec>,
}

impl IdOptions<'_> {
    /// The id the scan should follow as the load will compose it, when one is named.
    fn followed(&self) -> Option<IdSpec> {
        self.explicit.or(self.recorded).cloned()
    }
}

/// Scan a source within `limits` and settle what it holds. See [`scan`](super::scan) for how much
/// is read.
pub(crate) async fn analyze_source(
    client: &CameoClient,
    source: &str,
    delimiter: Delimiter,
    ids: IdOptions<'_>,
    limits: ScanLimits,
) -> Result<SourceAnalysis> {
    let format = detect_source_format_for_source(client, source).await?;
    match format {
        SourceFormat::CsvLike => {
            let data = SourceData::open(client, source).await?;
            let delimiter = delimiter_byte(delimiter, &data)?;
            let named = ids.followed();
            let (scan, data) = tokio::task::spawn_blocking(move || {
                scan_csv(&data, delimiter, &limits, named.as_ref()).map(|scan| (scan, data))
            })
            .await
            .map_err(|err| anyhow!("CSV scan failed: {err}"))??;
            SourceAnalysis::new(
                format,
                Some(data),
                Some(delimiter),
                scan.headers,
                (scan.profiler, scan.summary),
                &ids,
            )
        }
        SourceFormat::JsonDocument | SourceFormat::JsonArray | SourceFormat::JsonLines => {
            if is_http_source(source) && detect_compression(source) == Compression::None {
                let scan = scan_http_json(client, source, format, &limits, ids.followed().as_ref())
                    .await?;
                return SourceAnalysis::new(
                    format,
                    None,
                    None,
                    Vec::new(),
                    (scan.profiler, scan.summary),
                    &ids,
                );
            }
            let data = SourceData::open(client, source).await?;
            let named = ids.followed();
            let (scan, data) = tokio::task::spawn_blocking(move || {
                scan_json(&data, format, &limits, named.as_ref()).map(|scan| (scan, data))
            })
            .await
            .map_err(|err| anyhow!("JSON scan failed: {err}"))??;
            SourceAnalysis::new(
                format,
                Some(data),
                None,
                Vec::new(),
                (scan.profiler, scan.summary),
                &ids,
            )
        }
        SourceFormat::SchemaJson => Err(anyhow!(
            "Source is a schema, not data: there is nothing to scan"
        )),
    }
}

/// The schema a scan describes: `id` first, then a field per column in source order, each typed
/// by what all its scanned values fit. A single id column not named `id` becomes a shadow field
/// keeping its name; a composite id's columns stay fields of their own.
pub(crate) fn schema_from_analysis(analysis: &SourceAnalysis) -> Result<JsonValue> {
    let mut schema = IndexSchema::default();
    let columns = &analysis.profiler.columns;
    if let Some(shadow) = analysis.shadow_name() {
        let idx = analysis.id_columns()[0];
        let field_type = match analysis.delimiter {
            Some(_) => analysis.headers[idx].1.clone(),
            None => Some(analysis.choices[idx].field_type.clone()),
        };
        schema.add_shadow_field(shadow, field_type.unwrap_or(TantivyFieldType::Text));
    } else if analysis.id.reason == IdReason::Named && analysis.profiler.rows > 0 {
        // The source has its own `id`: an id-like column that held the same value in every row
        // is that id under its other name, and is kept as its shadow.
        if let Some(column) = columns
            .iter()
            .filter(|c| c.equals_id && !c.name.eq_ignore_ascii_case("id"))
            .filter(|c| is_id_like_name(&c.name))
            .min_by_key(|c| id_name_rank(&c.name))
        {
            schema.add_shadow_field(column.name.clone(), TantivyFieldType::Text);
        }
    }

    for (column, choice) in columns.iter().zip(&analysis.choices) {
        if column.name == "id" || schema.fields.contains_key(&column.name) {
            continue;
        }
        let mut field = FieldDef::new(column.name.clone(), choice.field_type.clone());
        if choice.category {
            // A category is filtered and grouped on: give it the column that takes.
            field.fast = Some(true);
        }
        if let Some(tokenizer) = choice.tokenizer {
            // One term per value: no positions to keep.
            field.tokenizer = Some(tokenizer.to_string());
            field.index_record_option = Some("Basic".to_string());
        }
        schema.fields.insert(column.name.clone(), field);
    }

    // Recorded so every later load keys documents the same way, unless told otherwise.
    schema.id_fields = analysis
        .id
        .spec
        .columns()
        .into_iter()
        .map(str::to_string)
        .collect();
    schema.fields.insert(
        "id".to_string(),
        FieldDef {
            name: "id".to_string(),
            field_type: TantivyFieldType::Text,
            indexed: true,
            stored: true,
            fast: Some(false),
            is_shadow: false,
            description: None,
            tokenizer: Some("raw".to_string()),
            index_record_option: Some("Basic".to_string()),
            learned: false,
        },
    );
    schema.auto_detect_routing_field();

    let mut schema_json = serde_json::to_value(schema).context("Failed to serialize schema")?;
    // The fields map in source order, `id` first, so the schema reads like the source.
    if let JsonValue::Object(ref mut root) = schema_json
        && let Some(JsonValue::Object(mut fields)) = root.remove("fields")
    {
        let mut ordered = JsonMap::new();
        if let Some(id) = fields.remove("id") {
            ordered.insert("id".to_string(), id);
        }
        if let Some(shadow) = analysis.shadow_name()
            && let Some(field) = fields.remove(&shadow)
        {
            ordered.insert(shadow, field);
        }
        for column in columns {
            if let Some(field) = fields.remove(&column.name) {
                ordered.insert(column.name.clone(), field);
            }
        }
        ordered.extend(fields);
        root.insert("fields".to_string(), JsonValue::Object(ordered));
    }
    Ok(schema_json)
}

/// The schema for loading an index by another id: the one the scan describes for that id, with
/// each field the index already declares kept as declared — a type, tokenizer, fast column or
/// description a person chose survives the change of key. Fields the index declares and the
/// source lacks are kept too. Only the id's shadow field is decided anew.
pub(crate) fn schema_for_new_id(
    analysis: &SourceAnalysis,
    existing: &ExistingSchema,
) -> Result<JsonValue> {
    let mut schema = schema_from_analysis(analysis)?;
    let fields = schema
        .get_mut("fields")
        .and_then(JsonValue::as_object_mut)
        .ok_or_else(|| anyhow!("A built schema has fields"))?;
    for declared in &existing.fields {
        let Some(name) = declared.get("name").and_then(JsonValue::as_str) else {
            continue;
        };
        if name == "id" || declared.get("shadow").and_then(JsonValue::as_bool) == Some(true) {
            continue;
        }
        if fields
            .get(name)
            .is_some_and(|f| f.get("is_shadow").and_then(JsonValue::as_bool) == Some(true))
        {
            continue;
        }
        let Some(field_type) = declared
            .get("type")
            .and_then(|t| serde_json::from_value::<TantivyFieldType>(t.clone()).ok())
        else {
            continue;
        };
        let mut field = FieldDef::new(name.to_string(), field_type);
        field.indexed = declared
            .get("indexed")
            .and_then(JsonValue::as_bool)
            .unwrap_or(true);
        field.fast = declared.get("fast").and_then(JsonValue::as_bool);
        field.tokenizer = declared
            .get("tokenizer")
            .and_then(JsonValue::as_str)
            .map(str::to_string);
        if field.tokenizer.as_deref() == Some("raw") {
            field.index_record_option = Some("Basic".to_string());
        }
        field.description = declared
            .get("description")
            .and_then(JsonValue::as_str)
            .map(str::to_string);
        fields.insert(
            name.to_string(),
            serde_json::to_value(field).context("Failed to serialize a field")?,
        );
    }
    if let Some(description) = &existing.description {
        schema["description"] = JsonValue::String(description.clone());
    }
    Ok(schema)
}

/// A row's id from its id columns' values: the one value, or each joined with `|` in the order
/// `--id` named them. `None` when any of them has no value.
pub(crate) fn compose_id(parts: impl IntoIterator<Item = Option<String>>) -> Option<String> {
    let parts: Option<Vec<String>> = parts.into_iter().collect();
    Some(parts?.join(ID_SEPARATOR))
}

/// What a load did with ids: the rows that repeated one already sent, and the rows that had
/// none and were skipped. A repeat replaces the document before it, so a source whose id is not
/// unique loads fewer documents than it has rows, with nothing refused to say so.
#[derive(Debug, Default)]
pub(crate) struct IdLedger {
    seen: HashSet<u64>,
    pub(crate) repeats: u64,
    first_repeat: Option<(Location, String)>,
    pub(crate) skipped: u64,
    first_skip: Option<Location>,
}

impl IdLedger {
    /// Record an id the load sends; `true` when it was sent before, so this row replaces it.
    pub(crate) fn admit(&mut self, id: &str, at: Location) -> bool {
        if self.seen.insert(hash_text(id)) {
            return false;
        }
        self.repeats += 1;
        self.first_repeat
            .get_or_insert_with(|| (at, id.to_string()));
        true
    }

    pub(crate) fn skip(&mut self, at: Location) {
        self.skipped += 1;
        self.first_skip.get_or_insert(at);
    }

    /// Say what the load could not keep. `candidate` is a unique key the scan found, offered as
    /// a suggestion; the id stays as it was named.
    /// An id named with `--id` repeats on purpose, so its replacements are information; an id
    /// the loader fell back to repeats by accident, so they are a warning with a better key.
    pub(crate) fn report(&self, id: &IdSpec, explicit: bool, candidate: Option<&str>) {
        let columns = id.columns().join(",");
        if let Some(at) = self.first_skip {
            eprintln!(
                "⚠️  {} rows had no value in the id ({columns}) and were skipped, the first at {at}.",
                grouped(self.skipped)
            );
        }
        if let Some((at, value)) = &self.first_repeat
            && explicit
        {
            eprintln!(
                "ℹ️  {} rows replaced a document an earlier row with the same id ({columns}) made, \
                 the first at {at} ('{value}').",
                grouped(self.repeats)
            );
        } else if let Some((at, value)) = &self.first_repeat {
            eprintln!(
                "⚠️  {} rows repeated an id already loaded, each replacing the document before it — \
                 the first at {at} ('{value}'). The id ({columns}) is not unique in this source.{}",
                grouped(self.repeats),
                candidate
                    .filter(|c| *c != columns)
                    .map(|c| format!(" Detected unique key candidate: --id {c}"))
                    .unwrap_or_default()
            );
        }
    }
}

/// Serialize one CSV record as the NDJSON payload line the ingest stream accepts: the row, its
/// id, and the id as its routing key.
pub(crate) fn csv_ndjson_line(
    record: &csv::StringRecord,
    headers: &[(String, Option<TantivyFieldType>)],
    columns: &[ColumnShape],
    id: &str,
) -> Result<Vec<u8>> {
    let unknown = ColumnShape::default();
    let mut doc_obj: JsonMap<String, JsonValue> = JsonMap::new();
    for (idx, value) in record.iter().enumerate() {
        if let Some((header, _)) = headers.get(idx) {
            doc_obj.insert(
                header.clone(),
                csv_cell(value, columns.get(idx).unwrap_or(&unknown)),
            );
        }
    }
    doc_obj.insert("id".to_string(), JsonValue::String(id.to_string()));
    let payload = json!({"id": id, "routing_key": id, "doc": doc_obj});
    let mut line = serde_json::to_vec(&payload).context("Failed to serialize CSV payload")?;
    line.push(b'\n');
    Ok(line)
}

/// The CSV loader's batch state: payload serialization, accumulation, and the flush that ships
/// a full batch.
pub(crate) struct CsvIngest {
    pub(crate) headers: Arc<Vec<(String, Option<TantivyFieldType>)>>,
    /// How each column's cells are read, settled by the scan before the first row is sent.
    pub(crate) columns: Arc<Vec<ColumnShape>>,
    /// The columns the id is made of.
    pub(crate) id_columns: Vec<usize>,
    pub(crate) batch_size: usize,
    /// The rows of the batch being gathered, each with its id. Turned into the request body by
    /// the batch's own task, so with `--parallel` the conversion runs beside the reading.
    rows: Vec<(csv::StringRecord, String)>,
    /// The file line each row in `rows` was read from, in order. See [`SourceLines`].
    batch_lines: Vec<u64>,
    /// A row in this batch repeats an id sent before. See [`BatchSender::send`].
    batch_repeats: bool,
    pub(crate) ledger: IdLedger,
}

impl CsvIngest {
    pub(crate) fn new(
        headers: Vec<(String, Option<TantivyFieldType>)>,
        columns: Vec<ColumnShape>,
        id_columns: Vec<usize>,
        batch_size: usize,
    ) -> Self {
        Self {
            headers: Arc::new(headers),
            columns: Arc::new(columns),
            id_columns,
            batch_size: batch_size.max(1),
            rows: Vec::new(),
            batch_lines: Vec::new(),
            batch_repeats: false,
            ledger: IdLedger::default(),
        }
    }

    /// Queue one row, read from file line `line`. A row with no id is skipped and counted.
    pub(crate) async fn push_row(
        &mut self,
        sender: &mut BatchSender,
        record: &csv::StringRecord,
        line: u64,
    ) -> Result<()> {
        let id = compose_id(self.id_columns.iter().map(|&idx| {
            record
                .get(idx)
                .map(str::trim)
                .filter(|cell| !cell.is_empty())
                .map(str::to_string)
        }));
        let Some(id) = id else {
            self.ledger.skip(Location::Line(line));
            return Ok(());
        };
        self.batch_repeats |= self.ledger.admit(&id, Location::Line(line));
        self.rows.push((record.clone(), id));
        self.batch_lines.push(line);
        if self.rows.len() >= self.batch_size {
            self.flush(sender).await?;
        }
        Ok(())
    }

    /// Hand the gathered rows to `sender` as one batch.
    pub(crate) async fn flush(&mut self, sender: &mut BatchSender) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        let headers = Arc::clone(&self.headers);
        let columns = Arc::clone(&self.columns);
        sender
            .send(
                SourceLines::File(std::mem::take(&mut self.batch_lines)),
                std::mem::take(&mut self.batch_repeats),
                move || {
                    let mut body = Vec::new();
                    for (record, id) in &rows {
                        body.extend_from_slice(&csv_ndjson_line(record, &headers, &columns, id)?);
                    }
                    Ok(body)
                },
            )
            .await
    }
}

/// The most batches `--parallel` sends at once: half of the 32 requests a node takes at once in
/// the shipped configuration, so a load leaves the other half to searches and other clients.
pub(crate) const MAX_PARALLEL: usize = 16;

/// Sends a load's batches, up to `parallel` at once, and adds up what the node made of them.
///
/// One at a time, the reader and the node took turns: the node idled while the next batch was
/// read and converted, and the reader while the node indexed the last. Measured on a 3-node
/// cluster with 4,000-row batches, 4 batches at once loaded 2.4× as fast as one, and 8 at once
/// 3.3×; a larger batch alone gained 5%.
///
/// Each batch is built in its own task — the conversion on a blocking thread — and sent from
/// it, so both the converting and the waiting run side by side. With `parallel` at 1 the
/// requests still go one at a time and in order; only the conversion of a batch overlaps with
/// reading the next.
pub(crate) struct BatchSender {
    client: CameoClient,
    index: String,
    parallel: usize,
    in_flight: tokio::task::JoinSet<Result<(JsonValue, SourceLines)>>,
    pub(crate) total_sent: usize,
    pub(crate) total_failed: usize,
}

impl BatchSender {
    pub(crate) fn new(client: &CameoClient, index: &str, parallel: usize) -> Self {
        Self {
            client: client.clone(),
            index: index.to_string(),
            parallel: parallel.clamp(1, MAX_PARALLEL),
            in_flight: tokio::task::JoinSet::new(),
            total_sent: 0,
            total_failed: 0,
        }
    }

    /// Send one batch: `build` makes its NDJSON body, and `lines` say where in the source each
    /// of its documents came from.
    ///
    /// `repeats` says a document in it has an id sent before. Batches in flight together can
    /// land in any order, and a later row is meant to replace an earlier one with its id — the
    /// upsert a load by `--id` relies on — so such a batch waits for every batch ahead of it to
    /// land first. A source with no repeated ids never waits.
    pub(crate) async fn send(
        &mut self,
        lines: SourceLines,
        repeats: bool,
        build: impl FnOnce() -> Result<Vec<u8>> + Send + 'static,
    ) -> Result<()> {
        if repeats {
            self.finish().await?;
        }
        while self.in_flight.len() >= self.parallel {
            self.settle_one().await?;
        }
        let client = self.client.clone();
        let index = self.index.clone();
        self.in_flight.spawn(async move {
            let body = tokio::task::spawn_blocking(build)
                .await
                .map_err(|e| anyhow!("Building a batch failed: {e}"))??;
            let response = client.stream_index_ndjson(&index, body).await?;
            Ok((response, lines))
        });
        Ok(())
    }

    /// Wait for one batch in flight and count what the node said of it. A batch the node could
    /// not take at all stops the load, as it did one batch at a time; the batches still in
    /// flight are dropped with the sender.
    async fn settle_one(&mut self) -> Result<()> {
        let Some(joined) = self.in_flight.join_next().await else {
            return Ok(());
        };
        match joined
            .map_err(|e| anyhow!("Sending a batch failed: {e}"))
            .and_then(|sent| sent)
        {
            Ok((response, lines)) => {
                self.record(&response, &lines);
                Ok(())
            }
            Err(err) => {
                // The batches still in flight may already be written: let them finish and count
                // them, so the totals said with the error are the index's, not a guess.
                while let Some(joined) = self.in_flight.join_next().await {
                    if let Ok(Ok((response, lines))) = joined {
                        self.record(&response, &lines);
                    }
                }
                Err(err.context(format!(
                    "the load stopped after {} rows were loaded and {} refused",
                    self.total_sent, self.total_failed
                )))
            }
        }
    }

    fn record(&mut self, response: &JsonValue, lines: &SourceLines) {
        record_ingest_response(
            response,
            lines,
            &mut self.total_sent,
            &mut self.total_failed,
        );
    }

    /// Wait for every batch in flight.
    pub(crate) async fn finish(&mut self) -> Result<()> {
        while !self.in_flight.is_empty() {
            self.settle_one().await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourceFormat {
    SchemaJson,
    JsonDocument,
    JsonArray,
    JsonLines,
    CsvLike,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Compression {
    None,
    Gzip,
    Zip,
}

pub(crate) fn detect_compression(source: &str) -> Compression {
    let path_lower = if is_http_source(source) {
        Url::parse(source)
            .ok()
            .map(|url| url.path().to_lowercase())
            .unwrap_or_default()
    } else {
        source.to_lowercase()
    };

    if path_lower.ends_with(".gz") || path_lower.ends_with(".gzip") {
        Compression::Gzip
    } else if path_lower.ends_with(".zip") {
        Compression::Zip
    } else {
        Compression::None
    }
}

pub(crate) fn zip_first_entry_bytes(data: &[u8]) -> Result<(Vec<u8>, Option<String>)> {
    let cursor = Cursor::new(data);
    let mut archive = zip::ZipArchive::new(cursor).context("Failed to open ZIP archive")?;

    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .with_context(|| format!("Failed to read ZIP entry {}", i))?;
        if entry.is_dir() {
            continue;
        }
        let entry_name = entry.name().to_string();
        let mut buf = Vec::new();
        entry
            .read_to_end(&mut buf)
            .with_context(|| format!("Failed to decompress ZIP entry '{}'", entry_name))?;
        return Ok((buf, Some(entry_name)));
    }

    Err(anyhow!("ZIP archive does not contain any files"))
}

pub(crate) fn decompress_bytes(bytes: Vec<u8>, compression: Compression) -> Result<Vec<u8>> {
    match compression {
        Compression::None => Ok(bytes),
        Compression::Gzip => {
            let mut decoder = GzDecoder::new(Cursor::new(bytes));
            let mut decompressed = Vec::new();
            decoder
                .read_to_end(&mut decompressed)
                .context("Failed to decompress gzip data")?;
            Ok(decompressed)
        }
        Compression::Zip => {
            let (data, _name) = zip_first_entry_bytes(&bytes)?;
            Ok(data)
        }
    }
}

pub(crate) fn open_local_reader(
    path: &Path,
    compression: Compression,
) -> Result<Box<dyn Read + Send>> {
    let file = fs::File::open(path)
        .with_context(|| format!("Failed to open source file: {}", path.display()))?;
    match compression {
        Compression::None => Ok(Box::new(file)),
        Compression::Gzip => Ok(Box::new(GzDecoder::new(file))),
        Compression::Zip => {
            let mut buf = Vec::new();
            BufReader::new(file)
                .read_to_end(&mut buf)
                .with_context(|| format!("Failed to read ZIP file: {}", path.display()))?;
            let (data, _name) = zip_first_entry_bytes(&buf)?;
            Ok(Box::new(Cursor::new(data)))
        }
    }
}

pub(crate) fn is_http_source(source: &str) -> bool {
    source.starts_with("http://") || source.starts_with("https://")
}

pub(crate) fn read_local_prefix_bytes(path: &Path, max_bytes: usize) -> Result<Vec<u8>> {
    let compression = detect_compression(path.to_str().unwrap_or(""));
    let reader = open_local_reader(path, compression)?;
    let mut reader = BufReader::new(reader);
    let mut buffer = vec![0u8; max_bytes];
    let bytes_read = reader
        .read(&mut buffer)
        .with_context(|| format!("Failed to read source file: {}", path.display()))?;
    buffer.truncate(bytes_read);
    Ok(buffer)
}

pub(crate) fn detect_source_format_from_bytes(bytes: &[u8]) -> Result<SourceFormat> {
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Err(anyhow!("Source is empty"));
    }

    let first_non_whitespace = bytes
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        .ok_or_else(|| anyhow!("Source is empty"))?;

    // Detection is by content alone — the file's extension is deliberately not consulted.
    match first_non_whitespace {
        b'[' => Ok(SourceFormat::JsonArray),
        b'{' => {
            if let Ok(text) = std::str::from_utf8(bytes) {
                let mut non_empty_lines =
                    text.lines().map(str::trim).filter(|line| !line.is_empty());
                if let (Some(first), Some(second)) =
                    (non_empty_lines.next(), non_empty_lines.next())
                    && let Ok(first_json) = serde_json::from_str::<JsonValue>(first)
                    && let Ok(second_json) = serde_json::from_str::<JsonValue>(second)
                    && first_json.is_object()
                    && second_json.is_object()
                {
                    return Ok(SourceFormat::JsonLines);
                }
            }

            if let Ok(json) = serde_json::from_slice::<JsonValue>(bytes)
                && let JsonValue::Object(obj) = json
                && obj.contains_key("fields")
            {
                return Ok(SourceFormat::SchemaJson);
            }

            Ok(SourceFormat::JsonDocument)
        }
        _ => Ok(SourceFormat::CsvLike),
    }
}

pub(crate) fn detect_local_source_format(path: &Path) -> Result<SourceFormat> {
    let prefix = read_local_prefix_bytes(path, SOURCE_SNIFF_BYTES)?;
    detect_source_format_from_bytes(&prefix)
}

pub(crate) fn effective_json_document(doc: &JsonValue) -> Result<JsonValue> {
    let obj = doc
        .as_object()
        .ok_or_else(|| anyhow!("JSON source documents must be objects"))?;
    if let Some(inner_doc) = obj.get("doc") {
        let inner_obj = inner_doc
            .as_object()
            .ok_or_else(|| anyhow!("Doc payload field 'doc' must be an object"))?;
        Ok(JsonValue::Object(inner_obj.clone()))
    } else {
        Ok(JsonValue::Object(obj.clone()))
    }
}

pub(crate) fn json_value_to_id_string(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        JsonValue::Number(n) => Some(n.to_string()),
        JsonValue::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

#[derive(Debug)]
pub(crate) struct JsonLinesChunkParser {
    pub(crate) buffer: Vec<u8>,
    pub(crate) line_number: usize,
    pub(crate) seen_docs: usize,
}

impl JsonLinesChunkParser {
    pub(crate) fn new() -> Self {
        Self {
            buffer: Vec::new(),
            line_number: 0,
            seen_docs: 0,
        }
    }

    pub(crate) fn push_chunk(&mut self, chunk: &[u8]) -> Result<Vec<JsonValue>> {
        self.buffer.extend_from_slice(chunk);
        let mut docs = Vec::new();

        while let Some(pos) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let line_bytes: Vec<u8> = self.buffer.drain(..=pos).collect();
            if let Some(doc) = self.parse_line_bytes(&line_bytes)? {
                docs.push(doc);
            }
        }

        Ok(docs)
    }

    pub(crate) fn finish(mut self) -> Result<Vec<JsonValue>> {
        let mut docs = Vec::new();
        if !self.buffer.is_empty() {
            let remaining = std::mem::take(&mut self.buffer);
            if let Some(doc) = self.parse_line_bytes(&remaining)? {
                docs.push(doc);
            }
        }

        if self.seen_docs == 0 {
            anyhow::bail!("JSON lines source does not contain any documents");
        }

        Ok(docs)
    }

    pub(crate) fn parse_line_bytes(&mut self, line_bytes: &[u8]) -> Result<Option<JsonValue>> {
        self.line_number += 1;
        let line = std::str::from_utf8(line_bytes).with_context(|| {
            format!(
                "JSON lines source is not valid UTF-8 at line {}",
                self.line_number
            )
        })?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }

        let doc: JsonValue = serde_json::from_str(trimmed)
            .with_context(|| format!("Invalid JSON on line {}", self.line_number))?;
        if !doc.is_object() {
            anyhow::bail!("JSON line {} must contain an object", self.line_number);
        }
        self.seen_docs += 1;
        Ok(Some(doc))
    }
}

#[derive(Debug)]
pub(crate) struct JsonArrayChunkParser {
    pub(crate) started: bool,
    pub(crate) finished: bool,
    pub(crate) current: Vec<u8>,
    pub(crate) depth: usize,
    pub(crate) in_string: bool,
    pub(crate) escape: bool,
    pub(crate) seen_docs: usize,
}

impl JsonArrayChunkParser {
    pub(crate) fn new() -> Self {
        Self {
            started: false,
            finished: false,
            current: Vec::new(),
            depth: 0,
            in_string: false,
            escape: false,
            seen_docs: 0,
        }
    }

    pub(crate) fn push_chunk(&mut self, chunk: &[u8]) -> Result<Vec<JsonValue>> {
        let mut docs = Vec::new();

        for &byte in chunk {
            if !self.started {
                if byte.is_ascii_whitespace() {
                    continue;
                }
                if byte != b'[' {
                    anyhow::bail!("JSON array source must start with '['");
                }
                self.started = true;
                continue;
            }

            if self.finished {
                if !byte.is_ascii_whitespace() {
                    anyhow::bail!("Invalid trailing data after JSON array source");
                }
                continue;
            }

            if self.current.is_empty() {
                match byte {
                    b' ' | b'\t' | b'\r' | b'\n' | b',' => continue,
                    b']' => {
                        self.finished = true;
                        continue;
                    }
                    b'{' => {
                        self.current.push(byte);
                        self.depth = 1;
                        self.in_string = false;
                        self.escape = false;
                    }
                    _ => anyhow::bail!("JSON array documents must be objects"),
                }
                continue;
            }

            self.current.push(byte);
            if self.in_string {
                if self.escape {
                    self.escape = false;
                } else if byte == b'\\' {
                    self.escape = true;
                } else if byte == b'"' {
                    self.in_string = false;
                }
                continue;
            }

            match byte {
                b'"' => self.in_string = true,
                b'{' | b'[' => self.depth += 1,
                b'}' | b']' => {
                    self.depth = self
                        .depth
                        .checked_sub(1)
                        .ok_or_else(|| anyhow!("Invalid JSON array nesting"))?;
                    if self.depth == 0 {
                        let doc = self.finish_current_document()?;
                        docs.push(doc);
                    }
                }
                _ => {}
            }
        }

        Ok(docs)
    }

    pub(crate) fn finish(self) -> Result<Vec<JsonValue>> {
        if !self.current.is_empty() {
            anyhow::bail!("JSON array source ended before a document was complete");
        }
        if !self.started {
            anyhow::bail!("JSON array source is empty");
        }
        if !self.finished {
            anyhow::bail!("JSON array source ended before closing ']'");
        }
        if self.seen_docs == 0 {
            anyhow::bail!("JSON array source does not contain any documents");
        }
        Ok(Vec::new())
    }

    pub(crate) fn finish_current_document(&mut self) -> Result<JsonValue> {
        let raw = std::mem::take(&mut self.current);
        let doc: JsonValue =
            serde_json::from_slice(&raw).context("Failed to parse JSON array document")?;
        if !doc.is_object() {
            anyhow::bail!("JSON array documents must be objects");
        }
        self.depth = 0;
        self.in_string = false;
        self.escape = false;
        self.seen_docs += 1;
        Ok(doc)
    }
}

#[derive(Debug)]
pub(crate) struct JsonObjectChunkParser {
    pub(crate) started: bool,
    pub(crate) finished: bool,
    pub(crate) current: Vec<u8>,
    pub(crate) depth: usize,
    pub(crate) in_string: bool,
    pub(crate) escape: bool,
}

impl JsonObjectChunkParser {
    pub(crate) fn new() -> Self {
        Self {
            started: false,
            finished: false,
            current: Vec::new(),
            depth: 0,
            in_string: false,
            escape: false,
        }
    }

    pub(crate) fn push_chunk(&mut self, chunk: &[u8]) -> Result<Vec<JsonValue>> {
        let mut docs = Vec::new();

        for &byte in chunk {
            if !self.started {
                if byte.is_ascii_whitespace() {
                    continue;
                }
                if byte != b'{' {
                    anyhow::bail!("JSON document source must start with '{{'");
                }
                self.started = true;
                self.current.push(byte);
                self.depth = 1;
                continue;
            }

            if self.finished {
                if !byte.is_ascii_whitespace() {
                    anyhow::bail!("Invalid trailing data after JSON document source");
                }
                continue;
            }

            self.current.push(byte);
            if self.in_string {
                if self.escape {
                    self.escape = false;
                } else if byte == b'\\' {
                    self.escape = true;
                } else if byte == b'"' {
                    self.in_string = false;
                }
                continue;
            }

            match byte {
                b'"' => self.in_string = true,
                b'{' | b'[' => self.depth += 1,
                b'}' | b']' => {
                    self.depth = self
                        .depth
                        .checked_sub(1)
                        .ok_or_else(|| anyhow!("Invalid JSON document nesting"))?;
                    if self.depth == 0 {
                        let raw = std::mem::take(&mut self.current);
                        let doc: JsonValue = serde_json::from_slice(&raw)
                            .context("Failed to parse JSON document")?;
                        if !doc.is_object() {
                            anyhow::bail!("JSON document source must contain an object");
                        }
                        self.finished = true;
                        docs.push(doc);
                    }
                }
                _ => {}
            }
        }

        Ok(docs)
    }

    pub(crate) fn finish(self) -> Result<Vec<JsonValue>> {
        if !self.started {
            anyhow::bail!("JSON document source is empty");
        }
        if !self.finished {
            anyhow::bail!("JSON document source ended before the object was complete");
        }
        Ok(Vec::new())
    }
}

#[derive(Debug)]
pub(crate) enum JsonChunkParser {
    Lines(JsonLinesChunkParser),
    Array(JsonArrayChunkParser),
    Object(JsonObjectChunkParser),
}

impl JsonChunkParser {
    pub(crate) fn new(format: SourceFormat) -> Result<Self> {
        match format {
            SourceFormat::JsonLines => Ok(Self::Lines(JsonLinesChunkParser::new())),
            SourceFormat::JsonArray => Ok(Self::Array(JsonArrayChunkParser::new())),
            SourceFormat::JsonDocument => Ok(Self::Object(JsonObjectChunkParser::new())),
            SourceFormat::SchemaJson => Err(anyhow!(
                "Schema JSON object cannot be used as document data"
            )),
            SourceFormat::CsvLike => Err(anyhow!("Source is not JSON data")),
        }
    }

    pub(crate) fn push_chunk(&mut self, chunk: &[u8]) -> Result<Vec<JsonValue>> {
        match self {
            Self::Lines(parser) => parser.push_chunk(chunk),
            Self::Array(parser) => parser.push_chunk(chunk),
            Self::Object(parser) => parser.push_chunk(chunk),
        }
    }

    pub(crate) fn finish(self) -> Result<Vec<JsonValue>> {
        match self {
            Self::Lines(parser) => parser.finish(),
            Self::Array(parser) => parser.finish(),
            Self::Object(parser) => parser.finish(),
        }
    }
}

pub(crate) fn read_json_value_from_path(path: &Path) -> Result<JsonValue> {
    let compression = detect_compression(path.to_str().unwrap_or(""));
    let reader = open_local_reader(path, compression)?;
    serde_json::from_reader(BufReader::new(reader))
        .with_context(|| format!("Failed to parse JSON source file: {}", path.display()))
}

pub(crate) fn process_json_array_reader<R, F>(reader: R, on_doc: &mut F) -> Result<usize>
where
    R: Read,
    F: FnMut(JsonValue) -> Result<()>,
{
    use serde::de::{self, DeserializeSeed, SeqAccess, Visitor};

    struct JsonArraySeed<'a, F> {
        on_doc: &'a mut F,
    }

    struct JsonArrayVisitor<'a, F> {
        on_doc: &'a mut F,
    }

    impl<'de, 'a, F> Visitor<'de> for JsonArrayVisitor<'a, F>
    where
        F: FnMut(JsonValue) -> Result<()>,
    {
        type Value = usize;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a JSON array of object documents")
        }

        fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut count = 0usize;
            while let Some(value) = seq.next_element::<JsonValue>()? {
                if !value.is_object() {
                    return Err(de::Error::custom("JSON array documents must be objects"));
                }
                (self.on_doc)(value).map_err(de::Error::custom)?;
                count += 1;
            }

            if count == 0 {
                return Err(de::Error::custom(
                    "JSON array source does not contain any documents",
                ));
            }

            Ok(count)
        }
    }

    impl<'de, 'a, F> DeserializeSeed<'de> for JsonArraySeed<'a, F>
    where
        F: FnMut(JsonValue) -> Result<()>,
    {
        type Value = usize;

        fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserializer.deserialize_seq(JsonArrayVisitor {
                on_doc: self.on_doc,
            })
        }
    }

    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let count = JsonArraySeed { on_doc }
        .deserialize(&mut deserializer)
        .context("Failed to parse JSON array source")?;
    deserializer
        .end()
        .context("Invalid trailing data after JSON array source")?;
    Ok(count)
}

pub(crate) fn for_each_json_document_in_reader<R, F>(
    reader: R,
    format: SourceFormat,
    mut on_doc: F,
) -> Result<usize>
where
    R: Read,
    F: FnMut(JsonValue) -> Result<()>,
{
    match format {
        SourceFormat::JsonLines => {
            let buf_reader = BufReader::new(reader);
            let mut count = 0usize;

            for (line_no, line_result) in buf_reader.lines().enumerate() {
                let line = line_result
                    .with_context(|| format!("Failed to read JSONL source line {}", line_no + 1))?;
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let doc: JsonValue = serde_json::from_str(trimmed)
                    .with_context(|| format!("Invalid JSON on line {}", line_no + 1))?;
                if !doc.is_object() {
                    anyhow::bail!("JSON line {} must contain an object", line_no + 1);
                }
                on_doc(doc)?;
                count += 1;
            }

            if count == 0 {
                anyhow::bail!("JSON lines source does not contain any documents");
            }

            Ok(count)
        }
        SourceFormat::JsonArray => process_json_array_reader(BufReader::new(reader), &mut on_doc),
        SourceFormat::JsonDocument => {
            let value: JsonValue = serde_json::from_reader(BufReader::new(reader))
                .context("Failed to parse JSON source")?;
            if !value.is_object() {
                anyhow::bail!("JSON document source must contain an object");
            }
            on_doc(value)?;
            Ok(1)
        }
        SourceFormat::SchemaJson => Err(anyhow!(
            "Schema JSON object cannot be used as document data"
        )),
        SourceFormat::CsvLike => Err(anyhow!("Source is not JSON data")),
    }
}

/// How a JSON load turns a source document into a payload: which fields make its id, and how
/// each field's values are read.
#[derive(Debug, Clone)]
pub(crate) struct JsonLoadPlan {
    pub(crate) id: IdSpec,
    /// Named with `--id`: the named fields make the id, whatever else the document carries.
    pub(crate) explicit: bool,
    pub(crate) shapes: HashMap<String, ColumnShape>,
}

/// The payload line for one source document and its id; `None` when the document has no id.
///
/// The id is the document's own `id` when it has one, else the id field's value — unless
/// `--id` named the fields, which then make it alone. Each field's value is fitted to its field
/// first: `"12"` in a count is 12, `"NA"` no value, `"TRUE"` in a flag `true`.
pub(crate) fn build_doc_payload_from_json_document(
    raw_doc: &JsonValue,
    plan: &JsonLoadPlan,
) -> Result<Option<(String, JsonValue)>> {
    let raw_obj = raw_doc
        .as_object()
        .ok_or_else(|| anyhow!("JSON source documents must be objects"))?;

    let mut doc_obj = if let Some(inner_doc) = raw_obj.get("doc") {
        inner_doc
            .as_object()
            .cloned()
            .ok_or_else(|| anyhow!("Doc payload field 'doc' must be an object"))?
    } else {
        raw_obj.clone()
    };

    let field_id = |name: &str| doc_obj.get(name).and_then(json_value_to_id_string);
    let id = match (&plan.id, plan.explicit) {
        (IdSpec::Column(name), false) => raw_obj
            .get("id")
            .and_then(json_value_to_id_string)
            .or_else(|| field_id("id"))
            .or_else(|| field_id(name)),
        (spec, _) => compose_id(spec.columns().into_iter().map(field_id)),
    };
    let Some(id) = id else {
        return Ok(None);
    };
    let routing_key = raw_obj
        .get("routing_key")
        .and_then(json_value_to_id_string)
        .or_else(|| plan.id.single().and_then(field_id))
        .unwrap_or_else(|| id.clone());

    for (name, value) in doc_obj.iter_mut() {
        if let Some(shape) = plan.shapes.get(name) {
            *value = fit_json(std::mem::take(value), shape);
        }
    }
    doc_obj.insert("id".to_string(), JsonValue::String(id.clone()));

    let payload = json!({
        "id": id,
        "routing_key": routing_key,
        "doc": JsonValue::Object(doc_obj),
    });
    Ok(Some((id, payload)))
}

/// What a load needs from the index's schema: each field's type, and the shadow field that
/// carries the source's own id, when there is one.
#[derive(Debug, Clone, Default)]
pub(crate) struct ExistingSchema {
    pub(crate) field_types: HashMap<String, TantivyFieldType>,
    pub(crate) shadow_field: Option<String>,
    /// The fields the index records its ids are made of.
    pub(crate) id_fields: Vec<String>,
    pub(crate) description: Option<String>,
    /// Each field as the node describes it, for carrying a person's edits into a schema rebuilt
    /// for another id.
    pub(crate) fields: Vec<JsonValue>,
}

impl ExistingSchema {
    /// The id the index already keys its documents by: its recorded fields, else its shadow
    /// field — the one record an index written before `id_fields` keeps of its id.
    pub(crate) fn recorded_id(&self) -> Option<IdSpec> {
        match self.id_fields.len() {
            0 => self.shadow_field.clone().map(IdSpec::Column),
            1 => Some(IdSpec::Column(self.id_fields[0].clone())),
            _ => Some(IdSpec::Composite(self.id_fields.clone())),
        }
    }
}

/// Whether two ids are made of the same columns in the same order, whatever their case.
pub(crate) fn same_id(a: &IdSpec, b: &IdSpec) -> bool {
    let (a, b) = (a.columns(), b.columns());
    a.len() == b.len() && a.iter().zip(&b).all(|(x, y)| x.eq_ignore_ascii_case(y))
}

/// The schema the index already has to load into; `None` when the loader should apply the one it
/// detects from the source.
///
/// A schema with no fields is none. A node on an older build answers a dropped index with the
/// record of the drop — no fields — and taken as a schema it made the loader skip the declared
/// types in the file's header, leaving the node to type every field by guesswork from its
/// documents.
///
/// Only a node's `404` says there is none. Any other failure was once read the same way, and the
/// loader went on to declare a schema of its own over the index it could not read.
pub(crate) async fn existing_schema(
    client: &CameoClient,
    index: &str,
) -> Result<Option<ExistingSchema>> {
    let Some(config) = client
        .find_index_config(index)
        .await
        .with_context(|| format!("Failed to read the schema of index '{index}'"))?
    else {
        return Ok(None);
    };
    if config.fields.is_empty() {
        return Ok(None);
    }
    let mut existing = ExistingSchema {
        id_fields: config.id_fields.clone(),
        description: config.description.clone(),
        fields: config.fields.clone(),
        ..Default::default()
    };
    for field in &config.fields {
        let Some(name) = field.get("name").and_then(JsonValue::as_str) else {
            continue;
        };
        if let Some(field_type) = field
            .get("type")
            .and_then(|t| serde_json::from_value(t.clone()).ok())
        {
            existing.field_types.insert(name.to_string(), field_type);
        }
        if field.get("shadow").and_then(JsonValue::as_bool) == Some(true) {
            existing.shadow_field = Some(name.to_string());
        }
    }
    Ok(Some(existing))
}

/// Each field's type in a schema the loader built, as it will be stored.
pub(crate) fn schema_field_types(schema_json: &JsonValue) -> HashMap<String, TantivyFieldType> {
    schema_json
        .get("fields")
        .and_then(JsonValue::as_object)
        .map(|fields| {
            fields
                .iter()
                .filter_map(|(name, field)| {
                    let field_type = field.get("field_type")?.clone();
                    Some((name.clone(), serde_json::from_value(field_type).ok()?))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Where each line of a batch's body came from in the source.
///
/// The node names a refused document by its line in the request body, and every batch is a
/// request of its own — so "line 3" meant the third document of whichever batch, and a load of
/// several batches reported line 1 several times over. This is what turns the node's line back
/// into a place in the source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SourceLines {
    /// The file line each body line was read from, in order: a delimited file.
    File(Vec<u64>),
    /// The source document number of each body line, in order: a JSON source, where one
    /// document need not be one line.
    Documents(Vec<u64>),
}

impl SourceLines {
    /// The source position of line `body_line` (1-based) of the request, as a reason names it.
    pub(crate) fn locate(&self, body_line: u64) -> Option<String> {
        let index = body_line.checked_sub(1)?;
        match self {
            SourceLines::File(lines) => usize::try_from(index)
                .ok()
                .and_then(|i| lines.get(i))
                .map(|line| format!("line {line}")),
            SourceLines::Documents(documents) => usize::try_from(index)
                .ok()
                .and_then(|i| documents.get(i))
                .map(|document| format!("document {document}")),
        }
    }
}

/// A reason the node gave, with its request line replaced by the source position it came from.
/// Anything that does not start `line <N>:` is returned as it was.
pub(crate) fn relocate_reason(reason: &str, lines: &SourceLines) -> String {
    let relocated = reason.strip_prefix("line ").and_then(|rest| {
        let (number, tail) = rest.split_once(':')?;
        let position = lines.locate(number.parse().ok()?)?;
        Some(format!("{position}:{tail}"))
    });
    relocated.unwrap_or_else(|| reason.to_string())
}

/// Tally one batch's answer and report what it refused.
///
/// The node lists the first hundred reasons and counts the rest in `suppressed_errors`. Only the
/// list used to be counted, so a batch refusing 3,900 of 4,000 documents added 100 to `failed`,
/// and the load's closing line — `loaded=1030 failed=500` for 16,559 documents sent — accounted
/// for fewer than a tenth of them. `items_written` plus every reason is every document sent.
pub(crate) fn record_ingest_response(
    response: &JsonValue,
    lines: &SourceLines,
    total_sent: &mut usize,
    total_failed: &mut usize,
) {
    let written = response
        .get("items_written")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;
    let listed = response.get("errors").and_then(|v| v.as_array());
    let suppressed = response
        .get("suppressed_errors")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;
    let failed = listed.map_or(0, |e| e.len()) + suppressed;

    *total_sent += written;
    *total_failed += failed;

    if failed > 0 {
        eprintln!("⚠️  Batch warning: {failed} items were not written.");
        let shown = listed.map_or(&[][..], |e| e.as_slice());
        for err in shown.iter().take(3) {
            eprintln!(
                "   - {}",
                relocate_reason(err.as_str().unwrap_or("Unknown error"), lines)
            );
        }
        if failed > 3 {
            eprintln!("   ... and {} more", failed - shown.len().min(3));
        }
    }
}

pub(crate) async fn fetch_source_prefix_bytes(
    client: &CameoClient,
    source: &str,
    max_bytes: usize,
) -> Result<Vec<u8>> {
    if is_http_source(source) {
        let compression = detect_compression(source);
        if compression != Compression::None {
            // Compressed remote: download all bytes, decompress, return prefix
            let all_bytes = fetch_bytes_source(client, source).await?;
            let len = all_bytes.len().min(max_bytes);
            return Ok(all_bytes[..len].to_vec());
        }

        let url = Url::parse(source).context("Invalid URL for source")?;
        let mut response = client
            .source_http()
            .get(url)
            .send()
            .await
            .context("Failed to fetch remote source")?;
        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("Failed to fetch remote source: {} - {}", status, text);
        }

        let mut prefix = Vec::new();
        while prefix.len() < max_bytes {
            match response
                .chunk()
                .await
                .context("Failed to read remote source body")?
            {
                Some(chunk) => {
                    let remaining = max_bytes - prefix.len();
                    let take_len = remaining.min(chunk.len());
                    prefix.extend_from_slice(&chunk[..take_len]);
                    if take_len < chunk.len() {
                        break;
                    }
                }
                None => break,
            }
        }

        Ok(prefix)
    } else {
        read_local_prefix_bytes(Path::new(source), max_bytes)
    }
}

pub(crate) async fn detect_source_format_for_source(
    client: &CameoClient,
    source: &str,
) -> Result<SourceFormat> {
    if is_http_source(source) {
        let prefix = fetch_source_prefix_bytes(client, source, SOURCE_SNIFF_BYTES).await?;
        detect_source_format_from_bytes(&prefix)
    } else {
        detect_local_source_format(Path::new(source))
    }
}

pub(crate) async fn load_json_value_from_source(
    client: &CameoClient,
    source: &str,
) -> Result<JsonValue> {
    if is_http_source(source) {
        let raw_bytes = fetch_bytes_source(client, source).await?;
        serde_json::from_slice(&raw_bytes).context("Failed to parse JSON source")
    } else {
        let source = source.to_string();
        tokio::task::spawn_blocking(move || read_json_value_from_path(Path::new(&source)))
            .await
            .map_err(|err| anyhow!("Local JSON source parsing failed: {}", err))?
    }
}

/// Open a streaming GET against a remote JSON source, refusing a non-2xx status before
/// the caller reads an error page as documents.
pub(crate) async fn open_http_json_stream(
    client: &CameoClient,
    source: &str,
) -> Result<reqwest::Response> {
    let url = Url::parse(source).context("Invalid URL for JSON source")?;
    let response = client
        .source_http()
        .get(url)
        .send()
        .await
        .context("Failed to fetch remote JSON source")?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        anyhow::bail!("Failed to fetch remote JSON source: {} - {}", status, text);
    }
    Ok(response)
}

pub(crate) async fn for_each_json_document_in_http_source<F>(
    client: &CameoClient,
    source: &str,
    format: SourceFormat,
    mut on_doc: F,
) -> Result<usize>
where
    F: FnMut(JsonValue) -> Result<()>,
{
    let mut response = open_http_json_stream(client, source).await?;
    let mut parser = JsonChunkParser::new(format)?;
    let mut count = 0usize;

    while let Some(chunk) = response
        .chunk()
        .await
        .context("Failed to read remote JSON source body")?
    {
        let docs = parser.push_chunk(&chunk)?;
        for doc in docs {
            on_doc(doc)?;
            count += 1;
        }
    }

    for doc in parser.finish()? {
        on_doc(doc)?;
        count += 1;
    }

    Ok(count)
}

pub(crate) enum JsonIngestEvent {
    /// A batch's NDJSON body, and where in the source each of its documents came from.
    DataBatch {
        body: Vec<u8>,
        lines: SourceLines,
        /// A document in it repeats an id sent before. See [`BatchSender::send`].
        repeats: bool,
    },
}

/// The JSON ingest protocol both loaders run, once the scan has settled the schema and the id:
/// each document becomes a payload line, and full batches become events. What differs between
/// the loaders is only how the events are delivered — awaited inline for an HTTP stream, sent
/// down a channel from the blocking reader thread.
pub(crate) struct JsonIngestPipeline {
    pub(crate) plan: JsonLoadPlan,
    pub(crate) batch_size: usize,
    pub(crate) batch_body: Vec<u8>,
    /// The source document number of each line in `batch_body`.
    pub(crate) batch_documents: Vec<u64>,
    /// Documents read from the source so far.
    pub(crate) documents: u64,
    /// A document in the batch being gathered repeats an id sent before.
    batch_repeats: bool,
    pub(crate) ledger: IdLedger,
}

impl JsonIngestPipeline {
    pub(crate) fn new(batch_size: usize, plan: JsonLoadPlan) -> Self {
        Self {
            plan,
            batch_size: batch_size.max(1),
            batch_body: Vec::new(),
            batch_documents: Vec::new(),
            documents: 0,
            batch_repeats: false,
            ledger: IdLedger::default(),
        }
    }

    pub(crate) fn push(
        &mut self,
        raw_doc: &JsonValue,
        events: &mut Vec<JsonIngestEvent>,
    ) -> Result<()> {
        self.documents += 1;
        let at = Location::Document(self.documents);
        let Some((id, payload)) = build_doc_payload_from_json_document(raw_doc, &self.plan)? else {
            self.ledger.skip(at);
            return Ok(());
        };
        self.batch_repeats |= self.ledger.admit(&id, at);
        let mut line = serde_json::to_vec(&payload).context("Failed to serialize JSON payload")?;
        line.push(b'\n');
        self.batch_body.extend_from_slice(&line);
        self.batch_documents.push(self.documents);
        if self.batch_documents.len() >= self.batch_size {
            self.emit_batch(events);
        }
        Ok(())
    }

    /// End of stream: emit the last partial batch.
    pub(crate) fn finish(&mut self, events: &mut Vec<JsonIngestEvent>) {
        if !self.batch_body.is_empty() {
            self.emit_batch(events);
        }
    }

    fn emit_batch(&mut self, events: &mut Vec<JsonIngestEvent>) {
        events.push(JsonIngestEvent::DataBatch {
            body: std::mem::take(&mut self.batch_body),
            lines: SourceLines::Documents(std::mem::take(&mut self.batch_documents)),
            repeats: std::mem::take(&mut self.batch_repeats),
        });
    }
}

/// Deliver one event the HTTP way: a batch becomes an NDJSON stream.
pub(crate) async fn deliver_json_ingest_event(
    sender: &mut BatchSender,
    event: JsonIngestEvent,
) -> Result<()> {
    let JsonIngestEvent::DataBatch {
        body,
        lines,
        repeats,
    } = event;
    sender.send(lines, repeats, move || Ok(body)).await
}

/// What a load sent, and what it did with ids.
pub(crate) struct LoadTotals {
    pub(crate) sent: usize,
    pub(crate) failed: usize,
    pub(crate) ledger: IdLedger,
}

pub(crate) async fn load_data_from_http_json_source_single_pass(
    client: &CameoClient,
    index: &str,
    source: &str,
    format: SourceFormat,
    batch_size: usize,
    parallel: usize,
    plan: JsonLoadPlan,
) -> Result<LoadTotals> {
    let mut pipeline = JsonIngestPipeline::new(batch_size, plan);
    let mut events = Vec::new();
    let mut sender = BatchSender::new(client, index, parallel);
    let mut response = open_http_json_stream(client, source).await?;
    let mut parser = JsonChunkParser::new(format)?;

    while let Some(chunk) = response
        .chunk()
        .await
        .context("Failed to read remote JSON source body")?
    {
        for doc in parser.push_chunk(&chunk)? {
            pipeline.push(&doc, &mut events)?;
        }
        for event in events.drain(..) {
            deliver_json_ingest_event(&mut sender, event).await?;
        }
    }
    for doc in parser.finish()? {
        pipeline.push(&doc, &mut events)?;
    }
    pipeline.finish(&mut events);
    for event in events.drain(..) {
        deliver_json_ingest_event(&mut sender, event).await?;
    }
    sender.finish().await?;
    Ok(LoadTotals {
        sent: sender.total_sent,
        failed: sender.total_failed,
        ledger: pipeline.ledger,
    })
}

pub(crate) async fn load_data_from_reader_json_source_single_pass(
    client: &CameoClient,
    index: &str,
    reader: Box<dyn Read + Send + 'static>,
    format: SourceFormat,
    batch_size: usize,
    parallel: usize,
    plan: JsonLoadPlan,
) -> Result<LoadTotals> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<JsonIngestEvent>(2);

    // The reader is blocking, so the pipeline runs on a worker thread and reports
    // readiness as events; the async side delivers them in the order they arrive.
    let producer = tokio::task::spawn_blocking(move || -> Result<IdLedger> {
        let mut pipeline = JsonIngestPipeline::new(batch_size, plan);
        let mut events = Vec::new();
        let send_err = || anyhow!("Failed to send message because receiver was dropped");
        let drain = |events: &mut Vec<JsonIngestEvent>| -> Result<()> {
            for event in events.drain(..) {
                tx.blocking_send(event).map_err(|_| send_err())?;
            }
            Ok(())
        };

        for_each_json_document_in_reader(reader, format, |raw_doc| {
            pipeline.push(&raw_doc, &mut events)?;
            drain(&mut events)
        })?;
        pipeline.finish(&mut events);
        drain(&mut events)?;
        Ok(pipeline.ledger)
    });

    let mut sender = BatchSender::new(client, index, parallel);
    let send_result: Result<()> = async {
        while let Some(event) = rx.recv().await {
            deliver_json_ingest_event(&mut sender, event).await?;
        }
        sender.finish().await
    }
    .await;

    drop(rx);
    let produced = producer.await;
    // A failed send is the cause, and the reader stopping because nobody was listening any more
    // only its echo, so the send's error is the one reported.
    send_result?;
    let ledger = produced.map_err(|err| anyhow!("Local JSON batch producer failed: {}", err))??;
    Ok(LoadTotals {
        sent: sender.total_sent,
        failed: sender.total_failed,
        ledger,
    })
}

/// The schema a source describes: a schema file as it is, or the one its data describes.
pub(crate) async fn detect_schema_from_source(
    client: &CameoClient,
    source: &str,
    delimiter: Delimiter,
    id: Option<&IdSpec>,
) -> Result<JsonValue> {
    let mut spinner = ProgressSpinner::new();
    let result = async {
        let format = detect_source_format_for_source(client, source).await?;
        if format == SourceFormat::SchemaJson
            || (format == SourceFormat::JsonDocument
                && load_json_value_from_source(client, source)
                    .await?
                    .get("fields")
                    .is_some())
        {
            return load_json_value_from_source(client, source).await;
        }
        let ids = IdOptions {
            explicit: id,
            recorded: None,
        };
        let analysis =
            analyze_source(client, source, delimiter, ids, ScanLimits::default()).await?;
        analysis.profiler.warn_about_id(&analysis.id);
        schema_from_analysis(&analysis)
    }
    .await;
    spinner.stop();
    result
}

/// The schema a source describes, to apply to an index.
pub(crate) async fn load_schema_from_source(
    client: &CameoClient,
    source: &str,
    delimiter: Delimiter,
    id: Option<&IdSpec>,
) -> Result<JsonValue> {
    detect_schema_from_source(client, source, delimiter, id).await
}

/// The analysis `schema detect --report` prints, in place of the schema.
pub(crate) async fn report_source(
    client: &CameoClient,
    source: &str,
    delimiter: Delimiter,
    id: Option<&IdSpec>,
) -> Result<String> {
    let mut spinner = ProgressSpinner::new();
    let ids = IdOptions {
        explicit: id,
        recorded: None,
    };
    let analysis = analyze_source(client, source, delimiter, ids, ScanLimits::default()).await;
    spinner.stop();
    Ok(analysis?.report(source))
}

/// Whether one batch read ahead of a load into a typed index left something to guess: the id
/// chosen by values (an index that records none, a source with no `id` column), a column of
/// numeric dates none of which says which way round they are, a column with no value yet — so
/// whether it holds lists is unknown — or, in a JSON source, a field the schema declares that the
/// batch never showed.
pub(crate) fn first_batch_inconclusive(
    analysis: &SourceAnalysis,
    field_types: &HashMap<String, TantivyFieldType>,
) -> bool {
    if matches!(analysis.id.reason, IdReason::Unique | IdReason::NameOnly) {
        return true;
    }
    let columns = &analysis.profiler.columns;
    if columns
        .iter()
        .any(|column| column.filled() == 0 || column.date_order_unsettled())
    {
        return true;
    }
    analysis.delimiter.is_none()
        && field_types
            .keys()
            .filter(|name| name.as_str() != "id")
            .any(|name| analysis.profiler.index_of(name).is_none())
}

/// Load a source into an index: scan it, apply the schema it describes when the index has none,
/// then send every row.
pub(crate) async fn load_data_from_source(
    client: &CameoClient,
    index: &str,
    source: &str,
    delimiter: Delimiter,
    pace: LoadPace,
    id: Option<&IdSpec>,
    recreate: bool,
) -> Result<()> {
    let batch_size = pace.batch_size.max(1);
    let parallel = check_parallel(pace.parallel)?;
    let format = detect_source_format_for_source(client, source).await?;
    if format == SourceFormat::SchemaJson {
        return Err(anyhow!("Schema JSON object cannot be loaded as index data"));
    }
    let existing = existing_schema(client, index).await?;
    let recorded = existing.as_ref().and_then(ExistingSchema::recorded_id);
    let new_id = id.is_some_and(|id| recorded.as_ref().is_none_or(|r| !same_id(id, r)));
    // An index with a schema has its fields typed and, usually, its id recorded: the load reads
    // only its first batch ahead, for the date orders and lists a schema does not record. A schema
    // still to be written from the scan — a new index, or a new id — is scanned in full.
    let typed = existing.is_some() && !new_id;
    let limits = if typed {
        ScanLimits::first_batch()
    } else {
        ScanLimits::default()
    };
    let mut spinner = ProgressSpinner::new();
    let result = async {
        let ids = IdOptions {
            explicit: id,
            recorded: recorded.as_ref(),
        };
        let mut analysis = analyze_source(client, source, delimiter, ids, limits).await?;
        if typed {
            let field_types = existing
                .as_ref()
                .map(|existing| existing.field_types.clone())
                .unwrap_or_default();
            if first_batch_inconclusive(&analysis, &field_types) {
                analysis =
                    analyze_source(client, source, delimiter, ids, ScanLimits::sampled()).await?;
            }
        }
        analysis.profiler.warn_about_id(&analysis.id);
        // Asked for by name, since it is the one step here that cannot be undone: the documents
        // go, the schema stays, and the load fills the index again. Only once the source has
        // been read and its id settled, so a load that cannot run leaves the index as it was.
        if recreate && existing.is_some() {
            client
                .delete_index(index, false)
                .await
                .with_context(|| format!("Failed to delete the documents of index '{index}'"))?;
            println!("Deleted the documents of index '{index}'; its schema is kept");
        }
        let field_types = match &existing {
            // Another id than the index records: its schema has to say so before a row is sent.
            // The node rebuilds an index with no documents for it, and refuses one with some —
            // two keys in one index cannot be told apart.
            Some(existing) if new_id => {
                let schema = schema_for_new_id(&analysis, existing)?;
                client
                    .put_index_config(index, &schema)
                    .await
                    .map_err(|err| {
                        // The way through a refusal for documents in the way, unless it was
                        // already taken; any other failure is said as it is.
                        let in_the_way = crate::failure_status(&err) == Some(409);
                        if recreate || !in_the_way {
                            return err;
                        }
                        anyhow!(
                            "{err}\nIndex '{index}' keys its documents by {}. To load it by {} \
                         instead, add --recreate: it deletes the documents, keeps the schema, and \
                         loads again.",
                            recorded
                                .as_ref()
                                .map(|r| r.columns().join(","))
                                .unwrap_or_else(|| "another id".to_string()),
                            analysis.id.spec.columns().join(","),
                        )
                    })?;
                println!(
                    "Index '{index}' now keys its documents by {}",
                    analysis.id.spec.columns().join(",")
                );
                schema_field_types(&schema)
            }
            Some(existing) => existing.field_types.clone(),
            None => {
                let schema = schema_from_analysis(&analysis)?;
                client
                    .put_index_config(index, &schema)
                    .await
                    .with_context(|| format!("Failed to create schema for index '{}'", index))?;
                println!(
                    "Schema was missing; detected and applied schema to index '{}'",
                    index
                );
                schema_field_types(&schema)
            }
        };
        let totals = match analysis.format {
            SourceFormat::CsvLike => {
                load_csv(client, index, &analysis, &field_types, batch_size, parallel).await?
            }
            format => {
                let mut shapes: HashMap<String, ColumnShape> = analysis
                    .profiler
                    .columns
                    .iter()
                    .map(|c| c.name.clone())
                    .zip(analysis.shapes(&field_types))
                    .collect();
                // A field the schema declares fits its values whether or not the scan met it:
                // a key first seen past what was read is still a count, a flag or a date.
                for (name, field_type) in &field_types {
                    shapes.entry(name.clone()).or_insert_with(|| ColumnShape {
                        field_type: Some(field_type.clone()),
                        ..Default::default()
                    });
                }
                let plan = JsonLoadPlan {
                    id: analysis.id.spec.clone(),
                    explicit: matches!(analysis.id.reason, IdReason::Explicit | IdReason::Existing),
                    shapes,
                };
                match &analysis.data {
                    Some(data) => {
                        load_data_from_reader_json_source_single_pass(
                            client,
                            index,
                            data.reader()?,
                            format,
                            batch_size,
                            parallel,
                            plan,
                        )
                        .await?
                    }
                    None => {
                        load_data_from_http_json_source_single_pass(
                            client, index, source, format, batch_size, parallel, plan,
                        )
                        .await?
                    }
                }
            }
        };
        let candidate = analysis.profiler.suggested_id();
        let explicit = matches!(analysis.id.reason, IdReason::Explicit | IdReason::Existing);
        Ok::<_, anyhow::Error>((totals, analysis.id.spec, explicit, candidate))
    }
    .await;
    spinner.stop();
    let (totals, id, explicit, candidate) = result?;
    // `loaded` counts rows written; a row that replaced an earlier one with its id is among them,
    // so the documents the index gained are `loaded - replaced`.
    println!(
        "Ingestion complete for index '{}': loaded={} replaced={} skipped={} failed={} (batch size {}, parallel {})",
        index,
        totals.sent,
        totals.ledger.repeats,
        totals.ledger.skipped,
        totals.failed,
        batch_size,
        parallel
    );
    totals.ledger.report(&id, explicit, candidate.as_deref());
    Ok(())
}

/// Send every row of a CSV the scan has settled.
async fn load_csv(
    client: &CameoClient,
    index: &str,
    analysis: &SourceAnalysis,
    field_types: &HashMap<String, TantivyFieldType>,
    batch_size: usize,
    parallel: usize,
) -> Result<LoadTotals> {
    let data = analysis
        .data
        .as_ref()
        .expect("a CSV is scanned from its data");
    let delimiter = analysis.delimiter.expect("a CSV has a delimiter");
    let mut reader = csv_reader(data.reader()?, delimiter);
    let raw_headers = reader
        .headers()
        .context("CSV file is missing headers")?
        .clone();
    let mut lines = RecordLines::after_header(&raw_headers, reader.position().line());
    let mut ingest = CsvIngest::new(
        analysis.headers.clone(),
        analysis.shapes(field_types),
        analysis.id_columns(),
        batch_size,
    );
    for ((name, _), shape) in analysis.headers.iter().zip(ingest.columns.iter()) {
        for departure in shape.dates.departures() {
            println!("Column '{name}' writes {departure}; loading them as YYYY-MM-DD");
        }
    }
    let mut sender = BatchSender::new(client, index, parallel);
    let mut record = csv::StringRecord::new();
    while reader
        .read_record(&mut record)
        .context("Failed to read CSV record")?
    {
        let line = lines.locate(&record, reader.position().line());
        ingest.push_row(&mut sender, &record, line).await?;
    }
    ingest.flush(&mut sender).await?;
    sender.finish().await?;
    Ok(LoadTotals {
        sent: sender.total_sent,
        failed: sender.total_failed,
        ledger: ingest.ledger,
    })
}

pub(crate) fn parse_csv_cell(raw: &str) -> JsonValue {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return JsonValue::Null;
    }

    // Booleans
    match trimmed.to_ascii_lowercase().as_str() {
        "true" => return JsonValue::Bool(true),
        "false" => return JsonValue::Bool(false),
        _ => {}
    }

    // Integers (prefer unsigned for non-negative)
    if !trimmed.contains('.') && !trimmed.contains(['e', 'E']) {
        if trimmed.starts_with('-') {
            if let Ok(v) = trimmed.parse::<i64>() {
                return JsonValue::Number(v.into());
            }
        } else if let Ok(v) = trimmed.parse::<u64>() {
            return JsonValue::Number(serde_json::Number::from(v));
        } else if let Ok(v) = trimmed.parse::<i64>() {
            return JsonValue::Number(v.into());
        }
    }

    // Floating point
    if let Ok(v) = trimmed.parse::<f64>()
        && let Some(num) = serde_json::Number::from_f64(v)
    {
        return JsonValue::Number(num);
    }

    // Fallback to string (dates/IPs will be inferred from string content)
    JsonValue::String(trimmed.to_string())
}

/// What spreadsheets and data-frame exports write in a cell that has no value.
///
/// Only a field that cannot hold text reads these as missing: in a text column `NA` is as likely
/// to be a value — Namibia, a grade, an answer — as the absence of one.
const MISSING_MARKERS: &[&str] = &["na", "n/a", "#n/a", "nan", "null", "none", "nil", "-"];

pub(crate) fn is_missing_marker(cell: &str) -> bool {
    MISSING_MARKERS
        .iter()
        .any(|marker| cell.eq_ignore_ascii_case(marker))
}

/// The order of day and month in a numeric date written with the year last.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DateOrder {
    MonthFirst,
    DayFirst,
}

/// The separators a numeric date is written with, and the order the node reads each in: slashes
/// American, month first; dots European, day first.
const NODE_DATE_ORDERS: [(char, DateOrder); 2] =
    [('/', DateOrder::MonthFirst), ('.', DateOrder::DayFirst)];

/// How one column writes its numeric dates, per separator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DateOrders {
    pub(crate) slash: DateOrder,
    pub(crate) dot: DateOrder,
}

/// The node's reading, which a column keeps unless its sample says otherwise.
impl Default for DateOrders {
    fn default() -> Self {
        Self {
            slash: DateOrder::MonthFirst,
            dot: DateOrder::DayFirst,
        }
    }
}

impl DateOrders {
    fn of(&self, separator: char) -> DateOrder {
        if separator == '/' {
            self.slash
        } else {
            self.dot
        }
    }

    pub(crate) fn set(&mut self, separator: char, order: DateOrder) {
        if separator == '/' {
            self.slash = order;
        } else {
            self.dot = order;
        }
    }

    /// What this column does that the node would read the other way, said with an example.
    pub(crate) fn departures(&self) -> Vec<&'static str> {
        let mut departures = Vec::new();
        if self.slash == DateOrder::DayFirst {
            departures.push("slash dates day first (such as 15/03/2024)");
        }
        if self.dot == DateOrder::MonthFirst {
            departures.push("dotted dates month first (such as 03.15.2024)");
        }
        departures
    }
}

/// A numeric date with a four-digit year last: `15/03/2024`, `03.15.2024 16:13`.
struct NumericDate<'a> {
    separator: char,
    first: u32,
    second: u32,
    year: &'a str,
    time: Option<&'a str>,
}

fn numeric_date_parts(cell: &str) -> Option<NumericDate<'_>> {
    let (date, time) = match cell.split_once(' ') {
        Some((date, time)) => (date, Some(time.trim())),
        None => (cell, None),
    };
    let separator = date.chars().find(|c| !c.is_ascii_digit())?;
    if !NODE_DATE_ORDERS
        .iter()
        .any(|(known, _)| *known == separator)
    {
        return None;
    }
    let mut parts = date.split(separator);
    let (first, second, year) = (parts.next()?, parts.next()?, parts.next()?);
    let is_number = |part: &str, digits: std::ops::RangeInclusive<usize>| {
        digits.contains(&part.len()) && part.bytes().all(|b| b.is_ascii_digit())
    };
    if parts.next().is_some()
        || !is_number(first, 1..=2)
        || !is_number(second, 1..=2)
        || !is_number(year, 4..=4)
    {
        return None;
    }
    Some(NumericDate {
        separator,
        first: first.parse().ok()?,
        second: second.parse().ok()?,
        year,
        time,
    })
}

/// Whether a cell is a numeric date at all, `03/04/2024` or `15.03.2024`.
pub(crate) fn is_numeric_date(cell: &str) -> bool {
    numeric_date_parts(cell).is_some()
}

/// What one numeric date says about its column's order, when only one reading of it is a date:
/// `15/03/2024` can only be day first, `03.15.2024` only month first, and `03/04/2024` says
/// nothing.
pub(crate) fn numeric_date_order(cell: &str) -> Option<(char, DateOrder)> {
    let date = numeric_date_parts(cell)?;
    match (date.first, date.second) {
        (13..=31, 1..=12) => Some((date.separator, DateOrder::DayFirst)),
        (1..=12, 13..=31) => Some((date.separator, DateOrder::MonthFirst)),
        _ => None,
    }
}

/// A numeric date the node would read the other way, as the ISO date the column means, its time
/// kept: in a day-first slash column `15/03/2024 16:13` becomes `2024-03-15 16:13`. `None` for a
/// date the node reads as meant, anything that is not a numeric date, or a day the calendar
/// does not have.
pub(crate) fn reordered_date(cell: &str, orders: &DateOrders) -> Option<String> {
    let date = numeric_date_parts(cell)?;
    let order = orders.of(date.separator);
    if order == DateOrders::default().of(date.separator) {
        return None;
    }
    let (day, month) = match order {
        DateOrder::DayFirst => (date.first, date.second),
        DateOrder::MonthFirst => (date.second, date.first),
    };
    let ymd = format!("{}-{month:02}-{day:02}", date.year);
    let iso = match date.time {
        Some(time) => format!("{ymd} {time}"),
        None => ymd,
    };
    storage::parse_date_to_timestamp_secs(&iso).map(|_| iso)
}

/// How the loader reads one column: the type its field declares, how it writes its numeric
/// dates, and whether each value is a list of them.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ColumnShape {
    pub(crate) field_type: Option<TantivyFieldType>,
    pub(crate) dates: DateOrders,
    /// Each value is a list written into the cell, `['a', 'b']`, loaded as several values.
    pub(crate) list: bool,
}

/// A CSV cell as the value its field can hold.
///
/// A CSV cell has no type; the field it lands in does, and reading the cell by its own look
/// instead was how a load went wrong in three ways. `NA` in a count column was sent as text and
/// the row refused. `20240315` in a date column was sent as a number, which a date field reads
/// as seconds since 1970 — the row landed in August 1970. And a text column's `007` was sent as
/// the number 7. A column no field describes is still read by its look, as it always was.
///
/// A blank cell is no value in every field, a boolean one included: `false` would be a guess.
pub(crate) fn csv_cell(raw: &str, shape: &ColumnShape) -> JsonValue {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return JsonValue::Null;
    }
    if shape.list
        && let Some(items) = parse_list(trimmed)
    {
        return fit_list(items, shape);
    }
    fit_text(trimmed, shape)
}

/// A non-blank text value as its field reads it.
fn fit_text(trimmed: &str, shape: &ColumnShape) -> JsonValue {
    match &shape.field_type {
        Some(TantivyFieldType::Text | TantivyFieldType::String | TantivyFieldType::Ip) => {
            JsonValue::String(trimmed.to_string())
        }
        Some(
            TantivyFieldType::I64
            | TantivyFieldType::U64
            | TantivyFieldType::F64
            | TantivyFieldType::Date
            | TantivyFieldType::Boolean,
        ) if is_missing_marker(trimmed) => JsonValue::Null,
        Some(TantivyFieldType::Date) => date_cell(trimmed, &shape.dates),
        Some(TantivyFieldType::Boolean) => boolean_word(trimmed)
            .map(JsonValue::Bool)
            .unwrap_or_else(|| JsonValue::String(trimmed.to_string())),
        _ => parse_csv_cell(trimmed),
    }
}

/// A list's elements, each fitted to the field, as several values of it; no value at all when
/// none is left.
fn fit_list(items: Vec<JsonValue>, shape: &ColumnShape) -> JsonValue {
    let element = ColumnShape {
        list: false,
        ..shape.clone()
    };
    let values: Vec<JsonValue> = items
        .into_iter()
        .map(|item| fit_json(item, &element))
        .filter(|value| !value.is_null())
        .collect();
    if values.is_empty() {
        JsonValue::Null
    } else {
        JsonValue::Array(values)
    }
}

/// A JSON value as the value its field can hold: the conversions a CSV cell gets, for a source
/// that writes `"12"` for a count, `"NA"` or `""` for none, or `"TRUE"` for a flag. A text field
/// keeps the string it is given; a json, bytes or facet field takes the value as it is.
pub(crate) fn fit_json(value: JsonValue, shape: &ColumnShape) -> JsonValue {
    use TantivyFieldType as T;
    let Some(field_type) = &shape.field_type else {
        return value;
    };
    if matches!(field_type, T::Json | T::Bytes | T::Facet) {
        return value;
    }
    let text_field = matches!(field_type, T::Text | T::String);
    match value {
        JsonValue::String(s) => {
            let trimmed = s.trim();
            if shape.list
                && let Some(items) = parse_list(trimmed)
            {
                return fit_list(items, shape);
            }
            if text_field {
                JsonValue::String(s)
            } else if trimmed.is_empty() {
                JsonValue::Null
            } else {
                fit_text(trimmed, shape)
            }
        }
        JsonValue::Number(n) if text_field => JsonValue::String(n.to_string()),
        JsonValue::Number(n) if *field_type == T::Boolean => {
            let number = JsonValue::Number(n);
            boolean_value(&number).map_or(number, JsonValue::Bool)
        }
        JsonValue::Bool(b) if text_field => JsonValue::String(b.to_string()),
        JsonValue::Array(items) => fit_list(items, shape),
        other => other,
    }
}

/// A date cell as the node's date parser reads it. Sent as the text it is whenever that text is
/// a date — `20240315` and `2024` are dates as text and seconds as numbers — and as a number only
/// when it is not: seconds before 2000, or before 1970, which the parser does not take as text.
fn date_cell(trimmed: &str, dates: &DateOrders) -> JsonValue {
    if let Some(iso) = reordered_date(trimmed, dates) {
        return JsonValue::String(iso);
    }
    if storage::parse_date_to_timestamp_secs(trimmed).is_none()
        && let Ok(seconds) = trimmed.parse::<i64>()
    {
        return JsonValue::Number(seconds.into());
    }
    JsonValue::String(trimmed.to_string())
}

/// Where each record of a delimited file starts, as `sed -n <N>p` counts lines.
///
/// The reader's `position()` for a record is taken before it skips blank lines, and — after a
/// record ending CRLF — before the `\n` of that ending, which it consumes as the next record
/// starts. So on a Windows-saved file every line reported was the one above the row, and after a
/// blank line every one after it was short again. The reader's position *after* a record has
/// consumed all of that: the record's first line is there, less the lines its quoted fields
/// span, less its own line ending when the reader has already consumed it — which it has after
/// `\n` and has not yet after `\r\n`, as the header's ending tells.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordLines {
    ending_pending: bool,
    next_start: u64,
}

fn line_breaks_in(record: &csv::StringRecord) -> u64 {
    record
        .iter()
        .map(|field| field.bytes().filter(|b| *b == b'\n').count() as u64)
        .sum()
}

impl RecordLines {
    /// From the header and the reader's line after reading it.
    pub(crate) fn after_header(header: &csv::StringRecord, reader_line: u64) -> Self {
        let header_breaks = line_breaks_in(header);
        Self {
            ending_pending: reader_line == 1 + header_breaks,
            next_start: header_breaks + 2,
        }
    }

    /// The line `record` starts on, from the reader's line after reading it. Called for every
    /// record, in order, skipped or not.
    pub(crate) fn locate(&mut self, record: &csv::StringRecord, reader_line: u64) -> u64 {
        let breaks = line_breaks_in(record);
        let own_ending = u64::from(!self.ending_pending);
        // Never above where the previous record ended: the last record of a file without a
        // final line ending has no ending to subtract.
        let start = reader_line
            .saturating_sub(breaks + own_ending)
            .max(self.next_start);
        self.next_start = start + breaks + 1;
        start
    }
}

pub(crate) async fn fetch_bytes_source(client: &CameoClient, source: &str) -> Result<Vec<u8>> {
    let compression = detect_compression(source);
    let is_http = source.starts_with("http://") || source.starts_with("https://");

    let raw_bytes = if is_http {
        let url = Url::parse(source).context("Invalid URL for schema source")?;
        let bytes = client
            .source_http()
            .get(url)
            .send()
            .await
            .context("Failed to fetch remote schema")?
            .bytes()
            .await
            .context("Failed to read remote schema body")?;
        bytes.to_vec()
    } else {
        let path = Path::new(source);
        fs::read(path).with_context(|| format!("Failed to read schema file: {}", path.display()))?
    };

    decompress_bytes(raw_bytes, compression)
}
