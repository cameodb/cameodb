//! The ingest pipeline: source and compression detection, the JSON stream parsers,
//! schema detection, and the CSV/JSON loaders behind `schema` and `data` commands.

use super::*;
use crate::sdk::CameoClient;
use anyhow::{Context, Result, anyhow};
use csv::ReaderBuilder;
use flate2::read::GzDecoder;
use reqwest::Url;
use serde_json::Map as JsonMap;
use serde_json::Value as JsonValue;
use serde_json::json;
use std::collections::HashSet;
use std::fs;
use std::io::{BufRead, BufReader, Cursor, Read};
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use storage::{FieldDef, IndexSchema, TantivyFieldType};

// Only import colored on non-Windows platforms

pub(crate) const SCHEMA_SAMPLE_LIMIT: usize = 200;
pub(crate) const DEFAULT_BATCH_SIZE: usize = 4000;
pub(crate) const SOURCE_SNIFF_BYTES: usize = 64 * 1024;

/// Simple progress spinner for long-running operations
pub(crate) struct ProgressSpinner {
    pub(crate) active: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) handle: Option<thread::JoinHandle<()>>,
}

impl ProgressSpinner {
    pub(crate) fn new() -> Self {
        let active = Arc::new(std::sync::atomic::AtomicBool::new(true));
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
                print!("\r{} ", spinner_chars[i % spinner_chars.len()]);
                std::io::Write::flush(&mut std::io::stdout()).ok();
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
            print!("\r");
            std::io::Write::flush(&mut std::io::stdout()).ok();
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

/// Result of ID field detection
#[derive(Debug)]
pub(crate) struct IdFieldDetection {
    pub(crate) index: usize,
    pub(crate) original_field_name: String,
    pub(crate) is_shadow: bool, // true if original field != "id"
}

/// The id-candidate ranking every source format shares: a field named exactly `id`,
/// then a hash column (`sha256`, `sha1`, `md5` in that order — more digest bits, better
/// spread), then anything ending in `id` (`user_id`, `videoId`), then anything containing
/// it. Inside a rank the first field in source order wins; an altogether unmatched list
/// falls back to its first field.
pub(crate) fn id_field_rank(name: &str) -> u8 {
    let lower = name.to_lowercase();
    if lower == "id" {
        0
    } else if lower == "sha256" {
        1
    } else if lower == "sha1" {
        2
    } else if lower == "md5" {
        3
    } else if lower.ends_with("id") {
        4
    } else if lower.contains("id") {
        5
    } else {
        6
    }
}

pub(crate) fn detect_id_field_index<'a>(names: impl Iterator<Item = &'a str>) -> Option<usize> {
    names
        .enumerate()
        .min_by_key(|(_, name)| id_field_rank(name))
        .map(|(idx, _)| idx)
}

/// Detect the ID field from CSV headers with shadow field support.
pub(crate) fn detect_id_field(headers: &[(String, Option<TantivyFieldType>)]) -> IdFieldDetection {
    let index = detect_id_field_index(headers.iter().map(|(name, _)| name.as_str()))
        .expect("CSV headers are non-empty");
    let name = &headers[index].0;
    let lower = name.to_lowercase();
    IdFieldDetection {
        index,
        // Exact and hash matches canonicalize to lowercase — "ID" and "SHA256" name the
        // same field their spelling hides. Suffix and substring matches keep the source
        // spelling, which is what the shadow field is named after.
        original_field_name: if id_field_rank(name) <= 3 {
            lower.clone()
        } else {
            name.clone()
        },
        is_shadow: lower != "id",
    }
}

/// The CSV schema's final pass, run the same way whether the schema was built by
/// `detect_schema_from_csv` or mid-ingest: every non-shadow field is marked indexed —
/// an explicit load is not write-time evolution, where fields start non-indexed — and
/// only `id` stays stored in Tantivy (the rest comes from redb). Shadow fields keep
/// their non-indexed, non-stored status, and header type hints are applied last.
pub(crate) fn finalize_csv_schema(
    schema: &mut IndexSchema,
    headers: &[(String, Option<TantivyFieldType>)],
) {
    for (name, field_def) in schema.fields.iter_mut() {
        if !field_def.is_shadow {
            field_def.indexed = true;
            field_def.stored = name == "id";
        }
    }

    for (name, hint) in headers {
        if let Some(t) = hint.clone()
            && !schema.fields.get(name).is_some_and(|f| f.is_shadow)
        {
            // FieldDef::new already sets the stored flag — only 'id' = true.
            let mut field_def = FieldDef::new(name.clone(), t);
            field_def.indexed = true;
            schema.fields.insert(name.clone(), field_def);
        }
    }
}

/// Build the schema a buffered CSV sample describes: the shadow for a non-`id` source
/// column, one evolution pass per sampled row, then the CSV finalization.
pub(crate) fn csv_sample_schema(
    headers: &[(String, Option<TantivyFieldType>)],
    id_detection: &IdFieldDetection,
    rows: &[csv::StringRecord],
) -> Result<JsonValue> {
    let mut schema = IndexSchema::default();
    if id_detection.is_shadow {
        let field_type = headers[id_detection.index]
            .1
            .clone()
            .unwrap_or(TantivyFieldType::Text);
        schema.add_shadow_field(id_detection.original_field_name.clone(), field_type);
    }

    for row in rows {
        let mut obj: JsonMap<String, JsonValue> = JsonMap::new();
        for (idx, value) in row.iter().enumerate() {
            if let Some((header, _)) = headers.get(idx) {
                obj.insert(header.clone(), parse_csv_cell(value));
            }
        }
        if let Some(raw_id) = row.get(id_detection.index) {
            let id_val = raw_id.trim();
            if !id_val.is_empty() {
                obj.insert("id".to_string(), JsonValue::String(id_val.to_string()));
            }
        }
        schema.evolve_from_document(&JsonValue::Object(obj));
    }

    finalize_csv_schema(&mut schema, headers);
    serde_json::to_value(&schema).context("Failed to serialize schema")
}

/// Serialize one CSV record as the NDJSON payload line the ingest stream accepts:
/// canonical `id`, the routing key (the source column's value, or the id), and the row.
pub(crate) fn csv_ndjson_line(
    record: &csv::StringRecord,
    headers: &[(String, Option<TantivyFieldType>)],
    id_detection: &IdFieldDetection,
    id_header: &str,
) -> Result<Vec<u8>> {
    let id_value = record
        .get(id_detection.index)
        .unwrap_or_default()
        .trim()
        .to_string();
    let mut doc_obj: JsonMap<String, JsonValue> = JsonMap::new();
    for (idx, value) in record.iter().enumerate() {
        if let Some((header, _)) = headers.get(idx) {
            doc_obj.insert(header.clone(), parse_csv_cell(value));
        }
    }
    doc_obj.insert("id".to_string(), JsonValue::String(id_value.clone()));
    let routing_key = doc_obj
        .get(id_header)
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| id_value.clone());
    let payload = json!({"id": id_value, "routing_key": routing_key, "doc": doc_obj});
    let mut line = serde_json::to_vec(&payload).context("Failed to serialize CSV payload")?;
    line.push(b'\n');
    Ok(line)
}

/// The CSV loader's batch state: payload serialization, accumulation, and the flush
/// that ships a full batch. Kept as a struct because the loader's two phases (replay
/// the sample buffer, then stream) both write through it.
pub(crate) struct CsvIngest {
    pub(crate) headers: Vec<(String, Option<TantivyFieldType>)>,
    pub(crate) id_detection: IdFieldDetection,
    pub(crate) id_header: String,
    pub(crate) batch_size: usize,
    pub(crate) batch_body: Vec<u8>,
    pub(crate) docs_in_batch: usize,
    pub(crate) total_sent: usize,
    pub(crate) total_failed: usize,
}

impl CsvIngest {
    pub(crate) fn new(
        headers: Vec<(String, Option<TantivyFieldType>)>,
        id_detection: IdFieldDetection,
        id_header: String,
        batch_size: usize,
    ) -> Self {
        Self {
            headers,
            id_detection,
            id_header,
            batch_size: batch_size.max(1),
            batch_body: Vec::new(),
            docs_in_batch: 0,
            total_sent: 0,
            total_failed: 0,
        }
    }

    pub(crate) async fn push_row(
        &mut self,
        client: &CameoClient,
        index: &str,
        record: &csv::StringRecord,
    ) -> Result<()> {
        let line = csv_ndjson_line(record, &self.headers, &self.id_detection, &self.id_header)?;
        self.batch_body.extend_from_slice(&line);
        self.docs_in_batch += 1;
        if self.docs_in_batch >= self.batch_size {
            self.flush(client, index).await?;
        }
        Ok(())
    }

    /// Build the schema the buffered rows describe, install it, then replay the rows as
    /// ordinary data — the order matters, the index must have its schema first.
    pub(crate) async fn create_schema_and_drain(
        &mut self,
        client: &CameoClient,
        index: &str,
        sample_rows: &mut Vec<csv::StringRecord>,
    ) -> Result<()> {
        let schema_json = csv_sample_schema(&self.headers, &self.id_detection, sample_rows)?;
        client
            .put_index_config(index, &schema_json)
            .await
            .with_context(|| format!("Failed to create schema for index '{}'", index))?;
        println!(
            "Schema was missing; detected and applied schema to index '{}'",
            index
        );
        for row in sample_rows.drain(..) {
            self.push_row(client, index, &row).await?;
        }
        Ok(())
    }

    pub(crate) async fn flush(&mut self, client: &CameoClient, index: &str) -> Result<()> {
        flush_ndjson_batch(
            client,
            index,
            &mut self.batch_body,
            &mut self.total_sent,
            &mut self.total_failed,
        )
        .await?;
        self.docs_in_batch = 0;
        Ok(())
    }
}

pub(crate) async fn detect_schema_from_csv(
    client: &CameoClient,
    source: &str,
    delimiter: Delimiter,
) -> Result<JsonValue> {
    let mut reader = open_csv_reader(client, source, delimiter).await?;
    let raw_headers = reader
        .headers()
        .context("CSV file is missing headers")?
        .clone();

    let headers: Vec<(String, Option<TantivyFieldType>)> =
        raw_headers.iter().map(parse_header_with_hint).collect();

    let id_detection = detect_id_field(&headers);

    let mut schema = IndexSchema::default();
    let mut sampled = 0usize;

    // Add a single shadow field for the detected id source when its name is not "id"
    if id_detection.is_shadow {
        let field_type = headers[id_detection.index]
            .1
            .clone()
            .unwrap_or(TantivyFieldType::Text);
        schema.add_shadow_field(id_detection.original_field_name.clone(), field_type);
    }

    // Collect id-like candidates (excluding primary) ordered by priority for equality promotion
    // Tuple: (priority, idx, name, hint, all_match, seen_any)
    let mut id_like_candidates: Vec<(u8, usize, String, Option<TantivyFieldType>, bool, bool)> =
        headers
            .iter()
            .enumerate()
            .filter_map(|(idx, (name, hint))| {
                if idx == id_detection.index {
                    return None;
                }

                let lower = name.to_lowercase();
                let (priority, looks_like_id) =
                    if ["sha256", "sha1", "md5"].contains(&lower.as_str()) {
                        (0u8, true)
                    } else if lower.ends_with("_id") || lower.ends_with("id") {
                        (1u8, true)
                    } else if lower.contains("id") {
                        (2u8, true)
                    } else {
                        (u8::MAX, false)
                    };

                if looks_like_id {
                    Some((priority, idx, name.clone(), hint.clone(), true, false))
                } else {
                    None
                }
            })
            .collect();
    id_like_candidates.sort_by_key(|(priority, _, _, _, _, _)| *priority);

    for record in reader.records() {
        let record = record.context("Failed to read CSV record")?;
        let mut obj: JsonMap<String, JsonValue> = JsonMap::new();

        let canonical_id_raw = record.get(id_detection.index).unwrap_or("");

        // Update equality flags for candidates
        for (_, idx, _, _, all_match, seen_any) in id_like_candidates.iter_mut() {
            if let Some(val) = record.get(*idx) {
                *seen_any = true;
                if *all_match && val.trim() != canonical_id_raw.trim() {
                    *all_match = false;
                }
            }
        }

        // Process all fields in CSV column order
        for (idx, value) in record.iter().enumerate() {
            if let Some((header, _)) = headers.get(idx) {
                let parsed = parse_csv_cell(value);
                obj.insert(header.clone(), parsed);
            }
        }

        // Inject canonical "id" field from the detected source field
        if let Some(raw_id) = record.get(id_detection.index) {
            let id_val = raw_id.trim();
            if !id_val.is_empty() {
                obj.insert("id".to_string(), JsonValue::String(id_val.to_string()));
            }
        }

        schema.evolve_from_document(&JsonValue::Object(obj));

        sampled += 1;
        if sampled >= SCHEMA_SAMPLE_LIMIT {
            break;
        }
    }

    // If canonical name was "id", promote the first candidate whose values always matched
    if !id_detection.is_shadow
        && let Some((_, _, name, hint, _all_match, _seen_any)) = id_like_candidates
            .into_iter()
            .find(|(_, _, _, _, all_match, seen_any)| *seen_any && *all_match)
    {
        let field_type = hint.unwrap_or(TantivyFieldType::Text);
        schema.add_shadow_field(name, field_type);
    }

    finalize_csv_schema(&mut schema, &headers);

    // Ensure 'id' field is explicitly defined in schema with proper settings
    if !schema.fields.contains_key("id") {
        let id_field = FieldDef {
            name: "id".to_string(),
            field_type: TantivyFieldType::Text,
            indexed: true,
            stored: true,
            fast: Some(false),
            is_shadow: false, // The canonical 'id' field is not a shadow field
            description: None,
            tokenizer: Some("raw".to_string()),
            index_record_option: Some("Basic".to_string()),
        };
        schema.fields.insert("id".to_string(), id_field);
    }

    schema.auto_detect_routing_field();

    let mut schema_json = serde_json::to_value(schema).context("Failed to serialize schema")?;

    // Reorder fields map: id first, then preserve CSV column order
    if let JsonValue::Object(ref mut root) = schema_json
        && let Some(JsonValue::Object(mut fields)) = root.remove("fields")
    {
        let mut ordered = JsonMap::new();

        // Always place 'id' first
        if let Some(id_val) = fields.remove("id") {
            ordered.insert("id".to_string(), id_val);
        }

        // Then add fields in CSV column order (preserving source structure)
        for (header_name, _) in &headers {
            if header_name != "id"
                && let Some(field_val) = fields.remove(header_name)
            {
                ordered.insert(header_name.clone(), field_val);
            }
        }

        // Add any remaining fields that weren't in headers (shouldn't happen, but safe)
        for (k, v) in fields {
            ordered.insert(k, v);
        }

        root.insert("fields".to_string(), JsonValue::Object(ordered));
    }

    Ok(schema_json)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourceFormat {
    SchemaJson,
    JsonDocument,
    JsonArray,
    JsonLines,
    CsvLike,
}

#[derive(Debug)]
pub(crate) struct JsonSourceAnalysis {
    pub(crate) sample_docs: Vec<JsonValue>,
    pub(crate) id_field: String,
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

pub(crate) fn collect_effective_json_documents(docs: &[JsonValue]) -> Result<Vec<JsonValue>> {
    docs.iter().map(effective_json_document).collect()
}

/// The same ranking as [`detect_id_field`], but JSON keeps the field's own spelling —
/// the shadow field is named after it.
pub(crate) fn detect_id_field_name(field_names: &[String]) -> Option<String> {
    detect_id_field_index(field_names.iter().map(String::as_str))
        .map(|idx| field_names[idx].clone())
}

pub(crate) fn detect_json_id_field_name(docs: &[JsonValue]) -> Result<String> {
    let mut field_names = Vec::new();
    let mut seen = HashSet::new();

    for doc in docs {
        let obj = doc
            .as_object()
            .ok_or_else(|| anyhow!("JSON source documents must be objects"))?;
        for key in obj.keys() {
            if seen.insert(key.clone()) {
                field_names.push(key.clone());
            }
        }
    }

    detect_id_field_name(&field_names)
        .ok_or_else(|| anyhow!("Unable to detect an id field from JSON documents"))
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

pub(crate) fn infer_json_field_type(docs: &[JsonValue], field_name: &str) -> TantivyFieldType {
    docs.iter()
        .filter_map(|doc| doc.as_object())
        .filter_map(|obj| obj.get(field_name))
        .find(|value| !value.is_null())
        .map(FieldDef::infer_type_from_value)
        .unwrap_or(TantivyFieldType::Text)
}

pub(crate) fn normalize_json_document_for_schema(
    doc: &JsonValue,
    id_field: &str,
) -> Result<JsonValue> {
    let mut obj = effective_json_document(doc)?
        .as_object()
        .cloned()
        .ok_or_else(|| anyhow!("JSON source documents must be objects"))?;

    let id = obj
        .get("id")
        .and_then(json_value_to_id_string)
        .or_else(|| obj.get(id_field).and_then(json_value_to_id_string))
        .ok_or_else(|| anyhow!("JSON document is missing a usable id field"))?;

    obj.insert("id".to_string(), JsonValue::String(id));
    Ok(JsonValue::Object(obj))
}

pub(crate) fn build_schema_from_effective_json_documents(
    effective_docs: &[JsonValue],
    id_field: &str,
) -> Result<JsonValue> {
    let mut schema = IndexSchema::default();
    if id_field != "id" {
        let field_type = infer_json_field_type(effective_docs, id_field);
        schema.add_shadow_field(id_field.to_string(), field_type);
    }

    let mut sampled = 0usize;
    for doc in effective_docs.iter().take(SCHEMA_SAMPLE_LIMIT) {
        let normalized = normalize_json_document_for_schema(doc, id_field)?;
        schema.evolve_from_document(&normalized);
        sampled += 1;
    }

    if sampled == 0 {
        anyhow::bail!("JSON source does not contain any valid object documents");
    }

    for (name, field_def) in schema.fields.iter_mut() {
        if !field_def.is_shadow {
            field_def.indexed = true;
            field_def.stored = name == "id";
        }
    }

    if !schema.fields.contains_key("id") {
        let id_field = FieldDef {
            name: "id".to_string(),
            field_type: TantivyFieldType::Text,
            indexed: true,
            stored: true,
            fast: Some(false),
            is_shadow: false,
            description: None,
            tokenizer: Some("raw".to_string()),
            index_record_option: Some("Basic".to_string()),
        };
        schema.fields.insert("id".to_string(), id_field);
    }

    schema.auto_detect_routing_field();

    serde_json::to_value(schema).context("Failed to serialize schema")
}

pub(crate) fn build_schema_from_json_documents(docs: &[JsonValue]) -> Result<JsonValue> {
    let effective_docs = collect_effective_json_documents(docs)?;
    let id_field = detect_json_id_field_name(&effective_docs)?;
    build_schema_from_effective_json_documents(&effective_docs, &id_field)
}

pub(crate) fn build_json_source_analysis_from_docs(
    docs: &[JsonValue],
) -> Result<JsonSourceAnalysis> {
    let effective_docs = collect_effective_json_documents(docs)?;
    if effective_docs.is_empty() {
        anyhow::bail!("JSON source does not contain any valid object documents");
    }

    let id_field = detect_json_id_field_name(&effective_docs)?;
    let sample_docs = effective_docs
        .into_iter()
        .take(SCHEMA_SAMPLE_LIMIT)
        .collect::<Vec<_>>();

    Ok(JsonSourceAnalysis {
        sample_docs,
        id_field,
    })
}

pub(crate) fn collect_json_analysis_doc(
    raw_doc: &JsonValue,
    sample_docs: &mut Vec<JsonValue>,
    field_names: &mut Vec<String>,
    seen: &mut HashSet<String>,
) -> Result<()> {
    let effective_doc = effective_json_document(raw_doc)?;
    let obj = effective_doc
        .as_object()
        .ok_or_else(|| anyhow!("JSON source documents must be objects"))?;

    if sample_docs.len() < SCHEMA_SAMPLE_LIMIT {
        sample_docs.push(JsonValue::Object(obj.clone()));
    }

    for key in obj.keys() {
        if seen.insert(key.clone()) {
            field_names.push(key.clone());
        }
    }

    Ok(())
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

/// The shared tail of every analyze pass: a stream that produced no usable document
/// cannot describe a schema, and one that did must name its id field.
pub(crate) fn json_source_analysis(
    sample_docs: Vec<JsonValue>,
    field_names: Vec<String>,
    count: usize,
) -> Result<JsonSourceAnalysis> {
    if count == 0 || sample_docs.is_empty() {
        anyhow::bail!("JSON source does not contain any valid object documents");
    }

    let id_field = detect_id_field_name(&field_names)
        .ok_or_else(|| anyhow!("Unable to detect an id field from JSON documents"))?;

    Ok(JsonSourceAnalysis {
        sample_docs,
        id_field,
    })
}

pub(crate) fn analyze_local_json_source_for_schema(
    source: &str,
    format: SourceFormat,
) -> Result<JsonSourceAnalysis> {
    let reader = open_local_reader(Path::new(source), detect_compression(source))?;
    analyze_reader_json_source_for_schema(reader, format)
}

pub(crate) fn analyze_reader_json_source_for_schema<R: Read>(
    reader: R,
    format: SourceFormat,
) -> Result<JsonSourceAnalysis> {
    let mut sample_docs = Vec::new();
    let mut field_names = Vec::new();
    let mut seen = HashSet::new();

    let count = for_each_json_document_in_reader(reader, format, |raw_doc| {
        collect_json_analysis_doc(&raw_doc, &mut sample_docs, &mut field_names, &mut seen)
    })?;

    json_source_analysis(sample_docs, field_names, count)
}

pub(crate) fn build_doc_payload_from_json_document(
    raw_doc: &JsonValue,
    id_field: &str,
) -> Result<JsonValue> {
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

    let id = raw_obj
        .get("id")
        .and_then(json_value_to_id_string)
        .or_else(|| doc_obj.get("id").and_then(json_value_to_id_string))
        .or_else(|| doc_obj.get(id_field).and_then(json_value_to_id_string))
        .ok_or_else(|| anyhow!("JSON document is missing a usable id field"))?;

    doc_obj.insert("id".to_string(), JsonValue::String(id.clone()));

    let routing_key = raw_obj
        .get("routing_key")
        .and_then(json_value_to_id_string)
        .or_else(|| doc_obj.get(id_field).and_then(json_value_to_id_string))
        .unwrap_or_else(|| id.clone());

    Ok(json!({
        "id": id,
        "routing_key": routing_key,
        "doc": JsonValue::Object(doc_obj),
    }))
}

pub(crate) fn record_ingest_response(
    response: &JsonValue,
    total_sent: &mut usize,
    total_failed: &mut usize,
) {
    let written = response
        .get("items_written")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;
    let errors_json = response.get("errors").and_then(|v| v.as_array());
    let error_count = errors_json.map(|e| e.len()).unwrap_or(0);

    *total_sent += written;
    *total_failed += error_count;

    if error_count > 0 {
        eprintln!(
            "⚠️  Batch warning: {} items failed validation.",
            error_count
        );
        if let Some(errs) = errors_json {
            for err in errs.iter().take(3) {
                eprintln!("   - {}", err.as_str().unwrap_or("Unknown error"));
            }
            if errs.len() > 3 {
                eprintln!("   ... and {} more", errs.len() - 3);
            }
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

pub(crate) async fn analyze_http_json_source_for_schema(
    client: &CameoClient,
    source: &str,
    format: SourceFormat,
) -> Result<JsonSourceAnalysis> {
    let mut sample_docs = Vec::new();
    let mut field_names = Vec::new();
    let mut seen = HashSet::new();

    let count = for_each_json_document_in_http_source(client, source, format, |raw_doc| {
        collect_json_analysis_doc(&raw_doc, &mut sample_docs, &mut field_names, &mut seen)
    })
    .await?;

    json_source_analysis(sample_docs, field_names, count)
}

pub(crate) async fn analyze_json_source_for_schema(
    client: &CameoClient,
    source: &str,
    format: SourceFormat,
) -> Result<JsonSourceAnalysis> {
    match format {
        SourceFormat::JsonDocument => {
            let value = load_json_value_from_source(client, source).await?;
            if value.get("fields").is_some() {
                anyhow::bail!("Schema JSON object cannot be used as document data");
            }
            build_json_source_analysis_from_docs(&[value])
        }
        SourceFormat::JsonArray | SourceFormat::JsonLines => {
            let compression = detect_compression(source);
            if is_http_source(source) && compression == Compression::None {
                analyze_http_json_source_for_schema(client, source, format).await
            } else if is_http_source(source) {
                // Compressed remote: download all, decompress, analyze in memory
                let bytes = fetch_bytes_source(client, source).await?;
                tokio::task::spawn_blocking(move || {
                    let reader = Cursor::new(bytes);
                    analyze_reader_json_source_for_schema(reader, format)
                })
                .await
                .map_err(|err| anyhow!("Compressed JSON source analysis failed: {}", err))?
            } else {
                let source = source.to_string();
                tokio::task::spawn_blocking(move || {
                    analyze_local_json_source_for_schema(&source, format)
                })
                .await
                .map_err(|err| anyhow!("Local JSON source analysis failed: {}", err))?
            }
        }
        SourceFormat::SchemaJson => Err(anyhow!(
            "Schema JSON object cannot be used as document data"
        )),
        SourceFormat::CsvLike => Err(anyhow!("Source is not JSON data")),
    }
}

pub(crate) async fn flush_ndjson_batch(
    client: &CameoClient,
    index: &str,
    batch_body: &mut Vec<u8>,
    total_sent: &mut usize,
    total_failed: &mut usize,
) -> Result<()> {
    if batch_body.is_empty() {
        return Ok(());
    }

    let response = client
        .stream_index_ndjson(index, std::mem::take(batch_body))
        .await?;
    record_ingest_response(&response, total_sent, total_failed);
    Ok(())
}

/// What a document pushed through [`JsonIngestPipeline`] made ready. The schema is
/// emitted once, the moment the sample names an id field; a batch is emitted each time
/// the buffer fills — and once at the end with whatever is left.
pub(crate) enum JsonIngestEvent {
    CreateSchema(JsonSourceAnalysis),
    DataBatch(Vec<u8>),
}

/// The single-pass JSON ingest protocol both loaders run: buffer up to
/// `SCHEMA_SAMPLE_LIMIT` documents until their fields name an id, emit the schema (when
/// the index has none), replay the buffer as the first batches, then stream the rest
/// straight into NDJSON batches. What differs between the loaders is only how the events
/// are delivered — awaited inline for an HTTP stream, sent down a channel from the
/// blocking reader thread.
pub(crate) struct JsonIngestPipeline {
    pub(crate) schema_exists: bool,
    pub(crate) batch_size: usize,
    pub(crate) sample_docs: Vec<JsonValue>,
    pub(crate) raw_sample_docs: Vec<JsonValue>,
    pub(crate) field_names: Vec<String>,
    pub(crate) seen_fields: HashSet<String>,
    pub(crate) batch_body: Vec<u8>,
    pub(crate) docs_in_batch: usize,
    pub(crate) id_field: Option<String>,
    pub(crate) samples_flushed: bool,
}

impl JsonIngestPipeline {
    pub(crate) fn new(batch_size: usize, schema_exists: bool) -> Self {
        Self {
            schema_exists,
            batch_size: batch_size.max(1),
            sample_docs: Vec::new(),
            raw_sample_docs: Vec::new(),
            field_names: Vec::new(),
            seen_fields: HashSet::new(),
            batch_body: Vec::new(),
            docs_in_batch: 0,
            id_field: None,
            samples_flushed: false,
        }
    }

    pub(crate) fn push(
        &mut self,
        raw_doc: &JsonValue,
        events: &mut Vec<JsonIngestEvent>,
    ) -> Result<()> {
        if self.samples_flushed {
            return self.append_doc(raw_doc, events);
        }

        let effective_doc = effective_json_document(raw_doc)?;
        let obj = effective_doc
            .as_object()
            .ok_or_else(|| anyhow!("JSON source documents must be objects"))?;
        if self.sample_docs.len() < SCHEMA_SAMPLE_LIMIT {
            self.sample_docs.push(JsonValue::Object(obj.clone()));
            self.raw_sample_docs.push(raw_doc.clone());
            for key in obj.keys() {
                if self.seen_fields.insert(key.clone()) {
                    self.field_names.push(key.clone());
                }
            }
        }
        if self.id_field.is_none() && self.sample_docs.len() >= SCHEMA_SAMPLE_LIMIT {
            self.id_field = Some(self.detect_id_field()?);
        }
        if self.id_field.is_some() {
            self.flush_samples(events)?;
        }
        Ok(())
    }

    /// End of stream: name the id field from whatever sample the source produced, replay
    /// it, and emit the last partial batch. An empty source is simply an empty load.
    pub(crate) fn finish(&mut self, events: &mut Vec<JsonIngestEvent>) -> Result<()> {
        if !self.samples_flushed && !self.sample_docs.is_empty() {
            if self.id_field.is_none() {
                self.id_field = Some(self.detect_id_field()?);
            }
            self.flush_samples(events)?;
        }
        if !self.batch_body.is_empty() {
            events.push(JsonIngestEvent::DataBatch(std::mem::take(
                &mut self.batch_body,
            )));
            self.docs_in_batch = 0;
        }
        Ok(())
    }

    pub(crate) fn detect_id_field(&self) -> Result<String> {
        detect_id_field_name(&self.field_names)
            .ok_or_else(|| anyhow!("Unable to detect an id field from JSON documents"))
    }

    /// The sample has named an id field: emit the schema built from it, then replay the
    /// buffered documents through the normal append path so they land in batches.
    pub(crate) fn flush_samples(&mut self, events: &mut Vec<JsonIngestEvent>) -> Result<()> {
        if !self.schema_exists {
            events.push(JsonIngestEvent::CreateSchema(JsonSourceAnalysis {
                sample_docs: self.sample_docs.clone(),
                id_field: self.id_field.clone().expect("set before flush_samples"),
            }));
        }
        let buffered = std::mem::take(&mut self.raw_sample_docs);
        self.samples_flushed = true;
        for raw_doc in &buffered {
            self.append_doc(raw_doc, events)?;
        }
        Ok(())
    }

    pub(crate) fn append_doc(
        &mut self,
        raw_doc: &JsonValue,
        events: &mut Vec<JsonIngestEvent>,
    ) -> Result<()> {
        let id_field = self
            .id_field
            .as_deref()
            .expect("set before the first append");
        let payload = build_doc_payload_from_json_document(raw_doc, id_field)?;
        let mut line = serde_json::to_vec(&payload).context("Failed to serialize JSON payload")?;
        line.push(b'\n');
        self.batch_body.extend_from_slice(&line);
        self.docs_in_batch += 1;
        if self.docs_in_batch >= self.batch_size {
            events.push(JsonIngestEvent::DataBatch(std::mem::take(
                &mut self.batch_body,
            )));
            self.docs_in_batch = 0;
        }
        Ok(())
    }
}

/// Deliver one event the HTTP way: the schema becomes a `PUT /index` and a batch becomes
/// an NDJSON stream. The reader loader's consumer runs the same match per channel message.
pub(crate) async fn deliver_json_ingest_event(
    client: &CameoClient,
    index: &str,
    event: JsonIngestEvent,
    total_sent: &mut usize,
    total_failed: &mut usize,
) -> Result<()> {
    match event {
        JsonIngestEvent::CreateSchema(analysis) => {
            let schema = build_schema_from_effective_json_documents(
                &analysis.sample_docs,
                &analysis.id_field,
            )
            .context("Failed to detect schema while auto-creating index schema")?;
            client
                .put_index_config(index, &schema)
                .await
                .with_context(|| format!("Failed to create schema for index '{}'", index))?;
            println!(
                "Schema was missing; detected and applied schema to index '{}'",
                index
            );
        }
        JsonIngestEvent::DataBatch(batch_body) => {
            let response = client.stream_index_ndjson(index, batch_body).await?;
            record_ingest_response(&response, total_sent, total_failed);
        }
    }
    Ok(())
}

pub(crate) async fn load_data_from_http_json_source_single_pass(
    client: &CameoClient,
    index: &str,
    source: &str,
    format: SourceFormat,
    batch_size: usize,
    schema_exists: bool,
) -> Result<()> {
    let batch_size = batch_size.max(1);
    let mut spinner = ProgressSpinner::new();

    let result: Result<(usize, usize)> = async {
        let mut response = open_http_json_stream(client, source).await?;
        let mut parser = JsonChunkParser::new(format)?;
        let mut pipeline = JsonIngestPipeline::new(batch_size, schema_exists);
        let mut events = Vec::new();
        let mut total_sent = 0usize;
        let mut total_failed = 0usize;

        while let Some(chunk) = response
            .chunk()
            .await
            .context("Failed to read remote JSON source body")?
        {
            for raw_doc in parser.push_chunk(&chunk)? {
                pipeline.push(&raw_doc, &mut events)?;
                for event in events.drain(..) {
                    deliver_json_ingest_event(
                        client,
                        index,
                        event,
                        &mut total_sent,
                        &mut total_failed,
                    )
                    .await?;
                }
            }
        }
        for raw_doc in parser.finish()? {
            pipeline.push(&raw_doc, &mut events)?;
            for event in events.drain(..) {
                deliver_json_ingest_event(client, index, event, &mut total_sent, &mut total_failed)
                    .await?;
            }
        }
        pipeline.finish(&mut events)?;
        for event in events.drain(..) {
            deliver_json_ingest_event(client, index, event, &mut total_sent, &mut total_failed)
                .await?;
        }
        Ok((total_sent, total_failed))
    }
    .await;

    spinner.stop();
    let (total_sent, total_failed) = result?;

    println!(
        "Ingestion complete for index '{}': loaded={} failed={} (batch size {})",
        index, total_sent, total_failed, batch_size
    );
    Ok(())
}

pub(crate) async fn load_data_from_reader_json_source_single_pass(
    client: &CameoClient,
    index: &str,
    reader: Box<dyn Read + Send + 'static>,
    format: SourceFormat,
    batch_size: usize,
    schema_exists: bool,
) -> Result<()> {
    let batch_size = batch_size.max(1);
    let mut spinner = ProgressSpinner::new();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<JsonIngestEvent>(2);

    // The reader is blocking, so the pipeline runs on a worker thread and reports
    // readiness as events; the async side delivers them in the order they arrive.
    let producer = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut pipeline = JsonIngestPipeline::new(batch_size, schema_exists);
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
        pipeline.finish(&mut events)?;
        drain(&mut events)?;
        Ok(())
    });

    let mut total_sent = 0usize;
    let mut total_failed = 0usize;

    let send_result: Result<()> = async {
        while let Some(event) = rx.recv().await {
            deliver_json_ingest_event(client, index, event, &mut total_sent, &mut total_failed)
                .await?;
        }
        Ok(())
    }
    .await;

    drop(rx);
    producer
        .await
        .map_err(|err| anyhow!("Local JSON batch producer failed: {}", err))??;

    spinner.stop();
    send_result?;

    println!(
        "Ingestion complete for index '{}': loaded={} failed={} (batch size {})",
        index, total_sent, total_failed, batch_size
    );
    Ok(())
}

pub(crate) async fn detect_schema_from_source(
    client: &CameoClient,
    source: &str,
    delimiter: Delimiter,
) -> Result<JsonValue> {
    load_schema_from_source(client, source, delimiter).await
}

pub(crate) async fn load_schema_from_source(
    client: &CameoClient,
    source: &str,
    delimiter: Delimiter,
) -> Result<JsonValue> {
    let mut spinner = ProgressSpinner::new();
    let format = detect_source_format_for_source(client, source).await?;

    let result = match format {
        SourceFormat::CsvLike => detect_schema_from_csv(client, source, delimiter).await,
        SourceFormat::SchemaJson => load_json_value_from_source(client, source).await,
        SourceFormat::JsonDocument => {
            let value = load_json_value_from_source(client, source).await?;
            if value.get("fields").is_some() {
                Ok(value)
            } else {
                build_schema_from_json_documents(&[value])
            }
        }
        SourceFormat::JsonArray | SourceFormat::JsonLines => {
            let analysis = analyze_json_source_for_schema(client, source, format).await?;
            build_schema_from_effective_json_documents(&analysis.sample_docs, &analysis.id_field)
        }
    };

    spinner.stop();
    result
}

pub(crate) async fn load_data_from_source(
    client: &CameoClient,
    index: &str,
    source: &str,
    delimiter: Delimiter,
    batch_size: usize,
) -> Result<()> {
    let format = detect_source_format_for_source(client, source).await?;

    match format {
        SourceFormat::CsvLike => {
            let schema_exists = client.get_index_config(index).await.is_ok();
            load_data_from_csv_single_pass(
                client,
                index,
                source,
                delimiter,
                batch_size,
                schema_exists,
            )
            .await
        }
        SourceFormat::SchemaJson => {
            Err(anyhow!("Schema JSON object cannot be loaded as index data"))
        }
        SourceFormat::JsonDocument | SourceFormat::JsonArray | SourceFormat::JsonLines => {
            let schema_exists = client.get_index_config(index).await.is_ok();
            let compression = detect_compression(source);

            if is_http_source(source) && compression == Compression::None {
                load_data_from_http_json_source_single_pass(
                    client,
                    index,
                    source,
                    format,
                    batch_size,
                    schema_exists,
                )
                .await
            } else if is_http_source(source) {
                // Compressed remote: download all, decompress, process via reader
                let bytes = fetch_bytes_source(client, source).await?;
                let reader: Box<dyn Read + Send> = Box::new(Cursor::new(bytes));
                load_data_from_reader_json_source_single_pass(
                    client,
                    index,
                    reader,
                    format,
                    batch_size,
                    schema_exists,
                )
                .await
            } else {
                let path = Path::new(source);
                let reader = open_local_reader(path, compression)?;
                load_data_from_reader_json_source_single_pass(
                    client,
                    index,
                    reader,
                    format,
                    batch_size,
                    schema_exists,
                )
                .await
            }
        }
    }
}

pub(crate) async fn load_data_from_csv_single_pass(
    client: &CameoClient,
    index: &str,
    source: &str,
    delimiter: Delimiter,
    batch_size: usize,
    schema_exists: bool,
) -> Result<()> {
    let mut spinner = ProgressSpinner::new();
    let mut reader = open_csv_reader(client, source, delimiter).await?;
    let raw_headers = reader
        .headers()
        .context("CSV file is missing headers")?
        .clone();
    let headers: Vec<(String, Option<TantivyFieldType>)> =
        raw_headers.iter().map(parse_header_with_hint).collect();
    let id_detection = detect_id_field(&headers);
    let id_header = id_detection.original_field_name.clone();
    let mut ingest = CsvIngest::new(headers, id_detection, id_header, batch_size);

    // Rows whose id cell is empty are skipped. Until the schema exists the rows are
    // only buffered — the sample must name the fields before anything can be sent.
    let mut sample_rows: Vec<csv::StringRecord> = Vec::new();
    let mut needs_schema = !schema_exists;

    for record in reader.records() {
        let record = record.context("Failed to read CSV record")?;
        if record
            .get(ingest.id_detection.index)
            .unwrap_or_default()
            .trim()
            .is_empty()
        {
            continue;
        }

        if needs_schema {
            sample_rows.push(record.clone());
            if sample_rows.len() >= SCHEMA_SAMPLE_LIMIT {
                ingest
                    .create_schema_and_drain(client, index, &mut sample_rows)
                    .await?;
                needs_schema = false;
            }
            continue;
        }

        ingest.push_row(client, index, &record).await?;
    }

    // A source smaller than the sample limit describes its schema at the end instead.
    if needs_schema && !sample_rows.is_empty() {
        ingest
            .create_schema_and_drain(client, index, &mut sample_rows)
            .await?;
    }

    ingest.flush(client, index).await?;
    spinner.stop();

    println!(
        "Ingestion complete for index '{}': loaded={} failed={} (batch size {})",
        index, ingest.total_sent, ingest.total_failed, batch_size
    );
    Ok(())
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

pub(crate) async fn open_csv_source(
    client: &CameoClient,
    source: &str,
) -> Result<Box<dyn Read + Send>> {
    let compression = detect_compression(source);
    let is_http = source.starts_with("http://") || source.starts_with("https://");

    if is_http {
        let url = Url::parse(source).context("Invalid URL for CSV source")?;
        let raw_bytes = client
            .source_http()
            .get(url)
            .send()
            .await
            .context("Failed to fetch remote CSV")?
            .bytes()
            .await
            .context("Failed to read remote CSV body")?;
        let decompressed = decompress_bytes(raw_bytes.to_vec(), compression)?;
        Ok(Box::new(Cursor::new(decompressed)) as Box<dyn Read + Send>)
    } else {
        let path = Path::new(source);
        open_local_reader(path, compression)
    }
}

pub(crate) async fn open_csv_reader(
    client: &CameoClient,
    source: &str,
    delimiter: Delimiter,
) -> Result<csv::Reader<Box<dyn Read + Send>>> {
    let mut builder = ReaderBuilder::new();
    // Remote TSV samples (e.g., book summaries) sometimes contain stray delimiters;
    // allow variable-length records so schema detection doesn't abort early.
    builder.flexible(true);
    match delimiter {
        Delimiter::Detect => {
            let bytes = fetch_bytes_source(client, source).await?;
            // Detect delimiter on first line
            let first_line_end = bytes
                .iter()
                .position(|b| *b == b'\n')
                .unwrap_or(bytes.len());
            let first_line = &bytes[..first_line_end];
            let tab_count = first_line.iter().filter(|b| **b == b'\t').count();
            let comma_count = first_line.iter().filter(|b| **b == b',').count();
            let semi_count = first_line.iter().filter(|b| **b == b';').count();

            let detected = if semi_count >= tab_count && semi_count >= comma_count {
                b';'
            } else if tab_count >= comma_count {
                b'\t'
            } else {
                b','
            };
            builder.delimiter(detected);
            return Ok(builder.from_reader(Box::new(Cursor::new(bytes)) as Box<dyn Read + Send>));
        }
        Delimiter::Comma => {
            builder.delimiter(b',');
        }
        Delimiter::Tab => {
            builder.delimiter(b'\t');
        }
        Delimiter::Semicolon => {
            builder.delimiter(b';');
        }
    }
    // Re-open source since detect may have consumed none; builder will read fresh
    let reader_source = open_csv_source(client, source).await?;
    Ok(builder.from_reader(reader_source))
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
