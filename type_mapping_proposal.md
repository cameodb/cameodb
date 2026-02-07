# Flexible Schema Type Mapping Proposal

## Problem
Current schema definition is rigid - Python scripts send field type names that don't match Rust enum serialization:
- Python: `"float"` → Rust: `"f64"`
- Python: `"integer"` → Rust: `"i64"`

## Solution: Add Type Alias Support

### 1. Extend TantivyFieldType with Custom Deserialization

```rust
use serde::{Deserialize, Deserializer};

impl<'de> Deserialize<'de> for TantivyFieldType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let normalized = s.to_lowercase();
        
        match normalized.as_str() {
            // Primary names (current)
            "text" => Ok(TantivyFieldType::Text),
            "string" => Ok(TantivyFieldType::String),
            "i64" => Ok(TantivyFieldType::I64),
            "u64" => Ok(TantivyFieldType::U64),
            "f64" => Ok(TantivyFieldType::F64),
            "date" => Ok(TantivyFieldType::Date),
            "boolean" => Ok(TantivyFieldType::Boolean),
            "bytes" => Ok(TantivyFieldType::Bytes),
            "ip" => Ok(TantivyFieldType::Ip),
            "json" => Ok(TantivyFieldType::Json),
            "facet" => Ok(TantivyFieldType::Facet),
            
            // Common aliases for compatibility
            "float" | "double" | "decimal" => Ok(TantivyFieldType::F64),
            "integer" | "int" | "number" | "signed" => Ok(TantivyFieldType::I64),
            "unsigned" | "uint" => Ok(TantivyFieldType::U64),
            "bool" => Ok(TantivyFieldType::Boolean),
            "datetime" | "timestamp" => Ok(TantivyFieldType::Date),
            "binary" | "blob" => Ok(TantivyFieldType::Bytes),
            "object" | "document" => Ok(TantivyFieldType::Json),
            "category" | "tag" => Ok(TantivyFieldType::Facet),
            
            // Fallback
            _ => Err(D::Error::custom(format!(
                "Unknown field type: '{}'. Supported types: text, string, i64, u64, f64, date, boolean, bytes, ip, json, facet",
                s
            ))),
        }
    }
}
```

### 2. Supported Type Aliases

| Primary Type | Common Aliases | Languages/Systems Using |
|--------------|----------------|------------------------|
| `Text` | `text`, `string` | Python, JavaScript, SQL |
| `I64` | `i64`, `integer`, `int`, `number`, `signed` | Python, SQL, Java |
| `U64` | `u64`, `unsigned`, `uint` | SQL, C, Rust |
| `F64` | `f64`, `float`, `double`, `decimal` | Python, SQL, JavaScript |
| `Boolean` | `boolean`, `bool` | Python, JavaScript, SQL |
| `Date` | `date`, `datetime`, `timestamp` | Python, SQL, JavaScript |
| `Bytes` | `bytes`, `binary`, `blob` | SQL, Python |
| `Json` | `json`, `object`, `document` | JavaScript, MongoDB |
| `Ip` | `ip`, `address` | Network tools |
| `Facet` | `facet`, `category`, `tag` | E-commerce, search |

### 3. Benefits

1. **Language Agnostic**: Python, JavaScript, Java, Go clients can use their native type names
2. **Backward Compatible**: Existing Python scripts work without changes
3. **Forward Compatible**: New aliases can be added easily
4. **Clear Error Messages**: Unknown types get helpful error messages
5. **Consistent with Client**: Rust client already uses similar mapping in `friendly_type_label()`

### 4. Implementation Steps

1. Add custom `Deserialize` impl to `TantivyFieldType`
2. Update tests to verify aliases work
3. Document supported types in API docs
4. Consider adding `Serialize` customization to always output canonical names

### 5. Alternative: Mapping Function

If we don't want to modify the enum directly:

```rust
impl TantivyFieldType {
    pub fn from_string(s: &str) -> Result<Self, String> {
        let normalized = s.to_lowercase();
        match normalized.as_str() {
            // ... same mapping logic
        }
    }
}

// Then in FieldDef deserialization:
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FieldDef {
    pub name: String,
    #[serde(deserialize_with = "deserialize_field_type")]
    pub field_type: TantivyFieldType,
    // ... other fields
}

fn deserialize_field_type<'de, D>(deserializer: D) -> Result<TantivyFieldType, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    TantivyFieldType::from_string(&s)
        .map_err(|e| D::Error::custom(e))
}
```

### 6. Migration Path

- Phase 1: Add alias support (no breaking changes)
- Phase 2: Update Python scripts to use canonical names (optional)
- Phase 3: Consider deprecating aliases in v2.0 (future)

### 7. Testing Strategy

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_type_aliases() {
        // Test various aliases deserialize correctly
        let cases = vec![
            ("float", TantivyFieldType::F64),
            ("double", TantivyFieldType::F64),
            ("integer", TantivyFieldType::I64),
            ("int", TantivyFieldType::I64),
            ("bool", TantivyFieldType::Boolean),
            ("datetime", TantivyFieldType::Date),
        ];
        
        for (alias, expected) in cases {
            let json = format!(r#"{{"field_type": "{}"}}"#, alias);
            let field_def: FieldDef = serde_json::from_str(&json).unwrap();
            assert_eq!(field_def.field_type, expected);
        }
    }
}
```

## Recommendation

Go with Option 1 (custom `Deserialize` impl) because:
- Cleaner integration
- No changes needed to `FieldDef`
- Works everywhere `TantivyFieldType` is used
- Minimal code changes
- Easy to extend with new aliases
