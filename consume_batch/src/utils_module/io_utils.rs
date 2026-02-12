use crate::common::*;

#[doc = r#"
    Generic function that reads a TOML format configuration file and deserializes it to the specified struct type.

    The application's various configuration files (index settings, email recipients, server settings, etc.)
    are managed in TOML format, and this function converts them type-safely into structs.

    1. Reads the TOML file at the specified path as a string
    2. Uses `toml::from_str()` to parse the TOML string to generic type T
    3. Converts to struct using serde's deserialization feature
    4. Returns appropriate error on file read or parsing failure

    # Type Parameters
    * `T` - Struct type that implements the `DeserializeOwned` trait

    # Arguments
    * `file_path` - Absolute or relative path to the TOML file to read

    # Returns
    * `Result<T, anyhow::Error>` - Parsed struct on success, error on failure

    # Errors
    - When file does not exist or lacks read permission
    - When TOML format is invalid and parsing fails
    - When struct fields and TOML keys do not match

    # Examples
    ```rust
    let config: ServerConfig = read_toml_from_file("config/server.toml")?;
    let index_list: IndexListConfig = read_toml_from_file(&INDEX_LIST_PATH)?;
    ```
"#]
/// # Arguments
/// * `file_path` - Path where the target toml file to read exists
///
/// # Returns
/// * Result<T, anyhow::Error> - Returns a json-compatible object if file is successfully read
pub fn read_toml_from_file<T: DeserializeOwned>(file_path: &str) -> Result<T, anyhow::Error> {
    let toml_content = std::fs::read_to_string(file_path)?;
    let toml: T = toml::from_str(&toml_content)?;

    Ok(toml)
}

#[doc = r#"
    Generic utility function that converts a struct to a JSON Value object.

    Used when converting struct data to serde_json::Value format
    for Elasticsearch queries or other JSON API calls.

    1. Converts struct to JSON Value using serde's serialization feature
    2. Performs safe conversion through `serde_json::to_value()`
    3. Returns anyhow::Error with detailed error message on conversion failure
    4. Returns serde_json::Value object on success

    This function is mainly used in the following situations:
    - Converting struct to JSON when composing Elasticsearch queries
    - Generating API request payloads
    - JSON output for logging or debugging purposes

    # Type Parameters
    * `T` - Struct type that implements the `Serialize` trait

    # Arguments
    * `input_struct` - Reference to the struct to convert to JSON

    # Returns
    * `Result<Value, anyhow::Error>` - JSON Value on success, error on failure

    # Errors
    When struct fields contain types that cannot be serialized to JSON

    # Examples
    ```rust
    let alert_index = AlertIndex::new("test_index".to_string(), 100, "2023-01-01".to_string());
    let json_value = convert_json_from_struct(&alert_index)?;
    ```
"#]
/// # Arguments
/// * input_struct - Struct to convert to json
///
/// # Returns
/// * Result<Value, anyhow::Error>
pub fn convert_json_from_struct<T: Serialize>(input_struct: &T) -> Result<Value, anyhow::Error> {
    serde_json::to_value(input_struct).map_err(|err| {
        anyhow!(
            "[Error][convert_json_from_struct()] Failed to serialize struct to JSON: {}",
            err
        )
    })
}
