use crate::common::*;

#[doc = "Function to convert structure to JSON value"]
/// # Arguments
/// * input_struct -
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

#[doc = ""]
/// # Arguments
/// * value -
///
/// # Returns
/// * String
pub fn format_number(value: i64) -> String {
    value.to_formatted_string(&Locale::en)
}

#[doc = "Functions that read the json file and return it in json value format"]
/// # Arguments
/// * file_path - Path the json file
///
/// # Returns
/// * Result<Value, anyhow::Error>
pub fn read_json_from_file(file_path: &str) -> Result<Value, anyhow::Error> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let json_body: Value = serde_json::from_reader(reader)?;

    Ok(json_body)
}
