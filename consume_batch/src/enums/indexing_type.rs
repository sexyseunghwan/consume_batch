use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexingType {
    Insert,
    Update,
    Delete,
}

impl FromStr for IndexingType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "I" | "INSERT" => Ok(IndexingType::Insert),
            "U" | "UPDATE" => Ok(IndexingType::Update),
            "D" | "DELETE" => Ok(IndexingType::Delete),
            _ => Err(format!("Invalid IndexingType: {}", s)),
        }
    }
}
