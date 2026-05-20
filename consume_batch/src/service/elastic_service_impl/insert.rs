use crate::common::*;
use crate::repository::es_repository::EsRepository;

use super::ElasticServiceImpl;

impl<R: EsRepository + Sync + Send> ElasticServiceImpl<R> {
    pub(super) async fn initialize_index(
        &self,
        index_name: &str,
        settings: &Value,
        mappings: &Value,
    ) -> anyhow::Result<()> {
        self.elastic_conn
            .initialize_index(index_name, settings, mappings)
            .await
    }

    pub(super) async fn input_bulk<T: Serialize + Send + Sync>(
        &self,
        index_name: &str,
        documents: Vec<T>,
        doc_id_field: Option<&str>,
    ) -> anyhow::Result<()> {
        self.elastic_conn
            .input_bulk(index_name, documents, doc_id_field)
            .await
    }

    pub(super) async fn initialize_full_index(
        &self,
        index_name: &str,
        mapping_schema_path: &str,
    ) -> anyhow::Result<String> {
        let schema_content: String = tokio::fs::read_to_string(mapping_schema_path)
            .await
            .inspect_err(|e| {
                error!(
                    "[ElasticServiceImpl::initialize_full_index] Failed to read mapping schema file: {}: {:#}",
                    mapping_schema_path, e
                );
            })?;

        let schema: Value = serde_json::from_str(&schema_content).inspect_err(|e| {
            error!(
                "[ElasticServiceImpl::initialize_full_index] Failed to parse mapping schema: {:#}",
                e
            );
        })?;

        let mappings: Value = schema
            .get("mappings")
            .ok_or_else(|| {
                anyhow!("[ElasticServiceImpl::initialize_full_index] Missing 'mappings' in schema")
            })?
            .clone();

        let mut settings: Value = schema.get("settings").cloned().unwrap_or_else(|| json!({}));

        if let Some(index_obj) = settings.get_mut("index") {
            if let Some(obj) = index_obj.as_object_mut() {
                obj.insert("number_of_shards".to_string(), json!(3));
                obj.insert("number_of_replicas".to_string(), json!(0));
                obj.insert("refresh_interval".to_string(), json!("-1"));
            }
        } else {
            settings["index"]["number_of_shards"] = json!(3);
            settings["index"]["number_of_replicas"] = json!(0);
            settings["index"]["refresh_interval"] = json!("-1");
        }

        let new_index_name: String =
            format!("{}_{}", index_name, Utc::now().format("%Y%m%d%H%M%S"));

        info!(
            "[ElasticServiceImpl::initialize_full_index] Creating new index: {}",
            new_index_name
        );

        self.initialize_index(&new_index_name, &settings, &mappings)
            .await
            .inspect_err(|e| {
                error!(
                    "[ElasticServiceImpl::initialize_full_index] Failed to create index: {:#}",
                    e
                );
            })?;

        Ok(new_index_name)
    }
}
