# The lab tooling (src/streamhouse_lab/config.py) reads these with
# `terraform output -json` and keeps the secrets in memory only.

output "organization_id" {
  value = data.confluent_organization.this.id
}

output "environment_id" {
  value = confluent_environment.lab.id
}

output "cloud_region" {
  value = var.cloud_region
}

output "kafka_cluster_id" {
  value = confluent_kafka_cluster.lab.id
}

output "kafka_cluster_type" {
  value = var.kafka_cluster_type
}

output "flink_catalog" {
  description = "Flink resolves bare table names against this catalog (the environment name)."
  value       = confluent_environment.lab.display_name
}

output "flink_database" {
  description = "Flink resolves bare table names against this database (the cluster name)."
  value       = confluent_kafka_cluster.lab.display_name
}

output "flink_compute_pool_id" {
  value = confluent_flink_compute_pool.lab.id
}

output "flink_rest_endpoint" {
  value = data.confluent_flink_region.lab.rest_endpoint
}

output "operator_id" {
  value = confluent_service_account.operator.id
}

output "operator_flink_api_key" {
  value     = confluent_api_key.operator_flink.id
  sensitive = true
}

output "operator_flink_api_secret" {
  value     = confluent_api_key.operator_flink.secret
  sensitive = true
}

output "lightning_topics" {
  value = sort(keys(confluent_rtce_topic.lightning))
}

output "tableflow_topics" {
  value = sort(keys(confluent_tableflow_topic.history))
}

output "context_mcp_endpoint" {
  description = "Real-Time Context Engine MCP endpoint for this cluster."
  value       = "https://mcp.${var.cloud_region}.aws.confluent.cloud/mcp/v1/context-engine/organizations/${data.confluent_organization.this.id}/environments/${confluent_environment.lab.id}/kafka-clusters/${confluent_kafka_cluster.lab.id}"
}

output "context_reader_api_key" {
  value     = one(confluent_api_key.context_reader_global[*].id)
  sensitive = true
}

output "context_reader_api_secret" {
  value     = one(confluent_api_key.context_reader_global[*].secret)
  sensitive = true
}

output "tableflow_catalog_endpoint" {
  description = "Tableflow Iceberg REST Catalog for this environment. Tables live in the namespace named after the cluster ID."
  value       = "https://tableflow.${var.cloud_region}.aws.confluent.cloud/iceberg/catalog/organizations/${data.confluent_organization.this.id}/environments/${confluent_environment.lab.id}"
}

output "operator_tableflow_api_key" {
  value     = one(confluent_api_key.operator_tableflow[*].id)
  sensitive = true
}

output "operator_tableflow_api_secret" {
  value     = one(confluent_api_key.operator_tableflow[*].secret)
  sensitive = true
}
