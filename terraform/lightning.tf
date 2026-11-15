# Capability 4 and 6: Lightning Tables and the Real-Time Context Engine.
#
# Enabling a Lightning Table on a topic is the same API object as an RTCE topic
# (rtce/v1/rtce-topics). Agents reach it over MCP; applications through the
# Lightning query API, which is Early Access at the time of writing.
#
# The context reader is a separate, read-only service account with a Global
# API key. MCP clients and the Flink agent's MCP connection authenticate with
# it, so the agent never holds the operator's EnvironmentAdmin rights.

locals {
  context_enabled = length(var.lightning_topics) > 0
}

resource "confluent_rtce_topic" "lightning" {
  for_each = var.lightning_topics

  cloud       = local.cloud
  region      = var.cloud_region
  topic_name  = each.key
  description = each.value

  environment {
    id = confluent_environment.lab.id
  }

  kafka_cluster {
    id = confluent_kafka_cluster.lab.id
  }
}

resource "confluent_service_account" "context_reader" {
  count = local.context_enabled ? 1 : 0

  display_name = "${var.name}-context-reader"
  description  = "Read-only access to Lightning Tables over the Real-Time Context Engine for ${var.name}"
}

# DeveloperRead on topics grants queryData.
# DeveloperRead on Schema Registry subjects grants listTopics and getMetadata.
# Source: docs.confluent.io/cloud/current/lightning-tables/access-control.html.
# Open question for Basic clusters, which do not support in-cluster RBAC roles:
# whether the topic-scoped binding is accepted. See docs/capabilities/06-agent-mcp.md.
resource "confluent_role_binding" "context_reader_topics" {
  count = local.context_enabled ? 1 : 0

  principal   = "User:${confluent_service_account.context_reader[0].id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${confluent_kafka_cluster.lab.rbac_crn}/kafka=${confluent_kafka_cluster.lab.id}/topic=*"
}

resource "confluent_role_binding" "context_reader_subjects" {
  count = local.context_enabled ? 1 : 0

  principal   = "User:${confluent_service_account.context_reader[0].id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.lab.resource_name}/subject=*"
}

resource "confluent_api_key" "context_reader_global" {
  count = local.context_enabled ? 1 : 0

  display_name = "${var.name}-context-reader-global"
  description  = "Global API key for Real-Time Context Engine and Lightning queries in ${var.name}"

  owner {
    id          = confluent_service_account.context_reader[0].id
    api_version = confluent_service_account.context_reader[0].api_version
    kind        = confluent_service_account.context_reader[0].kind
  }

  managed_resource {
    id          = "global"
    api_version = "global/v1"
    kind        = "Global"
  }

  depends_on = [
    confluent_role_binding.context_reader_topics,
    confluent_role_binding.context_reader_subjects,
  ]
}
