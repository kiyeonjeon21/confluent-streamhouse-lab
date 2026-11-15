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

# Read access for the context reader.
# Docs: DeveloperRead on topics grants queryData; DeveloperRead on Schema Registry
# subjects grants listTopics and getMetadata
# (docs.confluent.io/cloud/current/lightning-tables/access-control.html).
#
# Basic clusters cannot give a service account that access (observed 2026-11-15):
#   - every Kafka resource role is rejected, at topic and at cluster scope alike:
#     "403 Basic Clusters can not use resource roles";
#   - a READ ACL on all topics is accepted by Kafka, but the Real-Time Context
#     Engine ignores it: all three MCP tools return FE_AUTHORIZATION_FAILED.
# The narrowest role that works on Basic is CloudClusterAdmin on this one cluster.
# The MCP tools are read-only, but the key itself could administer the cluster,
# so on Basic the reader is not least-privilege. Standard keeps DeveloperRead.

locals {
  basic_cluster = var.kafka_cluster_type == "basic"
}

resource "confluent_role_binding" "context_reader_topics" {
  count = local.context_enabled && !local.basic_cluster ? 1 : 0

  principal   = "User:${confluent_service_account.context_reader[0].id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${confluent_kafka_cluster.lab.rbac_crn}/kafka=${confluent_kafka_cluster.lab.id}/topic=*"
}

resource "confluent_role_binding" "context_reader_cluster_admin" {
  count = local.context_enabled && local.basic_cluster ? 1 : 0

  principal   = "User:${confluent_service_account.context_reader[0].id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.lab.rbac_crn
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
    confluent_role_binding.context_reader_cluster_admin,
  ]
}
