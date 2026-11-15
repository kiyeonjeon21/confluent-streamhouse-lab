# Capability 5: Tableflow to Iceberg in Confluent Managed Storage.
#
# Managed storage needs no AWS account. Its tables are readable only through
# the Tableflow Iceberg REST Catalog with vended credentials, and they do not
# sync to external catalogs such as AWS Glue.

resource "confluent_api_key" "operator_tableflow" {
  count = length(var.tableflow_topics) > 0 ? 1 : 0

  display_name = "${var.name}-operator-tableflow"
  description  = "Tableflow API key for ${var.name}-operator"

  owner {
    id          = confluent_service_account.operator.id
    api_version = confluent_service_account.operator.api_version
    kind        = confluent_service_account.operator.kind
  }

  managed_resource {
    id          = "tableflow"
    api_version = "tableflow/v1"
    kind        = "Tableflow"
  }

  depends_on = [confluent_role_binding.operator_environment_admin]
}

resource "confluent_tableflow_topic" "history" {
  for_each = var.tableflow_topics

  display_name  = each.key
  table_formats = ["ICEBERG"]
  managed_storage {}

  environment {
    id = confluent_environment.lab.id
  }

  kafka_cluster {
    id = confluent_kafka_cluster.lab.id
  }

  credentials {
    key    = confluent_api_key.operator_tableflow[0].id
    secret = confluent_api_key.operator_tableflow[0].secret
  }
}
