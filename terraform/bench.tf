# Phase 4: client credentials for the data generator (src/streamhouse_lab/kafka_io.py).
#
# The generator produces to the bench_ topics as the operator, which already
# holds EnvironmentAdmin on this environment. Flink statements write to Kafka
# with the same principal, so this adds no new privilege, only new keys.

resource "confluent_api_key" "operator_kafka" {
  count = var.enable_bench_clients ? 1 : 0

  display_name = "${var.name}-operator-kafka"
  description  = "Kafka API key for ${var.name}-operator, used by the Phase 4 data generator"

  owner {
    id          = confluent_service_account.operator.id
    api_version = confluent_service_account.operator.api_version
    kind        = confluent_service_account.operator.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.lab.id
    api_version = confluent_kafka_cluster.lab.api_version
    kind        = confluent_kafka_cluster.lab.kind

    environment {
      id = confluent_environment.lab.id
    }
  }

  depends_on = [confluent_role_binding.operator_environment_admin]
}

resource "confluent_api_key" "operator_schema_registry" {
  count = var.enable_bench_clients ? 1 : 0

  display_name = "${var.name}-operator-schema-registry"
  description  = "Schema Registry API key for ${var.name}-operator, used by the Phase 4 data generator"

  owner {
    id          = confluent_service_account.operator.id
    api_version = confluent_service_account.operator.api_version
    kind        = confluent_service_account.operator.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.lab.id
    api_version = data.confluent_schema_registry_cluster.lab.api_version
    kind        = data.confluent_schema_registry_cluster.lab.kind

    environment {
      id = confluent_environment.lab.id
    }
  }

  depends_on = [confluent_role_binding.operator_environment_admin]
}
