# Platform for the River Air Streamhouse lab.
#
# Creates one environment, one Kafka cluster, one Flink compute pool, and the
# service account that runs every Flink statement. Flink SQL objects (tables,
# the materialized table, the model, tool, and agent) are not here: they are
# the subject of the lab and live in sql/, applied by `uv run lab apply`.
#
# Lightning Tables and Tableflow are in lightning.tf and tableflow.tf, gated by
# variables, because they need topics that Flink creates first.

locals {
  cloud = "AWS"
}

data "confluent_organization" "this" {}

resource "confluent_environment" "lab" {
  display_name = var.name

  # Essentials is enough: no feature this lab uses requires Advanced.
  stream_governance {
    package = "ESSENTIALS"
  }
}

resource "confluent_kafka_cluster" "lab" {
  display_name = "river-air"
  availability = "SINGLE_ZONE"
  cloud        = local.cloud
  region       = var.cloud_region

  dynamic "basic" {
    for_each = var.kafka_cluster_type == "basic" ? [1] : []
    content {}
  }

  dynamic "standard" {
    for_each = var.kafka_cluster_type == "standard" ? [1] : []
    content {}
  }

  environment {
    id = confluent_environment.lab.id
  }
}

# The environment's Schema Registry exists once a cluster does.
data "confluent_schema_registry_cluster" "lab" {
  environment {
    id = confluent_environment.lab.id
  }

  depends_on = [confluent_kafka_cluster.lab]
}

resource "confluent_flink_compute_pool" "lab" {
  display_name = "${var.name}-pool"
  cloud        = local.cloud
  region       = var.cloud_region
  max_cfu      = var.flink_max_cfu

  environment {
    id = confluent_environment.lab.id
  }
}

data "confluent_flink_region" "lab" {
  cloud  = local.cloud
  region = var.cloud_region
}

# ---------------------------------------------------------------------------
# Operator: the principal every Flink statement runs as.
# EnvironmentAdmin is broad, but scoped to this lab's environment only. It also
# covers Tableflow (CloudClusterAdmin + DeveloperRead on subjects) and managing
# Lightning Tables, and works on Basic clusters, which lack in-cluster RBAC.
# ---------------------------------------------------------------------------

resource "confluent_service_account" "operator" {
  display_name = "${var.name}-operator"
  description  = "Runs Flink statements and manages Tableflow for ${var.name}"
}

resource "confluent_role_binding" "operator_environment_admin" {
  principal   = "User:${confluent_service_account.operator.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.lab.resource_name
}

resource "confluent_api_key" "operator_flink" {
  display_name = "${var.name}-operator-flink"
  description  = "Flink API key for ${var.name}-operator"

  owner {
    id          = confluent_service_account.operator.id
    api_version = confluent_service_account.operator.api_version
    kind        = confluent_service_account.operator.kind
  }

  managed_resource {
    id          = data.confluent_flink_region.lab.id
    api_version = data.confluent_flink_region.lab.api_version
    kind        = data.confluent_flink_region.lab.kind

    environment {
      id = confluent_environment.lab.id
    }
  }

  depends_on = [confluent_role_binding.operator_environment_admin]
}
