variable "name" {
  description = "Base name for every lab resource. The Flink catalog is this name, so SQL files never hard-code it."
  type        = string
  default     = "river-air-lab"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,30}$", var.name))
    error_message = "name must be 3-31 lowercase letters, digits, or hyphens, starting with a letter."
  }
}

variable "cloud_region" {
  description = "AWS region. Must support Flink, managed model inference, Tableflow, Lightning Tables, and the Real-Time Context Engine."
  type        = string
  default     = "us-east-1"

  # Verified against docs.confluent.io/cloud/current/clusters/regions.html on 2026-11-15.
  validation {
    condition = contains([
      "us-east-1", "us-east-2", "us-west-2",
      "ap-northeast-2", "ap-southeast-2", "ap-south-1", "ap-southeast-1",
      "eu-central-1", "eu-west-1", "eu-west-2",
    ], var.cloud_region)
    error_message = "cloud_region must be an AWS region where every Streamhouse feature this lab uses is available."
  }
}

variable "kafka_cluster_type" {
  description = "basic (first eCKU free, 99.5% SLA) or standard ($0.75/eCKU-hour list, 99.9% SLA). Changing it replaces the cluster."
  type        = string
  default     = "basic"

  validation {
    condition     = contains(["basic", "standard"], var.kafka_cluster_type)
    error_message = "kafka_cluster_type must be basic or standard."
  }
}

variable "flink_max_cfu" {
  description = "Upper bound on Flink spend. It can be raised later but never lowered."
  type        = number
  default     = 5

  validation {
    condition     = contains([5, 10, 20, 30, 40, 50], var.flink_max_cfu)
    error_message = "flink_max_cfu must be one of 5, 10, 20, 30, 40, 50."
  }
}

variable "lightning_topics" {
  description = <<-EOT
    Topic name to model-readable description, for each topic to serve as a Lightning Table.
    The Real-Time Context Engine shows the description to agents.
    Empty until capability 4. Each topic must already exist.
    A non-empty map also creates the read-only context-reader service account and its Global API key.
  EOT
  type        = map(string)
  default     = {}
}

variable "tableflow_topics" {
  description = "Topics to materialize as Iceberg tables in Confluent Managed Storage. Empty until capability 5. Each topic must already exist."
  type        = set(string)
  default     = []
}

variable "enable_bench_clients" {
  description = "Create the operator's Kafka and Schema Registry API keys that the Phase 4 data generator produces with. Off until an experiment is approved."
  type        = bool
  default     = false
}
