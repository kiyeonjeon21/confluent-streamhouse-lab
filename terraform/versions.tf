terraform {
  required_version = ">= 1.9"

  required_providers {
    confluent = {
      source = "confluentinc/confluent"
      # 2.95.0 is the first version this lab was verified against.
      # It has confluent_rtce_topic, confluent_tableflow_topic, and Global API keys.
      version = "~> 2.95"
    }
  }
}

# Credentials come from CONFLUENT_CLOUD_API_KEY and CONFLUENT_CLOUD_API_SECRET.
# They are never written to a tracked file. See .env.example.
provider "confluent" {}
