---
title: Basic clusters cannot hold a read-only agent
description: On a Confluent Cloud Basic cluster, the read-only role an MCP agent needs is rejected and ACLs are ignored. What I tried and what worked.
date: 2026-11-15
part: 2
lang: en
alternate_url: /ko/02-basic-clusters-and-read-only-agents.html
translation_key: chapter-02
reading_time: 5
sample: 3 authorization attempts
takeaways:
  - label: "Observed"
    text: "DeveloperRead failed at topic and cluster scope; Kafka READ ACLs were accepted but authorized none of the three Real-Time Context Engine tools."
  - label: "Interpretation"
    text: "CloudClusterAdmin worked, so a client with read-only tools ended up holding a key that can administer the cluster."
  - label: "Limitation"
    text: "I only tested Basic. Least privilege needs a cluster type with resource roles, such as Standard."
---

## The goal

An AI agent that reads business state should not be able to change it.
So I designed the lab around a dedicated service account, which I call the *context reader*, with a Global API key and read access only.
On a Basic cluster, that last requirement turned out to be impossible.

The [Lightning Tables access-control page](https://docs.confluent.io/cloud/current/lightning-tables/access-control.html) lists what each role allows:

| Role | Scope | List topics | Query data | Get metadata |
|---|---|---|---|---|
| DeveloperRead | Cluster | Yes | Yes | Yes |
| DeveloperRead | Topic | No | Yes | No |
| DeveloperRead | Schema Registry | Yes | No | Yes |

My plan was `DeveloperRead` on the topics plus `DeveloperRead` on the Schema Registry subjects.

## Attempt 1: DeveloperRead on topics

Terraform rejected the role binding:

```
error creating role binding "DeveloperRead": 403 Forbidden:
Forbidden - Basic Clusters can not use resource roles
```

The cluster-types page does say that Basic clusters don't support RBAC roles for resources inside the cluster.
I had taken that to mean fine-grained topic roles, and hoped the cluster-scoped binding from the table would get through.

## Attempt 2: DeveloperRead on the cluster

It failed with the same error.
On Basic, a service account can't get a Kafka resource role at any scope.
The Schema Registry binding went through.

## Attempt 3: Kafka ACLs

Basic clusters authorize data access with ACLs, and the documentation says you can query only topics "that you already have permission to read in Kafka".
So I gave the context reader a `READ` ACL on all topics, and Kafka accepted it.

The Real-Time Context Engine still refused:

```json
{
  "status": "error",
  "error": {
    "code": "FE_AUTHORIZATION_FAILED",
    "message": "Not authorized to access resource: Access denied by
                authorization service"
  }
}
```

All three MCP tools (`listTopics`, `getMetadata`, and `queryData`) failed this way, and they were still failing more than ten minutes later, so it wasn't a propagation delay.
`getMetadata` was denied too, even though the table says it only needs Schema Registry access and the reader held `DeveloperRead` on every subject.
My conclusion is that the engine checks the RBAC authorization service and ignores Kafka ACLs.

## What worked

`CloudClusterAdmin` on the lab's one cluster is a cluster-level role rather than a resource role, so Basic accepts it.
With it, `listTopics` worked on the first call.
That makes the narrowest credential that worked for a read-only MCP client on this Basic cluster one that can also administer the cluster.

The options are these:

| Option | Credential privilege | Cluster requirement or trade-off |
|---|---|---|
| Basic + `CloudClusterAdmin` for the reader | The MCP tools are read-only, but a leaked key could administer the cluster | The first eCKU is free; load beyond it was billed at $0.135 per eCKU-hour ([part 7](07-is-a-streamhouse-worth-it.md#what-the-lab-cost)) |
| Standard + `DeveloperRead` | Read-only, as documented | From $0.75 per eCKU-hour, about $18 a day |
| Your own user's Global key, as in the quick start | Everything your user can do | No cluster-price change; uses one of only two Global keys per user |

For the lab I took the first option, on one disposable cluster, and wrote the Terraform so that a Standard cluster gets the documented read-only binding automatically.
For anything real I would pay for Standard.

## Service account keys work for the agent

A service account's Global API key works for the Flink agent's MCP connection the same way the user-owned key in the quick start does.
That means the agent can run under a principal that Terraform creates and destroys, and you keep both of your Global key slots.

## If you are choosing a cluster

If agents will sit in front of Lightning Tables, factor permissions into the cluster type along with throughput and price.
On Basic, the context reader has to hold an admin role on the cluster.
