---
title: Basic clusters cannot hold a read-only agent
description: On a Confluent Cloud Basic cluster, the read-only role an MCP agent needs is rejected and ACLs are ignored. What I tried and what worked.
date: 2026-11-15
---

*Part 2 of [Building a Streamhouse on Confluent Cloud](index.md).*

## The goal

An AI agent that reads business state should not be able to change it.
So the lab gives agents and MCP clients their own service account, the *context reader*, with a Global API key and nothing more than read access.

The Lightning Tables access-control page documents exactly what that takes:

| Role | Scope | List topics | Query data | Get metadata |
|---|---|---|---|---|
| DeveloperRead | Cluster | Yes | Yes | Yes |
| DeveloperRead | Topic | No | Yes | No |
| DeveloperRead | Schema Registry | Yes | No | Yes |

The plan was `DeveloperRead` on topics plus `DeveloperRead` on Schema Registry subjects.

## Attempt 1: DeveloperRead on topics

Terraform rejected the role binding:

```
error creating role binding "DeveloperRead": 403 Forbidden:
Forbidden - Basic Clusters can not use resource roles
```

The cluster-types page does say Basic clusters do not support RBAC roles for resources inside the cluster.
I had read that as a limit on fine-grained topic roles, and hoped the cluster-scoped binding from the table above would be different.

## Attempt 2: DeveloperRead on the cluster

Same error.
On Basic, a service account cannot get a Kafka resource role at any scope.
The Schema Registry binding, by contrast, was accepted.

## Attempt 3: Kafka ACLs

Basic clusters authorize data access with ACLs, and the documentation says you can query only topics "that you already have permission to read in Kafka".
So I granted the context reader a `READ` ACL on all topics.
Kafka accepted it.

The Real-Time Context Engine did not:

```json
{
  "status": "error",
  "error": {
    "code": "FE_AUTHORIZATION_FAILED",
    "message": "Not authorized to access resource: Access denied by authorization service"
  }
}
```

All three MCP tools, `listTopics`, `getMetadata`, and `queryData`, failed the same way, and still failed more than ten minutes later, so it was not propagation delay.
Even `getMetadata` was denied, although it needs only Schema Registry access per the table, and the reader did hold `DeveloperRead` on every Schema Registry subject.
The engine evidently consults the RBAC authorization service, not Kafka ACLs.

## What worked

`CloudClusterAdmin` on the lab's one cluster is a cluster-level role, not a resource role, so Basic accepts it.
With it, `listTopics` succeeded on the first call.

That leaves an honest trade-off:

| Option | Privilege | Cost |
|---|---|---|
| Basic + `CloudClusterAdmin` for the reader | The MCP tools are read-only, but a leaked key could administer the cluster | First eCKU free |
| Standard + `DeveloperRead` | Read-only, as documented | From $0.75 per eCKU-hour, about $18 a day |
| Your own user's Global key, as in the quick start | Everything your user can do | Free, uses one of only two Global keys per user |

For a lab I chose the first, scoped to one disposable cluster, and wrote the Terraform so that a Standard cluster gets the documented read-only binding automatically.
For anything real, I would pay for Standard.

## A related, better surprise

A service account's Global API key works for the Flink agent's MCP connection exactly like the user-owned key in the quick start.
So the agent can run under a principal that Terraform creates and destroys, without spending either of your two Global key slots.

## Takeaway

If you plan to put agents in front of Lightning Tables, decide on the cluster type with permissions in mind, not just throughput and price.
On Basic, least privilege for the context reader is not available.

Next: [Materialized tables in practice](03-materialized-tables-in-practice.md).
