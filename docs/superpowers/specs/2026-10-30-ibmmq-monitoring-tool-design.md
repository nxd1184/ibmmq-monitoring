# IBM MQ Monitoring Tool: Design Spec

Agreed on 2026-10-30. Terms are defined in [GLOSSARY.md](../../../GLOSSARY.md). Decisions with trade-offs are in [docs/adr](../../adr).

## Purpose

A web tool, like kafka-ui or Redpanda Console, that shows the **current** state of IBM MQ queue managers, queues and channels. It is strictly read-only. It never gets, browses or puts application messages, so it cannot affect a Consumer.

## Stack

- Java 21, Maven, Spring Boot 3.5.16.
- Thymeleaf server-rendered UI. HTMX 2.0.11 with the SSE extension 2.2.4 for realtime updates. Local static assets only, no CDN.
- PostgreSQL (driver 42.7.14) with Flyway 13.12.0. Spring Data JDBC. Stores Connections and Thresholds only. No Samples, no history.
- IBM MQ client `com.ibm.mq.allclient` 9.4.5.1 and `mq-jms-spring-boot-starter` 3.5.15.
- Spring Security form login. One shared admin user. Username and password come from environment variables, supplied by a Kubernetes secret.
- Micrometer with the Prometheus registry.
- Testcontainers 2.0.5 with `postgres:16-alpine` and `icr.io/ibm-messaging/mq:latest`.
- Ships as one jar and one Docker image.

## Connection

Fields: name, host, port (default 1414), queue manager, channel, username (optional), password (optional), Client Type, TLS Mode, Queue Filter (optional wildcard, default `*`), Sample Interval (seconds, default 5, minimum 1), Prometheus Export (default off), reply queue model (advanced, default `SYSTEM.DEFAULT.MODEL.QUEUE`), hide System Queues (default on), Thresholds override (optional).

Client Type: `NATIVE` (default) or `SPRING_JMS`. Both read with PCF through one `MqGateway` interface.

TLS Mode: `NONE`, `TLS`, `MUTUAL_TLS`.
- `TLS` requires cipher spec and a truststore path.
- `MUTUAL_TLS` also requires a keystore path and optional certificate label.
- Truststore and keystore are files mounted into the pod. Paths default to deployment-wide values from configuration, overridable per Connection. Store passwords come from configuration, never from the database.

The Connection password is encrypted at rest with AES-256-GCM. The key is 32 bytes, base64, from the environment variable `MQMONITOR_ENCRYPTION_KEY`. The app refuses to start without it. Passwords never render in the UI after save and never appear in logs.

UI: list, create, edit, delete with confirmation, and **Test connection** from the form before save. Test connection opens a gateway, inquires queue manager status, closes, and shows success or the error. No import or export.

## Monitored objects per Sample

- Queue manager: reachable yes/no, status, name, platform, command level (for n/a decisions).
- Local Queues matching the Queue Filter, System Queues hidden when the flag is on: name, Depth, max depth, Open Input Count, open output count, oldest message age, last put time, last get time. Attributes MQ does not report are `null` and render as "n/a".
- Channels: name, type, status.

## Sampling

- One Sampler per Connection. It keeps one gateway open while running and reconnects on failure.
- The Sampler runs while the Connection has at least one Watcher, and stops 30 seconds after the last Watcher leaves.
- When Prometheus Export is on, the Sampler runs continuously.
- The Connection list page shows reachability and Health for every Connection using a slower, queue-manager-only inquiry every 30 seconds while that page is watched.
- The browser never calls MQ. Each finished Sample is rendered to Thymeleaf fragments and pushed over SSE to every Watcher of that Connection.

## Health

Health is `HEALTHY` or `UNHEALTHY` with a reason. Computed per Sample.
- Queue: Depth above absolute Threshold, or Depth above percent-of-max Threshold (default 80), or Open Input Count 0 for longer than the no-consumer Threshold (default 60 seconds), or oldest message age above Threshold (default unset).
- Channel: Status not `RUNNING`.
- Connection: queue manager unreachable, or any Queue or Channel unhealthy.
Thresholds have global defaults in configuration and per-Connection overrides.

## Metrics

`/actuator/prometheus` exposes only `ibmmq_*` series for Connections with Prometheus Export on, labelled `connection`, `qmgr`, and `queue` or `channel`: `ibmmq_qmgr_reachable`, `ibmmq_queue_depth`, `ibmmq_queue_max_depth`, `ibmmq_queue_open_input_count`, `ibmmq_queue_open_output_count`, `ibmmq_queue_oldest_message_age_seconds`, `ibmmq_channel_status`, `ibmmq_health` (with `reason` label). JVM, system, HTTP, Tomcat, logback and other default binders are disabled.

Logs: ordinary application logs to stdout. No MQ event log.

## MQ compatibility

Floor MQ 7.5. Tested in Docker against MQ 9.x only. Older versions are code-level compatible: only PCF attributes that exist since MQ 7.5 are requested, and missing values render as "n/a". A `java.security` override file ships in the image for legacy cipher specs, enabled by an environment variable.

Required MQ authorities for the monitoring user, documented with a `setmqaut` script: `+connect +inq +dsp` on the queue manager, `+inq +dsp` on queues and channels, `+put` on `SYSTEM.ADMIN.COMMAND.QUEUE`, `+dsp +get +put` on the reply queue model. `MQRC_NOT_AUTHORIZED` (2035) renders as "authority missing" with the object name.

## Out of scope for version one

History and trends. Webhook, Slack or email alerts. Users and roles. Topics, subscriptions, listeners. Any action against MQ. Import or export of Connections. Bindings mode.
