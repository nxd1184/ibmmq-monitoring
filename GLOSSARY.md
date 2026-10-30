# IBM MQ Monitoring Tool

A read-only web tool that shows the current state of IBM MQ queue managers, queues and channels. It never reads, browses or writes application messages.

## Language

### Connections

**Connection**:
A saved set of client-mode parameters for one Queue Manager: name, host, port, queue manager name, channel, optional user and password, and a TLS Mode.
_Avoid_: Cluster, server, endpoint, profile

**Client Type**:
Which Java API a Connection uses to reach its Queue Manager: `NATIVE` (IBM MQ classes for Java) or `SPRING_JMS` (Spring JMS with the IBM MQ JMS connection factory). Default `NATIVE`. Both read state with PCF.
_Avoid_: Driver, transport, protocol

**TLS Mode**:
How a Connection secures its channel. One of `NONE`, `TLS` (server authentication only) or `MUTUAL_TLS` (server and client authentication).
_Avoid_: SSL, security mode

**Certificate Store**:
A truststore or keystore file mounted into the deployment and referenced by path from a Connection. Never stored in the database.
_Avoid_: Cert bundle, keyfile

**Connection Test**:
A one-off attempt to connect to a Queue Manager and inquire its status, run from the form before the Connection is saved.
_Avoid_: Ping, probe, dry run

### Monitored objects

**Queue Manager**:
The IBM MQ server process a Connection points at. One Connection targets exactly one Queue Manager.
_Avoid_: Broker, MQ server, qmgr (in prose)

**Queue**:
A local queue on the Queue Manager. The tool shows its depth, max depth, open counts and age attributes, never its messages.
_Avoid_: Topic, destination

**System Queue**:
A Queue whose name starts with `SYSTEM.`. Hidden from the Queue list by default.

**Queue Filter**:
An optional wildcard pattern on a Connection (for example `APP.*`) that limits which Queues are discovered.
_Avoid_: Whitelist, selection

**Channel**:
A server-connection or message channel defined on the Queue Manager, shown with its current status.

**Depth**:
The number of messages currently on a Queue.
_Avoid_: Length, size, backlog, lag

**Open Input Count**:
The number of applications that currently have a Queue open for getting. Zero means the Queue has no Consumer.

**Consumer**:
An application that gets messages from a Queue. The tool must never change what a Consumer sees.
_Avoid_: Subscriber, listener, reader

### Sampling and health

**Sample**:
One read of a Connection's Queue Manager, Queues and Channels, taken at one instant. Samples are not stored.
_Avoid_: Poll, snapshot, metric scrape

**Sampler**:
The server-side process that takes Samples for one Connection at a fixed interval while that Connection has at least one Watcher.
_Avoid_: Collector, poller, scheduler

**Sample Interval**:
The time between two Samples for one Connection. Default 5 seconds, minimum 1 second.

**Watcher**:
A browser that is currently viewing a Connection's dashboard and receives its Samples.
_Avoid_: Subscriber, client, session

**Health**:
A verdict of `HEALTHY` or `UNHEALTHY` with a reason, computed from a Sample against Thresholds. Applies to a Connection, a Queue or a Channel.
_Avoid_: Status (use Status for raw MQ state), state, alert

**Status**:
The raw state reported by MQ, for example a Channel status of `RUNNING` or `STOPPED`. Health is derived from Status; they are not the same word.

**Prometheus Export**:
A per-Connection toggle, off by default. When on, the Sampler runs continuously and the Connection's latest Sample is exposed as `ibmmq_*` metrics. When off, the Connection is absent from the metrics endpoint.
_Avoid_: Scrape, monitoring flag

**Threshold**:
A limit used to compute Health: absolute Depth, Depth as a percent of max depth, oldest message age, or time with no Consumer. Global defaults, overridable per Connection.
_Avoid_: Limit, rule, alarm
