# IBM MQ Monitoring Tool

This tool shows the current state of IBM MQ queue managers, queues and channels in a web page.
It keeps saved connections in PostgreSQL.
It reads MQ state with PCF inquiries, through the native IBM MQ client or through Spring JMS.
It exposes the latest state of chosen connections as `ibmmq_*` Prometheus metrics.
It stores no history.

## What it never does

- It never gets, browses or puts application messages.
- It sends PCF inquiries only. The one put it makes is the PCF request to `SYSTEM.ADMIN.COMMAND.QUEUE`.
- It never opens an application queue for input, output or browse.
- The reply queue is a temporary queue created from a model queue (default `SYSTEM.DEFAULT.MODEL.QUEUE`).
- It runs no action against MQ. It has no start, stop, clear or delete.

Read-only is enforced by MQ authority too. See [MQ authorities](#mq-authorities).

## Quick start

You need Docker.

1. Start the demo queue manager. It runs under emulation on Apple Silicon.

   ```sh
   docker run -d --name mq-demo --platform linux/amd64 -p 1414:1414 \
     -e LICENSE=accept -e MQ_QMGR_NAME=QM1 \
     -e MQ_ADMIN_PASSWORD=passw0rd -e MQ_APP_PASSWORD=passw0rd \
     icr.io/ibm-messaging/mq:latest
   ```

2. Start PostgreSQL and the app. The first run builds the image.

   ```sh
   docker compose up -d --build
   ```

3. Open `http://localhost:8080`.
4. Log in with `admin` and `demo-admin-password`. These are demo values from `docker-compose.yml`.
5. Create a connection with these values.
   The MQ host is `host.docker.internal` because the app runs in a container.

   | Field | Value |
   |---|---|
   | Host | `host.docker.internal` |
   | Port | `1414` |
   | Queue Manager | `QM1` |
   | Channel | `DEV.ADMIN.SVRCONN` |
   | User | `admin` |
   | Password | `passw0rd` |
   | TLS Mode | `NONE` |

6. Select Test, then save, then open the dashboard.

Compose publishes PostgreSQL on host port 5433, so it does not clash with a local PostgreSQL on 5432.
To run the app outside Docker, set `SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5433/mqmonitor`.

## Configuration

The app refuses to start without the admin username, the admin password and the encryption key.

| Environment variable | Property | Default | Meaning |
|---|---|---|---|
| `MQMONITOR_ADMIN_USERNAME` | `mqmonitor.admin.username` | none, required | Login user name. |
| `MQMONITOR_ADMIN_PASSWORD` | `mqmonitor.admin.password` | none, required | Login password. |
| `MQMONITOR_ENCRYPTION_KEY` | `mqmonitor.encryption-key` | none, required | Base64 of 32 bytes. Encrypts stored connection passwords with AES-GCM. |
| `SPRING_DATASOURCE_URL` | `spring.datasource.url` | none | JDBC URL of PostgreSQL. |
| `SPRING_DATASOURCE_USERNAME` | `spring.datasource.username` | none | Database user. |
| `SPRING_DATASOURCE_PASSWORD` | `spring.datasource.password` | none | Database password. |
| `MQMONITOR_TLS_TRUSTSTORE_PATH` | `mqmonitor.tls.truststore-path` | none | Default PKCS12 truststore. |
| `MQMONITOR_TLS_TRUSTSTORE_PASSWORD` | `mqmonitor.tls.truststore-password` | none | Password of that truststore. |
| `MQMONITOR_TLS_KEYSTORE_PATH` | `mqmonitor.tls.keystore-path` | none | Default PKCS12 keystore, for mutual TLS. |
| `MQMONITOR_TLS_KEYSTORE_PASSWORD` | `mqmonitor.tls.keystore-password` | none | Password of that keystore. |
| `MQMONITOR_SAMPLING_WATCHER_GRACE_SECONDS` | `mqmonitor.sampling.watcher-grace-seconds` | `30` | How long a sampler keeps running after the last watcher leaves. |
| `MQMONITOR_SAMPLING_LIST_INTERVAL_SECONDS` | `mqmonitor.sampling.list-interval-seconds` | `30` | Interval of the connection list page inquiry. |
| `MQMONITOR_THRESHOLDS_DEPTH_MAX` | `mqmonitor.thresholds.depth-max` | unset | Default absolute depth threshold. |
| `MQMONITOR_THRESHOLDS_DEPTH_PERCENT` | `mqmonitor.thresholds.depth-percent` | `80` | Default depth threshold in percent of max depth. |
| `MQMONITOR_THRESHOLDS_NO_CONSUMER_SECONDS` | `mqmonitor.thresholds.no-consumer-seconds` | `60` | Default time with no consumer. |
| `MQMONITOR_THRESHOLDS_OLDEST_MESSAGE_AGE_SECONDS` | `mqmonitor.thresholds.oldest-message-age-seconds` | unset | Default oldest message age threshold. |
| `MQMONITOR_MQ_CONNECT_TIMEOUT_SECONDS` | `mqmonitor.mq.connect-timeout-seconds` | `10` | Timeout for connect and for each PCF reply. |
| `MQMONITOR_LEGACY_TLS` | none, read by `entrypoint.sh` | unset | `true` loads the legacy TLS override. See [TLS and legacy queue managers](#tls-and-legacy-queue-managers). |
| `JAVA_OPTS` | none, read by `entrypoint.sh` | unset | Extra JVM options. `JAVA_TOOL_OPTIONS` is also honoured. |

The `/actuator/health` and `/actuator/prometheus` endpoints need no login.
Do not expose them to an untrusted network.

## Connections

A connection is one saved set of client-mode parameters for one queue manager.

| Field | Meaning |
|---|---|
| Name | Label shown in the tool. It is also the `connection` metric tag. |
| Host, Port | Where the queue manager listens. Port defaults to `1414`. |
| Queue Manager | Queue manager name. |
| Channel | Server-connection channel. |
| User, Password | Optional. When a user is set, the tool sends it with MQCSP authentication. The password is stored encrypted. |
| Client Type | `NATIVE` (IBM MQ classes for Java, default) or `SPRING_JMS` (JMS connection factory). Both read state with PCF. |
| TLS Mode | `NONE`, `TLS` or `MUTUAL_TLS`. See below. |
| Cipher Spec | Required for `TLS` and `MUTUAL_TLS`. |
| Truststore Path | Required for `TLS` and `MUTUAL_TLS`, unless `mqmonitor.tls.truststore-path` is set. |
| Keystore Path | Required for `MUTUAL_TLS`, unless `mqmonitor.tls.keystore-path` is set. |
| Certificate Label | Optional. The keystore alias of the client certificate to present. |
| Queue Filter | Wildcard pattern for queue names, for example `APP.*`. Default `*`. A filter that matches nothing gives an empty list. |
| Hide System Queues | On by default. Hides queues whose names start with `SYSTEM.`. |
| Sample Interval | Seconds between samples. Default `5`. Minimum `1`. This sets the load on MQ. |
| Prometheus Export | Off by default. When on, the connection appears in `/actuator/prometheus`. |
| Reply Queue Model | Model queue for the temporary reply queue. Default `SYSTEM.DEFAULT.MODEL.QUEUE`. |
| Thresholds | Optional overrides: max depth, depth percent, no-consumer seconds, oldest message age seconds. Empty means the global default. |

TLS Mode:

- `NONE`: no TLS.
- `TLS`: the client checks the server certificate. It needs a cipher spec and a truststore.
- `MUTUAL_TLS`: the server also checks the client. It needs a cipher spec, a truststore and a keystore.

Certificate stores are PKCS12 files.
You mount them into the container and give their paths.
They are never stored in the database.
Store passwords come from `mqmonitor.tls.*`.
The form has no password field for stores.
A path on a connection overrides the deployment-wide path for that connection only.

## Sampling model

- A sampler runs for a connection only while at least one browser watches its dashboard.
- It stops 30 seconds after the last watcher leaves. This is `mqmonitor.sampling.watcher-grace-seconds`.
- An idle deployment makes no MQ calls, except for connections with Prometheus Export on.
- The connection list page shows reachability and health with a queue-manager-only inquiry every 30 seconds while the page is open. This is `mqmonitor.sampling.list-interval-seconds`.
- A connection with Prometheus Export on is sampled all the time, with or without watchers.
- The browser never talks to MQ. The server pushes each sample to watchers over Server-Sent Events.
- Samples are not stored.
- One MQ connection stays open per running sampler. It reconnects after a failure.

## Health

Health is `HEALTHY` or `UNHEALTHY`, with a reason.

| Object | Unhealthy when | Health Reason |
|---|---|---|
| Queue | Depth is at or above the absolute threshold (when set). | `DEPTH_THRESHOLD` |
| Queue | Depth is at or above the percent threshold of max depth. | `DEPTH_PERCENT` |
| Queue | Open input count is 0 for at least the no-consumer time. | `NO_CONSUMER` |
| Queue | Oldest message age is above the threshold (when set). | `OLDEST_MESSAGE_AGE` |
| Channel | Status is not `RUNNING`. | `CHANNEL_NOT_RUNNING` |
| Connection | The queue manager cannot be reached. | `UNREACHABLE` |
| Connection | Any queue or channel is unhealthy. | `OBJECTS_UNHEALTHY` |

A healthy object has reason `OK`.

## Metrics

`/actuator/prometheus` exposes `ibmmq_*` series only.
Only connections with Prometheus Export on appear.
The default JVM, system and HTTP metrics are disabled.

| Metric | Tags |
|---|---|
| `ibmmq_qmgr_reachable` | `connection`, `qmgr` |
| `ibmmq_queue_depth` | `connection`, `qmgr`, `queue` |
| `ibmmq_queue_max_depth` | `connection`, `qmgr`, `queue` |
| `ibmmq_queue_open_input_count` | `connection`, `qmgr`, `queue` |
| `ibmmq_queue_open_output_count` | `connection`, `qmgr`, `queue` |
| `ibmmq_queue_oldest_message_age_seconds` | `connection`, `qmgr`, `queue` |
| `ibmmq_channel_status` | `connection`, `qmgr`, `channel` |
| `ibmmq_health` | `connection`, `qmgr`, `object`, `name`, `reason` |

`ibmmq_channel_status` is `1` when the channel is `RUNNING`, else `0`.
`ibmmq_health` is `1` when healthy, else `0`.
The `object` tag is `connection`, `queue` or `channel`.
The `reason` tag is the lower-case Health Reason, for example `no_consumer`.
A metric is absent when MQ gives no value, for example the oldest message age when MONQ is off.

## TLS and legacy queue managers

Set the TLS Mode, the Cipher Spec and the stores on the connection.
Enter the Cipher Spec as an MQ CipherSpec name or as a JSSE cipher suite name.
The tool maps an MQ CipherSpec such as `ANY_TLS12_OR_HIGHER` to the suite form the MQ client takes.
A value that is not a known CipherSpec is passed through as a suite name.
The tool sets `com.ibm.mq.cfg.useIBMCipherMappings=false`, so JSSE names work on any JDK.

Old queue managers (MQ 7.5 and 8.0) may only offer cipher specs that JDK 21 blocks.
Examples: `TLS_RSA_WITH_AES_128_CBC_SHA256`, `TLS_RSA_WITH_AES_256_CBC_SHA256`, `TLS_RSA_WITH_AES_256_CBC_SHA`.
Set `MQMONITOR_LEGACY_TLS=true` on the container.
`entrypoint.sh` then adds `-Djava.security.properties=/app/deploy/java.security.legacy-tls` to `JAVA_TOOL_OPTIONS`.
The file [deploy/java.security.legacy-tls](deploy/java.security.legacy-tls) changes `jdk.tls.disabledAlgorithms`:

- It allows `TLSv1` and `TLSv1.1`.
- It allows the `TLS_RSA_*` cipher suites.
- It allows SHA1-signed handshakes.
- It keeps every other JDK 21 default entry.

This weakens TLS for the whole JVM, for every connection in the tool.
Turn it on only when you have such a queue manager.
Certificates signed with SHA1 may still fail, because the JDK blocks them in a separate setting, `jdk.certpath.disabledAlgorithms`. This file does not change that.

## MQ authorities

Create a user or group on the queue manager host for the tool.
Run [deploy/setmqaut-monitor.sh](deploy/setmqaut-monitor.sh):

```sh
deploy/setmqaut-monitor.sh QM1 mqmonitor
deploy/setmqaut-monitor.sh QM1 -g mqmonitors
```

The script prints each `setmqaut` command before it runs it. It grants:

| Object | Authorities |
|---|---|
| Queue manager | `+connect +inq +dsp` |
| All queues (`**`) | `+inq +dsp` |
| `SYSTEM.ADMIN.COMMAND.QUEUE` | `+put +inq +dsp` |
| `SYSTEM.DEFAULT.MODEL.QUEUE` | `+dsp +get +put +inq` |
| All channels (`**`) | `+inq +dsp` |

If you set another Reply Queue Model on a connection, grant that queue the same authorities as `SYSTEM.DEFAULT.MODEL.QUEUE`.
You may also need CHLAUTH and CONNAUTH rules for the user on the channel. The script does not set them.

When an authority is missing, MQ returns reason 2035 (`MQRC_NOT_AUTHORIZED`).
The tool shows it as "authority missing" with the object name.
A wrong password also gives 2035 at connect time.

## Compatibility

- The floor is IBM MQ 7.5. The tool asks only for PCF attributes that exist since 7.5.
- It is tested in Docker against MQ 9.x and 10.x. MQ 7.5 and 8.0 are checked at code level only, because they cannot run in Docker.
- A value the queue manager does not give is shown as `n/a`. Example: the oldest message age and the last put and get times when queue monitoring (MONQ) is off.
- A Queue Filter that matches no queue makes MQ return reason 2085. The tool treats this as an empty list.
- With `SPRING_JMS`, replies come back in the queue manager encoding. The tool decodes each reply with its own encoding. It also reads the multi-message error reply that MQ sends for a filter with no match.
- Old queue managers may need the legacy TLS override.

## Kubernetes

[deploy/k8s-example.yaml](deploy/k8s-example.yaml) has a Secret, a ConfigMap, a Deployment and a Service.
It has no Ingress.
Run one replica.

Create the TLS secret from your PKCS12 files:

```sh
kubectl create secret generic mqmonitor-tls \
  --from-file=truststore.p12 --from-file=keystore.p12
```

Mount it in the pod and point the default store paths into it:

```yaml
env:
  - name: MQMONITOR_TLS_TRUSTSTORE_PATH
    value: /etc/mqmonitor/tls/truststore.p12
  - name: MQMONITOR_TLS_KEYSTORE_PATH
    value: /etc/mqmonitor/tls/keystore.p12
volumeMounts:
  - name: tls
    mountPath: /etc/mqmonitor/tls
    readOnly: true
volumes:
  - name: tls
    secret:
      secretName: mqmonitor-tls
      optional: true
```

The secret is optional, so the pod also starts without it, for connections with TLS Mode `NONE`.
Set the store passwords in `mqmonitor-secrets` (`MQMONITOR_TLS_TRUSTSTORE_PASSWORD`, `MQMONITOR_TLS_KEYSTORE_PASSWORD`).

## Development

- Use JDK 21. With sdkman: `JAVA_HOME=$HOME/.sdkman/candidates/java/21.0.10-oracle`.
- Unit tests: `mvn test`. They start no queue manager.
- Integration tests: `mvn test -Pintegration`. They need Docker and pull the `linux/amd64` IBM MQ image. On Apple Silicon they run under emulation and start slowly.
- Build the image: `docker build -t ibmmq-monitoring-tool:0.1.0 .`

TLS and mutual TLS are tested against a real queue manager in `NativePcfTransportIT` and `JmsPcfTransportIT`.
`MqContainer.tlsInstance()` in `src/test/java/com/tsa/mqmonitor/support/MqContainer.java` shows the manual procedure:

1. Make a self-signed server key and certificate with `openssl req -x509`.
2. Import the server certificate into a PKCS12 truststore with `keytool -importcert`.
3. Make a client key pair in a PKCS12 keystore with `keytool -genkeypair`, and export its certificate.
4. Copy the server key and certificate into `/etc/mqm/pki/keys/default/` and the client certificate into `/etc/mqm/pki/trust/0/` of the MQ container before it starts.
5. Set `SSLCIPH(ANY_TLS12_OR_HIGHER)` on the channel. Use `SSLCAUTH(REQUIRED)` for mutual TLS.
6. Connect with the truststore (and the keystore plus label `client` for mutual TLS).

## Documents

- [GLOSSARY.md](GLOSSARY.md): the terms used here.
- [docs/adr](docs/adr): the decisions and their reasons.
- [docs/superpowers/specs/2026-10-30-ibmmq-monitoring-tool-design.md](docs/superpowers/specs/2026-10-30-ibmmq-monitoring-tool-design.md): the agreed design.
