# IBM MQ Monitoring Tool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A read-only Spring Boot web tool that shows the current state of IBM MQ queue managers, queues and channels in realtime, with saved connections, health verdicts and Prometheus metrics.

**Architecture:** All MQ reads go through one `MqGateway` built on PCF inquire commands. A `PcfTransport` abstraction has two implementations, native `PCFMessageAgent` and Spring JMS `BytesMessage` round trips, so parsing lives in one place. A `Sampler` per connection runs only while watched (or while Prometheus Export is on), pushes each `EvaluatedSample` to SSE listeners and the metrics binder. PostgreSQL stores connections only.

**Tech Stack:** Java 21, Maven, Spring Boot 3.5.16, Spring Data JDBC, Flyway 13.12.0, PostgreSQL 42.7.14, Thymeleaf, HTMX 2.0.11 + htmx-ext-sse 2.2.4 (webjars), `com.ibm.mq.allclient` 9.4.5.1, `mq-jms-spring-boot-starter` 3.5.15, Spring Security, Micrometer Prometheus, Testcontainers 2.0.5, JUnit 5, AssertJ.

**Spec:** `docs/superpowers/specs/2026-10-30-ibmmq-monitoring-tool-design.md`. Vocabulary: `GLOSSARY.md`. Decisions: `docs/adr/0001` to `0004`.

## Global Constraints

- Java 21 (`<java.version>21</java.version>`, `maven.compiler.release` 21). Local JDK: `~/.sdkman/candidates/java/21.0.10-oracle`. Run Maven with `JAVA_HOME` set to it.
- Root package `com.tsa.mqmonitor`. Maven `groupId` `com.tsa`, `artifactId` `ibmmq-monitoring-tool`.
- The tool never calls `MQGET`, `MQPUT` (except the PCF command queue), browse, or any JMS consumer on an application queue. Any PR that does is rejected.
- No CDN. HTMX and the SSE extension come from webjars. CSS is a local file.
- Secrets: `MQMONITOR_ADMIN_USERNAME`, `MQMONITOR_ADMIN_PASSWORD`, `MQMONITOR_ENCRYPTION_KEY` (base64 of 32 bytes). App fails to start if any is missing.
- Property prefix `mqmonitor`. Defaults: `mqmonitor.sampling.watcher-grace-seconds=30`, `mqmonitor.sampling.list-interval-seconds=30`, `mqmonitor.thresholds.depth-percent=80`, `mqmonitor.thresholds.no-consumer-seconds=60`, `mqmonitor.mq.connect-timeout-seconds=10`.
- Glossary term "Connection" is the Java type `MqConnection`, to avoid `java.sql.Connection`. All other glossary terms keep their names in code.
- Attributes MQ does not report are `null`, rendered as `n/a`. Never `-1` or `0` in the UI.
- Commit messages: plain conventional commits, no AI attribution lines (user's global rule).
- Commit author identity: this path is not in the user's identity table. **Ask the user which author to use before the first commit** and record the mapping.
- Integration tests use Testcontainers. Tag them `@Tag("integration")`. `mvn test` runs unit tests only. `mvn verify -Pintegration` runs all.

## Review Focus

1. **Unreachable host**: a connection to a host that drops packets must fail within `connect-timeout-seconds`, not hang a sampler thread. Test in Task 6.
2. **Queue monitoring off (`MONQ(OFF)`)**: MQ returns `-1` for oldest message age and blank for last put/get. The UI must show `n/a`. Test in Task 6.
3. **No active channels**: `MQCMD_INQUIRE_CHANNEL_STATUS` returns reason 3065 `MQRCCF_CHL_STATUS_NOT_FOUND`. That is an empty list, not an error. Test in Task 6.
4. **Browser closes the tab**: the SSE emitter must unwatch on completion, timeout and error, or a sampler runs forever. Test in Task 10.
5. **Edit with blank password**: saving an edit form with an empty password keeps the stored password. Test in Task 4.

---

## File Structure

```
ibmmq-monitoring-tool/
├── pom.xml
├── Dockerfile
├── docker-compose.yml                      # postgres for local dev
├── README.md
├── deploy/
│   ├── setmqaut-monitor.sh                 # MQ authorities script
│   └── java.security.legacy-tls            # cipher override for old qmgrs
├── src/main/java/com/tsa/mqmonitor/
│   ├── MqMonitorApplication.java
│   ├── config/MqMonitorProperties.java     # @ConfigurationProperties("mqmonitor")
│   ├── config/SecurityConfig.java
│   ├── config/MetricsConfig.java           # disables default binders
│   ├── crypto/SecretCipher.java
│   ├── connection/MqConnection.java        # Spring Data JDBC aggregate
│   ├── connection/ClientType.java
│   ├── connection/TlsMode.java
│   ├── connection/MqConnectionRepository.java
│   ├── connection/ConnectionForm.java
│   ├── connection/ConnectionFormValidator.java
│   ├── connection/ConnectionService.java
│   ├── connection/ConnectionTestResult.java
│   ├── mq/MqGateway.java                   # interface
│   ├── mq/MqTarget.java                    # plain connect params
│   ├── mq/TlsSettings.java
│   ├── mq/QueueManagerInfo.java
│   ├── mq/QueueInfo.java
│   ├── mq/ChannelInfo.java
│   ├── mq/MqGatewayException.java
│   ├── mq/MqAuthorityException.java
│   ├── mq/MqUnreachableException.java
│   ├── mq/MqGatewayFactory.java            # interface
│   ├── mq/DefaultMqGatewayFactory.java
│   ├── mq/pcf/PcfTransport.java            # interface: send request, get responses
│   ├── mq/pcf/PcfMqGateway.java            # builds requests, parses responses
│   ├── mq/pcf/PcfCodec.java                # PCFMessage <-> byte[]
│   ├── mq/pcf/PcfErrors.java               # MQ reason code -> exception
│   ├── mq/pcf/NativePcfTransport.java      # MQQueueManager + PCFMessageAgent
│   ├── mq/pcf/JmsPcfTransport.java         # Spring JmsTemplate + temp queue
│   ├── mq/tls/SslContextFactory.java
│   ├── health/HealthState.java
│   ├── health/HealthVerdict.java
│   ├── health/Thresholds.java
│   ├── health/HealthEvaluator.java
│   ├── sampling/Sample.java
│   ├── sampling/EvaluatedSample.java
│   ├── sampling/SampleListener.java
│   ├── sampling/Sampler.java
│   ├── sampling/SamplerRegistry.java
│   ├── metrics/MqMeterBinder.java
│   └── web/ConnectionController.java
│   └── web/DashboardController.java
│   └── web/SseController.java
│   └── web/SampleFragmentRenderer.java
├── src/main/resources/
│   ├── application.yml
│   ├── db/migration/V1__connections.sql
│   ├── static/css/app.css
│   └── templates/ (layout.html, login.html, connections/list.html,
│                   connections/form.html, dashboard/index.html,
│                   fragments/qmgr.html, fragments/queues.html, fragments/channels.html)
└── src/test/java/com/tsa/mqmonitor/...     # mirrors main; support/MqContainer.java, support/PostgresTestBase.java
```

---

### Task 1: Project scaffold, database, login

**Files:**
- Create: `pom.xml`, `src/main/java/com/tsa/mqmonitor/MqMonitorApplication.java`, `config/MqMonitorProperties.java`, `config/SecurityConfig.java`, `src/main/resources/application.yml`, `src/main/resources/db/migration/V1__connections.sql`, `src/main/resources/templates/login.html`, `src/main/resources/templates/layout.html`, `docker-compose.yml`, `.gitignore`
- Test: `src/test/java/com/tsa/mqmonitor/support/PostgresTestBase.java`, `src/test/java/com/tsa/mqmonitor/ApplicationStartupTest.java`, `src/test/java/com/tsa/mqmonitor/config/SecurityConfigTest.java`

**Interfaces:**
- Produces: `MqMonitorProperties` with nested records `Admin(String username, String password)`, `Tls(String truststorePath, String truststorePassword, String keystorePath, String keystorePassword)`, `Sampling(int watcherGraceSeconds, int listIntervalSeconds)`, `Thresholds(Integer depthMax, int depthPercent, int noConsumerSeconds, Integer oldestMessageAgeSeconds)`, `Mq(int connectTimeoutSeconds)`, and `String encryptionKey`. Validated with `@NotBlank` on admin username, admin password and encryption key.
- Produces: `PostgresTestBase` (`@SpringBootTest`, `@Testcontainers`, static `PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")`, `@DynamicPropertySource` for datasource and for the three secrets with test values).
- Produces: table `mq_connection` (see Step 4).

- [ ] **Step 1: Write `pom.xml`**

Parent `spring-boot-starter-parent` 3.5.16. Dependencies: `spring-boot-starter-web`, `spring-boot-starter-thymeleaf`, `spring-boot-starter-security`, `spring-boot-starter-validation`, `spring-boot-starter-data-jdbc`, `spring-boot-starter-actuator`, `spring-boot-starter-jms` (via the MQ starter), `io.micrometer:micrometer-registry-prometheus`, `org.flywaydb:flyway-core`, `org.flywaydb:flyway-database-postgresql`, `org.postgresql:postgresql`, `com.ibm.mq:com.ibm.mq.allclient:9.4.5.1`, `com.ibm.mq:mq-jms-spring-boot-starter:3.5.15` (exclude its `com.ibm.mq.allclient` so the pin wins), `org.webjars.npm:htmx.org:2.0.11`, `org.webjars.npm:htmx-ext-sse:2.2.4`, `org.webjars:webjars-locator-lite`. Test: `spring-boot-starter-test`, `spring-security-test`, `org.testcontainers:junit-jupiter:2.0.5`, `org.testcontainers:postgresql:2.0.5`. Surefire excludes `@Tag("integration")` by default; profile `integration` includes it. `java.version` 21.

- [ ] **Step 2: Write the startup test**

```java
class ApplicationStartupTest extends PostgresTestBase {
  @Autowired MqMonitorProperties props;
  @Test void contextLoadsAndFlywayRan(@Autowired JdbcTemplate jdbc) {
    assertThat(jdbc.queryForObject("select count(*) from mq_connection", Integer.class)).isZero();
    assertThat(props.admin().username()).isEqualTo("admin");
  }
}
```

And a test that proves the app refuses to start without the key:

```java
class MissingEncryptionKeyTest {
  @Test void failsWithoutEncryptionKey() {
    var app = new SpringApplicationBuilder(MqMonitorApplication.class)
        .properties("mqmonitor.encryption-key=", "mqmonitor.admin.username=a", "mqmonitor.admin.password=b",
                    "spring.datasource.url=jdbc:postgresql://localhost:1/x", "spring.flyway.enabled=false");
    assertThatThrownBy(app::run).hasRootCauseInstanceOf(BindValidationException.class);
  }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `JAVA_HOME=~/.sdkman/candidates/java/21.0.10-oracle mvn -q test`
Expected: compile failure, `MqMonitorApplication` not found.

- [ ] **Step 4: Write application, properties, migration, security**

`application.yml`: binds the three secrets from env (`mqmonitor.admin.username: ${MQMONITOR_ADMIN_USERNAME}` etc.), datasource from `SPRING_DATASOURCE_*`, `management.endpoints.web.exposure.include: health,prometheus`, `spring.jms.auto-startup: false` is not needed but set `ibm.mq.enabled: false` to stop the starter auto-creating a connection factory from properties (the tool builds factories per connection).

`V1__connections.sql`:

```sql
create table mq_connection (
  id bigserial primary key,
  name varchar(100) not null unique,
  host varchar(255) not null,
  port integer not null default 1414,
  queue_manager varchar(48) not null,
  channel varchar(20) not null,
  username varchar(64),
  password_encrypted text,
  client_type varchar(16) not null default 'NATIVE',
  tls_mode varchar(16) not null default 'NONE',
  cipher_spec varchar(64),
  truststore_path varchar(512),
  keystore_path varchar(512),
  certificate_label varchar(64),
  queue_filter varchar(48) not null default '*',
  hide_system_queues boolean not null default true,
  sample_interval_seconds integer not null default 5,
  prometheus_export boolean not null default false,
  reply_queue_model varchar(48) not null default 'SYSTEM.DEFAULT.MODEL.QUEUE',
  threshold_depth_max integer,
  threshold_depth_percent integer,
  threshold_no_consumer_seconds integer,
  threshold_oldest_message_age_seconds integer,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
```

`SecurityConfig`: form login at `/login`, one `InMemoryUserDetailsManager` user from `MqMonitorProperties.admin()` with `{bcrypt}`-encoded password at startup, all routes authenticated except `/login`, `/css/**`, `/webjars/**`, `/actuator/health`, `/actuator/prometheus`. CSRF on, but ignore `/actuator/**`.

- [ ] **Step 5: Write the security test**

```java
class SecurityConfigTest extends PostgresTestBase {
  @Autowired MockMvc mvc;
  @Test void anonymousIsRedirectedToLogin() throws Exception {
    mvc.perform(get("/connections")).andExpect(status().is3xxRedirection()).andExpect(redirectedUrlPattern("**/login"));
  }
  @Test void prometheusIsPublic() throws Exception {
    mvc.perform(get("/actuator/prometheus")).andExpect(status().isOk());
  }
  @Test void adminCanLogIn() throws Exception {
    mvc.perform(formLogin().user("admin").password("admin-pass")).andExpect(authenticated());
  }
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `mvn -q test` (same `JAVA_HOME`). Expected: PASS, 5 tests.

- [ ] **Step 7: Initialise git and commit**

```bash
cd ibmmq-monitoring-tool && git init -b main
git add -A && git -c user.name="<ASK USER>" -c user.email="<ASK USER>" commit -m "feat: scaffold spring boot app with postgres, flyway and form login"
```

---

### Task 2: Secret cipher

**Files:**
- Create: `crypto/SecretCipher.java`, `config/CryptoConfig.java`
- Test: `crypto/SecretCipherTest.java`

**Interfaces:**
- Produces: `public final class SecretCipher { public SecretCipher(byte[] key32); public static SecretCipher fromBase64(String key); public String encrypt(String plain); public String decrypt(String token); }`. Token format: base64 of `12-byte IV || ciphertext || 16-byte tag`. `encrypt(null)` returns `null`, `decrypt(null)` returns `null`.
- Produces: bean `SecretCipher` built from `MqMonitorProperties.encryptionKey()`. Throws `IllegalStateException("MQMONITOR_ENCRYPTION_KEY must be base64 of 32 bytes")` for a wrong length.

- [ ] **Step 1: Write the failing tests**

```java
class SecretCipherTest {
  SecretCipher cipher = new SecretCipher(new byte[32]);
  @Test void roundTrips() { assertThat(cipher.decrypt(cipher.encrypt("s3cret"))).isEqualTo("s3cret"); }
  @Test void sameInputDifferentTokens() { assertThat(cipher.encrypt("x")).isNotEqualTo(cipher.encrypt("x")); }
  @Test void nullPassesThrough() { assertThat(cipher.encrypt(null)).isNull(); assertThat(cipher.decrypt(null)).isNull(); }
  @Test void tamperedTokenFails() {
    var t = cipher.encrypt("x").toCharArray(); t[t.length - 2] ^= 1;
    assertThatThrownBy(() -> cipher.decrypt(new String(t))).isInstanceOf(IllegalArgumentException.class);
  }
  @Test void rejectsWrongKeyLength() {
    assertThatThrownBy(() -> SecretCipher.fromBase64(Base64.getEncoder().encodeToString(new byte[16])))
      .isInstanceOf(IllegalStateException.class).hasMessageContaining("32 bytes");
  }
}
```

- [ ] **Step 2: Run to verify failure** — `mvn -q test -Dtest=SecretCipherTest`. Expected: compile error.
- [ ] **Step 3: Implement `SecretCipher`** with `Cipher.getInstance("AES/GCM/NoPadding")`, `GCMParameterSpec(128, iv)`, `SecureRandom` IV. Wrap `AEADBadTagException` in `IllegalArgumentException("ciphertext tampered or wrong key")`.
- [ ] **Step 4: Run to verify pass** — Expected: PASS, 5 tests.
- [ ] **Step 5: Commit** — `git commit -m "feat: add AES-GCM secret cipher for connection passwords"`

---

### Task 3: MQ read model, gateway interface and health evaluator

**Files:**
- Create: `mq/QueueManagerInfo.java`, `mq/QueueInfo.java`, `mq/ChannelInfo.java`, `mq/MqGateway.java`, `mq/MqGatewayFactory.java`, `mq/MqTarget.java`, `mq/TlsSettings.java`, `mq/MqGatewayException.java`, `mq/MqAuthorityException.java`, `mq/MqUnreachableException.java`, `connection/ClientType.java`, `connection/TlsMode.java`, `health/HealthState.java`, `health/HealthVerdict.java`, `health/Thresholds.java`, `health/HealthEvaluator.java`, `sampling/Sample.java`, `sampling/EvaluatedSample.java`
- Test: `health/HealthEvaluatorTest.java`

**Interfaces:**
- Produces:
  - `record QueueManagerInfo(String name, String status, Integer commandLevel, String platform)` — `status` is one of `RUNNING`, `QUIESCING`, `UNKNOWN`.
  - `record QueueInfo(String name, int depth, int maxDepth, Integer openInputCount, Integer openOutputCount, Integer oldestMessageAgeSeconds, LocalDateTime lastPut, LocalDateTime lastGet)`.
  - `record ChannelInfo(String name, String type, String status)` — `status` is the MQ name, e.g. `RUNNING`, `STOPPED`, `RETRYING`, `INACTIVE`.
  - `interface MqGateway extends AutoCloseable { QueueManagerInfo inquireQueueManager(); List<QueueInfo> inquireQueues(String filter, boolean hideSystemQueues); List<ChannelInfo> inquireChannels(); @Override void close(); }`
  - `interface MqGatewayFactory { MqGateway open(MqTarget target); }` (implementation in Task 6).
  - `record TlsSettings(TlsMode mode, String cipherSpec, String truststorePath, String truststorePassword, String keystorePath, String keystorePassword, String certificateLabel)` with `static TlsSettings none()`. `enum ClientType { NATIVE, SPRING_JMS }` and `enum TlsMode { NONE, TLS, MUTUAL_TLS }` live in package `connection`.
  - `record MqTarget(String host, int port, String queueManager, String channel, String username, String password, ClientType clientType, TlsSettings tls, String replyQueueModel, Duration connectTimeout)`.
  - `class MqGatewayException extends RuntimeException`; `class MqAuthorityException extends MqGatewayException { String objectName(); }` message `"authority missing on <object> (MQRC 2035)"`; `class MqUnreachableException extends MqGatewayException`.
  - `enum HealthState { HEALTHY, UNHEALTHY }`; `record HealthVerdict(HealthState state, String reason)` with `static HealthVerdict healthy()` (reason `"ok"`) and `static HealthVerdict unhealthy(String reason)`.
  - `record Thresholds(Integer depthMax, int depthPercent, int noConsumerSeconds, Integer oldestMessageAgeSeconds)` with `Thresholds withOverrides(Integer depthMax, Integer depthPercent, Integer noConsumerSeconds, Integer oldestAge)` (non-null override replaces).
  - `record Sample(long connectionId, Instant takenAt, QueueManagerInfo qmgr, List<QueueInfo> queues, List<ChannelInfo> channels, String error)` with `static Sample failed(long id, Instant at, String error)` (qmgr null, lists empty) and `boolean reachable()` (`error == null`).
  - `record EvaluatedSample(Sample sample, HealthVerdict connection, Map<String, HealthVerdict> queues, Map<String, HealthVerdict> channels)`.
  - `class HealthEvaluator { HealthVerdict queue(QueueInfo q, Thresholds t, Instant now, Instant noConsumerSince); HealthVerdict channel(ChannelInfo c); EvaluatedSample evaluate(Sample s, Thresholds t, Map<String, Instant> noConsumerSince, Instant now); }`

- [ ] **Step 1: Write the failing tests**

```java
class HealthEvaluatorTest {
  HealthEvaluator eval = new HealthEvaluator();
  Thresholds t = new Thresholds(null, 80, 60, null);
  Instant now = Instant.parse("2026-10-30T10:00:00Z");
  QueueInfo q(int depth, int max, Integer in, Integer age) { return new QueueInfo("APP.Q", depth, max, in, 1, age, null, null); }

  @Test void depthUnderPercentIsHealthy() { assertThat(eval.queue(q(79, 100, 1, null), t, now, null).state()).isEqualTo(HealthState.HEALTHY); }
  @Test void depthAtPercentIsUnhealthy() { assertThat(eval.queue(q(80, 100, 1, null), t, now, null).reason()).isEqualTo("depth 80 is 80% of max 100"); }
  @Test void absoluteDepthWins() { assertThat(eval.queue(q(10, 5000, 1, null), t.withOverrides(10, null, null, null), now, null).reason()).isEqualTo("depth 10 reached threshold 10"); }
  @Test void noConsumerWithinGraceIsHealthy() { assertThat(eval.queue(q(1, 100, 0, null), t, now, now.minusSeconds(59)).state()).isEqualTo(HealthState.HEALTHY); }
  @Test void noConsumerPastGraceIsUnhealthy() { assertThat(eval.queue(q(1, 100, 0, null), t, now, now.minusSeconds(60)).reason()).isEqualTo("no consumer for 60s"); }
  @Test void unknownOpenInputCountIsNotNoConsumer() { assertThat(eval.queue(q(1, 100, null, null), t, now, now.minusSeconds(999)).state()).isEqualTo(HealthState.HEALTHY); }
  @Test void oldestAgeThreshold() { assertThat(eval.queue(q(1, 100, 1, 301), t.withOverrides(null, null, null, 300), now, null).reason()).isEqualTo("oldest message age 301s over 300s"); }
  @Test void channelNotRunningIsUnhealthy() { assertThat(eval.channel(new ChannelInfo("C", "SVRCONN", "STOPPED")).reason()).isEqualTo("channel status STOPPED"); }
  @Test void unreachableSampleIsUnhealthyConnection() {
    var e = eval.evaluate(Sample.failed(1, now, "MQRC 2538"), t, Map.of(), now);
    assertThat(e.connection().reason()).isEqualTo("queue manager unreachable: MQRC 2538");
  }
  @Test void anyUnhealthyQueueMakesConnectionUnhealthy() {
    var s = new Sample(1, now, new QueueManagerInfo("QM1", "RUNNING", 940, "UNIX"), List.of(q(90, 100, 1, null)), List.of(), null);
    assertThat(eval.evaluate(s, t, Map.of(), now).connection().reason()).isEqualTo("1 queue unhealthy");
  }
}
```

- [ ] **Step 2: Run to verify failure** — `mvn -q test -Dtest=HealthEvaluatorTest`. Expected: compile error.
- [ ] **Step 3: Implement records, enums, interfaces, exceptions and `HealthEvaluator`.** The gateway interface and factory have no implementation yet. Queue checks run in this order and the first failure is the reason: absolute depth, percent (only when `maxDepth > 0`), no consumer (only when `openInputCount != null && == 0 && noConsumerSince != null && Duration >= noConsumerSeconds`), oldest age (only when both non-null). Connection reason format: `"<n> queue(s) unhealthy"`, `"<n> channel(s) unhealthy"`, joined with `", "`; singular when n is 1.
- [ ] **Step 4: Run to verify pass** — Expected: PASS, 10 tests.
- [ ] **Step 5: Commit** — `git commit -m "feat: add MQ read model and health evaluator"`

---

### Task 4: Connection aggregate, validation, service

**Files:**
- Create: `connection/MqConnection.java`, `connection/MqConnectionRepository.java`, `connection/ConnectionForm.java`, `connection/ConnectionFormValidator.java`, `connection/ConnectionService.java`, `connection/ConnectionTestResult.java`
- Test: `connection/ConnectionFormValidatorTest.java`, `connection/ConnectionServiceTest.java` (extends `PostgresTestBase`)

**Interfaces:**
- Consumes: `SecretCipher` (Task 2), `Thresholds`, `MqGateway`, `MqGatewayFactory` interface, `MqTarget`, `TlsSettings`, `MqUnreachableException` (Task 3). The factory is mocked with `@MockitoBean` here because no implementation exists yet.
- Produces:
  - `@Table("mq_connection") record MqConnection(@Id Long id, String name, String host, int port, String queueManager, String channel, String username, String passwordEncrypted, ClientType clientType, TlsMode tlsMode, String cipherSpec, String truststorePath, String keystorePath, String certificateLabel, String queueFilter, boolean hideSystemQueues, int sampleIntervalSeconds, boolean prometheusExport, String replyQueueModel, Integer thresholdDepthMax, Integer thresholdDepthPercent, Integer thresholdNoConsumerSeconds, Integer thresholdOldestMessageAgeSeconds, Instant createdAt, Instant updatedAt)`.
  - `interface MqConnectionRepository extends ListCrudRepository<MqConnection, Long> { Optional<MqConnection> findByName(String name); List<MqConnection> findByPrometheusExportTrue(); }`
  - `class ConnectionForm` — mutable bean with the same fields as the record except `passwordEncrypted`, plus `String password` (plain, may be blank) and `Long id`. Bean Validation: `@NotBlank name, host, queueManager, channel`, `@Min(1) @Max(65535) port`, `@Min(1) sampleIntervalSeconds`, `@NotNull clientType, tlsMode`.
  - `class ConnectionFormValidator implements Validator` — cross-field rules: `TLS` and `MUTUAL_TLS` require `cipherSpec`; `MUTUAL_TLS` requires `keystorePath` **or** a configured default keystore (`MqMonitorProperties.tls().keystorePath()`); `TLS` requires `truststorePath` or the configured default. Error codes `tls.cipherSpec.required`, `tls.keystore.required`, `tls.truststore.required`.
  - `class ConnectionService { MqTarget targetFor(ConnectionForm f); MqTarget targetFor(MqConnection c); List<MqConnection> findAll(); Optional<MqConnection> findById(long id); MqConnection save(ConnectionForm form); void delete(long id); ConnectionForm toForm(MqConnection c); String plainPassword(MqConnection c); ConnectionTestResult test(ConnectionForm form); Thresholds thresholdsFor(MqConnection c); }`
  - `record ConnectionTestResult(boolean ok, String message, QueueManagerInfo qmgr)`.

- [ ] **Step 1: Write the failing validator tests**

```java
class ConnectionFormValidatorTest {
  MqMonitorProperties.Tls noDefaults = new MqMonitorProperties.Tls(null, null, null, null);
  ConnectionFormValidator v = new ConnectionFormValidator(noDefaults);
  ConnectionForm f(TlsMode m) { var f = new ConnectionForm(); f.setTlsMode(m); return f; }
  Errors validate(ConnectionForm f) { var e = new BeanPropertyBindingResult(f, "form"); v.validate(f, e); return e; }

  @Test void noneNeedsNothing() { assertThat(validate(f(TlsMode.NONE)).hasErrors()).isFalse(); }
  @Test void tlsNeedsCipherAndTruststore() {
    var e = validate(f(TlsMode.TLS));
    assertThat(e.getFieldError("cipherSpec").getCode()).isEqualTo("tls.cipherSpec.required");
    assertThat(e.getFieldError("truststorePath").getCode()).isEqualTo("tls.truststore.required");
  }
  @Test void mutualTlsNeedsKeystore() {
    var f = f(TlsMode.MUTUAL_TLS); f.setCipherSpec("ANY_TLS12_OR_HIGHER"); f.setTruststorePath("/t.p12");
    assertThat(validate(f).getFieldError("keystorePath").getCode()).isEqualTo("tls.keystore.required");
  }
  @Test void configuredDefaultStoreSatisfiesRule() {
    var v2 = new ConnectionFormValidator(new MqMonitorProperties.Tls("/t.p12", "p", "/k.p12", "p"));
    var f = f(TlsMode.MUTUAL_TLS); f.setCipherSpec("ANY_TLS12_OR_HIGHER");
    var e = new BeanPropertyBindingResult(f, "form"); v2.validate(f, e);
    assertThat(e.hasErrors()).isFalse();
  }
}
```

- [ ] **Step 2: Write the failing service tests**

```java
class ConnectionServiceTest extends PostgresTestBase {
  @Autowired ConnectionService service; @Autowired MqConnectionRepository repo; @Autowired SecretCipher cipher;
  @MockitoBean MqGatewayFactory gatewayFactory;

  ConnectionForm valid() { var f = new ConnectionForm(); f.setName("QM1"); f.setHost("mq"); f.setPort(1414);
    f.setQueueManager("QM1"); f.setChannel("DEV.ADMIN.SVRCONN"); f.setUsername("admin"); f.setPassword("pw");
    f.setClientType(ClientType.NATIVE); f.setTlsMode(TlsMode.NONE); f.setQueueFilter("*"); f.setSampleIntervalSeconds(5);
    f.setHideSystemQueues(true); f.setReplyQueueModel("SYSTEM.DEFAULT.MODEL.QUEUE"); return f; }

  @Test void savesEncryptedPassword() {
    var saved = service.save(valid());
    assertThat(saved.passwordEncrypted()).isNotEqualTo("pw");
    assertThat(cipher.decrypt(saved.passwordEncrypted())).isEqualTo("pw");
  }
  @Test void blankPasswordOnEditKeepsExisting() {            // Review Focus 5
    var saved = service.save(valid());
    var edit = service.toForm(saved); assertThat(edit.getPassword()).isNull(); edit.setPassword(""); edit.setHost("mq2");
    var again = service.save(edit);
    assertThat(again.host()).isEqualTo("mq2");
    assertThat(service.plainPassword(again)).isEqualTo("pw");
  }
  @Test void thresholdsFallBackToGlobalDefaults() {
    var saved = service.save(valid());
    assertThat(service.thresholdsFor(saved)).isEqualTo(new Thresholds(null, 80, 60, null));
  }
  @Test void thresholdOverrideWins() {
    var f = valid(); f.setThresholdDepthPercent(50);
    assertThat(service.thresholdsFor(service.save(f)).depthPercent()).isEqualTo(50);
  }
  @Test void testConnectionReportsQmgr() throws Exception {
    var gw = mock(MqGateway.class);
    when(gw.inquireQueueManager()).thenReturn(new QueueManagerInfo("QM1", "RUNNING", 940, "UNIX"));
    when(gatewayFactory.open(any())).thenReturn(gw);
    var r = service.test(valid());
    assertThat(r.ok()).isTrue(); assertThat(r.qmgr().name()).isEqualTo("QM1");
    verify(gw).close();
  }
  @Test void testConnectionReportsFailure() {
    when(gatewayFactory.open(any())).thenThrow(new MqUnreachableException("MQRC 2538 host not available", null));
    var r = service.test(valid());
    assertThat(r.ok()).isFalse(); assertThat(r.message()).contains("2538");
  }
  @Test void duplicateNameRejected() {
    service.save(valid());
    assertThatThrownBy(() -> service.save(valid())).isInstanceOf(DuplicateKeyException.class);
  }
}
```

- [ ] **Step 3: Run to verify failure** — Expected: compile errors.
- [ ] **Step 4: Implement.** `save` encrypts `form.password` when non-blank, otherwise copies `passwordEncrypted` from the existing row (lookup by `form.id`). `toForm` never copies the password. `test` builds an `MqTarget` from the form, using `props.tls()` defaults for store paths and passwords when the form leaves them blank and `props.mq().connectTimeoutSeconds()` for the timeout (add `MqTarget targetFor(ConnectionForm f)` and `MqTarget targetFor(MqConnection c)` to the service), calls `gatewayFactory.open(target)`, `inquireQueueManager()`, `close()` in `finally`, and maps `MqGatewayException` to `ok=false` with its message. `thresholdsFor` = `props.thresholds()` as `Thresholds` then `withOverrides(...)` from the four columns.
- [ ] **Step 5: Run to verify pass** — Expected: PASS, 11 tests.
- [ ] **Step 6: Commit** — `git commit -m "feat: add connection aggregate, validation and service"`

---

### Task 5: PCF codec and gateway (transport-independent)

**Files:**
- Create: `mq/pcf/PcfTransport.java`, `mq/pcf/PcfErrors.java`, `mq/pcf/PcfCodec.java`, `mq/pcf/PcfMqGateway.java`
- Test: `mq/pcf/PcfCodecTest.java`, `mq/pcf/PcfMqGatewayTest.java` (fake transport, unit)

**Interfaces:**
- Consumes: `QueueManagerInfo`, `QueueInfo`, `ChannelInfo`, `MqGateway`, exceptions (Task 3).
- Produces:
  - `final class PcfErrors { static MqGatewayException translate(int reason, String objectName, Throwable cause); }` — the mapping in Step 4, shared with Task 7.
  - `interface PcfTransport extends AutoCloseable { List<PCFMessage> send(PCFMessage request) throws MQException, IOException; @Override void close(); }`
  - `final class PcfCodec { static byte[] encode(PCFMessage m); static PCFMessage decode(byte[] bytes); static boolean isLast(PCFMessage m); }` — encode writes into an `MQMessage` via `m.write(mqMessage)` then copies `mqMessage` bytes; decode wraps bytes in `MQMessage` and calls `new PCFMessage(mqMessage)`; `isLast` reads `MQCFH.Control == MQCFC_LAST` via `m.getControl()`.
  - `class PcfMqGateway implements MqGateway { PcfMqGateway(PcfTransport transport); }`

- [ ] **Step 1: Write the failing codec test**

```java
class PcfCodecTest {
  @Test void roundTripsInquireQueue() throws Exception {
    var req = new PCFMessage(CMQCFC.MQCMD_INQUIRE_Q);
    req.addParameter(CMQC.MQCA_Q_NAME, "APP.*");
    req.addParameter(CMQC.MQIA_Q_TYPE, CMQC.MQQT_LOCAL);
    var back = PcfCodec.decode(PcfCodec.encode(req));
    assertThat(back.getCommand()).isEqualTo(CMQCFC.MQCMD_INQUIRE_Q);
    assertThat(back.getStringParameterValue(CMQC.MQCA_Q_NAME).trim()).isEqualTo("APP.*");
  }
}
```

- [ ] **Step 2: Write the failing gateway tests with a fake transport**

The fake records requests and returns canned `PCFMessage` responses built with `new PCFMessage(MQCFT_RESPONSE, command, 1, true)` and `addParameter`.

```java
class PcfMqGatewayTest {
  FakeTransport t = new FakeTransport(); PcfMqGateway gw = new PcfMqGateway(t);

  @Test void inquireQueuesRequestsLocalQueuesWithFilterAndStatus() {
    t.respond(CMQCFC.MQCMD_INQUIRE_Q, qResp("APP.IN", 5, 5000), qResp("SYSTEM.ADMIN.X", 0, 100));
    t.respond(CMQCFC.MQCMD_INQUIRE_Q_STATUS, qStatusResp("APP.IN", 2, 1, 42));
    var qs = gw.inquireQueues("APP.*", true);
    assertThat(t.requests.get(0).getStringParameterValue(CMQC.MQCA_Q_NAME).trim()).isEqualTo("APP.*");
    assertThat(t.requests.get(0).getIntParameterValue(CMQC.MQIA_Q_TYPE)).isEqualTo(CMQC.MQQT_LOCAL);
    assertThat(qs).extracting(QueueInfo::name).containsExactly("APP.IN");
    assertThat(qs.get(0)).satisfies(q -> { assertThat(q.depth()).isEqualTo(5); assertThat(q.openInputCount()).isEqualTo(2); assertThat(q.oldestMessageAgeSeconds()).isEqualTo(42); });
  }
  @Test void systemQueuesShownWhenNotHidden() {
    t.respond(CMQCFC.MQCMD_INQUIRE_Q, qResp("SYSTEM.ADMIN.X", 0, 100)); t.respond(CMQCFC.MQCMD_INQUIRE_Q_STATUS);
    assertThat(gw.inquireQueues("*", false)).hasSize(1);
  }
  @Test void monqOffYieldsNullAge() {                         // Review Focus 2
    t.respond(CMQCFC.MQCMD_INQUIRE_Q, qResp("APP.IN", 0, 100)); t.respond(CMQCFC.MQCMD_INQUIRE_Q_STATUS, qStatusResp("APP.IN", 0, 0, -1));
    assertThat(gw.inquireQueues("*", true).get(0).oldestMessageAgeSeconds()).isNull();
  }
  @Test void blankLastPutIsNull() { /* qStatusResp with MQCACF_LAST_PUT_DATE "        " -> lastPut() null */ }
  @Test void noChannelStatusIsEmptyList() {                    // Review Focus 3
    t.fail(CMQCFC.MQCMD_INQUIRE_CHANNEL_STATUS, new PCFException(2, CMQCFC.MQRCCF_CHL_STATUS_NOT_FOUND, null));
    assertThat(gw.inquireChannels()).isEmpty();
  }
  @Test void notAuthorizedBecomesAuthorityException() {
    t.fail(CMQCFC.MQCMD_INQUIRE_Q, new PCFException(2, CMQC.MQRC_NOT_AUTHORIZED, null));
    assertThatThrownBy(() -> gw.inquireQueues("*", true)).isInstanceOf(MqAuthorityException.class).hasMessageContaining("2035");
  }
  @Test void qmgrStatusMapsToName() {
    t.respond(CMQCFC.MQCMD_INQUIRE_Q_MGR_STATUS, qmgrStatusResp("QM1", CMQCFC.MQQMSTA_RUNNING));
    t.respond(CMQCFC.MQCMD_INQUIRE_Q_MGR, qmgrResp(940, CMQC.MQPL_UNIX));
    var info = gw.inquireQueueManager();
    assertThat(info).isEqualTo(new QueueManagerInfo("QM1", "RUNNING", 940, "UNIX"));
  }
  @Test void closeClosesTransport() { gw.close(); assertThat(t.closed).isTrue(); }
}
```

- [ ] **Step 3: Run to verify failure** — Expected: compile errors.
- [ ] **Step 4: Implement `PcfCodec` and `PcfMqGateway`.**

Requests (attribute lists are fixed so old queue managers are not asked for new attributes):
- `MQCMD_INQUIRE_Q_MGR_STATUS` with no params. Status int to name: `MQQMSTA_RUNNING→RUNNING`, `MQQMSTA_QUIESCING→QUIESCING`, else `UNKNOWN`.
- `MQCMD_INQUIRE_Q_MGR` with `MQIACF_Q_MGR_ATTRS = [MQIA_COMMAND_LEVEL, MQIA_PLATFORM]`. Platform int to name via `MQConstants.lookup(value, "MQPL_.*")` with the `MQPL_` prefix stripped.
- `MQCMD_INQUIRE_Q`: `MQCA_Q_NAME = filter`, `MQIA_Q_TYPE = MQQT_LOCAL`, `MQIACF_Q_ATTRS = [MQCA_Q_NAME, MQIA_CURRENT_Q_DEPTH, MQIA_MAX_Q_DEPTH]`.
- `MQCMD_INQUIRE_Q_STATUS`: `MQCA_Q_NAME = filter`, `MQIACF_Q_STATUS_TYPE = MQIACF_Q_STATUS`, `MQIACF_Q_STATUS_ATTRS = [MQCA_Q_NAME, MQIA_OPEN_INPUT_COUNT, MQIA_OPEN_OUTPUT_COUNT, MQIACF_OLDEST_MSG_AGE, MQCACF_LAST_PUT_DATE, MQCACF_LAST_PUT_TIME, MQCACF_LAST_GET_DATE, MQCACF_LAST_GET_TIME]`. Join to queues by trimmed name. Age `< 0` → `null`. Date or time blank → `null`, else parse `yyyy-MM-dd` + `HH.mm.ss`.
- `MQCMD_INQUIRE_CHANNEL_STATUS`: `MQCACH_CHANNEL_NAME = "*"`, `MQIACH_CHANNEL_INSTANCE_TYPE = MQOT_CURRENT_CHANNEL`. Reason `MQRCCF_CHL_STATUS_NOT_FOUND` → empty list. Status via `MQConstants.lookup(v, "MQCHS_.*")` minus prefix; type via `"MQCHT_.*"`.
- Hide system queues: drop names starting with `SYSTEM.` when the flag is on. Also drop `AMQ.*` dynamic queues always.
- Error mapping: `PCFException`/`MQException` with reason `MQRC_NOT_AUTHORIZED` → `MqAuthorityException(objectName)` where objectName is the filter or `"queue manager"`; reasons `MQRC_HOST_NOT_AVAILABLE`, `MQRC_Q_MGR_NOT_AVAILABLE`, `MQRC_CONNECTION_BROKEN`, `MQRC_CHANNEL_NOT_AVAILABLE`, `MQRC_UNKNOWN_CHANNEL_NAME`, `MQRC_SSL_INITIALIZATION_ERROR` → `MqUnreachableException("MQRC <n> <MQConstants.lookupReasonCode(n)>", e)`; anything else → `MqGatewayException`.

- [ ] **Step 5: Run to verify pass** — Expected: PASS, 9 tests.
- [ ] **Step 6: Commit** — `git commit -m "feat: add PCF codec and transport-independent MQ gateway"`

---

### Task 6: Native transport, TLS, gateway factory, MQ integration test

**Files:**
- Create: `mq/pcf/NativePcfTransport.java`, `mq/tls/SslContextFactory.java`, `mq/DefaultMqGatewayFactory.java`
- Test: `support/MqContainer.java`, `mq/pcf/NativePcfTransportIT.java` (`@Tag("integration")`), `mq/tls/SslContextFactoryTest.java`

**Interfaces:**
- Consumes: `PcfTransport`, `PcfMqGateway`, `MqTarget`, `TlsSettings` (Task 5), `MqMonitorProperties` (Task 1).
- Produces:
  - `class NativePcfTransport implements PcfTransport { static NativePcfTransport connect(MqTarget target, SSLSocketFactory sslOrNull); }` — builds a `Hashtable<String,Object>` with `MQConstants.HOST_NAME_PROPERTY`, `PORT_PROPERTY`, `CHANNEL_PROPERTY`, `USER_ID_PROPERTY`, `PASSWORD_PROPERTY`, `USE_MQCSP_AUTHENTICATION_PROPERTY = true` (only when username non-blank), `CONNECT_OPTIONS_PROPERTY`, `SSL_CIPHER_SUITE_PROPERTY = target.tls().cipherSpec()` and `SSL_SOCKET_FACTORY_PROPERTY` when TLS is on. Creates `MQQueueManager(target.queueManager(), props)` then `PCFMessageAgent(qmgr)` with `agent.setModelQueueName(target.replyQueueModel())` and `agent.setWaitInterval((int) connectTimeout.toMillis())`. Connect timeout: run the constructor in a `CompletableFuture` and `get(connectTimeout)`; on `TimeoutException` throw `MqUnreachableException("connect timed out after <n>s", e)`.
  - `final class SslContextFactory { static SSLSocketFactory build(TlsSettings tls); }` — PKCS12 truststore always; keystore only for `MUTUAL_TLS`; when `certificateLabel` is non-blank wrap the `KeyManager` in an alias-selecting `X509ExtendedKeyManager`. Returns `null` for `TlsMode.NONE`. Also sets `System.setProperty("com.ibm.mq.cfg.useIBMCipherMappings", "false")` once so Oracle/OpenJDK cipher names work.
  - `@Component class DefaultMqGatewayFactory implements MqGatewayFactory` — `NATIVE` → `new PcfMqGateway(NativePcfTransport.connect(target, SslContextFactory.build(target.tls())))`; `SPRING_JMS` → Task 7.
  - `class MqContainer extends GenericContainer<MqContainer>` — image `icr.io/ibm-messaging/mq:latest`, env `LICENSE=accept`, `MQ_QMGR_NAME=QM1`, `MQ_ADMIN_PASSWORD=passw0rd`, `MQ_APP_PASSWORD=passw0rd`, exposed port 1414, wait for log `.*Started web server.*` or port 1414, and a method `void mqsc(String... commands)` that runs `runmqsc QM1` via `execInContainer`. Static singleton shared by integration tests.

- [ ] **Step 1: Write the failing SslContextFactory unit test**

Generate a PKCS12 keystore in `@TempDir` with `keytool -genkeypair` via `ProcessBuilder` (JDK is present), then:

```java
@Test void noneReturnsNull() { assertThat(SslContextFactory.build(TlsSettings.none())).isNull(); }
@Test void tlsBuildsFactoryFromTruststore() { assertThat(SslContextFactory.build(new TlsSettings(TlsMode.TLS, "ANY_TLS12_OR_HIGHER", p12, "changeit", null, null, null))).isNotNull(); }
@Test void mutualTlsWithWrongPasswordFails() { assertThatThrownBy(() -> SslContextFactory.build(new TlsSettings(TlsMode.MUTUAL_TLS, "ANY_TLS12_OR_HIGHER", p12, "changeit", p12, "wrong", null))).isInstanceOf(MqGatewayException.class).hasMessageContaining("keystore"); }
```

- [ ] **Step 2: Write the failing integration test**

```java
@Tag("integration")
class NativePcfTransportIT {
  static MqContainer mq = MqContainer.instance();   // starts once
  MqTarget target(String user, String pw) { return new MqTarget(mq.getHost(), mq.getMappedPort(1414), "QM1", "DEV.ADMIN.SVRCONN", user, pw, ClientType.NATIVE, TlsSettings.none(), "SYSTEM.DEFAULT.MODEL.QUEUE", Duration.ofSeconds(10)); }

  @Test void readsQueueManagerAndDevQueues() {
    try (var gw = new DefaultMqGatewayFactory(props()).open(target("admin", "passw0rd"))) {
      assertThat(gw.inquireQueueManager().status()).isEqualTo("RUNNING");
      assertThat(gw.inquireQueues("DEV.*", true)).extracting(QueueInfo::name).contains("DEV.QUEUE.1", "DEV.QUEUE.2", "DEV.QUEUE.3");
      assertThat(gw.inquireChannels()).extracting(ChannelInfo::name).contains("DEV.ADMIN.SVRCONN");  // our own connection
    }
  }
  @Test void depthReflectsMessagesPutByAnotherClient() {
    mq.mqsc("DEFINE QLOCAL(MON.TEST) REPLACE");
    mq.exec("/opt/mqm/samp/bin/amqsput MON.TEST QM1", "one\ntwo\nthree\n");
    try (var gw = open()) { assertThat(gw.inquireQueues("MON.TEST", true).get(0).depth()).isEqualTo(3); }
  }
  @Test void monqOffGivesNullAge() { mq.mqsc("ALTER QLOCAL(MON.TEST) MONQ(OFF)"); /* assert oldestMessageAgeSeconds null */ }   // Review Focus 2
  @Test void wrongPasswordIsUnreachableOrAuthority() {
    assertThatThrownBy(() -> open("admin", "bad")).isInstanceOf(MqGatewayException.class).hasMessageContaining("2035");
  }
  @Test void blackholeHostTimesOut() {                            // Review Focus 1
    var t = new MqTarget("10.255.255.1", 1414, "QM1", "X", null, null, ClientType.NATIVE, TlsSettings.none(), "SYSTEM.DEFAULT.MODEL.QUEUE", Duration.ofSeconds(2));
    long start = System.nanoTime();
    assertThatThrownBy(() -> new DefaultMqGatewayFactory(props()).open(t)).isInstanceOf(MqUnreachableException.class);
    assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(5));
  }
  @Test void tlsChannelConnects() {
    // MqContainer.enableTls(): copies a generated key.pem/cert.pem into /etc/mqm/pki/keys/default/ before start,
    // defines channel DEV.TLS.SVRCONN with SSLCIPH(ANY_TLS12_OR_HIGHER) SSLCAUTH(OPTIONAL) and CHLAUTH rules.
    // Client truststore = PKCS12 containing cert.pem.
  }
  @Test void mutualTlsChannelConnects() { /* SSLCAUTH(REQUIRED) on DEV.MTLS.SVRCONN, client cert added to qmgr trust via MQ_TLS or runmqakm */ }
}
```

If the TLS and mutual TLS cases prove too costly inside the IBM image (the key setup must happen before the queue manager starts), keep them as two tests marked `@Disabled("manual: see README TLS test procedure")` with the exact `runmqakm`/`keytool` steps in the README. State in the task commit which path was taken.

- [ ] **Step 3: Run to verify failure** — `mvn -q test -Dtest=SslContextFactoryTest` compile error; `mvn -q verify -Pintegration -Dit.test=NativePcfTransportIT` compile error.
- [ ] **Step 4: Implement `NativePcfTransport`, `SslContextFactory`, `DefaultMqGatewayFactory`, `MqContainer`.**
- [ ] **Step 5: Run to verify pass** — unit PASS 3; integration PASS (TLS tests pass or are `@Disabled` with reason).
- [ ] **Step 6: Commit** — `git commit -m "feat: add native PCF transport, TLS socket factory and MQ integration tests"`

---

### Task 7: Spring JMS transport

**Files:**
- Create: `mq/pcf/JmsPcfTransport.java`
- Modify: `mq/DefaultMqGatewayFactory.java` (the `SPRING_JMS` branch)
- Test: `mq/pcf/JmsPcfTransportIT.java` (`@Tag("integration")`)

**Interfaces:**
- Consumes: `PcfCodec`, `PcfTransport`, `MqTarget`, `SslContextFactory`, `MqContainer`.
- Produces: `class JmsPcfTransport implements PcfTransport { static JmsPcfTransport connect(MqTarget target, SSLSocketFactory sslOrNull); }`

- [ ] **Step 1: Write the failing integration test** — same shape as `NativePcfTransportIT.readsQueueManagerAndDevQueues` and `depthReflectsMessagesPutByAnotherClient`, with `ClientType.SPRING_JMS`. Add:

```java
@Test void multiMessageResponseIsReassembled() {
  for (int i = 0; i < 60; i++) mq.mqsc("DEFINE QLOCAL(MANY.Q" + i + ") REPLACE");   // forces several response messages
  try (var gw = open()) { assertThat(gw.inquireQueues("MANY.*", true)).hasSize(60); }
}
@Test void neverOpensAnApplicationQueueForInput() {
  try (var gw = open()) { gw.inquireQueues("DEV.*", true); }
  var status = mq.mqscOutput("DISPLAY QSTATUS(DEV.QUEUE.1) IPPROCS");
  assertThat(status).contains("IPPROCS(0)");
}
```

- [ ] **Step 2: Run to verify failure** — compile error.
- [ ] **Step 3: Implement `JmsPcfTransport`.** Build `com.ibm.mq.jakarta.jms.MQConnectionFactory` with `setTransportType(WMQConstants.WMQ_CM_CLIENT)`, host, port, channel, queue manager, `setSSLCipherSuite`, `setSSLSocketFactory`, `setStringProperty(WMQConstants.USERID/PASSWORD)` and `setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true)` when username non-blank. Wrap in `org.springframework.jms.core.JmsTemplate` with `setReceiveTimeout(connectTimeout)`. Per `send`: inside one `JmsTemplate.execute(SessionCallback)`: create `TemporaryQueue`, destination `queue:///SYSTEM.ADMIN.COMMAND.QUEUE?targetClient=1` (MQ target client, no RFH2), `BytesMessage` with `writeBytes(PcfCodec.encode(request))`, `setStringProperty("JMS_IBM_Format", "MQADMIN")` (`MQConstants.MQFMT_ADMIN`), `setIntProperty("JMS_IBM_MsgType", MQConstants.MQMT_REQUEST)`, `setJMSReplyTo(tempQueue)`, `setJMSExpiration` via `producer.setTimeToLive(connectTimeout)`; then loop `consumer.receive(timeout)` collecting `PcfCodec.decode(bytes)` until `PcfCodec.isLast(msg)`. A `null` receive is `MqUnreachableException("no PCF reply within <n>s")`. Map the first response's `getCompCode()/getReason()` through the same error mapping as Task 5 (via `PcfErrors.translate`). The temporary queue is created from `SYSTEM.DEFAULT.MODEL.QUEUE` by default; when `target.replyQueueModel()` differs, set `MQConnectionFactory.setTemporaryModel(replyQueueModel)`.
- [ ] **Step 4: Run to verify pass** — `mvn -q verify -Pintegration -Dit.test=JmsPcfTransportIT`. Expected: PASS, 4 tests.
- [ ] **Step 5: Commit** — `git commit -m "feat: add Spring JMS PCF transport"`

---

### Task 8: Sampler and registry

**Files:**
- Create: `sampling/SampleListener.java`, `sampling/Sampler.java`, `sampling/SamplerRegistry.java`, `config/SchedulingConfig.java`
- Modify: `connection/ConnectionService.java` (notify the registry on save and delete)
- Test: `sampling/SamplerTest.java`, `sampling/SamplerRegistryTest.java`

**Interfaces:**
- Consumes: `MqGateway`, `MqGatewayFactory`, `MqTarget`, `HealthEvaluator`, `Thresholds`, `EvaluatedSample`, `ConnectionService`, `MqMonitorProperties.sampling()`.
- Produces:
  - `interface SampleListener { void onSample(EvaluatedSample sample); }`
  - `class Sampler { Sampler(MqConnection c, MqTarget target, Thresholds t, MqGatewayFactory f, HealthEvaluator e, Clock clock, List<SampleListener> listeners); EvaluatedSample sampleOnce(); void close(); }` — keeps one open gateway, reopens on `MqGatewayException`, keeps `Map<String, Instant> noConsumerSince` across calls (set when `openInputCount == 0`, removed otherwise), and when `inquire*` throws produces `Sample.failed(...)` then closes the gateway so the next call reconnects.
  - `@Component class SamplerRegistry { String watch(long connectionId); void unwatch(long connectionId, String watcherId); void watchList(String watcherId); void unwatchList(String watcherId); Optional<EvaluatedSample> latest(long connectionId); void addListener(SampleListener l); void connectionChanged(long id); void connectionDeleted(long id); void refreshPrometheusExports(); }` — uses a `TaskScheduler`. Rules: a connection samples at `sampleIntervalSeconds` while `watchers(id) > 0` or `prometheusExport`; after the last watcher leaves, a stop is scheduled `watcherGraceSeconds` later and cancelled if a watcher returns; `watchList` runs a queue-manager-only sample (`inquireQueueManager()` only, queues and channels empty) for **every** connection at `listIntervalSeconds` unless that connection already has a full sampler running. `connectionChanged` restarts that sampler with the new target. On startup (`@EventListener(ApplicationReadyEvent)`) `refreshPrometheusExports()` starts samplers for `findByPrometheusExportTrue()`.

- [ ] **Step 1: Write the failing `SamplerTest`**

```java
@Test void reusesOneGatewayAcrossSamples() { sampler.sampleOnce(); sampler.sampleOnce(); verify(factory, times(1)).open(any()); }
@Test void failureProducesFailedSampleAndReconnectsNextTime() {
  when(gw.inquireQueueManager()).thenThrow(new MqUnreachableException("MQRC 2009 connection broken", null)).thenReturn(qm);
  assertThat(sampler.sampleOnce().sample().error()).isEqualTo("MQRC 2009 connection broken");
  sampler.sampleOnce();
  verify(factory, times(2)).open(any()); verify(gw).close();
}
@Test void noConsumerSinceIsTrackedAcrossSamples() {
  clock.set(T0); when(gw.inquireQueues(any(), anyBoolean())).thenReturn(List.of(q(0 /*in*/)));
  sampler.sampleOnce(); clock.set(T0.plusSeconds(61));
  assertThat(sampler.sampleOnce().queues().get("APP.Q").reason()).isEqualTo("no consumer for 61s");
}
@Test void consumerReturningResetsTracking() { /* in=0 then in=1 then in=0 -> healthy at the third sample */ }
@Test void listenersReceiveEverySample() { sampler.sampleOnce(); verify(listener).onSample(any()); }
```

- [ ] **Step 2: Write the failing `SamplerRegistryTest`** using a controllable `TaskScheduler` (e.g. a `ThreadPoolTaskScheduler` replaced by a test double that records scheduled tasks and lets the test fire them) and a mocked `MqGatewayFactory`.

```java
@Test void watchStartsSamplingAtConnectionInterval() { reg.watch(1); assertThat(scheduler.periodicTasks()).hasSize(1); assertThat(scheduler.period(0)).isEqualTo(Duration.ofSeconds(5)); }
@Test void secondWatcherDoesNotStartSecondSampler() { reg.watch(1); reg.watch(1); assertThat(scheduler.periodicTasks()).hasSize(1); }
@Test void samplerStopsGraceAfterLastWatcherLeaves() {
  var w = reg.watch(1); reg.unwatch(1, w);
  assertThat(scheduler.periodicTasks()).hasSize(1);                 // still running
  scheduler.advance(Duration.ofSeconds(30));
  assertThat(scheduler.periodicTasks()).isEmpty();
}
@Test void watcherReturningWithinGraceCancelsStop() { var w = reg.watch(1); reg.unwatch(1, w); scheduler.advance(Duration.ofSeconds(29)); reg.watch(1); scheduler.advance(Duration.ofSeconds(5)); assertThat(scheduler.periodicTasks()).hasSize(1); }
@Test void prometheusExportSamplesWithoutWatchers() { connection(2).prometheusExport = true; reg.refreshPrometheusExports(); assertThat(scheduler.periodicTasks()).hasSize(1); }
@Test void prometheusExportIgnoresUnwatch() { connection(2).prometheusExport = true; reg.refreshPrometheusExports(); var w = reg.watch(2); reg.unwatch(2, w); scheduler.advance(Duration.ofMinutes(5)); assertThat(scheduler.periodicTasks()).hasSize(1); }
@Test void listWatchSamplesQmgrOnlyAtListInterval() {
  reg.watchList("w"); scheduler.fireAll();
  verify(gw, atLeastOnce()).inquireQueueManager(); verify(gw, never()).inquireQueues(any(), anyBoolean());
  assertThat(scheduler.period(0)).isEqualTo(Duration.ofSeconds(30));
}
@Test void latestIsEmptyBeforeFirstSample() { assertThat(reg.latest(1)).isEmpty(); }
@Test void connectionDeletedStopsSampler() { reg.watch(1); reg.connectionDeleted(1); assertThat(scheduler.periodicTasks()).isEmpty(); }
```

- [ ] **Step 3: Run to verify failure** — compile errors.
- [ ] **Step 4: Implement.** `SchedulingConfig` provides a `ThreadPoolTaskScheduler` with pool size 8 and thread prefix `mq-sampler-`. Sampler runs are guarded so a slow sample never overlaps itself (`scheduleWithFixedDelay`). `ConnectionService.save/delete` call `connectionChanged`/`connectionDeleted` (modify Task 4's service; keep the registry an optional dependency via `ObjectProvider` to avoid cycles).
- [ ] **Step 5: Run to verify pass** — Expected: PASS, 14 tests.
- [ ] **Step 6: Commit** — `git commit -m "feat: add watcher-gated sampler registry"`

---

### Task 9: Prometheus metrics binder

**Files:**
- Create: `metrics/MqMeterBinder.java`, `config/MetricsConfig.java`
- Test: `metrics/MqMeterBinderTest.java`, `metrics/PrometheusEndpointTest.java` (extends `PostgresTestBase`)

**Interfaces:**
- Consumes: `SampleListener`, `EvaluatedSample`, `SamplerRegistry.addListener`, `MqConnectionRepository`.
- Produces: `@Component class MqMeterBinder implements SampleListener` registering `MultiGauge`s named exactly: `ibmmq_qmgr_reachable`, `ibmmq_queue_depth`, `ibmmq_queue_max_depth`, `ibmmq_queue_open_input_count`, `ibmmq_queue_open_output_count`, `ibmmq_queue_oldest_message_age_seconds`, `ibmmq_channel_status`, `ibmmq_health`. Tags: `connection` (name), `qmgr`; plus `queue` or `channel`; `ibmmq_health` adds `object` (`connection|queue|channel`), `name`, `reason`. `ibmmq_channel_status` value: `RUNNING=1`, otherwise `0`. Null attributes are not published. Only connections with `prometheusExport` are published; a sample for a non-export connection is ignored, and `connectionChanged` with export turned off removes its rows.

- [ ] **Step 1: Write the failing tests**

```java
class MqMeterBinderTest {
  SimpleMeterRegistry reg = new SimpleMeterRegistry(); MqMeterBinder binder = new MqMeterBinder(reg);
  @Test void publishesDepthForExportedConnection() {
    binder.onSample(evaluated(connection("QM1", export = true), queue("APP.Q", depth = 7)));
    assertThat(reg.get("ibmmq_queue_depth").tags("connection", "QM1", "queue", "APP.Q").gauge().value()).isEqualTo(7);
  }
  @Test void ignoresNonExportedConnection() { binder.onSample(evaluated(connection("QM2", export = false), queue("A", 1))); assertThat(reg.find("ibmmq_queue_depth").gauge()).isNull(); }
  @Test void nullAgeNotPublished() { /* oldestMessageAgeSeconds null -> no ibmmq_queue_oldest_message_age_seconds series */ }
  @Test void healthReasonIsATag() { /* ibmmq_health{object="queue",name="APP.Q",reason="depth 80 is 80% of max 100"} == 0 */ }
  @Test void unreachableSamplePublishesZeroReachable() { /* ibmmq_qmgr_reachable == 0 and queue rows from the previous sample are removed */ }
}
class PrometheusEndpointTest extends PostgresTestBase {
  @Test void exposesOnlyIbmmqSeries() throws Exception {
    var body = mvc.perform(get("/actuator/prometheus")).andReturn().getResponse().getContentAsString();
    assertThat(body).doesNotContain("jvm_", "process_", "system_", "http_server_", "tomcat_", "logback_", "executor_", "jdbc_", "hikaricp_", "disk_");
  }
}
```

- [ ] **Step 2: Run to verify failure.**
- [ ] **Step 3: Implement.** `MetricsConfig` sets `management.metrics.enable.all=false` and `management.metrics.enable.ibmmq=true` in `application.yml`, and disables binders with `management.metrics.binders.*.enabled=false` where those properties still exist; verify with the endpoint test, not by reading docs.
- [ ] **Step 4: Run to verify pass** — Expected: PASS, 6 tests.
- [ ] **Step 5: Commit** — `git commit -m "feat: expose ibmmq_* prometheus metrics for exported connections"`

---

### Task 10: Dashboard, SSE stream and fragments

**Files:**
- Create: `web/DashboardController.java`, `web/SseController.java`, `web/SampleFragmentRenderer.java`, `templates/dashboard/index.html`, `templates/fragments/qmgr.html`, `templates/fragments/queues.html`, `templates/fragments/channels.html`
- Test: `web/SseControllerTest.java`, `web/SampleFragmentRendererTest.java`, `web/DashboardControllerTest.java`

**Interfaces:**
- Consumes: `SamplerRegistry.watch/unwatch/latest/addListener`, `EvaluatedSample`, `ConnectionService`.
- Produces:
  - `GET /connections/{id}` → `dashboard/index` with model `connection`, `latest` (may be null → "waiting for first sample"), `showSystem` query param.
  - `GET /connections/{id}/stream` → `SseEmitter` (timeout 0 = none). On connect: `watcherId = registry.watch(id)`; send the latest sample at once if present. Each later sample for that id sends three named events `qmgr`, `queues`, `channels`, each with the rendered fragment HTML. `onCompletion`, `onTimeout`, `onError` all call `registry.unwatch(id, watcherId)` exactly once.
  - `GET /connections/stream` → list-page SSE: `registry.watchList(watcherId)`, events named `conn-<id>` carrying the rendered row fragment.
  - `class SampleFragmentRenderer { String render(String fragment, EvaluatedSample s, boolean showSystem); }` using `SpringTemplateEngine` with `TemplateSpec("fragments/queues", Set.of("table"), ...)`. Null attribute → `n/a`. Health → CSS class `health-healthy` / `health-unhealthy` and `title` = reason.
  - `index.html` uses `hx-ext="sse" sse-connect="/connections/{id}/stream"` with three `sse-swap` targets.

- [ ] **Step 1: Write the failing tests**

```java
class SampleFragmentRendererTest {   // @SpringBootTest(classes = ThymeleafAutoConfiguration-only slice) or WebMvcTest with the renderer
  @Test void nullAgeRendersNa() { assertThat(render("queues", sampleWith(queue age = null))).contains(">n/a<"); }
  @Test void unhealthyQueueHasClassAndReason() { assertThat(render("queues", unhealthySample)).contains("class=\"health-unhealthy\"").contains("title=\"depth 80 is 80% of max 100\""); }
  @Test void systemQueuesHiddenUnlessFlag() { /* SYSTEM.X absent when showSystem false, present when true */ }
  @Test void authorityErrorRendersObjectName() { assertThat(render("qmgr", failed("authority missing on DEV.* (MQRC 2035)"))).contains("authority missing on DEV.*"); }
}
class SseControllerTest extends PostgresTestBase {
  @MockitoBean SamplerRegistry registry;
  @Test void streamWatchesThenUnwatchesOnCompletion() throws Exception {        // Review Focus 4
    when(registry.watch(1L)).thenReturn("w1");
    var result = mvc.perform(get("/connections/1/stream").with(user("admin"))).andExpect(request().asyncStarted()).andReturn();
    verify(registry).watch(1L);
    result.getAsyncResult(100);    // completes/times out the emitter in the test container
    ((SseEmitter) result.getRequest().getAttribute("emitter")).complete();   // or trigger via controller hook
    verify(registry).unwatch(1L, "w1");
  }
  @Test void firstEventIsLatestSampleWhenPresent() { when(registry.latest(1L)).thenReturn(Optional.of(sample)); /* response body contains "event:queues" */ }
}
```

- [ ] **Step 2: Run to verify failure.**
- [ ] **Step 3: Implement** controllers, renderer, templates. Templates include `layout.html` (nav: Connections, logout) and load `/webjars/htmx.org/dist/htmx.min.js`, `/webjars/htmx-ext-sse/dist/sse.js`, `/css/app.css`. Keep HTML semantic and plain here; styling is Task 12.
- [ ] **Step 4: Run to verify pass** — Expected: PASS, 6 tests.
- [ ] **Step 5: Commit** — `git commit -m "feat: add realtime dashboard with SSE fragments"`

---

### Task 11: Connection pages (list, form, test, delete)

**Files:**
- Create: `web/ConnectionController.java`, `templates/connections/list.html`, `templates/connections/form.html`, `templates/fragments/connection-row.html`, `templates/fragments/test-result.html`
- Test: `web/ConnectionControllerTest.java` (extends `PostgresTestBase`, `@MockitoBean MqGatewayFactory`)

**Interfaces:**
- Consumes: `ConnectionService`, `ConnectionFormValidator`, `SamplerRegistry.latest`.
- Produces routes: `GET /` → redirect `/connections`; `GET /connections` (list, each row shows name, host:port, qmgr, client type, TLS mode, reachability and health from `latest`, SSE-updated via `/connections/stream`); `GET /connections/new`; `POST /connections` (create); `GET /connections/{id}/edit`; `POST /connections/{id}` (update); `POST /connections/{id}/delete` (confirm via `hx-confirm`); `POST /connections/test` (HTMX, returns `fragments/test-result` with the form's current values, never persists). TLS fields are in a `<fieldset>` toggled by `tlsMode` with `hx-trigger="change"` swapping the fieldset (server-side, no custom JS). Password input is always empty on edit, with help text "leave blank to keep the current password".

- [ ] **Step 1: Write the failing tests**

```java
@Test void listRequiresLogin()                         // 302 to /login
@Test void createPersistsAndRedirects()                // POST valid form -> 302 /connections, repo has 1 row
@Test void createWithTlsMissingCipherShowsError()      // 200 form with "tls.cipherSpec.required" message
@Test void editFormHasNoPassword()                     // GET edit -> input[name=password][value=""]
@Test void testEndpointDoesNotPersist()                // POST /connections/test -> fragment contains "QM1" and repo count unchanged
@Test void testEndpointShowsFailure()                  // factory throws MqUnreachableException -> fragment contains "2538"
@Test void deleteRemovesRow()                          // POST delete -> repo empty, registry.connectionDeleted called
@Test void listShowsHealthFromLatestSample()           // registry.latest -> row has class health-unhealthy
```

- [ ] **Step 2: Run to verify failure.**
- [ ] **Step 3: Implement controller and templates.** Register `ConnectionFormValidator` with `@InitBinder`. Port default 1414, interval default 5, filter default `*` are set in `ConnectionForm`'s field initialisers so the "new" form shows them.
- [ ] **Step 4: Run to verify pass** — Expected: PASS, 8 tests.
- [ ] **Step 5: Commit** — `git commit -m "feat: add connection list, form, test and delete pages"`

---

### Task 12: UI design pass

**Files:**
- Modify: all templates under `src/main/resources/templates/`, `static/css/app.css`

**Interfaces:** none new. HTML structure and HTMX attributes from Tasks 10 and 11 stay. Only markup classes and CSS change.

- [ ] **Step 1: Invoke the `ui-ux-pro-max:ui-ux-pro-max` skill** (user requirement). Ask it for: a monitoring-dashboard style suited to a dense ops tool, a palette with clear healthy/unhealthy/unknown semantic colors that pass WCAG AA on both light and dark, a font pairing available locally (system font stack, no web fonts), and layout guidance for a connection list, a form with a conditional fieldset, and a three-panel dashboard (queue manager card, queue table, channel table).
- [ ] **Step 2: Apply the result** to `app.css` (CSS custom properties on `:root`, `prefers-color-scheme: dark` variant) and the templates. Keep tables readable at 1280px wide with 50 queues. Health cell: colored dot + reason on hover and as visible text on the dashboard. Numbers right-aligned, tabular figures.
- [ ] **Step 3: Verify in the browser.** Start PostgreSQL with `docker compose up -d`, run the app with the three env vars, open `/connections`, create a connection to the Testcontainers MQ (or a `docker run icr.io/ibm-messaging/mq` started by hand), open the dashboard, put messages with `amqsput`, and confirm the depth changes without reload. Take screenshots of list, form (TLS fieldset open), dashboard light and dark.
- [ ] **Step 4: Run all tests** — `mvn -q test`. Expected: PASS, no template compile errors.
- [ ] **Step 5: Commit** — `git commit -m "feat: apply dashboard visual design"`

---

### Task 13: Packaging, legacy TLS override, MQ authorities, README

**Files:**
- Create: `Dockerfile`, `deploy/java.security.legacy-tls`, `deploy/setmqaut-monitor.sh`, `deploy/k8s-example.yaml`, `README.md`
- Modify: `docker-compose.yml` (add the app service for local demo)

- [ ] **Step 1: Write `Dockerfile`** — multi-stage: `maven:3.9-eclipse-temurin-21` build, `eclipse-temurin:21-jre` runtime, non-root user, `ENTRYPOINT ["java", "-jar", "/app/app.jar"]`, `JAVA_TOOL_OPTIONS` honoured. When env `MQMONITOR_LEGACY_TLS=true`, the entrypoint script adds `-Djava.security.properties=/app/deploy/java.security.legacy-tls`.
- [ ] **Step 2: Write `java.security.legacy-tls`** — removes `TLSv1, TLSv1.1, SHA1, RSA keySize < 1024` only as far as needed from `jdk.tls.disabledAlgorithms`, with a comment naming the cipher specs it re-enables (`TLS_RSA_WITH_AES_128_CBC_SHA256`, `TLS_RSA_WITH_AES_256_CBC_SHA256`) and the MQ versions that need them (7.5, 8.0).
- [ ] **Step 3: Write `setmqaut-monitor.sh`** — parameters `QMGR` and `USER`; grants exactly: `-t qmgr +connect +inq +dsp`, `-n '**' -t queue +inq +dsp`, `-n 'SYSTEM.ADMIN.COMMAND.QUEUE' -t queue +put +inq +dsp`, `-n 'SYSTEM.DEFAULT.MODEL.QUEUE' -t queue +dsp +get +put +inq`, `-n '**' -t channel +inq +dsp`. Nothing more.
- [ ] **Step 4: Write `README.md`** — sections: what it is and what it never does; quick start with docker compose; environment variables table; connection fields and TLS modes; mounting certificates in Kubernetes (example `Secret` + `volumeMounts`); MQ authorities (link the script); Prometheus metrics list; compatibility note (floor 7.5, attributes that show `n/a` on old versions, legacy TLS flag); manual TLS test procedure if Task 6 left those tests disabled.
- [ ] **Step 5: Verify** — `docker build -t ibmmq-monitoring-tool .` succeeds; `docker compose up` brings up postgres + app; login works; `curl localhost:8080/actuator/health` returns `{"status":"UP"}`.
- [ ] **Step 6: Commit** — `git commit -m "feat: add docker image, legacy TLS override, MQ authority script and README"`

---

## Self-review notes

- Spec coverage: every spec section maps to a task (stack → 1, Connection → 4 and 11, TLS → 6, Client Type → 6 and 7, objects → 5, sampling → 8, health → 3, metrics → 9, compatibility and authorities → 5, 6, 13, out-of-scope items have no task).
- Type consistency: `MqGateway`, `MqTarget`, `TlsSettings`, exceptions and the `MqGatewayFactory` interface are defined in Task 3 so Task 4 compiles before any transport exists. `MqGateway.inquireQueues(String filter, boolean hideSystemQueues)`, `MqGatewayFactory.open(MqTarget)`, `SamplerRegistry.watch(long) : String`, `unwatch(long, String)`, `EvaluatedSample(sample, connection, queues, channels)` are used with the same shapes in Tasks 4 to 11.
- Review Focus: items 1 to 3 tested in Tasks 5 and 6, item 4 in Task 10, item 5 in Task 4.
