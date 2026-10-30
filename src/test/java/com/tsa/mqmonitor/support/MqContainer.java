package com.tsa.mqmonitor.support;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * IBM MQ developer queue manager {@code QM1} for integration tests. The image has no arm64
 * manifest, so the container always runs as linux/amd64 (emulated on Apple Silicon).
 */
public class MqContainer extends GenericContainer<MqContainer> {
  public static final String PASSWORD = "passw0rd";
  public static final String STORE_PASSWORD = "changeit";
  public static final String CLIENT_LABEL = "client";

  private static final DockerImageName IMAGE = DockerImageName.parse("icr.io/ibm-messaging/mq:latest");

  private static MqContainer plain;
  private static MqContainer tls;

  private Path tlsDir;

  private MqContainer() {
    super(IMAGE);
    withCreateContainerCmdModifier(cmd -> cmd.withPlatform("linux/amd64"));
    withEnv("LICENSE", "accept");
    withEnv("MQ_QMGR_NAME", "QM1");
    withEnv("MQ_ADMIN_PASSWORD", PASSWORD);
    withEnv("MQ_APP_PASSWORD", PASSWORD);
    withExposedPorts(1414);
    waitingFor(
        Wait.forLogMessage("(?s).*Started queue manager.*", 1)
            .withStartupTimeout(Duration.ofMinutes(3)));
  }

  /** The shared plain-TCP queue manager, started on first use and removed by Ryuk at JVM exit. */
  public static synchronized MqContainer instance() {
    if (plain == null) {
      plain = new MqContainer();
      plain.start();
    }
    return plain;
  }

  /**
   * A second queue manager with a generated server key. The image then sets {@code
   * SSLCIPH(ANY_TLS12_OR_HIGHER) SSLCAUTH(OPTIONAL)} on the DEV channels and trusts the generated
   * client certificate. Also defines {@code DEV.MTLS.SVRCONN} with {@code SSLCAUTH(REQUIRED)}.
   */
  public static synchronized MqContainer tlsInstance() {
    if (tls == null) {
      var c = new MqContainer();
      c.tlsDir = generateTlsMaterial();
      c.withCopyFileToContainer(
          MountableFile.forHostPath(c.tlsDir.resolve("server.key"), 0644),
          "/etc/mqm/pki/keys/default/tls.key");
      c.withCopyFileToContainer(
          MountableFile.forHostPath(c.tlsDir.resolve("server.crt"), 0644),
          "/etc/mqm/pki/keys/default/tls.crt");
      c.withCopyFileToContainer(
          MountableFile.forHostPath(c.tlsDir.resolve("client.crt"), 0644),
          "/etc/mqm/pki/trust/0/client.crt");
      c.start();
      c.mqsc(
          "DEFINE CHANNEL('DEV.MTLS.SVRCONN') CHLTYPE(SVRCONN) SSLCIPH(ANY_TLS12_OR_HIGHER) SSLCAUTH(REQUIRED) REPLACE",
          "SET CHLAUTH('DEV.MTLS.SVRCONN') TYPE(BLOCKUSER) USERLIST('nobody') ACTION(REPLACE)",
          "SET CHLAUTH('DEV.MTLS.SVRCONN') TYPE(USERMAP) CLNTUSER('admin') USERSRC(MAP) MCAUSER('mqm') ACTION(REPLACE)");
      tls = c;
    }
    return tls;
  }

  /** PKCS12 truststore holding the queue manager certificate (TLS instance only). */
  public String truststore() {
    return tlsDir.resolve("trust.p12").toString();
  }

  /** PKCS12 keystore holding the client key under alias {@value #CLIENT_LABEL} (TLS instance only). */
  public String keystore() {
    return tlsDir.resolve("client.p12").toString();
  }

  /** Runs the MQSC commands with {@code runmqsc QM1} and returns its output. */
  public String mqsc(String... commands) {
    return exec("runmqsc QM1", String.join("\n", commands) + "\n");
  }

  /** Runs a shell command inside the container with {@code stdin} piped to it; fails on non-zero exit. */
  public String exec(String command, String stdin) {
    String quoted = "'" + stdin.replace("'", "'\\''") + "'";
    try {
      ExecResult r = execInContainer("bash", "-c", "printf '%s' " + quoted + " | " + command);
      if (r.getExitCode() != 0) {
        throw new IllegalStateException(
            command + " exited " + r.getExitCode() + ":\n" + r.getStdout() + r.getStderr());
      }
      return r.getStdout();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static Path generateTlsMaterial() {
    try {
      Path dir = Files.createTempDirectory("mq-tls");
      dir.toFile().deleteOnExit();
      String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
      run(dir, "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
          "-keyout", "server.key", "-out", "server.crt", "-subj", "/CN=QM1", "-days", "2");
      run(dir, keytool, "-importcert", "-noprompt", "-alias", "qm1", "-file", "server.crt",
          "-storetype", "PKCS12", "-keystore", "trust.p12", "-storepass", STORE_PASSWORD);
      run(dir, keytool, "-genkeypair", "-alias", CLIENT_LABEL, "-keyalg", "RSA", "-keysize", "2048",
          "-dname", "CN=mq-monitor", "-validity", "2",
          "-storetype", "PKCS12", "-keystore", "client.p12", "-storepass", STORE_PASSWORD);
      run(dir, keytool, "-exportcert", "-rfc", "-alias", CLIENT_LABEL, "-file", "client.crt",
          "-keystore", "client.p12", "-storepass", STORE_PASSWORD);
      return dir;
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void run(Path dir, String... command) throws IOException {
    Process p = new ProcessBuilder(command).directory(dir.toFile())
        .redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes());
    try {
      if (p.waitFor() != 0) throw new IllegalStateException(String.join(" ", command) + " failed:\n" + out);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
