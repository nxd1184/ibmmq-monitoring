package com.tsa.mqmonitor.service.impl;

import com.tsa.mqmonitor.enumerable.TlsMode;
import com.tsa.mqmonitor.exception.MqGatewayException;
import com.tsa.mqmonitor.model.TlsSettings;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509KeyManager;

/** Builds the client {@link SSLSocketFactory} for a TLS or mutual TLS MQ connection. */
public final class SslContextFactory {
  static {
    // Lets the MQ client accept JSSE cipher suite names (and the ANY_TLS* aliases) on any JDK.
    System.setProperty("com.ibm.mq.cfg.useIBMCipherMappings", "false");
  }

  private SslContextFactory() {}

  /** Returns {@code null} for {@link TlsMode#NONE}. */
  public static SSLSocketFactory build(TlsSettings tls) {
    if (tls == null || tls.mode() == null || tls.mode() == TlsMode.NONE) return null;
    TrustManager[] trust = trustManagers(tls);
    KeyManager[] keys = tls.mode() == TlsMode.MUTUAL_TLS ? keyManagers(tls) : null;
    try {
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(keys, trust, null);
      return context.getSocketFactory();
    } catch (GeneralSecurityException e) {
      throw new MqGatewayException("cannot create TLS context: " + e.getMessage(), e);
    }
  }

  /** Without a truststore path the JDK default trust anchors apply. */
  private static TrustManager[] trustManagers(TlsSettings tls) {
    if (isBlank(tls.truststorePath())) return null;
    KeyStore store = load(tls.truststorePath(), tls.truststorePassword(), "truststore");
    try {
      var factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      factory.init(store);
      return factory.getTrustManagers();
    } catch (GeneralSecurityException e) {
      throw new MqGatewayException("cannot load truststore: " + tls.truststorePath(), e);
    }
  }

  private static KeyManager[] keyManagers(TlsSettings tls) {
    if (isBlank(tls.keystorePath())) {
      throw new MqGatewayException("mutual TLS needs a keystore");
    }
    KeyStore store = load(tls.keystorePath(), tls.keystorePassword(), "keystore");
    KeyManager[] managers;
    try {
      var factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      factory.init(store, chars(tls.keystorePassword()));
      managers = factory.getKeyManagers();
    } catch (GeneralSecurityException e) {
      throw new MqGatewayException("cannot load keystore: " + tls.keystorePath(), e);
    }
    String label = tls.certificateLabel();
    if (isBlank(label)) return managers;
    for (int i = 0; i < managers.length; i++) {
      if (managers[i] instanceof X509KeyManager km) {
        if (km.getPrivateKey(label) == null) {
          throw new MqGatewayException(
              "certificate label '" + label + "' not found in keystore: " + tls.keystorePath());
        }
        managers[i] = new AliasKeyManager(km, label);
      }
    }
    return managers;
  }

  private static KeyStore load(String path, String password, String kind) {
    try (InputStream in = Files.newInputStream(Path.of(path))) {
      KeyStore store = KeyStore.getInstance("PKCS12");
      store.load(in, chars(password));
      return store;
    } catch (IOException | GeneralSecurityException e) {
      throw new MqGatewayException("cannot load " + kind + ": " + path, e);
    }
  }

  private static char[] chars(String password) {
    return password == null ? null : password.toCharArray();
  }

  private static boolean isBlank(String s) {
    return s == null || s.isBlank();
  }

  /** Always presents the configured client certificate. */
  private static final class AliasKeyManager extends X509ExtendedKeyManager {
    private final X509KeyManager x509KeyManager;
    private final String alias;

    AliasKeyManager(X509KeyManager x509KeyManager, String alias) {
      this.x509KeyManager = x509KeyManager;
      this.alias = alias;
    }

    @Override
    public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
      return alias;
    }

    @Override
    public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
      return alias;
    }

    @Override
    public String[] getClientAliases(String keyType, Principal[] issuers) {
      return x509KeyManager.getClientAliases(keyType, issuers);
    }

    @Override
    public String[] getServerAliases(String keyType, Principal[] issuers) {
      return x509KeyManager.getServerAliases(keyType, issuers);
    }

    @Override
    public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
      return x509KeyManager.chooseServerAlias(keyType, issuers, socket);
    }

    @Override
    public X509Certificate[] getCertificateChain(String alias) {
      return x509KeyManager.getCertificateChain(alias);
    }

    @Override
    public PrivateKey getPrivateKey(String alias) {
      return x509KeyManager.getPrivateKey(alias);
    }
  }
}
