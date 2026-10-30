---
status: accepted
---
# Two client types, NATIVE and SPRING_JMS, behind one MQ gateway

The user requires Spring JMS support because their existing Java application connects to MQ with mutual TLS through Spring JMS, and that setup is the known-good reference at their sites. JMS has no inquire API, so JMS alone cannot monitor queue depth. We therefore give each Connection a Client Type. `NATIVE` uses `MQQueueManager` and `PCFMessageAgent`. `SPRING_JMS` builds the connection with `mq-jms-spring-boot-starter` and IBM's `MQConnectionFactory`, then sends PCF inquire commands as JMS `BytesMessage` to `SYSTEM.ADMIN.COMMAND.QUEUE` and reads the reply from a JMS temporary queue. Both implement one `MqGateway` interface. Everything above the gateway (Samples, Health, UI, metrics) is unaware of the Client Type.

## Consequences

- Both client types ship in the one `com.ibm.mq.allclient` jar. No second dependency tree.
- The JMS path never consumes from an application queue. Read-only holds for both types.
- Default is `NATIVE`. `SPRING_JMS` exists for sites where the JMS TLS configuration is the trusted one.
