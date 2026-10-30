---
status: accepted
---
# Read MQ state with PCF over the native IBM MQ Java client

The tool must never read application messages, and it must reach queue managers from MQ 7.5 up to the current release. JMS was considered but rejected: JMS has no inquire operation, so every JMS monitor casts to the native classes anyway. The MQ REST admin API was rejected as the primary path because it needs the mqweb server, which old queue managers do not have. We use `com.ibm.mq.allclient` in client mode and PCF inquire commands (`MQCMD_INQUIRE_Q_MGR_STATUS`, `MQCMD_INQUIRE_Q`, `MQCMD_INQUIRE_Q_STATUS`, `MQCMD_INQUIRE_CHANNEL_STATUS`). The 9.x client negotiates down to old queue managers. MQ access sits behind one interface so a REST adapter can be added later.

## Consequences

- The MQ user needs only `+inq +dsp` on objects, `+put` on `SYSTEM.ADMIN.COMMAND.QUEUE` and a reply queue. Read-only is enforced by MQ authority, not by code review.
- Attributes absent on old versions are shown as "n/a", never guessed.
- Old versions cannot be run in Docker, so 7.5 and 8.0 compatibility is verified at code level only.
