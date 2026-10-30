---
status: accepted
---
# No stored history; sample MQ only while a browser watches

The tool shows current state only. Samples are pushed to browsers over Server-Sent Events and discarded. A Sampler for a Connection runs only while at least one Watcher is attached and stops 30 seconds after the last one leaves. This was chosen over storing time series because the user wants a simple tool, and over always-on polling because the tool's only load on MQ is its sampling. An idle deployment makes zero MQ calls. The browser never talks to MQ; the Sample Interval alone sets the load.

## Consequences

- No trends, no alert deduplication, so no webhook or email alerting. A Prometheus endpoint exposes the latest Sample for external alerting.
- Prometheus scrapes on its own schedule, so a Connection with Prometheus Export on runs its Sampler continuously, Watcher or not. The toggle is off by default so the MQ load is an explicit per-Connection choice. Sampling on each scrape was rejected because the scrape interval would silently become the MQ load.
- The metrics endpoint exposes only `ibmmq_*` series. JVM, HTTP and Tomcat metrics are disabled. The tool's own process is not what is monitored.
- Logs are ordinary application logs to stdout, collected by the cluster's log stack. No separate MQ event log.
- PostgreSQL stores only Connections and Thresholds.
- One PCF agent stays open per watched Connection, with reconnect on failure, to avoid repeated channel connects.
