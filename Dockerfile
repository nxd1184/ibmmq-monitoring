# Build stage: compiles the jar inside Docker. Needs network access for Maven.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
# Copy pom.xml first so the dependency layer is cached until the pom changes.
COPY pom.xml .
RUN mvn -q dependency:go-offline
COPY src ./src
RUN mvn -q -DskipTests package \
    && cp target/ibmmq-monitoring-tool-*.jar /build/app.jar

# Runtime stage: JRE only, runs as a non-root user.
FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin mqmonitor
WORKDIR /app
COPY --from=build /build/app.jar /app/app.jar
COPY deploy/java.security.legacy-tls /app/deploy/java.security.legacy-tls
COPY deploy/entrypoint.sh /app/entrypoint.sh
RUN chmod 0755 /app/entrypoint.sh
USER 10001
EXPOSE 8080
ENTRYPOINT ["/app/entrypoint.sh"]
