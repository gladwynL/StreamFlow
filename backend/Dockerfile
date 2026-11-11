# syntax=docker/dockerfile:1
# One image for every role; the role is chosen at run time with SPRING_PROFILES_ACTIVE.
ARG JDK_IMAGE=eclipse-temurin:21.0.13_9-jdk-alpine
ARG JRE_IMAGE=eclipse-temurin:21.0.13_9-jre-alpine

FROM ${JDK_IMAGE} AS build
WORKDIR /workspace
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
COPY streamflow-events/pom.xml streamflow-events/
COPY streamflow-app/pom.xml streamflow-app/
COPY streamflow-events/src streamflow-events/src
COPY streamflow-app/src streamflow-app/src
# Tests run in CI and locally with Testcontainers; the image build only packages.
RUN --mount=type=cache,target=/root/.m2 \
    sh ./mvnw -B -ntp -q package -DskipTests -Dspotless.check.skip=true -Djacoco.skip=true \
 && java -Djarmode=tools -jar streamflow-app/target/streamflow-app.jar extract --layers --launcher --destination extracted

FROM ${JRE_IMAGE}
RUN addgroup -S -g 10001 streamflow && adduser -S -u 10001 -G streamflow -H streamflow
WORKDIR /app
# Least-frequently changing layers first.
COPY --from=build /workspace/extracted/dependencies/ ./
COPY --from=build /workspace/extracted/spring-boot-loader/ ./
COPY --from=build /workspace/extracted/snapshot-dependencies/ ./
COPY --from=build /workspace/extracted/application/ ./
USER 10001:10001
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70 -XX:+ExitOnOutOfMemoryError"
EXPOSE 8080 8081
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
