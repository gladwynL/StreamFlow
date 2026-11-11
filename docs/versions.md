# Pinned versions

Verified on 2026-11-10. Backend library versions are managed by the Spring Boot 4.1.2 BOM and were
read from the resolved dependency list (`mvnw dependency:list`), not assumed.

## Backend

| Component | Version | Source |
|---|---|---|
| Java (Temurin) | 21.0.13+9 | local JDK; `eclipse-temurin:21.0.13_9` images |
| Spring Boot | 4.1.2 | `backend/pom.xml` parent |
| Spring Framework | 7.0.10 | Boot-managed |
| Spring for Apache Kafka | 4.1.2 | Boot-managed |
| Kafka clients | 4.2.2 | Boot-managed; compatible with the 4.3.2 broker |
| Jackson | 3.1.7 | Boot-managed |
| Flyway (+ PostgreSQL module) | 12.4.0 | Boot-managed; no PostgreSQL 18 compatibility warning observed |
| PostgreSQL JDBC driver | 42.7.14 | Boot-managed |
| HikariCP | 7.0.2 | Boot-managed |
| Tomcat | 11.0.27 | Boot-managed |
| JUnit Jupiter | 6.0.3 | Boot-managed |
| AssertJ / Mockito / Awaitility | 3.27.7 / 5.23.0 / 4.3.0 | Boot-managed |
| Testcontainers | 2.0.5 | Boot-managed |
| Spring WebSocket (`spring-boot-starter-websocket`) | 7.0.10 | Boot-managed; Phase 4 live feed |
| ArchUnit | 1.5.1 | `backend/pom.xml` |
| Spotless Maven plugin / palantir-java-format | 3.11.0 / 2.103.0 | `backend/pom.xml` |
| JaCoCo | 0.8.16 | `backend/pom.xml` |
| Maven (wrapper) | 3.10.0, wrapper 3.3.4 | `backend/.mvn/wrapper/maven-wrapper.properties`; distribution SHA-256 pinned |

## Infrastructure images

| Image | Tag | Where pinned |
|---|---|---|
| Apache Kafka | `apache/kafka:4.3.2` | `.env.example`, `TestImages.java` (a unit test keeps them equal) |
| PostgreSQL | `postgres:18.6-alpine` | `.env.example`, `TestImages.java` |
| JDK / JRE build images | `eclipse-temurin:21.0.13_9-jdk-alpine` / `-jre-alpine` | `backend/Dockerfile` |
| Node build image | `node:24.22.2-alpine` | `frontend/Dockerfile`, `frontend/.nvmrc` |
| nginx | `nginxinc/nginx-unprivileged:1.30.3-alpine` | `frontend/Dockerfile` |

Kafka 4.4.0 (released 2026-10-22) was deliberately not chosen: a 4.3 patch release is more mature.

## Frontend

Exact versions are pinned in `frontend/package.json` (no ranges) and locked in
`package-lock.json`. Each was the newest stable release published at least seven days before
pinning, checked against its peer-dependency ranges.

| Package | Version | Note |
|---|---|---|
| react / react-dom | 19.3.1 | |
| react-router | 8.5.1 | |
| @tanstack/react-query | 5.105.3 | |
| vite / @vitejs/plugin-react | 8.3.5 / 6.2.1 | |
| tailwindcss / @tailwindcss/vite | 4.3.4 | |
| typescript | 6.0.3 | TypeScript 7.0 is not yet supported by typescript-eslint (`<6.1.0`) |
| eslint / typescript-eslint | 10.14.0 / 8.72.1 | |
| vitest / jsdom | 5.1.0 / 30.1.3 | jsdom requires Node ≥ 24.15 |
| @testing-library/react / jest-dom / user-event / dom | 16.3.3 / 7.0.1 / 14.6.8 / 10.4.2 | |
| prettier | 3.9.10 | |
| recharts / react-is | 3.10.1 / 19.3.1 | Phase 4; `react-is` is a recharts peer dependency |
| @playwright/test / @axe-core/playwright | 1.64.1 / 4.14.0 | Phase 4; `playwright-core` overridden to 1.64.1 so axe uses the runner's copy |

Dependencies are installed with `npm ci --ignore-scripts`; no install scripts are needed.

## CI

GitHub Actions: `actions/checkout@v7`, `actions/setup-java@v6`, `actions/setup-node@v7`,
`actions/upload-artifact@v7`, on `ubuntu-24.04`.
