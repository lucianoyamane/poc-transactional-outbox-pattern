# POC Transactional Outbox Pattern Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a single-module Spring Boot POC that demonstrates the Transactional Outbox Pattern end-to-end — atomic write of a `Usuario` and its outbox event, an in-app poller publishing pending events to Kafka, and an in-app Kafka consumer closing the loop.

**Architecture:** One Maven module (`poc-transactional-outbox-pattern`). `UsuarioService.cadastrar()` writes `usuarios` + `outbox_event` in one `@Transactional` method. `OutboxPoller` (`@Scheduled`) reads pending outbox rows with `SELECT ... FOR UPDATE SKIP LOCKED`, publishes to Kafka via `KafkaTemplate`, marks them processed. `NotificacaoListener` (`@KafkaListener`) consumes the same topic in-process to simulate sending the welcome e-mail. Postgres and Kafka run via Docker Compose; the Spring Boot app itself runs locally, not containerized.

**Tech Stack:** Java 25, Spring Boot 3.5.0, Maven, Spring Web, Spring Data JPA (Hibernate 6), Spring for Apache Kafka, Flyway (+ `flyway-database-postgresql`), PostgreSQL 16, JUnit 5, Mockito, AssertJ, Testcontainers, Awaitility.

**Spec:** `docs/superpowers/specs/2026-09-20-transactional-outbox-poc-design.md`

## Global Constraints

- Java version: 25 (`java.version` Maven property / `--release 25`).
- Single Maven module — no multi-module split.
- Kafka topic name: `usuario.cadastrado` (exact string, used by producer, consumer, and `NewTopic` bean).
- Outbox status values: only `PENDING` and `PROCESSED` (no `FAILED` — see spec's error-handling section: publish failures stay `PENDING` and increment `attempts`).
- The Spring Boot application is **not** containerized; Docker Compose provides only Postgres, Kafka, and Kafka UI.
- Base package: `com.lucianoyamane.outbox`.
- Group/artifact: `com.lucianoyamane:poc-transactional-outbox-pattern`.

---

### Task 1: Project scaffolding (Maven + Spring Boot skeleton)

**Files:**
- Create: `pom.xml`
- Create: `src/main/java/com/lucianoyamane/outbox/OutboxApplication.java`
- Create: `src/main/resources/application.yml`
- Modify: `.gitignore` (only if `target/` is not already ignored)

**Interfaces:**
- Produces: a buildable Spring Boot app (`OutboxApplication` main class), Maven coordinates `com.lucianoyamane:poc-transactional-outbox-pattern`, dependency set every later task relies on (`spring-boot-starter-web`, `spring-boot-starter-data-jpa`, `spring-boot-starter-validation`, `spring-kafka`, `flyway-core`, `flyway-database-postgresql`, `org.postgresql:postgresql`, `spring-boot-starter-test`, `org.testcontainers:junit-jupiter`, `org.testcontainers:postgresql`, `org.testcontainers:kafka`, `org.awaitility:awaitility`).

- [ ] **Step 1: Check `.gitignore` already ignores Maven build output**

Run: `grep -n "target/" .gitignore || true`

If it prints nothing, append `target/` on its own line to `.gitignore`.

- [ ] **Step 2: Write `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.5.0</version>
        <relativePath/>
    </parent>

    <groupId>com.lucianoyamane</groupId>
    <artifactId>poc-transactional-outbox-pattern</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <name>poc-transactional-outbox-pattern</name>
    <description>POC do Transactional Outbox Pattern com Spring Boot e Kafka</description>

    <properties>
        <java.version>25</java.version>
        <awaitility.version>4.2.2</awaitility.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-validation</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-database-postgresql</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>postgresql</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>kafka</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <version>${awaitility.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 3: Write the main application class**

```java
package com.lucianoyamane.outbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class OutboxApplication {

    public static void main(String[] args) {
        SpringApplication.run(OutboxApplication.class, args);
    }
}
```

- [ ] **Step 4: Write `application.yml`**

```yaml
spring:
  application:
    name: poc-transactional-outbox-pattern
  datasource:
    url: jdbc:postgresql://localhost:5432/outbox
    username: outbox
    password: outbox
  jpa:
    hibernate:
      ddl-auto: validate
    open-in-view: false
  flyway:
    enabled: true
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
    consumer:
      group-id: notificacao-service
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer

outbox:
  poller:
    fixed-delay-ms: 2000

server:
  port: 8080
```

- [ ] **Step 5: Verify the project builds**

Run: `mvn -q -DskipTests package`
Expected: `BUILD SUCCESS`, and `target/poc-transactional-outbox-pattern-0.0.1-SNAPSHOT.jar` exists. The app will not start yet (no database available) — that's expected at this step; we're only verifying compilation and packaging.

- [ ] **Step 6: Commit**

```bash
git add pom.xml src/main/java/com/lucianoyamane/outbox/OutboxApplication.java src/main/resources/application.yml .gitignore
git commit -m "chore: scaffold Spring Boot module for the outbox POC"
```

---

### Task 2: Infra (Docker Compose) + Flyway schema

**Files:**
- Create: `docker-compose.yml`
- Create: `src/main/resources/db/migration/V1__create_usuarios.sql`
- Create: `src/main/resources/db/migration/V2__create_outbox_event.sql`

**Interfaces:**
- Consumes: `application.yml` datasource config from Task 1 (`jdbc:postgresql://localhost:5432/outbox`, user/password `outbox`/`outbox`).
- Produces: running Postgres (port 5432), Kafka (port 9092, internal `kafka:29092`), Kafka UI (port 8081) via `docker compose up -d`; tables `usuarios` and `outbox_event` that Task 3/4 entities map to.

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
services:
  postgres:
    image: postgres:16-alpine
    container_name: outbox-postgres
    environment:
      POSTGRES_DB: outbox
      POSTGRES_USER: outbox
      POSTGRES_PASSWORD: outbox
    ports:
      - "5432:5432"
    volumes:
      - outbox-postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U outbox -d outbox"]
      interval: 5s
      timeout: 5s
      retries: 5

  kafka:
    image: apache/kafka:3.8.0
    container_name: outbox-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: outbox-kafka-ui
    ports:
      - "8081:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
    depends_on:
      - kafka

volumes:
  outbox-postgres-data:
```

- [ ] **Step 2: Write `V1__create_usuarios.sql`**

```sql
CREATE TABLE usuarios (
    id UUID PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    criado_em TIMESTAMP NOT NULL
);
```

- [ ] **Step 3: Write `V2__create_outbox_event.sql`**

```sql
CREATE TABLE outbox_event (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('PENDING', 'PROCESSED')),
    created_at TIMESTAMP NOT NULL,
    processed_at TIMESTAMP,
    attempts INT NOT NULL DEFAULT 0
);

CREATE INDEX idx_outbox_event_status ON outbox_event (status);
```

- [ ] **Step 4: Start the infra and verify migrations apply**

Run: `docker compose up -d`
Expected: three containers running — check with `docker compose ps`, wait until `outbox-postgres` shows `healthy`.

Run: `mvn -q spring-boot:run &` then, after a few seconds, `curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/actuator/health || true` (actuator isn't added, so a 404 on any mapped path — e.g. `curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/usuarios` returning `405` or `200`, not a connection error — is enough to prove the app booted). Check the app logs for `Successfully validated 2 migrations` (Flyway) and no stack trace. Stop the app with `kill %1` (or `fg` then Ctrl+C) once confirmed.

Expected: Flyway log line `Successfully applied 2 migrations to schema "public"` on first run, app stays up (no crash on startup).

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml src/main/resources/db/migration/V1__create_usuarios.sql src/main/resources/db/migration/V2__create_outbox_event.sql
git commit -m "feat: add docker-compose infra and outbox/usuarios Flyway migrations"
```

---

### Task 3: Outbox domain model (`OutboxStatus`, `OutboxEvent`, `OutboxEventRepository`)

**Files:**
- Create: `src/main/java/com/lucianoyamane/outbox/outbox/OutboxStatus.java`
- Create: `src/main/java/com/lucianoyamane/outbox/outbox/OutboxEvent.java`
- Create: `src/main/java/com/lucianoyamane/outbox/outbox/OutboxEventRepository.java`
- Test: `src/test/java/com/lucianoyamane/outbox/outbox/OutboxEventTest.java`

**Interfaces:**
- Consumes: `outbox_event` table from Task 2 (columns: `id, aggregate_type, aggregate_id, event_type, payload, status, created_at, processed_at, attempts`).
- Produces:
  - `enum OutboxStatus { PENDING, PROCESSED }`
  - `OutboxEvent.novoPendente(UUID aggregateId, String aggregateType, String eventType, String payload)` — static factory used by `UsuarioService` (Task 4).
  - `OutboxEvent#getId(): UUID`, `#getAggregateType(): String`, `#getAggregateId(): UUID`, `#getEventType(): String`, `#getPayload(): String`, `#getStatus(): OutboxStatus`, `#getCreatedAt(): Instant`, `#getProcessedAt(): Instant`, `#getAttempts(): int`.
  - `OutboxEvent#marcarComoProcessado(Instant quando): void`, `#registrarTentativaFalha(): void` — used by `OutboxPoller` (Task 5).
  - `OutboxEventRepository extends JpaRepository<OutboxEvent, UUID>` with `List<OutboxEvent> buscarPendentesParaProcessar(Pageable pageable)` — used by `OutboxPoller` (Task 5) and the integration test (Task 7).

- [ ] **Step 1: Write the failing test for `OutboxEvent` behavior**

```java
package com.lucianoyamane.outbox.outbox;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxEventTest {

    @Test
    void novoPendenteComecaComStatusPendingEZeroTentativas() {
        UUID aggregateId = UUID.randomUUID();

        OutboxEvent evento = OutboxEvent.novoPendente(aggregateId, "Usuario", "UsuarioCadastrado", "{}");

        assertThat(evento.getId()).isNotNull();
        assertThat(evento.getAggregateId()).isEqualTo(aggregateId);
        assertThat(evento.getAggregateType()).isEqualTo("Usuario");
        assertThat(evento.getEventType()).isEqualTo("UsuarioCadastrado");
        assertThat(evento.getPayload()).isEqualTo("{}");
        assertThat(evento.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(evento.getAttempts()).isZero();
        assertThat(evento.getProcessedAt()).isNull();
    }

    @Test
    void marcarComoProcessadoAtualizaStatusEDataDeProcessamento() {
        OutboxEvent evento = OutboxEvent.novoPendente(UUID.randomUUID(), "Usuario", "UsuarioCadastrado", "{}");
        Instant quando = Instant.now();

        evento.marcarComoProcessado(quando);

        assertThat(evento.getStatus()).isEqualTo(OutboxStatus.PROCESSED);
        assertThat(evento.getProcessedAt()).isEqualTo(quando);
    }

    @Test
    void registrarTentativaFalhaIncrementaTentativasEMantemPendente() {
        OutboxEvent evento = OutboxEvent.novoPendente(UUID.randomUUID(), "Usuario", "UsuarioCadastrado", "{}");

        evento.registrarTentativaFalha();
        evento.registrarTentativaFalha();

        assertThat(evento.getAttempts()).isEqualTo(2);
        assertThat(evento.getStatus()).isEqualTo(OutboxStatus.PENDING);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -Dtest=OutboxEventTest test`
Expected: FAIL to compile — `OutboxStatus` and `OutboxEvent` don't exist yet.

- [ ] **Step 3: Write `OutboxStatus`**

```java
package com.lucianoyamane.outbox.outbox;

public enum OutboxStatus {
    PENDING,
    PROCESSED
}
```

- [ ] **Step 4: Write `OutboxEvent`**

```java
package com.lucianoyamane.outbox.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_event")
public class OutboxEvent {

    @Id
    private UUID id;

    @Column(name = "aggregate_type", nullable = false)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false)
    private UUID aggregateId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OutboxStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "processed_at")
    private Instant processedAt;

    @Column(nullable = false)
    private int attempts;

    protected OutboxEvent() {
        // JPA
    }

    private OutboxEvent(UUID id, String aggregateType, UUID aggregateId, String eventType, String payload,
                         OutboxStatus status, Instant createdAt, Instant processedAt, int attempts) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.status = status;
        this.createdAt = createdAt;
        this.processedAt = processedAt;
        this.attempts = attempts;
    }

    public static OutboxEvent novoPendente(UUID aggregateId, String aggregateType, String eventType, String payload) {
        return new OutboxEvent(UUID.randomUUID(), aggregateType, aggregateId, eventType, payload,
                OutboxStatus.PENDING, Instant.now(), null, 0);
    }

    public void marcarComoProcessado(Instant quando) {
        this.status = OutboxStatus.PROCESSED;
        this.processedAt = quando;
    }

    public void registrarTentativaFalha() {
        this.attempts++;
    }

    public UUID getId() {
        return id;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    public int getAttempts() {
        return attempts;
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `mvn -q -Dtest=OutboxEventTest test`
Expected: `Tests run: 3, Failures: 0, Errors: 0`

- [ ] **Step 6: Write `OutboxEventRepository`**

```java
package com.lucianoyamane.outbox.outbox;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;

import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
    @Query("select o from OutboxEvent o where o.status = com.lucianoyamane.outbox.outbox.OutboxStatus.PENDING order by o.createdAt asc")
    List<OutboxEvent> buscarPendentesParaProcessar(Pageable pageable);
}
```

- [ ] **Step 7: Verify the module still compiles**

Run: `mvn -q -DskipTests compile`
Expected: `BUILD SUCCESS`

- [ ] **Step 8: Commit**

```bash
git add src/main/java/com/lucianoyamane/outbox/outbox/OutboxStatus.java src/main/java/com/lucianoyamane/outbox/outbox/OutboxEvent.java src/main/java/com/lucianoyamane/outbox/outbox/OutboxEventRepository.java src/test/java/com/lucianoyamane/outbox/outbox/OutboxEventTest.java
git commit -m "feat: add OutboxEvent domain model and repository"
```

---

### Task 4: Usuario domain + `UsuarioService` (atomic write of usuario + outbox event)

**Files:**
- Create: `src/main/java/com/lucianoyamane/outbox/usuario/Usuario.java`
- Create: `src/main/java/com/lucianoyamane/outbox/usuario/UsuarioRepository.java`
- Create: `src/main/java/com/lucianoyamane/outbox/usuario/UsuarioCadastradoEvent.java`
- Create: `src/main/java/com/lucianoyamane/outbox/usuario/UsuarioCadastroRequest.java`
- Create: `src/main/java/com/lucianoyamane/outbox/usuario/UsuarioResponse.java`
- Create: `src/main/java/com/lucianoyamane/outbox/usuario/UsuarioService.java`
- Create: `src/main/java/com/lucianoyamane/outbox/usuario/UsuarioController.java`
- Test: `src/test/java/com/lucianoyamane/outbox/usuario/UsuarioServiceTest.java`

**Interfaces:**
- Consumes: `OutboxEvent.novoPendente(...)` and `OutboxEventRepository` (Task 3); `usuarios` table (Task 2).
- Produces:
  - `Usuario#getId(): UUID`, `#getNome(): String`, `#getEmail(): String`, `#getCriadoEm(): Instant`.
  - `UsuarioService#cadastrar(String nome, String email): Usuario` — used by `UsuarioController` and the integration test (Task 7).
  - `POST /usuarios` endpoint accepting `{"nome": "...", "email": "..."}`, returning `201` with `{id, nome, email, criadoEm}`.

- [ ] **Step 1: Write the failing test for `UsuarioService`**

```java
package com.lucianoyamane.outbox.usuario;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucianoyamane.outbox.outbox.OutboxEvent;
import com.lucianoyamane.outbox.outbox.OutboxEventRepository;
import com.lucianoyamane.outbox.outbox.OutboxStatus;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UsuarioServiceTest {

    private final UsuarioRepository usuarioRepository = mock(UsuarioRepository.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final UsuarioService usuarioService =
            new UsuarioService(usuarioRepository, outboxEventRepository, new ObjectMapper());

    @Test
    void cadastrarSalvaUsuarioEEventoOutboxPendente() {
        when(usuarioRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        Usuario usuario = usuarioService.cadastrar("Joao Silva", "joao@example.com");

        assertThat(usuario.getNome()).isEqualTo("Joao Silva");
        assertThat(usuario.getEmail()).isEqualTo("joao@example.com");

        verify(usuarioRepository).save(usuario);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent eventoSalvo = captor.getValue();

        assertThat(eventoSalvo.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(eventoSalvo.getAggregateType()).isEqualTo("Usuario");
        assertThat(eventoSalvo.getAggregateId()).isEqualTo(usuario.getId());
        assertThat(eventoSalvo.getEventType()).isEqualTo("UsuarioCadastrado");
        assertThat(eventoSalvo.getPayload()).contains("joao@example.com").contains("Joao Silva");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -Dtest=UsuarioServiceTest test`
Expected: FAIL to compile — `Usuario`, `UsuarioRepository`, `UsuarioService` don't exist yet.

- [ ] **Step 3: Write `Usuario`**

```java
package com.lucianoyamane.outbox.usuario;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "usuarios")
public class Usuario {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String nome;

    @Column(nullable = false, unique = true)
    private String email;

    @Column(name = "criado_em", nullable = false)
    private Instant criadoEm;

    protected Usuario() {
        // JPA
    }

    public Usuario(UUID id, String nome, String email, Instant criadoEm) {
        this.id = id;
        this.nome = nome;
        this.email = email;
        this.criadoEm = criadoEm;
    }

    public UUID getId() {
        return id;
    }

    public String getNome() {
        return nome;
    }

    public String getEmail() {
        return email;
    }

    public Instant getCriadoEm() {
        return criadoEm;
    }
}
```

- [ ] **Step 4: Write `UsuarioRepository`**

```java
package com.lucianoyamane.outbox.usuario;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface UsuarioRepository extends JpaRepository<Usuario, UUID> {
}
```

- [ ] **Step 5: Write `UsuarioCadastradoEvent`**

```java
package com.lucianoyamane.outbox.usuario;

import java.time.Instant;
import java.util.UUID;

public record UsuarioCadastradoEvent(
        UUID eventId,
        UUID usuarioId,
        String nome,
        String email,
        Instant cadastradoEm
) {
}
```

- [ ] **Step 6: Write `UsuarioService`**

```java
package com.lucianoyamane.outbox.usuario;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucianoyamane.outbox.outbox.OutboxEvent;
import com.lucianoyamane.outbox.outbox.OutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
public class UsuarioService {

    private final UsuarioRepository usuarioRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public UsuarioService(UsuarioRepository usuarioRepository, OutboxEventRepository outboxEventRepository,
                           ObjectMapper objectMapper) {
        this.usuarioRepository = usuarioRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Usuario cadastrar(String nome, String email) {
        Usuario usuario = new Usuario(UUID.randomUUID(), nome, email, Instant.now());
        usuarioRepository.save(usuario);

        UsuarioCadastradoEvent evento = new UsuarioCadastradoEvent(
                UUID.randomUUID(), usuario.getId(), usuario.getNome(), usuario.getEmail(), usuario.getCriadoEm());

        OutboxEvent outboxEvent = OutboxEvent.novoPendente(
                usuario.getId(), "Usuario", "UsuarioCadastrado", serializar(evento));
        outboxEventRepository.save(outboxEvent);

        return usuario;
    }

    private String serializar(UsuarioCadastradoEvent evento) {
        try {
            return objectMapper.writeValueAsString(evento);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Falha ao serializar evento UsuarioCadastrado", e);
        }
    }
}
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `mvn -q -Dtest=UsuarioServiceTest test`
Expected: `Tests run: 1, Failures: 0, Errors: 0`

- [ ] **Step 8: Write `UsuarioCadastroRequest` and `UsuarioResponse`**

```java
package com.lucianoyamane.outbox.usuario;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;

public record UsuarioCadastroRequest(
        @NotBlank String nome,
        @NotBlank @Email String email
) {
}
```

```java
package com.lucianoyamane.outbox.usuario;

import java.time.Instant;
import java.util.UUID;

public record UsuarioResponse(UUID id, String nome, String email, Instant criadoEm) {

    public static UsuarioResponse de(Usuario usuario) {
        return new UsuarioResponse(usuario.getId(), usuario.getNome(), usuario.getEmail(), usuario.getCriadoEm());
    }
}
```

- [ ] **Step 9: Write `UsuarioController`**

```java
package com.lucianoyamane.outbox.usuario;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/usuarios")
public class UsuarioController {

    private final UsuarioService usuarioService;

    public UsuarioController(UsuarioService usuarioService) {
        this.usuarioService = usuarioService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public UsuarioResponse cadastrar(@RequestBody @Valid UsuarioCadastroRequest request) {
        Usuario usuario = usuarioService.cadastrar(request.nome(), request.email());
        return UsuarioResponse.de(usuario);
    }
}
```

- [ ] **Step 10: Verify the module compiles and all tests still pass**

Run: `mvn -q -DskipTests=false test -Dtest=UsuarioServiceTest,OutboxEventTest`
Expected: `Tests run: 4, Failures: 0, Errors: 0`

- [ ] **Step 11: Commit**

```bash
git add src/main/java/com/lucianoyamane/outbox/usuario src/test/java/com/lucianoyamane/outbox/usuario/UsuarioServiceTest.java
git commit -m "feat: add Usuario domain and atomic usuario+outbox write"
```

---

### Task 5: Kafka topic config + `OutboxPoller`

**Files:**
- Create: `src/main/java/com/lucianoyamane/outbox/config/KafkaTopicConfig.java`
- Create: `src/main/java/com/lucianoyamane/outbox/outbox/OutboxPoller.java`
- Test: `src/test/java/com/lucianoyamane/outbox/outbox/OutboxPollerTest.java`

**Interfaces:**
- Consumes: `OutboxEventRepository#buscarPendentesParaProcessar(Pageable)`, `OutboxEvent#marcarComoProcessado`/`#registrarTentativaFalha` (Task 3); `outbox.poller.fixed-delay-ms` property (Task 1).
- Produces: Kafka topic `usuario.cadastrado` (created via `NewTopic` bean, 1 partition/1 replica); `OutboxPoller#publicarPendentes(): void`, scheduled, used only by the Spring scheduler and directly invoked in tests.

- [ ] **Step 1: Write the failing tests for `OutboxPoller`**

```java
package com.lucianoyamane.outbox.outbox;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OutboxPollerTest {

    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    private final OutboxPoller outboxPoller = new OutboxPoller(outboxEventRepository, kafkaTemplate);

    @Test
    void publicaEventoPendenteEMarcaComoProcessado() {
        OutboxEvent evento = OutboxEvent.novoPendente(UUID.randomUUID(), "Usuario", "UsuarioCadastrado", "{}");
        when(outboxEventRepository.buscarPendentesParaProcessar(any(Pageable.class))).thenReturn(List.of(evento));

        SendResult<String, String> sendResult = mock(SendResult.class);
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        outboxPoller.publicarPendentes();

        assertThat(evento.getStatus()).isEqualTo(OutboxStatus.PROCESSED);
        assertThat(evento.getProcessedAt()).isNotNull();
        assertThat(evento.getAttempts()).isZero();
    }

    @Test
    void mantemPendenteERegistraTentativaQuandoPublicacaoFalha() {
        OutboxEvent evento = OutboxEvent.novoPendente(UUID.randomUUID(), "Usuario", "UsuarioCadastrado", "{}");
        when(outboxEventRepository.buscarPendentesParaProcessar(any(Pageable.class))).thenReturn(List.of(evento));
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenThrow(new KafkaException("broker indisponivel"));

        outboxPoller.publicarPendentes();

        assertThat(evento.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(evento.getAttempts()).isEqualTo(1);
    }
}
```

Note: `Instant` and `RecordMetadata` imports above are unused by the final version of this file — remove them if your IDE flags them (they are not needed once the two tests above are the only content).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -q -Dtest=OutboxPollerTest test`
Expected: FAIL to compile — `OutboxPoller` doesn't exist yet.

- [ ] **Step 3: Write `OutboxPoller`**

```java
package com.lucianoyamane.outbox.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);
    private static final int TAMANHO_LOTE = 20;

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxPoller(OutboxEventRepository outboxEventRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${outbox.poller.fixed-delay-ms:2000}")
    @Transactional
    public void publicarPendentes() {
        List<OutboxEvent> pendentes = outboxEventRepository.buscarPendentesParaProcessar(PageRequest.of(0, TAMANHO_LOTE));
        for (OutboxEvent evento : pendentes) {
            publicar(evento);
        }
    }

    private void publicar(OutboxEvent evento) {
        try {
            kafkaTemplate.send(topicoPara(evento.getEventType()), evento.getAggregateId().toString(), evento.getPayload())
                    .get(5, TimeUnit.SECONDS);
            evento.marcarComoProcessado(Instant.now());
        } catch (Exception e) {
            log.warn("Falha ao publicar evento outbox {}: {}", evento.getId(), e.getMessage());
            evento.registrarTentativaFalha();
        }
    }

    private String topicoPara(String eventType) {
        return switch (eventType) {
            case "UsuarioCadastrado" -> "usuario.cadastrado";
            default -> throw new IllegalArgumentException("Tipo de evento desconhecido: " + eventType);
        };
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn -q -Dtest=OutboxPollerTest test`
Expected: `Tests run: 2, Failures: 0, Errors: 0`

- [ ] **Step 5: Write `KafkaTopicConfig`**

```java
package com.lucianoyamane.outbox.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic usuarioCadastradoTopic() {
        return TopicBuilder.name("usuario.cadastrado")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
```

- [ ] **Step 6: Verify the module compiles**

Run: `mvn -q -DskipTests compile`
Expected: `BUILD SUCCESS`

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/lucianoyamane/outbox/config/KafkaTopicConfig.java src/main/java/com/lucianoyamane/outbox/outbox/OutboxPoller.java src/test/java/com/lucianoyamane/outbox/outbox/OutboxPollerTest.java
git commit -m "feat: add OutboxPoller publishing pending events to Kafka"
```

---

### Task 6: `NotificacaoListener` (in-app consumer closing the loop)

**Files:**
- Create: `src/main/java/com/lucianoyamane/outbox/notificacao/NotificacaoListener.java`
- Create: `src/main/java/com/lucianoyamane/outbox/config/KafkaConsumerConfig.java`
- Test: `src/test/java/com/lucianoyamane/outbox/notificacao/NotificacaoListenerTest.java`

**Interfaces:**
- Consumes: Kafka topic `usuario.cadastrado` (String payload, JSON body of `UsuarioCadastradoEvent`).
- Produces: `NotificacaoListener#aoReceberUsuarioCadastrado(String payload): void` (the `@KafkaListener` method); `NotificacaoListener#getMensagensRecebidas(): List<String>` — used by the integration test (Task 7) to assert the message was consumed. `KafkaConsumerConfig` provides the `DefaultErrorHandler` bean the spec's error-handling section requires (a few retries, then just log — no dead-letter topic in this POC); Spring Boot's Kafka autoconfiguration wires any `CommonErrorHandler` bean it finds into the listener container factory automatically, so no custom container factory is needed.

- [ ] **Step 1: Write the failing test**

```java
package com.lucianoyamane.outbox.notificacao;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NotificacaoListenerTest {

    private final NotificacaoListener listener = new NotificacaoListener();

    @Test
    void armazenaMensagemRecebidaParaInspecao() {
        listener.aoReceberUsuarioCadastrado("{\"nome\":\"Joao\"}");

        assertThat(listener.getMensagensRecebidas()).containsExactly("{\"nome\":\"Joao\"}");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -Dtest=NotificacaoListenerTest test`
Expected: FAIL to compile — `NotificacaoListener` doesn't exist yet.

- [ ] **Step 3: Write `NotificacaoListener`**

```java
package com.lucianoyamane.outbox.notificacao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class NotificacaoListener {

    private static final Logger log = LoggerFactory.getLogger(NotificacaoListener.class);

    private final List<String> mensagensRecebidas = Collections.synchronizedList(new ArrayList<>());

    @KafkaListener(topics = "usuario.cadastrado", groupId = "notificacao-service")
    public void aoReceberUsuarioCadastrado(String payload) {
        log.info("[EMAIL BOAS-VINDAS] Simulando envio de e-mail para o evento recebido: {}", payload);
        mensagensRecebidas.add(payload);
    }

    public List<String> getMensagensRecebidas() {
        return List.copyOf(mensagensRecebidas);
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -q -Dtest=NotificacaoListenerTest test`
Expected: `Tests run: 1, Failures: 0, Errors: 0`

- [ ] **Step 5: Write `KafkaConsumerConfig`**

```java
package com.lucianoyamane.outbox.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(1000L, 2L));
    }
}
```

This bean is picked up automatically by Spring Boot's Kafka autoconfiguration
and applied to `NotificacaoListener`'s container: on an exception, it retries
twice (1s apart) then logs and gives up — no dead-letter topic, matching the
spec's error-handling section.

- [ ] **Step 6: Verify the module compiles**

Run: `mvn -q -DskipTests compile`
Expected: `BUILD SUCCESS`

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/lucianoyamane/outbox/notificacao src/main/java/com/lucianoyamane/outbox/config/KafkaConsumerConfig.java src/test/java/com/lucianoyamane/outbox/notificacao/NotificacaoListenerTest.java
git commit -m "feat: add NotificacaoListener and Kafka consumer error handling"
```

---

### Task 7: End-to-end integration test (Testcontainers)

**Files:**
- Test: `src/test/java/com/lucianoyamane/outbox/OutboxApplicationIntegrationTest.java`

**Interfaces:**
- Consumes: `UsuarioController` (`POST /usuarios`) (Task 4), `OutboxEventRepository` (Task 3), `NotificacaoListener#getMensagensRecebidas()` (Task 6), the full Spring context (all prior tasks).
- Produces: nothing new — this is the capstone test proving the whole pattern works together against real Postgres and Kafka.

- [ ] **Step 1: Write the integration test**

```java
package com.lucianoyamane.outbox;

import com.lucianoyamane.outbox.notificacao.NotificacaoListener;
import com.lucianoyamane.outbox.outbox.OutboxEvent;
import com.lucianoyamane.outbox.outbox.OutboxEventRepository;
import com.lucianoyamane.outbox.outbox.OutboxStatus;
import com.lucianoyamane.outbox.usuario.UsuarioCadastroRequest;
import com.lucianoyamane.outbox.usuario.UsuarioResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OutboxApplicationIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    @DynamicPropertySource
    static void propriedades(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private NotificacaoListener notificacaoListener;

    @Test
    void cadastraUsuarioPublicaNoKafkaEConsumidorRecebeOEvento() {
        UsuarioCadastroRequest request = new UsuarioCadastroRequest("Maria Silva", "maria@example.com");

        ResponseEntity<UsuarioResponse> resposta = restTemplate.postForEntity("/usuarios", request, UsuarioResponse.class);

        assertThat(resposta.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(resposta.getBody()).isNotNull();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            List<OutboxEvent> eventos = outboxEventRepository.findAll();
            assertThat(eventos).hasSize(1);
            assertThat(eventos.get(0).getStatus()).isEqualTo(OutboxStatus.PROCESSED);
        });

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(notificacaoListener.getMensagensRecebidas())
                        .anyMatch(mensagem -> mensagem.contains("maria@example.com")));
    }
}
```

- [ ] **Step 2: Run the integration test**

Run: `mvn -q -Dtest=OutboxApplicationIntegrationTest test`
Expected: `Tests run: 1, Failures: 0, Errors: 0`. This pulls `postgres:16-alpine` and `apache/kafka:3.8.0` images via Testcontainers on first run (requires Docker running locally) — allow extra time for image pulls.

- [ ] **Step 3: Run the full test suite**

Run: `mvn -q test`
Expected: `BUILD SUCCESS`, all tests from Tasks 3–7 passing.

- [ ] **Step 4: Commit**

```bash
git add src/test/java/com/lucianoyamane/outbox/OutboxApplicationIntegrationTest.java
git commit -m "test: add end-to-end Testcontainers test for the outbox flow"
```

---

### Task 8: README with run instructions

**Files:**
- Create: `README.md`

**Interfaces:**
- Consumes: everything from Tasks 1–7 (describes how to run them).
- Produces: nothing consumed by other tasks — final documentation deliverable.

- [ ] **Step 1: Write `README.md`**

```markdown
# POC: Transactional Outbox Pattern

POC didática do Transactional Outbox Pattern com Spring Boot, Postgres e Kafka.

Cenário: cadastro de usuário. `UsuarioService` grava o usuário e o evento
`UsuarioCadastrado` na tabela `outbox_event` **na mesma transação**. Um poller
agendado (`OutboxPoller`) publica os eventos pendentes no Kafka. A própria
aplicação consome esse tópico (`NotificacaoListener`) simulando o envio do
e-mail de boas-vindas — fechando o ciclo ponta a ponta.

Veja o desenho completo em
[`docs/superpowers/specs/2026-09-20-transactional-outbox-poc-design.md`](docs/superpowers/specs/2026-09-20-transactional-outbox-poc-design.md).

## Pré-requisitos

- Java 25
- Maven
- Docker + Docker Compose (para a infra e para os testes com Testcontainers)

## Subindo a infraestrutura

```bash
docker compose up -d
```

Isso sobe:
- Postgres em `localhost:5432` (db/user/senha: `outbox`/`outbox`/`outbox`)
- Kafka em `localhost:9092`
- Kafka UI em `http://localhost:8081` (para inspecionar o tópico `usuario.cadastrado`)

## Rodando a aplicação

```bash
mvn spring-boot:run
```

O Flyway aplica as migrações automaticamente na primeira execução.

## Testando o fluxo

```bash
curl -X POST http://localhost:8080/usuarios \
  -H "Content-Type: application/json" \
  -d '{"nome": "Maria Silva", "email": "maria@example.com"}'
```

Acompanhe os logs da aplicação: em poucos segundos o `OutboxPoller` publica o
evento no Kafka e o `NotificacaoListener` loga a simulação do envio do e-mail
de boas-vindas. Você também pode ver a mensagem chegando no tópico
`usuario.cadastrado` pelo Kafka UI.

## Rodando os testes

```bash
mvn test
```

O teste de integração (`OutboxApplicationIntegrationTest`) sobe Postgres e
Kafka via Testcontainers automaticamente — não depende do `docker compose`
acima.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add README with setup and run instructions"
```
