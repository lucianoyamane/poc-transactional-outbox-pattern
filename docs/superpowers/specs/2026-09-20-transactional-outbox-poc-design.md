# POC: Transactional Outbox Pattern — Design

## Contexto e problema

Em sistemas distribuídos, salvar um registro no banco e publicar um evento em um
message broker (ex: Kafka) não são operações atômicas entre si. Se a aplicação
falhar entre as duas escritas, o sistema fica inconsistente (ex: usuário criado
mas e-mail de boas-vindas nunca disparado).

O Transactional Outbox Pattern resolve isso persistindo o evento em uma tabela
`outbox_event` na **mesma transação local** do banco que salva o dado de
negócio. Um processo separado (Outbox Consumer) lê essa tabela e publica os
eventos pendentes no broker de forma assíncrona e resiliente a falhas.

Esta é uma POC didática: cenário de cadastro de usuário disparando um evento
`UsuarioCadastrado`, simulando o envio de um e-mail de boas-vindas.

## Objetivo

Projeto Spring Boot, Java 25, módulo único, demonstrando:
1. Escrita atômica de dado de negócio + evento outbox na mesma transação.
2. Um Outbox Consumer (poller agendado) publicando os eventos pendentes no Kafka.
3. Um consumidor do próprio evento (mesma aplicação) fechando o ciclo ponta a ponta.
4. Ambiente de infraestrutura (Postgres + Kafka) via Docker Compose — o projeto
   Spring Boot em si **não** é criado pelo compose, roda localmente.

Fora de escopo: CDC/Debezium (mencionado no material de referência como
abordagem avançada, mas não faz parte desta POC), autenticação, múltiplos
módulos/serviços, deploy em produção.

## Stack

- Java 25, Spring Boot (versão estável mais recente compatível com Java 25 no
  momento da implementação).
- Maven.
- Spring Web, Spring Data JPA, Spring for Apache Kafka.
- Flyway para migrações de schema.
- Postgres 16 (via Docker Compose).
- Kafka em modo KRaft, sem Zookeeper (via Docker Compose).
- Kafka UI (`provectuslabs/kafka-ui`) no Docker Compose, para inspeção visual
  das mensagens — opcional, mas incluído por ser uma POC didática.
- JUnit 5 + Mockito para testes unitários.
- Testcontainers (Postgres + Kafka) para um teste de integração ponta a ponta.

## Domínio

### Entidade `Usuario`
- `id (UUID)`, `nome`, `email`, `criado_em`.

### Tabela `outbox_event`
- `id (UUID)` — também usado como chave de idempotência/dedupe.
- `aggregate_type` (ex: `"Usuario"`)
- `aggregate_id` (UUID do agregado, ex: id do usuário)
- `event_type` (ex: `"UsuarioCadastrado"`)
- `payload` (`jsonb`) — corpo do evento serializado
- `status` (`PENDING`, `PROCESSED`) — apenas dois estados nesta POC; não há
  transição automática para um estado terminal de falha (ver "Tratamento de
  erros")
- `created_at`
- `processed_at` (nullable)
- `attempts` (int, default 0)

Migrações via Flyway: `V1__create_usuarios.sql`, `V2__create_outbox_event.sql`.

## Fluxo end-to-end

1. Cliente chama `POST /usuarios` com `{nome, email}`.
2. `UsuarioService.cadastrar(...)`, dentro de `@Transactional`:
   - Insere `Usuario`.
   - Insere `OutboxEvent` (status `PENDING`) com o payload do evento
     `UsuarioCadastrado`.
   - Commit atômico — ou os dois inserts persistem, ou nenhum.
3. `OutboxPoller` (`@Scheduled(fixedDelay = ...)`):
   - Seleciona lote de `outbox_event` com status `PENDING`, usando
     `SELECT ... FOR UPDATE SKIP LOCKED` (evita duplicidade caso haja mais de
     uma instância rodando o poller).
   - Para cada evento, publica no tópico Kafka `usuario.cadastrado` via
     `KafkaTemplate<String, String>` (payload JSON).
   - Sucesso → atualiza status para `PROCESSED` e seta `processed_at`.
   - Falha (ex: broker indisponível) → mantém `PENDING`, incrementa
     `attempts`, tenta novamente no próximo ciclo. Sem broker de dead-letter
     nesta POC (fora de escopo); número de tentativas é apenas informativo.
4. `NotificacaoListener` (`@KafkaListener` no tópico `usuario.cadastrado`,
   mesma aplicação): recebe o evento e loga a simulação do envio do e-mail de
   boas-vindas.

## Tratamento de erros

- Falha ao publicar no Kafka durante o poll: evento permanece `PENDING`,
  reprocessado no próximo ciclo. Log de warning com o erro.
- Falha do listener ao consumir (exceção no processamento): usa o
  `DefaultErrorHandler` do Spring Kafka com poucas tentativas e log de erro —
  não há fila de dead-letter nesta POC.
- Constraint de unicidade não é necessária no `outbox_event` (cada cadastro
  gera um evento novo); idempotência do lado do consumidor não é tratada nesta
  POC (fora de escopo, mencionar como ponto de evolução).

## Testes

- Unitários: `UsuarioService` (grava usuário + outbox na mesma transação, sem
  subir contexto Spring) e `OutboxPoller` (publica e atualiza status,
  mockando `KafkaTemplate` e o repositório).
- Integração (Testcontainers, Postgres real + Kafka real): cadastra usuário
  via camada de serviço/HTTP, aguarda o poller publicar, e verifica que o
  `NotificacaoListener` recebeu o evento — validando o ciclo completo.
- Testcontainers é usado apenas nos testes automatizados; não substitui nem
  interfere no Docker Compose de desenvolvimento manual.

## Docker Compose (infraestrutura apenas)

Serviços:
- `postgres` (imagem `postgres:16`), com volume nomeado e variáveis de
  ambiente (`POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`) já alinhadas
  com o `application.yml` que será usado pela aplicação local.
- `kafka` (imagem `apache/kafka`, modo KRaft, sem Zookeeper), com
  auto-criação de tópicos habilitada.
- `kafka-ui` (imagem `provectuslabs/kafka-ui`), apontando para o broker acima,
  exposto em porta local para inspeção visual das mensagens.

O projeto Spring Boot não é containerizado nesta POC; roda localmente
(`mvn spring-boot:run` ou pela IDE), conectando-se aos serviços do compose.

## Estrutura do módulo (alto nível)

```
src/main/java/.../outbox/
  usuario/       (Usuario, UsuarioRepository, UsuarioController, UsuarioService, dto)
  outbox/        (OutboxEvent, OutboxEventRepository, OutboxPoller, OutboxEventPublisher)
  notificacao/   (NotificacaoListener)
  config/        (KafkaConfig, se necessário)
src/main/resources/
  db/migration/  (V1__..., V2__...)
  application.yml
src/test/java/...
docker-compose.yml
```
