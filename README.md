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

## Troubleshooting

Se `mvn test` falhar com um erro parecido com `client version 1.32 is too old`
(comum em algumas instalações de Docker mais recentes/restritivas), rode os
testes passando a versão da API do Docker diretamente na linha de comando:

```bash
mvn -Dapi.version=1.44 test
```

Isso não é necessário na maioria dos ambientes — é um workaround local para
Docker daemons com uma versão mínima de API mais restritiva que o padrão do
Testcontainers. Não use `MAVEN_OPTS` para isso: essa variável não chega até a
JVM que o Surefire cria para rodar os testes; a flag `-D` na linha de comando
do `mvn` funciona porque o Surefire repassa propriedades `-D` da linha de
comando para a JVM dos testes por padrão.
