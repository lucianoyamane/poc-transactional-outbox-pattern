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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
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
