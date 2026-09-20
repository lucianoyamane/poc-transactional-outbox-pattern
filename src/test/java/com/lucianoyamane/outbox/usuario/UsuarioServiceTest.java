package com.lucianoyamane.outbox.usuario;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
            new UsuarioService(usuarioRepository, outboxEventRepository, createObjectMapper());

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }

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
