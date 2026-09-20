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
