package com.lucianoyamane.outbox.usuario;

import java.time.Instant;
import java.util.UUID;

public record UsuarioResponse(UUID id, String nome, String email, Instant criadoEm) {

    public static UsuarioResponse de(Usuario usuario) {
        return new UsuarioResponse(usuario.getId(), usuario.getNome(), usuario.getEmail(), usuario.getCriadoEm());
    }
}
