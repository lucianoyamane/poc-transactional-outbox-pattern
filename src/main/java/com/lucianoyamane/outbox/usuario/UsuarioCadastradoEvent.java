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
