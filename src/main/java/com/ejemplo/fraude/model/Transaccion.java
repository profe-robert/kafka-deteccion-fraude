package com.ejemplo.fraude.model;

public record Transaccion(
    String usuarioId,
    double monto,
    String comercio
) {}