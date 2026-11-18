package com.ejemplo.fraude.streams;

import com.ejemplo.fraude.model.Transaccion;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import java.time.Duration;

@Configuration
public class FraudeTopologyConfig {

    @Bean
    public KStream<String, Transaccion> fraudDetectionTopology(StreamsBuilder builder) {
        JacksonJsonSerde<Transaccion> transaccionSerde = new JacksonJsonSerde<>(Transaccion.class);

        KStream<String, Transaccion> stream = builder.stream(
                "transacciones-bancarias",
                Consumed.with(Serdes.String(), transaccionSerde)
        );

        // 1. OPERACIÓN STATELESS (Sin estado): Filtro por monto elevado (> $1000)
        stream.filter((usuarioId, tx) -> tx.monto() > 1000.0)
            .mapValues(tx -> "ALERTA MONTO ALTO: Usuario " + tx.usuarioId()
                    + " gastó $" + tx.monto() + " en " + tx.comercio())
            .to("alertas-fraude", Produced.with(Serdes.String(), Serdes.String()));

        // 2. OPERACIÓN STATEFUL + WINDOWING: Más de 3 compras en una Tumbling Window de 1 minuto
        stream.groupByKey(Grouped.with(Serdes.String(), transaccionSerde))
              .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
              .count(Materialized.as("conteo-transacciones-store")) // State Store en RocksDB
              .toStream()
              .filter((windowedKey, conteo) -> conteo > 3)
              .map((windowedKey, conteo) -> KeyValue.pair(
                    windowedKey.key(),
                    "ALERTA RÁFAGA: Usuario " + windowedKey.key()
                            + " realizó " + conteo + " transacciones en menos de 1 minuto"
              ))
              .to("alertas-fraude", Produced.with(Serdes.String(), Serdes.String()));

        return stream;
    }
}