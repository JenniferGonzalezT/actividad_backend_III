package com.bancoxyz.batch.common;

import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;

/**
 * Politicas de tolerancia a fallos compartidas por los 3 Jobs (punto 4).
 *
 * Skip y retry cubren clases de fallo distintas y por eso conviven:
 * - skip  -> el fallo es del DATO y es determinista (reintentar daria siempre el
 *            mismo error), asi que la fila se descarta y se audita.
 * - retry -> el fallo es del ENTORNO y es transitorio (deadlock, conexion caida un
 *            instante), asi que reintentar tiene sentido. Cobra relevancia real
 *            ahora que 3 hilos escriben concurrentemente sobre las mismas tablas.
 */
@Configuration
public class ToleranciaFallosConfig {

    @Bean
    public SkipPolicy politicaSkipPersonalizada(@Value("${batch.skip-limit}") int limiteSkips) {
        return new PoliticaSkipPersonalizada(limiteSkips);
    }

    /**
     * Espera creciente entre reintentos (0,5s -> 1s -> 2s ... tope 5s) para no
     * insistir de inmediato sobre una base que todavia no se recupero.
     */
    @Bean
    public BackOffPolicy backOffBatch() {
        ExponentialBackOffPolicy backOff = new ExponentialBackOffPolicy();
        backOff.setInitialInterval(500);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(5000);
        return backOff;
    }
}
