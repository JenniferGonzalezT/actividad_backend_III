package com.bancoxyz.batch.common;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PoliticaSkipPersonalizadaTest {

    private final PoliticaSkipPersonalizada politica = new PoliticaSkipPersonalizada(100);

    @Test
    void saltaUnRegistroInvalidoMientrasNoSePaseDelLimite() {
        assertTrue(politica.shouldSkip(new RegistroInvalidoException("monto vacio"), 0));
        assertTrue(politica.shouldSkip(new RegistroInvalidoException("fecha invalida"), 99));
    }

    @Test
    void saltaUnaFilaMalFormadaDelCsv() {
        FlatFileParseException error = new FlatFileParseException("columnas de menos", "1,2024-01-01");

        assertTrue(politica.shouldSkip(error, 10));
    }

    @Test
    void saltaAunqueLaCausaVengaEnvuelta() {
        Exception envuelta = new IllegalStateException("fallo el processor",
                new RegistroInvalidoException("saldo invalido"));

        assertTrue(politica.shouldSkip(envuelta, 5));
    }

    @Test
    void alLlegarAlLimiteAbortaElStep() {
        assertThrows(SkipLimitExceededException.class,
                () -> politica.shouldSkip(new RegistroInvalidoException("monto vacio"), 100));
    }

    @Test
    void nuncaSaltaUnFalloDeInfraestructura() {
        assertFalse(politica.shouldSkip(new CannotGetJdbcConnectionException("base caida"), 0));
        assertFalse(politica.shouldSkip(new CannotAcquireLockException("deadlock"), 0));
        assertFalse(politica.shouldSkip(new SQLException("error sql"), 0));
    }

    @Test
    void unFalloDeInfraestructuraGanaSobreLaCausaDeDato() {
        // el envoltorio es una excepcion "de dato" (saltable), pero la causa real es
        // la base caida: hay que fallar igual, porque saltar la fila esconderia una
        // perdida de datos detras de un Job en COMPLETED
        FlatFileParseException mixta = new FlatFileParseException("no se pudo mapear",
                new DataAccessResourceFailureException("base caida"), "1,2024-01-01", 7);

        assertFalse(politica.shouldSkip(mixta, 0));
    }

    @Test
    void nuncaSaltaUnaExcepcionInesperada() {
        assertFalse(politica.shouldSkip(new NullPointerException("bug"), 0));
        assertFalse(politica.shouldSkip(new OutOfMemoryError("sin memoria"), 0));
    }

    @Test
    void unLimiteEnCeroDescartaCualquierTolerancia() {
        PoliticaSkipPersonalizada estricta = new PoliticaSkipPersonalizada(0);

        assertThrows(SkipLimitExceededException.class,
                () -> estricta.shouldSkip(new RegistroInvalidoException("monto vacio"), 0));
    }

    @Test
    void rechazaUnLimiteNegativo() {
        assertThrows(IllegalArgumentException.class, () -> new PoliticaSkipPersonalizada(-1));
    }
}
