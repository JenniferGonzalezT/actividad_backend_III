package com.bancoxyz.batch.common;

import org.springframework.batch.core.SkipListener;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;

/**
 * Cada vez que Spring Batch salta una fila (por RegistroInvalidoException u otro
 * error), este listener la deja escrita en la tabla registro_error para poder
 * auditar despues que se salto y por que.
 */
public class RegistroErrorListener<T> implements SkipListener<T, T> {

    private final JdbcTemplate jdbcTemplate;
    private final String jobName;

    public RegistroErrorListener(JdbcTemplate jdbcTemplate, String jobName) {
        this.jdbcTemplate = jdbcTemplate;
        this.jobName = jobName;
    }

    @Override
    public void onSkipInRead(Throwable t) {
        guardarError("(error leyendo la linea)", "Error de lectura: " + t.getMessage());
    }

    @Override
    public void onSkipInProcess(T item, Throwable t) {
        guardarError(String.valueOf(item), t.getMessage());
    }

    @Override
    public void onSkipInWrite(T item, Throwable t) {
        guardarError(String.valueOf(item), "Error de escritura: " + t.getMessage());
    }

    private void guardarError(String lineaOriginal, String motivo) {
        jdbcTemplate.update(
                "INSERT INTO registro_error (job_name, linea_original, motivo, fecha_error) VALUES (?, ?, ?, ?)",
                jobName, lineaOriginal, motivo, LocalDateTime.now());
    }
}
