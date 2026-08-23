package com.bancoxyz.batch.common;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * Los CSV legacy traen fechas en 4 formatos distintos mezclados.
 * Este util intenta parsear probando los 4 patrones en orden y devuelve
 * null si ninguno funciona (fecha irrecuperable).
 */
public class FechaUtil {

    private static final List<DateTimeFormatter> FORMATOS = List.of(
            DateTimeFormatter.ofPattern("yyyy-MM-dd"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd"),
            DateTimeFormatter.ofPattern("dd-MM-yyyy"),
            DateTimeFormatter.ofPattern("dd/MM/yyyy")
    );

    private FechaUtil() {
    }

    public static LocalDate parseFecha(String texto) {
        if (texto == null || texto.isBlank()) {
            return null;
        }
        String valor = texto.trim();
        for (DateTimeFormatter formato : FORMATOS) {
            try {
                return LocalDate.parse(valor, formato);
            } catch (DateTimeParseException e) {
                // intenta el siguiente formato
            }
        }
        return null;
    }
}
