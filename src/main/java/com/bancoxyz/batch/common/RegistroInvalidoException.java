package com.bancoxyz.batch.common;

/**
 * Se lanza cuando una fila del CSV legacy tiene un dato irrecuperable
 * (fecha imposible de parsear, campo numerico vacio o invalido, etc).
 * Spring Batch la usa como skip exception: la fila se salta y queda
 * registrada en la tabla registro_error via RegistroErrorListener.
 */
public class RegistroInvalidoException extends RuntimeException {

    public RegistroInvalidoException(String mensaje) {
        super(mensaje);
    }
}
