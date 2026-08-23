package com.bancoxyz.batch.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.dao.DataAccessException;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Politica de skip personalizada (punto 4).
 *
 * Reemplaza al .skip(X).skipLimit(N) que trae Spring Batch de fabrica, porque ese
 * solo sabe contar: cuenta cualquier excepcion declarada y falla al pasarse. Aca
 * la decision depende de QUE fallo, no solo de cuantas veces:
 *
 * - Problema de calidad del dato (RegistroInvalidoException del ItemProcessor,
 *   FlatFileParseException de una fila mal formada): se salta, hasta el limite.
 *   Es lo esperable de un CSV legacy y no debe frenar el proceso completo.
 * - Problema de infraestructura (base caida, conexion perdida, error SQL): NO se
 *   salta NUNCA. Saltarlo significaria descartar filas buenas y terminar el Job
 *   en COMPLETED con datos incompletos, que es peor que fallar. Fail fast.
 * - Cualquier otra excepcion inesperada: tampoco se salta, para no esconder bugs.
 *
 * Ademas avisa por log al llegar al 80% del limite, como alerta temprana de que
 * la calidad de la entrada esta peor de lo tolerable.
 *
 * Es thread-safe (el step corre con varios hilos): la decision del limite se toma
 * con el skipCount que entrega el framework, que es el contador autoritativo del
 * StepExecution; el unico estado propio es un flag atomico para no repetir el aviso.
 */
public class PoliticaSkipPersonalizada implements SkipPolicy {

    private static final Logger log = LoggerFactory.getLogger(PoliticaSkipPersonalizada.class);

    /** Tope de profundidad al recorrer las causas, por si alguna excepcion viene encadenada en ciclo. */
    private static final int MAX_PROFUNDIDAD_CAUSAS = 10;

    private final int limiteSkips;
    private final long umbralAviso;
    private final AtomicBoolean avisoEmitido = new AtomicBoolean(false);

    public PoliticaSkipPersonalizada(int limiteSkips) {
        if (limiteSkips < 0) {
            throw new IllegalArgumentException("El limite de skips no puede ser negativo: " + limiteSkips);
        }
        this.limiteSkips = limiteSkips;
        this.umbralAviso = (long) (limiteSkips * 0.8);
    }

    @Override
    public boolean shouldSkip(Throwable t, long skipCount) throws SkipLimitExceededException {
        Throwable falloInfra = buscarFalloDeInfraestructura(t);
        if (falloInfra != null) {
            log.error("Fallo de infraestructura ({}), no se salta la fila y el step debe fallar: {}",
                    falloInfra.getClass().getSimpleName(), falloInfra.getMessage());
            return false;
        }

        if (!esProblemaDeDato(t)) {
            log.error("Excepcion inesperada ({}), no esta contemplada como saltable: {}",
                    t.getClass().getSimpleName(), t.getMessage());
            return false;
        }

        if (skipCount >= limiteSkips) {
            log.error("Se alcanzo el limite de {} filas saltadas. La calidad de la entrada esta por debajo "
                    + "de lo tolerable, se aborta el step.", limiteSkips);
            throw new SkipLimitExceededException(limiteSkips, t);
        }

        if (skipCount >= umbralAviso && avisoEmitido.compareAndSet(false, true)) {
            log.warn("Ya se saltaron {} filas, mas del 80% del limite permitido ({}). "
                    + "Revisar la calidad del archivo de entrada.", skipCount, limiteSkips);
        }

        return true;
    }

    /** Devuelve la excepcion de infraestructura encontrada en la cadena de causas, o null si no hay. */
    private Throwable buscarFalloDeInfraestructura(Throwable t) {
        Throwable causa = t;
        for (int i = 0; causa != null && i < MAX_PROFUNDIDAD_CAUSAS; i++, causa = causa.getCause()) {
            if (causa instanceof DataAccessException || causa instanceof SQLException) {
                return causa;
            }
        }
        return null;
    }

    private boolean esProblemaDeDato(Throwable t) {
        Throwable causa = t;
        for (int i = 0; causa != null && i < MAX_PROFUNDIDAD_CAUSAS; i++, causa = causa.getCause()) {
            if (causa instanceof RegistroInvalidoException || causa instanceof FlatFileParseException) {
                return true;
            }
        }
        return false;
    }

    public int getLimiteSkips() {
        return limiteSkips;
    }
}
