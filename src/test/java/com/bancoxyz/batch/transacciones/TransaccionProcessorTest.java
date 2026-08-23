package com.bancoxyz.batch.transacciones;

import com.bancoxyz.batch.common.RegistroInvalidoException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransaccionProcessorTest {

    private final TransaccionProcessor processor = new TransaccionProcessor();

    private Transaccion transaccion(String id, String fecha, String monto, String tipo) {
        Transaccion t = new Transaccion();
        t.setIdTexto(id);
        t.setFechaTexto(fecha);
        t.setMontoTexto(monto);
        t.setTipo(tipo);
        return t;
    }

    @Test
    void procesaUnaFilaValidaSinAnomalias() {
        Transaccion resultado = processor.process(transaccion("1", "2024-01-01", "1000", "debito"));

        assertEquals(1L, resultado.getId());
        assertEquals(new BigDecimal("1000"), resultado.getMonto());
        assertFalse(resultado.isEsAnomalia());
    }

    @Test
    void aceptaLosCuatroFormatosDeFecha() {
        assertEquals("2024-01-05", processor.process(transaccion("1", "2024-01-05", "100", "debito")).getFecha().toString());
        assertEquals("2024-01-05", processor.process(transaccion("2", "2024/01/05", "100", "debito")).getFecha().toString());
        assertEquals("2024-01-05", processor.process(transaccion("3", "05-01-2024", "100", "debito")).getFecha().toString());
        assertEquals("2024-01-05", processor.process(transaccion("4", "05/01/2024", "100", "debito")).getFecha().toString());
    }

    @Test
    void montoNegativoOCeroSeMarcaComoAnomaliaPeroNoSeDescarta() {
        Transaccion resultado = processor.process(transaccion("1", "2024-01-01", "-200", "debito"));

        assertTrue(resultado.isEsAnomalia());
        assertTrue(resultado.getMotivoAnomalia().contains("menor o igual a cero"));
    }

    @Test
    void tipoInvalidoSeMarcaComoAnomaliaPeroNoSeDescarta() {
        Transaccion resultado = processor.process(transaccion("1", "2024-01-01", "500", "invalid"));

        assertTrue(resultado.isEsAnomalia());
        assertTrue(resultado.getMotivoAnomalia().contains("tipo de transaccion invalido"));
    }

    @Test
    void fechaInvalidaSeDescartaConExcepcion() {
        assertThrows(RegistroInvalidoException.class,
                () -> processor.process(transaccion("1", "2024-13-01", "500", "debito")));
    }

    @Test
    void montoVacioSeDescartaConExcepcion() {
        assertThrows(RegistroInvalidoException.class,
                () -> processor.process(transaccion("1", "2024-01-01", "", "debito")));
    }
}
