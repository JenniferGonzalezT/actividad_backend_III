package com.bancoxyz.batch.estadocuenta;

import com.bancoxyz.batch.common.RegistroInvalidoException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MovimientoAnualProcessorTest {

    private final MovimientoAnualProcessor processor = new MovimientoAnualProcessor();

    private MovimientoAnual movimiento(String cuentaId, String fecha, String transaccion,
                                       String monto, String descripcion) {
        MovimientoAnual m = new MovimientoAnual();
        m.setCuentaIdTexto(cuentaId);
        m.setFechaTexto(fecha);
        m.setTransaccionTexto(transaccion);
        m.setMontoTexto(monto);
        m.setDescripcion(descripcion);
        return m;
    }

    @Test
    void procesaUnaFilaValida() {
        MovimientoAnual resultado = processor.process(
                movimiento("110", "2024-07-24", "deposito", "1500", "sueldo"));

        assertEquals(110L, resultado.getCuentaId());
        assertEquals("2024-07-24", resultado.getFecha().toString());
        assertEquals("deposito", resultado.getTipoMovimiento());
        assertEquals(new BigDecimal("1500"), resultado.getMonto());
        assertEquals("sueldo", resultado.getDescripcion());
    }

    @Test
    void unificaElTipoConTildeConElTipoSinTilde() {
        assertEquals("deposito", processor.process(
                movimiento("1", "2024-01-01", "depósito", "100", "x")).getTipoMovimiento());
        assertEquals("deposito", processor.process(
                movimiento("1", "2024-01-01", "  DEPOSITO ", "100", "x")).getTipoMovimiento());
    }

    @Test
    void aceptaTodoElCatalogoDeTiposValidos() {
        for (String tipo : new String[]{"deposito", "retiro", "compra", "pago"}) {
            assertEquals(tipo, processor.process(
                    movimiento("1", "2024-01-01", tipo, "100", "x")).getTipoMovimiento());
        }
    }

    @Test
    void unTipoFueraDelCatalogoSeMarcaDesconocidoPeroNoSeDescarta() {
        MovimientoAnual resultado = processor.process(
                movimiento("1", "2024-01-01", "transferencia_rara", "100", "x"));

        assertEquals("desconocido", resultado.getTipoMovimiento());
        assertEquals(new BigDecimal("100"), resultado.getMonto());
    }

    @Test
    void unTipoVacioSeMarcaDesconocido() {
        assertEquals("desconocido", processor.process(
                movimiento("1", "2024-01-01", "", "100", "x")).getTipoMovimiento());
        assertEquals("desconocido", processor.process(
                movimiento("1", "2024-01-01", null, "100", "x")).getTipoMovimiento());
    }

    @Test
    void laDescripcionVaciaSeRellena() {
        assertEquals("Sin descripcion", processor.process(
                movimiento("1", "2024-01-01", "retiro", "100", "")).getDescripcion());
        assertEquals("Sin descripcion", processor.process(
                movimiento("1", "2024-01-01", "retiro", "100", null)).getDescripcion());
    }

    @Test
    void aceptaLosCuatroFormatosDeFecha() {
        assertEquals("2024-01-05", processor.process(
                movimiento("1", "2024-01-05", "retiro", "100", "x")).getFecha().toString());
        assertEquals("2024-01-05", processor.process(
                movimiento("1", "2024/01/05", "retiro", "100", "x")).getFecha().toString());
        assertEquals("2024-01-05", processor.process(
                movimiento("1", "05-01-2024", "retiro", "100", "x")).getFecha().toString());
        assertEquals("2024-01-05", processor.process(
                movimiento("1", "05/01/2024", "retiro", "100", "x")).getFecha().toString());
    }

    @Test
    void fechaInvalidaSeDescartaConExcepcion() {
        assertThrows(RegistroInvalidoException.class, () -> processor.process(
                movimiento("1", "2024-13-01", "retiro", "100", "x")));
    }

    @Test
    void montoVacioSeDescartaConExcepcion() {
        assertThrows(RegistroInvalidoException.class, () -> processor.process(
                movimiento("1", "2024-01-01", "retiro", "", "x")));
    }

    @Test
    void cuentaIdInvalidoSeDescartaConExcepcion() {
        assertThrows(RegistroInvalidoException.class, () -> processor.process(
                movimiento("abc", "2024-01-01", "retiro", "100", "x")));
    }
}
