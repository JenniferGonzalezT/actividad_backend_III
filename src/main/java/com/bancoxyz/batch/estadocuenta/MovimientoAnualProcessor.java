package com.bancoxyz.batch.estadocuenta;

import com.bancoxyz.batch.common.FechaUtil;
import com.bancoxyz.batch.common.RegistroInvalidoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;
import java.text.Normalizer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Set;

/**
 * Valida y normaliza cada fila de cuentas_anuales.csv.
 * cuenta_id, fecha y monto son obligatorios: si no se pueden interpretar,
 * la fila se salta. La descripcion vacia y las tildes inconsistentes en el
 * tipo de movimiento (deposito / depósito) se corrigen sin descartar la fila.
 * El tipo de movimiento ademas se valida contra un catalogo canonico, porque el
 * estado de cuenta anual clasifica ingresos y egresos por ese campo.
 */
public class MovimientoAnualProcessor implements ItemProcessor<MovimientoAnual, MovimientoAnual> {

    private static final Logger log = LoggerFactory.getLogger(MovimientoAnualProcessor.class);

    /** Unicos tipos que el legacy deberia emitir; cualquier otro cae en "desconocido". */
    private static final Set<String> TIPOS_VALIDOS = Set.of("deposito", "retiro", "compra", "pago");

    @Override
    public MovimientoAnual process(MovimientoAnual item) {
        item.setCuentaId(parseCuentaId(item.getCuentaIdTexto()));

        LocalDate fecha = FechaUtil.parseFecha(item.getFechaTexto());
        if (fecha == null) {
            throw new RegistroInvalidoException("Fecha invalida: " + item.getFechaTexto());
        }
        item.setFecha(fecha);

        String montoTexto = item.getMontoTexto();
        if (montoTexto == null || montoTexto.isBlank()) {
            throw new RegistroInvalidoException("Monto vacio para cuenta_id=" + item.getCuentaIdTexto());
        }
        BigDecimal monto;
        try {
            monto = new BigDecimal(montoTexto.trim());
        } catch (NumberFormatException e) {
            throw new RegistroInvalidoException("Monto invalido: " + montoTexto);
        }
        item.setMonto(monto);

        item.setTipoMovimiento(normalizarTipoMovimiento(item.getTransaccionTexto(), item.getCuentaId()));

        String descripcion = item.getDescripcion();
        item.setDescripcion(descripcion == null || descripcion.isBlank() ? "Sin descripcion" : descripcion.trim());

        item.setFechaProcesado(LocalDateTime.now());

        return item;
    }

    /**
     * Saca tildes y pasa a minuscula para unificar "depósito" y "deposito", y luego
     * verifica contra el catalogo canonico. Un tipo fuera del catalogo es una anomalia
     * recuperable: se marca "desconocido" y se deja el aviso en el log, pero la fila
     * no se descarta (el monto sigue siendo trazable en movimiento_anual).
     */
    private String normalizarTipoMovimiento(String texto, Long cuentaId) {
        if (texto == null) {
            log.warn("Tipo de movimiento vacio para cuenta_id={}, se marca como desconocido", cuentaId);
            return "desconocido";
        }
        String sinTildes = Normalizer.normalize(texto.trim().toLowerCase(), Normalizer.Form.NFD)
                .replaceAll("\\p{M}", "");
        if (!TIPOS_VALIDOS.contains(sinTildes)) {
            log.warn("Tipo de movimiento fuera del catalogo para cuenta_id={}: '{}', se marca como desconocido",
                    cuentaId, sinTildes);
            return "desconocido";
        }
        return sinTildes;
    }

    private Long parseCuentaId(String cuentaIdTexto) {
        try {
            return Long.parseLong(cuentaIdTexto.trim());
        } catch (Exception e) {
            throw new RegistroInvalidoException("cuenta_id invalido: " + cuentaIdTexto);
        }
    }
}
