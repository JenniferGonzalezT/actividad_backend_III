package com.bancoxyz.batch.transacciones;

import com.bancoxyz.batch.common.FechaUtil;
import com.bancoxyz.batch.common.RegistroInvalidoException;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Valida y normaliza cada fila de transacciones.csv.
 * Si el dato es irrecuperable (id/fecha/monto que no se pueden interpretar)
 * se lanza RegistroInvalidoException y Spring Batch salta la fila.
 * Si el dato es "raro" pero se puede procesar igual (monto <= 0, tipo
 * desconocido) se deja pasar marcado como anomalia para no perder trazabilidad.
 */
public class TransaccionProcessor implements ItemProcessor<Transaccion, Transaccion> {

    @Override
    public Transaccion process(Transaccion item) {
        item.setId(parseId(item.getIdTexto()));

        LocalDate fecha = FechaUtil.parseFecha(item.getFechaTexto());
        if (fecha == null) {
            throw new RegistroInvalidoException("Fecha invalida: " + item.getFechaTexto());
        }
        item.setFecha(fecha);

        String montoTexto = item.getMontoTexto();
        if (montoTexto == null || montoTexto.isBlank()) {
            throw new RegistroInvalidoException("Monto vacio para id=" + item.getIdTexto());
        }
        BigDecimal monto;
        try {
            monto = new BigDecimal(montoTexto.trim());
        } catch (NumberFormatException e) {
            throw new RegistroInvalidoException("Monto invalido: " + montoTexto);
        }
        item.setMonto(monto);

        List<String> motivos = new ArrayList<>();

        if (monto.compareTo(BigDecimal.ZERO) <= 0) {
            motivos.add("monto menor o igual a cero");
        }

        String tipo = item.getTipo() == null ? "" : item.getTipo().trim().toLowerCase();
        if (!tipo.equals("debito") && !tipo.equals("credito")) {
            motivos.add("tipo de transaccion invalido: '" + tipo + "'");
        }
        item.setTipo(tipo);

        item.setEsAnomalia(!motivos.isEmpty());
        item.setMotivoAnomalia(motivos.isEmpty() ? null : String.join("; ", motivos));
        item.setFechaProcesado(LocalDateTime.now());

        return item;
    }

    private Long parseId(String idTexto) {
        try {
            return Long.parseLong(idTexto.trim());
        } catch (Exception e) {
            throw new RegistroInvalidoException("Id invalido: " + idTexto);
        }
    }
}
