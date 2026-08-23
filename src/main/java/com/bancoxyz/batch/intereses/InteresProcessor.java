package com.bancoxyz.batch.intereses;

import com.bancoxyz.batch.common.RegistroInvalidoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;

/**
 * Valida cada fila de intereses.csv y calcula el interes mensual.
 * cuenta_id y saldo son obligatorios: si faltan o no son numeros, se salta la fila.
 * edad y tipo se manejan con tolerancia: si vienen raros, se registra una
 * advertencia (edad) o se aplica tasa 0 (tipo desconocido) pero no se descarta la cuenta.
 */
public class InteresProcessor implements ItemProcessor<CuentaInteres, CuentaInteres> {

    private static final Logger log = LoggerFactory.getLogger(InteresProcessor.class);

    private final BigDecimal tasaAhorro;
    private final BigDecimal tasaPrestamo;
    private final BigDecimal tasaHipoteca;

    public InteresProcessor(BigDecimal tasaAhorro, BigDecimal tasaPrestamo, BigDecimal tasaHipoteca) {
        this.tasaAhorro = tasaAhorro;
        this.tasaPrestamo = tasaPrestamo;
        this.tasaHipoteca = tasaHipoteca;
    }

    @Override
    public CuentaInteres process(CuentaInteres item) {
        item.setCuentaId(parseCuentaId(item.getCuentaIdTexto()));

        String nombre = item.getNombre();
        item.setNombre(nombre == null || nombre.isBlank() ? "Desconocido" : nombre.trim());

        String saldoTexto = item.getSaldoTexto();
        if (saldoTexto == null || saldoTexto.isBlank()) {
            throw new RegistroInvalidoException("Saldo vacio para cuenta_id=" + item.getCuentaIdTexto());
        }
        BigDecimal saldo;
        try {
            saldo = new BigDecimal(saldoTexto.trim());
        } catch (NumberFormatException e) {
            throw new RegistroInvalidoException("Saldo invalido: " + saldoTexto);
        }
        item.setSaldoInicial(saldo);

        validarEdad(item);

        String tipo = item.getTipoTexto() == null ? "" : item.getTipoTexto().trim().toLowerCase();
        BigDecimal tasa = tasaSegunTipo(tipo);
        if (tasa == null) {
            log.warn("Tipo de cuenta desconocido para cuenta_id={}: '{}', se aplica tasa 0", item.getCuentaId(), tipo);
            tasa = BigDecimal.ZERO;
        }
        item.setTipoCuenta(tipo);
        item.setTasaAplicada(tasa);

        BigDecimal interes = saldo.multiply(tasa).setScale(2, RoundingMode.HALF_UP);
        item.setInteresCalculado(interes);
        item.setSaldoFinal(saldo.add(interes));
        item.setFechaProcesado(LocalDateTime.now());

        return item;
    }

    private void validarEdad(CuentaInteres item) {
        String edadTexto = item.getEdadTexto();
        if (edadTexto == null || edadTexto.isBlank()) {
            log.warn("Edad vacia para cuenta_id={}", item.getCuentaId());
            return;
        }
        try {
            int edad = Integer.parseInt(edadTexto.trim());
            if (edad < 0 || edad > 120) {
                log.warn("Edad fuera de rango para cuenta_id={}: {}", item.getCuentaId(), edad);
            }
        } catch (NumberFormatException e) {
            log.warn("Edad invalida para cuenta_id={}: '{}'", item.getCuentaId(), edadTexto);
        }
    }

    private BigDecimal tasaSegunTipo(String tipo) {
        return switch (tipo) {
            case "ahorro" -> tasaAhorro;
            case "prestamo" -> tasaPrestamo;
            case "hipoteca" -> tasaHipoteca;
            default -> null;
        };
    }

    private Long parseCuentaId(String cuentaIdTexto) {
        try {
            return Long.parseLong(cuentaIdTexto.trim());
        } catch (Exception e) {
            throw new RegistroInvalidoException("cuenta_id invalido: " + cuentaIdTexto);
        }
    }
}
