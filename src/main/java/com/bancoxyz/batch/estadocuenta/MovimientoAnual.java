package com.bancoxyz.batch.estadocuenta;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Los campos *Texto vienen crudos del CSV. El resto los completa
 * MovimientoAnualProcessor luego de validar y normalizar.
 */
public class MovimientoAnual {

    private String cuentaIdTexto;
    private String fechaTexto;
    private String transaccionTexto;
    private String montoTexto;
    private String descripcion;

    private Long cuentaId;
    private LocalDate fecha;
    private String tipoMovimiento;
    private BigDecimal monto;
    private LocalDateTime fechaProcesado;

    public String getCuentaIdTexto() {
        return cuentaIdTexto;
    }

    public void setCuentaIdTexto(String cuentaIdTexto) {
        this.cuentaIdTexto = cuentaIdTexto;
    }

    public String getFechaTexto() {
        return fechaTexto;
    }

    public void setFechaTexto(String fechaTexto) {
        this.fechaTexto = fechaTexto;
    }

    public String getTransaccionTexto() {
        return transaccionTexto;
    }

    public void setTransaccionTexto(String transaccionTexto) {
        this.transaccionTexto = transaccionTexto;
    }

    public String getMontoTexto() {
        return montoTexto;
    }

    public void setMontoTexto(String montoTexto) {
        this.montoTexto = montoTexto;
    }

    public String getDescripcion() {
        return descripcion;
    }

    public void setDescripcion(String descripcion) {
        this.descripcion = descripcion;
    }

    public Long getCuentaId() {
        return cuentaId;
    }

    public void setCuentaId(Long cuentaId) {
        this.cuentaId = cuentaId;
    }

    public LocalDate getFecha() {
        return fecha;
    }

    public void setFecha(LocalDate fecha) {
        this.fecha = fecha;
    }

    public String getTipoMovimiento() {
        return tipoMovimiento;
    }

    public void setTipoMovimiento(String tipoMovimiento) {
        this.tipoMovimiento = tipoMovimiento;
    }

    public BigDecimal getMonto() {
        return monto;
    }

    public void setMonto(BigDecimal monto) {
        this.monto = monto;
    }

    public LocalDateTime getFechaProcesado() {
        return fechaProcesado;
    }

    public void setFechaProcesado(LocalDateTime fechaProcesado) {
        this.fechaProcesado = fechaProcesado;
    }

    @Override
    public String toString() {
        return "MovimientoAnual{cuentaId=" + cuentaIdTexto + ", fecha=" + fechaTexto
                + ", transaccion=" + transaccionTexto + ", monto=" + montoTexto + "}";
    }
}
