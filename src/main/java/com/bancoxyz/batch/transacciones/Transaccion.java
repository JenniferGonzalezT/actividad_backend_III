package com.bancoxyz.batch.transacciones;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Los campos *Texto son los que vienen crudos del CSV (el reader los llena).
 * El resto los completa el TransaccionProcessor luego de validar y normalizar.
 */
public class Transaccion {

    private String idTexto;
    private String fechaTexto;
    private String montoTexto;
    private String tipo;

    private Long id;
    private LocalDate fecha;
    private BigDecimal monto;
    private boolean esAnomalia;
    private String motivoAnomalia;
    private LocalDateTime fechaProcesado;

    public String getIdTexto() {
        return idTexto;
    }

    public void setIdTexto(String idTexto) {
        this.idTexto = idTexto;
    }

    public String getFechaTexto() {
        return fechaTexto;
    }

    public void setFechaTexto(String fechaTexto) {
        this.fechaTexto = fechaTexto;
    }

    public String getMontoTexto() {
        return montoTexto;
    }

    public void setMontoTexto(String montoTexto) {
        this.montoTexto = montoTexto;
    }

    public String getTipo() {
        return tipo;
    }

    public void setTipo(String tipo) {
        this.tipo = tipo;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public LocalDate getFecha() {
        return fecha;
    }

    public void setFecha(LocalDate fecha) {
        this.fecha = fecha;
    }

    public BigDecimal getMonto() {
        return monto;
    }

    public void setMonto(BigDecimal monto) {
        this.monto = monto;
    }

    public boolean isEsAnomalia() {
        return esAnomalia;
    }

    public void setEsAnomalia(boolean esAnomalia) {
        this.esAnomalia = esAnomalia;
    }

    public String getMotivoAnomalia() {
        return motivoAnomalia;
    }

    public void setMotivoAnomalia(String motivoAnomalia) {
        this.motivoAnomalia = motivoAnomalia;
    }

    public LocalDateTime getFechaProcesado() {
        return fechaProcesado;
    }

    public void setFechaProcesado(LocalDateTime fechaProcesado) {
        this.fechaProcesado = fechaProcesado;
    }

    @Override
    public String toString() {
        return "Transaccion{id=" + idTexto + ", fecha=" + fechaTexto + ", monto=" + montoTexto + ", tipo=" + tipo + "}";
    }
}
