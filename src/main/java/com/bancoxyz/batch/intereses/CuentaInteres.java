package com.bancoxyz.batch.intereses;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Los campos *Texto vienen crudos del CSV. El resto los completa
 * InteresProcessor luego de validar y calcular el interes.
 */
public class CuentaInteres {

    private String cuentaIdTexto;
    private String nombre;
    private String saldoTexto;
    private String edadTexto;
    private String tipoTexto;

    private Long cuentaId;
    private BigDecimal saldoInicial;
    private String tipoCuenta;
    private BigDecimal tasaAplicada;
    private BigDecimal interesCalculado;
    private BigDecimal saldoFinal;
    private LocalDateTime fechaProcesado;

    public String getCuentaIdTexto() {
        return cuentaIdTexto;
    }

    public void setCuentaIdTexto(String cuentaIdTexto) {
        this.cuentaIdTexto = cuentaIdTexto;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public String getSaldoTexto() {
        return saldoTexto;
    }

    public void setSaldoTexto(String saldoTexto) {
        this.saldoTexto = saldoTexto;
    }

    public String getEdadTexto() {
        return edadTexto;
    }

    public void setEdadTexto(String edadTexto) {
        this.edadTexto = edadTexto;
    }

    public String getTipoTexto() {
        return tipoTexto;
    }

    public void setTipoTexto(String tipoTexto) {
        this.tipoTexto = tipoTexto;
    }

    public Long getCuentaId() {
        return cuentaId;
    }

    public void setCuentaId(Long cuentaId) {
        this.cuentaId = cuentaId;
    }

    public BigDecimal getSaldoInicial() {
        return saldoInicial;
    }

    public void setSaldoInicial(BigDecimal saldoInicial) {
        this.saldoInicial = saldoInicial;
    }

    public String getTipoCuenta() {
        return tipoCuenta;
    }

    public void setTipoCuenta(String tipoCuenta) {
        this.tipoCuenta = tipoCuenta;
    }

    public BigDecimal getTasaAplicada() {
        return tasaAplicada;
    }

    public void setTasaAplicada(BigDecimal tasaAplicada) {
        this.tasaAplicada = tasaAplicada;
    }

    public BigDecimal getInteresCalculado() {
        return interesCalculado;
    }

    public void setInteresCalculado(BigDecimal interesCalculado) {
        this.interesCalculado = interesCalculado;
    }

    public BigDecimal getSaldoFinal() {
        return saldoFinal;
    }

    public void setSaldoFinal(BigDecimal saldoFinal) {
        this.saldoFinal = saldoFinal;
    }

    public LocalDateTime getFechaProcesado() {
        return fechaProcesado;
    }

    public void setFechaProcesado(LocalDateTime fechaProcesado) {
        this.fechaProcesado = fechaProcesado;
    }

    @Override
    public String toString() {
        return "CuentaInteres{cuentaId=" + cuentaIdTexto + ", nombre=" + nombre
                + ", saldo=" + saldoTexto + ", tipo=" + tipoTexto + "}";
    }
}
