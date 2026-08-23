-- tablas de negocio del proyecto (las tablas BATCH_* las crea Spring Batch solo)

CREATE TABLE IF NOT EXISTS transaccion_diaria (
    id BIGINT,
    fecha DATE,
    monto NUMERIC,
    tipo VARCHAR(20),
    es_anomalia BOOLEAN,
    motivo_anomalia VARCHAR(200),
    fecha_procesado TIMESTAMP
);

CREATE TABLE IF NOT EXISTS resumen_diario (
    fecha DATE,
    total_debitos NUMERIC,
    total_creditos NUMERIC,
    cantidad_transacciones INT,
    cantidad_anomalias INT
);

CREATE TABLE IF NOT EXISTS cuenta_interes (
    cuenta_id BIGINT,
    nombre VARCHAR(100),
    saldo_inicial NUMERIC,
    tipo_cuenta VARCHAR(20),
    tasa_aplicada NUMERIC,
    interes_calculado NUMERIC,
    saldo_final NUMERIC,
    fecha_procesado TIMESTAMP
);

CREATE TABLE IF NOT EXISTS movimiento_anual (
    cuenta_id BIGINT,
    fecha DATE,
    tipo_movimiento VARCHAR(20),
    monto NUMERIC,
    descripcion VARCHAR(200),
    fecha_procesado TIMESTAMP
);

CREATE TABLE IF NOT EXISTS estado_cuenta_anual (
    cuenta_id BIGINT,
    saldo_inicial NUMERIC,
    total_depositos NUMERIC,
    total_retiros NUMERIC,
    saldo_final NUMERIC,
    cantidad_movimientos INT,
    fecha_generado TIMESTAMP
);

CREATE TABLE IF NOT EXISTS registro_error (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(60),
    linea_original VARCHAR(500),
    motivo VARCHAR(200),
    fecha_error TIMESTAMP
);
