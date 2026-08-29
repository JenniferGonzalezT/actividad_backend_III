# Sistema Batch - Banco XYZ (Migración Legacy)

Proyecto de modernización de procesos batch legacy utilizando Spring Batch, implementando escalamiento horizontal mediante **Particiones** y procesamiento paralelo.

## Arquitectura (Semana 3)
El sistema divide los archivos CSV de entrada en bloques independientes (particiones) para procesarlos de forma concurrente, optimizando los tiempos de ejecución y garantizando la resiliencia ante fallos. Se han configurado tres procesos clave:
1. `reporteTransaccionesDiariasJob`
2. `calculoInteresesMensualesJob`
3. `generacionEstadosCuentaAnualesJob`


## Requisitos
* Java 17
* Maven
* Docker (para la base de datos PostgreSQL)


## Instrucciones de Ejecución

### 1. Levantar la base de datos:
```bash
docker compose up -d
```
Esto levanta un PostgreSQL 16 en `localhost:5432` (db `bancoxyz_batch`, user/password
`bancoxyz`/`bancoxyz123`). Las tablas de negocio se crean solas al arrancar la app
(`schema-postgres.sql`); las tablas de metadata de Spring Batch (`BATCH_JOB_INSTANCE`,
etc.) las crea Spring Batch automáticamente.

### 2. Compilar el proyecto:
```bash
mvn clean package
```

### 3. Ejecutar cada Job

Para garantizar el aislamiento y observar correctamente el rendimiento de las particiones, ejecuta cada Job de forma individual por línea de comandos:

```bash
java -jar target/batch-legacy-migration-1.0.0.jar --spring.batch.job.name=reporteTransaccionesDiariasJob

java -jar target/batch-legacy-migration-1.0.0.jar --spring.batch.job.name=calculoInteresesMensualesJob

java -jar target/batch-legacy-migration-1.0.0.jar --spring.batch.job.name=generacionEstadosCuentaAnualesJob
```

Por default los Jobs leen los CSV de `data/semana_3`. Para probar con otra semana, se puede sobreescribir la ruta:

```bash
java -jar target/batch-legacy-migration-1.0.0.jar --spring.batch.job.name=reporteTransaccionesDiariasJob --batch.input.dir=data/semana_1
```

Los Jobs se pueden volver a correr las veces que sea necesario,no
hace falta pasar parámetros extras para poder reejecutar.

### 4. Ver resultados

```sql
-- transacciones diarias y anomalías detectadas
SELECT * FROM transaccion_diaria LIMIT 10;
SELECT * FROM resumen_diario ORDER BY fecha LIMIT 10;

-- intereses calculados
SELECT * FROM cuenta_interes LIMIT 20;

-- movimientos anuales y estado de cuenta por cuenta
SELECT * FROM movimiento_anual LIMIT 10;
SELECT * FROM estado_cuenta_anual ORDER BY cuenta_id LIMIT 10;

-- filas descartadas por dato irrecuperable, y por qué
SELECT job_name, motivo, COUNT(*) FROM registro_error GROUP BY job_name, motivo ORDER BY 3 DESC LIMIT 20;

-- desglose por tipo de movimiento (así se ve la clasificación del punto 3,
-- y si quedó algún 'desconocido' fuera del catálogo)
SELECT tipo_movimiento, COUNT(*), SUM(ABS(monto)) FROM movimiento_anual GROUP BY 1 ORDER BY 2 DESC;

-- integridad bajo concurrencia: escritas + saltadas debe dar 1000 (las filas del CSV)
-- Nota: Limpiar la tabla registro_error si se han realizado ejecuciones previas para no sumar el histórico.
SELECT (SELECT COUNT(*) FROM transaccion_diaria)
     + (SELECT COUNT(*) FROM registro_error WHERE job_name = 'reporteTransaccionesDiariasJob') AS total;
```


## Configuración de Escalamiento
Los parámetros de optimización pueden ajustarse directamente en el archivo application.yml:
* `batch.chunk-size`: Define el número de registros procesados por transacción (Commit).

* `batch.grid-size`: Define la cantidad de particiones lógicas en las que se divide el archivo.

* `batch.thread-count`: Define el tamaño del pool de hilos (corePoolSize) asignado para ejecutar las particiones en paralelo.


## Estructura del código
```
src/main/java/com/bancoxyz/batch/
├── BatchApplication.java                punto de entrada Spring Boot
├── common/
│   ├── ArchivoPartitioner.java          divide los datos estableciendo rangos lógicos
│   ├── EscalamientoConfig.java          pool de hilos para los particionadores
│   ├── FechaUtil.java                   parsea fechas en los 4 formatos legacy
│   ├── JobMetricasListener.java         hilos usados y contadores por step
│   ├── PoliticaSkipPersonalizada.java   decide que se salta y que hace fallar
│   ├── RegistroErrorListener.java       guarda en registro_error cada fila saltada
│   ├── RegistroInvalidoException.java   excepcion que dispara el skip de una fila
│   └── ToleranciaFallosConfig.java      beans de skip policy y backoff
├── transacciones/                       Job "reporteTransaccionesDiariasJob"
│   ├── Transaccion.java
│   ├── TransaccionProcessor.java
│   └── TransaccionDiariaJobConfig.java
├── intereses/                           Job "calculoInteresesMensualesJob"
│   ├── CuentaInteres.java
│   ├── InteresProcessor.java
│   └── InteresMensualJobConfig.java
└── estadocuenta/                        Job "generacionEstadosCuentaAnualesJob"
    ├── MovimientoAnual.java
    ├── MovimientoAnualProcessor.java
    └── EstadoCuentaAnualJobConfig.java
```

Cada Job sigue el mismo patrón de 3 pasos:

1. **Truncar** las tablas de resultado (para que cada corrida deje datos consistentes,
   sin ir acumulando duplicados de corridas anteriores). Secuencial.
2. **Cargar** (Partition & Worker Steps): El `PartitionStep` (Manager) utiliza el `ArchivoPartitioner` para dividir el CSV en rangos. Luego, inyecta el `ExecutionContext` en los `Worker Steps` paralelos. El `FlatFileItemReader` (con `@StepScope`) lee su porción asignada, el `ItemProcessor` valida y el `JdbcBatchItemWriter` inserta en PostgreSQL. Aquí actúan las políticas de skip y retry aisladas por partición.
3. **Agregar** (tasklet): calcula el resumen/reporte final a partir de la tabla cargada
   en el paso anterior. Secuencial.

(El Job de intereses no necesita el paso 3: el interés se calcula fila por fila en el
`ItemProcessor`, no hay una agregación posterior.)

## Reglas de manejo de errores

Cada `ItemProcessor` distingue dos tipos de problemas en los datos:

- **Anomalías "recuperables"** (monto <= 0, tipo de transacción/cuenta desconocido, edad
  fuera de rango, descripción vacía, tildes inconsistentes): se corrigen o se marcan, pero
  la fila **se guarda igual** para no perder trazabilidad de auditoría.
- **Datos irrecuperables** (fecha imposible de interpretar en ninguno de los 4 formatos,
  `id`/`cuenta_id`/`monto`/`saldo` vacíos o no numéricos): el `ItemProcessor` lanza
  `RegistroInvalidoException`. El Worker Step está configurado con `.faultTolerant()` y la
  `PoliticaSkipPersonalizada`, así que Spring Batch salta esa fila (no frena el Job
  completo) y `RegistroErrorListener` la deja escrita en la tabla `registro_error` con el
  motivo, para poder auditar después qué se descartó y por qué.

Las fechas llegan mezcladas en 4 formatos (`yyyy-MM-dd`, `yyyy/MM/dd`, `dd-MM-yyyy`,
`dd/MM/yyyy`); `FechaUtil` prueba los 4 patrones en orden y devuelve `null` si ninguno
aplica (fecha invalida, ej. `2024-13-01`).

### Clasificación de movimientos (dato mal clasificado)

El caso más sutil de "dato mal clasificado" está en `cuentas_anuales.csv`: el sistema
legacy exporta **todos** los montos en positivo, incluidos retiros, compras y pagos.
Por eso el estado de cuenta anual clasifica por `tipo_movimiento` y **no por el signo del
monto**, y suma con `ABS(monto)` para ser inmune a ese signo inconsistente.

`MovimientoAnualProcessor` normaliza el tipo (quita tildes y pasa a minúscula, unificando
`depósito` con `deposito`) y lo valida contra el catálogo canónico
`deposito | retiro | compra | pago`. Un tipo fuera del catálogo es una anomalía
recuperable: se marca `desconocido` con una advertencia en el log, pero la fila **no se
descarta** y su monto sigue siendo trazable en `movimiento_anual`. En el estado de cuenta,
todo lo que no es `deposito` cuenta como egreso (criterio conservador).

Sobre el dataset `semana_3` la diferencia es grande — clasificar por signo daba
`total_retiros = 80.900` cuando el valor correcto es `1.023.500`.


## Políticas personalizadas y tolerancia a fallos

El `.skip(X).skipLimit(N)` que trae Spring Batch de fábrica sólo sabe contar: cuenta
cualquier excepción declarada y falla al pasarse del número. `PoliticaSkipPersonalizada`
(un `SkipPolicy` propio) decide en función de **qué** falló, no sólo de cuántas veces:

| Situación | Decisión | Por qué |
|---|---|---|
| `RegistroInvalidoException` (dato irrecuperable del CSV) | **Se salta**, hasta el límite | Es lo esperable de un archivo legacy; una fila mala no debe frenar las otras 999 |
| `FlatFileParseException` (fila con columnas de más/menos) | **Se salta**, hasta el límite | Mismo caso: problema de la fila, no del proceso |
| Cualquier fallo de BD (`DataAccessException`, `SQLException`) | **Nunca se salta** → el step falla | Saltarlo descartaría filas *buenas* y terminaría el Job en `COMPLETED` con datos incompletos, que es peor que fallar |
| Cualquier otra excepción | **Nunca se salta** → el step falla | Evita que un bug quede escondido detrás de un "skip" |
| Se alcanza el límite de skips | `SkipLimitExceededException` | La entrada está por debajo de lo tolerable |

Detalles que importan:

- **Un fallo de infraestructura gana sobre la causa de dato.** La política recorre toda la
  cadena de causas: si en cualquier punto hay un error de BD, no se salta aunque el
  envoltorio sea una excepción "de dato".
- **Aviso temprano**: al superar el 80% del límite deja un `WARN` en el log, una sola vez.
- **El límite es real**: `batch.skip-limit` vale 300, por debajo de las 1000 filas del
  dataset y por encima del peor caso medido (215 filas saltadas en `transacciones.csv`).
  El valor anterior (2000) era mayor que el archivo completo, así que nunca podía
  dispararse.
- **Thread-safe**: la decisión del límite usa el `skipCount` que entrega el framework (el
  contador autoritativo del `StepExecution`), porque el step corre con varios hilos.

### Retry con backoff

Skip y retry cubren clases de fallo **distintas** y por eso conviven:

- `skip` → el fallo es del **dato** y es determinista: reintentar daría siempre el mismo
  error, así que la fila se descarta y se audita.
- `retry` → el fallo es del **entorno** y es transitorio (deadlock, conexión perdida un
  instante), así que reintentar tiene sentido.

Se reintenta `TransientDataAccessException` (que cubre deadlocks y timeouts de lock) y
`RecoverableDataAccessException`, hasta `batch.retry-limit` veces, con
`ExponentialBackOffPolicy` (0,5s → 1s → 2s… con tope de 5s) para no insistir de inmediato
sobre una base que todavía no se recuperó. Esto es vital bajo concurrencia, ya que múltiples particiones escriben sobre las mismas tablas. `RegistroInvalidoException` **no** se reintenta, pues es un error determinista de dato.


### Código de salida

`BatchApplication` propaga el resultado del Job al código de salida del proceso
(`SpringApplication.exit`). Un Job en `FAILED` devuelve un código distinto de 0, de modo
que un planificador que encadene los procesos detecte la falla en vez de darla por exitosa.


## Parámetros configurables

Todos viven en `application.yml` y se pueden sobreescribir por línea de comando con
`--parametro=valor`:

| Parámetro | Default | Para qué sirve |
|---|---|---|
| `batch.input.dir` | `data/semana_3` | Carpeta de los CSV legacy |
| `batch.chunk-size` | `50` | Filas por chunk (commit a base de datos) |
| `batch.grid-size` | `6` | Cantidad de particiones del archivo |
| `batch.thread-count` | `6` | Tamaño del pool de hilos paralelos |
| `batch.skip-limit` | `300` | Filas descartables antes de abortar el step |
| `batch.retry-limit` | `3` | Reintentos ante un fallo transitorio de BD |
| `intereses.tasa-*` | ver tabla | Tasas mensuales por tipo de cuenta |


### Evidenciar el escalamiento

Al terminar, cada Job imprime un resumen con los hilos que participaron y los contadores
por step:

```
===== Metricas de 'reporteTransaccionesDiariasJob' =====
Estado final: COMPLETED | duracion total: 466 ms
Step 'cargarTransaccionesStep': leidos=1000 escritos=785 saltados=215 ... duracion=424 ms
Hilos que procesaron chunks (3): [batch-worker-1, batch-worker-2, batch-worker-3]
```

### Comparativa de escalamiento
Al evaluar el rendimiento del proceso de transacciones (`semana_3` - 1000 filas) bajo distintas configuraciones en un entorno PostgreSQL local, se obtuvieron los siguientes resultados:

| Escenario | Configuración | Duración Total | Hilos / Particiones Activas |
|---|---|---|---|
| A (Base) | Chunks: 5, Particiones: 3, Hilos: 3 | ~692 ms | `batch-worker-1/2/3` (3 particiones) |
| B (Mejora I/O) | Chunks: 50, Particiones: 3, Hilos: 3 | ~600 ms | `batch-worker-1/2/3` (3 particiones) |
| C (Óptimo) | Chunks: 50, Particiones: 6, Hilos: 6 | ~508 ms | `batch-worker-1/2/3/4/5/6` (6 particiones) |

**Análisis:**
La configuración óptima (Escenario C) logra una mejora drástica en el rendimiento. Al aumentar el tamaño del chunk a 50, se reducen significativamente las transacciones (commits) en la base de datos, eliminando el cuello de botella de escritura (I/O). Al configurar el `grid-size` y `thread-count` a 6, el particionador distribuye la carga dividiendo el archivo en porciones más pequeñas (~167 registros por partición) procesadas en paralelo simultáneo. Durante todo el proceso, los contadores mantienen la exactitud absoluta (1000 leídas, 785 escritas, 215 saltadas), demostrando que la arquitectura particionada garantiza integridad de datos aislando las fallas de cada hilo.


### Evidenciar la tolerancia a fallos

Bajando el límite de skips por debajo de la cantidad de filas sucias, la política aborta el
Job en vez de terminar con datos incompletos:

```bash
java -jar target/batch-legacy-migration-1.0.0.jar --spring.batch.job.name=reporteTransaccionesDiariasJob --batch.skip-limit=10
```

```
ERROR ... PoliticaSkipPersonalizada : Se alcanzo el limite de 10 filas saltadas...
org.springframework.batch.core.step.skip.SkipLimitExceededException: Skip limit of '10' exceeded
... status: [FAILED]
```

Y el proceso devuelve un código de salida distinto de 0 (`echo $?` → `5`), mientras que una
corrida normal devuelve `0`.


## Tasas de interés aplicadas (ejemplo)

Configurables en `application.yml` (`intereses.tasa-*`):

| Tipo de cuenta | Tasa mensual |
|---|---|
| ahorro | 0.5% |
| préstamo | 1.2% |
| hipoteca | 0.8% |

Si el tipo de cuenta no es ninguno de los anteriores (dato legacy sucio, ej. `-1` o
`unknown`), se aplica tasa 0 y queda un log de advertencia (no se descarta la cuenta).


## Tests

```bash
mvn test
```

25 tests unitarios (JUnit 5, sin necesidad de levantar la base):

- `TransaccionProcessorTest` — parseo de los 4 formatos de fecha, marcado de anomalías
  (monto <= 0, tipo inválido) y descarte de filas irrecuperables (fecha inválida, monto vacío).
- `MovimientoAnualProcessorTest` — normalización de `depósito`/`deposito`, catálogo de tipos
  válidos, tipo fuera del catálogo marcado como `desconocido` sin descartar la fila,
  descripción vacía y descarte de `cuenta_id`/fecha/monto inválidos.
- `PoliticaSkipPersonalizadaTest` — qué se salta y qué no: datos irrecuperables sí,
  infraestructura no (ni siquiera envuelta en una excepción de dato), excepciones
  inesperadas no, y aborto al alcanzar el límite.
