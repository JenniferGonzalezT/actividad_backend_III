# Migración de procesos batch legacy del Banco XYZ

Proyecto de migración/modernización de 3 procesos batch legacy de un banco ficticio
(Banco XYZ) usando **Spring Batch 5** + **Spring Boot 3** + **PostgreSQL**. El sistema
legacy exportaba su información en archivos CSV con problemas típicos de datos sucios
(fechas en formatos distintos, montos vacíos o negativos, tipos de cuenta/transacción
inválidos, texto con tildes inconsistentes, filas duplicadas). Este proyecto recrea esos
3 procesos como Jobs de Spring Batch, validando y dejando trazabilidad de todo lo que no
se pudo procesar.

## Objetivo

Reemplazar 3 procesos batch legacy por Jobs de Spring Batch que:

1. **Reporte de transacciones diarias**: procesa `transacciones.csv`, detecta anomalías
   (montos <= 0, tipos inválidos) y genera un resumen diario.
2. **Cálculo de intereses mensuales**: procesa `intereses.csv`, calcula intereses sobre
   cuentas de ahorro/préstamo/hipoteca y guarda el saldo final.
3. **Generación de estados de cuenta anuales**: procesa `cuentas_anuales.csv`, compila el
   historial de movimientos por cuenta y genera un estado de cuenta para auditoría.

Los 3 procesos garantizan integridad de datos aplicando validación en el `ItemProcessor`
y políticas personalizadas de skip/retry, dejando registro de cada fila que no se pudo
procesar, y escalan procesando los chunks en paralelo con un pool de hilos.

## Dónde está resuelto cada requisito

| # | Requisito | Implementación |
|---|---|---|
| 1 | Proyecto Spring Batch con Jobs y Steps | `*JobConfig.java` de los 3 paquetes de negocio (`JobBuilder` / `StepBuilder`) |
| 2 | Leer CSV, procesar y escribir en BD relacional | `FlatFileItemReader` → `*Processor` → `JdbcBatchItemWriter` sobre PostgreSQL |
| 3 | Manejo de errores y datos mal clasificados | `*Processor` + `RegistroInvalidoException` + `RegistroErrorListener` + normalización de `tipo_movimiento` |
| 4 | Políticas personalizadas y tolerancia a fallos | `PoliticaSkipPersonalizada` + retry con `ExponentialBackOffPolicy` (`ToleranciaFallosConfig`) |
| 5 | Políticas de escalamiento (3 hilos, chunk 5) | `EscalamientoConfig` (pool de 3 hilos) + `.taskExecutor(...)` y `chunk(5)` en los 3 chunk steps |

## Estructura del código

```
src/main/java/com/bancoxyz/batch/
├── BatchApplication.java              punto de entrada Spring Boot
├── common/
│   ├── FechaUtil.java                 parsea fechas en los 4 formatos legacy
│   ├── RegistroInvalidoException.java excepcion que dispara el skip de una fila
│   ├── RegistroErrorListener.java     guarda en registro_error cada fila saltada
│   ├── PoliticaSkipPersonalizada.java  (punto 4) decide que se salta y que hace fallar
│   ├── ToleranciaFallosConfig.java     (punto 4) beans de skip policy y backoff
│   ├── EscalamientoConfig.java         (punto 5) pool de hilos para los chunk steps
│   └── JobMetricasListener.java        (puntos 4 y 5) hilos usados y contadores por step
├── transacciones/                     Job "reporteTransaccionesDiariasJob"
│   ├── Transaccion.java
│   ├── TransaccionProcessor.java
│   └── TransaccionDiariaJobConfig.java
├── intereses/                         Job "calculoInteresesMensualesJob"
│   ├── CuentaInteres.java
│   ├── InteresProcessor.java
│   └── InteresMensualJobConfig.java
└── estadocuenta/                      Job "generacionEstadosCuentaAnualesJob"
    ├── MovimientoAnual.java
    ├── MovimientoAnualProcessor.java
    └── EstadoCuentaAnualJobConfig.java
```

Cada Job sigue el mismo patrón de 3 pasos:

1. **Truncar** las tablas de resultado (para que cada corrida deje datos consistentes,
   sin ir acumulando duplicados de corridas anteriores). Secuencial.
2. **Cargar** (chunk step): `FlatFileItemReader` lee el CSV → `ItemProcessor` valida y
   normaliza → `JdbcBatchItemWriter` inserta en PostgreSQL. Es el paso que corre **en
   paralelo con 3 hilos y chunks de 5 filas**, y el que aplica las políticas de skip y retry.
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
  `RegistroInvalidoException`. El Step está configurado con `.faultTolerant()` y la
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

## Políticas personalizadas y tolerancia a fallos (punto 4)

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
sobre una base que todavía no se recuperó. Esto cobra relevancia real justamente ahora que
3 hilos escriben concurrentemente sobre las mismas tablas.

`RegistroInvalidoException` **no** se reintenta: es determinista y reintentarla sólo
gastaría trabajo.

### Código de salida

`BatchApplication` propaga el resultado del Job al código de salida del proceso
(`SpringApplication.exit`). Un Job en `FAILED` devuelve un código distinto de 0, de modo
que un planificador que encadene los procesos detecte la falla en vez de darla por exitosa.

## Políticas de escalamiento (punto 5)

Los 3 chunk steps procesan **en paralelo con 3 hilos y chunks de 5 filas**:

- `EscalamientoConfig` publica un `ThreadPoolTaskExecutor` con
  `corePoolSize = maxPoolSize = batch.thread-count` (3). El tamaño del pool es lo que acota
  la concurrencia: en Spring Batch 5.1 el `throttleLimit()` del `StepBuilder` quedó
  deprecado, así que fijar el pool es la forma correcta de garantizar exactamente N hilos.
- Cada chunk step usa `.chunk(batch.chunk-size, ...)` (5) y `.taskExecutor(batchTaskExecutor)`.
- Los hilos son **daemon**. Es imprescindible en una app batch de línea de comando: con
  hilos normales la JVM no terminaría nunca al acabar el Job. No hay riesgo de cortar
  trabajo a medias, porque el step bloquea hasta que todos sus chunks terminaron.

### Cómo se logró que sea seguro con varios hilos

- `FlatFileItemReader` **no es thread-safe**, así que va envuelto en un
  `SynchronizedItemStreamReader` que sincroniza el `read()` y evita que dos hilos se pisen
  sobre el mismo archivo.
- Los `ItemProcessor` son **stateless**, o sea seguros tal cual, sin cambios.
- `JdbcBatchItemWriter` y el `JdbcTemplate` del `RegistroErrorListener` ya son thread-safe.
- Los steps de **truncado y de agregación siguen siendo secuenciales**: son operaciones
  únicas de BD, paralelizarlas no aporta nada y rompería el orden truncar → cargar → agregar.

### Limitación conocida

Los readers usan `saveState(false)`, que es la contrapartida obligatoria de leer en
paralelo: con lectura concurrente el offset guardado no sería fiable. Se resigna el
reinicio desde la mitad del archivo, algo que no afecta a este proyecto porque cada Job
arranca truncando sus tablas y se reejecuta completo.

Por lo mismo, al arrancar aparecen estas advertencias, que son **esperables** y no indican
un problema:

```
Asynchronous TaskExecutor detected with ItemStream reader...
ItemStream was opened in a different thread. Restart data could be compromised.
No ItemReader set (must be concurrent step), so ignoring offset data.
```

## Parámetros configurables

Todos viven en `application.yml` y se pueden sobreescribir por línea de comando con
`--parametro=valor`:

| Parámetro | Default | Para qué sirve |
|---|---|---|
| `batch.input.dir` | `data/semana_3` | Carpeta de los CSV legacy |
| `batch.chunk-size` | `5` | Filas por chunk (commit cada 5 items) |
| `batch.thread-count` | `3` | Hilos de ejecución paralela por chunk step |
| `batch.skip-limit` | `300` | Filas descartables antes de abortar el step |
| `batch.retry-limit` | `3` | Reintentos ante un fallo transitorio de BD |
| `intereses.tasa-*` | ver tabla | Tasas mensuales por tipo de cuenta |

## Cómo ejecutar el proyecto

### 1. Levantar PostgreSQL

```bash
docker compose up -d
```

Esto levanta un PostgreSQL 16 en `localhost:5432` (db `bancoxyz_batch`, user/password
`bancoxyz`/`bancoxyz123`). Las tablas de negocio se crean solas al arrancar la app
(`schema-postgres.sql`); las tablas de metadata de Spring Batch (`BATCH_JOB_INSTANCE`,
etc.) las crea Spring Batch automáticamente.

### 2. Compilar

```bash
mvn clean package
```

### 3. Ejecutar cada Job

Cada uno de los 3 Jobs se corre por separado indicando su nombre:

```bash
java -jar target/batch-legacy-migration-1.0.0.jar --spring.batch.job.name=reporteTransaccionesDiariasJob
java -jar target/batch-legacy-migration-1.0.0.jar --spring.batch.job.name=calculoInteresesMensualesJob
java -jar target/batch-legacy-migration-1.0.0.jar --spring.batch.job.name=generacionEstadosCuentaAnualesJob
```

Por default los Jobs leen los CSV de `data/semana_3` (el dataset más grande y "sucio",
~1000 filas por archivo). Para probar con otra semana, se puede sobreescribir la ruta:

```bash
java -jar target/batch-legacy-migration-1.0.0.jar --spring.batch.job.name=reporteTransaccionesDiariasJob --batch.input.dir=data/semana_1
```

Los Jobs se pueden volver a correr las veces que sea necesario: cada ejecución arranca
truncando sus propias tablas de resultado y usa un `run.id` incremental automático (no
hace falta pasar parámetros extra para poder reejecutar).

### 4. Ver resultados

```sql
-- transacciones diarias y anomalías detectadas
SELECT * FROM transaccion_diaria LIMIT 20;
SELECT * FROM resumen_diario ORDER BY fecha;

-- intereses calculados
SELECT * FROM cuenta_interes LIMIT 20;

-- movimientos anuales y estado de cuenta por cuenta
SELECT * FROM movimiento_anual LIMIT 20;
SELECT * FROM estado_cuenta_anual ORDER BY cuenta_id;

-- filas descartadas por dato irrecuperable, y por qué
SELECT job_name, motivo, COUNT(*) FROM registro_error GROUP BY job_name, motivo ORDER BY 3 DESC;

-- desglose por tipo de movimiento (así se ve la clasificación del punto 3,
-- y si quedó algún 'desconocido' fuera del catálogo)
SELECT tipo_movimiento, COUNT(*), SUM(ABS(monto)) FROM movimiento_anual GROUP BY 1 ORDER BY 2 DESC;

-- integridad bajo concurrencia: escritas + saltadas debe dar 1000 (las filas del CSV)
SELECT (SELECT COUNT(*) FROM transaccion_diaria)
     + (SELECT COUNT(*) FROM registro_error WHERE job_name = 'reporteTransaccionesDiariasJob') AS total;
```

### 5. Evidenciar el escalamiento (punto 5)

Al terminar, cada Job imprime un resumen con los hilos que participaron y los contadores
por step:

```
===== Metricas de 'reporteTransaccionesDiariasJob' =====
Estado final: COMPLETED | duracion total: 466 ms
Step 'cargarTransaccionesStep': leidos=1000 escritos=785 saltados=215 ... duracion=424 ms
Hilos que procesaron chunks (3): [batch-worker-1, batch-worker-2, batch-worker-3]
```

Para comparar contra una ejecución secuencial basta cambiar un parámetro:

```bash
# 1 hilo
java -jar target/batch-legacy-migration-1.0.0.jar --spring.batch.job.name=reporteTransaccionesDiariasJob --batch.thread-count=1
# 3 hilos (default)
java -jar target/batch-legacy-migration-1.0.0.jar --spring.batch.job.name=reporteTransaccionesDiariasJob
```

Medición sobre `semana_3` (1000 filas, 3 corridas de cada uno, PostgreSQL local):

| Configuración | Duración del chunk step | Hilos |
|---|---|---|
| `--batch.thread-count=1` | 451 / 498 / 468 ms (~472 ms) | `batch-worker-1` |
| `--batch.thread-count=3` (default) | 426 / 412 / 413 ms (~417 ms) | `batch-worker-1/2/3` |

La mejora es moderada (~12%) y conviene ser honesto sobre por qué: con 1000 filas y chunks
de 5 el trabajo está dominado por los ~200 commits contra la base, que son el cuello de
botella real; el paralelismo rinde mucho más a medida que crece el volumen o el costo del
`ItemProcessor`. Lo importante es que **los contadores son idénticos en las dos
configuraciones** (1000 leídas, 785 escritas, 215 saltadas): el paralelismo cambia el orden
de inserción, no los resultados.

### 6. Evidenciar la tolerancia a fallos (punto 4)

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
