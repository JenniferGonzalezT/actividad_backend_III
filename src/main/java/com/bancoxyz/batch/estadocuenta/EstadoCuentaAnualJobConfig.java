package com.bancoxyz.batch.estadocuenta;

import com.bancoxyz.batch.common.JobMetricasListener;
import com.bancoxyz.batch.common.RegistroErrorListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class EstadoCuentaAnualJobConfig {

    private static final String JOB_NAME = "generacionEstadosCuentaAnualesJob";

    @Value("${batch.input.dir}/cuentas_anuales.csv")
    private String archivoCuentasAnuales;

    @Value("${batch.chunk-size}")
    private int chunkSize;

    @Value("${batch.retry-limit}")
    private int limiteReintentos;

    @Bean
    public Job generacionEstadosCuentaAnualesJob(JobRepository jobRepository,
                                                  Step truncarMovimientosStep,
                                                  Step cargarMovimientosStep,
                                                  Step generarEstadoCuentaStep,
                                                  JobMetricasListener jobMetricasListener) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(jobMetricasListener)
                .start(truncarMovimientosStep)
                .next(cargarMovimientosStep)
                .next(generarEstadoCuentaStep)
                .build();
    }

    @Bean
    public Step truncarMovimientosStep(JobRepository jobRepository,
                                        PlatformTransactionManager transactionManager,
                                        JdbcTemplate jdbcTemplate) {
        Tasklet tasklet = (contribution, chunkContext) -> {
            jdbcTemplate.update("DELETE FROM movimiento_anual");
            jdbcTemplate.update("DELETE FROM estado_cuenta_anual");
            return RepeatStatus.FINISHED;
        };
        return new StepBuilder("truncarMovimientosStep", jobRepository)
                .tasklet(tasklet, transactionManager)
                .build();
    }

    @Bean
    public Step cargarMovimientosStep(JobRepository jobRepository,
                                       PlatformTransactionManager transactionManager,
                                       JdbcTemplate jdbcTemplate,
                                       SkipPolicy politicaSkipPersonalizada,
                                       BackOffPolicy backOffBatch,
                                       JobMetricasListener jobMetricasListener,
                                       @Qualifier("batchTaskExecutor") TaskExecutor batchTaskExecutor) {
        return new StepBuilder("cargarMovimientosStep", jobRepository)
                .<MovimientoAnual, MovimientoAnual>chunk(chunkSize, transactionManager)
                .reader(movimientoAnualItemReader())
                .processor(new MovimientoAnualProcessor())
                .writer(movimientoAnualItemWriter(jdbcTemplate.getDataSource()))
                // punto 4: politicas personalizadas de tolerancia a fallos
                .faultTolerant()
                .skipPolicy(politicaSkipPersonalizada)
                .retry(TransientDataAccessException.class)
                .retry(RecoverableDataAccessException.class)
                .retryLimit(limiteReintentos)
                .backOffPolicy(backOffBatch)
                .listener(new RegistroErrorListener<MovimientoAnual>(jdbcTemplate, JOB_NAME))
                .listener(jobMetricasListener)
                // punto 5: procesamiento en paralelo con el pool de hilos configurado
                .taskExecutor(batchTaskExecutor)
                .build();
    }

    @Bean
    public Step generarEstadoCuentaStep(JobRepository jobRepository,
                                         PlatformTransactionManager transactionManager,
                                         JdbcTemplate jdbcTemplate) {
        Tasklet tasklet = (contribution, chunkContext) -> {
            // Se clasifica por tipo_movimiento y NO por el signo del monto: el sistema
            // legacy exporta los retiros/compras/pagos con monto positivo, asi que
            // mirar el signo los contaba a todos como depositos y dejaba total_retiros
            // en 0. ABS(monto) hace la suma inmune a ese signo inconsistente.
            // Los movimientos que quedaron en 'desconocido' se suman como egreso
            // (criterio conservador) y quedan visibles agrupando por tipo_movimiento.
            jdbcTemplate.update("""
                    INSERT INTO estado_cuenta_anual (cuenta_id, saldo_inicial, total_depositos, total_retiros, saldo_final, cantidad_movimientos, fecha_generado)
                    SELECT cuenta_id,
                           0,
                           COALESCE(SUM(CASE WHEN tipo_movimiento = 'deposito' THEN ABS(monto) ELSE 0 END), 0),
                           COALESCE(SUM(CASE WHEN tipo_movimiento <> 'deposito' THEN ABS(monto) ELSE 0 END), 0),
                           COALESCE(SUM(CASE WHEN tipo_movimiento = 'deposito' THEN ABS(monto) ELSE -ABS(monto) END), 0),
                           COUNT(*),
                           now()
                    FROM movimiento_anual
                    GROUP BY cuenta_id
                    """);
            return RepeatStatus.FINISHED;
        };
        return new StepBuilder("generarEstadoCuentaStep", jobRepository)
                .tasklet(tasklet, transactionManager)
                .build();
    }

    /** Ver la nota de thread-safety en TransaccionDiariaJobConfig.transaccionItemReader(). */
    private SynchronizedItemStreamReader<MovimientoAnual> movimientoAnualItemReader() {
        FlatFileItemReader<MovimientoAnual> delegate = new FlatFileItemReaderBuilder<MovimientoAnual>()
                .name("movimientoAnualItemReader")
                .resource(new FileSystemResource(archivoCuentasAnuales))
                .linesToSkip(1)
                .saveState(false)
                .delimited()
                .names("cuentaIdTexto", "fechaTexto", "transaccionTexto", "montoTexto", "descripcion")
                .targetType(MovimientoAnual.class)
                .build();

        SynchronizedItemStreamReader<MovimientoAnual> reader = new SynchronizedItemStreamReader<>();
        reader.setDelegate(delegate);
        return reader;
    }

    private JdbcBatchItemWriter<MovimientoAnual> movimientoAnualItemWriter(DataSource dataSource) {
        JdbcBatchItemWriter<MovimientoAnual> writer = new JdbcBatchItemWriterBuilder<MovimientoAnual>()
                .dataSource(dataSource)
                .sql("""
                        INSERT INTO movimiento_anual (cuenta_id, fecha, tipo_movimiento, monto, descripcion, fecha_procesado)
                        VALUES (:cuentaId, :fecha, :tipoMovimiento, :monto, :descripcion, :fechaProcesado)
                        """)
                .beanMapped()
                .build();
        // se construye "a mano" (no como @Bean), asi que hay que inicializarlo nosotros mismos
        try {
            writer.afterPropertiesSet();
        } catch (Exception e) {
            throw new IllegalStateException("No se pudo inicializar el writer de movimiento_anual", e);
        }
        return writer;
    }
}
