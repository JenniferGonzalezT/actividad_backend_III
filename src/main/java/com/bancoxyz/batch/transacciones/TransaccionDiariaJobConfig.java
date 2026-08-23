package com.bancoxyz.batch.transacciones;

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
public class TransaccionDiariaJobConfig {

    private static final String JOB_NAME = "reporteTransaccionesDiariasJob";

    @Value("${batch.input.dir}/transacciones.csv")
    private String archivoTransacciones;

    @Value("${batch.chunk-size}")
    private int chunkSize;

    @Value("${batch.retry-limit}")
    private int limiteReintentos;

    @Bean
    public Job reporteTransaccionesDiariasJob(JobRepository jobRepository,
                                               Step truncarTransaccionesStep,
                                               Step cargarTransaccionesStep,
                                               Step generarResumenDiarioStep,
                                               JobMetricasListener jobMetricasListener) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(jobMetricasListener)
                .start(truncarTransaccionesStep)
                .next(cargarTransaccionesStep)
                .next(generarResumenDiarioStep)
                .build();
    }

    @Bean
    public Step truncarTransaccionesStep(JobRepository jobRepository,
                                          PlatformTransactionManager transactionManager,
                                          JdbcTemplate jdbcTemplate) {
        Tasklet tasklet = (contribution, chunkContext) -> {
            jdbcTemplate.update("DELETE FROM transaccion_diaria");
            jdbcTemplate.update("DELETE FROM resumen_diario");
            return RepeatStatus.FINISHED;
        };
        return new StepBuilder("truncarTransaccionesStep", jobRepository)
                .tasklet(tasklet, transactionManager)
                .build();
    }

    @Bean
    public Step cargarTransaccionesStep(JobRepository jobRepository,
                                         PlatformTransactionManager transactionManager,
                                         JdbcTemplate jdbcTemplate,
                                         SkipPolicy politicaSkipPersonalizada,
                                         BackOffPolicy backOffBatch,
                                         JobMetricasListener jobMetricasListener,
                                         @Qualifier("batchTaskExecutor") TaskExecutor batchTaskExecutor) {
        return new StepBuilder("cargarTransaccionesStep", jobRepository)
                .<Transaccion, Transaccion>chunk(chunkSize, transactionManager)
                .reader(transaccionItemReader())
                .processor(new TransaccionProcessor())
                .writer(transaccionItemWriter(jdbcTemplate.getDataSource()))
                // punto 4: politicas personalizadas de tolerancia a fallos
                .faultTolerant()
                .skipPolicy(politicaSkipPersonalizada)
                .retry(TransientDataAccessException.class)
                .retry(RecoverableDataAccessException.class)
                .retryLimit(limiteReintentos)
                .backOffPolicy(backOffBatch)
                .listener(new RegistroErrorListener<Transaccion>(jdbcTemplate, JOB_NAME))
                .listener(jobMetricasListener)
                // punto 5: procesamiento en paralelo con el pool de hilos configurado
                .taskExecutor(batchTaskExecutor)
                .build();
    }

    @Bean
    public Step generarResumenDiarioStep(JobRepository jobRepository,
                                          PlatformTransactionManager transactionManager,
                                          JdbcTemplate jdbcTemplate) {
        Tasklet tasklet = (contribution, chunkContext) -> {
            jdbcTemplate.update("""
                    INSERT INTO resumen_diario (fecha, total_debitos, total_creditos, cantidad_transacciones, cantidad_anomalias)
                    SELECT fecha,
                           COALESCE(SUM(CASE WHEN tipo = 'debito' THEN monto ELSE 0 END), 0),
                           COALESCE(SUM(CASE WHEN tipo = 'credito' THEN monto ELSE 0 END), 0),
                           COUNT(*),
                           COALESCE(SUM(CASE WHEN es_anomalia THEN 1 ELSE 0 END), 0)
                    FROM transaccion_diaria
                    GROUP BY fecha
                    """);
            return RepeatStatus.FINISHED;
        };
        return new StepBuilder("generarResumenDiarioStep", jobRepository)
                .tasklet(tasklet, transactionManager)
                .build();
    }

    /**
     * FlatFileItemReader no es thread-safe, asi que al procesar con varios hilos
     * (punto 5) hay que envolverlo en un SynchronizedItemStreamReader: sincroniza
     * el read() y evita que dos hilos se pisen sobre el mismo archivo.
     * saveState(false) es la contrapartida obligatoria: con lectura concurrente el
     * offset guardado no seria fiable, asi que se resigna el reinicio desde la
     * mitad del archivo (cada Job trunca sus tablas y se reejecuta completo).
     */
    private SynchronizedItemStreamReader<Transaccion> transaccionItemReader() {
        FlatFileItemReader<Transaccion> delegate = new FlatFileItemReaderBuilder<Transaccion>()
                .name("transaccionItemReader")
                .resource(new FileSystemResource(archivoTransacciones))
                .linesToSkip(1)
                .saveState(false)
                .delimited()
                .names("idTexto", "fechaTexto", "montoTexto", "tipo")
                .targetType(Transaccion.class)
                .build();

        SynchronizedItemStreamReader<Transaccion> reader = new SynchronizedItemStreamReader<>();
        reader.setDelegate(delegate);
        return reader;
    }

    private JdbcBatchItemWriter<Transaccion> transaccionItemWriter(DataSource dataSource) {
        JdbcBatchItemWriter<Transaccion> writer = new JdbcBatchItemWriterBuilder<Transaccion>()
                .dataSource(dataSource)
                .sql("""
                        INSERT INTO transaccion_diaria (id, fecha, monto, tipo, es_anomalia, motivo_anomalia, fecha_procesado)
                        VALUES (:id, :fecha, :monto, :tipo, :esAnomalia, :motivoAnomalia, :fechaProcesado)
                        """)
                .beanMapped()
                .build();
        // se construye "a mano" (no como @Bean), asi que hay que inicializarlo nosotros mismos
        try {
            writer.afterPropertiesSet();
        } catch (Exception e) {
            throw new IllegalStateException("No se pudo inicializar el writer de transaccion_diaria", e);
        }
        return writer;
    }
}
