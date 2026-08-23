package com.bancoxyz.batch.intereses;

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
import java.math.BigDecimal;

@Configuration
public class InteresMensualJobConfig {

    private static final String JOB_NAME = "calculoInteresesMensualesJob";

    @Value("${batch.input.dir}/intereses.csv")
    private String archivoIntereses;

    @Value("${intereses.tasa-ahorro}")
    private BigDecimal tasaAhorro;

    @Value("${intereses.tasa-prestamo}")
    private BigDecimal tasaPrestamo;

    @Value("${intereses.tasa-hipoteca}")
    private BigDecimal tasaHipoteca;

    @Value("${batch.chunk-size}")
    private int chunkSize;

    @Value("${batch.retry-limit}")
    private int limiteReintentos;

    @Bean
    public Job calculoInteresesMensualesJob(JobRepository jobRepository,
                                             Step truncarCuentaInteresStep,
                                             Step calcularInteresesStep,
                                             JobMetricasListener jobMetricasListener) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(jobMetricasListener)
                .start(truncarCuentaInteresStep)
                .next(calcularInteresesStep)
                .build();
    }

    @Bean
    public Step truncarCuentaInteresStep(JobRepository jobRepository,
                                          PlatformTransactionManager transactionManager,
                                          JdbcTemplate jdbcTemplate) {
        Tasklet tasklet = (contribution, chunkContext) -> {
            jdbcTemplate.update("DELETE FROM cuenta_interes");
            return RepeatStatus.FINISHED;
        };
        return new StepBuilder("truncarCuentaInteresStep", jobRepository)
                .tasklet(tasklet, transactionManager)
                .build();
    }

    @Bean
    public Step calcularInteresesStep(JobRepository jobRepository,
                                       PlatformTransactionManager transactionManager,
                                       JdbcTemplate jdbcTemplate,
                                       SkipPolicy politicaSkipPersonalizada,
                                       BackOffPolicy backOffBatch,
                                       JobMetricasListener jobMetricasListener,
                                       @Qualifier("batchTaskExecutor") TaskExecutor batchTaskExecutor) {
        return new StepBuilder("calcularInteresesStep", jobRepository)
                .<CuentaInteres, CuentaInteres>chunk(chunkSize, transactionManager)
                .reader(cuentaInteresItemReader())
                .processor(new InteresProcessor(tasaAhorro, tasaPrestamo, tasaHipoteca))
                .writer(cuentaInteresItemWriter(jdbcTemplate.getDataSource()))
                // punto 4: politicas personalizadas de tolerancia a fallos
                .faultTolerant()
                .skipPolicy(politicaSkipPersonalizada)
                .retry(TransientDataAccessException.class)
                .retry(RecoverableDataAccessException.class)
                .retryLimit(limiteReintentos)
                .backOffPolicy(backOffBatch)
                .listener(new RegistroErrorListener<CuentaInteres>(jdbcTemplate, JOB_NAME))
                .listener(jobMetricasListener)
                // punto 5: procesamiento en paralelo con el pool de hilos configurado
                .taskExecutor(batchTaskExecutor)
                .build();
    }

    /** Ver la nota de thread-safety en TransaccionDiariaJobConfig.transaccionItemReader(). */
    private SynchronizedItemStreamReader<CuentaInteres> cuentaInteresItemReader() {
        FlatFileItemReader<CuentaInteres> delegate = new FlatFileItemReaderBuilder<CuentaInteres>()
                .name("cuentaInteresItemReader")
                .resource(new FileSystemResource(archivoIntereses))
                .linesToSkip(1)
                .saveState(false)
                .delimited()
                .names("cuentaIdTexto", "nombre", "saldoTexto", "edadTexto", "tipoTexto")
                .targetType(CuentaInteres.class)
                .build();

        SynchronizedItemStreamReader<CuentaInteres> reader = new SynchronizedItemStreamReader<>();
        reader.setDelegate(delegate);
        return reader;
    }

    private JdbcBatchItemWriter<CuentaInteres> cuentaInteresItemWriter(DataSource dataSource) {
        JdbcBatchItemWriter<CuentaInteres> writer = new JdbcBatchItemWriterBuilder<CuentaInteres>()
                .dataSource(dataSource)
                .sql("""
                        INSERT INTO cuenta_interes (cuenta_id, nombre, saldo_inicial, tipo_cuenta, tasa_aplicada, interes_calculado, saldo_final, fecha_procesado)
                        VALUES (:cuentaId, :nombre, :saldoInicial, :tipoCuenta, :tasaAplicada, :interesCalculado, :saldoFinal, :fechaProcesado)
                        """)
                .beanMapped()
                .build();
        // se construye "a mano" (no como @Bean), asi que hay que inicializarlo nosotros mismos
        try {
            writer.afterPropertiesSet();
        } catch (Exception e) {
            throw new IllegalStateException("No se pudo inicializar el writer de cuenta_interes", e);
        }
        return writer;
    }
}
