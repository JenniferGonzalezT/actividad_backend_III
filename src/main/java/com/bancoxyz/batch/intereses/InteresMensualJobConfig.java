package com.bancoxyz.batch.intereses;

import com.bancoxyz.batch.common.ArchivoPartitioner;
import com.bancoxyz.batch.common.JobMetricasListener;
import com.bancoxyz.batch.common.RegistroErrorListener;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
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

    @Value("${batch.grid-size}")
    private int gridSize;

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
    public Step calcularInteresesWorkerStep(JobRepository jobRepository,
                                            PlatformTransactionManager transactionManager,
                                            JdbcTemplate jdbcTemplate,
                                            SkipPolicy politicaSkipPersonalizada,
                                            BackOffPolicy backOffBatch,
                                            JobMetricasListener jobMetricasListener) {
        return new StepBuilder("calcularInteresesStep", jobRepository)
                .<CuentaInteres, CuentaInteres>chunk(chunkSize, transactionManager)
                .reader(cuentaInteresItemReader(null, null))
                .processor(new InteresProcessor(tasaAhorro, tasaPrestamo, tasaHipoteca))
                .writer(cuentaInteresItemWriter(jdbcTemplate.getDataSource()))
                // politicas personalizadas de tolerancia a fallos
                .faultTolerant()
                .skipPolicy(politicaSkipPersonalizada)
                .retry(TransientDataAccessException.class)
                .retry(RecoverableDataAccessException.class)
                .retryLimit(limiteReintentos)
                .backOffPolicy(backOffBatch)
                .listener(new RegistroErrorListener<CuentaInteres>(jdbcTemplate, JOB_NAME))
                .listener(jobMetricasListener)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<CuentaInteres> cuentaInteresItemReader(
            @Value("#{stepExecutionContext['linesToSkip']}") Integer linesToSkip,
            @Value("#{stepExecutionContext['itemCount']}") Integer itemCount) {

        return new FlatFileItemReaderBuilder<CuentaInteres>()
                .name("cuentaInteresItemReader")
                .resource(new FileSystemResource(archivoIntereses))
                .linesToSkip(linesToSkip != null ? linesToSkip : 1)
                .maxItemCount(itemCount != null ? itemCount : 1000)
                .delimited()
                .names("cuentaIdTexto", "nombre", "saldoTexto", "edadTexto", "tipoTexto")
                .targetType(CuentaInteres.class)
                .build();
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

    @Bean
    public Partitioner interesesPartitioner() {
        return new ArchivoPartitioner(1000); 
    }

    @Bean
    public TaskExecutorPartitionHandler interesesPartitionHandler(
            @Qualifier("calcularInteresesWorkerStep") Step workerStep,
            @Qualifier("batchTaskExecutor") TaskExecutor taskExecutor) {
        
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setGridSize(gridSize); // Número de particiones
        handler.setTaskExecutor(taskExecutor);
        handler.setStep(workerStep);
        return handler;
    }

    @Bean
    public Step calcularInteresesStep(JobRepository jobRepository,
                                      Partitioner interesesPartitioner,
                                      TaskExecutorPartitionHandler interesesPartitionHandler) {
        return new StepBuilder("calcularInteresesStep", jobRepository)
                .partitioner("calcularInteresesWorkerStep", interesesPartitioner)
                .partitionHandler(interesesPartitionHandler)
                .build();
    }
}
