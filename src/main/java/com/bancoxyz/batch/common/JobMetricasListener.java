package com.bancoxyz.batch.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Deja en el log la evidencia medible de los puntos 4 y 5:
 *
 * - como ChunkListener anota el nombre del hilo que ejecuta cada chunk, asi se
 *   comprueba que efectivamente trabajaron los N hilos configurados.
 * - como JobExecutionListener imprime al terminar un resumen con la duracion y
 *   los contadores de cada step (leidos, escritos, saltados, rollbacks), que es
 *   lo que permite comparar una corrida con --batch.thread-count=1 contra otra
 *   con --batch.thread-count=3.
 *
 * Se registra como bean unico y compartido entre el Job y el chunk step, por eso
 * el set de hilos es concurrente.
 */
public class JobMetricasListener implements JobExecutionListener, ChunkListener {

    private static final Logger log = LoggerFactory.getLogger(JobMetricasListener.class);

    private final Set<String> hilosUtilizados = new ConcurrentSkipListSet<>();

    @Override
    public void beforeChunk(ChunkContext context) {
        hilosUtilizados.add(Thread.currentThread().getName());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        log.info("===== Metricas de '{}' =====", jobExecution.getJobInstance().getJobName());
        log.info("Estado final: {} | duracion total: {} ms",
                jobExecution.getStatus(), duracionEnMs(jobExecution.getStartTime(), jobExecution.getEndTime()));

        for (StepExecution step : jobExecution.getStepExecutions()) {
            log.info("Step '{}': leidos={} escritos={} saltados={} (lectura={}, proceso={}, escritura={}) "
                            + "commits={} rollbacks={} duracion={} ms",
                    step.getStepName(),
                    step.getReadCount(),
                    step.getWriteCount(),
                    step.getSkipCount(),
                    step.getReadSkipCount(),
                    step.getProcessSkipCount(),
                    step.getWriteSkipCount(),
                    step.getCommitCount(),
                    step.getRollbackCount(),
                    duracionEnMs(step.getStartTime(), step.getEndTime()));
        }

        log.info("Hilos que procesaron chunks ({}): {}", hilosUtilizados.size(), hilosUtilizados);
        log.info("=========================================");
    }

    private long duracionEnMs(LocalDateTime inicio, LocalDateTime fin) {
        if (inicio == null || fin == null) {
            return -1;
        }
        return Duration.between(inicio, fin).toMillis();
    }
}
