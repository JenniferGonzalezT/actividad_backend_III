package com.bancoxyz.batch.common;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Politicas de escalamiento (punto 5).
 *
 * Los chunk steps de los 3 Jobs procesan en paralelo usando este pool. El tamano
 * del pool es lo que acota la concurrencia real: en Spring Batch 5.1 el
 * throttleLimit() del StepBuilder quedo deprecado, asi que fijar
 * corePoolSize == maxPoolSize == batch.thread-count es la forma correcta de
 * garantizar exactamente N hilos trabajando a la vez.
 *
 * Nota: declarar un bean propio de tipo Executor desactiva el
 * "applicationTaskExecutor" que autoconfigura Spring Boot
 * (TaskExecutionAutoConfiguration es @ConditionalOnMissingBean(Executor.class)).
 * Aca es inocuo porque el proyecto no levanta un servidor web, pero por eso el
 * bean tiene nombre explicito y se inyecta siempre con @Qualifier.
 */
@Configuration
public class EscalamientoConfig {

    @Bean("batchTaskExecutor")
    public TaskExecutor batchTaskExecutor(@Value("${batch.thread-count}") int hilos) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(hilos);
        executor.setMaxPoolSize(hilos);
        executor.setQueueCapacity(hilos * 4);
        executor.setThreadNamePrefix("batch-worker-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        // daemon = true es imprescindible en una app batch de linea de comando: si los
        // hilos del pool fueran "user threads" la JVM no terminaria nunca al acabar el
        // Job (los hilos vivos impiden el shutdown, y el shutdown hook que cerraria el
        // contexto y apagaria el pool solo corre cuando la JVM empieza a bajar).
        // No hay riesgo de cortar trabajo a medias: el step bloquea hasta que todos sus
        // chunks terminaron, asi que main() recien retorna con el Job ya finalizado.
        executor.setDaemon(true);
        executor.initialize();
        return executor;
    }

    /**
     * Se comparte entre el Job (para el resumen final) y el chunk step (para
     * registrar que hilos participaron), por eso es un bean y no un new.
     */
    @Bean
    public JobMetricasListener jobMetricasListener() {
        return new JobMetricasListener();
    }
}
