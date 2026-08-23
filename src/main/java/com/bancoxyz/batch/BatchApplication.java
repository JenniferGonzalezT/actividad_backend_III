package com.bancoxyz.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class BatchApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext contexto = SpringApplication.run(BatchApplication.class, args);
        // Parte de la tolerancia a fallos (punto 4): sin esto un Job que termina en
        // FAILED igual devolvia codigo de salida 0, y cualquier planificador que
        // encadene los procesos lo daria por exitoso. SpringApplication.exit consulta
        // al JobExecutionExitCodeGenerator y propaga un codigo != 0 si el Job fallo.
        System.exit(SpringApplication.exit(contexto));
    }
}
