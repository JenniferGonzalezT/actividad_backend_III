package com.bancoxyz.batch.common;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

public class ArchivoPartitioner implements Partitioner {

    private int totalRegistros;

    public ArchivoPartitioner(int totalRegistros) {
        this.totalRegistros = totalRegistros;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> result = new HashMap<>();
        
        // Calculamos cuántos registros irán en cada partición
        int targetSize = (totalRegistros / gridSize) + 1;
        
        int number = 0;
        int start = 1; // Empezamos en la línea 1 (ignorando la cabecera luego en el reader)
        int end = start + targetSize - 1;

        while (start <= totalRegistros) {
            ExecutionContext value = new ExecutionContext();
            result.put("partition" + number, value);

            if (end >= totalRegistros) {
                end = totalRegistros;
            }
            
            // Guardamos el rango en el contexto para que el Reader sepa qué leer
            value.putInt("itemCount", (end - start) + 1);
            
            // Si es la primera partición, saltamos solo 1 línea (la cabecera del CSV)
            // Para las siguientes, saltamos los registros anteriores más la cabecera
            if (number == 0) {
                value.putInt("linesToSkip", 1); 
            } else {
                value.putInt("linesToSkip", start); 
            }

            start += targetSize;
            end += targetSize;
            number++;
        }
        return result;
    }
}
