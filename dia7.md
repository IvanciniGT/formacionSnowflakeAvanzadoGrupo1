## TASK_HISTORY en Snowflake

La vista `TASK_HISTORY` en Snowflake proporciona detalles históricos sobre las ejecuciones de tareas, incluyendo información sobre tiempos de ejecución, éxito o fallo de las tareas y otros metadatos relevantes.

### Consultando TASK_HISTORY

Para obtener información del historial de una tarea específica o todas las tareas, se puede utilizar la siguiente consulta:

    SELECT *
    FROM "SNOWFLAKE"."ACCOUNT_USAGE"."TASK_HISTORY"
    WHERE NAME = 'nombre_de_tu_tarea'
    ORDER BY SCHEDULED_TIME DESC;

Reemplazar `'nombre_de_tu_tarea'` por el nombre real de la tarea. Para ver el historial de todas las tareas, omitir la cláusula `WHERE`.

### Columnas Importantes en TASK_HISTORY

- **NAME**: Nombre de la tarea.
- **DATABASE_NAME**, **SCHEMA_NAME**: La base de datos y esquema donde se ubica la tarea.
- **STATE**: Estado de la ejecución, como `SUCCEEDED` o `FAILED`.
- **SCHEDULED_TIME**: Hora programada para la ejecución.
- **COMPLETED_TIME**: Hora de finalización de la tarea.
- **ERROR_CODE**, **ERROR_MESSAGE**: Códigos y mensajes de error para ejecuciones fallidas.

### Consideraciones

- **Retención de Datos**: Las políticas de retención de Snowflake pueden limitar la disponibilidad de datos históricos en `TASK_HISTORY`.
- **Acceso**: Se requieren permisos adecuados para acceder a las vistas de uso de cuenta.

`TASK_HISTORY` es esencial para auditar y monitorear la eficiencia y fiabilidad de las tareas en Snowflake, ofreciendo información clave para la optimización y resolución de problemas.



SELECT * from midb.mies.eventos;

SELECT *
FROM "SNOWFLAKE"."ACCOUNT_USAGE"."TASK_HISTORY"
ORDER BY SCHEDULED_TIME DESC;



CREATE OR REPLACE PROCEDURE listar_tareas_por_tiempo(minutos INT)
RETURNS TABLE (TASK_NAME STRING, SCHEDULED_TIME TIMESTAMP_LTZ, STATE STRING)
LANGUAGE SQL
EXECUTE AS CALLER

AS
$$
SELECT NAME AS TASK_NAME, SCHEDULED_TIME, STATE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE STATE = 'RUNNING'
  AND SCHEDULED_TIME <= DATEADD(MINUTE, -MINUTOS, CURRENT_TIMESTAMP());
$$;

CREATE OR REPLACE PROCEDURE listar_tareas_por_tiempo_2(minutos DOUBLE)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  var query = `
    SELECT NAME AS TASK_NAME, SCHEDULED_TIME, STATE
    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
    WHERE STATE = 'RUNNING'
    AND SCHEDULED_TIME <= DATEADD(MINUTE, -5, CURRENT_TIMESTAMP())`;

  var statement = snowflake.createStatement({sqlText: query});
  var resultSet = statement.execute();
  var resultJson = {};
  
  if (resultSet.next()) {
    resultJson = resultSet.getColumnValue(1);
  }

  return JSON.stringify(resultJson);
$$;



DROP PROCEDURE listar_tareas_por_tiempo();
CREATE OR REPLACE PROCEDURE listar_tareas_por_tiempo()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var query = `
SELECT NAME AS TASK_NAME, SCHEDULED_TIME, STATE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE STATE = 'RUNNING'
  AND SCHEDULED_TIME <= DATEADD(MINUTE, -5, SYSDATE())`;
snowflake.execute({sqlText: query});
  

$$;

CALL listar_tareas_por_tiempo_2(10);


SELECT NAME AS TASK_NAME, SCHEDULED_TIME, STATE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE STATE = 'RUNNING'
  AND SCHEDULED_TIME <= DATEADD(MINUTE, -5, SYSDATE());



CREATE EVENT TABLE IF NOT EXISTS midb.mies.eventos;
ALTER ACCOUNT SET EVENT_TABLE = midb.mies.eventos;
SHOW PARAMETERS LIKE 'EVENT_TABLE' IN ACCOUNT;
ALTER DATABASE midb SET LOG_LEVEL = ERROR;
ALTER PROCEDURE extraer_datos_mes(DOUBLE,DOUBLE) SET LOG_LEVEL = DEBUG;
