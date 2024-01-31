```js


CREATE OR REPLACE PROCEDURE matar_tareas_lentas(minutes DOUBLE)
RETURNS VARIANT -- tipo de dato para devolver un JSON
EXECUTE AS CALLER
LANGUAGE JAVASCRIPT
AS
$$
    var datosADevolver = [];
    // Paso 1: Identificar las tareas lentas
    var queryCalculoMesAnterior = `
        SELECT 
            QUERY_ID, NAME
        FROM 
            TABLE(INFORMATION_SCHEMA.TASK_HISTORY()),
            (SELECT DATEADD(MINUTE, ?,CURRENT_TIMESTAMP()) as limite) tiempo
        WHERE 
            tiempo.limite > QUERY_START_TIME
            AND STATE = 'RUNNING' `;
    var datosMesAnterior = snowflake.execute({sqlText: queryCalculoMesAnterior, binds: [-1*MINUTES]});

    // Paso 2: Matar las tareas lentas
    var queryMatarTareas = "SELECT system$cancel_query(?)";
    var statement = snowflake.createStatement({sqlText: queryMatarTareas});
    while (datosMesAnterior.next()) {
        var queryId = datosMesAnterior.getColumnValue(1);
        var nombreTarea = datosMesAnterior.getColumnValue(2);
        var borrado = true;
        var error = null;
        try{
            statement.execute({binds: [queryId]});
        } catch (err) {
            borrado = false;
            error = err;
        }
        
        datosADevolver.push({queryId: queryId, nombreTarea: nombreTarea, borrado, error});
    }
    return datosADevolver;
$$
;

```