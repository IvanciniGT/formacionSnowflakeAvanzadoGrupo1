# Trabajar con vistas materializadas

Una vista materializada es un conjunto de datos precalculado derivado de una especificación de consulta (el SELECT en la definición de la vista) y almacenado para su uso posterior. Debido a que los datos se calculan previamente, consultar una vista materializada es más rápido que ejecutar una consulta en la tabla base de la vista. Esta diferencia de rendimiento puede ser significativa cuando una consulta se ejecuta con frecuencia o es lo suficientemente compleja. Como resultado, las vistas materializadas pueden acelerar costosas operaciones de agregación, proyección y selección, especialmente aquellas que se ejecutan con frecuencia y que se ejecutan en grandes conjuntos de datos.

## Nota

Las vistas materializadas están diseñadas para mejorar el rendimiento de las consultas para cargas de trabajo compuestas por patrones de consultas comunes y repetidos. Sin embargo, la materialización de resultados intermedios genera costos adicionales . Como tal, antes de crear vistas materializadas, debe considerarse si los costos se compensan con los ahorros que se obtienen al reutilizar estos resultados con suficiente frecuencia.

## Decidir cuándo crear una vista materializada

Las vistas materializadas son particularmente útiles cuando:

- Los resultados de la consulta contienen una pequeña cantidad de filas y/o columnas en relación con la tabla base (la tabla en la que se define la vista).

- Los resultados de la consulta contienen resultados que requieren un procesamiento significativo, que incluye:

- Análisis de datos semiestructurados.

- Agregados que tardan mucho en calcularse.

- La consulta se realiza en una tabla externa (es decir, conjuntos de datos almacenados en archivos en una etapa externa), lo que podría tener un rendimiento más lento en comparación con la consulta de tablas de bases de datos nativas.

- La tabla base de la vista no cambia con frecuencia.

## Ventajas de las vistas materializadas

La implementación de vistas materializadas por parte de Snowflake proporciona una serie de características únicas:

- Las vistas materializadas pueden mejorar el rendimiento de las consultas que utilizan los mismos resultados de subconsulta repetidamente.

- Snowflake mantiene las vistas materializadas de forma automática y transparente. Un servicio en segundo plano actualiza la vista materializada después de realizar cambios en la tabla base. Esto es más eficiente y menos propenso a errores que mantener manualmente el equivalente de una vista materializada en el nivel de la aplicación.

- Los datos a los que se accede a través de vistas materializadas siempre están actualizados, independientemente de la cantidad de DML que se haya realizado en la tabla base. Si se ejecuta una consulta antes de que la vista materializada esté actualizada, Snowflake actualiza la vista materializada o utiliza las partes actualizadas de la vista materializada y recupera los datos más nuevos necesarios de la tabla base.

### Importante

El mantenimiento automático de vistas materializadas consume créditos. 

## Decidir cuándo crear una vista materializada o una vista normal

En general, al decidir si crear una vista materializada o una vista normal, deben seguirse los siguientes criterios:

### Crear una vista materializada cuando se cumpla todo lo siguiente:

- Los resultados de la consulta de la vista no cambian con frecuencia. Esto casi siempre significa que la tabla subyacente/base de la vista no cambia con frecuencia, o al menos que el subconjunto de filas de la tabla base utilizadas en la vista materializada no cambia con frecuencia.

- Los resultados de la vista se utilizan con frecuencia (normalmente con mucha más frecuencia que los resultados de la consulta).

- La consulta consume muchos recursos. Normalmente, esto significa que la consulta consume mucho tiempo de procesamiento o créditos, pero también podría significar que la consulta consume mucho espacio de almacenamiento para resultados intermedios.

### Cree una vista normal cuando se cumpla alguna de las siguientes condiciones:

- Los resultados de la vista cambian con frecuencia.

- Los resultados no se utilizan con frecuencia (en relación con la velocidad a la que cambian).

- La consulta no requiere muchos recursos, por lo que no resulta costoso volver a ejecutarla.

Estos criterios son sólo pautas. Una vista materializada puede proporcionar beneficios incluso si no se usa con frecuencia, especialmente si los resultados cambian con menos frecuencia que el uso de la vista.

Además, hay otros factores a considerar al decidir si utilizar una vista normal o una vista materializada.

Por ejemplo, el coste de almacenar la vista materializada es un factor; Si los resultados no se utilizan con mucha frecuencia (incluso si se utilizan con más frecuencia de la que cambian), es posible que los costos de almacenamiento adicionales no valga la pena por la ganancia de rendimiento.

## Acerca de las vistas materializadas en Snowflake


## Costo de vistas materializadas

Las vistas materializadas impactan sus costos de almacenamiento y recursos informáticos:

- Almacenamiento: cada vista materializada almacena los resultados de las consultas, lo que se suma al uso de almacenamiento mensual de su cuenta.

- Recursos informáticos: para evitar que las vistas materializadas queden obsoletas, Snowflake realiza un mantenimiento automático en segundo plano de las vistas materializadas. Cuando cambia una tabla base, todas las vistas materializadas definidas en la tabla se actualizan mediante un servicio en segundo plano que utiliza recursos informáticos proporcionados por Snowflake.

  Estas actualizaciones pueden consumir recursos importantes, lo que resulta en un mayor uso de crédito. Sin embargo, Snowflake garantiza un uso eficiente del crédito al facturar a su cuenta solo los recursos reales utilizados. La facturación se calcula en incrementos de 1 segundo.

  Para saber cuántos créditos por hora de cálculo consumen las vistas materializadas, consulte la "Tabla de créditos de funciones sin servidor" en la tabla de consumo del servicio Snowflake .

### Estimación y control de costos

No existen herramientas para estimar los costos de mantener vistas materializadas. En general, los costos son proporcionales a:

- La cantidad de vistas materializadas creadas en cada tabla base y la cantidad de datos que cambian en cada una de esas vistas materializadas cuando cambia la tabla base. Cualquier cambio en las microparticiones en la tabla base requiere un eventual mantenimiento de la vista materializada, ya sea que esos cambios se deban a la reagrupación o a declaraciones DML ejecutadas en la tabla base.

- El número de esas vistas materializadas que están agrupadas. Mantener la agrupación (ya sea de una tabla o de una vista materializada) agrega costos.

- Si una vista materializada está agrupada de manera diferente a la tabla base, la cantidad de microparticiones cambiadas en la vista materializada puede ser sustancialmente mayor que la cantidad de microparticiones cambiadas en la tabla base.

## Limitaciones en la creación de vistas materializadas

Se aplican las siguientes limitaciones a la creación de vistas materializadas:
- Una vista materializada solo puede consultar una tabla.
- No se admiten uniones, incluidas las autouniones.
- Una vista materializada no puede consultar:
  - Una vista materializada.
  - Una vista no materializada.
- Una vista materializada no puede incluir:
  - UDF (esta limitación se aplica a todos los tipos de funciones definidas por el usuario, incluidas las funciones externas).
  - Funciones de ventana.
  - ORDER BY
  - LIMIT
  - GROUP BY con claves que no están en el SELECT
  - Los operadores  MINUS, EXCEPT, INTERSECT .
  - Algunas funciones agregadas  
  - Las funciones utilizadas en una vista materializada deben ser deterministas. Por ejemplo, no está permitido utilizar CURRENT_TIME o CURRENT_TIMESTAMP .



CREATE DATABASE IF NOT EXISTS mibd;
CREATE SCHEMA IF NOT EXISTS mibd.mies;
-- Create a new table with the same structure as the shared table
CREATE TABLE mibd.mies.clientes AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER LIMIT 0;

-- Insert data from the shared table into your new table
INSERT INTO mibd.mies.clientes SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER;
