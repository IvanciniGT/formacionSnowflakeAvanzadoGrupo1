# Task y procedimientos almacenados en Snowflake

## Tasks en Snowflake

Las "Tasks" en Snowflake son objetos diseñados para automatizar la ejecución de comandos SQL, incluidas consultas, operaciones DML (Data Manipulation Language) y DDL (Data Definition Language) sobre objetos de esquema. Se emplean para programar y automatizar flujos de trabajo de carga de datos, transformaciones ETL (Extract, Transform, Load) y otras tareas administrativas y de mantenimiento.

### Características Principales

- **Programación**: Se pueden configurar para ejecutarse en intervalos regulares, utilizando sintaxis tipo cron de Unix o mediante intervalos predefinidos.

- **Encadenamiento**: Permiten la configuración de dependencias, donde la ejecución de una task puede depender de la finalización de otra, facilitando la creación de flujos de trabajo complejos.

- **Activación**: Aunque típicamente se programan, también es posible su activación manual para ejecuciones ad-hoc.

- **Integración con Streams**: Se integran con Streams para procesar automáticamente los cambios incrementales en los datos, optimizando patrones de arquitectura como ELT.

- **Monitoreo**: Snowflake proporciona herramientas para monitorear el estado y el historial de ejecución, facilitando la auditoría y el diagnóstico.

### Creación y Uso

Para crear una task, se debe definir el comando SQL a ejecutar, la programación y, opcionalmente, la dependencia de otra task. Ejemplo de creación:

```sql
CREATE OR REPLACE TASK mi_task
  SCHEDULE = 'USING CRON 0 9 * * * America/New_York' -- Diariamente a las 9 AM en la zona horaria de Nueva York.
  TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24'
  AS
  INSERT INTO mi_tabla_de_destino SELECT * FROM mi_tabla_de_origen;
```
Este comando establece una task llamada mi_task que realiza una operación INSERT todos los días a las 9:00 AM en la zona horaria America/New_York.

### Consideraciones

- **Permisos**: Se requieren permisos adecuados para crear y manejar tasks, así como sobre los objetos involucrados en los comandos SQL de la task.

- **Habilitación**: Por defecto, las tasks se crean deshabilitadas y deben habilitarse explícitamente para que comiencen a ejecutarse según lo programado.

Las tasks son fundamentales en Snowflake para la automatización eficiente de operaciones recurrentes y flujos de trabajo de datos, manteniendo los sistemas actualizados y minimizando la necesidad de intervención manual.

---

## Procedimientos Almacenados en Snowflake

Los procedimientos almacenados proporcionan una manera de encapsular lógica compleja, operaciones de transformación de datos y tareas administrativas en Snowflake. Estas unidades de código ejecutable ofrecen funcionalidades avanzadas para realizar operaciones complejas.

### Características Principales

- **Uso de JavaScript**: Se emplea JavaScript para la implementación del cuerpo de los procedimientos almacenados, brindando flexibilidad y acceso a un amplio ecosistema de desarrollo.

- **Ejecución de Comandos SQL**: Las sentencias SQL se pueden ejecutar dentro de los procedimientos almacenados utilizando métodos de la API de JavaScript de Snowflake, como `snowflake.execute()`.

- **Manejo de Transacciones**: Es posible controlar transacciones dentro de los procedimientos almacenados, lo que incluye la capacidad de iniciar, confirmar y revertir transacciones.

- **Parametrización**: Se admiten parámetros de entrada, haciendo que los procedimientos almacenados sean reutilizables y configurables.

- **Lógica de Control de Flujo**: La lógica compleja de control de flujo se facilita a través de estructuras como bucles y condicionales en JavaScript.

- **Gestión de Errores**: Los errores se pueden manejar mediante bloques `try/catch`, permitiendo una gestión sofisticada y personalizada de los mismos.

### Creación de Procedimientos Almacenados

La definición de un procedimiento almacenado se realiza mediante la sentencia `CREATE PROCEDURE`, especificando parámetros y el cuerpo del procedimiento en JavaScript. Ejemplo:

```sql
CREATE OR REPLACE PROCEDURE my_procedure(param1 STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  // Ejemplo de cuerpo del procedimiento en JavaScript.
  var command = "SELECT CURRENT_TIMESTAMP() AS TIMESTAMP";
  var statement = snowflake.createStatement({sqlText: command});
  var result = statement.execute();
  result.next();
  return "La hora actual es: " + result.getColumnValue(1);
$$;
```

### Ejecución de Procedimientos Almacenados
Para invocar un procedimiento almacenado, se utiliza la sentencia CALL con los argumentos necesarios:

```sql
CALL my_procedure('Argumento de ejemplo');
```

### Consideraciones

- **Privilegios**: Se requieren privilegios adecuados para crear y ejecutar procedimientos almacenados.
- **Rendimiento**: Es crucial considerar el rendimiento, especialmente para operaciones complejas o que procesan grandes volúmenes de datos.
- **Depuración**: La depuración de procedimientos almacenados puede presentar desafíos adicionales debido a la naturaleza del código JavaScript y la ejecución de comandos SQL en este contexto.

Los procedimientos almacenados en Snowflake son herramientas poderosas para implementar lógica de negocios compleja y automatizar tareas, proporcionando capacidades avanzadas dentro del entorno de base de datos en la nube.

---

# Ejemplo: 

Un task que el día 1 de cada mes, saque de la tabla `SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES` las ventas que hemos tenido y las meta en una tabla nueva. 

Después que cree o reemplace una vista apuntando a ese tabla. 

La tabla debe tener por nombre ventas, seguido del mes y año. 

La vista siempre debe ser ventas_ultimas

## Procedimiento

Para montar una task en Snowflake que realice las acciones descritas, necesitaremos dividir el proceso en varios pasos. 

### Paso 1: Crear la Task
La task se ejecutará el primer día de cada mes y realizará las siguientes acciones:

- Crear una nueva tabla con el nombre ventas_MM_YYYY, donde MM es el mes y YYYY es el año.
- Insertar en esta tabla las ventas del mes anterior obtenidas de WEB_SALES.
- Crear o reemplazar una vista llamada ventas_ultimas que apunte a la tabla recién creada.

Debido a que Snowflake no permite ejecutar múltiples comandos SQL directamente dentro de una task (como crear tablas y luego insertar datos en la misma operación), es necesario utilizar un procedimiento almacenado que encapsule esta lógica.

### Paso 2: Crear el Procedimiento Almacenado
El procedimiento almacenado contendrá la lógica para crear la tabla y la vista, y luego insertará los datos. Aquí tienes un ejemplo de cómo podría ser el procedimiento (ajusta los tipos de datos y columnas según tus necesidades):

```sql
CREATE OR REPLACE PROCEDURE procesar_ventas_mensuales()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  var fecha_actual = new Date();
  var mes = String(fecha_actual.getMonth()).padStart(2, '0'); // Mes del año (0-11), ajustar según necesidad
  var año = fecha_actual.getFullYear();
  var nombre_tabla = `ventas_${mes}_${año}`;
  var nombre_vista = `ventas_ultimas`;

  // Crear la tabla nueva
  var sql_crear_tabla = `CREATE OR REPLACE TABLE ${nombre_tabla} AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES WHERE MONTH(TO_DATE(WEB_SALES.ws_sold_date_sk)) = ${mes} AND YEAR(TO_DATE(WEB_SALES.ws_sold_date_sk)) = ${año};`;
  snowflake.execute({sqlText: sql_crear_tabla});

  // Crear o reemplazar la vista
  var sql_crear_vista = `CREATE OR REPLACE VIEW ${nombre_vista} AS SELECT * FROM ${nombre_tabla};`;
  snowflake.execute({sqlText: sql_crear_vista});

  return "Proceso completado.";
$$;
```
Este procedimiento obtiene el mes y el año actuales, crea una tabla para las ventas de ese mes y luego crea una vista que apunta a esa tabla.

Paso 3: Crear la Task
Una vez que tienes el procedimiento almacenado, puedes crear la task que se ejecutará el primer día de cada mes:

```sql
CREATE OR REPLACE TASK procesar_ventas_task
  WAREHOUSE = 'mi_warehouse' -- Asegúrate de especificar tu propio warehouse
  SCHEDULE = 'USING CRON 0 0 1 * * UTC' -- A las 00:00 horas del primer día de cada mes, en UTC
AS
  CALL procesar_ventas_mensuales();
```
Asegúrate de ajustar el WAREHOUSE a uno que tengas disponible y tenga los permisos necesarios. Además, la expresión CRON está configurada para UTC; ajusta la zona horaria según sea necesario.

---

## Ejemplo 2

```sql
CREATE OR REPLACE PROCEDURE insert_with_auto_id(param1 VARCHAR, param2 VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  // Verificar si la tabla existe
  var checkTable = "SHOW TABLES LIKE 'mi_tabla'";
  var tableExists = snowflake.execute({sqlText: checkTable});
  var tableFound = tableExists.next();
  
  // Si la tabla no existe, crearla
  if (!tableFound) {
    var createTable = "CREATE TABLE mi_tabla (id INT AUTOINCREMENT, columna1 VARCHAR, columna2 VARCHAR)";
    snowflake.execute({sqlText: createTable});
  }
  
  // Insertar los datos en la tabla
  var insertData = "INSERT INTO mi_tabla(columna1, columna2) VALUES (?, ?)";
  var statement = snowflake.createStatement({sqlText: insertData, binds:[PARAM1, PARAM2]});
  statement.execute();
  
  // Obtener el último ID generado
  var lastIdQuery = "SELECT LAST_INSERT_ID()";
  var lastIdResult = snowflake.execute({sqlText: lastIdQuery});
  lastIdResult.next();
  var lastId = lastIdResult.getColumnValue(1);
  
  return lastId;
$$;

CALL insert_with_auto_id('valor1', 'valor2');

```