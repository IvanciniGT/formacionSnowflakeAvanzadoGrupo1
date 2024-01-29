
## FROM

En la clausula FROM podemos añadir:
- JOINS
  - Inner Join
  - Outer Join
    - Left Outer Join
    - Right Outer Join
    - Full Outer Join
- CARTESIAN PRODUCT / CROSS JOIN

    Tabla 1     Tabla 2
        1           A
        2           B
        3   

    Resultado del CROSS JOIN:
        1 A
        1 B
        2 A
        2 B
        3 A
        3 B
- LATERAL FLATTEN
  Hace un cross join de una tabla con un array 
    En nuestro ejemplo, la tabla solo tiene 1 fila.

        EXACTOS:
        WS_SHIP_MODE_SK,RECUENTO
            18,359928785
            13,359926003
            16,359919426
            20,359919241
            7,359916963
            10,359916609
            9,359913632
            14,359912555
            15,359910657
            2,359909064
        ESTIMACIONES: 
            18,359928785
            13,359926003
            16,359919426
            20,359919241
            7,359916963
            10,359916609
            9,359913632
            14,359912555
            15,359910657
            2,359909064



---

# TASKS, STREAMS, PROCEDIMIENTOS ALMACENADOS en Snowflake

## Tasks en Snowflake

1 query que se ejecutan de forma automática y con una periodicidad determinada dentro de Snowflake.
Esa periodicidad la puedo definir en:
- Intervalos de tiempo
- Sintaxis CRON (Muy potente)

## PROCEDIMIENTOS ALMACENADOS en Snowflake

Me permiten ejecutar pequeños scripts (con argumentos de entrada y salida) dentro de Snowflake.
Igual que en cualquier BBDD relacional.
Peculiaridad en Snowflake: PL/SQL.
    En el caso de Snowflake, No se admite PL/SQL --> Javascript
    JS Lo usamos para definir el flujo / lógica del procedimientos, variables...
    Queries van en SQL.

## STREAMS en Snowflake

Monitorizan tablas para ver los cambios que se van produciendo en ellas.
    - INSERTS
    - UPDATES
    - DELETES
    - MERGES
Nos pueden servir para montar ETLs


PROCEDIMIENTOS ALMACENADOS:
```sql
    CREATE OR REPLACE PROCEDURE <NOMBRE>(ARG1 TIPO, ARG2 TIPO,...)
    RETURNS TIPO
    LANGUAGE JAVASCRIPT (JAVA, PYTHON, Scala, SF Scripting)
    AS
    $$
        //Codigo en JS
        snowflake.execute({sqlText: 'SELECT * FROM TABLE'}); -> TABLA DE DATOS (resultset) 
        snowflake.createStatement({sqlText: 'INSERT INTO TABLE VALUES (?,?)', binds: [ARG1, ARG2]});  ---> Statement  .execute() --> Tabla de datos
            
        de una tabla de datos, para sacar el valor de una columna: .getColumnValue(1) --> Devuelve el valor de la primera columna
        
        que podremos navegar con un .next()
        return <VALOR>;
    $$
```

## Ejemplo:

1 vez al mes, el día 1 de cada mes... o no...(quizás el procedimineto se lanza el día 2, pero quiero hacer la operación siempre sobre el mes anterior), se genera una tabla extrayendo datos de otra tabla (ventas)... filtrando por las ventas del mes anterior.
Y esa tabla queremos que tenga por nombre: ventas_2024_01
La tabla puede existir o no.... si existe, la borramos y la volvemos a crear (o un truncate)
Después, vamos a querer una vista (que puede ya existir) que llamaremos ventas_mes_anterior, que apunte a esa tabla que hemos creado.

En nuestro caso tenemos en nuestra BBDD la tabla: ventas (antigua web_sales) y la tabla fechas (antigua date_dim).

PASOS:
1- Saber el mes anterior y el año del mes anterior:
    SELECT
    MONTH(DATEADD(MONTH, -1, CURRENT_DATE())) AS MES_ANTERIOR,
    YEAR(DATEADD(MONTH, -1, CURRENT_DATE())) AS MES_ANTERIOR;

2- Crear la tabla de ventas del mes anterior:
    CREATE TABLE IF NOT EXISTS ventas_2024_01 AS SELECT * FROM ventas LIMIT 0;

3- Por si acaso la tabla ya existia, me cepillo los datos que tuviera:
    TRUNCATE TABLE ventas_2024_01;

4- Le copio los datos:
    INSERT INTO ventas_2024_01
    SELECT 
      * 
    FROM 
        ventas,
        fechas
    WHERE ?????

5- Creo la vista o reemplazamos la que hubiera:
    CREATE OR REPLACE VIEW ventas_mes_anterior AS SELECT * FROM ventas_2024_01;
        -- básicamente estamos dando un alias a la tabla!

---
```sql
CREATE OR REPLACE PROCEDURE extraer_datos_mes_anterior()
RETURNS NUMBER
LANGUAGE JAVASCRIPT
AS
$$
    // Paso 1: Calcular mes y año del mes del mes anterior
    var queryCalculoMesAnterior = "SELECT DATEADD(MONTH, -1, CURRENT_DATE()) as MES_ANTERIOR, MONTH(MES_ANTERIOR), YEAR(MES_ANTERIOR)";
    var datosMesAnterior = snowflake.execute({sqlText: queryCalculoMesAnterior});
    datosMesAnterior.next();
    var mes = datosMesAnterior.getColumnValue(2);
    var anio = datosMesAnterior.getColumnValue(3);

    // Paso 2: Preparar la tabla receptora de los datos del mes anterior
    var queryExistenciaTablaNueva = "SHOW TABLES LIKE 'ventas_" + anio + "_" + mes + "'";
    var resultadoTablaNueva = snowflake.execute({sqlText: queryExistenciaTablaNueva});
    var existeLaTabla = resultadoTablaNueva.next();

    // En función de si la tabla existe ya o no, hago unas u otras tareas.
    var queryPreparacionNuevaTabla = "CREATE TABLE ventas_" + anio + "_" + mes + " AS SELECT * FROM ventas LIMIT 0";
    if(existeLaTabla){
        queryPreparacionNuevaTabla = "TRUNCATE TABLE ventas_" + anio + "_" + mes;
    }
    snowflake.execute({sqlText: queryPreparacionNuevaTabla});

    // Paso 3. Copiado de los datos del mes anterior a la nueva tabla
    var queryCopiadoDatos = "INSERT INTO ventas_" + anio + "_" + mes + " SELECT v.* FROM ventas v, fechas f WHERE v.ws_sold_date_sk = f.d_date_sk AND f.d_moy = " + mes + " AND f.d_year = " + anio;
    snowflake.execute({sqlText: queryCopiadoDatos});

    // Paso 4. Creación de la vista
    var queryCreacionVista = "CREATE OR REPLACE VIEW ventas_mes_anterior AS SELECT * FROM ventas_" + anio + "_" + mes;
    snowflake.execute({sqlText: queryCreacionVista});

    // Paso 5: Calculo de nuevos datos insertados y prueba de la vista
    var queryPruebaVista = "SELECT COUNT(*) FROM ventas_mes_anterior";
    var resultadoPruebaVista = snowflake.execute({sqlText: queryPruebaVista});
    resultadoPruebaVista.next();
    var numeroFilas = resultadoPruebaVista.getColumnValue(1);

    return numeroFilas;
$$
```

Vamos a crear una función nueva que reciba el mes y el año como parámetros de entrada, para hacer este trabajo:
    extraer_datos_mes(anio, mes)
Y actualizamos esta funcion que hemos creado, para que calcula los datos del mes anterior... y después llame al nuevo procedimiento
    extraer_datos_mes_anterior()
        Calcular datos mes anterior
        Y llamar al nuevo procedimiento extraer_datos_mes con los datos calculados.


```sql
CREATE EVENT TABLE IF NOT EXISTS midb.mies.eventos;
ALTER ACCOUNT SET EVENT_TABLE = midb.mies.eventos;
SHOW PARAMETERS LIKE 'EVENT_TABLE' IN ACCOUNT;


CREATE OR REPLACE PROCEDURE extraer_datos_mes(anio DOUBLE, mes DOUBLE)
RETURNS DOUBLE
LANGUAGE JAVASCRIPT
AS
$$
    // Paso -1: Comprobación de parámetros
    // Me aseguro que el año tenga 4 dígitos... y sea positivo
    if(anio < 1000 || anio > 9999){
        snowflake.log("error", "El año debe ser válido");
        throw "El año debe tener 4 dígitos"; // Lanza una exception y corta la ejecución del Procedure, mostrando al que ha llamado al procedure ese mensaje de error.
    }
    // Me aseguro que el mes sea un número entre 1 y 12
    if(mes < 1 || mes > 12){
        snowflake.log("error", "El mes debe ser válido");
        throw "El mes debe ser un número entre 1 y 12"; // Lanza una exception y corta la ejecución del Procedure, mostrando al que ha llamado al procedure ese mensaje de error.
    }

    // Paso 0: Me genero el nombre de la tabla con JS
    var nombreTabla = "ventas_" + ANIO + "_" + ('0' + MES).substr(-2);
    
    // Paso 0 .... opcion SQL
    var queryNombreTabla = "SELECT 'ventas_' || CAST(:1 AS STRING) || '_' || LPAD(CAST(:2 AS STRING),2,'0')";
    var resultadoNombreTabla = snowflake.execute({sqlText: queryNombreTabla, binds: [ANIO, MES]});
    resultadoNombreTabla.next();
    var nombreTabla = resultadoNombreTabla.getColumnValue(1);

    snowflake.log("debug", "Se procede a la creación/preparación de la tabla: '"+nombreTabla+"'");

    // Paso 1: Preparar la tabla receptora de los datos del mes anterior
    var queryExistenciaTablaNueva = "SHOW TABLES LIKE '"+nombreTabla+ "'";
    var resultadoTablaNueva = snowflake.execute({sqlText: queryExistenciaTablaNueva});
    var existeLaTabla = resultadoTablaNueva.next();

    // En función de si la tabla existe ya o no, hago unas u otras tareas.
    var queryPreparacionNuevaTabla = "CREATE TABLE "+nombreTabla+" AS SELECT * FROM ventas LIMIT 0";
    if(existeLaTabla){
        queryPreparacionNuevaTabla = "TRUNCATE TABLE "+nombreTabla;
    }
    snowflake.execute({sqlText: queryPreparacionNuevaTabla});
    snowflake.log("debug", "Se ha creado/preparado la tabla: '"+nombreTabla+"' correctamente");


    // Paso 2. Copiado de los datos del mes anterior a la nueva tabla
    var queryCopiadoDatos = "INSERT INTO " + nombreTabla + " SELECT v.* FROM ventas v, fechas f WHERE v.ws_sold_date_sk = f.d_date_sk AND f.d_moy = :1 AND f.d_year = :2";
    snowflake.execute({sqlText: queryCopiadoDatos, binds: [MES, ANIO]});
    snowflake.log("debug", "Se han copiado los datos a la tabla: '"+nombreTabla+"' correctamente");

    // Paso 3. Creación de la vista
    var queryCreacionVista = "CREATE OR REPLACE VIEW ventas_mes_anterior AS SELECT * FROM " + nombreTabla;
    snowflake.execute({sqlText: queryCreacionVista});
    snowflake.log("debug", "Se ha actualizado la referencia de la vista 'ventas_mes_anterior' a la nueva tabla: '"+nombreTabla+"'");

    // Paso 4: Calculo de nuevos datos insertados y prueba de la vista
    var queryPruebaVista = "SELECT COUNT(*) FROM ventas_mes_anterior";
    var resultadoPruebaVista = snowflake.execute({sqlText: queryPruebaVista});
    resultadoPruebaVista.next();
    var numeroFilas = resultadoPruebaVista.getColumnValue(1);

    snowflake.log("info", "Se han insertado "+numeroFilas+" filas en la tabla '"+nombreTabla+"' y se ha actualizado la referencia de la vista 'ventas_mes_anterior'");

    return numeroFilas;
$$
;


CREATE OR REPLACE PROCEDURE extraer_datos_mes_anterior()
RETURNS DOUBLE
LANGUAGE JAVASCRIPT
AS
$$
    // Paso 1: Calcular mes y año del mes del mes anterior
    var queryCalculoMesAnterior = "SELECT DATEADD(MONTH, -1, CURRENT_DATE()) as MES_ANTERIOR, MONTH(MES_ANTERIOR), YEAR(MES_ANTERIOR)";
    var datosMesAnterior = snowflake.execute({sqlText: queryCalculoMesAnterior});
    datosMesAnterior.next();
    var mes = datosMesAnterior.getColumnValue(2);
    var anio = datosMesAnterior.getColumnValue(3);

    // Paso 2, llamo al procedimiento extraer_datos_mes y devuelvo el resultado
    var queryInvocacionOtroProcedimiento = "CALL extraer_datos_mes(:1, :2)";
    var resultadoInvocacionOtroProcedimiento = snowflake.execute({sqlText: queryInvocacionOtroProcedimiento, binds: [anio, mes]});
    resultadoInvocacionOtroProcedimiento.next();
    var numeroFilas = resultadoInvocacionOtroProcedimiento.getColumnValue(1);
    return numeroFilas;
$$
;

call extraer_datos_mes(2000, 2);

SELECT count(*) FROM ventas_2000_02;

SELECT count(*) FROM ventas_mes_anterior;

call extraer_datos_mes_anterior();

SELECT count(*) FROM ventas_2023_12;

SELECT count(*) FROM ventas_mes_anterior;

```
