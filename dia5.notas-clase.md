
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
