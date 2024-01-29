-- Asignando el warehose
USE WAREHOUSE compute_wh;

-- Creando base de datos y esquema
CREATE DATABASE IF NOT EXISTS midb;
USE DATABASE midb;
CREATE SCHEMA IF NOT EXISTS mies;
USE SCHEMA midb.mies;

-- Me creo la tabla con la estructura adecuada
CREATE TABLE midb.mies.clientes AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER LIMIT 0;
-- Aquí creariamos los cluster keys.
SELECT * FROM midb.mies.clientes;

-- Le enchufo datos
INSERT INTO midb.mies.clientes SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER;
SELECT COUNT(*) FROM midb.mies.clientes;

-- Vamos a sacar a federico
SELECT * FROM midb.mies.clientes WHERE C_CUSTOMER_SK = 35313666;

-- El resultado hasta aquí es una ruina... Lee un huevo y medio de particiones (entorno a 90 de 150)
-- CLusterizamos por id
ALTER TABLE midb.mies.clientes CLUSTER BY(C_CUSTOMER_SK);

SELECT * FROM midb.mies.clientes WHERE C_CUSTOMER_SK = 35813666;
SELECT * FROM midb.mies.clientes WHERE C_CUSTOMER_SK = 35313666;
SELECT * FROM midb.mies.clientes WHERE C_CUSTOMER_SK = 34313266;
SELECT * FROM midb.mies.clientes WHERE C_CUSTOMER_SK = 35713666;
SELECT * FROM midb.mies.clientes WHERE C_CUSTOMER_SK = 35813665;

SELECT * FROM midb.mies.clientes WHERE C_CUSTOMER_SK = 35393666;

SELECT * FROM midb.mies.clientes WHERE C_CUSTOMER_SK = 58393666;
-- Y dentro de 5 minutos... ya buscará solo en 1 partición.
-- Arriba le hemos dado la instrucción... y me contesta que recbido
-- El trabajo se comienza a hacer en segundo plano... y en algún momento estará acabado...
-- Depende la carga de trabajo... el volumen de datos...

SELECT * FROM mibd.mies.clientes WHERE C_CUSTOMER_SK = 48393666;


-- TODO
-- Ventas en la WEB_Sales -> 7kM... 1kM... 50Gbs... Cambiais la máquina a SMALL... 3,5 mins
---LIMIT 1000*1000*1000
-- Fechas: DATE_DIM... entera 73000

-- QUERY:
-- Dame la cantidad de ventas agrupadas por meses del año 2001
    -- Dentro de DATE_DIM > D_MOY, D_YEAR

-- Analizamos la query -> Muirar el # de particiones que se usan

-- Añadimos clustering keys... poco a poco
ALTER SESSION SET use_cached_result = FALSE; -- Se fuerza que no se usen datos de la cache.



---



-- Me creo la tabla con la estructura adecuada
CREATE TABLE midb.mies.ventas AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES LIMIT 0;
-- Aquí creariamos los cluster keys.
SELECT * FROM midb.mies.ventas;
-- Le enchufo datos
INSERT INTO midb.mies.ventas SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES LIMIT 1000000000;
SELECT COUNT(*) FROM midb.mies.ventas;

-- Me creo la tabla con la estructura adecuada
CREATE TABLE midb.mies.fechas AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM LIMIT 0;
-- Aquí creariamos los cluster keys.
SELECT * FROM midb.mies.fechas;
-- Le enchufo datos
INSERT INTO midb.mies.fechas SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM;
SELECT COUNT(*) FROM midb.mies.fechas;

ALTER SESSION SET use_cached_result = FALSE; -- Se fuerza que no se usen datos de la cache.
SELECT 
    f.d_moy,
    count(*) 
FROM 
    midb.mies.ventas v
    INNER JOIN midb.mies.fechas f ON v.ws_sold_date_sk = f.d_date_sk
WHERE 
    f.d_year=1999
GROUP BY 
    f.d_moy
ORDER BY 
    f.d_moy;


SELECT 
    f.d_year,
    COUNT(*) 
FROM 
    midb.mies.ventas v
    INNER JOIN midb.mies.fechas f ON v.ws_sold_date_sk = f.d_date_sk
GROUP BY 
    f.d_year
ORDER BY 
    f.d_year;

ALTER TABLE midb.mies.fechas CLUSTER BY (d_date_sk);
ALTER TABLE midb.mies.ventas CLUSTER BY (ws_sold_date_sk); --ws_sold_date_sk

---

-- Lo que hemos vendido ($) por mes de cada año... y que tal ha sido ese mes, dentro del año

SELECT 
    f.d_year,
    f.d_moy,
    sum(v.ws_net_paid)  as pagado,
    rank() OVER(PARTITION BY f.d_year ORDER BY pagado ) as puesto
FROM 
    midb.mies.ventas v
    INNER JOIN midb.mies.fechas f ON v.ws_sold_date_sk = f.d_date_sk
GROUP BY 
    f.d_year,f.d_moy
ORDER BY 
    f.d_year,f.d_moy;

-- Los 3 mejores meses de cada año... y sus ventas
SELECT 
    *
FROM 
    (SELECT 
        f.d_year,
        f.d_moy,
        sum(v.ws_net_paid)  as pagado,
        rank() OVER(PARTITION BY f.d_year ORDER BY pagado ) as puesto
    FROM 
        midb.mies.ventas v
        INNER JOIN midb.mies.fechas f ON v.ws_sold_date_sk = f.d_date_sk
    GROUP BY 
        f.d_year,f.d_moy
    )
WHERE 
    puesto < 4
ORDER BY 
    d_year,puesto
    ;


SELECT 
    f.d_year,
    f.d_moy,
    sum(v.ws_net_paid)  as pagado,
    rank() OVER(PARTITION BY f.d_year ORDER BY pagado DESC ) as puesto
FROM 
    midb.mies.ventas v
    INNER JOIN midb.mies.fechas f ON v.ws_sold_date_sk = f.d_date_sk
GROUP BY 
    f.d_year,f.d_moy
QUALIFY puesto < 4
ORDER BY 
    f.d_year, puesto;

---
-- Saber los meses que han incrementado las ventas con respecto al mes anterior en más de un 50%

WITH ventas_por_meses AS (
    SELECT 
        f.d_year,
        f.d_moy,
        sum(v.ws_net_paid)  as importe
    FROM 
        midb.mies.ventas v
        INNER JOIN midb.mies.fechas f ON v.ws_sold_date_sk = f.d_date_sk
    GROUP BY 
        f.d_year,f.d_moy
)
SELECT 
    d_year,
    d_moy,
    importe,
    LAG(importe, 1) OVER (ORDER BY d_year, d_moy) as importe_anterior
FROM ventas_por_meses
QUALIFY importe / importe_anterior > 2;


-- Ventas por dia en el año 2001
USE SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL;

WITH importes_dia_2000 AS (
    SELECT 
        f.d_year,
        f.d_moy, 
        f.d_dom,
        sum(v.ws_net_paid) as importe_total_dia
    FROM 
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES v
        INNER JOIN SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM f ON v.ws_sold_date_sk = f.d_date_sk
    WHERE 
        f.d_year = 2000
    GROUP BY 
        f.d_year,f.d_moy, f.d_dom
)
SELECT 
        d_year,
        d_moy, 
        d_dom,
        importe_total_dia,
        sum(importe_total_dia) over() as importe_total_anual,
        SUM(importe_total_dia) over ( 
            order by d_year, d_moy, d_dom 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as acumulado_anual,
        avg(importe_total_dia) over ( 
            order by d_year, d_moy, d_dom 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as media_7_dias,
        rank() over (order by importe_total_dia desc) as puesto_anual,
        rank() over (partition by d_moy order by importe_total_dia desc) as puesto_mensual
FROM 
    importes_dia_2000
ORDER BY 
    d_year,
    d_moy, 
    d_dom;

--- Cuantos dias distintos tenemos en la tabla de ventas. Dias en los que hemos vendido

SELECT 
    COUNT (DISTINCT v.ws_sold_date_sk) as dias_de_venta
FROM 
   SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES v;

SELECT 
    APPROX_COUNT_DISTINCT ( v.ws_sold_date_sk) as dias_de_venta
FROM 
   SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES v;

---

SELECT 
    WS_ITEM_SK,
    count_if(ws_net_paid > 1000) AS ventas_mayores_de_1000,
    count_if(ws_net_paid > 2000) AS ventas_mayores_de_2000,
    count_if(ws_net_paid > 3000) AS ventas_mayores_de_3000,
    count_if(ws_net_paid > 4000) AS ventas_mayores_de_4000
FROM 
   SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES v
GROUP BY 
    WS_ITEM_SK;

---

SELECT 
    WS_ITEM_SK,
    IFF(ws_net_paid > 4000, 'Importante', 'Pqeueña') as TIPO_VENTA
FROM 
   SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES v
LIMIT 100;

---

--- Dame las 10 formas de envío más solicitadas
SELECT 
    WS_SHIP_MODE_SK,
    count(*) as recuento
FROM 
   SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES v
GROUP BY WS_SHIP_MODE_SK
ORDER BY recuento DESC
LIMIT 10;
---
SELECT APPROX_TOP_K(WS_SHIP_MODE_SK, 10)     
FROM 
   SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES v;
---

WITH top_tipos_envio AS (
    SELECT APPROX_TOP_K(WS_SHIP_MODE_SK, 10) as lista_tipos_envio     
    FROM 
       SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES
)
SELECT 
    --*
    value[0]::INT as tipo_envio,
    value[1]::INT as numero_ventas_estimado
FROM
    top_tipos_envio, 
    LATERAL FLATTEN(top_tipos_envio.lista_tipos_envio);

----
-- El precio maximo pagado en el 95% de las ventas
SELECT 
    APPROX_PERCENTILE(ws_net_paid, 0.95)
FROM 
   SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES; -- 8625

SELECT 
    MAX(ws_net_paid)
FROM 
   SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES;

---
-- Saber las ventas con importe menor a 8625... y las ventas totales... y dividirlos
-- Todo en una query
SELECT 
    COUNT_IF(ws_net_paid < 8625.25) as menores,
    COUNT(*) as totales,
    menores/totales as percentil95
FROM 
   SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES;

---
SELECT 
    MAX_BY(ws_item_sk, ws_net_paid, 3) -- MIN_BY
FROM 
   SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES;
---
SELECT 
    ws_item_sk, ws_net_paid
FROM 
   SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES
WHERE ws_net_paid is not null
ORDER BY
    ws_net_paid DESC
LIMIT 3;
---
SELECT ws_item_sk, max(ws_net_paid) as pagado
FROM 
SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES
WHERE 
ws_item_sk IN (166339,
  348584,
  194707,
  236224,
  35860,
  355482)
GROUP BY ws_item_sk;

---

SELECT 
    *
FROM (values 
    (1,2),
    (1, null),
    (null, 2),
    (null, null)
);
---
SELECT 
    column1, column2, coalesce(column1, column2) as primer_no_nulo
FROM (values 
    (1,2),
    (1, null),
    (null, 2),
    (null, null)
);
---
SELECT 
    column1, decode(column1,
        1, 'UNO',
        2, 'DOS',
        null, 'NULO',
        'OTRO'
    ) as resultado
FROM (values 
    (1),
    (2),
    (3),
    (null)
);
---
SELECT 
    column1, ifnull(column1, 0) as resultado
FROM (values 
    (1),
    (2),
    (3),
    (null)
);

---
-- FECHAS

-- FECHA DE HOY
ALTER SESSION SET TIMEZONE = 'Europe/Madrid';

SELECT
    SYSDATE(),
    CURRENT_TIMESTAMP();
-- SYSDATE que nos ofrece un TIMESTAMP... a pesar del nombre
-- DIFIEREN en la zona horaria
-- SYSDATE trabaja en UTC (ZONA 0)
-- CURRENT_TIMESTAMP trabaja en la zona definida en la sesion del usuario con la que conectamos.

ALTER SESSION SET TIMEZONE = 'Europe/Lisbon';
SELECT
    SYSDATE(),
    CURRENT_TIMESTAMP(),
    CURRENT_DATE,
    CURRENT_TIME,
    LOCALTIME,
    LOCALTIMESTAMP;

ALTER SESSION SET TIMESTAMP_NTZ_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI';
ALTER SESSION SET TIMESTAMP_LTZ_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI';

SELECT
    SYSDATE(),
    CURRENT_TIMESTAMP,
    LOCALTIME;
-- Todas esas son funciones... como no tienen argumentos, las puedo invocar con o sin parentesis.

SELECT TO_DATE('15/29/2024', 'MM/DD/YYYY'), TO_DATE('29/01/2024', 'DD/MM/YYYY');
SELECT TRY_TO_DATE('15/29/2024', 'MM/DD/YYYY'), TO_DATE('29/01/2024', 'DD/MM/YYYY');
SELECT TRY_CAST('8273' AS INT);
SELECT TRY_TO_DECIMAL('8273.98');

---
WITH frutas AS (
    SELECT 
        column1 as fruta, 
        column2 as colores 
    FROM values 
        ('manzana', 'roja,verde,amarilla'),
        ('ciruela', 'morada,verde,amarilla')
)
SELECT 
    fruta,
    split(colores, ',') as colores
FROM frutas;
---
WITH frutas AS (
    SELECT 
        column1 as fruta, 
        column2 as colores 
    FROM values 
        ('manzana', 'roja,verde,amarilla'),
        ('ciruela', 'morada,verde,amarilla')
)
SELECT 
    --*
    fruta,
    value::string as Color
FROM 
    frutas,
    LATERAL FLATTEN (split(colores, ',')) as colores_separados;

-- TODAS LAS FUNCIONES SNOWFLAKE: https://docs.snowflake.com/sql-reference/functions
