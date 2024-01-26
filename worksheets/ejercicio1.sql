-- Asignando el warehose
USE WAREHOUSE compute_wh;

-- Creando base de datos y esquema
CREATE DATABASE IF NOT EXISTS midb;
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

--- Dame los 10 productos más vendidos (que en más ventas aparecen)
SELECT 
    WS_ITEM_SK,
    count(*)
FROM 
   SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES v
GROUP BY WS_ITEM_SK
ORDER BY count(*) DESC
LIMIT 10;

SELECT APPROX_TOP_K(WS_ITEM_SK, 10)     
FROM 
   SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES v;

   -- TODO REVISAR ! No sale coherente
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

















