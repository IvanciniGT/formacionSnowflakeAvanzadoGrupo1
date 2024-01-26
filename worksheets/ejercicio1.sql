-- Asignando el warehose
USE WAREHOUSE compute_wh;

-- Creando base de datos y esquema
CREATE DATABASE IF NOT EXISTS midb;
CREATE SCHEMA IF NOT EXISTS mies;
USE SCHEMA midb.mies;

-- Me creo la tabla con la estructura adecuada
CREATE TABLE midb.mies.clientes AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER LIMIT 0;
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





