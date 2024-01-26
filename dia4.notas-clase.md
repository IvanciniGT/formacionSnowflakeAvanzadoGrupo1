
Hemos estado hablando con respecto a mejorar el rendimiento (/ costes):
- Modelos en estrella
- Técnicas dentro de snowflake: Clustering keys, materialized views, etc.
- Consultas:
  - JOINS FUERA !
  - FILTROS WHERE...
    - Lo antes posible
    - Nunca usar nada que no sea una columna pura en un where
        - a = 
        - a > 
        - a < 
    - Nada de funciones
      - MONTH(FECHA) = 1    RUINA
      - LIKE '%A%'          RUINA PROFUNDA
      - OPERADORES + - * /  RUINA
  - Columnas ... solo las que necesito...
  - Las columnas de etiquetas al final !
  - Limitar el uso de estas palabras a los casos estrictamente necesarios:
      - LEFT JOIN
      - RIGHT JOIN
        - OUTER JOIN
      - ORDER BY
      - GROUP BY
      - DISTINCT
      - UNION -> UNION ALL (que no hace distinct)
      - Subqueries
        En muchos casos se pueden sustituir por JOINS (normalmente el motor de BBDD identifica estos casos y los optimiza)... PERO OJO CON SNOWFLAKE...
            - Lo hace SOLO si las restricciones de integrad referencial están bien definidas.
  - Usar las funciones GUAYS que tiene propias el SNOWFLAKE
      - QUALIFY (para meter filtros en funciones de ventana)... sin necesidad de subqueries
      - COUNT(DISTINCT )
        SELECT 
            COUNT (DISTINCT v.ws_sold_date_sk) as dias_de_venta
        FROM 
        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES v;
            - APPROX_COUNT_DISTINCT (para hacerlo más rápido) Análisis exploratorios
      - --- Dame los 10 productos más vendidos (que en más ventas aparecen)
            SELECT 
                WS_ITEM_SK,
                count(WS_ITEM_SK)
            FROM 
            SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES v
            GROUP BY WS_ITEM_SK
            ORDER BY count(WS_ITEM_SK) DESC
            LIMIT 10;

            APPROX_TOP_K
                APPROX_TOP_K_ACCUMULATE
      - count_if
        - Me permite en una única query hacer un montón de cuentas, diferentes entre si, basadas en una condición. Condición que habitualmente metemos en un WHERE
      - iif
        - Sería un case típico pero con SI / NO en función de una condición


# BBDD NoSQL

Si no fueran SQL, en inglés NO ser diría NOT
No se llaman NOTSQL
Se llaman NoSQL... y viene el nombre de Not Only SQL

Vamos a tener:
- Muchos datos en el datalake, cada día más en formatos como JSON (MONGO, EVENTOS)
 - Tenemos muchas funciones dentro de Snowflake para trabajar con JSON
- Muchas funciones que trabajan con SQL y datos muy estructurados, producen en Snowflake datos en JSON... y vamos a tener que manipularlos.