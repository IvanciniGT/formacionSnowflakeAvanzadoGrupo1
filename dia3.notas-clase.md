
Tengo una operación que tarda 8 segundos en una máquina que me cuesta 1 credito/hora.
Tengo una operación que me tarda 1 segundo en una máquina que cuesta 8 creditos/hora.

¿Cuál opción elegir? La segunda siempre
Me sale el mismo precio

De funcionar esto así, solo tendríamos un tipo de máquinas... las gordas. Y PUNTO !

La cuestión es que la pasta sube mucho más rápido que el rendimiento que consigo.

Una tarea que tarda 8 segundos en una máquina de 1 credito/hora... en una máquina de 8 creditos la hora ni de coña va a tardar 1 segundo.... quizás 2... 3.... por ahí andará.

> La query 5

En un entorno Medium (4 créditos/hora) tarda 1 minuto: 61 segundos 
En un entorno X-Small (1 crédito/hora) tarda:
- 3.14 minutos: 188 segundos
- 3.13 minutos: 187 segundos

En mi máquina ha tardado un tercio... pero he pagado 4 veces más.
Acabo de gastar un 33% más en la factura.

# Consideraciones:

- Voy a tener diferentes warehouses... de distintos tamaños.
- El factor TIEMPO que tengo para generar la información (realizar el trabajo) es lo que va a determinar el warehouse donde voy a ejecutar ese trabajo.

> Ejemplo. Si necesito generar un informe todas las noches... para tenerlo disponible en la mañana.
  En este caso tengo una ventana de tiempo de 5-6 horas para generar el informe.

> Ejemplo2. Necesito generar un cuadro de mando en tiempo real 

Siempre intentaré coger el warehouse más pequeño posible que me ofrezca el resultado en la ventana de tiempo que necesito.

No hay una forma de preveer el tiempo que va a tardar una consulta en un determinado tipo de warehouse... hasta que no la ejecuto. PRUEBA y ERROR.

Mi tiempo es caro también. Si una query la voy a ejecutar 5 veces, donde sea...
Si una query la ejecuto todas las mañanas... o varias (incluso cientos) de veces al día...
me lo curro un poco. Mi hora/2 horas de trabajo se amortizan en créditos.


---

# Analizando la query 57

Con que objetivo:
- Reducir tiempo de ejecución
- Reducir costes
Y no necesariamente van ligados.

Visto que un único nodo es el que consume el 75% del tiempo de procesamiento... y de ese el 98% es lectura de datos del disco... qué puedo mirar para intentar mejorar el rendimiento / costes:
- Clustering... microparticiones... qué tal funcionan?
  Scan progress:   24.16%
  Bytes scanned:   18.07GB
  (Percentage scanned from cache:  0.00%)  que a veces me ayuda y a veces no.
  Partitions scanned:   13271
  Partitions total:     54922

  Yo sé... porque conozco mi BBDD la cantidad de datos que hacen falta leer:
    SELECT count(*) FROM mi tablita de marras WHERE las condiciones que use;
        // Estoy sacando datos del 2011... y son 300k
    SELECT count(*) FROM mi tablita de marras;
        // Tengo en total 3M de datos.

        Uso el 10% de la información... Si estoy leyendo un 25%.... aquí hay mucho margen de mejora.
        Si uso el 10% de la información... y estoy leyendo el 12% de los datos... tampoco está tan mal.
    CUIDADO!
        El clustering key es para la tabla... y solo puedo poner 1... quizás una query mejore si lo cambio... pero jodo otras 100.
    
    Si cambio el clustering key... esto implicará reescribir todos los ficheros de la TABLA (las microparticiones) = PASTA
    Y además, hay un mnto constante por parte de snowflake en optimizar esas microparticiones... PASTA
    Echar cuentas y tomar decisiones. El hecho de si la query la voy a ejecutar montón de veces es el factor determinante!

    > Si consigo mejorar este punto... qué gano? qué ahorro? Ahorro tiempo por hacer menos trabajo... y eso redunda en menos pasta.

- Columnas que estoy leyendo:
  Si leo muchas más columnas de las que necesito.... más trabajo... más tiempo y más pasta.
  > De nuevo, si consigo mejorar este punto... qué gano? qué ahorro? Ahorro tiempo por hacer menos trabajo... y eso redunda en menos pasta.

    ESTO ES RARO... A no ser que esté haciendo cacicadas del tipo SELECT *... esto no ocurre.

- Que los filtros estén bien... pero eso ya es más funcional que otra cosa... si no están bien, la query va a devolver algo distinto a lo que me piden.

- Máquina más grande (tamaño del WAREHOUSE)... En este caso, ahorro tiempo, pero la cantidad de trabajo que se realiza es la misma... por lo que no ahorro pasta... Solo que puedo poner más personitas (procesos) a hacer el trabajo en paralelo ... y por eso va más rápido.... De hecho sube dinero!

    CUIDADO...  qué tengo un límite en el ahorro de tiempo...

    Tengo que leer 13.000 ficheros.
    Puedo poner a 10 leyendo en paralelo... y en teoria tardo 10 veces menos.
    El problema es que los ficheros son como páginas de un libro.
    El libro es el HDD donde tengo guardados los ficheros.
    Cuántas páginas puedo estar leyendo en paralelo de un libro... si tengo 10 personas simultaneamente leyendo del libro?

    El volumen de almacenamiento que tenga por detrás... donde de se án realmente almacenando los datos, tendrá un límite de velocidad de lectura. Esos volumenes están muy optimizados. Y esos 13000 archivos están distribuidos entre 50 HDD.
    Otro limite puede ser la conexión de RED.
---

# Vistas en cualquier BBDD

    Tabla usuarios:

    | id | nombre | apellidos |
    |----|--------|-----------|
    | 1  | Pepe   | Pérez     |
    | 2  | Juan   | García    |
    | 3  | María  | López     |
    | 4  | Ana    | Martínez  |
    | 5  | Pedro  | Sánchez   |

    Tabla de direcciones:

    | id | usuario_id | calle | numero | codigo_postal |
    |----|------------|-------|--------|---------------|
    | 1  | 1          | A     | 1      | 28001         |
    | 2  | 1          | B     | 2      | 28002         |
    | 3  | 2          | C     | 3      | 28001         |
    | 4  | 3          | D     | 4      | 28002         |
    | 5  | 3          | E     | 5      | 28001         |
    | 6  | 3          | F     | 6      | 28003         |
    | 7  | 4          | G     | 7      | 28001         |

    usuarios   -<    direcciones
    1 usuario tiene muchas direcciones

    QUERY con un JOIN:

        SELECT 
            nombre,
            apellidos,
            codigo_postal
        FROM
            usuarios
                LEFT JOIN direcciones 
                    ON usuarios.id = direcciones.usuario_id

    Saco el nombre, apellidos de todos los usuarios... y de aquellos que tengan direcciones... saco los código postales.

    | nombre | apellidos | codigo_postal |
    |--------|-----------|---------------|
    | Pepe   | Pérez     | 28001         |
    | Pepe   | Pérez     | 28002         |
    | Juan   | García    | 28001         |
    | María  | López     | 28002         |
    | María  | López     | 28001         |
    | María  | López     | 28003         |
    | Ana    | Martínez  | 28001         |
    | Pedro  | Sánchez   | NULL          |

    Si esta query la voy a hacer muchas veces, me puedo definir una vista: VIEW:
        CREATE VIEW usuarios_con_codigos_postales
        SELECT 
            nombre,
            apellidos,
            codigo_postal
        FROM
            usuarios
                LEFT JOIN direcciones 
                    ON usuarios.id = direcciones.usuario_id;

    Lo único que hemos hecho con la VIEW es dar un ALIAS (un pseudónimo) a la query.
    Esto me permite a partir de este momento escribir otras queries de forma más sencilla:

        SELECT
            codigo_postal,
            count(*)
        FROM
            usuarios_con_codigos_postales
        GROUP BY
            codigo_postal;

        | codigo_postal | count(*) |
        |---------------|----------|
        | 28001         | 4        |
        | 28002         | 2        |
        | 28003         | 1        |
        | NULL          | 1        |
    
    Hay que entender, que lo que la BBDD hace cuando yo ejecuto esa query es:

        SELECT
            codigo_postal,
            count(*)
        FROM
            (SELECT 
            nombre,
            apellidos,
            codigo_postal
        FROM
            usuarios
                LEFT JOIN direcciones 
                    ON usuarios.id = direcciones.usuario_id)
        GROUP BY
            codigo_postal;

    Las views me ayudan a simplificar las queries. PUNTO PELOTA!Ni mejoran rendimiento, ni nada de nada.... solo ayudan a simplificar.
    Realmente hay una micro-optimización... la query asociada a la vista está precompilada... y es tiempo que me ahorro... aunque este tiempo es casi despreciable.

## Vistas materializadas...
Y esto es un mundo aparte.

    CREATE MATERIALIZED VIEW usuarios_con_codigos_postales
        SELECT 
            nombre,
            apellidos,
            codigo_postal
        FROM
            usuarios
                LEFT JOIN direcciones 
                    ON usuarios.id = direcciones.usuario_id;

Esto funciona muy distinto a una VIEW normal.
Cuando hago la creación de la vista, en ese momento se ejecuta la query... y el resultado se guarda en disco... se persiste (se materializa).
Cuando ahora uso esta vista dentro de otra query, estamos leyendo los datos que están persistidos.
No se reejecuta la query.

Evidentemente, esto va mucho más rápido en muchos escenarios... pero:
- Cuidado: Puedo estar leyendo datos desactualizados
- VA a haber una sobrecarga en el sistema para intentar mantener esa vista materializada actualizada.

Depende del motor de BBDD que use, se admiten distintas configuraciones de refresco de la vista materializada:
- Como parte de una transacción de INSERT / UPDATE / DELETE fuerzan la actualización de la vista materializada:
  - Los datos en la VM están siempre actualizados... pero esas operaciones van a ir más lentas.
- Cada X tiempo se actualiza la vista materializada:
  - En este caso, no estoy viendo los últimos datos... puede que no me importe... pero puede que sí.

La ventaja es que las BBDD tienen mecanismos que nos ayudan a mantener actualizadas esas materializaciones... yo me desentiendo de ello.

---

Otra cosa distinta es guardar los resultados de una query en una tabla.

    CREATE TABLE usuarios_con_codigos_postales AS 
        SELECT 
            nombre,
            apellidos,
            codigo_postal
        FROM
            usuarios
                LEFT JOIN direcciones 
                    ON usuarios.id = direcciones.usuario_id;

Eso si.. aquí no hay mecanismos de actualización ofrecidos por la BBDD.

    El día 1 de enero de 2024: CREATE TABLE usuarios_con_codigos_postales_1_enero_2024 AS...;
    El día 4 de enero de 2024: CREATE TABLE usuarios_con_codigos_postales_4_enero_2024 AS...;
    El día 1 de febrero de 2024: CREATE TABLE usuarios_con_codigos_postales_1_febrero_2024 AS...;
    El día 4 de enero de 2024: CREATE TABLE usuarios_con_codigos_postales_4_febrero_2024 AS...;
    El día 1 de marzo de 2024: CREATE TABLE usuarios_con_codigos_postales_1_marzo_2024 AS...;
    
    Son tablas que no quiero que se actualicen.

    CREATE VIEW codigo_postales_del_2023 AS
        SELECT * FROM usuarios_con_codigos_postales_1_enero_2023
        UNION ALL
        SELECT * FROM usuarios_con_codigos_postales_1_febrero_2023
        UNION ALL
        SELECT * FROM usuarios_con_codigos_postales_1_marzo_2023

    Estas tablas son las típicas candidatas a ser definidas en Snowflake como TRANSIENT TABLES.
    Persistencia, pero de la barata... que si hay una catástrofe... no me importa perderlas....
    las regenero... y no estoy pagando almacenamiento del caro por si acaso.

    Tengo cientos de queries que ejecuto sobre eso... para generar distintos informes.
        CREATE OR REPLACE VIEW codigos_postales_ultimo AS 
        select * from usuarios_con_codigos_postales_1_enero_2024;
    Y todos los informes trabajan contra esta view...
    El mes que viene, lo que hago es:
        CREATE OR REPLACE VIEW codigos_postales_ultimo AS 
        select * from usuarios_con_codigos_postales_1_febrero_2024;

    Y los informes trabajan en cada momento con la última versión de la tabla.

    Ni de coña querría esto con vistas materializadas...
    El segundo mes, pierdo los datos del primero... y si luego quiero hacer una comparativa o lo que sea... a reconsultar otra vez.

Otra cosa sería montar una vista materializada....
Eso serviría para montar un cuadro de mando en tiempo "real"... que se actualice cada 5 minutos... 
Aquí si entra una vista materializada.- SOLO QUIERO 1 tabla de valores... los que haya vigentes ahora.... y que se encargue la BBDD de su mnto.

---
Y ahora llegan las rebajas.... con Snowflake...

# Snowflake y SUS vistas materializadas

Y es que snowflake tiene un huevo y medio de limitaciones en las vistas materializadas:
- Las vistas materializadas en SF SOLO pueden consultar una UNICA TABLA! (PRIMER OSTION !)
  - NI UN JOIN... ni con la misma tabla... 
    - DEPENDE del tipo de modelo de BBDD que tenga.... y de la operación que haga
      esto no es una limitación tan grande como parece:
        En los modelos en estrella, es algo habitual usar vistas materializadas para las tablas de hechos... calculando agregados
- No pueden hacer suo de otras vistas (ni materializadas ni no materializadas)
- No pueden incluir order by
- Ni limit
- Los group by solo sobre campos que estén en el SELECT
- MINUS, EXCEPT, INTERSECT no se pueden usar
- No pueden hacer uso de funciones no deterministas ( now(), CURRENT_TIMESTAMP, etc...)
---
Imaginad que tengo una tabla de ventas... cuál sería el clustering key que voy a usar?
    Un campo que voy a usar en TODAS LAS QUERIES QUE HAGA ? FECHA (tablas de hechos)
    Imaginad que tengo 100.000.000 de productos... que he vendido uniformemente... en 10 años.
    El clustering lo hago por día... y tengo 3650 microparticiones... 
    y cada una de ellas tiene 100.000.000 / 3650 = 27.397 registros.

    Ahora digo... tengo 100 tiendas donde vendo... voy a usar también este dato como clustering key... los 2.
    Ahora tengo 27397 registros por día.... por dia y tienda tengo: 27397 / 100 = 273 registros.

    Una duda que a veces nos ocurre es que pensamos que para cada grupúsculo vamos a tener 1 archivo.... y eso no es así.
    Snowflake no va a crear: 100.000.000/273 = 366k ficheros de 273 registros cada uno.

    Snowflake a a crear ficheros... que tengan 100k-200k datos más o menos (esto depende del tamaño de cada registro)... pero por ahí anda la cosa.
    
    Lo que tratará de asegurarme snowflake es que dentro de un fichero tenga todos los datos de un grupúsculo....
    Pero dentro de un fichero puede haber datos de 50 grupúsculos diferentes...


---


Productos
nombre
codigo
color

Ventas
codigo_producto
fecha
importe
cantidad


SELECT 
    Productos.codigo,
    Productos.nombre,
    Productos.color,
    sum(Ventas.importe) as importe_total,
    sum(Ventas.cantidad) as cantidad_total
FROM 
    Productos,
    Ventas
WHERE 
    Productos.codigo = Ventas.codigo_producto
GROUP BY
    Productos.codigo,
    Productos.nombre,
    Productos.color

---

SELECT 
    Productos.codigo,
    max(Productos.nombre),
    max(Productos.color),
    sum(Ventas.importe) as importe_total,
    sum(Ventas.cantidad) as cantidad_total
FROM 
    Productos,
    Ventas
WHERE 
    Productos.codigo = Ventas.codigo_producto
GROUP BY
    Productos.codigo

Al hacer el groupby se ordena por todos los campos


| Producto | Nombre | Color | Importe | Cantidad |  MAX(nombre)  | MAX(color) | Sum(importe) | Sum(cantidad) |
|----------|--------|-------|---------|----------|
| 1        | A      | Rojo  | 100     | 10       | < A           |   Rojo     | 100          | 10            |
| 1        | A      | Rojo  | 200     | 20       | 
| 1        | A      | Rojo  | 300     | 30       |
| 2        | B      | Azul  | 400     | 40       |
| 2        | B      | Azul  | 500     | 50       |
| 2        | B      | Rojo  | 600     | 60       |
| 3        | C      | Rojo  | 700     | 70       |

El group by primero hace el order by y después el fullscan

---
# Modelos en estrella

Tengo tablas de hechos y tablas de dimensiones.
En el centro tengo las tablas de hechos, que guardan relaciones N/1 con las tablas de dimensiones.

En qué se diferencian estos modelos a los esquemas que usamos en las bbdd de producción?
- Las tablas de dimensiones se desnormalizan y enriquecen
- Algunas columnas de las tablas de hechos las normalizamos

--- 

## App para un web online de venta de productos

Clientes
    id
    nombre
    apellidos
Direcciones
    id
    cliente_id
    calle
    numero
    codigo_postal
    ciudad
    provincia
    pais
Productos
    id
    nombre
    precio
    categoria_id
Categoria
    id
    nombre
Ventas
    id
    cliente_id
    producto_id
    fecha
    cantidad
    importe

            Direcciones >- Clientes -< Ventas >- Productos -< Categoria

Es óptimo para un entorno de producción? para la web de la tienda?
- Puedo acceder a cada dato de forma individual... No están replicados. HACER INSERT, UPDATES, DELETES DE FORMA MUY EFICIENTE

Para informes es un buen modelo?
- Los modelos o se optimizan para actualizaciones de datos, o se optimizan para consultas. Un modelo no puede estar optimizado para ambas. En los entornos de producción buscamos modelos optimizados para actualizaciones de datos.
- Por contra en los entornos de BI buscamos modelos optimizados para consultas.

Para pasar de un modelo a otro, lo primero es identificar las tablas de hechos, las de dimensiones.
Las tablas de dimensiones son las que utilizaré para : FILTROS, AGRUPACIONES.... a la hora de analizar los hechos.

TABLAS DE HECHOS: 
    Ventas
        id
        cliente_id
        direccion_id
        producto_id
        fecha_id
        cantidad
        importe
        categoria_id

TABLAS DE DIMENSIONES: Son los datos por los que quiero hacer un análisis

    Fechas:
        id
        fecha
        mes
        dia
        año
        dia de la semana LUNES O SABADO
        findesemana SI
        número de dia del año
        trimestre
        semestre

    Clientes (depende del escenario... potencialmente tabla candidata a su eliminación)
        id
        
    Direcciones
        id
        (cliente_id) A tomar por culo!
        calle
        numero
        codigo_postal
        ciudad
        provincia
        pais

    Productos
        id
        nombre
        precio

    Categoria
        id
        nombre

BBDD producción -> misma estructura -> datalake -> transformación para unos objetivos -> data warehouse
                    ESTO ES RAPIDO


                                       Fechas  
                                         |
                                         ^
                        Categorias -< Ventas >- Productos
                                         v
                                         |
                                    Direcciones

En el caso de snowflake.... Qué clustering keys utilizo en cada tabla?
- Fechas
  Aquí es donde tendría gracia, meter las fechas ya ordenadas.... y usar el ID de la fecha como clustering key.
    (Año / Mes / ID) 
    Alter table Productos CLUSTER BY (Año, Mes, ID);
    Tiene sentido meter el ID? Solo tengo 1? En cambio, si meto solo 1, sería este.
    Si meto el año, el mes y el día... tengo 3 clustering keys... y no me sirve de nada... para la agrupación... pero snowflake tiene en cuenta el ORDEN de los IDS

        Si tengo un fichero... y tengo que meter 100 días en ese fichero... voy a meter 100 días consecutivos
        Cuando haga búsquedas por RANGO, los tendré juntos.
- Productos
     (ID)... teniendo el ID como clustering key, me aseguro que snowflake sabe en qué micro-partición está cada producto.
- Categorias
    (ID)... teniendo el ID como clustering key, me aseguro que snowflake sabe en qué micro-partición está cada categoría.
- Direcciones
    (ID)... teniendo el ID como clustering key, me aseguro que snowflake sabe en qué micro-partición está cada dirección.
    - Ventas 
    (Fechas)