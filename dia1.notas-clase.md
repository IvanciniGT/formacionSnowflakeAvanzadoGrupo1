# Snowflake. Repaso rápido

## ¿Qué es Snowflake?

Snowflake es un servicio de almacenamiento/gestión del dato de datos en la nube. 

Es un servicio de pago, pero ofrece una versión gratuita para que podamos probarlo. En este tutorial vamos a ver cómo crear una cuenta gratuita y cómo usarla para almacenar datos.

La gracia de snowflake está en su arquitectura.

Una plataforma en la que la flexibilidad, escalabilidad y rendimiento están garantizados / son muy sencillos de realizar.

## Diseño:

Tenemos varias capas independientes entre si (se apoyan una en la otra):
- Capa de almacenamiento de datos
    Trabajar tanto con datos estructurados (datos guardables en una BBDD Relacional) como no estructurados (archivos de texto, json, imágenes):
    - JSON
    - XML
    - AVRO
    - **PARQUET**
- Capa de cómputo / procesamiento
  - Warehouse
- Capa de Servicios

---

# Almacenamiento de la información dentro de Snowflake

## Repaso de cómo las BBDD relacionales almacenan la información.

Al final los datos se guardan en un fichero... con formato binario.

### Ejemplo: Tabla usuarios

| id | nombre | apellidos | edad |
|----|--------|-----------|------|
| 1  | Pepe   | Pérez     | 25   |
| 2  | Juan   | García    | 30   |
| 3  | María  | López     | 28   |

Esta información se guarda en el fichero... en binario... de forma secuencial.

Crear la tabla... y al crear la tabla, indicamos el tipo de dato de cada columna (Schema)

```sql
CREATE TABLE usuarios (
    id INT,
    nombre VARCHAR(50),
    apellidos VARCHAR(50),
    edad INT
);
```

La información dentro del fichero binario se guarda de forma secuencial.... orientada a filas:

BYTE 0 - Id de Pepe... y ocupa 4 bytes (INT) 00011010001010111001010010010010
BYTE 4 - Nombre de Pepe... y ocupa 50 bytes (VARCHAR) 01010100000....110010101010
BYTE 54 - Apellidos de Pepe... y ocupa 50 bytes (VARCHAR) 01010100000....110010101010
BYTE 104 - Edad de Pepe... y ocupa 4 bytes (INT) 00011010001010111001010010010010
ACABA conceptualmente la fila 1 de la tabla... pero el fichero sigue
BYTE 108 - Id de Juan... y ocupa 4 bytes (INT) 00011010001010111001010010010010
....

Cada fila ocupa 108 bytes... realmente algo más.-.. luego la BBDD usa algunos bytes adicionales para información propia (si un registro está bloqueado... si ha sido eliminado... etc)

--->1PepePérez252JuanGarcía303MaríaLópez28

Cuando necesitamos acceder a un dato... recuperar una fila.... la BBDD, si conoce qué fila es, rápidamente puede poner la aguja del HDD en el byte adecuado y leer la información necesaria de disco. 
Quiero el dato de la segunda fila:
- PASO 1. pongo la aguja en el byte: 108 * (n-1)
- PASO 2: De ahí leo 108 bytes y lo devuelvo al usuario.

En ocasiones queremos recuperar más de una fila (registro).... o sacar una fila que cumpla unas determinadas condiciones.

Imaginad que quiero sacar las personas cuya edad es mayor a 26 años.

    | id | nombre | apellidos | edad |
    |----|--------|-----------|------|
    | 1  | Pepe   | Pérez     | 25   |
    | 2  | Juan   | García    | 30   |
    | 3  | María  | López     | 28   |

La BBDD hace una operación denominada FULLSCAN.
Va leyendo todas y cada una de las filas de la tabla... y comprueba si la edad es mayor a 26 años.
De hecho, la BBDD sabe en que posición está la edad de cada fila: 104*(n)+4*(n-1) .

Esto es un desastre desde un punto de vista computacional.

### Formas de optimizar este trabajo en las BBDD relacionales:

Me creo un índice... para que la BBDD pueda hacer una búsqueda binaria.

1.000.000... Si parto a la mitad:
- 500.000... Si parto a la mitad:
- 250.000... Si parto a la mitad:
- 125.000... Si parto a la mitad:
- 62.500... Si parto a la mitad:
- 31.250... Si parto a la mitad:
- 15.625... Si parto a la mitad:
- 7.812... Si parto a la mitad:
- 3.906... Si parto a la mitad:
- 1.953... Si parto a la mitad:
- 976... Si parto a la mitad:
- 488... Si parto a la mitad:
- 244... Si parto a la mitad:
- 122... Si parto a la mitad:
- 61... Si parto a la mitad:
- 30... Si parto a la mitad:
- 15... Si parto a la mitad:
- 7... Si parto a la mitad:
- 3... Si parto a la mitad:
- 1... Si parto a la mitad: En 20 operaciones consigo sacar el dato que me interesa... como mucho.

El truco es que para poder hacer este tipo de búsqueda... los datos tienen que estar ORDENADOS.
    Si realmente tuviera los datos ordenados como en un diccionario... y conociera la distribución de los datos en el diccionario... podría optimizar el primer corte que hago.... ESTADISTICAS DE LA TABLA.

En mi caso, los datos no están ordenados por edad.... por lo que a priori, no puedo hacer una búsqueda binaria.
- Podría ordenarlos primero.... qué tal se le da a los ordenadores ORDENAR datos? Como el culo.
  - Esto sería un desastre... Ya que la ordenación tardaría más que el FULLSCAN!
Lo que hacemos en una BBDD si quiero optimizar este tipo de consultas es crear un índice.

Un índice es una copia de los datos de la tabla... pero ordenados por la columna que yo quiera.... en la que además se incluye información de su ubicación.
REQUIERE DE ESPACIO EN DISCO.

    Indice por edad:
    INFO  UBICACION
    25 -> 1
    28 -> 3
    30 -> 2

    Indice por nombre:
    Juan -> 2
    María -> 3
    Pepe -> 1

    Indice por apellidos:
    García -> 2
    López -> 3
    Pérez -> 1

Los índices funcionan guay! ... pero.... tienen sus cosillas:
- Necesito de espacio en disco para guardarlos.... estoy DUPLICANDO LA INFORMACIÓN a nivel del disco.
  Y OJO: MUCHO MAS ESPACIO EN DISCO 
    Tengo 1M de datos... de los que guardo la edad... La edad ocupa 4 bytes... y tengo 1M de datos... 4MB
    Además quiero guardar la ubicación... que sería otros 4 bytes: +4MB
    El índice podríamos pensar que va a ocupar 8Mbs... pero en la práctica... quizás me ocupe: 32Mbs

    Se reserva espacio que se deja en blanco para futuras inserciones/actualizaciones de datos.
        Al final los datos se guardan en un fichero... que podéis imaginarlo como una hoja de papel.
        Yo quiero los datos ORDENADOS... Los nuevos:
        - O los añado al final (JODO LA ORDENACION)
        - Dejo huecos... para ellos... y los voy colocando en su sitio.
- Requieren de un mantenimiento fuerte:
  De cuando en cuando, me quedo si espacio en el índice... y tengo que regenerarlo... Reescribir el fichero... dejando más blancos. 

No obstante... a pesar de estas cosillas, los índices han sido la solución tradicional en las BBDD Relacionales para hacer más eficientes las búsquedas.

SNOWFLAKE NO HACE NADA NI PARECIDO A ESTO !

## Almacenamiento de datos en Snowflake

Snowflake también usa archivos binarios para guardar la información.
En general cualquier sistema de almacenamiento de información usa archivos binarios... por qué? Son más eficientes en cuanto a lo que ocupan los datos.,.. y por ende, además de ahorrar espacio, necesitamos leer y escribir menos información a disco (va en favor del rendimiento)

Imaginad que quiero guardar un DNI en una BBDD (español):
- OPCION 1: Campo tipo VARCHAR(9) ---> Cuanto ocupa esto en disco?
    Depende del juego de caracteres que use mi BBDD (collate: UTF-8, ISO-8859-1...). Hoy en día, un estándar es UTF-8.
        UTF: Unicode Transformation Format
        Unicode: es un estándar que define un mapa de caracteres... que contiene todos los caracteres (o casi todos) que usa la humanidad: Tiene más de 150.000 caracteres:
            1 byte -> 256 caracteres
            2 bytes -> 65.536 caracteres
            4 bytes -> 4.294.967.296 caracteres
        Depende del caracter y de la transformación concreta, se usan más o menos bytes para representar cada caracter.

        En UTF-32, se usan 4 bytes (32 bbits) para representar cada caracter.
        En UTF-16, se usan de 2 a 4 bytes (16 bits) para representar cada caracter... los normalitos (habituales) ocupan 2 bytes.
                                                                                      los más "raros" ocupan 4 bytes: Emojis... caracteres chinos, japoneses, etc.
        En UTF-8, se usan de 1 a 4 bytes para cada caracter... Los más habituales (ASCII), ocupa 1 byte.
                                                               Los acentuados... ocupan 2 bytes.
                                                               Los más raros, ocupan 4 bytes.
        Ahora si... un DNI (que tiene caracteres simplones: 0-9 y una letra pela'), ocupa 9 bytes.


    En un entorno de producción cada dato es almacenado al menos en 3 copias... por si acaso.
    Con copias de seguridad... para guardar 1Tb de información, necesito 7/8 teras... de los caros... no de los western blue (GOLD... red pro)

- OPCION 2: Separar número y letra
  Números hasta el: 99.999.999  -> 
    1 byte: 256 diferentes
    2 bytes: 65.536 diferentes
    3 bytes: 16.777.216 diferentes
    4 bytes: 4.294.967.296 diferentes
  Letra: 1 byte

  En total necesito 5 bytes para guardar un DNI.

  Que en un entorno de producción si tengo 1M de datos: 5Mb x 3 copias = 15Mb x backups = 30Mb
  Mientras que en la opción 1:                          9Mb x 3 copias = 27Mb x backups = 54Mb... y disco del caro!

- OPCION 3: Guardar solo en número. La letra se calcula del número (es una huella del número que solo sirve para verificar que el número es correcto... o tratar de verificarlo)
- En este caso, un DNI ocupa solo 4 bytes -> Copias de seguridad... redunda en 12Mb x backups = 24Mb


La opción 1... descartada!
Entre la 2 y la 3...
    - La 2 me permite salvar capacidad de cómputo (no hay que regenerar la letra... la tengo guardada), a costa de más pasta en almacenamiento.
    - La 1 me permite ahorrar pasta en almacenamiento, a costa de más cómputo (tengo que calcular la letra cada vez que la necesite)
- Qué me cuesta más pasta? Y DECIDO!

Las BBDD usan formatos binarios propietarios...
En el mundo bigdata, se usan formatos abiertos (no propietarios) para guardar información persistente... que pueda ser posteriormente procesada... consultada... transmitida... por muchos programas distintos:
- AVRO
- PARQUET
Ambos 2 son formatos binarios (que permiten almacenar más información ocupando menos espacio... y por ende leer / escribir / transmitir por red datos más rápido)
Pero hay una diferencia MUY GRANDE ENTRE ELLOS:
- AVRO es un formato orientado a FILAS (como los ficheros de las BBDD relacionales)
- PARQUET es un formato orientado a COLUMNAS (como los ficheros de Snowflake)

La decisión entre uno y otro depende del USO que voy a hacer del dato.
Si quiero ser capaz de procesar el registro de una persona, de forma independiente al resto de registros... AVRO es mi opción.

    Cargamos datos desde KAFKA
    Qué es KAFKA? Un sistema de mensajería. (el whatsapp lo uso todos los días)
    Cada dato que yo guardo en ese sistema de mensajería quiero que pueda ser procesado de forma independiente al resto de datos.
    Me interesa AVRO para almacenar los datos dentro del sistema de mensajería.

Si quiero ser capaz de procesar la información de una columna de forma independiente al resto de columnas... PARQUET es mi opción.

    Quiero un informe de las ventas por comunidad autónoma (quiero la columna ventas y la columna comunidad autónoma... de entre las 20-100 columnas que tengo en el fichero)

    > En una tabla de una BBDD Relacional que haya que leer de disco:
    
        | id | comercial | fecha | **comunidad autónoma** | **ventas** |
        |----|-----------|-------|------------------------|------------|
        | 1  | Pepe      | 2020  | **Madrid**             | **100**    |
        | 2  | Juan      | 2020  | **Madrid**             | **200**    |
        | 3  | María     | 2020  | **Madrid**             | **300**    |
        | 4  | Pepe      | 2020  | **Barcelona**          | **400**    |
        | 5  | Juan      | 2020  | **Barcelona**          | **500**    |
        | 6  | María     | 2020  | **Barcelona**          | **600**    |

    > Me sería mucho más eficiente tener los datos almacenados por columnas
        id: 1,2,3,4,5,6
        comercial: Pepe, Juan, María, Pepe, Juan, María
        fecha: 2020, 2020, 2020, 2020, 2020, 2020
        **comunidad autónoma: Madrid, Madrid, Madrid, Barcelona, Barcelona, Barcelona**
        **ventas: 100, 200, 300, 400, 500, 600**

        En formato parquet, el fichero tiene un encabezado con metadatos:
        - Número de filas
        - Número de columnas
        - Tipo de dato de cada columna
        - Donde empieza cada columna

Snowflake, además, me ofrece 2 "funcionalidades" extra:
- fail-safe: Capacidad de recuperación ante fallos (nivel de redundancia/copias de seguridad)
- time-travel: Capacidad de recuperar información de un momento anterior en el tiempo.

Sus usos son diferentes:
- fail-safe: Es una funcionalidad que se usa en entornos de producción... donde la información es crítica... y no puede perderse.
             No es una funcionalidad al alcance de los usuarios de snowflake... Yo pido fail-safe... y me lo dan... y eso implica que si hay un problema con los datos... ELLOS pueden recuperarlos (no es algo que yo hago) 
- time-travel: Es una funcionalidad a disposición de los usuarios. Yo puedo ir en un momento dado a la vista que tenía de la información hace 2 semanas... y verla... o incluso recuperarla.

# Formatos columnares

Los formatos orientados a columnas no son adecuados para BBDD de producción, donde las aplicaciones van modificando continuamente datos de un registro.
    - Si tengo que modificar un dato de un registro... tengo que modificar todo el fichero... y eso es muy costoso.
    - Si tengo que añadir un registro... tengo que añadirlo al final de cada columna... y eso es muy costoso.
    - Si tengo que eliminar un registro... tengo que eliminarlo del fichero... y eso es muy costoso.

En las BBDD de los sistemas en producción nos interesan formatos orientados a filas... que permitan la actualización/alta/borrado de un registro de forma eficiente.

Estos formatos orientados a columnas van orientados a Datalakes y dataWarehouses, donde:
- No vamos a estar haciendo modificaciones de datos concretos.
- Lo que vamos a estar haciendo es analizando la información... y para ello, nos interesa poder acceder a una columna de forma independiente al resto de columnas.

El problema de esta solución... nos viene en su incapacidad para tener/definir INDICES tal y como lo hacemos en una BBDD relacional.


    > En una tabla de una BBDD Relacional que haya que leer de disco:
    
        | id | comercial | fecha | **comunidad autónoma** | **ventas** |
        |----|-----------|-------|------------------------|------------|
        | 1  | Pepe      | 2020  | **Madrid**             | **100**    |
        | 2  | Juan      | 2020  | **Madrid**             | **200**    |
        | 3  | María     | 2020  | **Madrid**             | **300**    |
        | 4  | Pepe      | 2020  | **Barcelona**          | **400**    |
        | 5  | Juan      | 2020  | **Barcelona**          | **500**    |
        | 6  | María     | 2020  | **Barcelona**          | **600**    |


    INDICE POR COMUNIDAD AUTONOMA:
        Madrid -> 1,2,3
        Barcelona -> 4,5,6
        Con esas ubicaciones, yo puedo calcular directamente en qué byte del fichero está la información que me interesa... y no tengo que hacer un FULLSCAN.

    > Me sería mucho más eficiente tener los datos almacenados por columnas
        id: 1,2,3,4,5,6
        comercial: Pepe, Juan, María, Pepe, Juan, María
        fecha: 2020, 2020, 2020, 2020, 2020, 2020
        **comunidad autónoma: Madrid, Madrid, Madrid, Barcelona, Barcelona, Barcelona**
        **ventas: 100, 200, 300, 400, 500, 600**
        detalle: [{"producto": 18274, "cantidad": 17}, {"producto": 111, "cantidad":19}], ....

    INDICE POR COMUNIDAD AUTONOMA:
        Madrid -> 1,2,3
        Barcelona -> 4,5,6
        Con esas ubicaciones, no puedo calcular directamente en qué byte del fichero está la información que me interesa... por qué?
        - Primero... la información está distribuida en varias zonas del fichero
        - Pero además, en este tipo de formatos, que además están pensados para almacenar tanto información estructurada como no estructurada (JSON), y que están también pensados para optimizar el espacio de almacenamiento:
          - Los datos no ocupan un espacio prefijado... sino que ocupan el espacio que necesitan:
            - Edad siempre ocupará 4 bytes... por ser un número...
            - Pero el nombre.. si es más corto ocupará menos... y si es más largo ocupará más
          - No hay manera de saber en que byte empieza un determinado dato.... los INDICES AQUI NO APORTAN NADA.

Problema... y si necesito empezar a hacer búsquedas:
WHERE año = 2023
WHERE comunidad autónoma = "Madrid"
WHERE ventas > 500

Entonces? Si no hay índices... y por ende no puedo hacer búsquedas binarias? Qué me queda? El FULLSCAN... que es lo que quería evitar con los índices en las BBDD relacionales.
- Y NO HAY ALTERNATIVA !
Al hacer una query en Snowflake, siempre vamos a hacer un fullscan!

Esto no es un decir... es la CRUDA REALIDAD de SnowFlake..
Ahora bien... cómo encaja esto, con que en la web... y en las publicaciones y por todos lados me hablen del buen rendimiento de Snowflake?

Lo que me toca, para poder hacer queries que funcionen más o menos rapidito... es:
1. Entender muy bien cómo funciona snowflake
2. Partirme la cabeza... y de paso los datos... para que las queries sean lo más eficientes posibles (usando fullscans):
    - Estrategia 1: Preparar los datos de forma que la respuesta a mis consultas sea lo más directa posible.
    - Estrategia 2: Entender el concepto de MICRO-PARTICIONADO en snowflake... y hacer un buen uso de él.

## Microparticionado de los datos:

En snowflake, es cierto que los datos se guardan en un formato columnar... y que no hay nada ni parecido a los índices de las BBDD relacionales... pero... lo que no os había dicho es que los datos de una tabla no se guardan en un único fichero, sino que se guardan en muchos ficheros... independientes entre si: MICROPARTICIONES.
Son ficheros que en la práctica tienen una 15-50 Mbs de datos (comprimidos)
Se gestionan por snowflake... aunque puedo INFLUIR en cómo snowflake va a particionar los datos (repartirlos entre esos ficheros) 

Mi objetivo, conseguir que los datos que necesito para una query CONCRETA que vaya a ejecutar están juntos... para que el fullscan vaya más rápido.

De cara a usar esto, lo que vamos a pedir a snowflake es que agrupe datos (cluster) en base a unas columnas concretas (columnas de agrupación)

```snowflake

CREATE DATABASE curso;
USE DATABASE curso;

CREATE OR REPLACE TABLE ventas (
    id INT,
    comercial STRING,
    fecha DATE,
    comunidad STRING,
    ventas INT
);

ALTER TABLE ventas CLUSTER BY (comunidad, fecha);
```

Esto es algo que podré cambiar a futuro.... pero cuidado... que si lo cambio... los datos se van a reorganizar a nivel de HDD... y eso es un proceso costoso.

En snowflake, esto recibe el nombre de CLUSTERING KEYs: Son las columnas que usamos para agrupar los datos en las microparticiones.

En la mayor parte de escenarios lo que nos interesa es preparar los datos para usos concretos:
- TAL CUADRO DE MANDO
  - Tabla1
  - Tabla2
  - Tabla3
  - Tabla4


No voy a estar trabajando contra el datalake... Sino contra el data warehouse... más datawarehouse que nunca.

Evidentemente esto implica más consumo en capa de almacenamiento... pero es que en snowflake, el almacenamiento es barato... y el cómputo es caro.


Ahí vamos atener varias cosas con las que trabajar:
- Distintos tipos de tablas en Snowflake:
  - Permanent tables (las tablas más habituales para tener información persistente) > Data Lake... y quizás también algunas tablas del Data Warehouse. Son las más caras: fail-safe + time-travel
    Las usamos para datos que requieren alta durabilidad y disponibilidad en el tiempo.

  - Transient tables (tablas transitorias) > Data Warehouse. 
    Son más baratas: no fail-safe + si time-travel

  - Temporary tables (tablas temporales) > Data Warehouse.
    Son las más baratas: no fail-safe + no time-travel
    Se usan para procesos que requieren de varias queries sobre la misma base de información... que habremos prefiltrado/preprocesado.... y que no necesitan persistencia en el tiempo.
    Se crean en el momento en el que se necesitan... y están vivas solo durante el tiempo que dura la sesión de trabajo del usuario que las ha creado. Se eliminan automáticamente al finalizar la sesión.
    Además solo son visibles para el usuario que las ha creado.                     <<<<---------->>>> tablas en memoria

- Distintos tipos de vistas en Snowflake:
  Las vistas, al igual que en las bbdd relacionales, me permiten simplificar el acceso y gestión de los datos que tengo.
  - Regular views: Vistas tradicionales. Básicamente me permiten referirme a una query con un nombre más compacto.
    
    CREATE VIEW ventas2024 as SELECT comunidad, ventas FROM ventas WHERE fecha = '2024';

    SELECT comunidad, sum(ventas) FROM ventas2024 GROUP BY comunidad;

    Estas vistas, al igual que ocurre en la BBDD no están precalculadas... sino que se calculan en el momento en el que se ejecuta la query.

    Usos típicos: 
    - Simplicar la escritura de queries complejas
    - Ocultar detalles de implementación (que tablas se usan, que joins, etc)... o el acceso a ciertas columnas.

    NO VAMOS A TENER NINGUN TIPO DE MEJORA EN EL RENDIMIENTO DE LAS QUERIES por usar este tipo de vistas.

  - Materialized Views
    Son similares a las vistas tradicionales... pero:
        Al crearse, se ejecuta la query... y se guarda el resultado de forma persistente. (es como otra tabla física que me creo)
        Problema... los datos se congelan en ese momento... y si hay cambios en las tablas que usa la query de la vista ... no se reflejan en la vista materializada.

    VOY A TENER UNA MEJORA ENORME EN EL RENDIMIENTO DE LAS CIERTAS QUERIES por usar este tipo de vistas.

    Podremos configurar trabajos que vayan actualizando en ciertos momentos estas vistas materializadas.


  - External tables (tablas externas)
    no fail-safe + no time-travel
    Se usan para acceder a datos que están en el datalake... pero que no queremos traer a snowflake... sino que queremos acceder a ellos desde snowflake.
---


## Términos:

Data Lake               Almacén de datos en bruto. Donde tengo datos, que almaceno según me vienen (un poco los transformo al hacer una
                        ingesta)
Data Warehouse          En él guardamos datos ya estructurados PARA UN PROPOSITO CONCRETO. Es decir, ya están transformados y listos para
                        ser usados. Es un almacén de datos para un propósito concreto.
Business Intelligence   Representar / Analizar el dato (SUPERFICIAL)... para sacar conclusiones que aporten valor desde el punto de
                        vista de negocio. ---> Data Warehouse
DataMining              Análisis más profundo de la información (donde ni siquiera sé lo que estoy buscando)
                        ---> Data Lake
Machine Learning        Predicciones a partir de datos históricos. ---> Data Lake

Warehouse de snowflake  Cluster virtual de máquinas en las que vamos a ejecutar trabajos de análisis, consulta, etc.

---

JAVA: y el uso que hace de la memoria RAM. JAVA hace un uso horrendo de la memoria RAM. (JS/PYTHON)

- String texto = "hola";  // El "hola" se guarda en algún sitio de la memoria RAM
- texto = "adios";        // El "adios" es lo que ahora se guarda en la memoria RAM...
                          // Donde? en el mismo sitio donde estaba "hola" o en otro?
                          //    En otro.. Una variable, al menos en JAVA no es un cajoncito donde pongo cosas... sino que es un puntero a una dirección de memoria (POSTIT)

                          // Llegados a este punto tengo 2 cosas en RAM: "hola" y "adios".
                          // Cago en la leche.. y si el hola ya no sirve?...
                          // En JAVA hemos creado el RECOLECTOR DE BASURA... que se encarga de ir limpiando la memoria RAM de cosas que ya no sirven.
    Esto consideráis que es bueno o malo? Ni bueno ni malo... ES UN FEATURE!

    Cuanto me cuesta hacer un programa A en C++: 300 horas de desarrollador para el programa + 50 horas extra de afinamiento de la memoria (Reservas de memoria, gestionar punteros... liberar memoria -> memory leaks)

    Cuanto me cuesta hacer un programa A en JAVA: 300 horas de desarrollador para el programa + 0 horas de afinamiento de la memoria (Reservas de memoria, gestionar punteros... liberar memoria -> memory leaks)... Eso si... necesito 32 GBS mas de RAM en la máquina.

    Cué cuesta más 32Gbs de RAM o 50 horas de desarrollador? 50 horas de desarrollador...

Análogo a ésto, ocurre en Snowflake