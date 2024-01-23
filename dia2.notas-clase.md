
Por defecto ya hemos dicho que snow flake va metiendo registros de una tabla en un montón de ficheros.
¿Cuántos ficheros? NPI a priori... a posteriori lo puedo consultar... Como anécdota!

Se intenta generar ficheros que tenga un determinado tamaño máximo óptimo de acuerdo al tamaño de cluster que tengo... y a la cantidad de registros cargados en la tabla.

Él va a ir guardando registros en ficheros... según le venga bien... habitualmente por fecha de inclusión.... pero no cierra fichero a fichero.

  FICHERO1          FICHERO2          FICHERO3
    registro1         registro2         registro3
    registro4         registro5         registro6
    ....              .....             ....
    -----             registro8

  FICHERO4
    registro7

DAME TODOS LOS REGISTROS [QUE...] -> FULLSCAN (FICHERO1+FICHERO2+FICHERO3+FICHERO4)

Si la tabla es muy grande... y por ende tiene muchas micro-particiones (ficheros de esos)
Y mis queries siempre (o suelen ) hacer uso de unos [QUE...] similares.

QUE = WHERE = comunidad autónoma del usuario sea "Cataluña"
Si supiera yo que en el fichero 2 y 3 son donde están todos los registros de "Cataluña"...
Me evito hacer el fullscan en el fichero1 y fichero4.... gasto menos pasta en computación... y va más rápido.

Esa columna comunidad autónoma sería mi clustering Key.

En nuestros casos habituales, no filtraremos solo por una columna... sino por varias... y en ese caso... la clave de clustering sería una concatenación de esas columnas.
---

# Consideraciones para elegir buenas claves de clustering:

- Necesitamos campos con una CARDINALDIAD (el número de valores diferentes que puedan tomar el campo)
  nio muy baja (booleano) , ni muy alta (reparto mucho los datos)
- En ocasiones aplciamos expresiones para reducir la cardinalidad de un dato:
  - Si tengo un campo TIMESTAMP: FECHA / HORA -> DATE
    - De forma que datos del mismo DIA se agrupan en la misma micro-partición.
- Los campos de tipo texto... solo se usan los primeros 5 caracteres en este proceso.

  CAMPO DNI: No mucho
  SEXO: No mucho

Los campos de fecha son ideales para usarlos como clustering key.

Una cosa a tener en cuenta con los clustering keys... es el mnto que hay por detrás... que es enorme.
Yo no me lo como... se lo come la capa de procesamiento de datos... = €€€€€€

# Consejos GENERALES para introducir las claves de clustering:
- Si uso varias, primero las de menor cardinalidad y luego las de mayor cardinalidad.
  - Necesito tener en cuenta los filtros que uso. TIPO, FECHA
- Los campos usados en clausulas WHERE Se benefician mucho más que los usados en ORDER BY del clustering.

El order by en Snowflake es una puñeta... en las bbdd relacionales se usan INDICES que tienen los datos preordenados. AQUI NO... por lo que esos ORDER BY hay que hacerlos bajo demanda.
Y ESO HACE UN USO INTENSIVISIMO DE RECURSOS... lo peor que puedo pedirle a snowflake.

Y cuidado... un ORDER BY no se hace solo cuando se escribe ORDER BY:
- DISTINCT: Lo primero que hace es un ORDERBY... para luego hacer un fullscan y mirar si un dato es como el anterior... y eliminarlo
- UNION: -> DISTINCT
  - - NOTA: EL "UNION ALL" ES GUAY !
- GROUP BY: Lo primero es un ORDER BY el campo de GROUP BY

Las ordenaciones se benefician ALGO de el clustering key... pero no tanto como los filtros.
Si tengo datos agrupados, las ordenaciones las hago sobre conjuntos más pequeños en paralelo... y luego concateno los resultados. -> MEJORA EL RENDIMIENTO....

Si no tengo los datos agrupados:
  MP1       MP2
  1 A       6 A
  2 B       7 B
  3 C       8 C
  4 D       9 D
  5 E       10 E

> Ordena por letra: Tengo que juntar los datos de las dos micro-particiones... y ordenarlos... y luego devolverlos. El proceso de ordenación lo tiene que llevar a cabo solo 1 nodo... eso va a ser más lento que:
 
 MP1    MP2   MP3
 1 A    3 C   5 E
 2 B    4 D   10 E
 6 A    8 C
 7 B    9 D

Puedo ordenar cada conjunto por separado:
  MP1: 1, 6, 2, 7
  MP2: 3, 8, 4, 9
  MP3: 5, 10
Y ahora concateno resultados: 1, 6, 2, 7, 3, 8, 4, 9, 5, 10
  La concatenación se hace en un solo nodo..
  Pero la ordenación se hace en 3 nodos... y eso es más rápido.

CUIDADO... ESTO NO AHORRA PASTA (POCA) ... lo que ahorro es mucho tiempo

---

NOTA: Cómo se usa eso del clustering key:
  Comunidad autónoma:
  17 valores posibles (textos) -> HASH -> NUMERO
                       números
                       fechas

  Madrid    -> mad . La m según el alfabeto es la letra número: 13
                    La a según el alfabeto es la letra número: 1
                    La d según el alfabeto es la letra número: 4
                                                            -------> 13+1+4 = 18 % número de ficheros
  Valencia  -> val . La v según el alfabeto es la letra número: 22
                    La a según el alfabeto es la letra número: 1
                    La l según el alfabeto es la letra número: 12
                                                            -------> 22+1+12 = 35 % número de ficheros

  Imaginad que tengo 10 ficheros:   El de madrid iría al 8
                                    El de valencia iría al 5
---

# Consideraciones para el diseño de tablas en Snowflake:

- Nunca usar campos de texto para albergar fechas
- Restricciones de integridad referencial, FOREIGN KEYS, PRIMARY KEYS... En Snowflake es como poner DOCUMENTACION... no se usan para nada! QUE BONITO !No hay validación de integridad referencial.
  - Eso no significa que no las defina... sino que en SnowFlake no se usan para nada.
  - Entonces, para qué quiero definirlas? 
    - Documentación
    - Cuando usamos herramientas externas que consumen datos de Snowflake... que sepan que relaciones tenemos por ahí. POWER BI, TABLEAU, etc...
- Similar a lo anterior lo tenemos con los tipos de datos de TEXTO: CHAR, NCHAR, VARCHAR, NVARCHAR..... STRING ... todo la misma mierda...No hay chequeo de nada, todas ocupan lo mismo.
  - Solo existen esos tipos de datos para facilitar la exportación de esquemas de BBDD desde otros motores de BBDD.
  - No obstante se recomienda establecer la longitud máxima... por si acaso otras herramientas que conecten con Snowflake hacen uso de esa información (que será lo más probable)