# Organización de tablas en Snowflake: Claves de Clustering y Tablas Agrupadas

En general, Snowflake produce datos bien agrupados en tablas; sin embargo, con el tiempo, especialmente con operaciones DML en tablas muy grandes (definidas por la cantidad de datos, no el número de filas), los datos en algunas filas de la tabla pueden no estar óptimamente agrupados en las dimensiones deseadas.

Para mejorar el agrupamiento de las micro-particiones de la tabla, se pueden ordenar manualmente las filas en columnas clave de la tabla y volver a insertarlas; sin embargo, estas tareas pueden ser engorrosas y costosas.

En su lugar, Snowflake permite automatizar estas tareas designando una o más columnas/expresiones de la tabla como clave de clustering. Una tabla con una clave de clustering definida se considera agrupada.

Puedes agrupar vistas materializadas, así como tablas. Las reglas para el agrupamiento de tablas y vistas materializadas son generalmente las mismas. Para algunos consejos adicionales específicos para vistas materializadas, consulta Vistas Materializadas y Agrupamiento y Mejores Prácticas para Vistas Materializadas.

## Atención

Las claves de clustering no son adecuadas para todas las tablas debido a los costos de agrupar inicialmente los datos y mantener el agrupamiento. El agrupamiento es óptimo cuando:

- Requieres los tiempos de respuesta más rápidos posibles, independientemente del costo.
- El mejor rendimiento de consulta compensa los créditos requeridos para agrupar y mantener la tabla.

# ¿Qué es una Clave de Clustering?

Una **Clave de Clustering** es un subconjunto de columnas (o expresiones sobre una tabla) en una tabla, designadas explícitamente para co-ubicar los datos de la tabla en las mismas micro-particiones. Esto resulta útil para tablas muy grandes donde el ordenamiento no era ideal al momento de insertar/cargar los datos, o donde operaciones DML extensas han degradado el agrupamiento natural de la tabla.

## Indicadores para Definir una Clave de Clustering:

- Las consultas en la tabla se ejecutan más lentamente de lo esperado o su rendimiento ha degradado con el tiempo.
- La profundidad de clustering de la tabla es grande.

## Implementación de una Clave de Clustering:

- Se puede definir al crear la tabla (`CREATE TABLE`) o después (`ALTER TABLE`).
- La clave de clustering de una tabla puede ser modificada o eliminada en cualquier momento.

# Beneficios de Definir Claves de Clustering (para Tablas Muy Grandes)

Las claves de clustering ofrecen varios beneficios en tablas de gran tamaño:

- **Eficiencia Mejorada en Scans de Consultas:** Permiten omitir datos que no coinciden con los predicados de filtrado, mejorando la eficiencia del escaneo.
- **Mejor Compresión de Columnas:** Las tablas con clustering tienden a tener mejor compresión que las tablas sin él, especialmente si otras columnas están fuertemente correlacionadas con las que componen la clave de clustering.
- **Mantenimiento Automático:** Una vez definida, Snowflake gestiona automáticamente el mantenimiento óptimo de clustering, sin necesidad de administración adicional.

## Consideraciones Importantes

- **Consumo de Recursos:** Clustering consume créditos por los recursos computacionales utilizados. Por lo tanto, es recomendable solo cuando las consultas se benefician sustancialmente del clustering.
- **Beneficio según Uso de Consultas:** Las consultas se benefician más del clustering cuando filtran o ordenan basándose en la clave de clustering.
- **Costo-Efectividad:** El clustering es más efectivo para tablas consultadas frecuentemente y que no cambian con frecuencia.

**Nota:** La actualización de filas después de definir una clave de clustering no es inmediata; Snowflake realiza mantenimiento automatizado solo si la tabla se beneficia de la operación.

# Consideraciones para Elegir Clustering para una Tabla

Clustering es más efectivo para tablas que cumplen con todos los siguientes criterios:

- **Gran Número de Micro-particiones:** Generalmente, tablas con múltiples terabytes (TB) de datos.
- **Consultas que Aprovechan el Clustering:**
  - **Consultas Selectivas:** Necesitan leer solo un pequeño porcentaje de filas (y, por lo tanto, de micro-particiones).
  - **Consultas que Ordenan Datos:** Por ejemplo, consultas con cláusulas `ORDER BY`.
- **Alto Porcentaje de Consultas Beneficiadas por la Misma Clave de Clustering:** Muchas o la mayoría de las consultas seleccionan o ordenan en las mismas columnas.

Si el objetivo es reducir costos, las tablas agrupadas deben tener un alto ratio de consultas a operaciones DML (INSERT/UPDATE/DELETE), indicando que se consulta frecuentemente y se actualiza poco. Para tablas con mucho DML, se recomienda agrupar las declaraciones DML en lotes grandes e infrecuentes.

**Nota:** Antes de decidir agrupar una tabla, se recomienda probar un conjunto representativo de consultas para establecer líneas base de rendimiento.

# Consideraciones Adicionales para Claves de Clustering

- **Alta Cardinalidad:** Mantener clustering en columnas de alta cardinalidad puede ser más costoso.
- **Clustering en Claves Únicas:** El costo de agrupar en una clave única podría ser mayor que el beneficio, especialmente si las búsquedas directas no son el uso principal de la tabla.
- **Uso de Expresiones para Reducir Cardinalidad:** Si se desea usar una columna de alta cardinalidad como clave de clustering, se recomienda definir la clave como una expresión sobre la columna para reducir la cantidad de valores distintos.
  - **Ejemplo:** Convertir un `TIMESTAMP` a `DATE` para reducir la cardinalidad.
  - **Ejemplo de Truncado de Números:** Usar la función `TRUNC` para reducir dígitos significativos.

## Consejos

- **Orden en Claves de Clustering Múltiples:** Ordenar las columnas de menor a mayor cardinalidad en la cláusula `CLUSTER BY`.
- **Clustering en Campos de Texto:** La clave de clustering rastrea solo los primeros bytes del campo de texto (generalmente 5 o 6 bytes).

En resumen, para claves de clustering en Snowflake, se sugiere enfocarse en columnas utilizadas en operaciones de filtro o JOIN más que en aquellas usadas en cláusulas GROUP BY u ORDER BY. Además, el reclustering periódico es necesario para mantener un agrupamiento óptimo ya que las operaciones DML pueden desorganizar los datos. Snowflake realiza el reclustering automáticamente, pero hay un impacto en créditos y almacenamiento debido a la reorganización de datos y la creación de nuevas micro-particiones. Antes de definir una clave de clustering, se deben considerar estos costos asociados.

# Reclustering

El reclustering es necesario en tablas agrupadas en Snowflake tras operaciones DML (INSERT, UPDATE, DELETE, MERGE, COPY) para mantener un agrupamiento óptimo.

- **Proceso:** Durante el reclustering, Snowflake reorganiza los datos de la columna utilizando la clave de clustering. Las filas afectadas se eliminan y se reinsertan agrupadas según la clave de clustering.
- **Automatización:** El reclustering en Snowflake es automático y no requiere mantenimiento.

## Impacto en Créditos y Almacenamiento

- **Consumo de Créditos:** Al igual que otras operaciones DML, el reclustering consume créditos. La cantidad depende del tamaño de la tabla y la cantidad de datos a reagrupar.
- **Costos de Almacenamiento:** El reclustering genera nuevos micro-particiones, aumentando los costos de almacenamiento.
- **Time Travel y Fail-safe:** Las micro-particiones originales se marcan como eliminadas pero se retienen en el sistema para habilitar Time Travel y Fail-safe, incrementando el costo de almacenamiento.

**Importante:** Antes de definir una clave de clustering, considera los costos asociados en créditos y almacenamiento.

# Notas Importantes de Uso para Claves de Clustering en Snowflake

- **Columnas VARCHAR:**
  - Solo se utilizan los primeros 5 bytes para el clustering.

- **Cardinalidad y Subcadenas:**
  - Si los primeros caracteres son iguales en todas las filas, considera agrupar por un substring que comience después de estos caracteres para lograr cardinalidad óptima.

- **Orden en Claves Múltiples:**
  - El orden de las columnas o expresiones en la clave de clustering afecta el agrupamiento en micro-particiones.

- **Clonación de Tablas:**
  - Al clonar una tabla (`CREATE TABLE … CLONE`), la clave de clustering se copia, pero el clustering automático se suspende y debe reactivarse.

- **Creación de Tablas con SELECT:**
  - Al crear una tabla con `CREATE TABLE … AS SELECT`, no se soporta la clave de clustering existente; sin embargo, se puede definir una clave de clustering después de crear la tabla.

- **Columnas VARIANT:**
  - No se soporta definir una clave de clustering directamente en columnas VARIANT, pero se puede especificar una columna VARIANT en una clave de clustering usando una expresión.

# Cuándo Establecer una Clave de Clustering

- **No Siempre Necesario:** La mayoría de las tablas no requieren una clave de clustering debido a la optimización automática y micro-particionamiento de Snowflake.
- **Tablas Pequeñas:** Clustering en tablas pequeñas generalmente no mejora significativamente el rendimiento de consulta.
- **Consideraciones para Tablas Grandes:**
  - Si el orden de carga de datos no coincide con la dimensión más consultada (por ejemplo, carga por fecha, pero se consulta por ID).
  - Si el perfil de consulta indica un tiempo significativo en el escaneo de datos para filtros en columnas específicas.

- **Impacto del Reclustering:**
  - Reclustering reordena los datos existentes. El orden anterior se almacena por 7 días para protección Fail-safe.
  - Reclustering incurre en costos de cómputo relacionados con el tamaño de los datos reordenados.


---

# Consideraciones para el Diseño de Tablas

- **Tipos de Datos Fecha/Hora para Columnas:**
  - Se recomienda usar tipos de datos de fecha o timestamp en lugar de tipo de caracteres. DATE y TIMESTAMP son más eficientes que VARCHAR, mejorando el rendimiento de las consultas.

- **Restricciones de Integridad Referencial:**
  - En Snowflake, las restricciones de integridad referencial son informativas y, con la excepción de NOT NULL, no se aplican. Las restricciones que no son NOT NULL se crean como deshabilitadas.
  - Proporcionan metadatos valiosos, ayudando en el diseño del esquema y la comprensión de las relaciones entre tablas.
  - Herramientas de BI y visualización utilizan definiciones de claves foráneas para construir condiciones de unión adecuadas, lo que ahorra tiempo y reduce errores.

- **Especificación de Restricciones:**
  - Se especifican al crear o modificar una tabla usando los comandos CREATE | ALTER TABLE … CONSTRAINT.

# Cuándo Especificar Longitudes de Columna en Snowflake

- **Compresión Efectiva:** Snowflake comprime los datos de las columnas eficazmente, por lo que tener columnas más grandes de lo necesario tiene un impacto mínimo en el tamaño de las tablas de datos.
- **Rendimiento de Consulta:** No hay diferencia en el rendimiento de consulta entre una columna con una declaración de longitud máxima (por ejemplo, `VARCHAR(16777216)`) y una de menor precisión.
- **Recomendaciones:**
  - **Definir Longitud Apropiada:** Si el tamaño de los datos de la columna es predecible, se recomienda definir una longitud de columna apropiada por las siguientes razones:
    - Las operaciones de carga de datos pueden detectar más fácilmente problemas, como columnas cargadas en un orden incorrecto.
    - Si la longitud de la columna no se especifica, algunas herramientas de terceros pueden anticipar el consumo del valor de tamaño máximo, lo que podría aumentar el uso de memoria del lado del cliente o causar comportamientos inusuales.
