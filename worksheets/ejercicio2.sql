-- Asignando el warehose
USE WAREHOUSE compute_wh;

-- Creando base de datos y esquema
CREATE DATABASE IF NOT EXISTS midb;
USE DATABASE midb;
CREATE SCHEMA IF NOT EXISTS mies;
USE SCHEMA midb.mies;


CREATE OR REPLACE FUNCTION procesar_dni(DNI STRING)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
    var resultado = {
                        dni: DNI.toUpperCase(), 
                        valido: true, 
                        numero: null,
                        letra: null
                    };
    
    // Validar la estructura del DNI entrante con una expresion regular
    var expresionRegular = /^[0-9]{1,8}[A-Z]$/;
    if(expresionRegular.test(resultado.dni)){
        resultado.numero = parseInt(resultado.dni.substr(0, resultado.dni.length-1),10);
        resultado.letra  = resultado.dni.substr(-1);
        // Validar la letra
        var letrasValidas = 'TRWAGMYFPDXBNJZSQVHLCKE';
        var restoDivision = resultado.numero % 23;
        var letraQueDeberiaTener = letrasValidas.substr(restoDivision, 1);
        if(letraQueDeberiaTener != resultado.letra){
            resultado.valido = false;
        }
    } else {
        resultado.valido = false;
    }

    return resultado;
$$;


SELECT procesar_dni('23000T') as datos_dni,
       datos_dni['valido']::Boolean as valido
;

----
-- STREAMS
-- Un Stream nos sirve para monitorizar los cambios que se van realizando en una tabla

CREATE OR REPLACE TABLE mascotas (
    id NUMBER NOT NULL,
    nombre STRING NOT NULL,
    tipo STRING NULL
);

INSERT INTO mascotas (id, nombre, tipo)
VALUES
    (1, 'Pipo', 'Perro'),
    (2, 'Popi', 'Gato'),
    (3, 'Pupa', NULL);

SELECT * FROM mascotas;

CREATE OR REPLACE STREAM cambios_en_mascotas ON TABLE mascotas;
-- Un stream es como una vista, que me va a ir informando de los INSERT, DELETES y 'UPDATES' que vaya haciendo sobre la tabla.
-- Un Stream es muy ligero... NO DUCPLICA DATOS. 
-- De hecho es muy habitual tener muchos STREAMS creados sobre la misma tabla.
SELECT * FROM cambios_en_mascotas;


INSERT INTO mascotas (id, nombre, tipo)
VALUES
    (4, 'Milu', 'Cocodrilo'),
    (5, 'Mola', 'Elefante'),
    (6, 'Meli', NULL);
    
SELECT * FROM mascotas;
SELECT * FROM cambios_en_mascotas;



INSERT INTO mascotas (id, nombre, tipo)
VALUES
    (7, 'Mela', 'Loro');

SELECT * FROM cambios_en_mascotas;


CREATE OR REPLACE TABLE nombres_mascotas (
    id NUMBER NOT NULL,
    nombre STRING NOT NULL
);
SELECT * FROM nombres_mascotas;

INSERT INTO nombres_mascotas (id, nombre) SELECT id,nombre FROM cambios_en_mascotas WHERE METADATA$ACTION='INSERT';
SELECT * FROM nombres_mascotas;

SELECT * FROM cambios_en_mascotas;
-- En cuanto uso los datos del STREAM en una transacción que actualice datos (INSERT, DELETE, UPDATE), los datos son consumidos.
-- Y Empezamos de nuevo



INSERT INTO mascotas (id, nombre, tipo)
VALUES
    (8, 'Nino', 'León'),
    (9, 'Nene', 'Gato'),
    (10, 'Nani', NULL);

SELECT * FROM mascotas;
SELECT * FROM cambios_en_mascotas;
DELETE FROM mascotas WHERE id = 1;
SELECT * FROM cambios_en_mascotas;
DELETE FROM mascotas WHERE id = 10;
SELECT * FROM cambios_en_mascotas;

INSERT INTO nombres_mascotas (id, nombre) SELECT id,nombre FROM cambios_en_mascotas WHERE METADATA$ACTION='INSERT';
SELECT * FROM nombres_mascotas;
SELECT * FROM cambios_en_mascotas;
-- Aunque el dato lo hayamos filtrado, es eliminado. OJO !!!!!
UPDATE mascotas SET tipo = 'Papagayo' WHERE tipo IS NULL;
SELECT * FROM mascotas;
SELECT * FROM cambios_en_mascotas;
-- los updates, generan un DELETE y un INSERT... pero, en la columna ISUPDATE tienen un TRUE

----- TAREA :
CREATE OR REPLACE TABLE personas (
    id NUMBER NOT NULL,
    nombre STRING NOT NULL,
    dni STRING NULL
);

CREATE OR REPLACE STREAM cambios_en_personas ON TABLE personas;

INSERT INTO PERSONAS (id, nombre, dni) VALUES
  (1, 'Felipe', '12345678T')  ,
  (2, 'Menchu', '23000000t')  ,
  (3, 'Lucas',  '23000023T')  ,
  (4, 'Marcial', NULL)
  ;
SELECT * FROM personas;
SELECT * FROM cambios_en_personas;

CREATE OR REPLACE TABLE personas_validas (
    id NUMBER NOT NULL,
    nombre STRING NOT NULL,
    numero_dni NUMBER NOT NULL,
    letra_dni STRING NOT NULL
);

-- Lo que queremos es:
-- Una tarea que se ejecute cada 5 minutos, que lea los cambios en la tabla personas, 
-- y sincronice adecuadamente con la tabla personas_validas
-- (insertando los nuevos QUE SEAN VALIDOS, actualizando los que han cambiado (O BORRANDO o INSERTANDO) , y eliminando los que ya no existen)

CREATE OR REPLACE PROCEDURE sincronizar_personas()
RETURN VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var aDevolver = {
                        validos: 0;
                        invalidos: 0;
                    };
    
    // Tengo que leer todos los cambios del stream: cambios_en_personas
    // Depende del tipo de cambio... así hago
    // DELETE QUE NO ES PARTE DE UN UPDATE... intento borrar por si existe personas_validas
    // DELETE QUE SI ES PARTE DE UN UPDATE... nada
    // INSERT 
        // SI QUE ES PARTE DE UN UPDATE... 
            // intento eliminar de personas_validas
        // Es válido: inserto en personas_validas
    
    return aDevolver;
$$;


CREATE OR REPLACE TASK tarea_sincronizar_personas
    WAREHOUSE = compute_wh
    SCHEDULE = 'USING CRON 5 * * * * UTC'
    AS
        CALL sincronizar_personas();

ALTER TASK tarea_sincronizar_personas RESUME; -- Pon la tarea a funcionar