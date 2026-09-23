-- =============================================================================
-- ETL SYMMETRY · Estructura propuesta en BigQuery
-- -----------------------------------------------------------------------------
-- NO es obligatorio ejecutar esto: la función instalar() de Apps Script crea
-- el dataset y las 3 tablas automáticamente. Sirve como referencia o para
-- crearlas a mano desde la consola de BigQuery.
-- Reemplace  TU_PROYECTO  por el ID de su proyecto de Google Cloud.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS `TU_PROYECTO.symmetry`
OPTIONS (location = 'US', description = 'ETL Symmetry: transacciones de acceso (tarjetas)');

-- 1) Tabla final: una fila por transacción, sin duplicados (clave lógica: id_transaccion)
CREATE TABLE IF NOT EXISTS `TU_PROYECTO.symmetry.transacciones` (
  id_transaccion      INT64    OPTIONS (description = 'Text8: id único de la transacción'),
  cedula              STRING   OPTIONS (description = 'Text10: documento sin puntos. NULL si no vino'),
  nombres             STRING   OPTIONS (description = 'DataCardHolderTransactions_FirstName'),
  apellidos           STRING   OPTIONS (description = 'DataCardHolderTransactions_LastName'),
  fecha_hora          DATETIME OPTIONS (description = 'TransactionTime en hora local (YYYY-MM-DD HH:MM:SS)'),
  fecha_hora_original STRING   OPTIONS (description = 'TransactionTime tal como vino en el CSV'),
  lugar               STRING   OPTIONS (description = 'Text6: punto de acceso'),
  sentido             STRING   OPTIONS (description = 'ENTRADA / SALIDA / OTRO (derivado de lugar)'),
  id_carga            STRING   OPTIONS (description = 'Carga (archivo) que trajo la fila'),
  archivo_origen      STRING   OPTIONS (description = 'Nombre del CSV'),
  fila_origen         INT64    OPTIONS (description = 'Fila del CSV donde estaba la transacción'),
  cargado_en          TIMESTAMP OPTIONS (description = 'Momento en que entró a la tabla final')
)
PARTITION BY DATE(fecha_hora)
CLUSTER BY cedula, lugar
OPTIONS (description = 'Transacciones consolidadas (sin duplicados)');

-- 2) Staging: recibe cada archivo completo (se reemplaza en cada carga)
CREATE TABLE IF NOT EXISTS `TU_PROYECTO.symmetry.transacciones_staging` (
  id_transaccion INT64, cedula STRING, nombres STRING, apellidos STRING, fecha_hora DATETIME,
  fecha_hora_original STRING, lugar STRING, sentido STRING, id_carga STRING, archivo_origen STRING, fila_origen INT64
)
OPTIONS (description = 'Zona intermedia: última carga enviada');

-- 3) Auditoría: una fila por archivo procesado (completado o con error)
CREATE TABLE IF NOT EXISTS `TU_PROYECTO.symmetry.cargas` (
  id_carga STRING, archivo STRING, file_id STRING, md5 STRING, estado STRING,
  filas_csv INT64, transacciones INT64, validas INT64, rechazadas INT64, sin_cedula INT64,
  duplicadas_archivo INT64, insertadas INT64, ya_existian INT64,
  fecha_min DATETIME, fecha_max DATETIME, inicio DATETIME, fin DATETIME, duracion_seg INT64, mensaje STRING
)
OPTIONS (description = 'Auditoría: una fila por archivo procesado');


-- =============================================================================
-- CONSULTAS ÚTILES
-- =============================================================================

-- Control de integridad: debe devolver 0 filas (ningún id repetido)
SELECT id_transaccion, COUNT(*) AS veces
FROM `TU_PROYECTO.symmetry.transacciones`
GROUP BY id_transaccion HAVING COUNT(*) > 1;

-- Historial de cargas
SELECT inicio, archivo, estado, transacciones, insertadas, ya_existian, rechazadas, fecha_min, fecha_max, mensaje
FROM `TU_PROYECTO.symmetry.cargas`
ORDER BY inicio DESC;

-- Movimientos de una persona en un rango de fechas
SELECT fecha_hora, lugar, sentido
FROM `TU_PROYECTO.symmetry.transacciones`
WHERE cedula = '1007629528'
  AND fecha_hora BETWEEN '2025-09-08' AND '2025-09-16'
ORDER BY fecha_hora;

-- Entradas y salidas por día
SELECT DATE(fecha_hora) AS dia,
       COUNTIF(sentido = 'ENTRADA') AS entradas,
       COUNTIF(sentido = 'SALIDA')  AS salidas
FROM `TU_PROYECTO.symmetry.transacciones`
GROUP BY dia ORDER BY dia DESC;
