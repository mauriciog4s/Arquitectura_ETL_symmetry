# ETL Symmetry

Carga automática del reporte semanal de Symmetry (CSV) a **BigQuery**, pasando por el Google Sheet **ETL_Symmetry**. Todo funciona con Google Workspace y Apps Script, así que se puede operar desde un Chromebook.

```
Carpeta Drive (CSV) ─► Apps Script (lunes 12:00) ─► Sheet "Transaccional" ─► BigQuery staging ─► BigQuery transacciones
                                   │
                                   ├─► ¿No llegó el CSV? → correo a quien lo carga + revisión diaria hasta que llegue
                                   └─► Control_Archivos · Log · Errores · Portal web · Correo
```

Resumen de la arquitectura en una página: [`docs/Arquitectura_ETL_Symmetry.pdf`](docs/Arquitectura_ETL_Symmetry.pdf)

## Contenido del repositorio

| Archivo | Para qué sirve |
|---|---|
| `apps_script/Code.gs` | Toda la lógica del ETL y las funciones del portal |
| `apps_script/Portal.html` | Portal web para ver qué está pasando |
| `apps_script/appsscript.json` | Manifiesto: zona horaria de Bogotá y activación del servicio BigQuery |
| `bigquery/esquema.sql` | Estructura de las tablas y consultas útiles (opcional: el script crea las tablas solo) |
| `docs/Arquitectura_ETL_Symmetry.pdf` | La arquitectura en una hoja |

---

## Instalación (una sola vez, unos 15 minutos)

### Paso 1. Crear el proyecto de BigQuery
1. Entra a <https://console.cloud.google.com> con tu cuenta.
2. Arriba, en el selector de proyectos, elige **Proyecto nuevo**. Ponle un nombre (por ejemplo `etl-symmetry`) y pulsa **Crear**.
3. Copia el **ID del proyecto**. Aparece en la tarjeta "Información del proyecto" (por ejemplo `etl-symmetry-472913`). Lo vas a usar en el paso 3.
4. **Recomendado:** activa la facturación del proyecto (menú ☰ → Facturación). Sin facturación, BigQuery funciona en "modo Sandbox" y **borra las tablas a los 60 días**. Con este volumen de datos el costo es prácticamente cero, porque cabe en la capa gratuita (10 GB de almacenamiento y 1 TB de consultas al mes).

### Paso 2. Abrir Apps Script dentro del Sheet
1. Abre el Google Sheet **ETL_Symmetry**.
2. Menú **Extensiones → Apps Script**. Se abre el editor.

### Paso 3. Pegar el código
1. **Code.gs:** borra lo que haya y pega el contenido de `apps_script/Code.gs`. Luego, al inicio del archivo, completa dos datos:
   - Línea 29, `PROJECT_ID`: reemplaza `PEGAR-AQUI-ID-DEL-PROYECTO` por el ID que copiaste en el paso 1.
   - Línea 41, `EMAIL_RESPONSABLE_CARGA`: el correo de la persona que sube el CSV cada lunes, por ejemplo `'persona@empresa.com'`. A ella le llega el aviso cuando falta el archivo. Para varias personas, sepáralas con coma.
2. **Portal:** pulsa **＋ → HTML**, nómbralo exactamente `Portal` (sin ".html") y pega el contenido de `apps_script/Portal.html`.
3. **Manifiesto:** ⚙️ *Configuración del proyecto* → marca **"Mostrar el archivo de manifiesto appsscript.json en el editor"**. Vuelve al editor, abre `appsscript.json` y reemplaza su contenido por el de `apps_script/appsscript.json`. Este archivo activa BigQuery y la zona horaria de Bogotá.
4. Guarda con 💾 o `Ctrl + S`.

### Paso 4. Instalar
1. Arriba, en el selector de funciones, elige **`instalar`** y pulsa **▶ Ejecutar**.
2. Google pedirá permisos. Pulsa *Revisar permisos* → elige tu cuenta → *Configuración avanzada* → *Ir a (proyecto)* → **Permitir**. Es normal: el script es tuyo.
3. Al terminar, el Sheet tendrá las hojas `Transaccional`, `Control_Archivos`, `Log` y `Errores`. BigQuery tendrá el dataset `symmetry` con 3 tablas, y quedará programada la revisión automática **cada lunes a las 12:00**. En la hoja **Log** verás todo lo que hizo `instalar`.

### Paso 5. Publicar el portal
1. **Implementar → Nueva implementación** → ⚙️ tipo **Aplicación web**.
2. *Ejecutar como:* **Yo**. *Quién tiene acceso:* **Solo yo**. Si otra persona debe verlo, elige *Cualquier usuario con cuenta de Google* y comparte el enlace.
3. Pulsa **Implementar** y copia la URL: ese es tu portal.

> Si más adelante cambias el código, publícalo en **Implementar → Gestionar implementaciones → ✏️ → Versión: Nueva versión**. Así el portal mantiene la misma URL.

### Paso 6. Probar
Sube un CSV a la carpeta de entrada y pulsa **Procesar ahora** en el portal. No hace falta esperar al lunes. Con el archivo de ejemplo (166.241 filas crudas) el resultado esperado es **49.041 transacciones** cargadas.

---

## Programación semanal

Cada lunes se sube el CSV de la semana anterior. El script revisa la carpeta **cada lunes a las 12:00**. Google ejecuta el disparador en algún momento entre las 12:00 y las 12:59.

| Qué encuentra el lunes | Qué hace |
|---|---|
| Un CSV nuevo | Lo procesa hasta BigQuery y te envía el resumen por correo. |
| Nada, o solo archivos que no sirven (repetidos, que no son CSV o con error) | Envía un correo **a quien carga el archivo** (`EMAIL_RESPONSABLE_CARGA`) y a ti: "no ha llegado el CSV de esta semana", con el enlace a la carpeta y el motivo de cada archivo que no sirvió. Después **vuelve a revisar cada día a las 12:00** y manda un recordatorio, hasta que llegue el archivo (máximo `MAX_RECORDATORIOS` días). |
| El archivo llega el martes o el miércoles | La revisión diaria lo encuentra, lo procesa y detiene los recordatorios. |
| Un archivo grande que no termina en 6 minutos | Programa solo una continuación a los 10 minutos, hasta terminar. |

El portal muestra arriba si el archivo de la semana llegó o no, a quién se avisó y cuándo es la próxima revisión. Si subes un archivo fuera de horario, también puedes pulsar **Procesar ahora**.

---

## Cómo funciona

Cada archivo pasa por 6 pasos. El estado de cada archivo queda guardado en la hoja `Control_Archivos`. Si Google corta la ejecución (Apps Script tiene un límite de 6 minutos), el proceso se programa solo para continuar 10 minutos después, desde donde quedó.

| Paso | Qué hace | Estado |
|---|---|---|
| 1. Recibo | Detecta los CSV nuevos en la carpeta. Calcula una "huella" (md5) para no subir dos veces el mismo contenido. Ignora lo que no es CSV. | `RECIBIDO` |
| 2. Organizo | Une las filas desfasadas, convierte la fecha y limpia los datos. Escribe el resultado en la hoja **Transaccional**. | `ORGANIZADO` |
| 3. Envío | Relee la hoja, revalida los datos y los envía a BigQuery (`transacciones_staging`). Confirma cuántas filas llegaron. | `ENVIADO` |
| 4. Consolido | Pasa a la tabla final `transacciones` **solo** las transacciones cuyo `id_transaccion` todavía no existe. | `CONSOLIDADO` |
| 5. Verifico | Comprueba en BigQuery que todas las transacciones del archivo estén en la tabla final y que no haya duplicados. | `VERIFICADO` |
| 6. Limpio | Vacía la hoja Transaccional, mueve el CSV a `PROCESADOS`, registra la auditoría en la tabla `cargas` y envía un correo. | `COMPLETADO` |

Otros estados posibles: `ERROR` (el archivo necesita revisión), `DUPLICADO` (tiene el mismo contenido que un archivo ya cargado) e `IGNORADO` (no es CSV o está vacío).

### Qué se corrige del CSV
Cada transacción llega partida en dos filas: una con nombres y apellidos, y la siguiente con cédula (`Text10`), fecha, lugar (`Text6`) e id (`Text8`). Entre medio hay filas vacías. El script une esas piezas. Si algún día el CSV llega bien alineado, también lo procesa sin cambios.

| CSV (origen) | Sheet / BigQuery | Tratamiento |
|---|---|---|
| `Text8` | `id_transaccion` (INT64) | Clave única. Evita duplicados. |
| `Text10` | `cedula` (STRING) | Se quitan puntos y espacios (`1.001.389.415` → `1001389415`). Si no viene, queda vacía. |
| `DataCardHolderTransactions_FirstName` | `nombres` | Se quitan espacios sobrantes. |
| `DataCardHolderTransactions_LastName` | `apellidos` | Se quitan espacios sobrantes. |
| `DataCardHolderTransactions_TransactionTime` | `fecha_hora` (DATETIME) | `15 September 2025 23:58` → `2025-09-15 23:58:00`. El texto original se conserva en `fecha_hora_original`. |
| `Text6` | `lugar` | Se quitan espacios dobles. |
| (derivado) | `sentido` | `ENTRADA` / `SALIDA` / `OTRO`, según el nombre del lugar. |
| (control) | `id_carga`, `archivo_origen`, `fila_origen`, `cargado_en` | Trazabilidad: de qué archivo y de qué fila salió cada dato. |

### Qué pasa si algo falla

| Situación | Qué hace el sistema |
|---|---|
| El lunes no llegó el CSV (o llegó uno repetido o que no es CSV) | Avisa a quien lo carga y revisa de nuevo cada día a las 12:00 hasta que llegue. |
| Algunas filas con fecha o id inválido (hasta el 1 %) | Carga las válidas. Las malas quedan en la hoja **Errores** con la fila original. |
| Más del 1 % de filas malas, o faltan columnas | No carga el archivo, lo marca `ERROR`, lo mueve a `CON_ERROR` y envía un correo a ti **y a quien lo cargó**, para que suba uno corregido. |
| Falla temporal de Google (timeout, cuota) | Reintenta solo cada 10 minutos, hasta 4 veces por archivo. |
| Alguien editó la hoja Transaccional a mano | Lo detecta al releer la hoja y la regenera desde el CSV original. |
| BigQuery recibe menos filas de las enviadas, o faltan filas en la tabla final | Repite el paso. No limpia el Sheet hasta que la verificación cuadra. |
| Aparecen duplicados en la tabla final | Se detiene (`ERROR`) y conserva el Sheet para que lo revises. |
| Falta el PROJECT_ID, los permisos o la facturación | Registra el error y te envía un correo con la instrucción para corregirlo. |

**El CSV original nunca se borra.** Cuando corrijas la causa de un error, pulsa **Reintentar** en el portal (o ejecuta `reintentarCarga('ID')`). El archivo se procesa de nuevo desde cero sin duplicar nada.

### Dónde ver qué pasó
- **Portal web:** estado general, archivo en curso con sus 6 pasos, historial, log con filtros, errores de datos y totales en vivo desde BigQuery.
- **Hoja `Log`:** una fila por evento (fecha, ejecución, carga, archivo, etapa, nivel y mensaje).
- **Hoja `Control_Archivos`:** una fila por archivo, con todos sus conteos.
- **Tabla `symmetry.cargas` en BigQuery:** auditoría permanente de cada archivo procesado.
- **Correo:** avisa cuando una carga termina bien, cuando hay un error y cuando falta el CSV de la semana.

### Ajustes (al inicio de `Code.gs`, en `CONFIG`)
| Parámetro | Por defecto | Qué controla |
|---|---|---|
| `PROJECT_ID` | — | ID del proyecto de Google Cloud (**obligatorio**) |
| `UMBRAL_ERRORES` | `0.01` | Porcentaje máximo de filas malas para cargar igual |
| `EMAIL_RESPONSABLE_CARGA` | *(vacío)* | Quién recibe el aviso de "falta el CSV" (además de ti) |
| `DIA_EJECUCION` / `HORA_EJECUCION` | `MONDAY` / `12` | Día y hora de la revisión semanal. Si los cambias, vuelve a ejecutar `instalar()` |
| `MAX_RECORDATORIOS` | `4` | Cuántos días seguidos se revisa y se recuerda si falta el CSV |
| `MOVER_ARCHIVOS` | `true` | Mover los CSV a las carpetas `PROCESADOS` / `CON_ERROR` / `DUPLICADOS` |
| `NOTIFICAR_EXITO` | `true` | Enviar correo también cuando una carga termina bien |
| `EMAIL_ALERTAS` | *(dueño)* | A quién llegan los correos |

Para pausar la automatización ejecuta `pausarAutomatizacion()`. Para reanudarla, ejecuta `instalar()`.
