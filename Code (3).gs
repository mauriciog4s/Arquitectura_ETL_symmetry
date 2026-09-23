/*******************************************************************************
 * ETL SYMMETRY  ·  CSV (Drive)  →  Google Sheet "Transaccional"  →  BigQuery
 * -----------------------------------------------------------------------------
 * Archivos de este proyecto de Apps Script:
 *   Code.gs          → toda la lógica (este archivo)
 *   Portal.html      → portal web de monitoreo
 *   appsscript.json  → manifiesto (zona horaria + servicio BigQuery)
 *
 * Funciones que usted puede ejecutar a mano (editor ▶ Ejecutar):
 *   instalar()              → prepara las hojas, BigQuery y la ejecución semanal. Se puede repetir sin riesgo.
 *   procesar()              → corre el ETL ahora (lo mismo que el botón "Procesar ahora" del portal).
 *   reintentarCarga('ID')   → vuelve a procesar una carga que quedó en ERROR.
 *   pausarAutomatizacion()  → apaga el disparador automático (instalar() lo vuelve a encender).
 *
 * Programación: cada lunes a las 12:00 revisa la carpeta y procesa el CSV de la semana anterior.
 *   · Si no llegó ningún CSV válido → avisa por correo a quien lo carga y vuelve a revisar
 *     cada día a las 12:00 (hasta MAX_RECORDATORIOS días) hasta que llegue.
 *   · Si un archivo no alcanza a terminar en 6 minutos → se continúa solo a los 10 minutos.
 *
 * Flujo de cada archivo (cada paso se puede repetir sin duplicar ni perder datos):
 *   RECIBIDO → ORGANIZADO (Sheet) → ENVIADO (BigQuery staging) → CONSOLIDADO (tabla final)
 *            → VERIFICADO → COMPLETADO (Sheet limpio, CSV movido a PROCESADOS)
 ******************************************************************************/

// ============================================================================
// 1. CONFIGURACIÓN  (lo único que normalmente hay que tocar es PROJECT_ID)
// ============================================================================
const CONFIG = {
  PROJECT_ID: 'PEGAR-AQUI-ID-DEL-PROYECTO',          // ID del proyecto de Google Cloud donde vive BigQuery
  DATASET: 'symmetry',                               // Dataset de BigQuery (se crea solo)
  UBICACION_BQ: 'US',                                // Región del dataset
  SPREADSHEET_ID: '1ptkkcxqZRF5UT8tVMzCgLhxkbfhfYVNp8_Q6sK2wymQ',  // Google Sheet "ETL_Symmetry"
  CARPETA_ENTRADA_ID: '1ku053Dha1kGlXpKbvRDFgP6HEGpvbKEz',        // Carpeta donde suben los CSV
  CARPETA_SHEET_ID: '1ymvIjFzdHf3g1IXHsl0Bn2qw546O_H_g',          // Carpeta donde vive el Google Sheet
  MOVER_ARCHIVOS: true,          // Mover CSV a PROCESADOS / CON_ERROR / DUPLICADOS al terminar
  UMBRAL_ERRORES: 0.01,          // Si más del 1 % de transacciones tiene error, el archivo no se carga
  MAX_INTENTOS: 4,               // Fallos temporales tolerados por archivo antes de marcarlo ERROR
  DIA_EJECUCION: 'MONDAY',       // Día de la revisión semanal: MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY...
  HORA_EJECUCION: 12,            // Hora de la revisión (0-23). Google la ejecuta dentro de esa hora (12:00-12:59)
  MAX_RECORDATORIOS: 4,          // Si falta el CSV: cuántos días seguidos se vuelve a revisar y avisar
  EMAIL_RESPONSABLE_CARGA: '',   // Correo(s) de quien sube el CSV, separados por coma. Recibe el aviso de "falta archivo"
  MINUTOS_CONTINUACION: 10,      // Si un archivo no termina en una ejecución, se continúa a los N minutos
  MINUTOS_ESPERA_ARCHIVO: 2,     // Espera a que un archivo recién subido termine de subir
  EMAIL_ALERTAS: '',             // Vacío = correo del dueño del script
  NOTIFICAR_EXITO: true,         // Enviar correo también cuando una carga termina bien
  MAX_FILAS_LOG: 20000,          // El Log conserva las últimas N filas
  MAX_ERRORES_POR_ARCHIVO: 2000, // Máximo de filas de detalle en la hoja Errores por archivo
};

// ============================================================================
// 2. CONSTANTES
// ============================================================================
const TIEMPO_MAX_MS = 5 * 60 * 1000;   // Google corta a los 6 min; se deja 1 min de margen
const LIMITE_CELDAS_SHEET = 10000000;
const LOTE_ESCRITURA = 10000;
const DISPARADORES = ['ejecucionSemanal', 'continuarProceso', 'recordatorioArchivo', 'procesar'];
const DIAS = { MONDAY: [1, 'lunes'], TUESDAY: [2, 'martes'], WEDNESDAY: [3, 'miércoles'], THURSDAY: [4, 'jueves'],
  FRIDAY: [5, 'viernes'], SATURDAY: [6, 'sábado'], SUNDAY: [7, 'domingo'] };

const HOJAS = { TX: 'Transaccional', CONTROL: 'Control_Archivos', LOG: 'Log', ERRORES: 'Errores' };
const SUBCARPETAS = { OK: 'PROCESADOS', ERROR: 'CON_ERROR', DUPLICADO: 'DUPLICADOS' };
const TABLAS = { FINAL: 'transacciones', STAGING: 'transacciones_staging', CARGAS: 'cargas' };

const E = {
  RECIBIDO: 'RECIBIDO', ORGANIZANDO: 'ORGANIZANDO', ORGANIZADO: 'ORGANIZADO',
  ENVIANDO: 'ENVIANDO', ENVIADO: 'ENVIADO', CONSOLIDANDO: 'CONSOLIDANDO', CONSOLIDADO: 'CONSOLIDADO',
  VERIFICADO: 'VERIFICADO', COMPLETADO: 'COMPLETADO',
  ERROR: 'ERROR', DUPLICADO: 'DUPLICADO', IGNORADO: 'IGNORADO',
};
const TERMINALES = [E.COMPLETADO, E.ERROR, E.DUPLICADO, E.IGNORADO];
const ETAPA_DE_ESTADO = {
  RECIBIDO: 'ORGANIZACION', ORGANIZANDO: 'ORGANIZACION', ORGANIZADO: 'ENVIO', ENVIANDO: 'ENVIO',
  ENVIADO: 'CONSOLIDACION', CONSOLIDANDO: 'CONSOLIDACION', CONSOLIDADO: 'VERIFICACION', VERIFICADO: 'LIMPIEZA',
};

// Columnas de la hoja Transaccional = columnas de BigQuery (mismo orden, nombres limpios)
const COLUMNAS_TX = ['id_transaccion', 'cedula', 'nombres', 'apellidos', 'fecha_hora', 'fecha_hora_original',
  'lugar', 'sentido', 'id_carga', 'archivo_origen', 'fila_origen'];
const COL = {};
COLUMNAS_TX.forEach((c, i) => COL[c] = i);

const COLUMNAS_CONTROL = ['id_carga', 'archivo', 'file_id', 'md5', 'tamano_kb', 'archivo_modificado', 'detectado_en',
  'estado', 'fallo_en', 'intentos', 'filas_csv', 'transacciones', 'validas', 'rechazadas', 'sin_cedula',
  'duplicadas_archivo', 'fecha_min', 'fecha_max', 'insertadas_bq', 'ya_existian_bq', 'progreso', 'job_id',
  'inicio', 'fin', 'ultimo_mensaje'];
const COLUMNAS_LOG = ['fecha_hora', 'ejecucion', 'id_carga', 'archivo', 'etapa', 'nivel', 'mensaje', 'detalle'];
const COLUMNAS_ERRORES = ['fecha_hora', 'id_carga', 'archivo', 'fila_origen', 'tipo_error', 'detalle', 'datos_originales'];
const ENCABEZADOS = {
  [HOJAS.TX]: COLUMNAS_TX, [HOJAS.CONTROL]: COLUMNAS_CONTROL, [HOJAS.LOG]: COLUMNAS_LOG, [HOJAS.ERRORES]: COLUMNAS_ERRORES,
};

// Esquemas de BigQuery
const ESQUEMA_TX = [
  { name: 'id_transaccion', type: 'INTEGER', description: 'Text8: id único de la transacción' },
  { name: 'cedula', type: 'STRING', description: 'Text10: documento (sin puntos). NULL si no vino' },
  { name: 'nombres', type: 'STRING', description: 'DataCardHolderTransactions_FirstName' },
  { name: 'apellidos', type: 'STRING', description: 'DataCardHolderTransactions_LastName' },
  { name: 'fecha_hora', type: 'DATETIME', description: 'TransactionTime en hora local (YYYY-MM-DD HH:MM:SS)' },
  { name: 'fecha_hora_original', type: 'STRING', description: 'TransactionTime tal como vino en el CSV' },
  { name: 'lugar', type: 'STRING', description: 'Text6: punto de acceso' },
  { name: 'sentido', type: 'STRING', description: 'ENTRADA / SALIDA / OTRO (derivado de lugar)' },
  { name: 'id_carga', type: 'STRING', description: 'Carga (archivo) que trajo la fila' },
  { name: 'archivo_origen', type: 'STRING', description: 'Nombre del CSV' },
  { name: 'fila_origen', type: 'INTEGER', description: 'Fila del CSV donde estaba la transacción' },
];
const ESQUEMA_FINAL = ESQUEMA_TX.concat([
  { name: 'cargado_en', type: 'TIMESTAMP', description: 'Momento en que entró a la tabla final' },
]);
const ESQUEMA_CARGAS = [
  ['id_carga', 'STRING'], ['archivo', 'STRING'], ['file_id', 'STRING'], ['md5', 'STRING'], ['estado', 'STRING'],
  ['filas_csv', 'INTEGER'], ['transacciones', 'INTEGER'], ['validas', 'INTEGER'], ['rechazadas', 'INTEGER'],
  ['sin_cedula', 'INTEGER'], ['duplicadas_archivo', 'INTEGER'], ['insertadas', 'INTEGER'], ['ya_existian', 'INTEGER'],
  ['fecha_min', 'DATETIME'], ['fecha_max', 'DATETIME'], ['inicio', 'DATETIME'], ['fin', 'DATETIME'],
  ['duracion_seg', 'INTEGER'], ['mensaje', 'STRING'],
].map(([name, type]) => ({ name, type }));

// Encabezados esperados en el CSV (se aceptan variantes sin prefijo)
const ENCABEZADOS_CSV = {
  cedula: ['text10'],
  nombres: ['datacardholdertransactions_firstname', 'firstname'],
  apellidos: ['datacardholdertransactions_lastname', 'lastname'],
  fecha: ['datacardholdertransactions_transactiontime', 'transactiontime'],
  lugar: ['text6'],
  id: ['text8'],
};

// Contexto de la ejecución actual
const Ctx = { inicio: Date.now(), ejecucion: '', reg: null, libro: null, bqOk: false };

// ============================================================================
// 3. FUNCIONES PRINCIPALES
// ============================================================================

/** Prepara todo. Ejecútela una vez (y cada vez que cambie la configuración). */
function instalar() {
  iniciarContexto_('INSTALACION');
  try {
    const libro = libro_();
    Object.keys(ENCABEZADOS).forEach(nombre => prepararHoja_(libro, nombre, true));
    const porDefecto = libro.getSheets().filter(h => /^(Hoja|Sheet)\s?1$/i.test(h.getName()) && h.getLastRow() === 0);
    porDefecto.forEach(h => libro.deleteSheet(h));
    Log.info('SISTEMA', `Hojas listas en "${libro.getName()}": ${Object.keys(ENCABEZADOS).join(', ')}`);

    const entrada = DriveApp.getFolderById(CONFIG.CARPETA_ENTRADA_ID);
    Log.info('SISTEMA', `Carpeta de entrada accesible: "${entrada.getName()}"`);
    const padres = DriveApp.getFileById(CONFIG.SPREADSHEET_ID).getParents();
    let enCarpeta = false;
    while (padres.hasNext()) if (padres.next().getId() === CONFIG.CARPETA_SHEET_ID) enCarpeta = true;
    if (!enCarpeta) Log.aviso('SISTEMA', 'El Google Sheet no está en la carpeta CARPETA_SHEET_ID indicada (no impide funcionar).');

    validarConfig_();
    asegurarBigQuery_(true);

    if (!DIAS[CONFIG.DIA_EJECUCION]) throw errorFatal_('CONFIG.DIA_EJECUCION no es válido: use MONDAY, TUESDAY, WEDNESDAY...');
    borrarDisparadores_(DISPARADORES);
    ScriptApp.newTrigger('ejecucionSemanal').timeBased()
      .onWeekDay(ScriptApp.WeekDay[CONFIG.DIA_EJECUCION]).atHour(CONFIG.HORA_EJECUCION).create();
    Log.info('SISTEMA', `Ejecución automática: cada ${programacion_()}`);
    if (!CONFIG.EMAIL_RESPONSABLE_CARGA) {
      Log.aviso('SISTEMA', 'CONFIG.EMAIL_RESPONSABLE_CARGA está vacío: el aviso de "falta el CSV" solo le llegará a usted.');
    }
    Log.info('SISTEMA', 'Instalación completada ✔');
  } catch (e) {
    Log.error('SISTEMA', 'La instalación no terminó: ' + e.message, { pista: pista_(e.message) });
    throw e;
  } finally {
    Log.flush();
  }
}

/** Apaga el disparador automático. */
function pausarAutomatizacion() {
  iniciarContexto_('PAUSA');
  borrarDisparadores_(DISPARADORES);
  Log.aviso('SISTEMA', 'Automatización pausada. Ejecute instalar() para reanudar.');
  Log.flush();
}

/** Revisión semanal (disparador: lunes 12:00). Procesa y controla que haya llegado el CSV. */
function ejecucionSemanal() {
  borrarDisparadores_(['recordatorioArchivo']);
  controlSemanal_(ejecutar_(), false);
}

/** Revisión diaria de recordatorio: solo existe mientras falte el CSV de la semana. */
function recordatorioArchivo() {
  borrarDisparadores_(['recordatorioArchivo']);
  controlSemanal_(ejecutar_(), true);
}

/** Continuación automática cuando un archivo no alcanzó a terminar en una ejecución. */
function continuarProceso() {
  borrarDisparadores_(['continuarProceso']);
  ejecutar_();
}

/** Corre el ETL ahora (botón "Procesar ahora" del portal o editor). */
function procesar() {
  const r = ejecutar_();
  return `${r.resultado}: ${r.mensaje}`;
}

/** Corazón del ETL: detecta archivos nuevos y los lleva hasta BigQuery. */
function ejecutar_() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) return { resultado: 'OCUPADO', mensaje: 'Ya hay otra ejecución en curso; se omite esta.', deteccion: false };
  iniciarContexto_(Utilities.formatDate(new Date(), tz_(), 'yyyyMMdd-HHmmss'));
  const props = PropertiesService.getScriptProperties();
  const resumen = { fecha: ahora_(), resultado: 'OK', mensaje: 'Sin archivos nuevos', archivos_nuevos: 0, cargas_trabajadas: 0,
    en_espera: 0, deteccion: false };
  try {
    validarConfig_();
    const det = detectarArchivos_();
    resumen.archivos_nuevos = det.recibidos;
    resumen.en_espera = det.enEspera;
    resumen.deteccion = true;
    for (let i = 0; i < 20; i++) {
      if (tiempoRestante_() < 60000) { resumen.resultado = 'PAUSA'; break; }
      const reg = siguienteCarga_();
      if (!reg) break;
      resumen.cargas_trabajadas++;
      if (!avanzarCarga_(reg)) { resumen.resultado = 'PAUSA'; break; }
    }
    const pendientes = Registro.todos().filter(r => !TERMINALES.includes(r.estado)).length;
    if (pendientes || resumen.en_espera) {
      programarContinuacion_();
      resumen.mensaje = `${pendientes + resumen.en_espera} archivo(s) pendiente(s); continúa solo en ${CONFIG.MINUTOS_CONTINUACION} min`;
    } else {
      resumen.mensaje = resumen.cargas_trabajadas ? 'Cargas al día' : 'Sin archivos nuevos';
    }
  } catch (e) {
    resumen.resultado = 'ERROR';
    resumen.mensaje = e.message;
    Log.error('SISTEMA', 'La ejecución se detuvo: ' + e.message, { pista: pista_(e.message), stack: String(e.stack || '') });
    notificar_('❌ ETL Symmetry: la ejecución se detuvo', `${e.message}\n\nQué hacer: ${pista_(e.message) || 'revise el Log en el portal.'}`);
  } finally {
    Log.flush();
    resumen.duracion_seg = Math.round((Date.now() - Ctx.inicio) / 1000);
    props.setProperty('ULTIMA_EJECUCION', JSON.stringify(resumen));
    lock.releaseLock();
  }
  return resumen;
}

// ============================================================================
// 3b. CONTROL SEMANAL: ¿llegó el CSV de la semana?
// ============================================================================
/**
 * "La semana" = todo lo detectado desde la última vez que se confirmó un CSV válido.
 * Si no hay ninguno → correo a quien carga el archivo (y al dueño) y nueva revisión mañana a la misma hora.
 */
function controlSemanal_(resumen, esRecordatorio) {
  if (!resumen.deteccion) return;   // no se pudo leer la carpeta: ese error ya se registró y notificó
  Ctx.reg = null;
  const props = PropertiesService.getScriptProperties();
  const desde = props.getProperty('SEMANA_CUBIERTA_HASTA') || fecha_(new Date(Date.now() - 7 * 864e5));
  const recientes = Registro.todos().filter(r => String(r.detectado_en) > desde);
  const validos = recientes.filter(r => ![E.ERROR, E.DUPLICADO, E.IGNORADO].includes(r.estado));
  const estado = { fecha: ahora_(), desde, recordatorios: 0, proxima: '' };

  if (validos.length || resumen.en_espera) {
    props.setProperty('SEMANA_CUBIERTA_HASTA', ahora_());
    props.deleteProperty('RECORDATORIOS');
    borrarDisparadores_(['recordatorioArchivo']);
    estado.resultado = 'OK';
    estado.mensaje = validos.length ? `CSV de la semana recibido: ${validos.map(r => r.archivo).join(', ')}`
      : 'CSV de la semana recibido (terminando de subir; se procesa en unos minutos)';
    Log.info('CONTROL_SEMANAL', estado.mensaje);
  } else {
    const n = esRecordatorio ? (Number(props.getProperty('RECORDATORIOS')) || 0) + 1 : 1;
    props.setProperty('RECORDATORIOS', String(n));
    estado.recordatorios = n;
    estado.proxima = n <= CONFIG.MAX_RECORDATORIOS ? programarRecordatorio_() : '';
    const cuando = estado.proxima ? `Se revisará de nuevo el ${estado.proxima}.`
      : `Se revisará de nuevo en la próxima revisión semanal (${programacion_()}).`;
    const noSirven = recientes.length
      ? '\n\nArchivos que llegaron pero no se pudieron usar:\n' +
        recientes.map(r => `• ${r.archivo} → ${r.estado}: ${r.ultimo_mensaje}`).join('\n')
      : '';
    const dest = destinatarios_(true);
    notificar_(n === 1 ? '⚠️ ETL Symmetry: no ha llegado el CSV de esta semana'
      : `⚠️ ETL Symmetry: recordatorio ${n} – falta el CSV de la semana`,
      `Hola:\n\nAl ${ahora_()} no hay un archivo CSV nuevo de Symmetry en la carpeta de carga:\n` +
      `https://drive.google.com/drive/folders/${CONFIG.CARPETA_ENTRADA_ID}\n\n` +
      'Por favor suba el reporte de la semana anterior en formato .csv. El sistema lo procesa solo.' +
      `${noSirven}\n\n${cuando}`, dest);
    estado.resultado = 'FALTA';
    estado.mensaje = `No hay CSV nuevo desde ${desde}. Aviso ${n} enviado a ${dest}.` + (estado.proxima ? ` Próxima revisión: ${estado.proxima}.` : '');
    Log.aviso('CONTROL_SEMANAL', estado.mensaje, noSirven ? recientes.map(r => `${r.archivo}: ${r.estado}`) : undefined);
  }
  props.setProperty('CONTROL_SEMANAL', JSON.stringify(estado));
  Log.flush();
}

function programarContinuacion_() {
  borrarDisparadores_(['continuarProceso']);
  ScriptApp.newTrigger('continuarProceso').timeBased().after(CONFIG.MINUTOS_CONTINUACION * 60000).create();
}

/** Programa la revisión de mañana a HORA_EJECUCION. Devuelve la fecha legible, o '' si mañana ya toca la semanal. */
function programarRecordatorio_() {
  borrarDisparadores_(['recordatorioArchivo']);
  const manana = new Date(Date.now() + 864e5);
  if (Number(Utilities.formatDate(manana, tz_(), 'u')) === DIAS[CONFIG.DIA_EJECUCION][0]) return '';
  const hora = String(CONFIG.HORA_EJECUCION).padStart(2, '0');
  const cuando = new Date(`${Utilities.formatDate(manana, tz_(), 'yyyy-MM-dd')}T${hora}:00:00${Utilities.formatDate(manana, tz_(), 'XXX')}`);
  ScriptApp.newTrigger('recordatorioArchivo').timeBased().at(cuando).create();
  const nombre = Object.values(DIAS).find(d => d[0] === Number(Utilities.formatDate(cuando, tz_(), 'u')))[1];
  return `${nombre} ${Utilities.formatDate(cuando, tz_(), 'dd/MM')} a las ${hora}:00`;
}

function borrarDisparadores_(funciones) {
  ScriptApp.getProjectTriggers().filter(t => funciones.includes(t.getHandlerFunction())).forEach(t => ScriptApp.deleteTrigger(t));
}

function programacion_() {
  return `${DIAS[CONFIG.DIA_EJECUCION] ? DIAS[CONFIG.DIA_EJECUCION][1] : CONFIG.DIA_EJECUCION} a las ${String(CONFIG.HORA_EJECUCION).padStart(2, '0')}:00`;
}

/** Vuelve a procesar desde cero una carga que quedó en ERROR. Ej: reintentarCarga('20260923_101500_ab12cd') */
function reintentarCarga(idCarga) {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(30000)) throw new Error('Hay una ejecución en curso; intente en un minuto.');
  iniciarContexto_('REINTENTO');
  try {
    const reg = Registro.todos().find(r => r.id_carga === idCarga);
    if (!reg) throw new Error('No existe la carga ' + idCarga);
    if (reg.estado !== E.ERROR) throw new Error(`La carga está en estado ${reg.estado}; solo se reintentan cargas en ERROR.`);
    Ctx.reg = reg;
    Object.assign(reg, { estado: E.RECIBIDO, fallo_en: '', intentos: 0, progreso: '', job_id: '', fin: '',
      ultimo_mensaje: 'Reintento solicitado' });
    Registro.guardar(reg);
    moverArchivo_(reg, null);
    Log.info('RECIBO', 'Reintento solicitado: la carga vuelve a empezar desde el CSV original');
    programarContinuacion_();
    return `Reintento programado: se procesará solo en ${CONFIG.MINUTOS_CONTINUACION} minutos (o pulse "Procesar ahora").`;
  } finally {
    Ctx.reg = null;
    Log.flush();
    lock.releaseLock();
  }
}

// ============================================================================
// 4. DETECCIÓN DE ARCHIVOS NUEVOS (control para no subir dos veces lo mismo)
// ============================================================================
function detectarArchivos_() {
  const registros = Registro.todos();
  const vistos = {}, activos = {}, porMd5 = {};
  registros.forEach(r => {
    vistos[r.file_id + '|' + r.archivo_modificado] = true;
    if (!TERMINALES.includes(r.estado)) activos[r.file_id] = true;
    if (r.md5 && r.estado !== E.ERROR && r.estado !== E.IGNORADO && r.estado !== E.DUPLICADO) porMd5[r.md5] = r;
  });

  const nuevos = [];
  const it = conReintentos_(() => DriveApp.getFolderById(CONFIG.CARPETA_ENTRADA_ID).getFiles(), 'leer carpeta de entrada');
  while (it.hasNext()) {
    const f = it.next();
    if (activos[f.getId()] || vistos[f.getId() + '|' + fecha_(f.getLastUpdated())]) continue;
    nuevos.push(f);
  }
  nuevos.sort((a, b) => a.getDateCreated() - b.getDateCreated());

  let recibidos = 0, enEspera = 0;
  nuevos.forEach(f => {
    if (Date.now() - f.getLastUpdated().getTime() < CONFIG.MINUTOS_ESPERA_ARCHIVO * 60000) { enEspera++; return; } // aún subiendo
    const reg = {
      id_carga: Utilities.formatDate(new Date(), tz_(), 'yyyyMMdd_HHmmss') + '_' + Utilities.getUuid().slice(0, 6),
      archivo: f.getName(), file_id: f.getId(), md5: '', tamano_kb: Math.round(f.getSize() / 1024),
      archivo_modificado: fecha_(f.getLastUpdated()), detectado_en: ahora_(), intentos: 0,
    };
    Ctx.reg = reg;
    const mime = f.getMimeType();
    if (mime === MimeType.GOOGLE_SHEETS) {
      ignorar_(reg, 'Se subió convertido a Google Sheets. Súbalo como archivo .csv (sin conversión automática).');
    } else if (!/\.csv$/i.test(f.getName()) && !/csv/i.test(mime)) {
      ignorar_(reg, `No es un archivo CSV (tipo: ${mime}). Solo se procesan archivos .csv`);
    } else if (f.getSize() === 0) {
      ignorar_(reg, 'El archivo está vacío (0 bytes)');
    } else {
      reg.md5 = md5_(f.getBlob());
      const previo = porMd5[reg.md5];
      if (previo) {
        reg.estado = E.DUPLICADO;
        reg.ultimo_mensaje = `Contenido idéntico a "${previo.archivo}" (carga ${previo.id_carga}). No se vuelve a cargar.`;
        Registro.agregar(reg);
        Log.aviso('RECIBO', reg.ultimo_mensaje);
        moverArchivo_(reg, SUBCARPETAS.DUPLICADO);
      } else {
        reg.estado = E.RECIBIDO;
        reg.ultimo_mensaje = 'En cola para procesar';
        Registro.agregar(reg);
        porMd5[reg.md5] = reg;
        recibidos++;
        Log.info('RECIBO', `Archivo nuevo recibido (${reg.tamano_kb} KB, md5 ${reg.md5.slice(0, 8)}…)`);
      }
    }
    Ctx.reg = null;
  });
  return { recibidos, enEspera };
}

function ignorar_(reg, motivo) {
  reg.estado = E.IGNORADO;
  reg.ultimo_mensaje = motivo;
  Registro.agregar(reg);
  Log.aviso('RECIBO', 'Archivo ignorado: ' + motivo);
}

function siguienteCarga_() {
  const activos = Registro.todos().filter(r => !TERMINALES.includes(r.estado));
  // Primero la que ya está en curso (ocupa la hoja Transaccional), luego la más antigua
  activos.sort((a, b) => (a.estado === E.RECIBIDO) - (b.estado === E.RECIBIDO)
    || String(a.detectado_en).localeCompare(String(b.detectado_en)));
  return activos[0] || null;
}

// ============================================================================
// 5. MÁQUINA DE ESTADOS
// ============================================================================
/** Avanza una carga paso a paso. Devuelve false si hay que continuar en otra ejecución. */
function avanzarCarga_(reg) {
  Ctx.reg = reg;
  try {
    while (!TERMINALES.includes(reg.estado)) {
      if (tiempoRestante_() < 45000) {
        Log.info(ETAPA_DE_ESTADO[reg.estado] || 'SISTEMA', `Pausa por tiempo; continúa solo en ${CONFIG.MINUTOS_CONTINUACION} min`);
        return false;
      }
      let listo;
      const estadoAntes = reg.estado;
      try {
        switch (reg.estado) {
          case E.RECIBIDO: case E.ORGANIZANDO: listo = pasoOrganizar_(reg); break;
          case E.ORGANIZADO: case E.ENVIANDO: listo = pasoEnviar_(reg); break;
          case E.ENVIADO: case E.CONSOLIDANDO: listo = pasoConsolidar_(reg); break;
          case E.CONSOLIDADO: listo = pasoVerificar_(reg); break;
          case E.VERIFICADO: listo = pasoLimpiar_(reg); break;
          default: throw errorFatal_('Estado desconocido: ' + reg.estado);
        }
      } catch (e) {
        return manejarFallo_(reg, e, estadoAntes);
      }
      if (!listo) { Registro.guardar(reg); return false; }
      Registro.guardar(reg);
      Log.flush();
    }
    return true;
  } finally {
    Ctx.reg = null;
  }
}

/** Errores fatales → ERROR. Errores temporales → se reintenta en la continuación automática (hasta MAX_INTENTOS). */
function manejarFallo_(reg, e, estadoAntes) {
  const etapa = ETAPA_DE_ESTADO[estadoAntes] || 'SISTEMA';
  const fatal = e.fatal || esFatalTecnico_(e.message);
  reg.intentos = (Number(reg.intentos) || 0) + 1;
  reg.ultimo_mensaje = `[${etapa}] ${e.message}`;
  const detalle = { pista: pista_(e.message), estado: estadoAntes, intento: reg.intentos, stack: String(e.stack || '').slice(0, 1500) };

  if (fatal || reg.intentos >= CONFIG.MAX_INTENTOS) {
    Log.error(etapa, `Carga detenida${fatal ? '' : ` tras ${reg.intentos} intentos`}: ${e.message}`, detalle);
    reg.fallo_en = estadoAntes;
    reg.estado = E.ERROR;
    reg.fin = ahora_();
    reg.job_id = '';
    Registro.guardar(reg);
    Log.flush();
    moverArchivo_(reg, SUBCARPETAS.ERROR);
    registrarCargaBQ_(reg);
    const esDelArchivo = e.fatal && etapa === 'ORGANIZACION';   // problema del CSV: debe saberlo quien lo carga
    notificar_(`❌ ETL Symmetry: error en "${reg.archivo}"`,
      `Carga: ${reg.id_carga}\nEtapa: ${etapa}\nError: ${e.message}\n\nQué hacer: ${detalle.pista || 'revise el Log y la hoja Errores.'}\n` +
      'El CSV original NO se borra. Cuando corrija la causa, pulse "Reintentar" en el portal ' +
      '(o suba de nuevo el CSV corregido).', destinatarios_(esDelArchivo));
    return true;     // sigue con el siguiente archivo
  }
  Log.aviso(etapa, `Intento ${reg.intentos}/${CONFIG.MAX_INTENTOS} falló; se reintenta solo en ${CONFIG.MINUTOS_CONTINUACION} min: ${e.message}`, detalle);
  Registro.guardar(reg);
  return false;      // detiene esta ejecución para no insistir de inmediato
}

// ---------------------------------------------------------------------------
// Paso 1: ORGANIZAR  (CSV → hoja Transaccional)
// ---------------------------------------------------------------------------
function pasoOrganizar_(reg) {
  const etapa = 'ORGANIZACION';
  const archivo = conReintentos_(() => DriveApp.getFileById(reg.file_id), 'abrir el CSV');
  const blob = archivo.getBlob();
  if (reg.md5 && md5_(blob) !== reg.md5) {
    throw errorFatal_('El archivo cambió después de ser recibido. La nueva versión se registrará como una carga nueva.');
  }
  const matriz = parsearCsv_(leerTexto_(blob));
  const res = organizarRegistros_(matriz, reg);
  const est = res.stats;
  ['filas_csv', 'transacciones', 'validas', 'rechazadas', 'sin_cedula', 'duplicadas_archivo', 'fecha_min', 'fecha_max']
    .forEach(k => reg[k] = est[k]);

  if (est.validas === 0) {
    registrarErrores_(reg, res.errores);
    throw errorFatal_('No se encontró ninguna transacción válida en el archivo.');
  }
  const pct = est.rechazadas / Math.max(1, est.transacciones);
  if (pct > CONFIG.UMBRAL_ERRORES) {
    registrarErrores_(reg, res.errores);
    throw errorFatal_(`${(pct * 100).toFixed(2)} % de las transacciones tiene errores (umbral ${CONFIG.UMBRAL_ERRORES * 100} %). ` +
      'Revise la hoja Errores; el archivo no se cargó.');
  }

  const hoja = hoja_(HOJAS.TX);
  const n = res.filas.length;
  let desde = reg.estado === E.ORGANIZANDO ? (Number(reg.progreso) || 0) : 0;
  if (desde > 0 && !continuidadValida_(hoja, reg, desde)) {
    Log.aviso(etapa, 'La hoja Transaccional no coincide con el avance guardado; se reescribe desde cero');
    desde = 0;
  }
  if (desde === 0) {
    reg.inicio = reg.inicio || ahora_();
    Log.info(etapa, `CSV leído: ${est.filas_csv} filas crudas → ${est.transacciones} transacciones ` +
      `(${est.validas} válidas, ${est.rechazadas} rechazadas, ${est.duplicadas_archivo} repetidas, ` +
      `${est.sin_cedula} sin cédula, ${est.nombres_huerfanos} nombres sin transacción). Rango: ${est.fecha_min} a ${est.fecha_max}`, est);
    verificarCupoCeldas_(hoja, n);
    prepararTransaccional_(hoja, n);
  }
  reg.estado = E.ORGANIZANDO;
  reg.progreso = desde;
  Registro.guardar(reg);

  while (desde < n) {
    if (tiempoRestante_() < 60000) {
      Log.info(etapa, `Escritas ${desde} de ${n} filas en el Sheet; continúa solo en ${CONFIG.MINUTOS_CONTINUACION} min`);
      return false;
    }
    const lote = res.filas.slice(desde, desde + LOTE_ESCRITURA);
    conReintentos_(() => {
      hoja.getRange(2 + desde, 1, lote.length, COLUMNAS_TX.length).setValues(lote);
      SpreadsheetApp.flush();
    }, 'escribir lote en el Sheet');
    desde += lote.length;
    reg.progreso = desde;
    Registro.guardar(reg);
  }

  // Vuelta: confirmar que el Sheet quedó con exactamente lo que se escribió
  const enHoja = hoja.getLastRow() - 1;
  if (enHoja !== n) {
    reg.estado = E.RECIBIDO;
    throw new Error(`Verificación del Sheet: se escribieron ${n} filas pero la hoja tiene ${enHoja}.`);
  }
  registrarErrores_(reg, res.errores);
  reg.estado = E.ORGANIZADO;
  reg.progreso = n;
  Log.info(etapa, `✔ ${n} transacciones organizadas en la hoja "${HOJAS.TX}"`);
  return true;
}

function continuidadValida_(hoja, reg, desde) {
  if (hoja.getLastRow() - 1 !== desde) return false;
  return String(hoja.getRange(desde + 1, COL.id_carga + 1).getValue()) === reg.id_carga;
}

// ---------------------------------------------------------------------------
// Paso 2: ENVIAR  (hoja Transaccional → BigQuery staging)
// ---------------------------------------------------------------------------
function pasoEnviar_(reg, recienLanzado) {
  const etapa = 'ENVIO';
  asegurarBigQuery_();

  if (reg.estado === E.ENVIANDO && reg.job_id) {
    const r = esperarJob_(reg.job_id, 120000);
    if (r.noExiste) {
      reg.job_id = ''; reg.estado = E.ORGANIZADO;
      if (recienLanzado) throw new Error('BigQuery no encuentra el trabajo recién lanzado (revise CONFIG.UBICACION_BQ).');
      return pasoEnviar_(reg);
    }
    if (!r.terminado) { Log.info(etapa, `BigQuery sigue recibiendo la carga; se revisa en ${CONFIG.MINUTOS_CONTINUACION} min`); return false; }
    reg.job_id = '';
    if (r.error) { reg.estado = E.ORGANIZADO; throw errorDeJob_('La carga a BigQuery (staging) falló', r.job); }
    const recibidas = Number(r.job.statistics.load.outputRows);
    if (recibidas !== Number(reg.validas)) {
      reg.estado = E.ORGANIZADO;
      throw new Error(`BigQuery recibió ${recibidas} filas y se enviaron ${reg.validas}.`);
    }
    reg.estado = E.ENVIADO;
    Log.info(etapa, `✔ BigQuery confirmó ${recibidas} filas en ${TABLAS.STAGING} (ida y vuelta OK)`);
    return true;
  }

  // Ida: leer el Sheet y revalidar (alguien pudo editarlo a mano)
  const filas = leerYValidarTransaccional_(reg);
  const ndjson = filas.map(f => {
    const o = {};
    COLUMNAS_TX.forEach((c, i) => {
      const v = String(f[i]).trim();
      o[c] = v === '' ? null : (c === 'fila_origen' ? Number(v) : v);
    });
    return JSON.stringify(o);
  }).join('\n');
  const blob = Utilities.newBlob(ndjson, 'application/octet-stream');
  if (blob.getBytes().length > 45 * 1024 * 1024) {
    throw errorFatal_('El archivo supera 45 MB al enviarlo a BigQuery; divídalo en dos CSV.');
  }

  reg.job_id = `etl_envio_${reg.id_carga}_${Date.now()}`;
  reg.estado = E.ENVIANDO;
  Registro.guardar(reg);   // se guarda ANTES de lanzar: si algo se corta, se sabe qué job revisar
  lanzarJob_({
    jobReference: { projectId: CONFIG.PROJECT_ID, jobId: reg.job_id, location: CONFIG.UBICACION_BQ },
    configuration: { load: {
      destinationTable: tabla_(TABLAS.STAGING), sourceFormat: 'NEWLINE_DELIMITED_JSON',
      writeDisposition: 'WRITE_TRUNCATE', createDisposition: 'CREATE_IF_NEEDED',
      schema: { fields: ESQUEMA_TX }, maxBadRecords: 0,
    } },
  }, blob);
  Log.info(etapa, `${filas.length} filas enviadas a BigQuery (job ${reg.job_id})`);
  return pasoEnviar_(reg, true);
}

function leerYValidarTransaccional_(reg) {
  const hoja = hoja_(HOJAS.TX);
  const n = hoja.getLastRow() - 1;
  const volverAOrganizar = (motivo, problemas) => {
    if (problemas) registrarErrores_(reg, problemas);
    reg.estado = E.RECIBIDO;   // el CSV original es la fuente: se regenera la hoja
    throw new Error(motivo + ' La hoja se regenerará desde el CSV original.');
  };
  const encabezado = hoja.getRange(1, 1, 1, COLUMNAS_TX.length).getValues()[0].map(String);
  if (encabezado.join('|') !== COLUMNAS_TX.join('|')) volverAOrganizar('Los encabezados de "Transaccional" fueron modificados.');
  if (n !== Number(reg.validas)) volverAOrganizar(`"Transaccional" tiene ${n} filas y se esperaban ${reg.validas} (¿se editó a mano?).`);

  const valores = conReintentos_(() => hoja.getRange(2, 1, n, COLUMNAS_TX.length).getValues(), 'leer el Sheet');
  const problemas = [];
  const ids = {};
  valores.forEach((v, i) => {
    const fila = i + 2, id = String(v[COL.id_transaccion]).trim();
    const p = [];
    if (String(v[COL.id_carga]) !== reg.id_carga) p.push('id_carga no corresponde a esta carga');
    if (!/^\d{1,18}$/.test(id)) p.push('id_transaccion no numérico');
    else if (ids[id]) p.push(`id_transaccion repetido (fila ${ids[id]})`);
    if (!/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(String(v[COL.fecha_hora]))) p.push('fecha_hora sin formato YYYY-MM-DD HH:MM:SS');
    ids[id] = fila;
    if (p.length) problemas.push({ fila: `Sheet ${fila}`, tipo: 'SHEET_MODIFICADO', detalle: p.join('; '), datos: v.join(' | ') });
  });
  if (problemas.length) volverAOrganizar(`${problemas.length} fila(s) de "Transaccional" no pasan la validación de regreso.`, problemas);
  return valores;
}

// ---------------------------------------------------------------------------
// Paso 3: CONSOLIDAR  (staging → tabla final, sin duplicar por id_transaccion)
// ---------------------------------------------------------------------------
function pasoConsolidar_(reg, recienLanzado) {
  const etapa = 'CONSOLIDACION';
  if (reg.estado === E.CONSOLIDANDO && reg.job_id) {
    const r = esperarJob_(reg.job_id, 120000);
    if (r.noExiste) {
      reg.job_id = ''; reg.estado = E.ENVIADO;
      if (recienLanzado) throw new Error('BigQuery no encuentra el trabajo recién lanzado (revise CONFIG.UBICACION_BQ).');
      return pasoConsolidar_(reg);
    }
    if (!r.terminado) { Log.info(etapa, `BigQuery sigue consolidando; se revisa en ${CONFIG.MINUTOS_CONTINUACION} min`); return false; }
    reg.job_id = '';
    if (r.error) { reg.estado = E.ENVIADO; throw errorDeJob_('La consolidación en BigQuery falló', r.job); }
    reg.estado = E.CONSOLIDADO;
    Log.info(etapa, `✔ Staging consolidado en la tabla ${TABLAS.FINAL}`);
    return true;
  }
  const cols = ESQUEMA_TX.map(c => 's.' + c.name).join(', ');
  const sql = `SELECT ${cols}, CURRENT_TIMESTAMP() AS cargado_en
FROM ${ref_(TABLAS.STAGING)} s
WHERE s.id_carga = @id_carga
  AND NOT EXISTS (SELECT 1 FROM ${ref_(TABLAS.FINAL)} t WHERE t.id_transaccion = s.id_transaccion)
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.id_transaccion ORDER BY s.fila_origen) = 1`;
  reg.job_id = `etl_consolida_${reg.id_carga}_${Date.now()}`;
  reg.estado = E.CONSOLIDANDO;
  Registro.guardar(reg);
  lanzarJob_({
    jobReference: { projectId: CONFIG.PROJECT_ID, jobId: reg.job_id, location: CONFIG.UBICACION_BQ },
    configuration: { query: {
      query: sql, useLegacySql: false, destinationTable: tabla_(TABLAS.FINAL),
      writeDisposition: 'WRITE_APPEND', createDisposition: 'CREATE_NEVER',
      parameterMode: 'NAMED', queryParameters: [paramTexto_('id_carga', reg.id_carga)],
    } },
  });
  Log.info(etapa, `Consolidación lanzada (job ${reg.job_id}): solo entran id_transaccion que no existan`);
  return pasoConsolidar_(reg, true);
}

// ---------------------------------------------------------------------------
// Paso 4: VERIFICAR  (vuelta: ¿todo lo del archivo quedó en la tabla final?)
// ---------------------------------------------------------------------------
function pasoVerificar_(reg) {
  const etapa = 'VERIFICACION';
  const sql = `SELECT
  (SELECT COUNT(*) FROM ${ref_(TABLAS.STAGING)} WHERE id_carga = @id_carga) AS en_staging,
  COUNT(DISTINCT t.id_transaccion) AS presentes,
  COUNT(t.id_transaccion) AS filas_final,
  COUNTIF(t.id_carga = @id_carga) AS insertadas
FROM ${ref_(TABLAS.STAGING)} s
JOIN ${ref_(TABLAS.FINAL)} t ON t.id_transaccion = s.id_transaccion
WHERE s.id_carga = @id_carga`;
  const f = consultar_(sql, [paramTexto_('id_carga', reg.id_carga)])[0];
  const esperadas = Number(reg.validas);
  const v = { en_staging: Number(f[0]), presentes: Number(f[1]), filas_final: Number(f[2]), insertadas: Number(f[3]) };

  if (v.en_staging !== esperadas) {
    reg.estado = E.RECIBIDO;
    throw new Error(`Staging tiene ${v.en_staging} filas de esta carga y se esperaban ${esperadas}; se rehace la carga.`);
  }
  if (v.presentes !== esperadas) {
    reg.estado = E.ENVIADO;
    throw new Error(`Solo ${v.presentes} de ${esperadas} transacciones están en la tabla final; se repite la consolidación.`);
  }
  if (v.filas_final !== v.presentes) {
    throw errorFatal_(`La tabla final tiene ${v.filas_final - v.presentes} filas duplicadas por id_transaccion. ` +
      'Revise la tabla en BigQuery antes de reintentar.');
  }
  reg.insertadas_bq = v.insertadas;
  reg.ya_existian_bq = esperadas - v.insertadas;
  reg.estado = E.VERIFICADO;
  Log.info(etapa, `✔ Verificado: las ${esperadas} transacciones están en BigQuery ` +
    `(${reg.insertadas_bq} nuevas, ${reg.ya_existian_bq} ya existían)`, v);
  return true;
}

// ---------------------------------------------------------------------------
// Paso 5: LIMPIAR  (solo después de verificar)
// ---------------------------------------------------------------------------
function pasoLimpiar_(reg) {
  const etapa = 'LIMPIEZA';
  const hoja = hoja_(HOJAS.TX);
  const n = hoja.getLastRow() - 1;
  if (n > 0) {
    if (String(hoja.getRange(2, COL.id_carga + 1).getValue()) === reg.id_carga) {
      limpiarTransaccional_(hoja);
      Log.info(etapa, `Hoja "${HOJAS.TX}" limpia (${n} filas retiradas). Lista para el siguiente archivo`);
    } else {
      Log.aviso(etapa, `La hoja "${HOJAS.TX}" tiene datos de otra carga; no se toca`);
    }
  }
  reg.estado = E.COMPLETADO;
  reg.fin = ahora_();
  reg.progreso = '';
  reg.ultimo_mensaje = `OK: ${reg.insertadas_bq} nuevas en BigQuery, ${reg.ya_existian_bq} ya existían`;
  Registro.guardar(reg);
  moverArchivo_(reg, SUBCARPETAS.OK);
  registrarCargaBQ_(reg);
  Log.info(etapa, `✔ Carga COMPLETADA en ${duracionSeg_(reg)} s`);
  if (CONFIG.NOTIFICAR_EXITO) {
    notificar_(`✅ ETL Symmetry: "${reg.archivo}" cargado`,
      `Transacciones en el archivo: ${reg.transacciones}\nVálidas: ${reg.validas}\nNuevas en BigQuery: ${reg.insertadas_bq}\n` +
      `Ya existían: ${reg.ya_existian_bq}\nRechazadas: ${reg.rechazadas}\nSin cédula: ${reg.sin_cedula}\n` +
      `Rango de fechas: ${reg.fecha_min} a ${reg.fecha_max}`);
  }
  return true;
}

// ============================================================================
// 6. TRANSFORMACIÓN  (une las filas desfasadas y limpia los datos)
// ============================================================================
/**
 * El CSV trae cada transacción partida en varias filas:
 *   fila A: nombres y apellidos
 *   fila B: cédula (Text10), fecha, lugar (Text6) e id (Text8)
 * Regla: se acumulan las filas "sin transacción" (nombres/cédula) y se cierran con la
 * siguiente fila que trae datos de transacción. Funciona también si algún día el CSV viene alineado.
 */
function organizarRegistros_(matriz, meta) {
  if (!matriz || !matriz.length) throw errorFatal_('El archivo está vacío.');
  const idx = mapearEncabezados_(matriz[0]);
  const stats = { filas_csv: matriz.length - 1, transacciones: 0, validas: 0, rechazadas: 0, sin_cedula: 0,
    duplicadas_archivo: 0, nombres_huerfanos: 0, fecha_min: '', fecha_max: '' };
  const filas = [], errores = [], vistos = {};
  let pendiente = null;

  const huerfano = p => {
    stats.nombres_huerfanos++;
    errores.push({ fila: p.fila, tipo: 'NOMBRE_SIN_TRANSACCION', detalle: 'Fila de nombre sin transacción asociada (advertencia, se omite)',
      datos: p.crudo.join(' | ') });
  };

  for (let i = 1; i < matriz.length; i++) {
    const f = matriz[i];
    const filaOrigen = i + 1;
    const ced = limpiar_(f[idx.cedula]), nom = limpiar_(f[idx.nombres]), ape = limpiar_(f[idx.apellidos]);
    const fec = limpiar_(f[idx.fecha]), lug = limpiar_(f[idx.lugar]), id = limpiar_(f[idx.id]);
    if (!ced && !nom && !ape && !fec && !lug && !id) continue;             // fila vacía

    if (!fec && !lug && !id) {                                             // pieza de nombre/cédula
      if (pendiente && (nom || ape) && (pendiente.nombres || pendiente.apellidos)) { huerfano(pendiente); pendiente = null; }
      if (!pendiente) pendiente = { cedula: '', nombres: '', apellidos: '', fila: filaOrigen, crudo: [] };
      if (ced) pendiente.cedula = ced;
      if (nom) pendiente.nombres = nom;
      if (ape) pendiente.apellidos = ape;
      pendiente.crudo.push(f.join(','));
      continue;
    }

    const p = pendiente || { cedula: '', nombres: '', apellidos: '', crudo: [] };  // fila de transacción
    pendiente = null;
    stats.transacciones++;
    const crudo = p.crudo.concat(f.join(',')).join(' | ');
    const idNum = id.replace(/^0+(?=\d)/, '');
    const fecha = parsearFecha_(fec);
    const problemas = [];
    if (!id) problemas.push(['SIN_ID', 'Text8 (id de transacción) vacío']);
    else if (!/^\d{1,18}$/.test(idNum)) problemas.push(['ID_INVALIDO', `Text8 no es numérico: "${id}"`]);
    if (!fecha) problemas.push(['FECHA_INVALIDA', `Fecha no reconocida: "${fec}"`]);
    if (problemas.length) {
      stats.rechazadas++;
      errores.push({ fila: filaOrigen, tipo: problemas.map(x => x[0]).join('+'), detalle: problemas.map(x => x[1]).join('; '), datos: crudo });
      continue;
    }
    if (vistos[idNum]) {
      stats.duplicadas_archivo++;
      errores.push({ fila: filaOrigen, tipo: 'DUPLICADO_EN_ARCHIVO', detalle: `Text8 ${idNum} ya apareció en la fila ${vistos[idNum]} (se conserva la primera)`, datos: crudo });
      continue;
    }
    vistos[idNum] = filaOrigen;
    const cedula = normalizarCedula_(ced || p.cedula);
    if (!cedula) stats.sin_cedula++;
    if (!stats.fecha_min || fecha < stats.fecha_min) stats.fecha_min = fecha;
    if (!stats.fecha_max || fecha > stats.fecha_max) stats.fecha_max = fecha;
    filas.push([idNum, cedula, nom || p.nombres, ape || p.apellidos, fecha, fec, lug, sentido_(lug),
      meta.id_carga, meta.archivo, filaOrigen]);
  }
  if (pendiente && (pendiente.nombres || pendiente.apellidos)) huerfano(pendiente);
  stats.validas = filas.length;
  return { filas, errores, stats };
}

function mapearEncabezados_(encabezado) {
  const norm = encabezado.map(h => String(h).replace(/^﻿/, '').trim().toLowerCase());
  const idx = {}, faltan = [];
  Object.keys(ENCABEZADOS_CSV).forEach(k => {
    const pos = norm.findIndex(h => ENCABEZADOS_CSV[k].includes(h));
    if (pos === -1) faltan.push(ENCABEZADOS_CSV[k][0]); else idx[k] = pos;
  });
  if (faltan.length) {
    throw errorFatal_(`Faltan columnas en el CSV: ${faltan.join(', ')}. Encabezados recibidos: ${encabezado.join(', ')}`);
  }
  return idx;
}

const MESES = {
  january: 1, jan: 1, enero: 1, ene: 1, february: 2, feb: 2, febrero: 2, march: 3, mar: 3, marzo: 3,
  april: 4, apr: 4, abril: 4, abr: 4, may: 5, mayo: 5, june: 6, jun: 6, junio: 6, july: 7, jul: 7, julio: 7,
  august: 8, aug: 8, agosto: 8, ago: 8, september: 9, sep: 9, sept: 9, septiembre: 9, setiembre: 9, set: 9,
  october: 10, oct: 10, octubre: 10, november: 11, nov: 11, noviembre: 11,
  december: 12, dec: 12, diciembre: 12, dic: 12,
};

/** "15 September 2025 23:58" → "2025-09-15 23:58:00" (formato DATETIME de BigQuery). null si no es válida. */
function parsearFecha_(txt) {
  const t = String(txt || '').trim();
  let a, mes, d, h, mi, s;
  let m = t.match(/^(\d{4})-(\d{1,2})-(\d{1,2})[ T](\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (m) {
    [a, mes, d, h, mi, s] = [+m[1], +m[2], +m[3], +m[4], +m[5], +(m[6] || 0)];
  } else {
    m = t.match(/^(\d{1,2})[\s\-\/]+([a-záéíóú]+)\.?[\s\-\/,]+(\d{4})[\s,T]+(\d{1,2}):(\d{2})(?::(\d{2}))?(?:\s*([ap])\.?\s*m\.?)?$/i);
    if (!m) return null;
    mes = MESES[m[2].toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '')];
    [d, a, h, mi, s] = [+m[1], +m[3], +m[4], +m[5], +(m[6] || 0)];
    if (m[7]) {
      if (h < 1 || h > 12) return null;
      const pm = m[7].toLowerCase() === 'p';
      h = pm ? (h % 12) + 12 : h % 12;
    }
  }
  if (!mes || mes > 12 || a < 2000 || a > 2100 || h > 23 || mi > 59 || s > 59) return null;
  const diasMes = new Date(Date.UTC(a, mes, 0)).getUTCDate();
  if (d < 1 || d > diasMes) return null;
  const p2 = x => String(x).padStart(2, '0');
  return `${a}-${p2(mes)}-${p2(d)} ${p2(h)}:${p2(mi)}:${p2(s)}`;
}

function normalizarCedula_(c) {
  const limpio = String(c || '').replace(/[.\s,]/g, '').toUpperCase();
  return limpio;
}

function sentido_(lugar) {
  if (/\bentrada\b/i.test(lugar)) return 'ENTRADA';
  if (/\bsalida\b/i.test(lugar)) return 'SALIDA';
  return 'OTRO';
}

function limpiar_(v) {
  return v === undefined || v === null ? '' : String(v).replace(/\s+/g, ' ').trim();
}

/** Lee el CSV detectando la codificación (UTF-8 o Windows-1252/Latin-1, que es como llega hoy). */
function leerTexto_(blob) {
  let texto = blob.getDataAsString('UTF-8');
  if (texto.indexOf('�') !== -1) {
    try { texto = blob.getDataAsString('windows-1252'); } catch (e) { texto = blob.getDataAsString('ISO-8859-1'); }
  }
  return texto.replace(/^﻿/, '');
}

/** Interpreta el CSV detectando el separador (, ; o tabulador). */
function parsearCsv_(texto) {
  const t = texto.replace(/\r\n?/g, '\n');
  const primera = t.slice(0, t.indexOf('\n') === -1 ? t.length : t.indexOf('\n'));
  const cuenta = ch => primera.split(ch).length;
  const sep = cuenta(';') > cuenta(',') ? ';' : (cuenta('\t') > cuenta(',') ? '\t' : ',');
  try {
    return Utilities.parseCsv(t, sep);
  } catch (e) {
    throw errorFatal_('El CSV no se pudo leer (¿comillas sin cerrar o archivo dañado?): ' + e.message);
  }
}

// ============================================================================
// 7. GOOGLE SHEETS
// ============================================================================
function libro_() {
  if (!Ctx.libro) Ctx.libro = conReintentos_(() => SpreadsheetApp.openById(CONFIG.SPREADSHEET_ID), 'abrir el Google Sheet');
  return Ctx.libro;
}

function hoja_(nombre) {
  return libro_().getSheetByName(nombre) || prepararHoja_(libro_(), nombre, false);
}

/** Crea la hoja si falta y le deja encabezados limpios, formato texto y el tamaño justo. */
function prepararHoja_(libro, nombre, reescribirEncabezado) {
  let hoja = libro.getSheetByName(nombre);
  const nueva = !hoja;
  if (nueva) hoja = libro.insertSheet(nombre);
  const cols = ENCABEZADOS[nombre];
  ajustarColumnas_(hoja, cols.length);
  if (nueva || reescribirEncabezado) {
    hoja.getRange(1, 1, hoja.getMaxRows(), cols.length).setNumberFormat('@');
    hoja.getRange(1, 1, 1, cols.length).setValues([cols]).setFontWeight('bold').setBackground('#e8eaed');
    hoja.setFrozenRows(1);
  }
  if (nombre === HOJAS.TX) {
    if (nueva && hoja.getMaxRows() > 2) hoja.deleteRows(3, hoja.getMaxRows() - 2);
    if (!hoja.getProtections(SpreadsheetApp.ProtectionType.SHEET).length) {
      hoja.protect().setDescription('Hoja gestionada por el ETL: no editar a mano').setWarningOnly(true);
    }
  }
  return hoja;
}

function ajustarColumnas_(hoja, n) {
  const m = hoja.getMaxColumns();
  if (m > n) hoja.deleteColumns(n + 1, m - n);
  else if (m < n) hoja.insertColumnsAfter(m, n - m);
}

function prepararTransaccional_(hoja, n) {
  conReintentos_(() => {
    hoja.clearContents();
    ajustarColumnas_(hoja, COLUMNAS_TX.length);
    const filas = n + 1, max = hoja.getMaxRows();
    if (max < filas) hoja.insertRowsAfter(max, filas - max);
    else if (max > filas) hoja.deleteRows(filas + 1, max - filas);
    hoja.getRange(1, 1, filas, COLUMNAS_TX.length).setNumberFormat('@');
    hoja.getRange(1, 1, 1, COLUMNAS_TX.length).setValues([COLUMNAS_TX]).setFontWeight('bold');
    SpreadsheetApp.flush();
  }, 'preparar la hoja Transaccional');
}

function limpiarTransaccional_(hoja) {
  conReintentos_(() => {
    hoja.clearContents();
    if (hoja.getMaxRows() > 2) hoja.deleteRows(3, hoja.getMaxRows() - 2);
    hoja.getRange(1, 1, 1, COLUMNAS_TX.length).setValues([COLUMNAS_TX]).setFontWeight('bold');
    SpreadsheetApp.flush();
  }, 'limpiar la hoja Transaccional');
}

function contarCeldas_(libro) {
  return libro.getSheets().reduce((t, h) => t + h.getMaxRows() * h.getMaxColumns(), 0);
}

function verificarCupoCeldas_(hojaTx, n) {
  const otras = contarCeldas_(libro_()) - hojaTx.getMaxRows() * hojaTx.getMaxColumns();
  const necesarias = (n + 1) * COLUMNAS_TX.length;
  if (otras + necesarias > LIMITE_CELDAS_SHEET * 0.95) {
    throw errorFatal_(`El archivo necesita ${necesarias.toLocaleString()} celdas y el Sheet ya usa ${otras.toLocaleString()} ` +
      `(límite ${LIMITE_CELDAS_SHEET.toLocaleString()}). Divida el CSV o libere hojas.`);
  }
}

function anexarFilas_(hoja, filas) {
  if (!filas.length) return;
  const inicio = hoja.getLastRow() + 1;
  const faltan = inicio + filas.length - 1 - hoja.getMaxRows();
  if (faltan > 0) hoja.insertRowsAfter(hoja.getMaxRows(), faltan);
  hoja.getRange(inicio, 1, filas.length, filas[0].length).setNumberFormat('@').setValues(filas);
}

function recortarHoja_(hoja, max) {
  const n = hoja.getLastRow() - 1;
  if (n > max) hoja.deleteRows(2, n - Math.floor(max * 0.8));
}

/** Hoja Control_Archivos: una fila por archivo detectado. */
const Registro = {
  todos() {
    const h = hoja_(HOJAS.CONTROL);
    const n = h.getLastRow() - 1;
    if (n < 1) return [];
    return h.getRange(2, 1, n, COLUMNAS_CONTROL.length).getValues().map((f, i) => {
      const o = { _fila: i + 2 };
      COLUMNAS_CONTROL.forEach((c, j) => o[c] = String(f[j]));
      return o;
    });
  },
  aFila_(reg) {
    return COLUMNAS_CONTROL.map(c => (reg[c] === undefined || reg[c] === null ? '' : String(reg[c]).slice(0, 5000)));
  },
  agregar(reg) {
    const h = hoja_(HOJAS.CONTROL);
    anexarFilas_(h, [this.aFila_(reg)]);
    reg._fila = h.getLastRow();
  },
  guardar(reg) {
    conReintentos_(() => hoja_(HOJAS.CONTROL).getRange(reg._fila, 1, 1, COLUMNAS_CONTROL.length)
      .setNumberFormat('@').setValues([this.aFila_(reg)]), 'guardar control de archivos');
  },
};

/** Log estructurado: se acumula en memoria y se escribe en bloque (rápido). */
const Log = {
  buffer: [],
  info(etapa, mensaje, detalle) { this.agregar_('INFO', etapa, mensaje, detalle); },
  aviso(etapa, mensaje, detalle) { this.agregar_('ADVERTENCIA', etapa, mensaje, detalle); },
  error(etapa, mensaje, detalle) { this.agregar_('ERROR', etapa, mensaje, detalle); },
  agregar_(nivel, etapa, mensaje, detalle) {
    const r = Ctx.reg;
    const d = detalle === undefined ? '' : (typeof detalle === 'string' ? detalle : JSON.stringify(detalle));
    this.buffer.push([ahora_(), Ctx.ejecucion, r ? r.id_carga : '', r ? r.archivo : '', etapa, nivel, mensaje, d.slice(0, 5000)]);
    console.log(`[${nivel}] ${etapa} ${r ? r.archivo + ' ' : ''}- ${mensaje}`);
  },
  flush() {
    if (!this.buffer.length) return;
    const filas = this.buffer;
    this.buffer = [];
    try {
      const h = hoja_(HOJAS.LOG);
      anexarFilas_(h, filas);
      recortarHoja_(h, CONFIG.MAX_FILAS_LOG);
    } catch (e) {
      console.error('No se pudo escribir el Log en el Sheet: ' + e.message + '\n' + JSON.stringify(filas));
    }
  },
};

function registrarErrores_(reg, lista) {
  if (!lista || !lista.length) return;
  const max = CONFIG.MAX_ERRORES_POR_ARCHIVO;
  const filas = lista.slice(0, max).map(e => [ahora_(), reg.id_carga, reg.archivo, String(e.fila || ''), e.tipo,
    e.detalle, String(e.datos || '').slice(0, 1000)]);
  if (lista.length > max) filas.push([ahora_(), reg.id_carga, reg.archivo, '', 'RESUMEN', `Se omitieron ${lista.length - max} registros más`, '']);
  try {
    const h = hoja_(HOJAS.ERRORES);
    anexarFilas_(h, filas);
    recortarHoja_(h, 20000);
  } catch (e) {
    Log.aviso('SISTEMA', 'No se pudo escribir en la hoja Errores: ' + e.message);
  }
}

// ============================================================================
// 8. BIGQUERY
// ============================================================================
function tabla_(id) { return { projectId: CONFIG.PROJECT_ID, datasetId: CONFIG.DATASET, tableId: id }; }
function ref_(id) { return '`' + [CONFIG.PROJECT_ID, CONFIG.DATASET, id].join('.') + '`'; }
function paramTexto_(nombre, valor) {
  return { name: nombre, parameterType: { type: 'STRING' }, parameterValue: { value: valor } };
}

/** Crea dataset y tablas si no existen. */
function asegurarBigQuery_(informar) {
  if (Ctx.bqOk) return;
  const P = CONFIG.PROJECT_ID, D = CONFIG.DATASET;
  let ds;
  try {
    ds = BigQuery.Datasets.get(P, D);
  } catch (e) {
    if (!/not found/i.test(e.message)) throw e;
    ds = BigQuery.Datasets.insert({ datasetReference: { projectId: P, datasetId: D }, location: CONFIG.UBICACION_BQ,
      description: 'ETL Symmetry: transacciones de acceso (tarjetas)' }, P);
    Log.info('SISTEMA', `Dataset ${P}.${D} creado en ${CONFIG.UBICACION_BQ}`);
  }
  if (ds.defaultTableExpirationMs && informar) {
    Log.aviso('SISTEMA', 'El proyecto parece estar en modo Sandbox (las tablas vencen a los 60 días). ' +
      'Habilite facturación para no perder datos; el uso de este ETL cabe en la capa gratuita.');
  }
  asegurarTabla_(TABLAS.STAGING, { schema: { fields: ESQUEMA_TX }, description: 'Zona intermedia: última carga enviada' });
  asegurarTabla_(TABLAS.FINAL, { schema: { fields: ESQUEMA_FINAL }, description: 'Transacciones consolidadas (sin duplicados)',
    timePartitioning: { type: 'DAY', field: 'fecha_hora' }, clustering: { fields: ['cedula', 'lugar'] } });
  asegurarTabla_(TABLAS.CARGAS, { schema: { fields: ESQUEMA_CARGAS }, description: 'Auditoría: una fila por archivo procesado' });
  if (informar) Log.info('SISTEMA', `BigQuery listo: ${P}.${D} (${Object.values(TABLAS).join(', ')})`);
  Ctx.bqOk = true;
}

function asegurarTabla_(id, definicion) {
  try {
    BigQuery.Tables.get(CONFIG.PROJECT_ID, CONFIG.DATASET, id);
  } catch (e) {
    if (!/not found/i.test(e.message)) throw e;
    BigQuery.Tables.insert(Object.assign({ tableReference: tabla_(id) }, definicion), CONFIG.PROJECT_ID, CONFIG.DATASET);
    Log.info('SISTEMA', `Tabla ${CONFIG.DATASET}.${id} creada`);
  }
}

function lanzarJob_(job, blob) {
  return conReintentos_(() => {
    try {
      return blob ? BigQuery.Jobs.insert(job, CONFIG.PROJECT_ID, blob) : BigQuery.Jobs.insert(job, CONFIG.PROJECT_ID);
    } catch (e) {
      if (/already exists/i.test(e.message)) return job;   // un intento previo sí llegó
      throw e;
    }
  }, 'lanzar trabajo en BigQuery');
}

/** Espera un job. {terminado, error, job} o {noExiste:true}. */
function esperarJob_(jobId, maxMs) {
  const limite = Date.now() + Math.min(maxMs, Math.max(0, tiempoRestante_() - 40000));
  for (;;) {
    let job;
    try {
      job = conReintentos_(() => BigQuery.Jobs.get(CONFIG.PROJECT_ID, jobId, { location: CONFIG.UBICACION_BQ }), 'consultar trabajo');
    } catch (e) {
      if (/not found: job/i.test(e.message)) return { noExiste: true };
      throw e;
    }
    if (job.status.state === 'DONE') return { terminado: true, error: !!job.status.errorResult, job };
    if (Date.now() > limite) return { terminado: false, job };
    Utilities.sleep(3000);
  }
}

/** Convierte los errores que devuelve BigQuery en un mensaje claro y los guarda en la hoja Errores. */
function errorDeJob_(titulo, job) {
  const er = job.status.errorResult || {};
  const lista = (job.status.errors || []).slice(0, 50);
  registrarErrores_(Ctx.reg, lista.map(x => ({ fila: x.location || '', tipo: 'BIGQUERY_' + (x.reason || 'ERROR').toUpperCase(),
    detalle: x.message, datos: '' })));
  const msg = `${titulo}: ${er.reason || ''} ${er.message || ''}`.trim();
  const e = new Error(msg + (lista.length > 1 ? ` (+${lista.length - 1} detalles en hoja Errores)` : ''));
  e.fatal = ['invalid', 'invalidQuery', 'accessDenied', 'billingNotEnabled', 'notFound'].includes(er.reason);
  return e;
}

/** Consulta corta (SELECT) → matriz de valores. */
function consultar_(sql, parametros) {
  const pedido = { query: sql, useLegacySql: false, location: CONFIG.UBICACION_BQ, timeoutMs: 60000 };
  if (parametros) { pedido.parameterMode = 'NAMED'; pedido.queryParameters = parametros; }
  let r = conReintentos_(() => BigQuery.Jobs.query(pedido, CONFIG.PROJECT_ID), 'consultar BigQuery');
  const jobId = r.jobReference.jobId;
  while (!r.jobComplete) {
    if (tiempoRestante_() < 40000) throw new Error('La consulta de BigQuery tardó demasiado; se reintentará.');
    Utilities.sleep(2000);
    r = BigQuery.Jobs.getQueryResults(CONFIG.PROJECT_ID, jobId, { location: CONFIG.UBICACION_BQ, timeoutMs: 30000 });
  }
  return (r.rows || []).map(fila => fila.f.map(c => c.v));
}

/** Deja una fila de auditoría por archivo en la tabla `cargas` (no bloquea si falla). */
function registrarCargaBQ_(reg) {
  try {
    asegurarBigQuery_();
    const num = x => (x === '' || x === undefined || x === null ? null : Number(x));
    const txt = x => (x === '' || x === undefined || x === null ? null : String(x));
    const fila = {
      id_carga: reg.id_carga, archivo: reg.archivo, file_id: reg.file_id, md5: txt(reg.md5), estado: reg.estado,
      filas_csv: num(reg.filas_csv), transacciones: num(reg.transacciones), validas: num(reg.validas),
      rechazadas: num(reg.rechazadas), sin_cedula: num(reg.sin_cedula), duplicadas_archivo: num(reg.duplicadas_archivo),
      insertadas: num(reg.insertadas_bq), ya_existian: num(reg.ya_existian_bq),
      fecha_min: txt(reg.fecha_min), fecha_max: txt(reg.fecha_max), inicio: txt(reg.inicio), fin: txt(reg.fin),
      duracion_seg: duracionSeg_(reg), mensaje: txt(reg.ultimo_mensaje),
    };
    lanzarJob_({
      jobReference: { projectId: CONFIG.PROJECT_ID, location: CONFIG.UBICACION_BQ },
      configuration: { load: { destinationTable: tabla_(TABLAS.CARGAS), sourceFormat: 'NEWLINE_DELIMITED_JSON',
        writeDisposition: 'WRITE_APPEND', schema: { fields: ESQUEMA_CARGAS } } },
    }, Utilities.newBlob(JSON.stringify(fila), 'application/octet-stream'));
    Log.info(reg.estado === E.ERROR ? 'SISTEMA' : 'LIMPIEZA', `Auditoría registrada en BigQuery (tabla ${TABLAS.CARGAS})`);
  } catch (e) {
    Log.aviso('SISTEMA', 'No se pudo registrar la auditoría en BigQuery: ' + e.message);
  }
}

// ============================================================================
// 9. PORTAL WEB (Implementar → Aplicación web)
// ============================================================================
function doGet() {
  return HtmlService.createHtmlOutputFromFile('Portal')
    .setTitle('Portal ETL Symmetry')
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

/** Datos para el portal (solo lectura). */
function obtenerDatosPortal() {
  const libro = libro_();
  const leer = (nombre, cols, maxFilas) => {
    const h = hoja_(nombre);
    const n = h.getLastRow() - 1;
    if (n < 1) return [];
    const desde = Math.max(2, h.getLastRow() - maxFilas + 1);
    return h.getRange(desde, 1, h.getLastRow() - desde + 1, cols.length).getDisplayValues().reverse();
  };
  const props = PropertiesService.getScriptProperties();
  const hojaTx = hoja_(HOJAS.TX);
  let url = '';
  try { url = ScriptApp.getService().getUrl(); } catch (e) { /* sin implementar aún */ }
  return {
    generado: ahora_(),
    config: {
      proyecto: CONFIG.PROJECT_ID, dataset: CONFIG.DATASET, programacion: programacion_(),
      responsable: CONFIG.EMAIL_RESPONSABLE_CARGA,
      umbral: CONFIG.UMBRAL_ERRORES, sheetUrl: libro.getUrl(), portalUrl: url,
      carpetaUrl: 'https://drive.google.com/drive/folders/' + CONFIG.CARPETA_ENTRADA_ID,
      bqUrl: `https://console.cloud.google.com/bigquery?project=${CONFIG.PROJECT_ID}&ws=!1m4!1m3!3m2!1s${CONFIG.PROJECT_ID}!2s${CONFIG.DATASET}`,
    },
    ultima: JSON.parse(props.getProperty('ULTIMA_EJECUCION') || 'null'),
    automatizacion: ScriptApp.getProjectTriggers().some(t => t.getHandlerFunction() === 'ejecucionSemanal'),
    controlSemanal: JSON.parse(props.getProperty('CONTROL_SEMANAL') || 'null'),
    celdas: contarCeldas_(libro), limiteCeldas: LIMITE_CELDAS_SHEET,
    filasTransaccional: Math.max(0, hojaTx.getLastRow() - 1),
    cargas: leer(HOJAS.CONTROL, COLUMNAS_CONTROL, 500), columnasCargas: COLUMNAS_CONTROL,
    log: leer(HOJAS.LOG, COLUMNAS_LOG, 400), columnasLog: COLUMNAS_LOG,
    errores: leer(HOJAS.ERRORES, COLUMNAS_ERRORES, 300), columnasErrores: COLUMNAS_ERRORES,
  };
}

function portalProcesarAhora() { return procesar(); }
function portalReintentar(idCarga) { return reintentarCarga(idCarga); }

/** Resumen en vivo desde BigQuery (consulta pequeña, dentro de la capa gratuita). */
function portalConsultarBigQuery() {
  iniciarContexto_('PORTAL');
  validarConfig_();
  const t = consultar_(`SELECT COUNT(*), COUNT(DISTINCT cedula), CAST(MIN(fecha_hora) AS STRING), CAST(MAX(fecha_hora) AS STRING),
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', MAX(cargado_en), '${tz_()}'), COUNT(DISTINCT id_carga), COUNTIF(cedula IS NULL)
FROM ${ref_(TABLAS.FINAL)}`)[0];
  const dias = consultar_(`SELECT CAST(DATE(fecha_hora) AS STRING) AS dia, COUNT(*) AS n,
  COUNTIF(sentido = 'ENTRADA') AS entradas, COUNTIF(sentido = 'SALIDA') AS salidas
FROM ${ref_(TABLAS.FINAL)}
WHERE fecha_hora >= DATETIME_SUB((SELECT MAX(fecha_hora) FROM ${ref_(TABLAS.FINAL)}), INTERVAL 30 DAY)
GROUP BY dia ORDER BY dia`);
  const lugares = consultar_(`SELECT lugar, COUNT(*) AS n FROM ${ref_(TABLAS.FINAL)}
WHERE fecha_hora >= DATETIME_SUB((SELECT MAX(fecha_hora) FROM ${ref_(TABLAS.FINAL)}), INTERVAL 7 DAY)
GROUP BY lugar ORDER BY n DESC LIMIT 8`);
  return {
    total: t[0], personas: t[1], fechaMin: t[2], fechaMax: t[3], ultimaCarga: t[4], cargas: t[5], sinCedula: t[6],
    dias: dias.map(d => ({ dia: d[0], n: +d[1], entradas: +d[2], salidas: +d[3] })),
    lugares: lugares.map(l => ({ lugar: l[0], n: +l[1] })),
  };
}

// ============================================================================
// 10. UTILIDADES
// ============================================================================
function iniciarContexto_(ejecucion) {
  Ctx.inicio = Date.now();
  Ctx.ejecucion = ejecucion;
  Ctx.reg = null;
  Ctx.bqOk = false;
}

function tiempoRestante_() { return TIEMPO_MAX_MS - (Date.now() - Ctx.inicio); }
function tz_() { return Session.getScriptTimeZone() || 'America/Bogota'; }
function fecha_(d) { return Utilities.formatDate(d, tz_(), 'yyyy-MM-dd HH:mm:ss'); }
function ahora_() { return fecha_(new Date()); }

function duracionSeg_(reg) {
  const a = Date.parse(String(reg.inicio).replace(' ', 'T')), b = Date.parse(String(reg.fin).replace(' ', 'T'));
  return isNaN(a) || isNaN(b) ? null : Math.round((b - a) / 1000);
}

function md5_(blob) {
  return Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, blob.getBytes())
    .map(b => ('0' + (b & 0xff).toString(16)).slice(-2)).join('');
}

function errorFatal_(mensaje) {
  const e = new Error(mensaje);
  e.fatal = true;
  return e;
}

function validarConfig_() {
  if (!CONFIG.PROJECT_ID || /PEGAR-AQUI/i.test(CONFIG.PROJECT_ID)) {
    throw errorFatal_('Falta poner el ID del proyecto de Google Cloud en CONFIG.PROJECT_ID (Code.gs, línea 29).');
  }
  if (typeof BigQuery === 'undefined') {
    throw errorFatal_('BigQuery is not defined: el servicio avanzado de BigQuery no está activo.');
  }
}

/** Reintenta operaciones de Google que fallan de forma temporal (espera 2 s, 4 s, 8 s). */
function conReintentos_(fn, descripcion, intentos) {
  const max = intentos || 3;
  for (let i = 1; ; i++) {
    try {
      return fn();
    } catch (e) {
      const temporal = /timed out|timeout|unavailable|internal error|backendError|rateLimitExceeded|try again|Service error|temporar|Address unavailable|\b50[0-4]\b/i.test(e.message);
      if (i >= max || !temporal) {
        if (descripcion && !e.fatal) try { e.message = `${e.message} (al ${descripcion})`; } catch (x) { /* solo lectura */ }
        throw e;
      }
      console.warn(`Fallo temporal al ${descripcion} (intento ${i}): ${e.message}`);
      Utilities.sleep(Math.pow(2, i) * 1000);
    }
  }
}

function esFatalTecnico_(msg) {
  return /BigQuery is not defined|billing|has not been used|is disabled|SERVICE_DISABLED|Access Denied|PERMISSION_DENIED|Not found: (Dataset|Table|Project)|No item with the given ID/i.test(msg);
}

/** Traduce errores técnicos a una indicación práctica. */
function pista_(msg) {
  const reglas = [
    [/BigQuery is not defined/i, 'En el editor de Apps Script: Servicios (+) → BigQuery API → Agregar.'],
    [/billing/i, 'El proyecto de Google Cloud necesita facturación habilitada (este volumen cabe en la capa gratuita).'],
    [/has not been used|is disabled|SERVICE_DISABLED/i, 'Active la "BigQuery API" en el proyecto de Google Cloud.'],
    [/Access Denied|PERMISSION_DENIED|permission/i, 'La cuenta que ejecuta el script no tiene permisos (BigQuery: Editor de datos + Usuario de trabajos; Drive/Sheet: Editor).'],
    [/Not found: (Dataset|Table|Project)/i, 'Revise CONFIG.PROJECT_ID y ejecute instalar() para crear dataset y tablas.'],
    [/PROJECT_ID/i, 'Abra Code.gs y escriba el ID de su proyecto en CONFIG.PROJECT_ID.'],
    [/celdas|10000000|10,000,000/i, 'El Google Sheet está cerca del límite de 10 millones de celdas.'],
    [/Faltan columnas/i, 'El CSV no trae las columnas esperadas: pida el reporte con el formato habitual de Symmetry.'],
    [/umbral/i, 'Revise la hoja Errores para ver qué filas vienen mal en el CSV.'],
    [/Service invoked too many times|quota|Quota/i, 'Se alcanzó una cuota diaria de Google; se reintenta solo.'],
    [/No item with the given ID|not found/i, 'El archivo o la carpeta no existe o la cuenta perdió el acceso.'],
    [/modificad|editó/i, 'No edite la hoja Transaccional a mano; el ETL la regenera desde el CSV.'],
  ];
  const r = reglas.find(x => x[0].test(msg || ''));
  return r ? r[1] : '';
}

function moverArchivo_(reg, subcarpeta) {
  if (!CONFIG.MOVER_ARCHIVOS) return;
  try {
    const entrada = DriveApp.getFolderById(CONFIG.CARPETA_ENTRADA_ID);
    let destino = entrada;
    if (subcarpeta) {
      const it = entrada.getFoldersByName(subcarpeta);
      destino = it.hasNext() ? it.next() : entrada.createFolder(subcarpeta);
    }
    DriveApp.getFileById(reg.file_id).moveTo(destino);
    Log.info('ARCHIVO', `CSV movido a la carpeta "${subcarpeta || destino.getName()}"`);
  } catch (e) {
    Log.aviso('ARCHIVO', `No se pudo mover el CSV (no afecta los datos; el control evita reprocesarlo): ${e.message}`);
  }
}

function destinatarios_(incluirResponsable) {
  const lista = [CONFIG.EMAIL_ALERTAS || Session.getEffectiveUser().getEmail()];
  if (incluirResponsable && CONFIG.EMAIL_RESPONSABLE_CARGA) lista.push(CONFIG.EMAIL_RESPONSABLE_CARGA);
  return lista.join(',');
}

function notificar_(asunto, cuerpo, para) {
  try {
    para = para || destinatarios_(false);
    let url = '';
    try { url = ScriptApp.getService().getUrl() || ''; } catch (e) { /* sin portal */ }
    MailApp.sendEmail(para, asunto, cuerpo + (url ? `\n\nPortal: ${url}` : '') + `\nSheet: ${libro_().getUrl()}`);
  } catch (e) {
    console.warn('No se pudo enviar el correo: ' + e.message);
  }
}
