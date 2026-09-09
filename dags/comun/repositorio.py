"""Unico punto de acceso a MySQL.

Ningun DAG abre conexiones ni escribe SQL: todo pasa por aqui. Asi el SQL se
revisa en un solo archivo y cambiar de motor no obliga a tocar los cinco DAGs.

Reglas que se respetan en todo el modulo:
  - Los valores SIEMPRE viajan como parametros (%s), nunca concatenados en la
    cadena SQL. Concatenar abre la puerta a inyeccion y rompe con comillas.
  - Toda conexion se cierra en un `finally`.
  - Las cargas son idempotentes: re-ejecutar una tarea deja la base igual, no
    duplicada.

EL DDL NO ESTA AQUI. Vive en sql/01_esquemas.sql y sql/02_tablas.sql, que se
montan en /docker-entrypoint-initdb.d/ del contenedor de MySQL. Tener las mismas
tablas definidas en dos lugares garantiza que tarde o temprano divergan, y el
sintoma aparece semanas despues como una columna que existe en un entorno y no
en otro. Aqui solo se verifica que el esquema este.
"""

from contextlib import contextmanager

from airflow.providers.mysql.hooks.mysql import MySqlHook

from comun import config

TABLAS_ESPERADAS = [
    config.TABLA_ACTIVOS,
    config.TABLA_OHLCV,
    config.TABLA_LOTES,
    config.TABLA_CONCILIACION,
]


@contextmanager
def _conexion():
    """Conexion a MySQL que siempre se cierra, haya o no excepcion.

    Sin el `finally`, una tarea que falla deja la conexion colgada; tras varios
    reintentos se agota el pool del servidor y empiezan a fallar tareas que no
    tienen nada que ver.
    """
    hook = MySqlHook(mysql_conn_id=config.CONN_MYSQL)
    conexion = hook.get_conn()
    try:
        yield conexion
    finally:
        conexion.close()


# ---------------------------------------------------------------------------
# VERIFICACION
# ---------------------------------------------------------------------------
def verificar_esquema():
    """Comprueba que existan las cuatro tablas. Falla con instrucciones si no.

    Se ejecuta al inicio del DAG 01. Es preferible fallar aqui, con un mensaje
    que dice exactamente que hacer, a fallar tres tareas mas adelante con un
    'table doesn't exist' que obliga a rastrear el origen.
    """
    encontradas = {fila["Tables_in_cripto"] if "Tables_in_cripto" in fila else list(fila.values())[0]
                   for fila in consultar("SHOW TABLES")}

    faltantes = [tabla for tabla in TABLAS_ESPERADAS if tabla not in encontradas]
    if faltantes:
        raise RuntimeError(
            "Faltan tablas en MySQL: " + ", ".join(faltantes) + ". "
            "El esquema se crea con los archivos de sql/, que solo se ejecutan "
            "la primera vez que arranca el contenedor con el volumen vacio. "
            "Si se modificaron despues, hay que rehacer el volumen: "
            "`docker compose down -v` y `docker compose up -d`."
        )

    return sorted(encontradas & set(TABLAS_ESPERADAS))


# ---------------------------------------------------------------------------
# CARGA
# ---------------------------------------------------------------------------
def cargar_activos(activos):
    """Inserta o actualiza la dimension de activos.

    ON DUPLICATE KEY UPDATE y no INSERT a secas: los mismos activos reaparecen
    en cada lote. Con un INSERT normal, la segunda corrida fallaria entera por
    clave duplicada.
    """
    if not activos:
        return 0

    sql = (
        "INSERT INTO " + config.TABLA_ACTIVOS +
        " (id_activo, simbolo, activo_base, activo_cotizacion, nombre, categoria, estado) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE "
        "simbolo = VALUES(simbolo), "
        "activo_base = VALUES(activo_base), "
        "activo_cotizacion = VALUES(activo_cotizacion), "
        "nombre = VALUES(nombre), "
        "categoria = VALUES(categoria), "
        "estado = VALUES(estado)"
    )
    registros = [
        (
            activo["id_activo"], activo["simbolo"], activo["activo_base"],
            activo["activo_cotizacion"], activo["nombre"],
            activo.get("categoria"), activo.get("estado", "ACTIVO"),
        )
        for activo in activos
    ]

    with _conexion() as conexion:
        cursor = conexion.cursor()
        try:
            cursor.executemany(sql, registros)
            conexion.commit()
        finally:
            cursor.close()

    return len(registros)


def cargar_ohlcv(lote_id, filas, columnas):
    """Carga las velas del lote de forma idempotente.

    IDEMPOTENCIA POR CLAVE NATURAL, NO POR LOTE. Aqui se hace `ON DUPLICATE KEY
    UPDATE` sobre (id_activo, fecha), y NO `DELETE WHERE lote_id = ...` seguido
    de INSERT.

    El motivo es concreto. Dos corridas distintas cubren, a proposito, los
    mismos dias: la descarga diaria trae 365 dias hacia atras cada vez. Borrar
    por lote_id no eliminaria las filas del lote anterior, que ya ocupan esas
    claves primarias, y el INSERT reventaria con 'Duplicate entry'. El sintoma
    seria que la primera corrida siempre funciona y la segunda siempre falla.

    El `lote_id` se actualiza junto con los valores: la fila queda atribuida a la
    corrida mas reciente que la escribio, que es lo que interesa para trazar de
    donde salio el dato que esta ahora en la tabla.
    """
    if not filas:
        return {"afectadas": 0, "enviadas": 0}

    asignaciones = ", ".join(
        columna + " = VALUES(" + columna + ")"
        for columna in columnas
        # Las dos columnas de la clave primaria no se actualizan: son la
        # condicion del conflicto, no un valor a sobrescribir.
        if columna not in ("id_activo", "fecha")
    )

    sql = (
        "INSERT INTO " + config.TABLA_OHLCV + " (" + ", ".join(columnas) + ") "
        "VALUES (" + ", ".join(["%s"] * len(columnas)) + ") "
        "ON DUPLICATE KEY UPDATE " + asignaciones
    )
    registros = [tuple(fila.get(columna) for columna in columnas) for fila in filas]

    afectadas = 0
    with _conexion() as conexion:
        cursor = conexion.cursor()
        try:
            for inicio in range(0, len(registros), config.TAMANO_LOTE_INSERT):
                cursor.executemany(sql, registros[inicio:inicio + config.TAMANO_LOTE_INSERT])
                afectadas += cursor.rowcount
            conexion.commit()
        finally:
            cursor.close()

    # rowcount cuenta 1 por insercion y 2 por actualizacion, asi que no es el
    # numero de filas: se devuelve tal cual, etiquetado, en vez de disfrazarlo
    # de conteo.
    return {"afectadas": afectadas, "enviadas": len(registros)}


def cargar_conciliacion(filas):
    """Guarda el resultado de la conciliacion del DAG 05, de forma idempetente."""
    if not filas:
        return 0

    sql = (
        "INSERT INTO " + config.TABLA_CONCILIACION +
        " (simbolo, fecha_hora, lote_id, vwap_streaming, n_trades_streaming, "
        "  cierre_batch, n_trades_batch, desviacion_pct, cobertura_pct, veredicto) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE "
        "lote_id = VALUES(lote_id), "
        "vwap_streaming = VALUES(vwap_streaming), "
        "n_trades_streaming = VALUES(n_trades_streaming), "
        "cierre_batch = VALUES(cierre_batch), "
        "n_trades_batch = VALUES(n_trades_batch), "
        "desviacion_pct = VALUES(desviacion_pct), "
        "cobertura_pct = VALUES(cobertura_pct), "
        "veredicto = VALUES(veredicto)"
    )
    registros = [
        (
            fila["simbolo"], fila["fecha_hora"], fila["lote_id"],
            fila.get("vwap_streaming"), fila.get("n_trades_streaming"),
            fila.get("cierre_batch"), fila.get("n_trades_batch"),
            fila.get("desviacion_pct"), fila.get("cobertura_pct"),
            fila["veredicto"],
        )
        for fila in filas
    ]

    with _conexion() as conexion:
        cursor = conexion.cursor()
        try:
            cursor.executemany(sql, registros)
            conexion.commit()
        finally:
            cursor.close()

    return len(registros)


def registrar_lote(lote_id, estado, metricas=None):
    """Anota el estado del lote en la tabla de control.

    Es la bitacora del pipeline: permite saber en que quedo cada corrida sin
    rebuscar en los logs de Airflow.
    """
    metricas = metricas or {}
    sql = (
        "INSERT INTO " + config.TABLA_LOTES +
        " (lote_id, estado, simbolos, filas_descargadas, filas_validas,"
        "  filas_rechazadas, tasa_rechazo, filas_cargadas) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE "
        "estado = VALUES(estado), "
        # COALESCE conserva lo que ya habia si esta llamada no trae el dato: el
        # DAG 05 actualiza el estado sin volver a enviar los conteos.
        "simbolos          = COALESCE(VALUES(simbolos),          simbolos), "
        "filas_descargadas = COALESCE(VALUES(filas_descargadas), filas_descargadas), "
        "filas_validas     = COALESCE(VALUES(filas_validas),     filas_validas), "
        "filas_rechazadas  = COALESCE(VALUES(filas_rechazadas),  filas_rechazadas), "
        "tasa_rechazo      = COALESCE(VALUES(tasa_rechazo),      tasa_rechazo), "
        "filas_cargadas    = COALESCE(VALUES(filas_cargadas),    filas_cargadas)"
    )
    parametros = (
        lote_id,
        estado,
        metricas.get("simbolos"),
        metricas.get("filas_descargadas"),
        metricas.get("filas_validas"),
        metricas.get("filas_rechazadas"),
        metricas.get("tasa_rechazo"),
        metricas.get("filas_cargadas"),
    )

    with _conexion() as conexion:
        cursor = conexion.cursor()
        try:
            cursor.execute(sql, parametros)
            conexion.commit()
        finally:
            cursor.close()

    return estado


# ---------------------------------------------------------------------------
# CONSULTA
# ---------------------------------------------------------------------------
def consultar(sql, parametros=None):
    """Ejecuta un SELECT y devuelve una lista de diccionarios."""
    with _conexion() as conexion:
        cursor = conexion.cursor()
        try:
            cursor.execute(sql, parametros or ())
            columnas = [descripcion[0] for descripcion in cursor.description]
            return [dict(zip(columnas, fila)) for fila in cursor.fetchall()]
        finally:
            cursor.close()


def contar_velas_del_lote(lote_id):
    filas = consultar(
        "SELECT COUNT(*) AS total FROM " + config.TABLA_OHLCV + " WHERE lote_id = %s",
        (lote_id,),
    )
    return int(filas[0]["total"])


def verificar_integridad_referencial(lote_id):
    """Busca velas del lote cuyo activo no exista en la dimension.

    La clave foranea ya lo impide a nivel de motor. Esta consulta existe para
    que el resultado quede visible en el log de la tarea y en el reporte: una
    verificacion que nadie puede leer no sirve como evidencia.
    """
    return consultar(
        "SELECT h.id_activo, h.fecha "
        "FROM " + config.TABLA_OHLCV + " h "
        "LEFT JOIN " + config.TABLA_ACTIVOS + " a ON a.id_activo = h.id_activo "
        "WHERE h.lote_id = %s AND a.id_activo IS NULL",
        (lote_id,),
    )


def velas_horarias(simbolo, desde, hasta):
    """OBSOLETA - no usar. Se conserva el nombre solo para explicar por que se fue.

    Consultaba hechos_ohlcv_diario, que es una tabla DIARIA por diseno. O sea que
    habria devuelto velas diarias con nombre de horarias, y la conciliacion
    habria comparado el VWAP de UNA HORA contra el cierre de UN DIA ENTERO sin
    dar ningun error: solo un numero de desviacion que parece plausible y no
    significa nada.

    La referencia horaria que necesita el DAG 05 se descarga en el momento, en
    comun/conciliacion.obtener_referencia_batch, y no se persiste. Mezclar dos
    granularidades en una tabla de hechos es la forma mas rapida de que un
    conteo posterior salga mal sin que nadie lo note.
    """
    raise NotImplementedError(
        "velas_horarias fue retirada: consultaba la tabla diaria. Usa "
        "comun.conciliacion.obtener_referencia_batch, que descarga velas "
        "horarias sin persistirlas."
    )
