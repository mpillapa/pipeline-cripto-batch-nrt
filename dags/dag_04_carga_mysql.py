"""DAG 04 - Carga al almacen analitico y exportacion a Elasticsearch.

PROPOSITO
    Llevar la zona plata a MySQL (dimension y hechos) y dejar el mismo conjunto
    exportado en NDJSON para que Logstash lo indexe en Elasticsearch. Es el
    punto donde el camino batch se une con la vista unificada de Kibana.

POSICION EN EL PIPELINE
    01 ingesta -> 02 calidad -> 03 transformacion -> [04 carga] -> 05 conciliacion

TAREAS
    cargar_dimension        dim_activo primero: los hechos tienen FK hacia ella.
    cargar_hechos           hechos_ohlcv_diario, de forma idempotente.
    verificar_carga         Integridad referencial y conteos.
    exportar_a_logstash     NDJSON en la zona exportada.
    cerrar_carga            Registra el estado final del lote.

POR QUE ESTE DISENO
    - La dimension va ANTES que los hechos, en serie y no en paralelo. La clave
      foranea lo exige: un hecho cuyo activo aun no existe hace fallar el INSERT
      entero.
    - La carga de hechos es idempotente POR CLAVE NATURAL (id_activo, fecha),
      con ON DUPLICATE KEY UPDATE, y no por lote_id. Ver la nota extensa en
      comun/repositorio.cargar_ohlcv: el patron de borrar por lote_id, que
      funcionaba en un dominio donde cada lote traia registros nuevos, aqui
      fallaria siempre en la segunda corrida.
    - La exportacion a NDJSON es una tarea aparte de la carga a MySQL. Si
      Logstash esta caido, los datos ya estan en MySQL y solo hay que repetir la
      exportacion. Mezclarlas obligaria a repetir la carga entera.
    - El NDJSON incluye `simbolo`, que en MySQL vive solo en la dimension.
      Elasticsearch no hace joins: un documento sin el simbolo seria inutil en
      Kibana.
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from comun import config, observabilidad, repositorio, transformaciones, utilidades, zonas

DAG_SIGUIENTE = "dag_05_conciliacion"
COMPONENTE = "dag_04_carga_mysql"

ARCHIVO_CATALOGO = "catalogo_activos.csv"
DIR_SEMILLA = os.environ.get("CRIPTO_DIR_SEMILLA", "/opt/airflow/datos_semilla")


# ---------------------------------------------------------------------------
# TAREAS
# ---------------------------------------------------------------------------
def cargar_dimension(**context):
    """Carga dim_activo a partir del lote y del catalogo semilla.

    El catalogo aporta los atributos descriptivos; el lote aporta que activos
    existen realmente. Un simbolo presente en el lote pero ausente del catalogo
    entra igual, con el nombre derivado del par: es preferible una dimension
    incompleta a una carga de hechos que falla por clave foranea.
    """
    utilidades.encabezado("DAG 04 - CARGAR DIMENSION")

    lote_id = utilidades.resolver_lote_id(context)
    marco = zonas.leer_plata(lote_id)

    activos = transformaciones.extraer_dimension(marco)
    catalogo = _leer_catalogo()
    activos = transformaciones.combinar_con_catalogo(activos, catalogo)

    cargados = repositorio.cargar_activos(activos)

    utilidades.resumen([
        ("lote_id", lote_id),
        ("activos en el lote", len(activos)),
        ("entradas en el catalogo semilla", len(catalogo)),
        ("filas cargadas o actualizadas", cargados),
    ])
    for activo in activos:
        print("  " + activo["simbolo"].ljust(10) + " " + str(activo["nombre"]))

    context["ti"].xcom_push(key="lote_id", value=lote_id)
    return {"lote_id": lote_id, "activos": cargados}


def _leer_catalogo():
    """Lee el catalogo semilla. Devuelve lista vacia si no existe.

    No falla si falta: el catalogo es un enriquecimiento, no un requisito. Que
    el pipeline se caiga porque falta un archivo de nombres bonitos seria
    desproporcionado.
    """
    import csv

    ruta = os.path.join(DIR_SEMILLA, ARCHIVO_CATALOGO)
    if not os.path.exists(ruta):
        print("No hay catalogo semilla en " + ruta + "; se usan nombres derivados.")
        return []

    with open(ruta, newline="", encoding="utf-8") as archivo:
        return list(csv.DictReader(archivo))


def cargar_hechos(**context):
    """Carga hechos_ohlcv_diario de forma idempotente.

    Los NaN de pandas se convierten a None antes de enviar: sin eso, el conector
    intenta insertar la cadena 'nan' en una columna DECIMAL. Los nulos son
    legitimos aqui -- los primeros dias de la serie no tienen media movil de 30
    dias -- asi que no se pueden reemplazar por cero.
    """
    utilidades.encabezado("DAG 04 - CARGAR HECHOS")

    ti = context["ti"]
    lote_id = ti.xcom_pull(task_ids="cargar_dimension", key="lote_id")
    marco = zonas.leer_plata(lote_id)

    limpio = transformaciones.sustituir_nulos(marco[transformaciones.COLUMNAS_HECHOS])
    filas = limpio.to_dict("records")

    resultado = repositorio.cargar_ohlcv(
        lote_id, filas, transformaciones.COLUMNAS_HECHOS
    )

    utilidades.resumen([
        ("lote_id", lote_id),
        ("filas enviadas", resultado["enviadas"]),
        # rowcount cuenta 1 por insercion y 2 por actualizacion, asi que este
        # numero no es un conteo de filas. Se etiqueta tal cual en vez de
        # disfrazarlo.
        ("filas afectadas (insert=1, update=2)", resultado["afectadas"]),
    ])
    return resultado


def verificar_carga(**context):
    """Comprueba que lo que quedo en MySQL sea lo que se envio.

    Dos verificaciones. La integridad referencial ya la garantiza la clave
    foranea del motor, pero ejecutarla y dejar el resultado en el log convierte
    una garantia invisible en evidencia revisable: una verificacion que nadie
    puede leer no sirve como prueba.
    """
    utilidades.encabezado("DAG 04 - VERIFICAR CARGA")

    ti = context["ti"]
    lote_id = ti.xcom_pull(task_ids="cargar_dimension", key="lote_id")

    en_base = repositorio.contar_velas_del_lote(lote_id)
    huerfanos = repositorio.verificar_integridad_referencial(lote_id)

    marco = zonas.leer_plata(lote_id)
    esperadas = len(marco)

    if huerfanos:
        for fila in huerfanos[:10]:
            print("HUERFANO: " + str(fila))
        raise ValueError(
            str(len(huerfanos)) + " vela(s) sin activo en la dimension. "
            "No deberia poder ocurrir: la clave foranea lo impide. Si aparece, "
            "la restriccion no esta activa en la tabla."
        )

    # El conteo en base puede ser MENOR que las filas de plata: una vela que ya
    # existia de un lote anterior se actualiza y queda atribuida a ESTE lote,
    # pero una que fue actualizada por un lote posterior ya no cuenta como de
    # este. Se informa, no se falla.
    utilidades.resumen([
        ("lote_id", lote_id),
        ("filas en la zona plata", esperadas),
        ("filas atribuidas a este lote en MySQL", en_base),
        ("velas huerfanas", len(huerfanos)),
    ])

    if en_base != esperadas:
        print(
            "NOTA: la diferencia de " + str(esperadas - en_base) + " fila(s) es "
            "esperable si otra corrida posterior reescribio parte de la serie."
        )

    return {"lote_id": lote_id, "en_base": en_base, "esperadas": esperadas}


def exportar_a_logstash(**context):
    """Escribe el lote en NDJSON para que Logstash lo indexe en Elasticsearch.

    Un objeto JSON por linea, sin indentacion: es lo que espera el input `file`
    con codec json_lines. Un objeto indentado ocupa varias lineas fisicas y
    Logstash lo leeria como varios eventos rotos.

    Cada documento lleva `tipo_fuente: batch_ohlcv`, que es el discriminante que
    usa Logstash para enrutar al indice correcto (ver contrato de datos).
    """
    utilidades.encabezado("DAG 04 - EXPORTAR A LOGSTASH")

    ti = context["ti"]
    lote_id = ti.xcom_pull(task_ids="cargar_dimension", key="lote_id")
    marco = zonas.leer_plata(lote_id)

    limpio = transformaciones.sustituir_nulos(marco)
    documentos = []
    for fila in limpio.to_dict("records"):
        documento = dict(fila)
        documento["tipo_fuente"] = "batch_ohlcv"
        # Elasticsearch necesita una marca temporal completa; `fecha` es solo el
        # dia. Se envia como fecha_hora en UTC a medianoche, que es el momento
        # de apertura de la vela diaria.
        documento["fecha_hora"] = str(fila["fecha"]) + "T00:00:00.000Z"
        documentos.append(documento)

    # Marca de corrida en el nombre, por el mismo motivo que en el DAG 05: el
    # input `file` de Logstash va en modo `tail` y recuerda por inodo hasta
    # donde leyo. Reejecutar este DAG sobre el mismo lote sobrescribiria el
    # archivo, y Logstash retomaria desde el desplazamiento anterior en vez de
    # releerlo entero. Se pierden documentos sin ningun error visible.
    marca = utilidades.ahora_utc().strftime("%Y%m%dT%H%M%S")
    ruta = utilidades.ruta_en_lote(
        config.DIR_EXPORTADO, lote_id, f"ohlcv_{marca}.ndjson"
    )
    escritas = utilidades.escribir_ndjson(ruta, documentos)

    utilidades.resumen([
        ("lote_id", lote_id),
        ("documentos exportados", escritas),
        ("archivo", ruta),
        ("tipo_fuente", "batch_ohlcv"),
    ])
    print("Logstash lo recogera con su input `file` apuntando a " + config.DIR_EXPORTADO)
    return {"ruta": ruta, "documentos": escritas}


def cerrar_carga(**context):
    """Registra el estado final del lote en la bitacora."""
    utilidades.encabezado("DAG 04 - CIERRE DE LA CARGA")

    ti = context["ti"]
    lote_id = ti.xcom_pull(task_ids="cargar_dimension", key="lote_id")
    verificacion = ti.xcom_pull(task_ids="verificar_carga")
    exportacion = ti.xcom_pull(task_ids="exportar_a_logstash")

    repositorio.registrar_lote(lote_id, "CARGADO", {
        "filas_cargadas": verificacion["esperadas"],
    })

    observabilidad.publicar_control(
        COMPONENTE, "fin_carga", lote_id=lote_id, estado="CARGADO",
        metricas={
            "filas_cargadas": verificacion["esperadas"],
            "documentos_exportados": exportacion["documentos"],
        },
    )

    utilidades.resumen([
        ("lote_id", lote_id),
        ("estado", "CARGADO"),
        ("filas en MySQL", verificacion["esperadas"]),
        ("documentos para Elasticsearch", exportacion["documentos"]),
    ])
    return {"lote_id": lote_id, "estado": "CARGADO"}


# ---------------------------------------------------------------------------
# DEFINICION DEL DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dag_04_carga_mysql",
    description="04 - Carga la dimension y los hechos en MySQL y exporta a Elasticsearch",
    default_args=config.ARGS_POR_DEFECTO,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=config.ETIQUETAS_BASE + ["04-carga", "oro"],
    doc_md=__doc__,
) as dag:

    dimension = PythonOperator(
        task_id="cargar_dimension",
        python_callable=cargar_dimension,
    )

    hechos = PythonOperator(
        task_id="cargar_hechos",
        python_callable=cargar_hechos,
    )

    verificar = PythonOperator(
        task_id="verificar_carga",
        python_callable=verificar_carga,
    )

    exportar = PythonOperator(
        task_id="exportar_a_logstash",
        python_callable=exportar_a_logstash,
    )

    cierre = PythonOperator(
        task_id="cerrar_carga",
        python_callable=cerrar_carga,
    )

    disparar_05 = TriggerDagRunOperator(
        task_id="disparar_dag_05",
        trigger_dag_id=DAG_SIGUIENTE,
        conf={"lote_id": "{{ ti.xcom_pull(task_ids='cargar_dimension', key='lote_id') }}"},
        trigger_run_id="lote_{{ ti.xcom_pull(task_ids='cargar_dimension', key='lote_id') }}",
        wait_for_completion=True,
        poke_interval=config.INTERVALO_ESPERA_TRIGGER,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=True,
        retries=0,
    )

    # La dimension va antes que los hechos: la clave foranea lo exige.
    # La verificacion y la exportacion son independientes entre si y corren en
    # paralelo; ambas necesitan que los hechos ya esten cargados.
    dimension >> hechos >> [verificar, exportar] >> cierre >> disparar_05
