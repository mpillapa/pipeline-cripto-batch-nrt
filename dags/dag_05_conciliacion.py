"""DAG 05 - Conciliacion entre el flujo near real-time y el flujo batch.

PROPOSITO
    Responder con un numero si los dos flujos estan midiendo lo mismo. Agrega
    las metricas por ventana que el job de Spark dejo en Elasticsearch, las
    compara contra velas horarias oficiales y escribe el veredicto en MySQL.

POSICION EN EL PIPELINE
    01 ingesta -> 02 calidad -> 03 transformacion -> 04 carga -> [05 conciliacion]

TAREAS
    resolver_lote            Toma el lote_id y calcula la ventana de tiempo.
    conciliar_<simbolo>      Una tarea POR SIMBOLO, ejecutadas EN PARALELO.
    consolidar_conciliacion  Carga a MySQL, escribe el reporte y exporta a Kibana.
    cerrar_conciliacion      Registra el estado final del lote.

POR QUE ESTE DISENO
    - La logica NO esta en este archivo: vive en comun/conciliacion.py, con su
      propia prueba que usa una respuesta de Elasticsearch guardada. Eso permitio
      escribirla y verificarla ANTES de que el flujo near real-time existiera.
    - Si no hay metricas en Elasticsearch, el DAG termina en `success` con cero
      horas conciliadas, no falla. Es el estado normal antes de que el flujo NRT
      arranque, y tambien el estado cuando el job de Spark estuvo caido. Un DAG
      que falla por no encontrar datos que legitimamente pueden no existir
      obliga a distinguir a mano entre "no hay datos" y "algo se rompio".
    - La referencia horaria se descarga en el momento y no se persiste.
      `hechos_ohlcv_diario` es diario por diseno; meter velas horarias ahi
      romperia la granularidad de la tabla de hechos.
    - La descarga de la referencia solo ocurre si hay metricas que conciliar.
      Sin eso, cada corrida gastaria peticiones a la API para comparar contra
      nada.

PARAMETROS  (UI -> Trigger DAG w/ config)
    {"horas": 6}    Concilia las ultimas 6 horas en vez de 24.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from comun import conciliacion, config, observabilidad, repositorio, utilidades

COMPONENTE = "dag_05_conciliacion"

HORAS_POR_DEFECTO = 24


# ---------------------------------------------------------------------------
# TAREAS
# ---------------------------------------------------------------------------
def resolver_lote(**context):
    """Toma el lote_id y calcula la ventana de tiempo a conciliar.

    La ventana termina en la hora EN CURSO, no en el momento actual: la hora que
    todavia no ha cerrado tendria ventanas incompletas en el lado NRT y una vela
    horaria parcial en el lado batch, y compararlas produciria una desviacion
    que solo mide que la hora no ha terminado.

    Es el mismo problema de la vela en curso del DAG 01, un nivel mas abajo.
    """
    utilidades.encabezado("DAG 05 - RESOLVER LOTE Y VENTANA")

    lote_id = utilidades.resolver_lote_id(context)
    horas = int(utilidades.leer_conf(context, "horas", HORAS_POR_DEFECTO))

    ahora = utilidades.ahora_utc()
    hasta = ahora.replace(minute=0, second=0, microsecond=0)
    desde = hasta - timedelta(hours=horas)

    ventana = {
        "lote_id": lote_id,
        "desde": utilidades.a_iso(desde),
        "hasta": utilidades.a_iso(hasta),
        "horas": horas,
    }

    utilidades.resumen([
        ("lote_id", lote_id),
        ("desde", ventana["desde"]),
        ("hasta", ventana["hasta"]),
        ("horas", horas),
        ("indice consultado", config.INDICE_METRICAS_NRT),
    ])
    return ventana


def conciliar_simbolo(simbolo, **context):
    """Concilia un simbolo. La ejecutan las tres tareas paralelas.

    Devuelve las filas listas para insertar en la tabla `conciliacion`, o una
    lista vacia si el flujo near real-time no dejo metricas en esa ventana.
    """
    utilidades.encabezado("DAG 05 - CONCILIAR " + simbolo)

    ventana = context["ti"].xcom_pull(task_ids="resolver_lote")

    try:
        metricas = conciliacion.consultar_metricas_nrt(
            simbolo, ventana["desde"], ventana["hasta"]
        )
    except conciliacion.FlujoNrtNoDisponible as error:
        # NO se deja propagar. El DAG 04 dispara a este con
        # wait_for_completion=True: si esta tarea fallara, fallaria el 04 y la
        # cadena entera se pondria en rojo hasta el DAG 01. El camino batch
        # quedaria roto por algo que no le corresponde, que es que la otra mitad
        # del pipeline no este corriendo.
        #
        # Se avisa por el canal de observabilidad y queda en el reporte, asi que
        # tampoco pasa desapercibido.
        observabilidad.advertencia(
            COMPONENTE,
            "Conciliacion omitida para " + simbolo + ": " + str(error),
        )
        return {
            "simbolo": simbolo, "filas": [], "horas_nrt": 0,
            "estado": "NRT_NO_DISPONIBLE",
        }

    if not metricas:
        print(
            "Sin metricas de streaming para " + simbolo + " entre " +
            ventana["desde"] + " y " + ventana["hasta"] + "."
        )
        print(
            "Es lo esperado si el job de Spark todavia no ha corrido. No es un "
            "fallo: la tarea termina bien con cero horas conciliadas."
        )
        return {"simbolo": simbolo, "filas": [], "horas_nrt": 0, "estado": "SIN_METRICAS"}

    print(str(len(metricas)) + " hora(s) con metricas de streaming.")

    # La referencia solo se descarga si hay algo que conciliar. Se piden dos
    # dias para cubrir con holgura cualquier ventana de hasta 24 horas, incluso
    # si cruza la medianoche.
    referencia = conciliacion.obtener_referencia_batch(simbolo, dias=2)
    print(str(len(referencia)) + " vela(s) horaria(s) de referencia.")

    filas = conciliacion.comparar(simbolo, metricas, referencia, ventana["lote_id"])
    resumen = conciliacion.resumir(filas)

    utilidades.resumen([
        ("simbolo", simbolo),
        ("horas conciliadas", resumen["horas"]),
        ("coinciden", resumen["coinciden"]),
        ("desviadas", resumen["desviadas"]),
        ("sin datos", resumen["sin_datos"]),
        ("desviacion media", str(resumen["desviacion_media_pct"]) + " %"),
        ("cobertura media", str(resumen["cobertura_media_pct"]) + " %"),
    ])

    return {
        "simbolo": simbolo, "filas": filas, "horas_nrt": len(metricas),
        "estado": "CONCILIADO",
    }


def consolidar_conciliacion(**context):
    """Carga el resultado en MySQL, escribe el reporte y exporta a Elasticsearch.

    Las tres cosas en una sola tarea porque operan sobre el mismo conjunto de
    filas y ninguna tiene sentido sin las otras dos.
    """
    utilidades.encabezado("DAG 05 - CONSOLIDAR CONCILIACION")

    ti = context["ti"]
    ventana = ti.xcom_pull(task_ids="resolver_lote")
    lote_id = ventana["lote_id"]

    resultados = [
        ti.xcom_pull(task_ids=_nombre_tarea(simbolo))
        for simbolo in config.SIMBOLOS
    ]
    faltantes = [
        simbolo for simbolo, resultado in zip(config.SIMBOLOS, resultados)
        if resultado is None
    ]
    if faltantes:
        raise ValueError(
            "No hay resultado de conciliacion para: " + ", ".join(faltantes) +
            ". Revisa el log de esas tareas."
        )

    todas = [fila for resultado in resultados for fila in resultado["filas"]]
    resumen_global = conciliacion.resumir(todas)

    cargadas = repositorio.cargar_conciliacion(todas) if todas else 0

    reporte = {
        "lote_id": lote_id,
        "evaluado_en": utilidades.a_iso(utilidades.ahora_utc()),
        "dag_origen": context["dag"].dag_id,
        "run_id": context["run_id"],
        "ventana": {"desde": ventana["desde"], "hasta": ventana["hasta"]},
        "umbrales": {
            "desviacion_aceptable_pct": config.CONCILIACION_DESVIACION_ACEPTABLE,
            "cobertura_minima_pct": config.CONCILIACION_COBERTURA_MINIMA,
        },
        "global": resumen_global,
        "por_simbolo": {
            resultado["simbolo"]: dict(
                conciliacion.resumir(resultado["filas"]),
                # El estado distingue "no habia nada que conciliar" de "no se
                # pudo consultar". Sin el, un reporte con cero horas no dice si
                # el flujo NRT estuvo callado o si Elasticsearch estaba caido, y
                # son dos problemas distintos.
                estado=resultado.get("estado", "DESCONOCIDO"),
            )
            for resultado in resultados
        },
    }

    ruta_reporte = utilidades.ruta_en_lote(
        config.DIR_REPORTES, lote_id, "conciliacion.json"
    )
    utilidades.escribir_json(ruta_reporte, reporte)

    # Exportacion a Kibana, por el mismo camino que el DAG 04: NDJSON que recoge
    # Logstash. Asi la conciliacion se ve en la misma linea de tiempo que los
    # dos flujos que compara, que es donde tiene sentido leerla.
    documentos = []
    for fila in todas:
        documento = dict(fila)
        documento["tipo_fuente"] = "batch_conciliacion"
        documento["fecha_hora"] = fila["fecha_hora"].replace(" ", "T") + ".000Z"
        documentos.append(documento)

    ruta_ndjson = utilidades.ruta_en_lote(
        config.DIR_EXPORTADO, lote_id, "conciliacion.ndjson"
    )
    exportados = utilidades.escribir_ndjson(ruta_ndjson, documentos)

    utilidades.resumen([
        ("lote_id", lote_id),
        ("horas conciliadas", resumen_global["horas"]),
        ("coinciden", resumen_global["coinciden"]),
        ("desviadas", resumen_global["desviadas"]),
        ("sin datos", resumen_global["sin_datos"]),
        ("desviacion media", str(resumen_global["desviacion_media_pct"]) + " %"),
        ("cobertura media", str(resumen_global["cobertura_media_pct"]) + " %"),
        ("filas en MySQL", cargadas),
        ("documentos exportados", exportados),
        ("reporte", ruta_reporte),
    ])

    if resumen_global["horas"] == 0:
        print(
            "Ninguna hora conciliada. El flujo near real-time no dejo metricas "
            "en la ventana consultada; cuando el job de Spark este corriendo, "
            "esta tarea empezara a producir filas sin ningun cambio de codigo."
        )

    return {"lote_id": lote_id, "cargadas": cargadas, "resumen": resumen_global}


def _nombre_tarea(simbolo):
    """Nombre de la tarea de conciliacion de un simbolo.

    Centralizado porque se usa al construir el DAG y al recuperar el XCom.
    Escribirlo dos veces es garantia de que un dia dejen de coincidir.
    """
    return "conciliar_" + simbolo.lower()


def cerrar_conciliacion(**context):
    """Registra el estado final del lote en la bitacora."""
    utilidades.encabezado("DAG 05 - CIERRE")

    ti = context["ti"]
    consolidado = ti.xcom_pull(task_ids="consolidar_conciliacion")
    lote_id = consolidado["lote_id"]
    resumen = consolidado["resumen"]

    repositorio.registrar_lote(lote_id, "CONCILIADO")

    observabilidad.publicar_control(
        COMPONENTE, "fin_conciliacion", lote_id=lote_id, estado="CONCILIADO",
        metricas=resumen,
    )

    utilidades.resumen([
        ("lote_id", lote_id),
        ("estado", "CONCILIADO"),
        ("horas conciliadas", resumen["horas"]),
    ])
    return {"lote_id": lote_id, "estado": "CONCILIADO"}


# ---------------------------------------------------------------------------
# DEFINICION DEL DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dag_05_conciliacion",
    description="05 - Compara el flujo near real-time contra el batch y mide la desviacion",
    default_args=config.ARGS_POR_DEFECTO,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=config.ETIQUETAS_BASE + ["05-conciliacion", "nrt-vs-batch"],
    doc_md=__doc__,
) as dag:

    resolver = PythonOperator(
        task_id="resolver_lote",
        python_callable=resolver_lote,
    )

    conciliaciones = [
        PythonOperator(
            task_id=_nombre_tarea(simbolo),
            python_callable=conciliar_simbolo,
            op_kwargs={"simbolo": simbolo},
        )
        for simbolo in config.SIMBOLOS
    ]

    consolidar = PythonOperator(
        task_id="consolidar_conciliacion",
        python_callable=consolidar_conciliacion,
    )

    cierre = PythonOperator(
        task_id="cerrar_conciliacion",
        python_callable=cerrar_conciliacion,
    )

    resolver >> conciliaciones >> consolidar >> cierre
