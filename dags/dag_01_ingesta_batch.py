"""DAG 01 - Ingesta batch de velas OHLCV.

PROPOSITO
    Traer la serie historica de cada simbolo desde la API REST publica y
    aterrizarla en la zona bronce, sin transformar. Es el punto de entrada del
    camino batch y el que crea el lote_id que recorre los cinco DAGs.

POSICION EN EL PIPELINE
    [01 ingesta] -> 02 calidad -> 03 transformacion -> 04 carga -> 05 conciliacion

TAREAS
    preparar_lote           Verifica el esquema de MySQL y crea el lote_id.
    descargar_<simbolo>     Una tarea POR SIMBOLO, ejecutadas EN PARALELO.
    consolidar_ingesta      Reune los resultados y escribe el manifiesto.
    disparar_dag_02         Encadena con el control de calidad.

POR QUE ESTE DISENO
    - Una tarea por simbolo, no un bucle dentro de una sola tarea. Si la
      descarga de un simbolo falla, se reintenta ese y no los tres. En la UI se
      ve exactamente cual fallo, en vez de un unico rectangulo rojo.
    - La zona bronce NO se transforma. Guardar el dato como llego permite
      reprocesar con reglas nuevas sin volver a pedirselo a la API, que es
      justamente lo que uno no puede hacer con una fuente externa.
    - `verificar_esquema` corre al principio, no al final. Descubrir que falta
      una tabla despues de descargar 1095 velas es tiempo perdido.
    - Los datos van a config.DIR_BRONCE, nunca a dags/. El scheduler reescanea
      esa carpeta buscando codigo Python cada 30 segundos.

PARAMETROS  (UI -> Trigger DAG w/ config)
    {"dias": 90}              Descarga 90 dias en vez de 365.
    {"forzar_sintetico": true} Ignora la API y genera la serie localmente.
    {"tasa_defectos": 0.40}   Solo con forzar_sintetico: inyecta velas invalidas
                              para que el DAG 02 bloquee el lote. Sirve para
                              capturar la rama de bloqueo.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from comun import clientes_api, config, observabilidad, utilidades, zonas
from comun import repositorio

DAG_SIGUIENTE = "dag_02_calidad"
COMPONENTE = "dag_01_ingesta_batch"

ARCHIVO_MANIFIESTO = "manifiesto.json"


# ---------------------------------------------------------------------------
# TAREAS
# ---------------------------------------------------------------------------
def preparar_lote(**context):
    """Verifica que MySQL este listo y crea el identificador del lote.

    El lote_id se genera aqui y viaja por XCom al resto de tareas y, via
    TriggerDagRunOperator, a los DAGs siguientes. Es el hilo que permite saber
    que archivos, que filas y que registros de la base pertenecen a esta corrida.
    """
    utilidades.encabezado("DAG 01 - PREPARAR LOTE")

    tablas = repositorio.verificar_esquema()
    print("Tablas encontradas en MySQL: " + ", ".join(tablas))

    lote_id = utilidades.nuevo_lote_id()
    dias = int(utilidades.leer_conf(context, "dias", config.DIAS_HISTORIA))

    repositorio.registrar_lote(lote_id, "INICIADO", {
        "simbolos": ",".join(config.SIMBOLOS),
    })

    observabilidad.publicar_control(
        COMPONENTE, "inicio_lote", lote_id=lote_id, estado="INICIADO",
        metricas={"simbolos": config.SIMBOLOS, "dias": dias},
    )

    utilidades.resumen([
        ("lote_id", lote_id),
        ("simbolos", ", ".join(config.SIMBOLOS)),
        ("dias de historia", dias),
        ("zona bronce", config.DIR_BRONCE),
    ])
    return {"lote_id": lote_id, "dias": dias}


def descargar_simbolo(simbolo, **context):
    """Descarga la serie de un simbolo y la escribe en bronce.

    Es la funcion que ejecutan las tres tareas paralelas, una por simbolo. El
    simbolo llega por `op_kwargs`, no se lee de una variable global: asi la
    funcion es probable con cualquier valor y no depende del orden en que se
    construyo el DAG.
    """
    utilidades.encabezado("DAG 01 - DESCARGAR " + simbolo)

    preparacion = context["ti"].xcom_pull(task_ids="preparar_lote")
    lote_id = preparacion["lote_id"]
    dias = preparacion["dias"]

    forzar_sintetico = bool(utilidades.leer_conf(context, "forzar_sintetico", False))
    tasa_defectos = float(utilidades.leer_conf(context, "tasa_defectos",
                                               config.TASA_DEFECTOS_PRUEBA))

    if forzar_sintetico:
        print("Modo sintetico forzado por configuracion del dag_run.")
        marco = clientes_api.generar_klines_sinteticas(
            simbolo, dias=dias, tasa_defectos=tasa_defectos
        )
    else:
        marco = clientes_api.descargar_klines(simbolo, dias=dias)

    if marco.empty:
        raise ValueError(
            "La descarga de " + simbolo + " no devolvio ninguna vela. Revisa el "
            "log de esta tarea: si la API respondio pero vacia, el simbolo puede "
            "haber dejado de cotizar."
        )

    ruta, filas = zonas.escribir_bronce(lote_id, simbolo, marco)

    # El origen se lee del propio dato, no se asume: si la API fallo y se uso el
    # respaldo, el manifiesto y el reporte final tienen que decirlo.
    origenes = sorted(marco["origen"].unique())

    utilidades.resumen([
        ("simbolo", simbolo),
        ("filas", filas),
        ("origen", ", ".join(origenes)),
        ("primera fecha", marco["fecha"].min()),
        ("ultima fecha", marco["fecha"].max()),
        ("archivo", ruta),
    ])

    if "sintetico" in origenes or "sintetico_defectuoso" in origenes:
        observabilidad.advertencia(
            COMPONENTE,
            "Se usaron velas sinteticas para " + simbolo + ": la API no respondio",
        )

    return {
        "simbolo": simbolo,
        "filas": filas,
        "origen": ",".join(origenes),
        "ruta": ruta,
        "primera_fecha": str(marco["fecha"].min()),
        "ultima_fecha": str(marco["fecha"].max()),
    }


def consolidar_ingesta(**context):
    """Reune los resultados de las tareas paralelas y escribe el manifiesto.

    El manifiesto es el contrato entre este DAG y el siguiente: declara cuantas
    filas se escribieron y en que archivos. El DAG 02 lo compara contra lo que
    encuentra en disco, y una discrepancia delata una escritura incompleta antes
    de que los datos malos avancen.
    """
    utilidades.encabezado("DAG 01 - CONSOLIDAR INGESTA")

    ti = context["ti"]
    lote_id = ti.xcom_pull(task_ids="preparar_lote")["lote_id"]

    resultados = [
        ti.xcom_pull(task_ids=_nombre_tarea(simbolo))
        for simbolo in config.SIMBOLOS
    ]
    # Una tarea que fallo y quedo sin XCom devuelve None. Se filtra aqui en vez
    # de dejar que reviente al sumar, para que el mensaje diga que falta.
    faltantes = [
        simbolo for simbolo, resultado in zip(config.SIMBOLOS, resultados)
        if resultado is None
    ]
    if faltantes:
        raise ValueError(
            "No hay resultado de descarga para: " + ", ".join(faltantes) +
            ". Revisa el log de esas tareas."
        )

    total_filas = sum(resultado["filas"] for resultado in resultados)
    origenes = sorted({
        origen
        for resultado in resultados
        for origen in resultado["origen"].split(",")
    })

    manifiesto = {
        "lote_id": lote_id,
        "generado_en": utilidades.a_iso(utilidades.ahora_utc()),
        "dag_origen": context["dag"].dag_id,
        "run_id": context["run_id"],
        "simbolos": config.SIMBOLOS,
        "origenes": origenes,
        "conteos": {resultado["simbolo"]: resultado["filas"] for resultado in resultados},
        "total_filas": total_filas,
        "archivos": {resultado["simbolo"]: resultado["ruta"] for resultado in resultados},
    }

    ruta = utilidades.ruta_en_lote(config.DIR_BRONCE, lote_id, ARCHIVO_MANIFIESTO)
    utilidades.escribir_json(ruta, manifiesto)

    repositorio.registrar_lote(lote_id, "INGESTADO", {
        "simbolos": ",".join(config.SIMBOLOS),
        "filas_descargadas": total_filas,
    })

    observabilidad.publicar_control(
        COMPONENTE, "fin_ingesta", lote_id=lote_id, estado="INGESTADO",
        metricas={"total_filas": total_filas, "origenes": origenes},
    )

    utilidades.resumen([
        ("lote_id", lote_id),
        ("simbolos", len(config.SIMBOLOS)),
        ("total de filas", total_filas),
        ("origenes", ", ".join(origenes)),
        ("manifiesto", ruta),
    ])
    return manifiesto


def _nombre_tarea(simbolo):
    """Nombre de la tarea de descarga de un simbolo.

    Se centraliza aqui porque el nombre se usa en dos sitios: al construir el
    DAG y al recuperar el XCom. Escribirlo dos veces es garantia de que un dia
    dejen de coincidir.
    """
    return "descargar_" + simbolo.lower()


# ---------------------------------------------------------------------------
# DEFINICION DEL DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dag_01_ingesta_batch",
    description="01 - Descarga velas OHLCV desde la API publica y las deja en bronce",
    default_args=config.ARGS_POR_DEFECTO,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=config.ETIQUETAS_BASE + ["01-ingesta", "bronce"],
    doc_md=__doc__,
) as dag:

    preparar = PythonOperator(
        task_id="preparar_lote",
        python_callable=preparar_lote,
    )

    # Una tarea por simbolo. Se construyen en un bucle, pero cada una es una
    # tarea independiente en el grafo: se reintenta sola y se ve sola en la UI.
    descargas = [
        PythonOperator(
            task_id=_nombre_tarea(simbolo),
            python_callable=descargar_simbolo,
            op_kwargs={"simbolo": simbolo},
        )
        for simbolo in config.SIMBOLOS
    ]

    consolidar = PythonOperator(
        task_id="consolidar_ingesta",
        python_callable=consolidar_ingesta,
    )

    disparar_02 = TriggerDagRunOperator(
        task_id="disparar_dag_02",
        trigger_dag_id=DAG_SIGUIENTE,
        conf={"lote_id": "{{ ti.xcom_pull(task_ids='preparar_lote')['lote_id'] }}"},
        trigger_run_id="lote_{{ ti.xcom_pull(task_ids='preparar_lote')['lote_id'] }}",
        wait_for_completion=True,
        poke_interval=config.INTERVALO_ESPERA_TRIGGER,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=True,
        # Sin reintentos. Con reset_dag_run=True, un reintento limpia y relanza
        # el DAG hijo ENTERO, y con la cadena de cuatro niveles cada fallo se
        # multiplica: en el taller anterior el try_number llego a 7.
        retries=0,
    )

    # Flujo:
    #   preparar -> (descargar BTC || ETH || SOL) -> consolidar -> DAG 02
    preparar >> descargas >> consolidar >> disparar_02
