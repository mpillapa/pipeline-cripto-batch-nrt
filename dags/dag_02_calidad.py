"""DAG 02 - Control de calidad y cuarentena.

PROPOSITO
    Decidir si el lote de bronce merece seguir. Valida las velas contra el
    catalogo de reglas, aparta las defectuosas en cuarentena y, segun el
    porcentaje de rechazo, promueve el lote o lo bloquea.

POSICION EN EL PIPELINE
    01 ingesta -> [02 calidad] -> 03 transformacion -> 04 carga -> 05 conciliacion

TAREAS
    verificar_lote          Localiza el lote y comprueba el manifiesto.
    validar_estructura      | Tres verificaciones INDEPENDIENTES sobre el
    aplicar_reglas_calidad  | mismo insumo, ejecutadas EN PARALELO.
    verificar_continuidad   |
    decidir_ruta            Bifurcacion: promover o bloquear.
    promover_lote / bloquear_lote   Ramas mutuamente excluyentes.
    disparar_dag_03         Solo se ejecuta si el lote fue promovido.
    cerrar_calidad          Cierre unico para ambas ramas.

POR QUE ESTE DISENO
    - Las reglas de negocio NO estan en este archivo: viven en
      comun/reglas_calidad.py, donde se pueden leer y probar sin Airflow.
    - La bifurcacion es un BranchPythonOperator y no un `raise`: un lote sucio
      no es un error del pipeline, es un resultado valido que hay que registrar.
      Si se lanzara una excepcion, Airflow reintentaria una tarea que va a
      fallar siempre, porque los datos no van a mejorar solos.
    - `disparar_dag_03` cuelga de `promover_lote`. Cuando el lote se bloquea,
      Airflow marca esa rama como `skipped` y la cadena se detiene aqui sola.
    - `cerrar_calidad` usa NONE_FAILED_MIN_ONE_SUCCESS porque una de sus dos
      ramas padre SIEMPRE va a estar en `skipped`. Con la regla por defecto
      (ALL_SUCCESS) la tarea final quedaria omitida en los dos escenarios.

COMO PROBAR LAS DOS RAMAS  (UI -> Trigger DAG w/ config, en el DAG 01)
    {"forzar_sintetico": true, "tasa_defectos": 0.02}  -> lote PROMOVIDO
    {"forzar_sintetico": true, "tasa_defectos": 0.40}  -> lote BLOQUEADO
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from comun import config, observabilidad, reglas_calidad, repositorio, utilidades, zonas

DAG_SIGUIENTE = "dag_03_transformacion"
COMPONENTE = "dag_02_calidad"

ARCHIVO_MANIFIESTO = "manifiesto.json"
ARCHIVO_VALIDADOS = "validados"
ARCHIVO_INFORME = "informe_calidad.json"

# Ids de las dos ramas. Constantes y no literales sueltos: el
# BranchPythonOperator devuelve uno de estos nombres y un error de tipeo ahi
# provoca un fallo confuso en tiempo de ejecucion.
RAMA_PROMOVER = "promover_lote"
RAMA_BLOQUEAR = "bloquear_lote"


# ---------------------------------------------------------------------------
# TAREAS
# ---------------------------------------------------------------------------
def verificar_lote(**context):
    """Resuelve el lote_id y comprueba que el insumo del DAG 01 este completo.

    Falla temprano y con un mensaje claro si falta un archivo. Sin esta tarea,
    el fallo aparece mas adelante como un FileNotFoundError en medio de la
    validacion, mucho mas dificil de interpretar.
    """
    utilidades.encabezado("DAG 02 - VERIFICAR LOTE")

    lote_id = utilidades.resolver_lote_id(context)
    carpeta = utilidades.dir_lote(config.DIR_BRONCE, lote_id, crear=False)

    if not os.path.isdir(carpeta):
        raise FileNotFoundError(
            "No existe la carpeta del lote " + lote_id + " en " + config.DIR_BRONCE +
            ". Ejecuta primero dag_01_ingesta_batch."
        )

    ruta_manifiesto = os.path.join(carpeta, ARCHIVO_MANIFIESTO)
    if not os.path.exists(ruta_manifiesto):
        raise FileNotFoundError(
            "El lote " + lote_id + " no tiene manifiesto. El DAG 01 creo la "
            "carpeta y fallo antes de consolidar."
        )

    manifiesto = utilidades.leer_json(ruta_manifiesto)

    utilidades.resumen([
        ("lote_id", lote_id),
        ("carpeta", carpeta),
        ("simbolos", ", ".join(manifiesto.get("simbolos", []))),
        ("filas declaradas", manifiesto.get("total_filas")),
        ("origenes", ", ".join(manifiesto.get("origenes", []))),
    ])
    return lote_id


def validar_estructura(**context):
    """Verifica la FORMA de los datos, no el contenido de los registros.

    Compara el conteo real en disco contra lo que declara el manifiesto. Un
    desajuste aqui indica escritura corrupta o interrumpida, no datos de mala
    calidad, y son dos problemas con causas y soluciones distintas.
    """
    utilidades.encabezado("DAG 02 - VALIDAR ESTRUCTURA")

    lote_id = context["ti"].xcom_pull(task_ids="verificar_lote")
    marco = zonas.leer_bronce(lote_id)

    if marco.empty:
        raise ValueError("El lote " + lote_id + " no tiene ninguna vela en bronce")

    manifiesto = utilidades.leer_json(
        utilidades.ruta_en_lote(config.DIR_BRONCE, lote_id, ARCHIVO_MANIFIESTO, crear=False)
    )
    declaradas = manifiesto.get("total_filas")
    if declaradas is not None and declaradas != len(marco):
        raise ValueError(
            "El manifiesto declara " + str(declaradas) + " velas y en disco hay " +
            str(len(marco)) + ". Indica una escritura incompleta o solapada."
        )

    # Las columnas se comprueban contra la lista del cliente de la API, que es
    # quien define la forma de la zona bronce.
    from comun.clientes_api import COLUMNAS_BRONCE
    faltan = [columna for columna in COLUMNAS_BRONCE if columna not in marco.columns]
    if faltan:
        raise ValueError("Faltan columnas en bronce: " + ", ".join(faltan))

    utilidades.resumen([
        ("filas en disco", len(marco)),
        ("filas declaradas", declaradas),
        ("columnas", len(marco.columns)),
        ("simbolos presentes", ", ".join(sorted(marco["simbolo"].unique()))),
    ])
    return {"filas": len(marco), "columnas": len(marco.columns)}


def aplicar_reglas_calidad(**context):
    """Aplica el catalogo de reglas y separa validas de rechazadas.

    Las rechazadas se conservan en cuarentena con la columna `motivo_rechazo`,
    no se descartan: sin el motivo escrito al lado del registro, nadie puede
    corregir el origen del problema.
    """
    utilidades.encabezado("DAG 02 - APLICAR REGLAS DE CALIDAD")

    lote_id = context["ti"].xcom_pull(task_ids="verificar_lote")
    marco = zonas.leer_bronce(lote_id)

    # Las reglas trabajan sobre diccionarios, no sobre el DataFrame: son
    # funciones puras probables sin pandas. A esta escala (unos miles de filas)
    # la conversion es irrelevante frente a la claridad que da.
    filas = marco.to_dict("records")
    validas, rechazadas, informe = reglas_calidad.validar_lote(filas)

    import pandas as pd

    ruta_validas, _ = zonas.escribir_zona(
        config.DIR_PLATA, lote_id, ARCHIVO_VALIDADOS, pd.DataFrame(validas)
    )

    ruta_rechazos = None
    if rechazadas:
        ruta_rechazos, _ = zonas.escribir_cuarentena(lote_id, pd.DataFrame(rechazadas))

    utilidades.resumen([
        ("filas evaluadas", informe["filas_evaluadas"]),
        ("validas", informe["filas_validas"]),
        ("rechazadas", informe["filas_rechazadas"]),
        ("tasa de rechazo", str(round(informe["tasa_rechazo"] * 100, 2)) + " %"),
        ("umbral configurado", str(round(config.UMBRAL_RECHAZO * 100, 2)) + " %"),
    ])
    print("Rechazos por regla:")
    for regla, cantidad in informe["conteo_por_regla"].items():
        print("  " + regla + ": " + str(cantidad))
    if not informe["conteo_por_regla"]:
        print("  ninguno")

    informe["ruta_validadas"] = ruta_validas
    informe["ruta_rechazadas"] = ruta_rechazos
    return informe


def verificar_continuidad(**context):
    """Busca huecos en la serie temporal de cada simbolo.

    Es una verificacion INFORMATIVA: reporta los dias faltantes pero no rechaza
    nada. Un hueco no invalida las velas que si llegaron, pero si explica una
    media movil que se ve rara, y tenerlo medido en el log evita que alguien
    pierda una tarde buscando el motivo.

    Ninguna regla de fila puede detectar esto: un hueco no es una fila mala, es
    una fila que no esta.
    """
    utilidades.encabezado("DAG 02 - VERIFICAR CONTINUIDAD DE LA SERIE")

    import pandas as pd

    lote_id = context["ti"].xcom_pull(task_ids="verificar_lote")
    marco = zonas.leer_bronce(lote_id)

    resultado = {}
    for simbolo in sorted(marco["simbolo"].unique()):
        fechas = pd.to_datetime(marco[marco["simbolo"] == simbolo]["fecha"]).sort_values()
        if len(fechas) < 2:
            resultado[simbolo] = {"dias": len(fechas), "huecos": 0}
            continue

        esperadas = pd.date_range(fechas.min(), fechas.max(), freq="D")
        faltantes = sorted(set(esperadas) - set(fechas))

        resultado[simbolo] = {
            "dias": len(fechas),
            "huecos": len(faltantes),
            "primeros_faltantes": [f.date().isoformat() for f in faltantes[:5]],
        }

        print(simbolo + ": " + str(len(fechas)) + " dias, " + str(len(faltantes)) + " huecos")
        if faltantes:
            print("  primeros: " + ", ".join(f.date().isoformat() for f in faltantes[:5]))

    return resultado


def decidir_ruta(**context):
    """Bifurcacion del DAG: devuelve el task_id de la rama a ejecutar.

    Un BranchPythonOperator debe devolver el NOMBRE de una tarea. Airflow
    ejecuta esa y marca las demas ramas como `skipped`.

    El criterio no se calcula aqui: se delega en reglas_calidad.decidir_promocion,
    que es una funcion pura y esta cubierta por la prueba de logica. Esta tarea
    solo traduce la decision a un nombre de tarea.
    """
    utilidades.encabezado("DAG 02 - DECIDIR RUTA")

    informe = context["ti"].xcom_pull(task_ids="aplicar_reglas_calidad")
    promover, motivo = reglas_calidad.decidir_promocion(informe)

    utilidades.resumen([
        ("tasa de rechazo", str(round(informe["tasa_rechazo"] * 100, 2)) + " %"),
        ("filas validas", informe["filas_validas"]),
        ("decision", "PROMOVER" if promover else "BLOQUEAR"),
        ("motivo", motivo),
    ])

    if promover:
        return RAMA_PROMOVER

    # Se deja el motivo en XCom para que la rama de bloqueo lo escriba en el
    # informe sin tener que recalcularlo.
    context["ti"].xcom_push(key="motivo_bloqueo", value=motivo)
    return RAMA_BLOQUEAR


def promover_lote(**context):
    """Marca el lote como apto y escribe el informe de calidad."""
    utilidades.encabezado("DAG 02 - PROMOVER LOTE")

    lote_id = context["ti"].xcom_pull(task_ids="verificar_lote")
    informe = _armar_informe(context, lote_id, estado="PROMOVIDO", motivo=None)

    utilidades.resumen([
        ("lote_id", lote_id),
        ("estado", "PROMOVIDO"),
        ("filas validas", informe["calidad"]["filas_validas"]),
        ("informe", informe["ruta_informe"]),
    ])
    return lote_id


def bloquear_lote(**context):
    """Registra el bloqueo y detiene la cadena.

    No lanza excepcion. El lote bloqueado es un resultado legitimo del control
    de calidad, no un fallo del pipeline: la tarea termina en `success` y es
    `disparar_dag_03` la que queda en `skipped`.
    """
    utilidades.encabezado("DAG 02 - BLOQUEAR LOTE")

    ti = context["ti"]
    lote_id = ti.xcom_pull(task_ids="verificar_lote")
    motivo = ti.xcom_pull(task_ids="decidir_ruta", key="motivo_bloqueo") or "sin motivo"

    informe = _armar_informe(context, lote_id, estado="BLOQUEADO", motivo=motivo)

    observabilidad.advertencia(
        COMPONENTE, "Lote " + lote_id + " bloqueado: " + motivo
    )

    utilidades.resumen([
        ("lote_id", lote_id),
        ("estado", "BLOQUEADO"),
        ("motivo", motivo),
        ("informe", informe["ruta_informe"]),
    ])
    print("La cadena se detiene aqui: disparar_dag_03 quedara en estado 'skipped'.")
    return lote_id


def _armar_informe(context, lote_id, estado, motivo):
    """Construye y escribe el informe de calidad del lote.

    Compartido por las dos ramas: cambia el estado y el motivo, no la
    estructura. Duplicar esta funcion en cada rama garantizaria que tarde o
    temprano diverjan.
    """
    ti = context["ti"]
    calidad = ti.xcom_pull(task_ids="aplicar_reglas_calidad")

    informe = {
        "lote_id": lote_id,
        "evaluado_en": utilidades.a_iso(utilidades.ahora_utc()),
        "dag_origen": context["dag"].dag_id,
        "run_id": context["run_id"],
        "estado": estado,
        "motivo": motivo,
        "umbral_rechazo": config.UMBRAL_RECHAZO,
        "minimo_filas_validas": config.MINIMO_FILAS_VALIDAS,
        "estructura": ti.xcom_pull(task_ids="validar_estructura"),
        "calidad": calidad,
        "continuidad": ti.xcom_pull(task_ids="verificar_continuidad"),
    }

    ruta = utilidades.ruta_en_lote(config.DIR_PLATA, lote_id, ARCHIVO_INFORME)
    utilidades.escribir_json(ruta, informe)
    informe["ruta_informe"] = ruta

    repositorio.registrar_lote(lote_id, estado, {
        "filas_validas": calidad["filas_validas"],
        "filas_rechazadas": calidad["filas_rechazadas"],
        "tasa_rechazo": calidad["tasa_rechazo"],
    })

    observabilidad.publicar_control(
        COMPONENTE, "fin_calidad", lote_id=lote_id, estado=estado,
        metricas={
            "filas_evaluadas": calidad["filas_evaluadas"],
            "filas_validas": calidad["filas_validas"],
            "filas_rechazadas": calidad["filas_rechazadas"],
            "tasa_rechazo": calidad["tasa_rechazo"],
        },
    )
    return informe


def cerrar_calidad(**context):
    """Cierre comun a las dos ramas.

    Se ejecuta tanto si el lote se promovio como si se bloqueo, por eso su
    trigger_rule es NONE_FAILED_MIN_ONE_SUCCESS. Deja en el log el resultado
    final del control de calidad, que es lo que se revisa en la UI.
    """
    utilidades.encabezado("DAG 02 - CIERRE DEL CONTROL DE CALIDAD")

    ti = context["ti"]
    lote_id = ti.xcom_pull(task_ids="verificar_lote")
    # Solo una de las dos ramas dejo valor; la otra quedo en skipped y devuelve None.
    estado = "PROMOVIDO" if ti.xcom_pull(task_ids=RAMA_PROMOVER) else "BLOQUEADO"

    utilidades.resumen([("lote_id", lote_id), ("resultado", estado)])
    return {"lote_id": lote_id, "estado": estado}


# ---------------------------------------------------------------------------
# DEFINICION DEL DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dag_02_calidad",
    description="02 - Valida el lote, aparta rechazos en cuarentena y decide si continua",
    default_args=config.ARGS_POR_DEFECTO,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=config.ETIQUETAS_BASE + ["02-calidad", "cuarentena"],
    doc_md=__doc__,
) as dag:

    verificar = PythonOperator(
        task_id="verificar_lote",
        python_callable=verificar_lote,
    )

    estructura = PythonOperator(
        task_id="validar_estructura",
        python_callable=validar_estructura,
    )

    reglas = PythonOperator(
        task_id="aplicar_reglas_calidad",
        python_callable=aplicar_reglas_calidad,
    )

    continuidad = PythonOperator(
        task_id="verificar_continuidad",
        python_callable=verificar_continuidad,
    )

    decision = BranchPythonOperator(
        task_id="decidir_ruta",
        python_callable=decidir_ruta,
    )

    promover = PythonOperator(
        task_id=RAMA_PROMOVER,
        python_callable=promover_lote,
    )

    bloquear = PythonOperator(
        task_id=RAMA_BLOQUEAR,
        python_callable=bloquear_lote,
    )

    disparar_03 = TriggerDagRunOperator(
        task_id="disparar_dag_03",
        trigger_dag_id=DAG_SIGUIENTE,
        conf={"lote_id": "{{ ti.xcom_pull(task_ids='verificar_lote') }}"},
        trigger_run_id="lote_{{ ti.xcom_pull(task_ids='verificar_lote') }}",
        wait_for_completion=True,
        poke_interval=config.INTERVALO_ESPERA_TRIGGER,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=True,
        retries=0,
    )

    cierre = PythonOperator(
        task_id="cerrar_calidad",
        python_callable=cerrar_calidad,
        # Imprescindible: una de las dos ramas padre siempre estara en skipped.
        # Con ALL_SUCCESS esta tarea no se ejecutaria nunca.
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Flujo:
    #   verificar -> (estructura || reglas || continuidad) -> decision
    #   decision -> promover -> DAG 03 -> cierre
    #   decision -> bloquear ------------> cierre   (la cadena se corta aqui)
    verificar >> [estructura, reglas, continuidad] >> decision
    decision >> promover >> disparar_03 >> cierre
    decision >> bloquear >> cierre
