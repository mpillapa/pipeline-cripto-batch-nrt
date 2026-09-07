"""DAG 03 - Transformacion y enriquecimiento (zona plata).

PROPOSITO
    Convertir las velas validadas en una serie lista para analizar: tipos
    correctos, simbolos normalizados e indicadores derivados (retorno, medias
    moviles, volatilidad y su clasificacion).

POSICION EN EL PIPELINE
    01 ingesta -> 02 calidad -> [03 transformacion] -> 04 carga -> 05 conciliacion

TAREAS
    esperar_validadas    Sensor: comprueba que el archivo del DAG 02 exista.
    transformar_serie    Normaliza, calcula indicadores y clasifica.
    verificar_plata      Comprueba el resultado ANTES de que llegue a MySQL.
    disparar_dag_04      Encadena con la carga.

POR QUE ESTE DISENO
    - Aqui NO hay tres tareas paralelas como en el DAG 02. Los indicadores son
      acumulativos: la volatilidad se calcula sobre el retorno, que a su vez
      necesita la serie ordenada. Partirlo en tareas paralelas obligaria a
      reconstruir el orden tres veces y a unir por clave despues, mas trabajo y
      mas superficie de error para ganar nada.
    - `verificar_plata` existe porque los indicadores son el punto donde un
      error deja de ser evidente. Una vela mal formada la detecta el DAG 02;
      una media movil calculada sobre filas desordenadas produce numeros
      perfectamente plausibles y completamente falsos.
    - El sensor usa un conn_id propio (`fs_cripto`) declarado en el compose. NO
      se puede usar `fs_default`: esa conexion solo existe si la base de Airflow
      se inicializa con --load-default-connections, y este compose no lo hace.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor

from comun import config, observabilidad, repositorio, transformaciones, utilidades, zonas

DAG_SIGUIENTE = "dag_04_carga_mysql"
COMPONENTE = "dag_03_transformacion"

ARCHIVO_VALIDADOS = "validados"
CONN_ARCHIVOS = "fs_cripto"


# ---------------------------------------------------------------------------
# TAREAS
# ---------------------------------------------------------------------------
def ruta_validadas(**context):
    """Resuelve la ruta que el sensor debe vigilar.

    El FileSensor necesita una ruta concreta, y la ruta depende del lote_id, que
    solo se conoce en tiempo de ejecucion. Esta tarea la calcula y la deja en
    XCom para que el sensor la tome por plantilla.
    """
    utilidades.encabezado("DAG 03 - RESOLVER RUTA DEL INSUMO")

    lote_id = utilidades.resolver_lote_id(context)
    ruta = utilidades.ruta_en_lote(
        config.DIR_PLATA, lote_id, ARCHIVO_VALIDADOS + ".parquet", crear=False
    )

    utilidades.resumen([("lote_id", lote_id), ("ruta esperada", ruta)])
    context["ti"].xcom_push(key="lote_id", value=lote_id)
    return ruta


def transformar_serie(**context):
    """Normaliza, calcula indicadores y clasifica la volatilidad.

    Toda la logica vive en comun/transformaciones.py como funciones puras. Esta
    tarea solo lee, llama y escribe: si algo sale mal en un indicador, el sitio
    donde mirar es el modulo, no este archivo.
    """
    utilidades.encabezado("DAG 03 - TRANSFORMAR SERIE")

    ti = context["ti"]
    lote_id = ti.xcom_pull(task_ids="resolver_ruta", key="lote_id")

    validadas = zonas.leer_zona(config.DIR_PLATA, lote_id, ARCHIVO_VALIDADOS)
    filas = validadas.to_dict("records")

    marco = transformaciones.transformar(lote_id, filas)
    if marco.empty:
        raise ValueError(
            "La transformacion del lote " + lote_id + " no produjo ninguna fila. "
            "El DAG 02 promovio un lote vacio, lo que no deberia ocurrir con el "
            "minimo de filas validas configurado."
        )

    ruta, escritas = zonas.escribir_plata(lote_id, marco)

    # Conteo de indicadores calculados frente a nulos. No es un adorno: si
    # sma_30 sale nula en todas las filas, la serie es demasiado corta y los
    # paneles del DAG 05 saldran vacios sin ningun error visible.
    con_sma30 = int(marco["sma_30"].notna().sum())
    con_volatilidad = int(marco["volatilidad_30d"].notna().sum())

    utilidades.resumen([
        ("lote_id", lote_id),
        ("filas transformadas", escritas),
        ("simbolos", ", ".join(sorted(marco["simbolo"].unique()))),
        ("con sma_30 calculada", str(con_sma30) + " de " + str(escritas)),
        ("con volatilidad_30d", str(con_volatilidad) + " de " + str(escritas)),
        ("archivo", ruta),
    ])

    print("Distribucion de nivel_volatilidad:")
    conteo = marco["nivel_volatilidad"].value_counts(dropna=False).to_dict()
    for nivel, cantidad in conteo.items():
        print("  " + str(nivel) + ": " + str(cantidad))

    return {
        "lote_id": lote_id,
        "filas": escritas,
        "con_sma30": con_sma30,
        "con_volatilidad": con_volatilidad,
        "ruta": ruta,
    }


def verificar_plata(**context):
    """Comprueba el resultado de la transformacion ANTES de que llegue a MySQL.

    Tres verificaciones que ninguna regla de calidad del DAG 02 puede hacer,
    porque todas necesitan ver la serie ya transformada:

      1. El orden por simbolo y fecha se mantiene. Si se perdio, todos los
         indicadores estan mal aunque parezcan razonables.
      2. Las columnas coinciden exactamente con las que espera MySQL. Si
         divergen, el INSERT del DAG 04 mete valores corridos de columna, un
         error que el motor no siempre detecta.
      3. No hay infinitos. Un retorno se calcula dividiendo por el cierre
         anterior; si alguno fuera cero se colaria un inf que DECIMAL no admite.
    """
    utilidades.encabezado("DAG 03 - VERIFICAR ZONA PLATA")

    import numpy as np

    ti = context["ti"]
    lote_id = ti.xcom_pull(task_ids="resolver_ruta", key="lote_id")
    marco = zonas.leer_plata(lote_id)

    problemas = []

    # 1. Orden
    ordenado = marco.sort_values(["simbolo", "fecha"]).reset_index(drop=True)
    if not marco.reset_index(drop=True).equals(ordenado):
        problemas.append(
            "La serie no esta ordenada por (simbolo, fecha). Los indicadores "
            "acumulativos calculados sobre este orden no son fiables."
        )

    # 2. Columnas
    faltan = [c for c in transformaciones.COLUMNAS_HECHOS if c not in marco.columns]
    if faltan:
        problemas.append("Faltan columnas que MySQL espera: " + ", ".join(faltan))

    # 3. Infinitos
    numericas = marco.select_dtypes(include=[np.number])
    infinitos = int(np.isinf(numericas.to_numpy()).sum())
    if infinitos:
        problemas.append(
            str(infinitos) + " valor(es) infinito(s) en columnas numericas. "
            "Se produce al dividir por un cierre igual a cero."
        )

    if problemas:
        for problema in problemas:
            print("PROBLEMA: " + problema)
        raise ValueError(
            "La zona plata del lote " + lote_id + " no supera la verificacion. "
            "Ver los problemas listados arriba."
        )

    utilidades.resumen([
        ("filas", len(marco)),
        ("orden por simbolo y fecha", "correcto"),
        ("columnas para MySQL", "completas"),
        ("valores infinitos", 0),
    ])

    repositorio.registrar_lote(lote_id, "TRANSFORMADO")
    observabilidad.publicar_control(
        COMPONENTE, "fin_transformacion", lote_id=lote_id, estado="TRANSFORMADO",
        metricas={"filas": len(marco)},
    )

    return {"lote_id": lote_id, "filas": len(marco)}


# ---------------------------------------------------------------------------
# DEFINICION DEL DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dag_03_transformacion",
    description="03 - Normaliza la serie y calcula retorno, medias moviles y volatilidad",
    default_args=config.ARGS_POR_DEFECTO,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=config.ETIQUETAS_BASE + ["03-transformacion", "plata"],
    doc_md=__doc__,
) as dag:

    resolver = PythonOperator(
        task_id="resolver_ruta",
        python_callable=ruta_validadas,
    )

    esperar = FileSensor(
        task_id="esperar_validadas",
        fs_conn_id=CONN_ARCHIVOS,
        filepath="{{ ti.xcom_pull(task_ids='resolver_ruta') }}",
        poke_interval=10,
        # Dos minutos: el archivo ya deberia existir cuando este DAG arranca,
        # porque el DAG 02 lo escribio antes de dispararlo. El sensor esta para
        # detectar una escritura incompleta, no para esperar de verdad.
        timeout=120,
        mode="reschedule",
    )

    transformar = PythonOperator(
        task_id="transformar_serie",
        python_callable=transformar_serie,
    )

    verificar = PythonOperator(
        task_id="verificar_plata",
        python_callable=verificar_plata,
    )

    disparar_04 = TriggerDagRunOperator(
        task_id="disparar_dag_04",
        trigger_dag_id=DAG_SIGUIENTE,
        conf={"lote_id": "{{ ti.xcom_pull(task_ids='resolver_ruta', key='lote_id') }}"},
        trigger_run_id="lote_{{ ti.xcom_pull(task_ids='resolver_ruta', key='lote_id') }}",
        wait_for_completion=True,
        poke_interval=config.INTERVALO_ESPERA_TRIGGER,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=True,
        retries=0,
    )

    resolver >> esperar >> transformar >> verificar >> disparar_04
