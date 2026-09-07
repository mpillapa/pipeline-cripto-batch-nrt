"""Conciliacion entre el flujo near real-time y el flujo batch.

ES LA PIEZA QUE JUSTIFICA TENER DOS FLUJOS.

Cualquiera puede construir un pipeline batch y otro streaming y presentarlos por
separado. La pregunta que casi nadie responde es si los dos estan midiendo lo
mismo. Este modulo la responde con un numero.

QUE SE COMPARA, Y POR QUE ASI

  Granularidad horaria. Las ventanas del streaming son de un minuto; la vela
  oficial mas fina que se puede traer sin explotar el volumen de la API es la
  horaria. Comparar minuto a minuto exigiria 1440 velas por simbolo y por dia.

  El VWAP horario del streaming se obtiene agregando las 60 ventanas de esa
  hora, ponderado por volumen. La aritmetica sale gratis:

      vwap_hora = suma(volumen_usdt) / suma(volumen_base)

  porque `volumen_usdt` de cada ventana ya es suma(precio * cantidad) y
  `volumen_base` es suma(cantidad). No hace falta ninguna formula ponderada ni
  un script en Elasticsearch: dos sumas simples y una division. Promediar los
  60 valores de `vwap` sin ponderar seria incorrecto, porque daria el mismo peso
  a un minuto con dos operaciones que a uno con dos mil.

  La referencia batch son velas HORARIAS que se descargan en el momento, no se
  guardan. `hechos_ohlcv_diario` es diario por diseno; meter velas horarias ahi
  romperia la granularidad de la tabla de hechos. Y no hacen falta: la
  conciliacion cubre las pocas horas en que el flujo NRT estuvo corriendo, no
  toda la historia.

LAS DOS METRICAS, Y CUAL IMPORTA MAS

  desviacion_pct  Cuanto se aparta el VWAP del streaming del cierre oficial.
  cobertura_pct   Que porcentaje de los trades reales alcanzo a ver el flujo.

  La segunda importa mas. Una desviacion pequena con cobertura del 40 % no
  significa que el streaming este midiendo bien: significa que se perdio mas de
  la mitad de los datos y aun asi el promedio salio parecido, que es casi
  esperable si las perdidas son aleatorias. Sin la cobertura, la desviacion sola
  se puede leer como un exito que no es tal.
"""

import requests

from comun import clientes_api, config, utilidades

# Elasticsearch responde 10 000 documentos como maximo por defecto, pero aqui se
# usa una agregacion y no una busqueda: el tamano de la respuesta lo fija el
# numero de cubos horarios, no el de documentos.
TIEMPO_LIMITE_ES = 30


# ---------------------------------------------------------------------------
# LADO NEAR REAL-TIME
# ---------------------------------------------------------------------------
def construir_consulta(simbolo, desde, hasta):
    """Arma la agregacion que Elasticsearch tiene que resolver.

    Separada de la peticion HTTP a proposito: asi se puede verificar la forma de
    la consulta en una prueba, sin Elasticsearch levantado.

    `size: 0` porque no interesan los documentos, solo los cubos. Sin eso,
    Elasticsearch devuelve ademas los diez primeros documentos de cada consulta,
    que aqui son ruido.
    """
    return {
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    # `simbolo` es keyword en la plantilla de indice. Si fuera
                    # text, este term no encontraria nada y la conciliacion
                    # saldria vacia sin ningun error: es exactamente el fallo
                    # silencioso que la plantilla explicita evita.
                    {"term": {"simbolo": simbolo}},
                    {"range": {"@timestamp": {"gte": desde, "lt": hasta}}},
                ]
            }
        },
        "aggs": {
            "por_hora": {
                "date_histogram": {
                    "field": "@timestamp",
                    "calendar_interval": "hour",
                    # UTC explicito. Sin esto, Elasticsearch usa la zona del
                    # nodo y los cubos horarios quedarian desplazados respecto
                    # de las velas del exchange, que son UTC.
                    "time_zone": "UTC",
                    "min_doc_count": 1,
                },
                "aggs": {
                    "volumen_usdt": {"sum": {"field": "volumen_usdt"}},
                    "volumen_base": {"sum": {"field": "volumen_base"}},
                    "n_trades": {"sum": {"field": "n_trades"}},
                    # Cuantas de las 60 ventanas del minuto llegaron. Un cubo
                    # con 12 ventanas indica que el job estuvo caido casi toda
                    # la hora, y eso cambia como se lee la desviacion.
                    "ventanas": {"value_count": {"field": "vwap"}},
                },
            }
        },
    }


def consultar_metricas_nrt(simbolo, desde, hasta, url_base=None):
    """Pregunta a Elasticsearch por las metricas del streaming, agregadas por hora.

    Devuelve {hora_iso: {vwap, n_trades, ventanas, volumen_base}}.

    Un indice que todavia no existe devuelve 404. Se trata como "no hay datos"
    y no como error: es lo que pasa el primer dia, antes de que el flujo NRT
    haya escrito nada, y no tiene sentido que el DAG falle por eso.
    """
    url_base = url_base or config.ELASTICSEARCH_URL
    url = url_base.rstrip("/") + "/" + config.INDICE_METRICAS_NRT + "/_search"

    try:
        respuesta = requests.get(
            url, json=construir_consulta(simbolo, desde, hasta),
            timeout=TIEMPO_LIMITE_ES,
        )
        if respuesta.status_code == 404:
            print("El indice " + config.INDICE_METRICAS_NRT + " no existe todavia.")
            return {}
        respuesta.raise_for_status()
        cuerpo = respuesta.json()
    except requests.RequestException as error:
        raise RuntimeError(
            "No se pudo consultar Elasticsearch en " + url + ": " + str(error) +
            ". La conciliacion necesita que el flujo near real-time haya "
            "escrito metricas; revisa que Logstash y el job de Spark esten "
            "corriendo."
        )

    return interpretar_respuesta(cuerpo)


def interpretar_respuesta(cuerpo):
    """Traduce la respuesta de Elasticsearch al diccionario por hora.

    Separada de la peticion para poder probarla con una respuesta guardada, sin
    Elasticsearch levantado. Es lo que permite tener la conciliacion terminada y
    verificada antes de que el flujo NRT exista.
    """
    cubos = cuerpo.get("aggregations", {}).get("por_hora", {}).get("buckets", [])

    por_hora = {}
    for cubo in cubos:
        volumen_base = cubo["volumen_base"]["value"] or 0.0
        volumen_usdt = cubo["volumen_usdt"]["value"] or 0.0

        # Sin volumen no hay precio ponderado que calcular. Dividir daria una
        # division por cero; devolver 0.0 seria peor, porque un precio de cero
        # se compararia contra el cierre real y produciria una desviacion del
        # 100 % que parece un hallazgo y es un artefacto.
        vwap = (volumen_usdt / volumen_base) if volumen_base > 0 else None

        por_hora[cubo["key_as_string"]] = {
            "vwap": round(vwap, 8) if vwap is not None else None,
            "n_trades": int(cubo["n_trades"]["value"] or 0),
            "ventanas": int(cubo["ventanas"]["value"] or 0),
            "volumen_base": round(volumen_base, 8),
        }

    return por_hora


# ---------------------------------------------------------------------------
# LADO BATCH
# ---------------------------------------------------------------------------
def obtener_referencia_batch(simbolo, dias=2):
    """Descarga velas HORARIAS como referencia. No se guardan en MySQL.

    Devuelve {hora_iso: {cierre, n_trades}}.

    No se persisten a proposito: `hechos_ohlcv_diario` es diario por diseno, y
    mezclar granularidades en una tabla de hechos es la forma mas rapida de que
    un conteo posterior salga mal sin que nadie lo note.

    Si la API no responde, `descargar_klines` cae a la serie sintetica. Eso
    haria que la conciliacion comparase el streaming real contra una referencia
    inventada, lo que no significa nada, asi que aqui se desactiva el respaldo
    con `permitir_respaldo=False` y se deja fallar.
    """
    marco = clientes_api.descargar_klines(
        simbolo,
        intervalo=config.INTERVALO_HORARIO,
        dias=dias,
        permitir_respaldo=False,
    )

    referencia = {}
    for fila in marco.to_dict("records"):
        hora = utilidades.desde_milisegundos(fila["apertura_ms"])
        clave = hora.strftime("%Y-%m-%dT%H:00:00.000Z")
        referencia[clave] = {
            "cierre": float(fila["cierre"]),
            "n_trades": int(fila["n_trades"]),
        }

    return referencia


# ---------------------------------------------------------------------------
# COMPARACION
# ---------------------------------------------------------------------------
def comparar(simbolo, metricas_nrt, referencia_batch, lote_id):
    """Cruza ambos lados y produce las filas de la tabla `conciliacion`.

    Se recorren las horas del lado NRT, no las del batch: solo tiene sentido
    conciliar las horas en que el flujo en vivo estuvo corriendo. El batch
    siempre tiene mas horas —trae dos dias completos— y compararlas todas
    llenaria la tabla de filas SIN_DATOS que no dicen nada.
    """
    filas = []

    for hora, metrica in sorted(metricas_nrt.items()):
        vela = referencia_batch.get(hora)

        fila = {
            "simbolo": simbolo,
            # MySQL espera DATETIME, no ISO con Z.
            "fecha_hora": hora.replace("T", " ").replace(".000Z", ""),
            "lote_id": lote_id,
            "vwap_streaming": metrica["vwap"],
            "n_trades_streaming": metrica["n_trades"],
            "cierre_batch": vela["cierre"] if vela else None,
            "n_trades_batch": vela["n_trades"] if vela else None,
            "desviacion_pct": None,
            "cobertura_pct": None,
            "veredicto": "SIN_DATOS",
        }

        if vela is None or metrica["vwap"] is None:
            # Falta un lado. La hora se registra igual: una hora sin
            # conciliar es informacion, y borrarla del reporte esconderia
            # justamente los periodos en que el flujo estuvo caido.
            filas.append(fila)
            continue

        if vela["cierre"] > 0:
            fila["desviacion_pct"] = round(
                (metrica["vwap"] - vela["cierre"]) / vela["cierre"] * 100, 4
            )

        if vela["n_trades"] > 0:
            fila["cobertura_pct"] = round(
                metrica["n_trades"] / vela["n_trades"] * 100, 4
            )

        fila["veredicto"] = veredicto(fila["desviacion_pct"], fila["cobertura_pct"])
        filas.append(fila)

    return filas


def veredicto(desviacion_pct, cobertura_pct):
    """Decide el veredicto de una hora. Funcion pura, probada sin infraestructura.

    Tres resultados posibles:

      SIN_DATOS  Falta uno de los dos lados, o no se pudo calcular.
      COINCIDE   La desviacion esta dentro del margen Y la cobertura es
                 suficiente. Las dos condiciones, no una.
      DESVIADO   Cualquier otro caso.

    Que la cobertura entre en el criterio y no sea solo informativa es
    deliberado. Con una cobertura del 30 %, una desviacion pequena no demuestra
    que el streaming mida bien: demuestra que lo poco que vio era representativo,
    que es otra cosa. Marcar eso como COINCIDE seria declarar un exito que no se
    ha probado.
    """
    if desviacion_pct is None or cobertura_pct is None:
        return "SIN_DATOS"

    dentro_del_margen = abs(desviacion_pct) <= config.CONCILIACION_DESVIACION_ACEPTABLE
    cobertura_suficiente = cobertura_pct >= config.CONCILIACION_COBERTURA_MINIMA

    if dentro_del_margen and cobertura_suficiente:
        return "COINCIDE"
    return "DESVIADO"


def resumir(filas):
    """Agrega las filas en las cifras que van al reporte y al log."""
    if not filas:
        return {
            "horas": 0, "coinciden": 0, "desviadas": 0, "sin_datos": 0,
            "desviacion_media_pct": None, "cobertura_media_pct": None,
        }

    desviaciones = [f["desviacion_pct"] for f in filas if f["desviacion_pct"] is not None]
    coberturas = [f["cobertura_pct"] for f in filas if f["cobertura_pct"] is not None]

    return {
        "horas": len(filas),
        "coinciden": sum(1 for f in filas if f["veredicto"] == "COINCIDE"),
        "desviadas": sum(1 for f in filas if f["veredicto"] == "DESVIADO"),
        "sin_datos": sum(1 for f in filas if f["veredicto"] == "SIN_DATOS"),
        # Media de valores ABSOLUTOS. Sin el valor absoluto, una desviacion de
        # +0,4 % y otra de -0,4 % se cancelarian y darian una media de cero, que
        # sugeriria una precision perfecta donde no la hay.
        "desviacion_media_pct": round(
            sum(abs(d) for d in desviaciones) / len(desviaciones), 4
        ) if desviaciones else None,
        "cobertura_media_pct": round(
            sum(coberturas) / len(coberturas), 4
        ) if coberturas else None,
    }
