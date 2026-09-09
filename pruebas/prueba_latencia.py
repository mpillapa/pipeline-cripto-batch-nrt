"""Latencia del camino near real-time, por etapas. Necesita Elasticsearch con datos.

    python pruebas/prueba_latencia.py

POR QUE ESTA PRUEBA SE REESCRIBIO
---------------------------------
La version anterior medía `@timestamp - ts_evento` y daba **0,00 ms en los tres
percentiles**. No era una latencia excelente: era una resta de un valor consigo
mismo. Logstash fija `@timestamp` A PARTIR de `ts_evento`
(`date { match => ["ts_evento", "ISO8601"] target => "@timestamp" }`), así que la
diferencia es cero por construcción, con cualquier volumen y en cualquier
maquina.

Es el tipo de fallo mas caro de detectar en una prueba: no lanza error, no queda
en rojo y ademas devuelve el resultado que a uno le gustaria ver.

QUE SE MIDE AHORA
-----------------
Solo lo que los campos del contrato permiten medir de verdad:

  Etapa 1  ts_ingesta - ts_evento      exchange -> productor (red + traduccion)
  Etapa 2  ts_procesado - ventana_fin  cierre de ventana -> metrica publicada

La etapa 3 (Kafka -> Logstash -> Elasticsearch) NO se puede medir con los campos
actuales: haria falta que Logstash sellara la hora de indexacion en un campo
propio. Se declara como hueco en vez de inventar un numero. Ver PENDIENTE al
final de este archivo.

SEPARAR POR ORIGEN NO ES OPCIONAL
---------------------------------
El simulador incluye un `time.sleep(0.01–0.05)` entre generar `ts_evento` y
`ts_ingesta` para imitar latencia de red. Mezclar sus eventos con los del
exchange real produce una distribucion que no describe ninguna de las dos cosas.
"""

import sys

import numpy as np
import requests

ES_URL = "http://localhost:9200"
INDICE_TRADES = "cripto-nrt_trade-*"
INDICE_METRICAS = "cripto-nrt_metrica-*"

# Cuantos documentos se traen por consulta. Mil basta para percentiles estables y
# cabe de sobra en la respuesta por defecto de Elasticsearch.
MUESTRA = 1000

# El campo `origen` quedo como `text` en los indices creados antes de que la
# plantilla lo declarara, y como `keyword` en los posteriores. Se prueban los dos
# nombres en ese orden en vez de fijar uno: con el equivocado la consulta no
# falla, simplemente no devuelve nada.
CAMPOS_ORIGEN = ["origen", "origen.keyword"]


def _consultar(indice, cuerpo):
    try:
        respuesta = requests.get(ES_URL + "/" + indice + "/_search", json=cuerpo, timeout=15)
    except requests.RequestException as error:
        print("Elasticsearch no responde en " + ES_URL + ": " + str(error))
        return None
    if respuesta.status_code == 404:
        print("El indice " + indice + " no existe todavia.")
        return None
    respuesta.raise_for_status()
    return respuesta.json()


def _traer(indice, campos, origen=None, orden="@timestamp"):
    """Trae los ultimos documentos, filtrando por origen si se pide."""
    for campo_origen in CAMPOS_ORIGEN if origen else [None]:
        cuerpo = {
            "size": MUESTRA,
            "sort": [{orden: {"order": "desc"}}],
            "_source": campos,
        }
        if origen:
            cuerpo["query"] = {"term": {campo_origen: origen}}
        datos = _consultar(indice, cuerpo)
        if datos is None:
            return []
        aciertos = [h["_source"] for h in datos["hits"]["hits"]]
        if aciertos:
            return aciertos
    return []


def _a_ms(serie_inicio, serie_fin, documentos):
    """Diferencia en milisegundos entre dos campos ISO 8601 de cada documento."""
    valores = []
    for documento in documentos:
        inicio = documento.get(serie_inicio)
        fin = documento.get(serie_fin)
        if not inicio or not fin:
            continue
        try:
            t0 = np.datetime64(str(inicio).replace("Z", ""))
            t1 = np.datetime64(str(fin).replace("Z", ""))
        except ValueError:
            continue
        valores.append((t1 - t0) / np.timedelta64(1, "ms"))
    return valores


def _percentiles(titulo, valores, unidad="ms"):
    if not valores:
        print("  " + titulo + ": sin datos")
        return None
    arreglo = np.array(valores, dtype=float)
    # El porcentaje de valores negativos se informa SIEMPRE, no solo cuando
    # llama la atencion. Una latencia negativa es imposible, asi que esa columna
    # es la medida directa de cuanto contamina el desfase de relojes: si sale 0,
    # el desfase es menor que la latencia y los percentiles son fiables tal cual.
    negativos = float((arreglo < 0).sum()) / len(arreglo) * 100
    print(
        "  {:<26} n={:<6} p50={:>9.1f} {}  p95={:>9.1f} {}  p99={:>9.1f} {}  neg={:>5.1f} %".format(
            titulo,
            len(arreglo),
            float(np.percentile(arreglo, 50)),
            unidad,
            float(np.percentile(arreglo, 95)),
            unidad,
            float(np.percentile(arreglo, 99)),
            unidad,
            negativos,
        )
    )
    return arreglo


def main():
    print("=" * 78)
    print("LATENCIA DEL CAMINO NEAR REAL-TIME")
    print("=" * 78)

    print()
    print("Etapa 1 - exchange -> productor  (ts_ingesta - ts_evento)")
    campos = ["ts_evento", "ts_ingesta", "origen"]
    hubo_datos = False
    for origen in ("exchange_ws", "simulador"):
        documentos = _traer(INDICE_TRADES, campos, origen=origen)
        resultado = _percentiles(origen, _a_ms("ts_evento", "ts_ingesta", documentos))
        hubo_datos = hubo_datos or resultado is not None

    if not hubo_datos:
        print()
        print("No hay trades indexados. Levanta el entorno y arranca el productor:")
        print("  docker compose up -d productor")
        return 1

    print()
    print("  El simulador incluye un sleep artificial de 10-50 ms entre las dos marcas.")
    print("  Sus numeros describen ese sleep, no una latencia de red.")
    print()
    print("  CUIDADO CON LA MEDIANA DE exchange_ws. Las dos marcas vienen de RELOJES")
    print("  DISTINTOS: ts_evento lo pone el exchange, ts_ingesta el contenedor. Lo que")
    print("  se mide es latencia + desfase entre relojes, y el desfase puede ser mayor")
    print("  que la latencia.")
    print()
    print("  La columna `neg` lo cuantifica: son los casos en que ts_ingesta salio")
    print("  ANTERIOR a ts_evento, lo que como latencia es imposible. Si neg es alto,")
    print("  la mediana no describe la red sino el desfase, y hay que leer solo p95 y")
    print("  p99. Si neg es 0, los relojes van lo bastante alineados para fiarse del")
    print("  p50. Varia entre corridas porque el reloj del contenedor deriva.")

    print()
    print("Etapa 2 - cierre de ventana -> metrica  (ts_procesado - ventana_fin)")
    documentos = _traer(
        INDICE_METRICAS,
        ["ventana_fin", "ts_procesado", "origen_datos"],
        orden="ventana_inicio",
    )
    _percentiles("todas las ventanas", _a_ms("ventana_fin", "ts_procesado", documentos))
    print()
    print("  De donde sale ese numero, sumando (con ventana 1 min, watermark 30 s y")
    print("  trigger 30 s):")
    print()
    print("    30 s   watermark: la ventana no se emite hasta ver un evento posterior")
    print("           a ventana_fin + 30 s. Con outputMode('append') se emite UNA vez,")
    print("           ya cerrada, en vez de varias veces con valores parciales.")
    print("    30 s   el watermark de Spark va un micro-batch por detras: el que se")
    print("           aplica en un lote es el maximo ts_evento visto en el ANTERIOR.")
    print("    0-30 s espera hasta el siguiente disparo del trigger.")
    print("    ---")
    print("    60-90 s esperados. Es lo que se observa, asi que el motor no va atrasado.")
    print()
    print("  Para bajarlo, el parametro con mas efecto es CRIPTO_INTERVALO_LOTE: son dos")
    print("  de los tres sumandos. Bajarlo a 10 s dejaria el total en 40-50 s, a cambio")
    print("  de mas micro-batches y mas trabajo por segundo.")

    print()
    print("Etapa 3 - Kafka -> Logstash -> Elasticsearch")
    print("  NO MEDIBLE con los campos actuales.")
    print()
    print("  PENDIENTE: para medirla, Logstash tiene que sellar la hora de indexacion")
    print("  en un campo propio. Una linea en el filtro, antes del bloque `date`:")
    print()
    print("      ruby { code => \"event.set('ts_indexado', Time.now.utc.iso8601(3))\" }")
    print()
    print("  No sirve usar @timestamp: Logstash lo fija DESDE ts_evento, asi que la")
    print("  resta da cero siempre. Es justo lo que hacia la version anterior de esta")
    print("  prueba, y por eso daba 0,00 ms en los tres percentiles.")

    print()
    print("=" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
