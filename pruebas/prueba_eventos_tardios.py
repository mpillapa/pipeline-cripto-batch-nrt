"""Tratamiento de eventos tardios por el watermark (P6).

    docker compose run --rm --no-deps -e CRIPTO_KAFKA_EXTERNO=kafka:29092 \\
      --entrypoint python productor -u /pruebas/prueba_eventos_tardios.py

Necesita Kafka, Spark, Logstash y Elasticsearch levantados y el flujo principal
corriendo: es el que hace avanzar el watermark.

QUE DEMUESTRA
-------------
Que el job **no trata igual a todos los eventos que llegan tarde**. Con un
watermark de 30 segundos:

  - un trade con `ts_evento` de hace 20 s entra en su ventana, aunque llegue
    despues de que el reloj haya pasado;
  - uno de hace 90 s se descarta, porque su ventana ya se cerro y reabrirla
    obligaria a mantener estado indefinidamente.

Esa frontera es una decision de diseno, no un accidente: el watermark acota
cuanta memoria puede acumular el motor a cambio de perder los eventos que
lleguen mas tarde que ese margen. Lo importante para la exposicion es que el
descarte es **explicito y acotado**, no un dato que se pierde en silencio por
un defecto.

COMO SE AISLA LA MEDICION
-------------------------
Los trades de prueba se publican con un simbolo propio (`TESTLATE`), que no
existe en el flujo real. Como Spark agrupa por simbolo, sus ventanas salen
limpias y no hay que separar sus trades de los de BTCUSDT.

El simbolo de prueba no rompe nada aguas abajo: la plantilla de Elasticsearch
declara `simbolo` como `keyword`, y el DAG 05 concilia solo los tres simbolos
del contrato.
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import urllib.error
import urllib.request

from kafka import KafkaProducer

KAFKA_BROKER = os.environ.get("CRIPTO_KAFKA_EXTERNO", "localhost:9095")
ES_URL = os.environ.get("CRIPTO_ES", "http://elasticsearch:9200")
TOPIC = "trades.crudo"
SIMBOLO = "TESTLATE"

# El job corre con watermark de 30 s. `A_TIEMPO` cae dentro y `TARDE` fuera, con
# margen suficiente para que la prueba no dependa de decimas de segundo.
RETRASO_A_TIEMPO_S = 20
RETRASO_TARDE_S = 90
TRADES_POR_TANDA = 30

# La ventana tarda 60-90 s en emitirse (watermark + micro-batch + trigger) y
# despues hay que indexarla.
ESPERA_MAX_S = 300


def _marca(momento):
    return momento.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _publicar(productor, retraso_s, contador_desde):
    """Publica una tanda con `ts_evento` retrasado. Devuelve la ventana afectada."""
    ahora = datetime.now(timezone.utc)
    ts_evento = ahora - timedelta(seconds=retraso_s)
    # Minuto al que pertenece: es la ventana en la que deberia caer si se acepta.
    ventana = ts_evento.replace(second=0, microsecond=0)

    for i in range(TRADES_POR_TANDA):
        evento = {
            "id_evento": f"tardio-{retraso_s}-{contador_desde + i}",
            "tipo_fuente": "nrt_trade",
            "simbolo": SIMBOLO,
            "id_trade": contador_desde + i,
            # Precio distinto por tanda para reconocer cual entro si se mezclaran.
            "precio": 100.0 if retraso_s == RETRASO_A_TIEMPO_S else 200.0,
            "cantidad": 1.0,
            "importe_usdt": 100.0 if retraso_s == RETRASO_A_TIEMPO_S else 200.0,
            "comprador_es_maker": True,
            "ts_evento": _marca(ts_evento),
            "ts_ingesta": _marca(ahora),
            "origen": "simulador",
        }
        productor.send(TOPIC, key=SIMBOLO, value=evento)
    productor.flush()
    return ventana


def _metricas_del_simbolo():
    """Consulta con urllib y no con requests: el contenedor del productor solo
    trae kafka-python y websocket-client, y esta prueba corre ahi porque es
    donde vive la libreria de Kafka. Mismo criterio que `observabilidad.py`."""
    cuerpo = json.dumps({
        "size": 50,
        "query": {"term": {"simbolo": SIMBOLO}},
        "sort": [{"ventana_inicio": "desc"}],
        "_source": ["ventana_inicio", "n_trades", "vwap", "precio_maximo"],
    }).encode("utf-8")
    peticion = urllib.request.Request(
        f"{ES_URL}/cripto-nrt_metrica-*/_search",
        data=cuerpo,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(peticion, timeout=30) as respuesta:
            datos = json.loads(respuesta.read())
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return []
        raise
    return [h["_source"] for h in datos["hits"]["hits"]]


def main():
    print("=" * 78)
    print("EVENTOS TARDIOS Y WATERMARK")
    print("=" * 78)
    print()
    print(f"Simbolo de prueba : {SIMBOLO}")
    print(f"Watermark del job : 30 s")
    print(f"Tanda A TIEMPO    : ts_evento de hace {RETRASO_A_TIEMPO_S} s"
          f"  ({TRADES_POR_TANDA} trades, precio 100)")
    print(f"Tanda TARDE       : ts_evento de hace {RETRASO_TARDE_S} s"
          f"  ({TRADES_POR_TANDA} trades, precio 200)")
    print()

    try:
        productor = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )
    except Exception as error:
        print(f"No se pudo conectar a Kafka en {KAFKA_BROKER}: {error}")
        return 1

    previas = {m["ventana_inicio"] for m in _metricas_del_simbolo()}

    ventana_a_tiempo = _publicar(productor, RETRASO_A_TIEMPO_S, 1000)
    ventana_tarde = _publicar(productor, RETRASO_TARDE_S, 2000)
    productor.close()

    print(f"Publicadas. Ventana esperada A TIEMPO : {_marca(ventana_a_tiempo)}")
    print(f"            Ventana esperada TARDE    : {_marca(ventana_tarde)}")
    print()
    print(f"Esperando a que el job cierre las ventanas (hasta {ESPERA_MAX_S} s) ...")

    inicio = time.time()
    metricas = []
    while time.time() - inicio < ESPERA_MAX_S:
        time.sleep(15)
        metricas = _metricas_del_simbolo()
        if {m["ventana_inicio"] for m in metricas} - previas:
            # Un margen extra: puede que la segunda ventana aun este por salir.
            time.sleep(45)
            metricas = _metricas_del_simbolo()
            break

    nuevas = [m for m in metricas if m["ventana_inicio"] not in previas]
    print(f"Ventanas nuevas de {SIMBOLO}: {len(nuevas)}")
    for m in nuevas:
        print(f"  {m['ventana_inicio']}  n_trades={m['n_trades']:>4}  "
              f"vwap={m['vwap']:.2f}  max={m['precio_maximo']:.2f}")
    print()

    marca_a_tiempo = _marca(ventana_a_tiempo)
    marca_tarde = _marca(ventana_tarde)
    por_ventana = {m["ventana_inicio"]: m for m in nuevas}

    print("-" * 78)
    fallos = 0

    aceptada = por_ventana.get(marca_a_tiempo)
    if aceptada:
        print(f"  OK   la tanda de {RETRASO_A_TIEMPO_S} s ENTRO en su ventana "
              f"({aceptada['n_trades']} trades)")
    else:
        print(f"  FALLA  la tanda de {RETRASO_A_TIEMPO_S} s no aparece en "
              f"{marca_a_tiempo}")
        print("         Deberia entrar: 20 s < 30 s de watermark.")
        fallos += 1

    descartada = por_ventana.get(marca_tarde)
    if descartada is None:
        print(f"  OK   la tanda de {RETRASO_TARDE_S} s se DESCARTO "
              f"(no hay ventana {marca_tarde[11:19]})")
    elif descartada["precio_maximo"] < 150:
        print(f"  OK   la ventana {marca_tarde[11:19]} existe pero NO contiene los")
        print(f"       trades tardios: su precio maximo es "
              f"{descartada['precio_maximo']:.2f}, no 200")
    else:
        print(f"  FALLA  los trades de {RETRASO_TARDE_S} s entraron en "
              f"{marca_tarde}: precio maximo {descartada['precio_maximo']:.2f}")
        print("         Con watermark de 30 s deberian haberse descartado.")
        fallos += 1

    print("-" * 78)
    print()
    if fallos:
        print(f"RESULTADO: {fallos} comprobacion(es) fallaron")
        return 1

    print("RESULTADO: el watermark distingue lo tardio recuperable de lo tardio perdido")
    print()
    print("Que se descarte lo de 90 s no es una perdida silenciosa: es el precio")
    print("declarado de acotar el estado. Ampliar el watermark recuperaria mas eventos")
    print("tardios a cambio de mas memoria y de emitir cada ventana mas tarde, porque")
    print("el margen se suma a la latencia de TODAS las ventanas, no solo de las que")
    print("traen eventos con retraso.")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
