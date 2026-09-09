"""Deduplicacion de trades en el flujo NRT (P4).

    docker compose run --rm --no-deps -e CRIPTO_KAFKA_EXTERNO=kafka:29092 \\
      --entrypoint python productor -u /pruebas/prueba_deduplicacion.py

Necesita Kafka, Spark, Logstash y Elasticsearch levantados y el flujo principal
corriendo: es el que hace avanzar el watermark.

QUE DEMUESTRA
-------------
Que reenviar trades **ya publicados** no altera la ventana. Kafka garantiza
entrega al-menos-una-vez, asi que un reintento del productor o un consumidor
que reprocesa desde un offset anterior pueden entregar el mismo trade dos
veces. Sin deduplicacion, `n_trades` y `volumen_base` se inflarian y el VWAP
quedaria intacto -porque es una media ponderada- de modo que **el error pasaria
desapercibido justo en la metrica que se mira**.

El job deduplica por `id_trade`, que es unico y creciente por simbolo tanto en
el exchange real como en el simulador.

POR QUE LA PRUEBA DE LOGICA NO BASTA
------------------------------------
`prueba_logica_streaming.py` comprueba la deduplicacion sobre un DataFrame en
memoria. Eso valida la expresion, no el circuito: no dice nada sobre si el
`dropDuplicates` sobrevive al paso por Kafka, al watermark y al estado del
checkpoint. Esta prueba publica de verdad y lee el resultado en Elasticsearch.

COMO SE AISLA
-------------
Simbolo propio (`TESTDEDUP`) para que la ventana salga limpia, y las dos tandas
se publican seguidas para que caigan en la MISMA ventana: si se esperara a que
cerrara, el reenvio llegaria tarde y lo descartaria el watermark, que es otra
cosa distinta y ya la comprueba `prueba_eventos_tardios.py`.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_BROKER = os.environ.get("CRIPTO_KAFKA_EXTERNO", "localhost:9095")
ES_URL = os.environ.get("CRIPTO_ES", "http://elasticsearch:9200")
TOPIC = "trades.crudo"
SIMBOLO = "TESTDEDUP"

TRADES = 40
PRECIO = 50.0
CANTIDAD = 2.0
ESPERA_MAX_S = 300


def _marca(momento):
    return momento.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _tanda(momento):
    """Los mismos TRADES trades, identicos en cada llamada. La igualdad de
    `id_trade` es lo que el job usa para reconocer el duplicado."""
    eventos = []
    for i in range(1, TRADES + 1):
        eventos.append({
            "id_evento": f"dedup-{i}",
            "tipo_fuente": "nrt_trade",
            "simbolo": SIMBOLO,
            "id_trade": 5000 + i,
            "precio": PRECIO,
            "cantidad": CANTIDAD,
            "importe_usdt": PRECIO * CANTIDAD,
            "comprador_es_maker": True,
            "ts_evento": _marca(momento),
            "ts_ingesta": _marca(momento),
            "origen": "simulador",
        })
    return eventos


def _publicar(productor, eventos):
    for evento in eventos:
        productor.send(TOPIC, key=SIMBOLO, value=evento)
    productor.flush()


def _metricas():
    cuerpo = json.dumps({
        "size": 20,
        "query": {"term": {"simbolo": SIMBOLO}},
        "sort": [{"ventana_inicio": "desc"}],
        "_source": ["ventana_inicio", "n_trades", "volumen_base", "vwap"],
    }).encode("utf-8")
    peticion = urllib.request.Request(
        f"{ES_URL}/cripto-nrt_metrica-*/_search",
        data=cuerpo,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(peticion, timeout=30) as respuesta:
            return [h["_source"] for h in json.loads(respuesta.read())["hits"]["hits"]]
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return []
        raise


def main():
    print("=" * 78)
    print("DEDUPLICACION DE TRADES EN EL FLUJO NRT")
    print("=" * 78)
    print()
    print(f"Simbolo de prueba : {SIMBOLO}")
    print(f"Se publican       : {TRADES} trades, y despues LOS MISMOS {TRADES} otra vez")
    print(f"Esperado          : n_trades = {TRADES}      (con deduplicacion)")
    print(f"Si fallara        : n_trades = {TRADES * 2}      (sin deduplicacion)")
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

    previas = {m["ventana_inicio"] for m in _metricas()}

    ahora = datetime.now(timezone.utc)
    eventos = _tanda(ahora)
    _publicar(productor, eventos)
    print(f"Tanda 1 publicada: {TRADES} trades con ts_evento {_marca(ahora)}")
    # Sin pausa larga: las dos tandas tienen que caer en la misma ventana.
    time.sleep(2)
    _publicar(productor, eventos)
    print(f"Tanda 2 publicada: los mismos {TRADES} trades, mismos id_trade")
    productor.close()

    ventana = ahora.replace(second=0, microsecond=0)
    marca = _marca(ventana)
    print(f"Ventana esperada : {marca}")
    print()
    print(f"Esperando el cierre de la ventana (hasta {ESPERA_MAX_S} s) ...")

    inicio = time.time()
    resultado = None
    while time.time() - inicio < ESPERA_MAX_S:
        time.sleep(15)
        for m in _metricas():
            if m["ventana_inicio"] == marca and marca not in previas:
                resultado = m
                break
        if resultado:
            break

    print()
    if not resultado:
        print(f"La ventana {marca} no aparecio en {ESPERA_MAX_S} s.")
        print("Comprueba que Spark esta emitiendo:")
        print("  docker compose logs --tail 30 spark-streaming")
        return 1

    n = resultado["n_trades"]
    volumen = resultado["volumen_base"]
    print(f"  ventana_inicio : {resultado['ventana_inicio']}")
    print(f"  n_trades       : {n}")
    print(f"  volumen_base   : {volumen}")
    print(f"  vwap           : {resultado['vwap']}")
    print()

    print("-" * 78)
    fallos = 0

    if n == TRADES:
        print(f"  OK   n_trades = {TRADES}: el reenvio no se conto dos veces")
    elif n == TRADES * 2:
        print(f"  FALLA  n_trades = {n}: los duplicados se contaron. No hay dedup.")
        fallos += 1
    else:
        print(f"  FALLA  n_trades = {n}, se esperaba {TRADES}")
        fallos += 1

    esperado_volumen = TRADES * CANTIDAD
    if abs(volumen - esperado_volumen) < 0.01:
        print(f"  OK   volumen_base = {esperado_volumen}: tampoco se duplico el volumen")
    else:
        print(f"  FALLA  volumen_base = {volumen}, se esperaba {esperado_volumen}")
        fallos += 1

    print("-" * 78)
    print()
    if fallos:
        print(f"RESULTADO: {fallos} comprobacion(es) fallaron")
        return 1

    print("RESULTADO: la deduplicacion por id_trade funciona de punta a punta")
    print()
    print("Detalle que conviene senalar en la exposicion: el VWAP habria salido")
    print(f"CORRECTO ({PRECIO:.2f}) incluso sin deduplicar, porque duplicar todos los")
    print("trades por igual no cambia una media ponderada. Los que delatan el")
    print("problema son `n_trades` y `volumen_base`, y son justo los dos campos que")
    print("usa la conciliacion para calcular la cobertura.")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
