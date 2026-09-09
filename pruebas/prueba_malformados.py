"""Tolerancia a eventos malformados en el flujo NRT (P7).

    docker compose run --rm --no-deps -e CRIPTO_KAFKA_EXTERNO=kafka:29092 \\
      --entrypoint python productor -u /pruebas/prueba_malformados.py

QUE DEMUESTRA
-------------
Que un mensaje corrupto en el topic **no tumba el pipeline ni contamina las
metricas**. Es lo que separa un pipeline de demostracion de uno utilizable: en
un bus compartido siempre acaba entrando basura -un productor a medio desplegar,
un mensaje truncado, un campo que cambio de tipo- y el consumidor no puede
elegir el dia en que se encuentra con ella.

Un stream que muere al primer JSON invalido, ademas, se queda muerto: al
reiniciar vuelve al mismo offset, encuentra el mismo mensaje y vuelve a morir.

QUE SE INYECTA
--------------
Cinco formas distintas de estar mal, porque fallan en sitios distintos:

  1. JSON sintacticamente invalido      -> falla al parsear
  2. JSON valido que no es un objeto    -> falla al aplicar el esquema
  3. Faltan campos obligatorios         -> el esquema los deja nulos
  4. `precio` como texto no numerico    -> el cast a double da nulo
  5. `ts_evento` con formato invalido   -> sin marca de tiempo no hay ventana

El job los descarta en `parsear()`, que filtra las filas con nulos en los
campos que la agregacion necesita. Sin ese filtro no fallaria: produciria un
VWAP con nulos, que es peor, porque nadie se entera.

COMO SE COMPRUEBA
-----------------
Se publica una tanda mezclada -validos y malformados- con simbolo propio, y se
verifica que la ventana resultante contiene EXACTAMENTE los validos.
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
SIMBOLO = "TESTMAL"

VALIDOS = 20
PRECIO = 10.0
CANTIDAD = 3.0
ESPERA_MAX_S = 300


def _marca(momento):
    return momento.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _valido(i, momento):
    return {
        "id_evento": f"mal-ok-{i}",
        "tipo_fuente": "nrt_trade",
        "simbolo": SIMBOLO,
        "id_trade": 9000 + i,
        "precio": PRECIO,
        "cantidad": CANTIDAD,
        "importe_usdt": PRECIO * CANTIDAD,
        "comprador_es_maker": True,
        "ts_evento": _marca(momento),
        "ts_ingesta": _marca(momento),
        "origen": "simulador",
    }


def _malformados(momento):
    """Devuelve [(descripcion, bytes_crudos)]. Se publican tal cual, sin
    serializar: parte de la gracia es mandar cosas que no son JSON valido."""
    base = _valido(999, momento)

    sin_precio = dict(base)
    sin_precio.pop("precio")
    sin_precio["id_evento"] = "mal-sin-precio"

    precio_texto = dict(base)
    precio_texto["precio"] = "no-es-un-numero"
    precio_texto["id_evento"] = "mal-precio-texto"

    fecha_rota = dict(base)
    fecha_rota["ts_evento"] = "ayer por la tarde"
    fecha_rota["id_evento"] = "mal-fecha"

    return [
        ("JSON invalido", b'{"simbolo": "TESTMAL", "precio": '),
        ("JSON que no es objeto", b'"solo una cadena suelta"'),
        ("faltan campos obligatorios", json.dumps(sin_precio).encode()),
        ("precio no numerico", json.dumps(precio_texto).encode()),
        ("ts_evento con formato invalido", json.dumps(fecha_rota).encode()),
    ]


def _metricas():
    cuerpo = json.dumps({
        "size": 20,
        "query": {"term": {"simbolo": SIMBOLO}},
        "sort": [{"ventana_inicio": "desc"}],
        "_source": ["ventana_inicio", "n_trades", "volumen_base", "vwap"],
    }).encode("utf-8")
    peticion = urllib.request.Request(
        f"{ES_URL}/cripto-nrt_metrica-*/_search", data=cuerpo,
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(peticion, timeout=30) as r:
            return [h["_source"] for h in json.loads(r.read())["hits"]["hits"]]
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        raise


def main():
    print("=" * 78)
    print("TOLERANCIA A EVENTOS MALFORMADOS")
    print("=" * 78)
    print()

    malos = _malformados(datetime.now(timezone.utc))
    print(f"Simbolo de prueba : {SIMBOLO}")
    print(f"Trades validos    : {VALIDOS}  (precio {PRECIO}, cantidad {CANTIDAD})")
    print(f"Trades corruptos  : {len(malos)}")
    for descripcion, _ in malos:
        print(f"                    - {descripcion}")
    print()

    try:
        # Sin `value_serializer`: se publican bytes tal cual, porque parte de la
        # prueba es mandar cosas que json.dumps nunca produciria.
        productor = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            key_serializer=lambda k: k.encode("utf-8"),
        )
    except Exception as error:
        print(f"No se pudo conectar a Kafka en {KAFKA_BROKER}: {error}")
        return 1

    previas = {m["ventana_inicio"] for m in _metricas()}
    ahora = datetime.now(timezone.utc)

    # Mezclados: los corruptos entre los validos, no todos al final. Si el job
    # muriera con el primero, los validos posteriores no llegarian y eso se ve.
    for i in range(1, VALIDOS + 1):
        productor.send(TOPIC, key=SIMBOLO,
                       value=json.dumps(_valido(i, ahora)).encode())
        if i % 4 == 0 and malos:
            _, crudo = malos[(i // 4 - 1) % len(malos)]
            productor.send(TOPIC, key=SIMBOLO, value=crudo)
    productor.flush()
    productor.close()

    ventana = ahora.replace(second=0, microsecond=0)
    marca = _marca(ventana)
    print(f"Publicados intercalados. Ventana esperada: {marca}")
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
    print("-" * 78)
    fallos = 0

    if not resultado:
        print(f"  FALLA  la ventana {marca} no aparecio.")
        print("         O el job murio con algun mensaje corrupto, o no llego nada.")
        print("         Revisa: docker compose logs --tail 50 spark-streaming")
        print("-" * 78)
        return 1

    n = resultado["n_trades"]
    volumen = resultado["volumen_base"]
    vwap = resultado["vwap"]
    print(f"  ventana        : {resultado['ventana_inicio']}")
    print(f"  n_trades       : {n}")
    print(f"  volumen_base   : {volumen}")
    print(f"  vwap           : {vwap}")
    print()

    if n == VALIDOS:
        print(f"  OK   n_trades = {VALIDOS}: entraron los validos y solo los validos")
    else:
        print(f"  FALLA  n_trades = {n}, se esperaban {VALIDOS}")
        if n > VALIDOS:
            print("         Algun mensaje corrupto se conto como trade.")
        else:
            print("         Se perdieron trades validos.")
        fallos += 1

    if abs(volumen - VALIDOS * CANTIDAD) < 0.01:
        print(f"  OK   volumen_base = {VALIDOS * CANTIDAD}: sin nulos en la suma")
    else:
        print(f"  FALLA  volumen_base = {volumen}, se esperaba {VALIDOS * CANTIDAD}")
        fallos += 1

    if vwap is not None and abs(vwap - PRECIO) < 0.01:
        print(f"  OK   vwap = {PRECIO}: el corrupto con precio de texto no lo ensucio")
    else:
        print(f"  FALLA  vwap = {vwap}, se esperaba {PRECIO}")
        fallos += 1

    print("-" * 78)
    print()
    if fallos:
        print(f"RESULTADO: {fallos} comprobacion(es) fallaron")
        return 1

    print("RESULTADO: los cinco tipos de mensaje corrupto se descartaron sin ruido")
    print()
    print("Donde ocurre: `parsear()` filtra las filas con nulos en simbolo, precio,")
    print("cantidad o ts_evento. Un JSON que no cumple el esquema no lanza excepcion")
    print("en Spark: deja TODAS las columnas nulas. Sin ese filtro el job no se")
    print("caeria, que suena bien, pero emitiria ventanas con VWAP nulo y nadie se")
    print("enteraria hasta mirar un panel vacio.")
    print()
    print("Diferencia con el camino batch: alli los registros invalidos van a la zona")
    print("de cuarentena CON su motivo, y se pueden revisar. En NRT se descartan, y")
    print("eso es una decision consciente: guardar cada mensaje corrupto de un flujo")
    print("continuo cuesta mas de lo que vale.")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
