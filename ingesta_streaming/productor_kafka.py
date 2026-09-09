"""Publica trades en Kafka desde una de las dos fuentes del contrato.

Fuentes disponibles (`CRIPTO_FUENTE_TRADES`):

  websocket  F1 del contrato. Trades reales del exchange. `origen=exchange_ws`.
             Es el modo por defecto y el unico con el que la conciliacion del
             DAG 05 significa algo, porque hace que batch y streaming lean el
             mismo mercado.

  simulador  F2 del contrato. Trades generados localmente. `origen=simulador`.
             Sirve para trabajar sin red y para las pruebas de carga y de
             deduplicacion, donde hace falta controlar la tasa y los ids.

Este archivo NO decide como se traduce un trade ni como se genera uno: eso vive
en cliente_websocket.py y simulador_trades.py. Aqui solo se elige la fuente, se
publica y se reporta.

Sobre la caida al simulador
---------------------------
Si el modo es `websocket` y el exchange no responde, el productor puede caer al
simulador (`CRIPTO_RESPALDO_SIMULADOR`). Esa caida es RUIDOSA a proposito: se
avisa por consola y se publica un evento `ops_control`. Ademas cada trade lleva
su `origen`, asi que un analisis posterior siempre puede separar las dos
poblaciones. Una caida silenciosa seria peor que el fallo: la conciliacion
volveria a comparar constantes inventadas contra el mercado real y nadie sabria
por que.
"""

import json
import os
import time

from kafka import KafkaProducer

import observabilidad
from cliente_websocket import ErrorWebSocket, abrir_flujo
from simulador_trades import generar_trade

# Por variable de entorno, con el valor de fuera del contenedor por defecto.
#
# Kafka anuncia DOS listeners y hay que usar el correcto segun donde se ejecute
# esto: `kafka:29092` desde dentro de la red de Docker, `localhost:9095` desde
# Windows. Con el valor fijo solo funcionaba uno de los dos casos.
KAFKA_BROKER = os.environ.get("CRIPTO_KAFKA", "localhost:9095")
TOPIC = os.environ.get("CRIPTO_TOPIC_TRADES", "trades.crudo")

# Cada cuantos mensajes se fuerza el envio. `flush()` en cada mensaje obliga a
# esperar la confirmacion del broker uno a uno, y con eso la prueba de carga a
# 2000 eventos por segundo no pasa de unas decenas. El productor ya agrupa por
# su cuenta; el flush periodico solo garantiza que nada se quede en el bufer
# durante mucho tiempo cuando la tasa es baja.
FLUSH_CADA = int(os.environ.get("CRIPTO_FLUSH_CADA", "50"))

# Pausa entre mensajes. SOLO aplica al simulador: en el flujo real el ritmo lo
# marca el exchange y dormir entre mensajes solo serviria para acumular retraso
# y falsear la latencia que mide la prueba P2.
PAUSA = float(os.environ.get("CRIPTO_PAUSA", "0.1"))

FUENTE = os.environ.get("CRIPTO_FUENTE_TRADES", "websocket").strip().lower()
SIMBOLOS = [s for s in os.environ.get("CRIPTO_SIMBOLOS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",") if s.strip()]

# Reconexiones consecutivas al exchange antes de rendirse. Con la espera
# creciente del cliente (1, 2, 4, 8...) tres intentos son unos 7 segundos: lo
# justo para distinguir un corte pasajero de un exchange inalcanzable.
WS_RECONEXIONES = int(os.environ.get("CRIPTO_WS_RECONEXIONES", "3"))

RESPALDO_SIMULADOR = os.environ.get("CRIPTO_RESPALDO_SIMULADOR", "true").strip().lower() in (
    "1",
    "true",
    "si",
    "yes",
)


def crear_productor():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )


def flujo_simulado(pausa=PAUSA):
    """Adapta el simulador a la misma interfaz que `abrir_flujo`: un generador.

    Que las dos fuentes se consuman igual es lo que permite que el bucle de
    publicacion no sepa cual esta usando.
    """
    while True:
        yield generar_trade()
        if pausa > 0:
            time.sleep(pausa)


def elegir_fuente():
    """Devuelve (generador, etiqueta) segun CRIPTO_FUENTE_TRADES.

    En modo `websocket` intenta el exchange y, si no responde y el respaldo esta
    habilitado, devuelve el simulador dejando constancia del cambio.
    """
    if FUENTE == "simulador":
        print("Fuente: SIMULADOR (F2) por configuracion explicita", flush=True)
        return flujo_simulado(), "simulador"

    if FUENTE != "websocket":
        raise SystemExit(
            "CRIPTO_FUENTE_TRADES='{}' no es valido; use 'websocket' o 'simulador'".format(FUENTE)
        )

    def avisar_corte(intento, motivo):
        observabilidad.reportar(
            {"hito": "websocket_reconexion", "intento": intento, "motivo": motivo}
        )

    flujo = abrir_flujo(SIMBOLOS, reconexiones_maximas=WS_RECONEXIONES, al_reconectar=avisar_corte)

    # Se pide el primer trade AQUI, no en el bucle de publicacion. Un generador
    # no ejecuta nada hasta la primera peticion, asi que sin esto un exchange
    # inalcanzable no se detectaria al arrancar sino a mitad del bucle, cuando ya
    # no se puede cambiar de fuente limpiamente.
    try:
        primero = next(flujo)
    except (ErrorWebSocket, StopIteration) as error:
        print("No se pudo abrir el flujo del exchange: {}".format(error), flush=True)
        observabilidad.reportar(
            {"hito": "websocket_inalcanzable", "motivo": str(error), "respaldo": RESPALDO_SIMULADOR}
        )
        if not RESPALDO_SIMULADOR:
            raise SystemExit(
                "exchange inalcanzable y CRIPTO_RESPALDO_SIMULADOR=false; se detiene la ingesta"
            ) from error
        print(
            "AVISO: cayendo al SIMULADOR. Los trades llevaran origen='simulador' "
            "y la conciliacion del DAG 05 no sera comparable con el mercado real.",
            flush=True,
        )
        return flujo_simulado(), "simulador"

    print("Fuente: EXCHANGE por WebSocket (F1) | simbolos: {}".format(",".join(SIMBOLOS)), flush=True)

    def flujo_con_primero():
        yield primero
        for trade in flujo:
            yield trade

    return flujo_con_primero(), "exchange_ws"


def publicar(productor, fuente):
    enviados = 0
    for evento in fuente:
        simbolo = evento["simbolo"]

        # El simbolo como clave de particion: garantiza que todos los trades de
        # un activo caigan en la misma particion y conserven el orden.
        productor.send(TOPIC, key=simbolo, value=evento)
        enviados += 1

        if enviados % FLUSH_CADA == 0:
            productor.flush()
            # Una linea cada N mensajes, no una por mensaje: a tasas altas,
            # imprimir por evento es lo que acaba limitando el rendimiento.
            print(
                "-> {} trades enviados | ultimo: {} @ {}".format(enviados, simbolo, evento["precio"]),
                flush=True,
            )
    return enviados


if __name__ == "__main__":
    print("Conectando a Kafka en {}...".format(KAFKA_BROKER), flush=True)
    productor = None
    try:
        productor = crear_productor()
        print("Conexion con Kafka establecida. Topic '{}'".format(TOPIC), flush=True)

        fuente, etiqueta = elegir_fuente()
        observabilidad.reportar(
            {"hito": "ingesta_iniciada", "fuente": etiqueta, "simbolos": SIMBOLOS, "topic": TOPIC}
        )
        publicar(productor, fuente)

    except KeyboardInterrupt:
        print("\nProductor detenido manualmente.", flush=True)
    except Exception as error:  # noqa: BLE001 - se quiere el tipo en el log
        print("Error del productor: {}: {}".format(type(error).__name__, error), flush=True)
        observabilidad.reportar({"hito": "ingesta_fallida", "motivo": str(error)})
        raise
    finally:
        if productor is not None:
            productor.flush()
            productor.close()
