"""Fuente F1 del contrato: trades reales del exchange por WebSocket.

Este modulo tiene DOS partes deliberadamente separadas:

  1. `traducir_trade()` y sus auxiliares: funciones PURAS que convierten el
     payload del exchange al evento del contrato. No abren sockets y se pueden
     probar sin red ni dependencias externas (pruebas/prueba_websocket.py).

  2. `abrir_flujo()`: el generador que mantiene la conexion viva. Es la unica
     parte que necesita `websocket-client` instalado, y por eso la importacion
     esta DENTRO de la funcion: asi el modulo se puede importar y probar la
     traduccion desde cualquier entorno, incluido uno sin la libreria.

Por que este flujo y no el simulador
------------------------------------
La conciliacion del DAG 05 compara el VWAP que calcula el streaming contra el
cierre de la vela horaria que descarga el batch. Con el simulador esa
comparacion no vale nada: el simulador genera precios a partir de constantes
escritas a mano, asi que la desviacion mide la distancia entre esas constantes y
el mercado, no la exactitud del pipeline. Medido el 9/9/2026: -20 %, +36 % y
+40 % de desviacion, con cobertura del 8 %, 10 % y 33 %.

Con este flujo ambos lados leen el MISMO mercado, y entonces la desviacion mide
lo que debe medir: si la ventana de Spark reproduce el precio real.

Limitacion declarada
--------------------
Depende de que el exchange sea alcanzable en el momento de la demo. No hay forma
de eliminar esa dependencia sin volver a datos inventados. Lo que si se hace es
que el fallo sea VISIBLE y no silencioso: si se cae al simulador, el campo
`origen` del evento pasa a valer `simulador` y la conciliacion puede excluirlo.
"""

import json
import os
import uuid
from datetime import datetime, timezone

# Endpoint publico de streams combinados. En variable de entorno por el mismo
# motivo que API_BASE en config.py: poder apuntar a un espejo o a un servidor de
# pruebas sin tocar codigo.
WS_BASE = os.environ.get("CRIPTO_WS_BASE", "wss://stream.binance.com:9443")

# Valor del campo `origen` para este flujo. Lo fija el contrato, seccion 3.
ORIGEN = "exchange_ws"

# Segundos sin recibir NADA tras los cuales se considera muerta la conexion y se
# reconecta. En BTCUSDT llegan decenas de trades por segundo, asi que un minuto
# de silencio no es un mercado tranquilo: es un socket colgado.
TIEMPO_LIMITE_LECTURA = 60

# Reconexion con espera creciente. El exchange cierra la conexion a las 24 h por
# diseno, asi que reconectar no es el camino de error: es el normal.
ESPERA_RECONEXION_INICIAL = 1
ESPERA_RECONEXION_MAXIMA = 30


class ErrorWebSocket(RuntimeError):
    """No se pudo establecer ni mantener la conexion con el exchange.

    Tipo propio y no RuntimeError a secas para que el productor pueda distinguir
    "el exchange no responde" (recuperable, cae al simulador) de un error de
    programacion, que debe romper ruidosamente.
    """


class TradeInvalido(ValueError):
    """El payload llego pero no tiene la forma de un trade del contrato."""


def construir_url(simbolos, base=None):
    """Arma la URL de streams combinados para la lista de simbolos.

    El exchange exige el simbolo en MINUSCULAS en el nombre del stream, mientras
    que el contrato exige MAYUSCULAS en el campo `simbolo`. Las dos convenciones
    conviven, y confundirlas devuelve un stream vacio sin error: el socket
    conecta y no llega nada.
    """
    if not simbolos:
        raise ValueError("construir_url necesita al menos un simbolo")
    canales = "/".join("{}@trade".format(s.strip().lower()) for s in simbolos)
    return "{}/stream?streams={}".format((base or WS_BASE).rstrip("/"), canales)


def _a_iso(epoch_ms):
    """Convierte epoch en milisegundos a ISO 8601 UTC con milisegundos.

    El exchange publica enteros epoch; el contrato exige cadena ISO con `Z`.
    """
    momento = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
    return momento.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _ahora_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def traducir_trade(payload, ts_ingesta=None):
    """Traduce un mensaje del exchange al evento de trade del contrato.

    Funcion pura salvo por `ts_ingesta`, que por definicion es la hora de
    recepcion; se acepta como parametro para poder fijarla en las pruebas.

    Acepta tanto el sobre de streams combinados
    `{"stream": "...", "data": {...}}` como el evento suelto, porque las dos
    formas existen segun se conecte a `/stream?streams=` o a `/ws/<canal>`.

    Devuelve None para los mensajes que NO son trades: confirmaciones de
    suscripcion (`{"result": null, "id": 1}`) y demas ruido de control. Devolver
    None y no lanzar es deliberado: esos mensajes son normales, no errores.

    Lanza TradeInvalido si dice ser un trade pero le faltan campos o los tiene
    con valores imposibles. Un trade con precio 0 o cantidad negativa no debe
    entrar al pipeline y contaminar el VWAP.
    """
    if isinstance(payload, (str, bytes)):
        payload = json.loads(payload)

    if not isinstance(payload, dict):
        raise TradeInvalido("se esperaba un objeto JSON, llego " + type(payload).__name__)

    # Sobre de streams combinados: el evento real viene dentro de `data`.
    datos = payload.get("data", payload)
    if not isinstance(datos, dict):
        raise TradeInvalido("el campo 'data' no es un objeto")

    # Ruido de control: no es un trade y no es un error.
    if datos.get("e") != "trade":
        return None

    faltantes = [c for c in ("s", "t", "p", "q", "T") if c not in datos]
    if faltantes:
        raise TradeInvalido("faltan campos del trade: " + ", ".join(faltantes))

    try:
        # Precio y cantidad llegan como CADENA, no como numero. El exchange lo
        # hace a proposito para no perder precision al serializar. Convertirlos
        # es obligatorio: sin esto Spark recibe texto donde espera double y la
        # columna sale nula sin ningun error visible.
        precio = float(datos["p"])
        cantidad = float(datos["q"])
        id_trade = int(datos["t"])
        ts_evento_ms = int(datos["T"])
    except (TypeError, ValueError) as error:
        raise TradeInvalido("campo numerico ilegible: {}".format(error)) from error

    if precio <= 0:
        raise TradeInvalido("precio no positivo: {}".format(precio))
    if cantidad <= 0:
        raise TradeInvalido("cantidad no positiva: {}".format(cantidad))

    return {
        "id_evento": str(uuid.uuid4()),
        "tipo_fuente": "nrt_trade",
        "simbolo": str(datos["s"]).upper(),
        "id_trade": id_trade,
        "precio": precio,
        "cantidad": cantidad,
        "importe_usdt": round(precio * cantidad, 2),
        # `m` = el comprador es el maker. Ausente en algunas variantes del
        # stream; se deja None en vez de inventar False, porque el contrato dice
        # que un vacio es ausencia de dato y False es un dato.
        "comprador_es_maker": datos.get("m"),
        "ts_evento": _a_iso(ts_evento_ms),
        "ts_ingesta": ts_ingesta or _ahora_iso(),
        "origen": ORIGEN,
    }


def abrir_flujo(simbolos, reconexiones_maximas=None, al_reconectar=None):
    """Generador infinito de trades del exchange, ya traducidos al contrato.

    Mantiene la conexion viva y reconecta con espera creciente. Cada elemento
    que devuelve es un dict listo para publicar en Kafka.

    `reconexiones_maximas` a None significa reconectar indefinidamente, que es lo
    que se quiere mientras el pipeline esta en marcha. Un entero limita los
    intentos y hace que se lance ErrorWebSocket al agotarlos; asi el productor
    puede caer al simulador en lugar de quedarse reintentando para siempre
    contra un exchange inalcanzable.

    `al_reconectar` es una funcion opcional que se llama con (intento, motivo).
    Existe para que el productor pueda registrar el corte como evento de
    observabilidad sin que este modulo tenga que saber que existe Logstash.
    """
    # Importaciones diferidas: ver la cabecera del modulo. Permiten probar
    # traducir_trade() sin tener websocket-client instalado.
    import time

    try:
        import websocket
    except ImportError as error:
        raise ErrorWebSocket(
            "falta la dependencia 'websocket-client'; se instala en Dockerfile.productor"
        ) from error

    url = construir_url(simbolos)
    intentos = 0
    espera = ESPERA_RECONEXION_INICIAL

    while True:
        conexion = None
        try:
            conexion = websocket.create_connection(url, timeout=TIEMPO_LIMITE_LECTURA)
            # Conexion buena: se reinicia la cuenta para que un corte dentro de
            # seis horas no herede la espera de un corte de hace seis horas.
            intentos = 0
            espera = ESPERA_RECONEXION_INICIAL

            while True:
                crudo = conexion.recv()
                if not crudo:
                    # Cadena vacia = el otro extremo cerro. Sale al bucle
                    # exterior a reconectar.
                    raise websocket.WebSocketConnectionClosedException("cierre del exchange")
                try:
                    evento = traducir_trade(crudo)
                except (TradeInvalido, json.JSONDecodeError) as error:
                    # Un mensaje malo no tumba el flujo: se descarta y se sigue.
                    # Tumbar la ingesta entera por un evento corrupto entre miles
                    # es peor que perder ese evento.
                    print("[ws] descartado: {}".format(error), flush=True)
                    continue
                if evento is not None:
                    yield evento

        except GeneratorExit:
            # El consumidor cerro el generador. No es un fallo de red y no debe
            # disparar una reconexion.
            raise
        except Exception as error:  # noqa: BLE001 - cualquier fallo de red reconecta
            intentos += 1
            motivo = "{}: {}".format(type(error).__name__, error)
            if al_reconectar is not None:
                al_reconectar(intentos, motivo)
            if reconexiones_maximas is not None and intentos > reconexiones_maximas:
                raise ErrorWebSocket(
                    "{} reconexiones fallidas contra {}; ultimo motivo: {}".format(
                        intentos - 1, url, motivo
                    )
                ) from error
            print("[ws] corte ({}); reintento {} en {}s".format(motivo, intentos, espera), flush=True)
            time.sleep(espera)
            espera = min(espera * 2, ESPERA_RECONEXION_MAXIMA)
        finally:
            if conexion is not None:
                try:
                    conexion.close()
                except Exception:  # noqa: BLE001 - cerrar nunca debe romper nada
                    pass


if __name__ == "__main__":
    # Igual que el simulador: se puede ejecutar suelto para ver el esquema a ojo
    # antes de meter Kafka en la ecuacion.
    #   python cliente_websocket.py BTCUSDT,ETHUSDT 10
    import sys

    argumentos = sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT,ETHUSDT,SOLUSDT"
    limite = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    lista = argumentos.split(",")
    print("Conectando a " + construir_url(lista), flush=True)
    vistos = 0
    for trade in abrir_flujo(lista, reconexiones_maximas=2):
        print(json.dumps(trade))
        vistos += 1
        if vistos >= limite:
            break
