"""Prueba de la traduccion exchange -> contrato. NO necesita red ni Kafka.

    python pruebas/prueba_websocket.py

La traduccion es el punto exacto donde el formato del exchange se convierte en el
formato que el resto del pipeline da por bueno. Un fallo aqui no produce ningun
error: produce columnas nulas en Spark, ventanas vacias y una conciliacion que
devuelve cero sin quejarse. Por eso se prueba aparte y sin infraestructura.

Lo que NO cubre: que el exchange siga publicando con este formato. Eso solo se
comprueba conectando de verdad, y para eso esta:

    docker compose exec productor python cliente_websocket.py BTCUSDT 5
"""

import os
import sys

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(RAIZ, "ingesta_streaming"))

import cliente_websocket as ws  # noqa: E402

fallos = []


def comprobar(descripcion, condicion, detalle=""):
    if condicion:
        print("  OK   " + descripcion)
    else:
        print("  FALLA " + descripcion + ("  -> " + detalle if detalle else ""))
        fallos.append(descripcion)


def titulo(texto):
    print()
    print("-" * 70)
    print(texto)
    print("-" * 70)


# ---------------------------------------------------------------------------
# Mensaje real del stream combinado, con el sobre {"stream": ..., "data": ...}.
# Los valores estan copiados de la forma que publica el exchange: precio y
# cantidad como CADENA, tiempos como epoch en milisegundos.
# ---------------------------------------------------------------------------
MENSAJE = {
    "stream": "btcusdt@trade",
    "data": {
        "e": "trade",
        "E": 1789000391500,
        "s": "BTCUSDT",
        "t": 4210987654,
        "p": "78774.65000000",
        "q": "0.00310000",
        "T": 1789000391482,
        "m": False,
        "M": True,
    },
}


titulo("1. Construccion de la URL")

url = ws.construir_url(["BTCUSDT", "ETHUSDT", "SOLUSDT"])

comprobar(
    "el simbolo va en MINUSCULAS en el nombre del canal",
    "btcusdt@trade" in url and "BTCUSDT" not in url,
    "en mayusculas el socket conecta y no llega ningun mensaje: no hay error, "
    "solo silencio, que es el fallo mas caro de diagnosticar",
)
comprobar(
    "los tres canales se combinan separados por /",
    url.endswith("btcusdt@trade/ethusdt@trade/solusdt@trade"),
    url,
)
comprobar(
    "se toleran espacios sobrantes en la lista de simbolos",
    ws.construir_url([" btcusdt ", "ETHUSDT"]).endswith("btcusdt@trade/ethusdt@trade"),
    "CRIPTO_SIMBOLOS se parte por comas y es facil que llegue con espacios",
)

sin_simbolos = False
try:
    ws.construir_url([])
except ValueError:
    sin_simbolos = True
comprobar(
    "una lista vacia de simbolos falla en vez de conectar a nada",
    sin_simbolos,
)


titulo("2. Traduccion de un trade valido")

trade = ws.traducir_trade(MENSAJE, ts_ingesta="2026-09-10T00:33:11.617Z")

esperados = {
    "id_evento",
    "tipo_fuente",
    "simbolo",
    "id_trade",
    "precio",
    "cantidad",
    "importe_usdt",
    "comprador_es_maker",
    "ts_evento",
    "ts_ingesta",
    "origen",
}
comprobar(
    "estan los 11 campos del contrato, ni uno mas ni uno menos",
    set(trade) == esperados,
    "sobran " + str(set(trade) - esperados) + " / faltan " + str(esperados - set(trade)),
)
comprobar("tipo_fuente es nrt_trade", trade["tipo_fuente"] == "nrt_trade", str(trade["tipo_fuente"]))
comprobar("simbolo en MAYUSCULAS", trade["simbolo"] == "BTCUSDT", trade["simbolo"])
comprobar(
    "origen es exchange_ws y no simulador",
    trade["origen"] == "exchange_ws",
    "es lo que permite separar las dos poblaciones y decidir si la "
    "conciliacion es comparable",
)


titulo("3. Tipos: el error que no da error")

comprobar(
    "precio es float, no la cadena que envia el exchange",
    isinstance(trade["precio"], float) and abs(trade["precio"] - 78774.65) < 1e-9,
    repr(trade["precio"]),
)
comprobar(
    "cantidad es float",
    isinstance(trade["cantidad"], float) and abs(trade["cantidad"] - 0.0031) < 1e-9,
    repr(trade["cantidad"]),
)
comprobar(
    "id_trade es entero (long), clave de deduplicacion",
    isinstance(trade["id_trade"], int) and trade["id_trade"] == 4210987654,
    repr(trade["id_trade"]),
)
comprobar(
    "id_trade admite valores mayores que 2^31",
    trade["id_trade"] > 2**31,
    "el esquema de Spark lo declara LongType justamente por esto",
)
comprobar(
    "comprador_es_maker es booleano",
    trade["comprador_es_maker"] is False,
    repr(trade["comprador_es_maker"]),
)
comprobar(
    "importe_usdt = precio * cantidad, redondeado a 2",
    abs(trade["importe_usdt"] - round(78774.65 * 0.0031, 2)) < 1e-9,
    repr(trade["importe_usdt"]),
)


titulo("4. Marcas de tiempo")

comprobar(
    "ts_evento sale de T (hora del trade), no de E (hora del evento)",
    trade["ts_evento"] == "2026-09-10T00:33:11.482Z",
    trade["ts_evento"] + "  (E daria ...11.500Z)",
)
comprobar(
    "ts_evento es ISO 8601 UTC con milisegundos y Z",
    trade["ts_evento"].endswith("Z") and len(trade["ts_evento"]) == 24,
    trade["ts_evento"],
)
comprobar(
    "ts_ingesta se respeta cuando se pasa",
    trade["ts_ingesta"] == "2026-09-10T00:33:11.617Z",
    trade["ts_ingesta"],
)
comprobar(
    "ts_ingesta se genera solo cuando no se pasa",
    ws.traducir_trade(MENSAJE)["ts_ingesta"].endswith("Z"),
)
comprobar(
    "ts_ingesta es posterior a ts_evento (latencia no negativa)",
    trade["ts_ingesta"] > trade["ts_evento"],
    "si saliera al reves, la prueba de latencia P2 mediria numeros negativos",
)


titulo("5. Formas alternativas del payload")

suelto = ws.traducir_trade(MENSAJE["data"], ts_ingesta="2026-09-10T00:33:11.617Z")
comprobar(
    "acepta el evento sin el sobre de streams combinados",
    suelto["simbolo"] == "BTCUSDT" and suelto["id_trade"] == 4210987654,
    "/ws/<canal> entrega el evento suelto; /stream?streams= lo envuelve",
)
comprobar(
    "acepta el mensaje como cadena JSON",
    ws.traducir_trade('{"e":"trade","s":"ETHUSDT","t":9,"p":"2496.28","q":"1.5","T":1789000391482}')[
        "simbolo"
    ]
    == "ETHUSDT",
)
comprobar(
    "comprador_es_maker queda en None si el campo no viene",
    ws.traducir_trade({"e": "trade", "s": "S", "t": 1, "p": "1", "q": "1", "T": 1})[
        "comprador_es_maker"
    ]
    is None,
    "el contrato dice que un vacio es ausencia de dato; False seria un dato",
)


titulo("6. Ruido de control: no es un trade y no es un error")

comprobar(
    "la confirmacion de suscripcion devuelve None",
    ws.traducir_trade({"result": None, "id": 1}) is None,
    "si esto lanzara, el flujo se cortaria en el primer mensaje de la sesion",
)
comprobar(
    "otro tipo de evento del exchange devuelve None",
    ws.traducir_trade({"stream": "btcusdt@depth", "data": {"e": "depthUpdate", "s": "BTCUSDT"}})
    is None,
)


titulo("7. Trades que NO deben entrar al pipeline")


def rechaza(payload):
    try:
        ws.traducir_trade(payload)
        return False
    except ws.TradeInvalido:
        return True


base = dict(MENSAJE["data"])

comprobar(
    "precio 0 se rechaza",
    rechaza(dict(base, p="0")),
    "un precio 0 no da error aritmetico: arrastra el VWAP de la ventana hacia "
    "abajo y la conciliacion lo reporta como desviacion del pipeline",
)
comprobar("precio negativo se rechaza", rechaza(dict(base, p="-1.5")))
comprobar(
    "cantidad 0 se rechaza",
    rechaza(dict(base, q="0")),
    "cantidad 0 en toda una ventana hace volumen_base 0 y el VWAP sale infinito",
)
comprobar("precio no numerico se rechaza", rechaza(dict(base, p="ochenta mil")))
comprobar("id_trade no entero se rechaza", rechaza(dict(base, t="abc")))

for campo in ("s", "t", "p", "q", "T"):
    incompleto = dict(base)
    del incompleto[campo]
    comprobar("falta '" + campo + "' -> se rechaza", rechaza(incompleto))

comprobar(
    "un payload que no es objeto se rechaza",
    rechaza([1, 2, 3]),
)


titulo("8. Contrato compartido con el simulador")

sys.path.insert(0, os.path.join(RAIZ, "ingesta_streaming"))
import simulador_trades  # noqa: E402

simulado = simulador_trades.generar_trade()

comprobar(
    "ambas fuentes producen exactamente los mismos campos",
    set(simulado) == set(trade),
    "sobran " + str(set(simulado) - set(trade)) + " / faltan " + str(set(trade) - set(simulado)),
)
comprobar(
    "el origen es lo unico que distingue una fuente de la otra",
    simulado["origen"] == "simulador" and trade["origen"] == "exchange_ws",
    simulado["origen"] + " / " + trade["origen"],
)
comprobar(
    "los tipos coinciden campo a campo entre las dos fuentes",
    all(
        type(simulado[c]) is type(trade[c])
        for c in simulado
        if simulado[c] is not None and trade[c] is not None
    ),
    str([c for c in simulado if type(simulado[c]) is not type(trade[c])]),
)


# ---------------------------------------------------------------------------
print()
print("=" * 70)
if fallos:
    print("RESULTADO: " + str(len(fallos)) + " comprobacion(es) fallida(s)")
    for descripcion in fallos:
        print("  - " + descripcion)
    print("=" * 70)
    sys.exit(1)

print("RESULTADO: todas las comprobaciones pasan")
print("=" * 70)
