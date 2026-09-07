"""Descarga de velas OHLCV desde la API REST publica del exchange (fuente F3).

Encapsula tres cosas que no deben estar en el DAG: la forma de la respuesta de
la API, la paginacion y la politica de reintentos.

MODO SIN RED. `descargar_klines` cae a `generar_klines_sinteticas` cuando la API
no responde. No es un adorno: la regla del proyecto es que el pipeline funcione
de punta a punta sin internet, porque de eso depende que la demostracion no
dependa de la red del aula. Las velas sinteticas se marcan con origen
'sintetico' para que nunca se confundan con datos reales, ni en el reporte ni en
los paneles.
"""

import math
import random
import time

import pandas as pd
import requests

from comun import config, utilidades

# ---------------------------------------------------------------------------
# FORMA DE LA RESPUESTA
# ---------------------------------------------------------------------------
# La API devuelve cada vela como una lista posicional, no como un objeto. Este
# mapa es el unico lugar donde se traduce posicion -> nombre. Si el proveedor
# cambia el orden, se corrige aqui y no en cinco sitios distintos.
POSICIONES = {
    "apertura_ms": 0,
    "apertura": 1,
    "maximo": 2,
    "minimo": 3,
    "cierre": 4,
    "volumen_base": 5,
    "cierre_ms": 6,
    "volumen_usdt": 7,
    "n_trades": 8,
}

COLUMNAS_BRONCE = [
    "simbolo",
    "fecha",
    "apertura_ms",
    "apertura",
    "maximo",
    "minimo",
    "cierre",
    "volumen_base",
    "volumen_usdt",
    "n_trades",
    "origen",
    "descargado_en",
]

MILISEGUNDOS_POR_DIA = 86_400_000
MILISEGUNDOS_POR_HORA = 3_600_000


# ---------------------------------------------------------------------------
# PETICION CON REINTENTOS
# ---------------------------------------------------------------------------
def _pedir(parametros):
    """Una peticion a /klines con reintentos y espera creciente.

    La espera se duplica en cada intento (2 s, 4 s, 8 s). Reintentar de
    inmediato contra un servidor que devuelve 429 solo consigue que el bloqueo
    dure mas.

    Devuelve la lista de velas, o lanza la ultima excepcion si se agotan los
    intentos. Quien llama decide si eso significa fallar o usar el respaldo.
    """
    url = config.API_BASE + config.API_RUTA_KLINES
    espera = config.API_ESPERA_INICIAL
    ultimo_error = None

    for intento in range(1, config.API_REINTENTOS + 1):
        try:
            respuesta = requests.get(url, params=parametros,
                                     timeout=config.API_TIEMPO_LIMITE)

            # 429 y 418 son limite de tasa; 5xx es problema del servidor. Los
            # tres se reintentan. Un 400 se debe a parametros mal formados y
            # reintentarlo daria el mismo resultado, asi que se propaga.
            if respuesta.status_code in (429, 418) or respuesta.status_code >= 500:
                raise requests.HTTPError(
                    "Codigo " + str(respuesta.status_code) + " del servidor"
                )

            respuesta.raise_for_status()
            return respuesta.json()

        except (requests.RequestException, ValueError) as error:
            ultimo_error = error
            print("Intento " + str(intento) + "/" + str(config.API_REINTENTOS) +
                  " fallido: " + str(error))
            if intento < config.API_REINTENTOS:
                time.sleep(espera)
                espera *= 2

    raise ultimo_error


# ---------------------------------------------------------------------------
# DESCARGA PAGINADA
# ---------------------------------------------------------------------------
def descargar_klines(simbolo, intervalo=None, dias=None, permitir_respaldo=True,
                     incluir_vela_en_curso=False):
    """Descarga la serie de velas de un simbolo. Devuelve un DataFrame.

    Pagina hacia adelante desde `inicio`: la API entrega como maximo 1000 velas
    por peticion, y con 365 dias diarios cabe en una, pero con intervalo horario
    son 8760 y hacen falta nueve. Se pagina siempre, aunque en el caso diario
    baste una vuelta, porque un camino que solo se ejecuta a veces es un camino
    que nadie prueba.

    LA ULTIMA VELA SE DESCARTA POR DEFECTO. La API devuelve tambien el periodo
    en curso, que todavia no ha cerrado. Esa vela parece valida -- tiene OHLC
    coherente, volumen positivo y pasa todas las reglas de calidad -- pero sus
    valores son parciales: su cierre es el precio de este instante, no el del
    fin del dia, y su volumen es una fraccion del real.

    Cargarla arruina dos cosas a la vez. La serie queda con un ultimo punto que
    cambia cada vez que se ejecuta el DAG, asi que dos corridas del mismo dia
    producen resultados distintos y la carga deja de ser idempotente en la
    practica. Y como las medias moviles son acumulativas, ese valor parcial
    contamina los 30 dias siguientes de sma_30 y volatilidad.

    Se detecta comparando el cierre teorico de la vela contra el momento actual;
    ninguna regla de calidad puede detectarlo mirando la fila.
    """
    simbolo = simbolo.strip().upper()
    intervalo = intervalo or config.INTERVALO_DIARIO
    dias = dias or config.DIAS_HISTORIA

    paso = MILISEGUNDOS_POR_HORA if intervalo.endswith("h") else MILISEGUNDOS_POR_DIA
    fin = int(utilidades.ahora_utc().timestamp() * 1000)
    inicio = fin - dias * MILISEGUNDOS_POR_DIA

    print("Descargando " + simbolo + " intervalo=" + intervalo + " dias=" + str(dias))

    crudas = []
    cursor = inicio
    vuelta = 0

    try:
        while cursor < fin:
            vuelta += 1
            lote = _pedir({
                "symbol": simbolo,
                "interval": intervalo,
                "startTime": cursor,
                "limit": config.MAXIMO_VELAS_POR_PETICION,
            })

            if not lote:
                break

            crudas.extend(lote)

            # El cursor avanza al cierre de la ultima vela recibida, no a
            # cursor + limite * paso. Si el exchange devuelve menos velas de las
            # pedidas (un periodo sin datos), la segunda formula saltaria un
            # hueco y perderia velas sin que nadie lo note.
            ultimo_apertura_ms = int(lote[-1][POSICIONES["apertura_ms"]])
            siguiente = ultimo_apertura_ms + paso
            if siguiente <= cursor:
                break  # proteccion contra bucle infinito si la API repite la pagina
            cursor = siguiente

            if len(lote) < config.MAXIMO_VELAS_POR_PETICION:
                break

        print("  " + str(len(crudas)) + " velas en " + str(vuelta) + " peticiones")
        return _a_marco(simbolo, crudas, origen="exchange_rest",
                        incluir_vela_en_curso=incluir_vela_en_curso)

    except Exception as error:
        if not permitir_respaldo:
            raise
        print("La API no respondio (" + str(error) + ").")
        print("Se usan velas sinteticas de respaldo para " + simbolo + ".")
        return generar_klines_sinteticas(simbolo, intervalo=intervalo, dias=dias)


def _a_marco(simbolo, crudas, origen, incluir_vela_en_curso=False):
    """Traduce la respuesta posicional de la API a un DataFrame con nombres.

    Descarta la vela aun abierta salvo que se pida lo contrario. Ver la nota en
    descargar_klines sobre por que importa.
    """
    descargado_en = utilidades.a_iso(utilidades.ahora_utc())
    ahora_ms = int(utilidades.ahora_utc().timestamp() * 1000)
    filas = []
    descartadas = 0

    for vela in crudas:
        apertura_ms = int(vela[POSICIONES["apertura_ms"]])
        cierre_ms = int(vela[POSICIONES["cierre_ms"]])

        if not incluir_vela_en_curso and cierre_ms > ahora_ms:
            descartadas += 1
            continue

        filas.append({
            "simbolo": simbolo,
            # La fecha se deriva de la marca de apertura, en UTC. Derivarla en
            # hora local haria que la vela de las 19:00 de un dia apareciera con
            # la fecha del siguiente, y la conciliacion compararia dias
            # distintos.
            "fecha": utilidades.desde_milisegundos(apertura_ms).date().isoformat(),
            "apertura_ms": apertura_ms,
            "apertura": float(vela[POSICIONES["apertura"]]),
            "maximo": float(vela[POSICIONES["maximo"]]),
            "minimo": float(vela[POSICIONES["minimo"]]),
            "cierre": float(vela[POSICIONES["cierre"]]),
            "volumen_base": float(vela[POSICIONES["volumen_base"]]),
            "volumen_usdt": float(vela[POSICIONES["volumen_usdt"]]),
            "n_trades": int(vela[POSICIONES["n_trades"]]),
            "origen": origen,
            "descargado_en": descargado_en,
        })

    if descartadas:
        print("  " + str(descartadas) + " vela(s) aun abierta(s) descartada(s) para " + simbolo)

    return pd.DataFrame(filas, columns=COLUMNAS_BRONCE)


# ---------------------------------------------------------------------------
# RESPALDO SIN RED
# ---------------------------------------------------------------------------
# Precio inicial aproximado por simbolo, solo para que las series sinteticas
# tengan ordenes de magnitud distintos y los paneles no salgan todos iguales.
# No son cotizaciones: son puntos de partida arbitrarios.
PRECIOS_INICIALES = {
    "BTCUSDT": 60000.0,
    "ETHUSDT": 2400.0,
    "SOLUSDT": 140.0,
}


def generar_klines_sinteticas(simbolo, intervalo=None, dias=None, semilla=None,
                              tasa_defectos=0.0):
    """Genera una serie de velas plausible mediante una caminata aleatoria.

    Se usa cuando no hay red, y tambien en las pruebas: con `semilla` fija, dos
    ejecuciones producen exactamente la misma serie. Un fallo intermitente sobre
    datos irreproducibles no se puede diagnosticar.

    `tasa_defectos` inyecta velas invalidas a proposito. Es lo que permite
    demostrar que la cuarentena y la bifurcacion del DAG 02 funcionan: sin
    defectos, esas ramas nunca se ejecutan y no se pueden mostrar.
    """
    simbolo = simbolo.strip().upper()
    intervalo = intervalo or config.INTERVALO_DIARIO
    dias = dias or config.DIAS_HISTORIA

    # Semilla derivada del simbolo: cada activo tiene su propia serie, pero la
    # misma en cada ejecucion.
    aleatorio = random.Random(semilla if semilla is not None else hash(simbolo) % 10**8)

    paso_ms = MILISEGUNDOS_POR_HORA if intervalo.endswith("h") else MILISEGUNDOS_POR_DIA
    periodos = dias * 24 if intervalo.endswith("h") else dias

    fin = int(utilidades.ahora_utc().timestamp() * 1000)
    inicio = fin - periodos * paso_ms

    precio = PRECIOS_INICIALES.get(simbolo, 100.0)
    descargado_en = utilidades.a_iso(utilidades.ahora_utc())
    filas = []

    for indice in range(periodos):
        apertura_ms = inicio + indice * paso_ms

        # Caminata aleatoria multiplicativa: el precio varia un porcentaje, no
        # una cantidad fija. Un movimiento de 500 USD es enorme para SOL y
        # despreciable para BTC; en porcentaje ambos son comparables.
        deriva = aleatorio.gauss(0, 0.02)
        apertura = precio
        cierre = max(apertura * math.exp(deriva), 0.00000001)
        maximo = max(apertura, cierre) * (1 + abs(aleatorio.gauss(0, 0.008)))
        minimo = min(apertura, cierre) * (1 - abs(aleatorio.gauss(0, 0.008)))

        volumen_base = abs(aleatorio.gauss(1000, 300))
        volumen_usdt = volumen_base * (maximo + minimo) / 2
        n_trades = int(abs(aleatorio.gauss(50000, 15000)))

        fila = {
            "simbolo": simbolo,
            "fecha": utilidades.desde_milisegundos(apertura_ms).date().isoformat(),
            "apertura_ms": apertura_ms,
            "apertura": round(apertura, 8),
            "maximo": round(maximo, 8),
            "minimo": round(minimo, 8),
            "cierre": round(cierre, 8),
            "volumen_base": round(volumen_base, 8),
            "volumen_usdt": round(volumen_usdt, 8),
            "n_trades": n_trades,
            "origen": "sintetico",
            "descargado_en": descargado_en,
        }

        if tasa_defectos > 0 and aleatorio.random() < tasa_defectos:
            fila = _inyectar_defecto(fila, aleatorio)

        filas.append(fila)
        precio = cierre

    return pd.DataFrame(filas, columns=COLUMNAS_BRONCE)


# Cada defecto esta pensado para que lo detecte UNA regla concreta. La prueba de
# logica comprueba justamente esa correspondencia: si un defecto deja de ser
# detectado, la regla que lo cubria se rompio.
def _inyectar_defecto(fila, aleatorio):
    """Corrompe una vela de una de cinco formas conocidas."""
    fila = dict(fila)
    defecto = aleatorio.choice([
        "campo_vacio",        # -> R01
        "ohlc_incoherente",   # -> R02
        "precio_negativo",    # -> R03
        "volumen_negativo",   # -> R04
        "volumen_desfasado",  # -> R07
    ])

    if defecto == "campo_vacio":
        fila["cierre"] = None
    elif defecto == "ohlc_incoherente":
        fila["maximo"], fila["minimo"] = fila["minimo"], fila["maximo"]
    elif defecto == "precio_negativo":
        fila["apertura"] = -abs(fila["apertura"])
    elif defecto == "volumen_negativo":
        fila["volumen_base"] = -abs(fila["volumen_base"])
    elif defecto == "volumen_desfasado":
        fila["volumen_usdt"] = fila["volumen_usdt"] * 1000

    fila["origen"] = "sintetico_defectuoso"
    return fila
