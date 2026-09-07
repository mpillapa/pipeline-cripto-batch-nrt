"""Normalizacion e indicadores de la zona plata (DAG 03).

Igual que reglas_calidad.py: la logica vive fuera del DAG para poder leerla y
probarla sin levantar Airflow.

Dos niveles distintos, y la diferencia importa:

  - `normalizar_vela` es una funcion pura por fila: no necesita ver el resto del
    lote. Se prueba con un diccionario.
  - `calcular_indicadores` opera sobre la serie completa de un simbolo, porque
    una media movil de 30 dias no se puede calcular mirando un solo dia. Recibe
    un DataFrame ya ordenado.

SUPUESTOS: las ventanas (7 y 30 dias) y los cortes de la clasificacion de
volatilidad estan en config.py y son valores elegidos para este trabajo. No son
un estandar del sector. Estan fuera del DAG precisamente para que cambiarlos sea
trivial y evidente.
"""

import numpy as np
import pandas as pd

from comun import config

# ---------------------------------------------------------------------------
# T01 - NORMALIZACION
# ---------------------------------------------------------------------------
def normalizar_vela(fila):
    """Deja una vela en tipos y formatos unicos y comparables.

    Sin esto, ' btcusdt ' y 'BTCUSDT' se agrupan como dos activos distintos en
    cualquier conteo posterior, y la union con la dimension falla en silencio
    dejando el hecho sin activo.
    """
    simbolo = str(fila["simbolo"]).strip().upper()
    fecha = pd.to_datetime(fila["fecha"], utc=True).date()

    return {
        "simbolo": simbolo,
        # El id_activo se deriva del simbolo, no se inventa: asi el DAG 04 puede
        # unir hechos y dimension sin un catalogo intermedio en memoria.
        "id_activo": "ACT-" + simbolo,
        "fecha": fecha.isoformat(),
        "apertura": round(float(fila["apertura"]), 8),
        "maximo": round(float(fila["maximo"]), 8),
        "minimo": round(float(fila["minimo"]), 8),
        "cierre": round(float(fila["cierre"]), 8),
        "volumen_base": round(float(fila["volumen_base"]), 8),
        "volumen_usdt": round(float(fila["volumen_usdt"]), 8),
        "n_trades": int(float(fila["n_trades"])),
    }


def normalizar_lote(filas):
    """Aplica normalizar_vela a todo el lote y devuelve un DataFrame ordenado.

    El orden por (simbolo, fecha) NO es cosmetico: todos los indicadores que
    vienen despues son acumulativos sobre la serie. Con las filas desordenadas
    la media movil mezcla dias sin ningun aviso, y el resultado parece razonable
    aunque sea falso. Se ordena aqui, una sola vez, y las funciones siguientes
    dan ese orden por hecho.
    """
    normalizadas = [normalizar_vela(fila) for fila in filas]
    marco = pd.DataFrame(normalizadas)
    if marco.empty:
        return marco
    return marco.sort_values(["simbolo", "fecha"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# T02 - INDICADORES
# ---------------------------------------------------------------------------
def calcular_indicadores(marco):
    """Agrega retorno, medias moviles y volatilidad a la serie de cada simbolo.

    Formulas (ver docs/REGLAS_NEGOCIO.md):
      retorno_pct     = (cierre - cierre_anterior) / cierre_anterior * 100
      sma_7 / sma_30  = media movil simple del cierre
      volatilidad_30d = desviacion estandar muestral de retorno_pct, 30 dias

    Todos se calculan POR SIMBOLO con groupby. Calcularlos sobre el marco
    completo mezclaria el ultimo dia de BTC con el primero de ETH y produciria
    un retorno absurdo justo en el limite entre ambas series.

    Los primeros dias de cada serie quedan en NaN a proposito. Rellenarlos con
    cero seria inventar un dato: no es que el retorno haya sido nulo, es que no
    hay dia anterior con el cual compararlo.
    """
    if marco.empty:
        return marco

    marco = marco.sort_values(["simbolo", "fecha"]).reset_index(drop=True)
    agrupado = marco.groupby("simbolo", sort=False)["cierre"]

    marco["retorno_pct"] = (agrupado.pct_change() * 100).round(4)

    # min_periods igual a la ventana: sin esto pandas devuelve una media parcial
    # desde el primer dia, y una "media de 30 dias" calculada con 3 dias es un
    # numero que no significa lo que dice.
    marco["sma_7"] = agrupado.transform(
        lambda serie: serie.rolling(config.VENTANA_SMA_CORTA,
                                    min_periods=config.VENTANA_SMA_CORTA).mean()
    ).round(8)

    marco["sma_30"] = agrupado.transform(
        lambda serie: serie.rolling(config.VENTANA_SMA_LARGA,
                                    min_periods=config.VENTANA_SMA_LARGA).mean()
    ).round(8)

    marco["volatilidad_30d"] = marco.groupby("simbolo", sort=False)["retorno_pct"].transform(
        lambda serie: serie.rolling(config.VENTANA_VOLATILIDAD,
                                    min_periods=config.VENTANA_VOLATILIDAD).std()
    ).round(4)

    return marco


# ---------------------------------------------------------------------------
# T03 - CLASIFICACION
# ---------------------------------------------------------------------------
def clasificar_volatilidad(valor):
    """Traduce la volatilidad numerica a una etiqueta legible.

    Existe porque un panel con 'BAJA / MEDIA / ALTA' se lee de un vistazo y uno
    con '2.7431' no. Los cortes estan en config.py, no aqui, para poder
    recalibrarlos sin tocar esta funcion.

    Devuelve None cuando no hay volatilidad calculada, y no 'BAJA': una serie sin
    suficiente historia no es una serie tranquila, es una serie desconocida.
    """
    if valor is None or (isinstance(valor, float) and valor != valor):
        return None
    if valor <= config.CORTE_VOLATILIDAD_BAJA:
        return "BAJA"
    if valor <= config.CORTE_VOLATILIDAD_MEDIA:
        return "MEDIA"
    return "ALTA"


def agregar_clasificacion(marco):
    """Agrega la columna nivel_volatilidad al marco."""
    if marco.empty:
        return marco
    marco["nivel_volatilidad"] = marco["volatilidad_30d"].apply(clasificar_volatilidad)
    return marco


# ---------------------------------------------------------------------------
# CONSOLIDACION
# ---------------------------------------------------------------------------
# Orden de columnas de la zona plata. Coincide con las columnas que el DAG 04
# inserta en hechos_ohlcv_diario: si divergen, la carga falla de forma evidente
# en vez de insertar valores corridos de columna.
COLUMNAS_PLATA = [
    "id_activo",
    "simbolo",
    "fecha",
    "lote_id",
    "apertura",
    "maximo",
    "minimo",
    "cierre",
    "volumen_base",
    "volumen_usdt",
    "n_trades",
    "retorno_pct",
    "sma_7",
    "sma_30",
    "volatilidad_30d",
    "nivel_volatilidad",
]

# Columnas que van a MySQL. `simbolo` no esta: vive en dim_activo y repetirlo en
# la tabla de hechos seria denormalizar sin motivo. Si se agrega, hay que
# actualizar tambien sql/02_tablas.sql.
COLUMNAS_HECHOS = [
    "id_activo",
    "fecha",
    "lote_id",
    "apertura",
    "maximo",
    "minimo",
    "cierre",
    "volumen_base",
    "volumen_usdt",
    "n_trades",
    "retorno_pct",
    "sma_7",
    "sma_30",
    "volatilidad_30d",
    "nivel_volatilidad",
]


def transformar(lote_id, filas_validas):
    """Cadena completa de la zona plata: normaliza, calcula y clasifica.

    Es el unico punto de entrada que necesita el DAG 03. Devuelve un DataFrame
    con las columnas de COLUMNAS_PLATA, en ese orden.
    """
    marco = normalizar_lote(filas_validas)
    if marco.empty:
        return marco

    marco = calcular_indicadores(marco)
    marco = agregar_clasificacion(marco)
    marco["lote_id"] = lote_id

    return marco[COLUMNAS_PLATA]


def extraer_dimension(marco):
    """Deriva dim_activo a partir de los simbolos presentes en el lote.

    El catalogo semilla (datos_semilla/catalogo_activos.csv) es la fuente de los
    atributos descriptivos; esta funcion solo produce el minimo necesario para
    que la clave foranea de hechos_ohlcv_diario se satisfaga aunque el catalogo
    no incluya un simbolo. Sin esto, un simbolo nuevo haria fallar la carga
    entera del DAG 04 por violacion de clave foranea.
    """
    if marco.empty:
        return []

    activos = []
    for simbolo in sorted(marco["simbolo"].unique()):
        # Los pares del exchange se nombran BASE+COTIZACION sin separador. Se
        # asume USDT como cotizacion porque es lo unico que descarga el DAG 01;
        # si se agregan pares con otra moneda hay que revisar este corte.
        if simbolo.endswith("USDT"):
            base, cotizacion = simbolo[:-4], "USDT"
        else:
            base, cotizacion = simbolo, ""

        activos.append({
            "id_activo": "ACT-" + simbolo,
            "simbolo": simbolo,
            "activo_base": base,
            "activo_cotizacion": cotizacion,
            "nombre": base,          # el catalogo semilla lo sobrescribe si lo trae
            "categoria": None,
            "estado": "ACTIVO",
        })
    return activos


def combinar_con_catalogo(activos, catalogo):
    """Enriquece la dimension con los atributos del catalogo semilla.

    El catalogo manda en los campos descriptivos (nombre, categoria); los
    derivados del simbolo se conservan. Un simbolo ausente del catalogo entra
    igual, con el nombre derivado: es preferible una dimension incompleta a una
    carga de hechos que falla.
    """
    indice = {str(fila.get("simbolo", "")).strip().upper(): fila for fila in catalogo}

    combinados = []
    for activo in activos:
        entrada = indice.get(activo["simbolo"])
        if entrada:
            activo = dict(activo)
            if str(entrada.get("nombre", "")).strip():
                activo["nombre"] = str(entrada["nombre"]).strip()
            if str(entrada.get("categoria", "")).strip():
                activo["categoria"] = str(entrada["categoria"]).strip()
        combinados.append(activo)
    return combinados


def sustituir_nulos(marco):
    """Convierte los NaN de pandas en None para que MySQL reciba NULL.

    Sin esto, el conector inserta la cadena 'nan' en una columna DECIMAL y falla,
    o peor, guarda un valor sin sentido. Es el paso que separa un DataFrame de un
    conjunto de filas listo para insertar.
    """
    return marco.replace({np.nan: None}).where(pd.notnull(marco), None)
