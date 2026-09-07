"""Reglas de calidad de las velas OHLCV.

POR QUE ESTAN AQUI Y NO DENTRO DEL DAG:

Una regla escondida dentro de una tarea de Airflow solo se puede leer abriendo
el DAG, no se puede probar sin levantar Airflow y no se puede reutilizar. Aqui
cada regla es una funcion pura -- recibe una fila, devuelve un texto de error o
None -- que se ejecuta con Python a secas.

El catalogo completo, con su justificacion, esta en docs/REGLAS_NEGOCIO.md.

SUPUESTOS: las reglas se apoyan en propiedades aritmeticas de una vela OHLCV
(el maximo no puede ser menor que el cierre, un precio no puede ser negativo) y
en tolerancias elegidas para este trabajo. No provienen de ningun manual ni
sistema de terceros.
"""

from datetime import date, datetime, timezone

from comun import config

# Campos sin los cuales el registro no identifica una vela.
CAMPOS_OBLIGATORIOS = [
    "simbolo",
    "fecha",
    "apertura",
    "maximo",
    "minimo",
    "cierre",
    "volumen_base",
    "volumen_usdt",
    "n_trades",
]

_SIMBOLOS_VALIDOS = {s.strip().upper() for s in config.SIMBOLOS}


# ---------------------------------------------------------------------------
# CONVERSIONES TOLERANTES
# ---------------------------------------------------------------------------
# Un valor no convertible es un dato a rechazar, no un fallo del pipeline. Si
# estas funciones lanzaran excepcion, una sola vela mala tumbaria el lote entero
# en vez de irse a cuarentena.
def _a_decimal(valor):
    try:
        numero = float(str(valor).strip())
    except (TypeError, ValueError):
        return None
    # NaN se cuela por float("nan") sin lanzar excepcion, y luego toda
    # comparacion con el devuelve False, lo que hace que las reglas siguientes
    # lo den por bueno en silencio.
    if numero != numero:
        return None
    return numero


def _a_fecha(valor):
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor
    try:
        return datetime.fromisoformat(str(valor).strip()[:10]).date()
    except (TypeError, ValueError):
        return None


def _esta_vacio(valor):
    """Decide si un valor representa la ausencia de dato.

    Tres formas distintas de lo mismo, y hay que reconocer las tres:

      None      el valor nunca existio, o vino nulo de un JSON.
      ""        cadena vacia, tipica de un CSV con la columna presente y sin dato.
      NaN       lo que devuelve pandas al leer un nulo en una columna numerica.

    El tercer caso es el que se escapa si no se contempla, y es el mas frecuente
    en este pipeline: la zona bronce es Parquet, asi que TODO nulo numerico
    llega aqui como NaN. Comprobar solo `is None` dejaria pasar el campo vacio,
    y aunque otra regla acabe rechazando la fila, el motivo escrito en cuarentena
    seria el equivocado. Un motivo de rechazo incorrecto es peor que ninguno:
    manda a quien revisa a buscar el problema donde no esta.

    NaN se detecta con `valor != valor`, que es cierto unicamente para NaN.
    """
    if valor is None:
        return True
    if isinstance(valor, float) and valor != valor:
        return True
    texto = str(valor).strip()
    # 'nan' aparece cuando un NaN pasa por str() en algun punto intermedio.
    return texto == "" or texto.lower() == "nan"


# ---------------------------------------------------------------------------
# REGLAS  (cada una devuelve un texto de error, o None si la fila la cumple)
# ---------------------------------------------------------------------------
def r01_campos_obligatorios(fila):
    """R01 - Ningun campo obligatorio puede venir vacio."""
    faltantes = [campo for campo in CAMPOS_OBLIGATORIOS if _esta_vacio(fila.get(campo))]
    if faltantes:
        return "R01 campos obligatorios vacios: " + ", ".join(faltantes)
    return None


def r02_coherencia_ohlc(fila):
    """R02 - El maximo y el minimo deben contener a la apertura y al cierre.

    Es la propiedad que define una vela: durante el periodo el precio toco un
    maximo y un minimo, y tanto el primer precio como el ultimo estan entre
    ambos. Una vela que no lo cumple esta corrupta, venga de donde venga.

    Esta regla es la que detecta el error de transporte mas comun: columnas
    desplazadas. Si el maximo y el minimo se intercambian al parsear, ninguna
    regla de rango lo nota, pero esta si.
    """
    valores = {
        campo: _a_decimal(fila.get(campo))
        for campo in ("apertura", "maximo", "minimo", "cierre")
    }
    ilegibles = [campo for campo, valor in valores.items() if valor is None]
    if ilegibles:
        return "R02 precio ilegible en: " + ", ".join(sorted(ilegibles))

    if valores["maximo"] < valores["minimo"]:
        return "R02 maximo " + str(valores["maximo"]) + " es menor que el minimo " + \
               str(valores["minimo"])

    techo = max(valores["apertura"], valores["cierre"])
    piso = min(valores["apertura"], valores["cierre"])

    if valores["maximo"] < techo:
        return "R02 maximo " + str(valores["maximo"]) + " es menor que apertura/cierre " + str(techo)
    if valores["minimo"] > piso:
        return "R02 minimo " + str(valores["minimo"]) + " es mayor que apertura/cierre " + str(piso)
    return None


def r03_precios_positivos(fila):
    """R03 - Ningun precio puede ser cero ni negativo.

    Un precio de cero no es un precio bajo: es la ausencia del dato codificada
    como numero. Dejarla pasar arruina cualquier media movil que la incluya.
    """
    for campo in ("apertura", "maximo", "minimo", "cierre"):
        valor = _a_decimal(fila.get(campo))
        if valor is None:
            return "R03 precio ilegible en " + campo + ": " + repr(fila.get(campo))
        if valor <= 0:
            return "R03 precio no positivo en " + campo + ": " + str(valor)
    return None


def r04_volumenes_no_negativos(fila):
    """R04 - Volumenes y conteo de operaciones no pueden ser negativos.

    Se admite el cero, a diferencia de R03: un periodo sin operaciones es un
    dato legitimo, sobre todo en activos de bajo volumen.
    """
    for campo in ("volumen_base", "volumen_usdt"):
        valor = _a_decimal(fila.get(campo))
        if valor is None:
            return "R04 volumen ilegible en " + campo + ": " + repr(fila.get(campo))
        if valor < 0:
            return "R04 volumen negativo en " + campo + ": " + str(valor)

    conteo = _a_decimal(fila.get("n_trades"))
    if conteo is None:
        return "R04 n_trades ilegible: " + repr(fila.get("n_trades"))
    if conteo < 0:
        return "R04 n_trades negativo: " + str(conteo)
    return None


def r05_simbolo_en_catalogo(fila):
    """R05 - El simbolo debe pertenecer al catalogo configurado.

    Protege contra una respuesta de la API que traiga un par distinto del
    solicitado, por ejemplo tras un cambio en el endpoint.
    """
    simbolo = str(fila.get("simbolo", "")).strip().upper()
    if simbolo not in _SIMBOLOS_VALIDOS:
        return "R05 simbolo fuera de catalogo: " + repr(simbolo) + \
               " (esperados: " + ", ".join(sorted(_SIMBOLOS_VALIDOS)) + ")"
    return None


def r06_fecha_plausible(fila):
    """R06 - La fecha debe ser legible y no puede estar en el futuro.

    Una vela con fecha futura significa que se esta interpretando mal la marca
    de tiempo de la API, casi siempre por confundir segundos con milisegundos.
    El sintoma tipico es una fecha del ano 56000.
    """
    fecha = _a_fecha(fila.get("fecha"))
    if fecha is None:
        return "R06 fecha ilegible: " + repr(fila.get("fecha"))

    hoy = datetime.now(timezone.utc).date()
    if fecha > hoy:
        return "R06 fecha futura: " + fecha.isoformat() + " es posterior a hoy " + hoy.isoformat()
    return None


def r07_coherencia_volumen(fila):
    """R07 - El volumen en USDT debe ser compatible con volumen_base por precio.

    No se exige igualdad. El volumen cotizado real se acumula operacion a
    operacion, con el precio de cada una; aqui solo se dispone del OHLC, asi que
    lo mejor que se puede estimar es `volumen_base * precio_medio`, tomando como
    precio medio el punto medio entre maximo y minimo.

    La tolerancia (config.TOLERANCIA_VOLUMEN_PCT) es amplia a proposito: la
    regla busca detectar un desfase de columnas o un factor de escala
    equivocado, no validar la aritmetica del exchange. Un valor mil veces mayor
    lo detecta; uno un 10 % distinto, no, y no debe.
    """
    volumen_base = _a_decimal(fila.get("volumen_base"))
    volumen_usdt = _a_decimal(fila.get("volumen_usdt"))
    maximo = _a_decimal(fila.get("maximo"))
    minimo = _a_decimal(fila.get("minimo"))

    if None in (volumen_base, volumen_usdt, maximo, minimo):
        return None  # R02, R03 y R04 ya reportan lo ilegible; no se duplica el error

    # Sin operaciones no hay nada que contrastar.
    if volumen_base == 0 and volumen_usdt == 0:
        return None

    precio_medio = (maximo + minimo) / 2
    if precio_medio <= 0:
        return None  # R03 ya lo reporto

    estimado = volumen_base * precio_medio
    if estimado <= 0:
        return None

    desvio_pct = abs(volumen_usdt - estimado) / estimado * 100
    if desvio_pct > config.TOLERANCIA_VOLUMEN_PCT:
        return "R07 volumen_usdt " + str(round(volumen_usdt, 2)) + \
               " se desvia " + str(round(desvio_pct, 1)) + "% del estimado " + \
               str(round(estimado, 2)) + " (tolerancia " + \
               str(config.TOLERANCIA_VOLUMEN_PCT) + "%)"
    return None


# R08 (unicidad) no esta en esta lista: no se puede evaluar mirando una sola
# fila, necesita el lote completo. Se aplica aparte, en validar_lote().
REGLAS_POR_FILA = [
    r01_campos_obligatorios,
    r02_coherencia_ohlc,
    r03_precios_positivos,
    r04_volumenes_no_negativos,
    r05_simbolo_en_catalogo,
    r06_fecha_plausible,
    r07_coherencia_volumen,
]


# ---------------------------------------------------------------------------
# EVALUACION DEL LOTE
# ---------------------------------------------------------------------------
def validar_fila(fila):
    """Aplica todas las reglas de fila. Devuelve la lista de errores encontrados.

    Se evaluan TODAS las reglas, no se corta en la primera que falla: para
    corregir un dato es mas util saber todo lo que tiene mal de una vez.
    """
    errores = []
    for regla in REGLAS_POR_FILA:
        error = regla(fila)
        if error:
            errores.append(error)
    return errores


def validar_lote(filas):
    """Valida el lote completo.

    Devuelve (validas, rechazadas, informe):
      validas    - filas que cumplen todas las reglas
      rechazadas - filas con una columna extra `motivo_rechazo`
      informe    - conteos por regla, para el reporte y la decision del branching

    La regla R08 (unicidad de simbolo + fecha) se aplica aqui porque requiere ver
    el lote entero. Se conserva la primera aparicion y se rechazan las
    siguientes: lo contrario descartaria el registro original por culpa del
    duplicado.

    La unicidad importa mas que en otros dominios: dos velas del mismo dia para
    el mismo simbolo no solo duplican una fila, sino que descuadran todas las
    medias moviles calculadas despues.
    """
    validas = []
    rechazadas = []
    conteo_por_regla = {}
    vistos = set()

    for fila in filas:
        errores = validar_fila(fila)

        simbolo = str(fila.get("simbolo", "")).strip().upper()
        fecha = _a_fecha(fila.get("fecha"))
        if simbolo and fecha is not None:
            clave = (simbolo, fecha.isoformat())
            if clave in vistos:
                errores.append("R08 vela duplicada en el lote: " + simbolo + " " + fecha.isoformat())
            else:
                vistos.add(clave)

        if errores:
            for error in errores:
                codigo = error.split(" ", 1)[0]
                conteo_por_regla[codigo] = conteo_por_regla.get(codigo, 0) + 1
            rechazada = dict(fila)
            rechazada["motivo_rechazo"] = " | ".join(errores)
            rechazadas.append(rechazada)
        else:
            validas.append(fila)

    total = len(filas)
    informe = {
        "filas_evaluadas": total,
        "filas_validas": len(validas),
        "filas_rechazadas": len(rechazadas),
        # Redondeo a 4 decimales: el valor se compara contra UMBRAL_RECHAZO y se
        # imprime en el reporte; la precision completa de un float no aporta.
        "tasa_rechazo": round(len(rechazadas) / total, 4) if total else 0.0,
        "conteo_por_regla": dict(sorted(conteo_por_regla.items())),
    }
    return validas, rechazadas, informe


def decidir_promocion(informe):
    """Decide si el lote sigue adelante o se bloquea. La usa el branching del DAG 02.

    Devuelve (promover, motivo). Dos criterios independientes:

      1. Tasa de rechazo por encima del umbral: la fuente esta entregando datos
         malos y procesarlos produciria indicadores enganosos.
      2. Muy pocas filas validas: aunque la tasa sea buena, sin suficiente
         historia las medias moviles de 30 dias salen todas nulas y el lote no
         aporta nada.
    """
    if informe["tasa_rechazo"] > config.UMBRAL_RECHAZO:
        return False, (
            "Tasa de rechazo " + str(informe["tasa_rechazo"]) +
            " supera el umbral " + str(config.UMBRAL_RECHAZO)
        )

    if informe["filas_validas"] < config.MINIMO_FILAS_VALIDAS:
        return False, (
            "Solo " + str(informe["filas_validas"]) + " filas validas; se "
            "necesitan al menos " + str(config.MINIMO_FILAS_VALIDAS) +
            " para calcular los indicadores"
        )

    return True, "Lote dentro de los umbrales de calidad"
