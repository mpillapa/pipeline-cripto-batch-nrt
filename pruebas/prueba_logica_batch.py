"""Prueba de humo de la logica del camino batch. NO necesita Airflow ni Docker.

    python pruebas/prueba_logica_batch.py

Tarda segundos y cubre lo que los DAGs no pueden probar por si solos: que las
reglas de calidad detecten lo que dicen detectar y que los indicadores calculen
lo que dicen calcular.

La comprobacion mas importante es el bloque [2]: verifica que CADA defecto que
inyecta el generador sea detectado por LA REGLA QUE LE CORRESPONDE. Sin esa
correspondencia, una regla puede romperse y las pruebas seguir en verde porque
otra regla atrapa el defecto por casualidad.
"""

import os
import sys

# El modulo `comun` vive en dags/. Se agrega al path para poder ejecutarlo desde
# la raiz del proyecto sin instalar nada ni levantar Airflow.
RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(RAIZ, "dags"))

# Las rutas apuntan a /opt/airflow/datos dentro del contenedor. Fuera de el se
# redirigen a una carpeta temporal: ninguna prueba de este archivo escribe en
# disco, pero config.py construye las rutas al importarse.
os.environ.setdefault("CRIPTO_DIR_DATOS", os.path.join(RAIZ, "datos"))

from comun import clientes_api, config, reglas_calidad, transformaciones  # noqa: E402

fallos = []


def comprobar(descripcion, condicion, detalle=""):
    """Registra el resultado de una comprobacion y lo imprime."""
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
titulo("[1] El generador produce velas que pasan todas las reglas")
# ---------------------------------------------------------------------------
limpias = clientes_api.generar_klines_sinteticas(
    "BTCUSDT", dias=120, semilla=42, tasa_defectos=0.0
)
filas_limpias = limpias.to_dict("records")

comprobar("genera 120 velas", len(filas_limpias) == 120, str(len(filas_limpias)))

validas, rechazadas, informe = reglas_calidad.validar_lote(filas_limpias)
comprobar(
    "ninguna vela limpia es rechazada",
    len(rechazadas) == 0,
    "rechazadas: " + str([r["motivo_rechazo"] for r in rechazadas[:3]]),
)
comprobar("tasa de rechazo cero", informe["tasa_rechazo"] == 0.0, str(informe["tasa_rechazo"]))


# ---------------------------------------------------------------------------
titulo("[2] Cada defecto inyectado lo detecta la regla que le corresponde")
# ---------------------------------------------------------------------------
base = filas_limpias[10]

casos = [
    ("campo_vacio",       {"cierre": None},                             "R01"),
    # NaN y no None: es lo que devuelve pandas al leer un nulo de una columna
    # numerica de Parquet, o sea la forma REAL en que llega un campo vacio a
    # este pipeline. La version anterior de R01 solo miraba `is None` y este
    # caso se le escapaba: la fila acababa rechazada por R02, con un motivo
    # equivocado escrito en cuarentena.
    ("campo_nan",         {"cierre": float("nan")},                     "R01"),
    ("campo_cadena_nan",  {"cierre": "nan"},                            "R01"),
    ("ohlc_incoherente",  {"maximo": base["minimo"], "minimo": base["maximo"]}, "R02"),
    ("precio_negativo",   {"apertura": -100.0},                         "R03"),
    ("volumen_negativo",  {"volumen_base": -5.0},                       "R04"),
    ("simbolo_ajeno",     {"simbolo": "DOGEUSDT"},                      "R05"),
    ("fecha_futura",      {"fecha": "2099-01-01"},                      "R06"),
    ("volumen_desfasado", {"volumen_usdt": base["volumen_usdt"] * 1000}, "R07"),
]

for nombre, cambios, codigo_esperado in casos:
    fila = dict(base)
    fila.update(cambios)
    errores = reglas_calidad.validar_fila(fila)
    codigos = {error.split(" ", 1)[0] for error in errores}
    comprobar(
        nombre + " -> " + codigo_esperado,
        codigo_esperado in codigos,
        "codigos detectados: " + str(sorted(codigos)),
    )


# ---------------------------------------------------------------------------
titulo("[3] R02 detecta el intercambio de maximo y minimo")
# ---------------------------------------------------------------------------
# Caso especifico porque es el error de transporte mas silencioso: ninguna regla
# de rango lo nota, todos los valores siguen siendo positivos y plausibles.
intercambiada = dict(base)
intercambiada["maximo"] = base["minimo"]
intercambiada["minimo"] = base["maximo"]
error_r02 = reglas_calidad.r02_coherencia_ohlc(intercambiada)
comprobar("maximo < minimo se detecta", error_r02 is not None, str(error_r02))

# Y el caso contrario: un maximo que no cubre al cierre.
sin_cubrir = dict(base)
sin_cubrir["maximo"] = min(base["apertura"], base["cierre"]) * 0.5
comprobar(
    "maximo que no cubre apertura/cierre se detecta",
    reglas_calidad.r02_coherencia_ohlc(sin_cubrir) is not None,
)


# ---------------------------------------------------------------------------
titulo("[4] R08 detecta velas duplicadas dentro del lote")
# ---------------------------------------------------------------------------
con_duplicado = filas_limpias + [dict(filas_limpias[0])]
_, rechazadas_dup, informe_dup = reglas_calidad.validar_lote(con_duplicado)

comprobar("se rechaza exactamente una fila", len(rechazadas_dup) == 1, str(len(rechazadas_dup)))
comprobar("el motivo es R08", "R08" in informe_dup["conteo_por_regla"],
          str(informe_dup["conteo_por_regla"]))
comprobar(
    "se conserva la primera aparicion, no la segunda",
    len(informe_dup["conteo_por_regla"]) == 1,
    str(informe_dup["conteo_por_regla"]),
)


# ---------------------------------------------------------------------------
titulo("[5] La decision de promocion usa sus dos criterios")
# ---------------------------------------------------------------------------
promover_ok, motivo_ok = reglas_calidad.decidir_promocion({
    "tasa_rechazo": 0.02, "filas_validas": 300,
})
comprobar("lote sano se promueve", promover_ok is True, motivo_ok)

promover_tasa, motivo_tasa = reglas_calidad.decidir_promocion({
    "tasa_rechazo": 0.40, "filas_validas": 300,
})
comprobar("tasa alta bloquea", promover_tasa is False, motivo_tasa)

promover_pocas, motivo_pocas = reglas_calidad.decidir_promocion({
    "tasa_rechazo": 0.01, "filas_validas": 10,
})
comprobar("pocas filas bloquean aunque la tasa sea buena",
          promover_pocas is False, motivo_pocas)


# ---------------------------------------------------------------------------
titulo("[6] Los indicadores se calculan por simbolo, no sobre el marco entero")
# ---------------------------------------------------------------------------
# Se mezclan dos series a proposito. Si el calculo no agrupara por simbolo, el
# primer dia de ETH tomaria como referencia el ultimo de BTC y produciria un
# retorno absurdo.
btc = clientes_api.generar_klines_sinteticas("BTCUSDT", dias=60, semilla=1).to_dict("records")
eth = clientes_api.generar_klines_sinteticas("ETHUSDT", dias=60, semilla=2).to_dict("records")

marco = transformaciones.transformar("L_PRUEBA", btc + eth)

comprobar("el marco tiene 120 filas", len(marco) == 120, str(len(marco)))

primer_eth = marco[marco["simbolo"] == "ETHUSDT"].iloc[0]
comprobar(
    "el primer dia de cada simbolo no tiene retorno",
    primer_eth["retorno_pct"] != primer_eth["retorno_pct"],  # es NaN
    str(primer_eth["retorno_pct"]),
)

# El retorno maximo de una caminata aleatoria con sigma 2% no deberia acercarse
# a 100%. Si el groupby fallara, el salto entre series lo dispararia.
retorno_maximo = marco["retorno_pct"].abs().max()
comprobar(
    "ningun retorno absurdo por mezcla de series",
    retorno_maximo < 50,
    "retorno maximo: " + str(retorno_maximo),
)


# ---------------------------------------------------------------------------
titulo("[7] Las medias moviles respetan su ventana minima")
# ---------------------------------------------------------------------------
solo_btc = marco[marco["simbolo"] == "BTCUSDT"].reset_index(drop=True)

nulos_sma7 = solo_btc["sma_7"].isna().sum()
nulos_sma30 = solo_btc["sma_30"].isna().sum()

comprobar(
    "sma_7 nula en los primeros 6 dias",
    nulos_sma7 == config.VENTANA_SMA_CORTA - 1,
    "nulos: " + str(nulos_sma7),
)
comprobar(
    "sma_30 nula en los primeros 29 dias",
    nulos_sma30 == config.VENTANA_SMA_LARGA - 1,
    "nulos: " + str(nulos_sma30),
)

# Verificacion aritmetica directa: la sma_7 del dia 7 es el promedio de los 7
# primeros cierres. Sin esto solo se estaria comprobando que hay un numero, no
# que sea el numero correcto.
esperada = solo_btc["cierre"].head(config.VENTANA_SMA_CORTA).mean()
obtenida = solo_btc["sma_7"].iloc[config.VENTANA_SMA_CORTA - 1]
comprobar(
    "sma_7 coincide con el promedio calculado a mano",
    abs(float(esperada) - float(obtenida)) < 0.01,
    "esperada " + str(esperada) + " obtenida " + str(obtenida),
)


# ---------------------------------------------------------------------------
titulo("[8] La clasificacion de volatilidad no inventa etiquetas")
# ---------------------------------------------------------------------------
comprobar("volatilidad nula -> sin etiqueta",
          transformaciones.clasificar_volatilidad(None) is None)
comprobar("volatilidad nula (NaN) -> sin etiqueta",
          transformaciones.clasificar_volatilidad(float("nan")) is None)
comprobar("1.0 -> BAJA", transformaciones.clasificar_volatilidad(1.0) == "BAJA")
comprobar("3.5 -> MEDIA", transformaciones.clasificar_volatilidad(3.5) == "MEDIA")
comprobar("9.0 -> ALTA", transformaciones.clasificar_volatilidad(9.0) == "ALTA")


# ---------------------------------------------------------------------------
titulo("[9] La normalizacion unifica formas distintas del mismo dato")
# ---------------------------------------------------------------------------
sucia = dict(base)
sucia["simbolo"] = "  btcusdt  "
normalizada = transformaciones.normalizar_vela(sucia)

comprobar("simbolo sin espacios y en mayusculas",
          normalizada["simbolo"] == "BTCUSDT", normalizada["simbolo"])
comprobar("id_activo derivado del simbolo",
          normalizada["id_activo"] == "ACT-BTCUSDT", normalizada["id_activo"])
comprobar("fecha en formato ISO",
          len(normalizada["fecha"]) == 10 and normalizada["fecha"][4] == "-",
          normalizada["fecha"])


# ---------------------------------------------------------------------------
titulo("[10] La dimension se deriva correctamente del lote")
# ---------------------------------------------------------------------------
activos = transformaciones.extraer_dimension(marco)
comprobar("un activo por simbolo del lote", len(activos) == 2, str(len(activos)))

simbolos = sorted(activo["simbolo"] for activo in activos)
comprobar("los simbolos son los esperados", simbolos == ["BTCUSDT", "ETHUSDT"], str(simbolos))

btc_activo = next(a for a in activos if a["simbolo"] == "BTCUSDT")
comprobar("el par se separa en base y cotizacion",
          btc_activo["activo_base"] == "BTC" and btc_activo["activo_cotizacion"] == "USDT",
          str(btc_activo))

# Un simbolo ausente del catalogo debe entrar igual: es preferible una dimension
# incompleta a una carga de hechos que falla por clave foranea.
combinados = transformaciones.combinar_con_catalogo(
    activos, [{"simbolo": "BTCUSDT", "nombre": "Bitcoin", "categoria": "Reserva"}]
)
btc_combinado = next(a for a in combinados if a["simbolo"] == "BTCUSDT")
eth_combinado = next(a for a in combinados if a["simbolo"] == "ETHUSDT")

comprobar("el catalogo enriquece el nombre",
          btc_combinado["nombre"] == "Bitcoin", btc_combinado["nombre"])
comprobar("el simbolo ausente del catalogo sobrevive",
          eth_combinado["nombre"] == "ETH", eth_combinado["nombre"])


# ---------------------------------------------------------------------------
titulo("[11] Un lote con defectos se bloquea y deja rastro en cuarentena")
# ---------------------------------------------------------------------------
defectuosas = clientes_api.generar_klines_sinteticas(
    "BTCUSDT", dias=200, semilla=7, tasa_defectos=0.40
).to_dict("records")

validas_d, rechazadas_d, informe_d = reglas_calidad.validar_lote(defectuosas)
promover_d, motivo_d = reglas_calidad.decidir_promocion(informe_d)

comprobar("hay filas rechazadas", len(rechazadas_d) > 0, str(len(rechazadas_d)))
comprobar("el lote se bloquea", promover_d is False, motivo_d)
comprobar(
    "toda fila rechazada lleva su motivo",
    all(fila.get("motivo_rechazo") for fila in rechazadas_d),
)
comprobar(
    "las velas sinteticas defectuosas quedan marcadas en el origen",
    any(fila.get("origen") == "sintetico_defectuoso" for fila in rechazadas_d),
)


# ---------------------------------------------------------------------------
titulo("[12] Las columnas de plata y de MySQL no divergen")
# ---------------------------------------------------------------------------
# Si estas dos listas se desincronizan, la carga del DAG 04 inserta valores
# corridos de columna: un error que MySQL no siempre detecta y que produce datos
# sin sentido en silencio.
faltantes = [c for c in transformaciones.COLUMNAS_HECHOS
             if c not in transformaciones.COLUMNAS_PLATA]
comprobar("toda columna de MySQL existe en plata", not faltantes, str(faltantes))

en_marco = [c for c in transformaciones.COLUMNAS_HECHOS if c not in marco.columns]
comprobar("el marco transformado trae todas las columnas de MySQL",
          not en_marco, str(en_marco))


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
