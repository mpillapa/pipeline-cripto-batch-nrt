"""Prueba de la conciliacion NRT <-> batch. NO necesita Elasticsearch ni Airflow.

    python pruebas/prueba_conciliacion.py

La conciliacion es la pieza que justifica tener dos flujos, y es tambien la
ultima en poder probarse de extremo a extremo, porque necesita que el camino
near real-time este escribiendo. Esta prueba rompe esa dependencia: alimenta las
funciones con una respuesta de Elasticsearch guardada y verifica la aritmetica y
los veredictos.

Lo que NO cubre: que la consulta real devuelva lo que se espera. Eso solo se
comprueba con Elasticsearch levantado y datos dentro.
"""

import os
import sys

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(RAIZ, "dags"))
os.environ.setdefault("CRIPTO_DIR_DATOS", os.path.join(RAIZ, "datos"))

from comun import conciliacion, config  # noqa: E402

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
# Respuesta de Elasticsearch simulada.
#
# Tres horas con situaciones distintas a proposito:
#   14:00  hora sana, mucho volumen y las 60 ventanas
#   15:00  hora con pocas ventanas -> cobertura baja
#   16:00  hora sin volumen -> no se puede calcular el VWAP
# ---------------------------------------------------------------------------
RESPUESTA_ES = {
    "aggregations": {
        "por_hora": {
            "buckets": [
                {
                    "key_as_string": "2026-09-07T14:00:00.000Z",
                    "doc_count": 60,
                    # 200 unidades por 63 250 de precio medio
                    "volumen_usdt": {"value": 12650000.0},
                    "volumen_base": {"value": 200.0},
                    "n_trades": {"value": 24000},
                    "ventanas": {"value": 60},
                },
                {
                    "key_as_string": "2026-09-07T15:00:00.000Z",
                    "doc_count": 12,
                    "volumen_usdt": {"value": 2530000.0},
                    "volumen_base": {"value": 40.0},
                    "n_trades": {"value": 4000},
                    "ventanas": {"value": 12},
                },
                {
                    "key_as_string": "2026-09-07T16:00:00.000Z",
                    "doc_count": 3,
                    "volumen_usdt": {"value": 0.0},
                    "volumen_base": {"value": 0.0},
                    "n_trades": {"value": 0},
                    "ventanas": {"value": 3},
                },
            ]
        }
    }
}

REFERENCIA_BATCH = {
    "2026-09-07T14:00:00.000Z": {"cierre": 63250.0, "n_trades": 25000},
    "2026-09-07T15:00:00.000Z": {"cierre": 63250.0, "n_trades": 26000},
    "2026-09-07T16:00:00.000Z": {"cierre": 63300.0, "n_trades": 24000},
    # Una hora que el batch tiene y el NRT no. No debe aparecer en el resultado.
    "2026-09-07T17:00:00.000Z": {"cierre": 63400.0, "n_trades": 23000},
}


# ---------------------------------------------------------------------------
titulo("[1] La consulta a Elasticsearch tiene la forma correcta")
# ---------------------------------------------------------------------------
consulta = conciliacion.construir_consulta(
    "BTCUSDT", "2026-09-07T00:00:00Z", "2026-09-08T00:00:00Z"
)

comprobar("no pide documentos, solo cubos", consulta["size"] == 0)

filtros = consulta["query"]["bool"]["filter"]
comprobar(
    "filtra por simbolo con term, no con match",
    any("term" in f and "simbolo" in f["term"] for f in filtros),
    str(filtros),
)

histograma = consulta["aggs"]["por_hora"]["date_histogram"]
comprobar("agrupa por hora", histograma["calendar_interval"] == "hour")
comprobar(
    "fija la zona horaria en UTC",
    histograma.get("time_zone") == "UTC",
    "sin esto los cubos se desplazan respecto de las velas del exchange",
)

sub = consulta["aggs"]["por_hora"]["aggs"]
comprobar(
    "suma los dos volumenes, que es lo que permite el VWAP ponderado",
    "volumen_usdt" in sub and "volumen_base" in sub,
    str(sorted(sub)),
)


# ---------------------------------------------------------------------------
titulo("[2] El VWAP horario es ponderado por volumen, no un promedio simple")
# ---------------------------------------------------------------------------
metricas = conciliacion.interpretar_respuesta(RESPUESTA_ES)

comprobar("se interpretan las tres horas", len(metricas) == 3, str(len(metricas)))

hora_sana = metricas["2026-09-07T14:00:00.000Z"]
# 12 650 000 / 200 = 63 250
comprobar(
    "vwap = suma(volumen_usdt) / suma(volumen_base)",
    abs(hora_sana["vwap"] - 63250.0) < 0.01,
    str(hora_sana["vwap"]),
)
comprobar("se conserva el conteo de trades", hora_sana["n_trades"] == 24000)
comprobar("se conserva el numero de ventanas", hora_sana["ventanas"] == 60)


# ---------------------------------------------------------------------------
titulo("[3] Una hora sin volumen no produce un VWAP inventado")
# ---------------------------------------------------------------------------
hora_vacia = metricas["2026-09-07T16:00:00.000Z"]
comprobar(
    "vwap nulo, no cero",
    hora_vacia["vwap"] is None,
    "un cero se compararia contra el cierre real y daria una desviacion del "
    "100% que parece un hallazgo y es un artefacto: " + str(hora_vacia["vwap"]),
)


# ---------------------------------------------------------------------------
titulo("[4] Los veredictos usan las DOS condiciones")
# ---------------------------------------------------------------------------
print("  umbrales: desviacion <= " + str(config.CONCILIACION_DESVIACION_ACEPTABLE) +
      " %, cobertura >= " + str(config.CONCILIACION_COBERTURA_MINIMA) + " %")

comprobar("desviacion baja y cobertura alta -> COINCIDE",
          conciliacion.veredicto(0.1, 96.0) == "COINCIDE")
comprobar("desviacion alta y cobertura alta -> DESVIADO",
          conciliacion.veredicto(3.0, 96.0) == "DESVIADO")
comprobar(
    "desviacion baja pero cobertura baja -> DESVIADO",
    conciliacion.veredicto(0.1, 30.0) == "DESVIADO",
    "una desviacion pequena viendo el 30% de los trades no demuestra que el "
    "streaming mida bien",
)
comprobar("falta un lado -> SIN_DATOS",
          conciliacion.veredicto(None, 96.0) == "SIN_DATOS")
comprobar("desviacion negativa se evalua en valor absoluto",
          conciliacion.veredicto(-0.1, 96.0) == "COINCIDE")


# ---------------------------------------------------------------------------
titulo("[5] La comparacion cruza bien los dos lados")
# ---------------------------------------------------------------------------
filas = conciliacion.comparar("BTCUSDT", metricas, REFERENCIA_BATCH, "L_PRUEBA")

comprobar(
    "solo se concilian las horas que el NRT vio",
    len(filas) == 3,
    "el batch tiene 4 horas; la de las 17:00 no debe aparecer: " + str(len(filas)),
)

por_hora = {f["fecha_hora"]: f for f in filas}

sana = por_hora["2026-09-07 14:00:00"]
comprobar("la hora sana COINCIDE", sana["veredicto"] == "COINCIDE", str(sana))
comprobar("desviacion practicamente nula",
          abs(sana["desviacion_pct"]) < 0.01, str(sana["desviacion_pct"]))
# 24000 / 25000 = 96 %
comprobar("cobertura del 96 %",
          abs(sana["cobertura_pct"] - 96.0) < 0.01, str(sana["cobertura_pct"]))

parcial = por_hora["2026-09-07 15:00:00"]
# 2 530 000 / 40 = 63 250 -> desviacion nula, pero 4000/26000 = 15,4 % de cobertura
comprobar(
    "la hora con pocas ventanas sale DESVIADA pese a la desviacion nula",
    parcial["veredicto"] == "DESVIADO",
    "desviacion " + str(parcial["desviacion_pct"]) + " cobertura " +
    str(parcial["cobertura_pct"]),
)

vacia = por_hora["2026-09-07 16:00:00"]
comprobar("la hora sin volumen queda SIN_DATOS", vacia["veredicto"] == "SIN_DATOS")
comprobar(
    "aun asi se registra la hora",
    vacia["simbolo"] == "BTCUSDT" and vacia["lote_id"] == "L_PRUEBA",
    "borrarla del reporte escondería los periodos en que el flujo estuvo caido",
)


# ---------------------------------------------------------------------------
titulo("[6] El formato de fecha_hora es el que acepta MySQL")
# ---------------------------------------------------------------------------
for fila in filas:
    comprobar(
        "fecha_hora sin T ni Z: " + fila["fecha_hora"],
        "T" not in fila["fecha_hora"] and "Z" not in fila["fecha_hora"],
        fila["fecha_hora"],
    )


# ---------------------------------------------------------------------------
titulo("[7] El resumen no se deja enganar por desviaciones de signo opuesto")
# ---------------------------------------------------------------------------
resumen = conciliacion.resumir(filas)

comprobar("cuenta las tres horas", resumen["horas"] == 3, str(resumen))
comprobar("una coincide", resumen["coinciden"] == 1, str(resumen["coinciden"]))
comprobar("una desviada", resumen["desviadas"] == 1, str(resumen["desviadas"]))
comprobar("una sin datos", resumen["sin_datos"] == 1, str(resumen["sin_datos"]))

# Caso explicito: +0,4 % y -0,4 %. Promediados con signo darian 0,0 y sugerirían
# una precision perfecta que no existe.
opuestas = [
    {"veredicto": "DESVIADO", "desviacion_pct": 0.4, "cobertura_pct": 90.0},
    {"veredicto": "DESVIADO", "desviacion_pct": -0.4, "cobertura_pct": 90.0},
]
resumen_opuestas = conciliacion.resumir(opuestas)
comprobar(
    "la media de desviacion usa valores absolutos",
    abs(resumen_opuestas["desviacion_media_pct"] - 0.4) < 0.001,
    "con signo daria 0.0 y sugeriria una precision perfecta: " +
    str(resumen_opuestas["desviacion_media_pct"]),
)

comprobar("un conjunto vacio no revienta",
          conciliacion.resumir([])["horas"] == 0)


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
