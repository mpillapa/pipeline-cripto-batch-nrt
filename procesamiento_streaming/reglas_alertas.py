"""Reglas de alerta del flujo near real-time. Funciones puras, sin Spark.

QUE CAMBIO Y POR QUE
--------------------
La primera version de este archivo creaba una regla en **Kibana Alerting** por
API. Se sustituyo por lo que dice el contrato (seccion 5): la alerta se publica
en el topic `alertas.precio` y la genera el job de Spark.

El motivo no es formal. Una regla de Kibana vive dentro de Kibana: no es un
dato, no viaja por el bus, no se puede reprocesar, no se concilia con nada y
desaparece si alguien reconstruye la instancia. Publicada en Kafka, la alerta
es un evento como cualquier otro -con su `id_alerta`, su marca de tiempo y su
indice- y Logstash la indexa por el mismo camino que los trades y las metricas.

Ademas, la version anterior no habria funcionado: declaraba
`rule_type_id: metrics.alert.threshold` y le pasaba parametros de `es_query`,
y apuntaba a `localhost:5602`, que no resuelve desde dentro de la red de Docker.

DONDE VIVE CADA COSA
--------------------
Este modulo tiene los **umbrales y la clasificacion**, en Python puro y por
tanto probables sin levantar nada. `job_metricas_ventana.py` importa los
umbrales de aqui y construye con ellos la expresion de columna equivalente.

No se usa una UDF de Python para reutilizar `clasificar_severidad()` dentro de
Spark: una UDF por fila serializa datos entre la JVM y el interprete y es
notablemente mas lenta que un `when()` nativo. El precio de esa decision es que
la logica esta expresada dos veces, y por eso `prueba_logica_streaming.py`
comprueba que las dos coinciden en los bordes.
"""

import os

# ---------------------------------------------------------------------------
# UMBRALES
# ---------------------------------------------------------------------------
# El contrato fija la FORMA del mensaje y deja el corte a la configuracion del
# job. Esta es esa configuracion, y es la unica fuente: el job la importa.
#
# El valor por defecto de disparo (0,50 %) es el del contrato. En el compose se
# baja a 0,25 %, calibrado contra la volatilidad observada en el mercado real
# -mediana 0,13 %, p90 0,30 %-, para que la alerta suene en los minutos movidos
# y calle en los tranquilos.
UMBRAL_PCT = float(os.environ.get("CRIPTO_UMBRAL_ALERTA_PCT", "0.50"))
UMBRAL_MEDIA_PCT = float(os.environ.get("CRIPTO_UMBRAL_ALERTA_MEDIA_PCT", "1.00"))
UMBRAL_ALTA_PCT = float(os.environ.get("CRIPTO_UMBRAL_ALERTA_ALTA_PCT", "2.00"))

REGLA = "variacion_precio_1min"


def supera_umbral(volatilidad_pct, umbral_pct=None):
    """Si la ventana merece alerta.

    Estrictamente mayor, no mayor o igual: un valor exactamente igual al umbral
    no lo supera. Importa fijarlo porque el limite se cruza a menudo cuando el
    umbral se calibra cerca de la mediana.
    """
    if volatilidad_pct is None:
        return False
    umbral = UMBRAL_PCT if umbral_pct is None else umbral_pct
    return volatilidad_pct > umbral


def clasificar_severidad(volatilidad_pct, media_pct=None, alta_pct=None):
    """`BAJA`, `MEDIA` o `ALTA` segun la magnitud de la variacion.

    Los cortes se evaluan de mayor a menor: con 2,5 % y los cortes por defecto,
    la respuesta es ALTA, no MEDIA. Invertir el orden daria siempre el escalon
    mas bajo que se cumpla, que es justo el error contrario al que interesa.
    """
    media = UMBRAL_MEDIA_PCT if media_pct is None else media_pct
    alta = UMBRAL_ALTA_PCT if alta_pct is None else alta_pct
    if volatilidad_pct is None:
        return None
    if volatilidad_pct >= alta:
        return "ALTA"
    if volatilidad_pct >= media:
        return "MEDIA"
    return "BAJA"


def describir(volatilidad_pct, umbral_pct=None):
    """Texto del campo `detalle`. Dos decimales, como en el contrato."""
    umbral = UMBRAL_PCT if umbral_pct is None else umbral_pct
    return (
        f"Variacion de {volatilidad_pct:.2f}% supera el umbral de {umbral:.2f}%"
    )


if __name__ == "__main__":
    print("Reglas de alerta del flujo NRT")
    print(f"  regla            : {REGLA}")
    print(f"  umbral de disparo: {UMBRAL_PCT} %")
    print(f"  corte MEDIA      : {UMBRAL_MEDIA_PCT} %")
    print(f"  corte ALTA       : {UMBRAL_ALTA_PCT} %")
    print()
    print("Este modulo no genera alertas: solo define las reglas. Las alertas las")
    print("emite job_metricas_ventana.py hacia el topic alertas.precio.")
