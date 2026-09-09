"""Envio de eventos `ops_control` a Logstash desde el productor.

Vive en su propio modulo por la misma razon que repositorio.py en el camino
batch: es una integracion con un sistema externo y no debe estar mezclada con la
logica del productor. Si manana la telemetria va a otro sitio, se cambia aqui.

Usa solo la biblioteca estandar a proposito. El contenedor del productor tiene
una unica dependencia (kafka-python) mas websocket-client; anadir `requests`
solo para tres POST no se justifica.

REGLA: reportar telemetria NUNCA puede tumbar la ingesta. Todas las funciones de
este modulo tragan sus excepciones y devuelven True/False. Si Logstash esta
caido, el productor sigue publicando trades.
"""

import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone

LOGSTASH_HTTP_URL = os.environ.get("CRIPTO_LOGSTASH_HTTP", "http://logstash:8088")

# Corto y deliberado: mismo criterio que OBSERVABILIDAD_TIEMPO_LIMITE en el
# config del batch. Ver seccion 8 del contrato.
TIEMPO_LIMITE = 2


def reportar(evento, componente="productor"):
    """Publica un evento `ops_control`. Devuelve True si Logstash lo acepto.

    `evento` es un dict con los campos propios del hito. Los campos comunes
    (`tipo_fuente`, `componente`, `ts_evento`) los pone esta funcion para que
    quien la llama no tenga que acordarse.
    """
    cuerpo = {
        "tipo_fuente": "ops_control",
        "componente": componente,
        "ts_evento": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
    }
    cuerpo.update(evento)

    peticion = urllib.request.Request(
        LOGSTASH_HTTP_URL,
        data=json.dumps(cuerpo).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(peticion, timeout=TIEMPO_LIMITE) as respuesta:
            return 200 <= respuesta.status < 300
    except (urllib.error.URLError, OSError, ValueError) as error:
        # Se avisa por consola pero no se propaga: la telemetria es accesoria.
        print("[ops] no se pudo reportar ({}): {}".format(type(error).__name__, error), flush=True)
        return False
