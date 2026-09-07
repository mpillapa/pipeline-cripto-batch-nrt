"""Publicacion de eventos de control y logs hacia Logstash (fuentes F6 y F7).

Reutiliza los dos inputs que el entorno del Taller 2 ya tiene configurados:

    HTTP  puerto 8088  -> hitos de los DAGs (tipo_fuente = ops_control)
    TCP   puerto 5000  -> lineas de log de los componentes (ops_log)

Gracias a esto el pipeline se observa a si mismo: en Kibana queda una vista de
operacion junto a la de negocio, sobre la misma linea de tiempo.

PRINCIPIO QUE GOBIERNA TODO ESTE ARCHIVO: la telemetria NUNCA puede tumbar al
emisor. Si Logstash esta caido, lento o cambio de puerto, publicar un evento
falla en silencio y el DAG sigue. Un DAG que se cae porque no pudo reportar su
propia telemetria es peor que un DAG sin telemetria: convierte un problema de
observabilidad en una perdida de datos.

Por eso todas las excepciones se capturan aqui, el tiempo limite es de dos
segundos y no hay reintentos.
"""

import json
import socket

import requests

from comun import config, utilidades


# ---------------------------------------------------------------------------
# CONTROL  (HTTP -> puerto 8088)
# ---------------------------------------------------------------------------
def publicar_control(componente, evento, lote_id=None, estado=None, metricas=None):
    """Envia un hito del pipeline a Logstash por HTTP.

    Devuelve True si se publico, False si no. El resultado se ignora en la
    mayoria de los llamados: esta ahi para que las pruebas puedan comprobar el
    camino feliz.
    """
    cuerpo = {
        "tipo_fuente": "ops_control",
        "componente": componente,
        "evento": evento,
        "lote_id": lote_id,
        "estado": estado,
        "metricas": metricas or {},
        "ts": utilidades.a_iso(utilidades.ahora_utc()),
    }

    try:
        respuesta = requests.post(
            config.LOGSTASH_HTTP_URL,
            json=cuerpo,
            timeout=config.OBSERVABILIDAD_TIEMPO_LIMITE,
        )
        return respuesta.status_code < 400
    except Exception as error:
        # Se imprime, no se lanza. El log de Airflow deja constancia de que la
        # telemetria no salio, sin afectar el resultado de la tarea.
        print("Telemetria no publicada (" + type(error).__name__ + "): " + str(error))
        return False


# ---------------------------------------------------------------------------
# LOG  (TCP -> puerto 5000)
# ---------------------------------------------------------------------------
def publicar_log(componente, nivel, mensaje):
    """Envia una linea de log a Logstash por socket TCP.

    Un objeto JSON por linea, terminado en salto de linea: es lo que espera el
    input tcp con codec json_lines. Sin el salto final, Logstash mantiene el
    evento en el buffer esperando el resto y no lo indexa nunca.

    Se abre y cierra un socket por mensaje. Es ineficiente y aqui da igual: se
    publican unas decenas de lineas por corrida, no miles. Mantener un socket
    persistente obligaria a gestionar reconexiones, y esa complejidad no se paga
    a este volumen.
    """
    linea = {
        "tipo_fuente": "ops_log",
        "componente": componente,
        "nivel": nivel,
        "mensaje": mensaje,
        "ts": utilidades.a_iso(utilidades.ahora_utc()),
    }

    conexion = None
    try:
        conexion = socket.create_connection(
            (config.LOGSTASH_TCP_HOST, config.LOGSTASH_TCP_PUERTO),
            timeout=config.OBSERVABILIDAD_TIEMPO_LIMITE,
        )
        conexion.sendall((json.dumps(linea, ensure_ascii=False) + "\n").encode("utf-8"))
        return True
    except Exception as error:
        print("Log no publicado (" + type(error).__name__ + "): " + str(error))
        return False
    finally:
        if conexion is not None:
            try:
                conexion.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# ATAJOS POR NIVEL
# ---------------------------------------------------------------------------
def info(componente, mensaje):
    print(mensaje)
    return publicar_log(componente, "INFO", mensaje)


def advertencia(componente, mensaje):
    print("ADVERTENCIA: " + mensaje)
    return publicar_log(componente, "WARN", mensaje)


def error(componente, mensaje):
    print("ERROR: " + mensaje)
    return publicar_log(componente, "ERROR", mensaje)
