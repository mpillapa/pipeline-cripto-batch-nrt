"""Prueba de carga del bus de eventos (P9). Necesita Kafka levantado.

    python pruebas/prueba_carga.py

POR QUE ESTA PRUEBA SE REESCRIBIO
---------------------------------
La version anterior inyectaba a 500, 1000 y 2000 eventos/s y reportaba la tasa
que habia conseguido enviar. Eso **mide el productor contra si mismo**: si el
proceso logra empujar 2000 eventos/s, imprime "2000 EPS" y parece un exito,
aunque Logstash vaya cinco minutos por detras y el pipeline este acumulando
retraso sin que nadie lo vea.

Es el mismo defecto que tenia `prueba_latencia.py` cuando restaba `@timestamp`
de `ts_evento`: una medicion que no puede fallar no demuestra nada.

El criterio del plan (seccion 8, P9) no es la tasa de inyeccion sino
**"hasta donde aguanta y donde crece el lag del consumidor"**. Eso es lo que
mide esta version.

QUE SE MIDE
-----------
Para cada tasa objetivo:

  1. Tasa real de inyeccion      lo que el productor consigue empujar
  2. Lag maximo                  pendientes, muestreado DURANTE la inyeccion
  3. Tiempo de drenaje           cuanto tarda el consumidor en ponerse al dia
  4. Tasa de consumo             cota inferior de lo que drena el consumidor

El sistema "aguanta" una tasa si el lag vuelve a su nivel de reposo cuando la
inyeccion para. Si el lag no baja, la tasa de consumo es menor que la de
inyeccion y ese es el techo real del pipeline.

LO QUE ESTA PRUEBA NO MIDE
--------------------------
El consumidor observado es Logstash. Spark tiene su propio ritmo y no aparece
aqui: usa el conector nativo con offsets en su checkpoint, no un grupo de
consumidores de Kafka, asi que su retraso no se ve con esta herramienta.
"""

import json
import os
import sys
import threading
import time

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient

# Desde el repositorio, el simulador esta en `../ingesta_streaming`. Dentro del
# contenedor del productor, ese mismo directorio esta montado en `/app`. Se
# prueban los dos para que la prueba corra en ambos sitios sin editarla.
for _ruta in (
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ingesta_streaming"),
    "/app",
):
    if os.path.isdir(_ruta):
        sys.path.append(_ruta)
from simulador_trades import generar_trade  # noqa: E402

# Desde Windows se resuelve `localhost`; el listener interno `kafka:29092` solo
# existe dentro de la red de Docker.
KAFKA_BROKER = os.environ.get("CRIPTO_KAFKA_EXTERNO", "localhost:9095")
TOPIC = "trades.crudo"
GRUPO = "logstash-cripto-trades"

TASAS = (500, 1000, 2000)
DURACION_S = 10
# Cuanto se espera como maximo a que el consumidor drene. Si se agota, la tasa
# de consumo es menor que la de inyeccion y esa es la conclusion de la prueba.
ESPERA_DRENAJE_S = 90
# Por debajo de esto se considera que el consumidor esta al dia: nunca baja a
# cero exacto porque el productor de fondo sigue publicando.
LAG_REPOSO = 200


def _lag(admin, consumidor):
    """Mensajes pendientes del grupo: fin del log menos offset comprometido."""
    comprometidos = admin.list_consumer_group_offsets(GRUPO)
    particiones = [tp for tp in comprometidos if tp.topic == TOPIC]
    if not particiones:
        return None
    finales = consumidor.end_offsets(particiones)
    total = 0
    for tp in particiones:
        offset = comprometidos[tp].offset
        # -1 significa que el grupo aun no ha comprometido nada en esa particion.
        if offset is not None and offset >= 0:
            total += max(0, finales[tp] - offset)
    return total


def _muestrear_lag(admin, consumidor, parar, muestras):
    """Anota el lag cada medio segundo mientras `parar` no este activo.

    Hace falta muestrear DURANTE la inyeccion. Medir el lag solo al terminar da
    un numero enganoso: Logstash consume en paralelo, asi que para cuando el
    productor cierra ya ha drenado buena parte y el "pico" observado sale menor
    que el real -y a veces menor con 1000 eventos/s que con 500-.
    """
    while not parar.is_set():
        valor = _lag(admin, consumidor)
        if valor is not None:
            muestras.append((time.time(), valor))
        time.sleep(0.5)


def _inyectar(tasa_eps, duracion_s):
    """Publica a la tasa objetivo y devuelve (enviados, segundos, tasa real)."""
    productor = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=5,
        batch_size=32768,
    )
    inicio = time.time()
    enviados = 0
    # Ciclos de 100 ms: reparte la carga en vez de mandar una rafaga por segundo.
    # El `flush` va FUERA del bucle, al final: hacerlo en cada ciclo obliga a
    # esperar la confirmacion del broker antes de seguir y limita el throughput
    # justo en la prueba que quiere averiguar cual es el techo.
    while time.time() - inicio < duracion_s:
        ciclo = time.time()
        for _ in range(tasa_eps // 10):
            # Sin la latencia simulada: su `sleep` promedia 30 ms y toparia la
            # generacion en ~30 eventos/s, que es menos que la tasa mas baja que
            # se quiere probar. Con ella puesta esta prueba mide el `sleep`.
            evento = generar_trade(latencia_simulada=False)
            productor.send(TOPIC, key=evento["simbolo"], value=evento)
            enviados += 1
        resto = 0.1 - (time.time() - ciclo)
        if resto > 0:
            time.sleep(resto)
    productor.flush()
    productor.close()
    segundos = time.time() - inicio
    return enviados, segundos, enviados / segundos


def _drenar(admin, consumidor, lag_pico):
    """Espera a que el grupo se ponga al dia. Devuelve (segundos, lag_final)."""
    inicio = time.time()
    while time.time() - inicio < ESPERA_DRENAJE_S:
        actual = _lag(admin, consumidor)
        if actual is None or actual <= LAG_REPOSO:
            return time.time() - inicio, actual
        time.sleep(1)
    return None, _lag(admin, consumidor)


def main():
    print("=" * 78)
    print("PRUEBA DE CARGA DEL BUS DE EVENTOS")
    print("=" * 78)
    print()
    print(f"Broker   : {KAFKA_BROKER}")
    print(f"Topic    : {TOPIC}")
    print(f"Grupo    : {GRUPO}  (Logstash)")
    print(f"Duracion : {DURACION_S} s por tasa")
    print()

    try:
        admin = KafkaAdminClient(bootstrap_servers=[KAFKA_BROKER])
        consumidor = KafkaConsumer(bootstrap_servers=[KAFKA_BROKER])
    except Exception as error:
        print(f"No se pudo conectar a Kafka en {KAFKA_BROKER}: {error}")
        print("Levanta el entorno:  docker compose up -d zookeeper kafka")
        return 1

    if _lag(admin, consumidor) is None:
        print(f"El grupo '{GRUPO}' no tiene offsets en {TOPIC}.")
        print("Arranca Logstash y espera a que consuma:  docker compose up -d logstash")
        return 1

    filas = []
    for tasa in TASAS:
        reposo = _lag(admin, consumidor)
        print(f"--- {tasa} eventos/s " + "-" * (74 - len(str(tasa))))
        print(f"  lag en reposo      : {reposo}")

        muestras = []
        parar = threading.Event()
        vigilante = threading.Thread(
            target=_muestrear_lag, args=(admin, consumidor, parar, muestras), daemon=True
        )
        vigilante.start()
        enviados, segundos, real = _inyectar(tasa, DURACION_S)
        parar.set()
        vigilante.join(timeout=3)

        pico = max((v for _, v in muestras), default=0)
        print(f"  enviados           : {enviados} en {segundos:.1f} s")
        print(f"  tasa real          : {real:.0f} eventos/s", end="")
        print("  (el productor no alcanzo la tasa objetivo)" if real < tasa * 0.9 else "")
        print(f"  lag maximo         : {pico}   (muestreado cada 0,5 s durante la"
              f" inyeccion, {len(muestras)} muestras)")

        # Que el lag del final sea el mayor de la serie significa que seguia
        # subiendo cuando se corto: el consumidor no daba abasto.
        crecia = bool(muestras) and muestras[-1][1] >= pico

        drenaje, final = _drenar(admin, consumidor, pico)
        if drenaje is None:
            print(f"  drenaje            : NO se puso al dia en {ESPERA_DRENAJE_S} s"
                  f" (lag {final})")
            veredicto = "NO AGUANTA"
            consumo = None
        else:
            # Se drena lo acumulado en `drenaje` segundos. Es una cota INFERIOR
            # de la capacidad real: durante la inyeccion el consumidor ya venia
            # drenando, y eso no se cuenta aqui.
            consumo = (pico - (final or 0)) / drenaje if drenaje > 0 else float("inf")
            print(f"  drenaje            : {drenaje:.1f} s hasta lag {final}")
            print(f"  consumo (cota inf.): {consumo:.0f} eventos/s")
            veredicto = "AGUANTA"
            if crecia:
                veredicto += " (al limite: el lag aun subia al cortar)"
        print(f"  veredicto          : {veredicto}")
        print()
        filas.append((tasa, real, pico, drenaje, consumo, veredicto))

    print("=" * 78)
    print("RESUMEN")
    print("=" * 78)
    print(f"{'objetivo':>10} {'real':>10} {'lag max':>10} {'drenaje':>10} "
          f"{'consumo':>10}  veredicto")
    for tasa, real, pico, drenaje, consumo, veredicto in filas:
        d = f"{drenaje:.1f} s" if drenaje is not None else "-"
        c = f"{consumo:.0f}/s" if consumo else "-"
        print(f"{tasa:>10} {real:>10.0f} {pico:>10} {d:>10} {c:>10}  {veredicto}")

    print()
    print("Como leerlo. `AGUANTA` significa que el lag volvio a su nivel de reposo al")
    print("parar la inyeccion, es decir que el consumidor drena mas rapido de lo que se")
    print("le inyecta. El techo del pipeline es la tasa de consumo, no la de inyeccion:")
    print("un productor siempre puede empujar mas de lo que alguien es capaz de leer.")
    print()
    print("El consumidor medido es Logstash. Spark lleva sus offsets en el checkpoint y")
    print("no forma grupo de consumidores, asi que su retraso no aparece aqui.")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
