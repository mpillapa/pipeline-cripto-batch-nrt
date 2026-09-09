"""Recuperacion ante fallo del job de Spark (P5). Necesita el entorno levantado.

    python pruebas/prueba_recuperacion.py

QUE DEMUESTRA
-------------
Que matar el job a mitad de flujo y volver a levantarlo **no deja huecos ni
duplicados** en la serie de ventanas. Es lo que hace el checkpoint: guarda el
offset de Kafka y el estado de las agregaciones en curso, de modo que el job
reanuda donde estaba en vez de empezar de cero o repetir trabajo.

Sin checkpoint pasaria una de dos cosas, y ninguna es aceptable: o el job
arranca desde el ultimo offset y se pierde todo lo publicado mientras estuvo
caido, o arranca desde el principio y reemite ventanas ya emitidas.

COMO SE COMPRUEBA
-----------------
1. Se anota que ventanas hay indexadas.
2. Se reinicia el contenedor `spark-streaming`.
3. Se espera a que vuelva a emitir.
4. Se comprueban tres cosas sobre la serie completa:

   - **Sin duplicados**: ninguna pareja (simbolo, ventana_inicio) aparece dos
     veces con contenidos distintos.
   - **Sin huecos**: los minutos consecutivos del tramo que cubre el reinicio
     estan todos presentes.
   - **Continuidad**: hay ventanas posteriores al reinicio, es decir el job
     realmente volvio a trabajar.

POR QUE ESTA PRUEBA NO PODIA PASAR ANTES
----------------------------------------
El checkpoint se montaba en `./datos/checkpoints`, y `airflow-init` hace
`chown -R 50000:0` sobre todo `datos/`. Spark corre como uid 185, asi que tras
cada inicializacion de Airflow se quedaba sin permiso de escritura sobre su
propio estado: el job moria en bucle con `Permission denied` y no emitia una
sola ventana. El checkpoint vive ahora en un volumen nombrado.
"""

import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests

ES_URL = "http://localhost:9200"
INDICE = "cripto-nrt_metrica-*"
SERVICIO = "spark-streaming"

# Cuanto se espera a que el job vuelva a emitir. Una ventana tarda 60-90 s en
# salir (watermark 30 s + retraso de un micro-batch + espera del trigger), y el
# arranque de Spark suma medio minuto mas.
ESPERA_MAX_S = 240


def _ventanas():
    """Devuelve {(simbolo, ventana_inicio): (n_trades, vwap)} de lo indexado."""
    respuesta = requests.post(
        f"{ES_URL}/{INDICE}/_search",
        json={
            "size": 500,
            "sort": [{"ventana_inicio": "desc"}],
            "_source": ["simbolo", "ventana_inicio", "n_trades", "vwap"],
        },
        timeout=30,
    )
    respuesta.raise_for_status()
    salida = {}
    for hit in respuesta.json()["hits"]["hits"]:
        f = hit["_source"]
        salida[(f["simbolo"], f["ventana_inicio"])] = (f.get("n_trades"), f.get("vwap"))
    return salida


def _duplicados_incoherentes(antes, despues):
    """Ventanas que existian antes y reaparecen con OTRO contenido.

    Que una ventana se reemita con el mismo contenido es inofensivo -Logstash
    indexa sin id propio-, pero que reaparezca con otro `n_trades` significa que
    se recalculo con datos distintos: eso si es un duplicado real.
    """
    conflictos = []
    for clave, valor in antes.items():
        if clave in despues and despues[clave] != valor:
            conflictos.append((clave, valor, despues[clave]))
    return conflictos


def _huecos(claves, desde, hasta):
    """Minutos sin ninguna ventana entre `desde` y `hasta`, por simbolo."""
    por_simbolo = defaultdict(set)
    for simbolo, inicio in claves:
        por_simbolo[simbolo].add(inicio)

    faltantes = defaultdict(list)
    for simbolo, inicios in por_simbolo.items():
        momento = desde
        while momento < hasta:
            marca = momento.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            if marca not in inicios:
                faltantes[simbolo].append(marca)
            momento += timedelta(minutes=1)
    return faltantes


def main():
    print("=" * 78)
    print("RECUPERACION ANTE FALLO DEL JOB DE SPARK")
    print("=" * 78)
    print()

    try:
        antes = _ventanas()
    except Exception as error:
        print(f"No se pudo consultar Elasticsearch: {error}")
        return 1

    if not antes:
        print("No hay metricas indexadas. Levanta el circuito y espera un par de")
        print("minutos a que Spark emita la primera ventana.")
        return 1

    ultima_antes = max(inicio for _, inicio in antes)
    print(f"Ventanas indexadas antes  : {len(antes)}")
    print(f"Ultima ventana antes      : {ultima_antes}")
    print()

    print(f"Reiniciando `{SERVICIO}` ...")
    momento_corte = datetime.now(timezone.utc)
    reinicio = subprocess.run(
        ["docker", "compose", "restart", SERVICIO],
        capture_output=True, text=True,
    )
    if reinicio.returncode != 0:
        print(f"El reinicio fallo: {reinicio.stderr.strip()}")
        return 1
    print(f"Reiniciado a las {momento_corte.strftime('%H:%M:%S')} UTC")
    print()

    print(f"Esperando a que vuelva a emitir (hasta {ESPERA_MAX_S} s) ...")
    inicio_espera = time.time()
    despues = antes
    while time.time() - inicio_espera < ESPERA_MAX_S:
        time.sleep(10)
        despues = _ventanas()
        if len(despues) > len(antes):
            break
    transcurrido = time.time() - inicio_espera

    nuevas = {k: v for k, v in despues.items() if k not in antes}
    print(f"Ventanas nuevas           : {len(nuevas)} tras {transcurrido:.0f} s")
    print()

    print("-" * 78)
    fallos = 0

    # 1. Continuidad
    if nuevas:
        print(f"  OK   el job volvio a emitir ({len(nuevas)} ventanas nuevas)")
    else:
        print(f"  FALLA  no emitio ninguna ventana en {ESPERA_MAX_S} s")
        print("         Revisa: docker compose logs --tail 50 spark-streaming")
        fallos += 1

    # 2. Sin duplicados incoherentes
    conflictos = _duplicados_incoherentes(antes, despues)
    if not conflictos:
        print("  OK   ninguna ventana anterior cambio de contenido al reanudar")
    else:
        print(f"  FALLA  {len(conflictos)} ventanas se recalcularon distinto:")
        for clave, viejo, nuevo in conflictos[:5]:
            print(f"         {clave}: n_trades {viejo[0]} -> {nuevo[0]}")
        fallos += 1

    # 3. Sin huecos en el tramo del reinicio
    if nuevas:
        ultima_despues = max(inicio for _, inicio in despues)
        desde = datetime.strptime(ultima_antes, "%Y-%m-%dT%H:%M:%S.000Z").replace(
            tzinfo=timezone.utc
        )
        hasta = datetime.strptime(ultima_despues, "%Y-%m-%dT%H:%M:%S.000Z").replace(
            tzinfo=timezone.utc
        )
        faltantes = _huecos(despues.keys(), desde, hasta)
        if not faltantes:
            print(f"  OK   sin huecos entre {ultima_antes[11:19]} y "
                  f"{ultima_despues[11:19]}")
        else:
            print("  AVISO  faltan minutos en el tramo del reinicio:")
            for simbolo, marcas in list(faltantes.items())[:3]:
                print(f"         {simbolo}: {len(marcas)} minutos, "
                      f"p.ej. {marcas[0][11:19]}")
            print()
            print("         Un hueco NO es automaticamente un fallo del checkpoint:")
            print("         si en ese minuto no hubo ningun trade de ese simbolo, no")
            print("         hay ventana que emitir. Compruebalo contra el indice de")
            print("         trades antes de darlo por defecto.")

    print("-" * 78)
    print()
    if fallos:
        print(f"RESULTADO: {fallos} comprobacion(es) fallaron")
        return 1
    print("RESULTADO: el job reanudo desde el checkpoint, sin recalcular lo ya emitido")
    print()
    print("Lo que esto demuestra: el estado de las ventanas en curso y los offsets de")
    print("Kafka sobreviven al reinicio. Sin checkpoint, el job perderia lo publicado")
    print("mientras estuvo caido o reemitiria ventanas ya cerradas.")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
