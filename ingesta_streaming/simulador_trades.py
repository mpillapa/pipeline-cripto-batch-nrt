import json
import time
import uuid
import random
from datetime import datetime, timezone

# Configuración base
SIMBOLOS = {
    "BTCUSDT": {"precio_base": 63000.0, "vol_rango": (0.001, 0.05)},
    "ETHUSDT": {"precio_base": 3400.0, "vol_rango": (0.05, 1.5)},
    "SOLUSDT": {"precio_base": 145.0, "vol_rango": (1.0, 15.0)}
}

# Contador de id_trade POR SIMBOLO.
#
# Antes era random.randint(), y eso tenia dos problemas. El contrato lo define
# como entero y salia como texto. Y sobre todo: al ser aleatorio no se repetia
# nunca, asi que la deduplicacion del job de Spark no se podia probar. La
# prueba P4 consiste precisamente en reenviar 1000 trades ya procesados y
# comprobar que las metricas no cambian; con ids aleatorios eso es imposible.
#
# Con un contador, reenviar es tan facil como reiniciar el contador.
_CONTADOR = {simbolo: 0 for simbolo in SIMBOLOS}


def siguiente_id_trade(simbolo):
    """Devuelve un id_trade unico y creciente dentro del simbolo.

    El id_trade es unico POR SIMBOLO en el exchange, no globalmente, y el job de
    Spark deduplica por (simbolo, id_trade). Un contador por simbolo reproduce
    esa semantica.
    """
    _CONTADOR[simbolo] += 1
    return _CONTADOR[simbolo]


def reiniciar_contadores(desde=0):
    """Vuelve a empezar la numeracion. Sirve para la prueba de deduplicacion.

    Reiniciando y volviendo a publicar se generan trades con ids ya vistos, que
    es exactamente lo que el job debe descartar.
    """
    for simbolo in _CONTADOR:
        _CONTADOR[simbolo] = desde

def generar_trade():
    """Genera un evento de trade simulado cumpliendo el contrato de datos."""
    simbolo = random.choice(list(SIMBOLOS.keys()))
    configs = SIMBOLOS[simbolo]
    
    # Simulamos una ligera variación de precio
    variacion = random.uniform(-0.002, 0.002) 
    precio = round(configs["precio_base"] * (1 + variacion), 2)
    cantidad = round(random.uniform(*configs["vol_rango"]), 4)
    importe_usdt = round(precio * cantidad, 2)
    
    ahora_utc = datetime.now(timezone.utc)
    ts_evento = ahora_utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    
    # Simulamos un ligero delay de red (latencia de ingesta)
    time.sleep(random.uniform(0.01, 0.05))
    ts_ingesta = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    trade = {
        "id_evento": str(uuid.uuid4()),
        "tipo_fuente": "nrt_trade",
        "simbolo": simbolo,
        # Entero, no cadena: lo exige el contrato y es la clave de dedup.
        "id_trade": siguiente_id_trade(simbolo),
        "precio": precio,
        "cantidad": cantidad,
        "importe_usdt": importe_usdt,
        "comprador_es_maker": random.choice([True, False]),
        "ts_evento": ts_evento,
        "ts_ingesta": ts_ingesta,
        "origen": "simulador"
    }
    
    return trade

if __name__ == "__main__":
    print("Iniciando simulador de trades (Ctrl+C para detener)...")
    try:
        while True:
            evento = generar_trade()
            # Manuel recomienda: "escribe a consola primero, sin Kafka. Valida el esquema a ojo"
            print(json.dumps(evento))
            time.sleep(random.uniform(0.2, 1.0)) # Tasa configurable
    except KeyboardInterrupt:
        print("\nSimulador detenido.")