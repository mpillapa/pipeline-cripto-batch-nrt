import time
import json
import threading
import sys
import os
from kafka import KafkaProducer

# Agregar el directorio de ingesta al path para importar el simulador
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'ingesta_streaming'))
from simulador_trades import generar_trade

KAFKA_BROKER = 'localhost:9095'
TOPIC = 'trades.crudo'

def productor_carga(tasa_objetivo_eps, duracion_segundos):
    try:
        productor = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8'),
            linger_ms=5,        # Optimización para alto throughput
            batch_size=32768    # Optimización para alto throughput
        )
    except Exception as e:
        print(f"Error conectando a Kafka: {e}")
        return

    inicio = time.time()
    eventos_enviados = 0
    
    print(f"\n[Prueba de Carga] Iniciando inyección a {tasa_objetivo_eps} eventos/s por {duracion_segundos} s...")
    
    # Bucle para mantener la inyección durante la duración deseada
    while time.time() - inicio < duracion_segundos:
        ciclo_inicio = time.time()
        
        # Enviar la fracción correspondiente de eventos en este ciclo de 100ms
        eventos_a_enviar = int(tasa_objetivo_eps / 10) 
        for _ in range(eventos_a_enviar):
            evento = generar_trade()
            productor.send(TOPIC, key=evento['simbolo'], value=evento)
            eventos_enviados += 1
            
        productor.flush()
        
        # Dormir lo necesario para mantener el ritmo (aproximadamente 100ms por ciclo)
        tiempo_tomado = time.time() - ciclo_inicio
        if tiempo_tomado < 0.1:
            time.sleep(0.1 - tiempo_tomado)
            
    productor.close()
    tiempo_total = time.time() - inicio
    print(f"-> Prueba finalizada. Se enviaron {eventos_enviados} eventos en {tiempo_total:.2f} s.")
    print(f"-> Tasa real procesada: {eventos_enviados / tiempo_total:.2f} EPS (Eventos Por Segundo)")

if __name__ == "__main__":
    print("=== Suite de Pruebas de Carga - Pipeline NRT ===")
    productor_carga(tasa_objetivo_eps=500, duracion_segundos=5)
    productor_carga(tasa_objetivo_eps=1000, duracion_segundos=5)
    productor_carga(tasa_objetivo_eps=2000, duracion_segundos=5)
