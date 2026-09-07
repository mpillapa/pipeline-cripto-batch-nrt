import json
import time
from kafka import KafkaProducer
from simulador_trades import generar_trade

# Kafka broker corriendo en el puerto 9095 
KAFKA_BROKER = 'localhost:9095'
TOPIC = 'trades.crudo'

def crear_productor():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8')
    )

if __name__ == '__main__':
    print(f"Conectando a Kafka en {KAFKA_BROKER}...")
    try:
        productor = crear_productor()
        print(f"Conexión exitosa. Publicando en el topic '{TOPIC}' (Ctrl+C para detener)")
        
        while True:
            # Reutilizamos la función de tu simulador
            evento = generar_trade()
            simbolo = evento['simbolo']
            
            # Publicar en Kafka usando el símbolo como partition key
            productor.send(TOPIC, key=simbolo, value=evento)
            productor.flush()
            
            print(f"-> Enviado a Kafka: {simbolo} | Precio: {evento['precio']} | Importe: {evento['importe_usdt']}")
            time.sleep(0.5)
            
    except Exception as e:
        print(f"Error de conexión: {e}")
    except KeyboardInterrupt:
        print("\nProductor detenido manualmente.")
    finally:
        if 'productor' in locals():
            productor.close()