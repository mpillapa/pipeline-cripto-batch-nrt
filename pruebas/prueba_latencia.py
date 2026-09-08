import requests
from datetime import datetime
import numpy as np

ES_URL = "http://localhost:9200"
INDEX = "cripto-nrt_trade-*"

def calcular_latencia():
    print(f"Consultando Elasticsearch en {ES_URL}/{INDEX}...")
    query = {
        "size": 1000,
        "sort": [{"@timestamp": {"order": "desc"}}],
        "_source": ["ts_evento", "@timestamp", "ts_ingesta"]
    }
    
    try:
        res = requests.get(f"{ES_URL}/{INDEX}/_search", json=query)
        if res.status_code == 404:
            print("El índice no existe aún. Asegúrate de que Logstash esté indexando datos.")
            return
        res.raise_for_status()
        
        hits = res.json().get('hits', {}).get('hits', [])
        latencias_totales = []
        
        for hit in hits:
            source = hit['_source']
            if 'ts_evento' in source and '@timestamp' in source:
                try:
                    # Remplazamos la Z por +00:00 para parsear correctamente el ISO 8601
                    ts_evento = datetime.fromisoformat(source['ts_evento'].replace('Z', '+00:00'))
                    ts_es = datetime.fromisoformat(source['@timestamp'].replace('Z', '+00:00'))
                    
                    # Latencia total: desde que ocurrió el evento hasta que llegó a Elasticsearch
                    diff_ms = (ts_es - ts_evento).total_seconds() * 1000
                    
                    # Filtrar posibles valores negativos o irreales por desincronización de relojes
                    if diff_ms >= 0:
                        latencias_totales.append(diff_ms)
                except Exception as e:
                    continue
        
        if latencias_totales:
            print(f"\n--- Resultados de Latencia (Evento -> Elasticsearch) sobre {len(latencias_totales)} registros ---")
            print(f"p50 (Mediana) : {np.percentile(latencias_totales, 50):.2f} ms")
            print(f"p95           : {np.percentile(latencias_totales, 95):.2f} ms")
            print(f"p99           : {np.percentile(latencias_totales, 99):.2f} ms")
        else:
            print("No se encontraron registros válidos para calcular la latencia. Revisa los formatos de fecha.")
            
    except requests.exceptions.ConnectionError:
        print("Error: No se pudo conectar a Elasticsearch. ¿Está corriendo en el puerto 9200?")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")

if __name__ == "__main__":
    calcular_latencia()
