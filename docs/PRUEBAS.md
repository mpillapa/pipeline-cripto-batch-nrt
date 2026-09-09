# Pruebas y validación

Las doce pruebas del [plan](../PLAN.md#8-pruebas-y-validación), con el resultado real de
cada una y el comando para repetirla. Los números son de la corrida del **9 de septiembre
de 2026**, con el circuito completo levantado y el productor leyendo el mercado real.

> **Criterio para dar una prueba por buena.** Que exista un script no cuenta. Varias de
> estas pruebas pasaron meses «escritas» y al ejecutarlas por primera vez resultaron no
> comprobar nada: la de latencia restaba un campo consigo mismo y devolvía 0 ms, y la de
> carga medía la velocidad del productor contra sí misma. Una prueba que no puede fallar
> no es evidencia.

---

## 1. Resumen

| # | Prueba | Resultado | Dónde |
|---|---|---|---|
| P1 | Lógica del batch sin infraestructura | **42 de 42** | `prueba_logica_batch.py` |
| P2 | Idempotencia del batch | **Pasa** — 1 092 filas constantes en 3 lotes | MySQL |
| P3 | Dos lotes el mismo día | **Pasa** — sin colisión de claves | `control_lotes` |
| P4 | Deduplicación en NRT | **Pasa** — 40 y no 80 | `prueba_deduplicacion.py` |
| P5 | Recuperación ante fallo | **Pasa** — sin huecos ni duplicados | `prueba_recuperacion.py` |
| P6 | Eventos tardíos | **Pasa** — 20 s entra, 90 s se descarta | `prueba_eventos_tardios.py` |
| P7 | Datos malformados | **Pasa** — 5 tipos descartados sin ruido | `prueba_malformados.py` |
| P8 | Latencia p50/p95/p99 | **Medida** — tres etapas | `prueba_latencia.py` |
| P9 | Carga a 500/1 000/2 000 ev/s | **Aguanta las tres** | `prueba_carga.py` |
| P10 | Conciliación batch ↔ NRT | **Pasa** — 9 ventanas con veredicto | DAG 05 |
| P11 | Mapeo de campos en ES | **Pasa** — `double`, no `text` | Elasticsearch |
| P12 | Arranque desde cero | **Pendiente** — Día 7 del plan | — |
| — | Lógica del streaming | **14 de 14** | `prueba_logica_streaming.py` |
| — | Lógica de la conciliación | **30 de 30** | `prueba_conciliacion.py` |
| — | Traducción del WebSocket | **8 bloques** | `prueba_websocket.py` |

**P12 no se ha ejecutado a propósito.** Exige `docker compose down -v`, que borraría el
millón de trades indexados y las ventanas del mercado real de hoy — la evidencia de las
demás pruebas. El plan la sitúa en el Día 7, después de recoger las capturas, y ahí debe
quedarse.

---

## 2. Pruebas que no necesitan infraestructura

Corren en segundos porque las reglas de negocio son funciones puras.

```bash
python pruebas/prueba_logica_batch.py     # P1
python pruebas/prueba_conciliacion.py
python pruebas/prueba_websocket.py
```

### P1 · Lógica del batch — 42 de 42

Doce bloques: reglas de calidad R01–R08 con defectos inyectados, transformaciones,
tipos tras el viaje por Parquet, y que las columnas de la zona plata no diverjan de las
de MySQL.

**Lo que cubre y no se ve a simple vista:** que toda fila rechazada lleve su motivo, y que
un campo vacío que ha pasado por Parquet siga reconociéndose como vacío. Ese segundo caso
es un defecto real que se encontró ejecutando, no leyendo.

### Lógica de la conciliación — 30 comprobaciones

Con la respuesta de Elasticsearch simulada, así que no necesita el flujo NRT. Nueve
bloques, incluido el filtro por `origen_datos`: que se use `term` y no `match`, y que una
métrica sin el campo quede fuera.

### Traducción del WebSocket — 8 bloques

Los tres detalles del formato del exchange que rompen en silencio: precio y cantidad como
cadena, el símbolo en minúsculas en el canal y en mayúsculas en el campo, y `ts_evento`
que sale de `T` y no de `E`. Cada uno con su comprobación y el motivo al lado.

---

## 3. Pruebas del camino batch

### P2 y P3 · Idempotencia y dos lotes el mismo día

```bash
docker compose exec mysql mysql -ucripto -pcripto cripto \
  -e "SELECT lote_id, estado, filas_cargadas FROM control_lotes ORDER BY lote_id;
      SELECT COUNT(*) AS filas FROM hechos_ohlcv_diario;"
```

Tres lotes ejecutados, `hechos_ohlcv_diario` clavada en **1 092 filas**. La descarga trae
365 días hacia atrás en cada corrida, así que los lotes cubren los mismos días a
propósito: si la carga no fuera idempotente, el conteo crecería.

| Lote | Estado | Filas |
|---|---|---|
| L20260907_013936 | CARGADO | 1 092 |
| L20260907_014212 | CONCILIADO | 1 092 |
| L20260907_020115 | CONCILIADO | — |

> **Anomalía conocida.** El tercer lote figura como `CONCILIADO` con `filas_cargadas` en
> NULL. No afecta a los datos —las 1 092 filas están— pero la bitácora no debería quedar a
> medias. Sin resolver.

### P10 · Conciliación batch ↔ NRT

```bash
docker compose exec mysql mysql -ucripto -pcripto cripto \
  -e "SELECT simbolo, fecha_hora, desviacion_pct, cobertura_pct, veredicto
      FROM conciliacion ORDER BY fecha_hora, simbolo;"
```

Nueve ventanas conciliadas. El resultado que **valida el diseño de la métrica**:

| Hora | Símbolo | Desviación | Cobertura | Veredicto |
|---|---|---|---|---|
| 00:00 | BTCUSDT | −20,03 % | 8,08 % | DESVIADO |
| 01:00 | BTCUSDT | **−0,064 %** | 44,98 % | DESVIADO |
| 01:00 | ETHUSDT | **−0,038 %** | 44,91 % | DESVIADO |
| 02:00 | BTCUSDT | 0,086 % | 100 % | COINCIDE |
| 02:00 | ETHUSDT | 0,069 % | 100 % | COINCIDE |

Las de las 01:00 tienen una desviación **diez veces menor que el umbral** y aun así salen
`DESVIADO`: la cobertura era del 45 % porque el productor real llevaba corriendo media
hora. Eso es lo correcto, no un fallo. La métrica dice dos cosas a la vez y las separa
—*el precio coincide* y *no escuché la hora entera*— y un solo número no podría.

La comparación entre la hora del simulador (−20 %) y la del exchange (−0,064 %), con el
mismo código a ambos lados, es la mejor evidencia de que la conciliación mide algo real.

> **Discrepancia encontrada y resuelta.** MySQL tenía las 9 filas y Elasticsearch solo 6.
> No era cosa de Logstash estando caído: el DAG exportaba siempre al mismo
> `conciliacion.ndjson`, y el input `file` de Logstash va en modo `tail` recordando por
> inodo hasta dónde leyó. Al sobrescribir el archivo, retomaba desde el desplazamiento
> anterior en lugar de leer el contenido nuevo entero — **el DAG exportaba N filas y
> Elasticsearch recibía menos, sin un solo error por medio**.
>
> Arreglado en los DAG 04 y 05: cada corrida escribe un archivo con marca de tiempo en el
> nombre, así Logstash lo trata como archivo nuevo. Las 9 filas están ahora indexadas.

---

## 4. Pruebas del camino NRT

Todas necesitan el circuito levantado. Corren dentro del contenedor del productor, que
es donde están `kafka-python` y el simulador.

### P4 · Deduplicación — pasa

```bash
docker compose run --rm --no-deps -e CRIPTO_KAFKA_EXTERNO=kafka:29092 \
  --entrypoint python productor -u /pruebas/prueba_deduplicacion.py
```

Se publican 40 trades y después **los mismos 40 otra vez**, en la misma ventana.
Resultado: `n_trades = 40`, `volumen_base = 80`.

**El detalle que conviene contar en la exposición:** el VWAP habría salido correcto
incluso sin deduplicar, porque duplicar todos los trades por igual no cambia una media
ponderada. Los que delatan el problema son `n_trades` y `volumen_base` — que son
exactamente los dos campos con los que la conciliación calcula la cobertura.

### P5 · Recuperación ante fallo — pasa

```bash
python pruebas/prueba_recuperacion.py
```

Reinicia `spark-streaming` a mitad de flujo y comprueba tres cosas sobre la serie:

- **12 ventanas nuevas** tras el reinicio: el job volvió a trabajar.
- **Ninguna ventana anterior cambió de contenido**: no recalculó lo ya emitido.
- **Sin huecos entre 15:10 y 15:14**, con el reinicio a las 15:12:57.

> Esta prueba **no podía pasar hasta el 9 de septiembre**, y no por el código. El
> checkpoint se montaba en `./datos/checkpoints`, y `airflow-init` hace `chown -R 50000:0`
> sobre todo `datos/`. Spark corre como uid 185, así que tras cada inicialización de
> Airflow se quedaba sin permiso de escritura sobre su propio estado: moría en bucle con
> `Permission denied` —59 reinicios acumulados— sin emitir una sola ventana. El checkpoint
> vive ahora en un volumen nombrado.

### P6 · Eventos tardíos — pasa

```bash
docker compose run --rm --no-deps -e CRIPTO_KAFKA_EXTERNO=kafka:29092 \
  -e CRIPTO_ES=http://elasticsearch:9200 \
  --entrypoint python productor -u /pruebas/prueba_eventos_tardios.py
```

Con watermark de 30 s:

| Tanda | `ts_evento` | Resultado |
|---|---|---|
| A tiempo | hace 20 s | **Entró** en su ventana, 30 trades |
| Tarde | hace 90 s | **Descartada**, no se creó la ventana |

El descarte es el precio declarado de acotar el estado. Ampliar el watermark recuperaría
más eventos tardíos a cambio de más memoria **y de emitir todas las ventanas más tarde**,
porque el margen se suma a la latencia de todas, no solo de las que traen retraso.

### P7 · Datos malformados — pasa

```bash
docker compose run --rm --no-deps -e CRIPTO_KAFKA_EXTERNO=kafka:29092 \
  -e CRIPTO_ES=http://elasticsearch:9200 \
  --entrypoint python productor -u /pruebas/prueba_malformados.py
```

Cinco formas de estar mal, intercaladas entre 20 trades válidos: JSON inválido, JSON que
no es un objeto, campos obligatorios ausentes, `precio` como texto y `ts_evento` con
formato inválido. Resultado: `n_trades = 20`, `volumen_base = 60`, `vwap = 10,00`.

**Por qué importa que estén intercalados y no al final:** si el job muriera con el primer
mensaje corrupto, los válidos posteriores no llegarían y el conteo lo delataría. Un stream
que muere con un JSON inválido además se queda muerto: al reiniciar vuelve al mismo
offset, encuentra el mismo mensaje y vuelve a caer.

Los mensajes que Logstash no puede parsear tampoco se pierden: acaban en
`cripto-desconocido-*` con las etiquetas `_jsonparsefailure` y `sin_tipo_fuente`. Hoy hay
tres documentos ahí, y el más antiguo —un `"}`, un JSON truncado— es de una corrida
anterior. Descartarlos los haría invisibles, y el síntoma sería «faltan datos» sin pista.

### P8 · Latencia — medida

```bash
python pruebas/prueba_latencia.py
```

| Etapa | Qué mide | p50 | p95 | p99 |
|---|---|---|---|---|
| 1 | Exchange → productor (`ts_ingesta − ts_evento`) | 1 185 ms | 1 400 ms | 1 406 ms |
| 3 | Productor → Logstash (`ts_indexado − ts_ingesta`) | **162 ms** | 532 ms | 598 ms |
| 2 | Cierre de ventana → métrica (`ts_procesado − ventana_fin`) | 84,2 s | 112,4 s | 143,7 s |

**Cómo leer la etapa 1.** Las dos marcas vienen de relojes distintos: `ts_evento` lo pone
el exchange y `ts_ingesta` el contenedor. Lo que se mide es latencia *más* desfase entre
relojes. La prueba reporta una columna `neg` con el porcentaje de casos en que
`ts_ingesta` salió *anterior* a `ts_evento` —imposible como latencia—: si es 0 %, los
relojes van alineados y la mediana es fiable. En esta corrida fue 0 %.

**La etapa 3 sí es fiable**, porque compara dos relojes del mismo host.

**Los 84 s de la etapa 2 no son un atasco, son la suma esperada:** 30 s de watermark, más
30 s porque el watermark de Spark va un micro-batch por detrás, más 0–30 s hasta el
siguiente disparo del trigger. Total esperado 60–90 s, y es lo que se observa. El
parámetro con más efecto para bajarlo es `CRIPTO_INTERVALO_LOTE`, que interviene en dos
de los tres sumandos.

> **Esta prueba medía 0,00 ms en los tres percentiles hasta que se reescribió.** Restaba
> `@timestamp` menos `ts_evento`, y Logstash *deriva* `@timestamp` de `ts_evento`: la
> resta era cero por construcción, con cualquier volumen y en cualquier máquina. Para
> medir la etapa 3 hubo que añadir un sello propio (`ts_indexado`) en el filtro de
> Logstash.

### P9 · Carga — aguanta las tres tasas

```bash
docker compose stop productor    # el productor de fondo contamina la medida
docker compose run --rm --no-deps -e CRIPTO_KAFKA_EXTERNO=kafka:29092 \
  --entrypoint python productor -u /pruebas/prueba_carga.py
docker compose up -d productor
```

| Objetivo | Tasa real | Lag máximo | Drenaje | Consumo (cota inf.) | Veredicto |
|---|---|---|---|---|---|
| 500/s | 499/s | 2 400 | 3,0 s | 797/s | AGUANTA |
| 1 000/s | 999/s | 4 900 | 5,0 s | 977/s | AGUANTA |
| 2 000/s | 1 998/s | 10 000 | 4,0 s | 2 493/s | AGUANTA |

**El criterio no es la tasa de inyección sino el lag del consumidor.** Un productor
siempre puede empujar más de lo que alguien es capaz de leer; el techo del pipeline es la
tasa de consumo. «Aguanta» significa que el lag vuelve a cero al parar la inyección: en el
peor caso, 20 000 mensajes acumulados se drenaron en 4 segundos.

El lag se muestrea **durante** la inyección, cada medio segundo. Medirlo solo al terminar
da un pico falso —Logstash consume en paralelo— y llegaba a salir menor con 1 000 ev/s que
con 500.

Dos cosas hubo que arreglar para que esta prueba midiera algo:

- La versión anterior reportaba la tasa que el productor conseguía enviar, sin mirar al
  consumidor. Medía el productor contra sí mismo.
- El `sleep` de 10–50 ms que el simulador incluye para imitar latencia de red **topaba la
  generación en unos 30 eventos/s**. Con él puesto, la prueba concluía que Kafka no pasa
  de 34 ev/s. Ahora `generar_trade(latencia_simulada=False)` lo desactiva.

El consumidor medido es Logstash. Spark lleva sus offsets en el checkpoint y no forma
grupo de consumidores, así que su retraso no aparece aquí.

---

## 5. Pruebas de la lógica del streaming

```bash
docker compose run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit \
  spark-streaming --master "local[2]" /opt/spark/pruebas/prueba_logica_streaming.py
```

**14 de 14.** Corre con `spark-submit` y no con `python3`: pyspark vive en
`/opt/spark/python` y solo `spark-submit` lo pone en el `PYTHONPATH`.

Cubre la aritmética que se ejecuta en producción —llama a la misma `calcular_metricas()`
del job, no a una copia— y las reglas de alerta:

- VWAP ponderado por cantidad, no promedio simple.
- `volatilidad_pct` como rango relativo, no desviación típica.
- OHLC por orden temporal (`min_by`/`max_by`), no por orden de llegada.
- `ventana_inicio` inclusivo y `ventana_fin` exclusivo.
- `origen_datos` distingue la fuente, que es de lo que depende la regla C01.
- **Que la regla de alerta escrita en Python puro y la escrita con `when()` de Spark
  coincidan en los bordes.** La lógica está duplicada a propósito —una UDF por fila sería
  lenta en un stream— y estas comprobaciones vigilan que las dos copias no se separen.

> Esta prueba tampoco era ejecutable hasta el 9 de septiembre, y el motivo registrado
> —«importa pyspark, que no está instalado»— era correcto pero incompleto: dentro del
> contenedor tampoco corría, porque `pruebas/` no estaba montado. El único sitio donde
> podía ejecutarse era el único donde no existía.

---

## 6. P11 · Mapeo de campos en Elasticsearch

```bash
curl -s "localhost:9200/cripto-nrt_metrica-*/_mapping?filter_path=**.properties.vwap,**.properties.volumen_usdt"
curl -s "localhost:9200/cripto-nrt_trade-*/_mapping?filter_path=**.properties.precio"
```

`precio`, `cantidad`, `vwap` y `volumen_usdt` son `double`; `volatilidad_pct` es `float`.
Ninguno cayó como `text`, que es el riesgo que la plantilla existe para evitar: una
agregación sobre `text` **falla sin error visible** y el panel sale vacío.

**Dos defectos reales encontrados al construir el tablero**, ambos del mismo tipo:

1. `origen` había quedado como `text` en el índice del día, creado antes de que la
   plantilla lo declarara. Se reindexó: 998 982 documentos, 0 fallos, y el reparto por
   fuente se conserva (`exchange_ws` 894 533, `simulador` 104 449).
2. **La plantilla solo cubría los campos del flujo NRT.** Todo el camino batch se mapeaba
   dinámicamente, y por eso `veredicto` era `text` y el panel de conciliación no podía
   agrupar por él. Se añadieron 20 campos del batch y 7 de las alertas: la plantilla pasó
   de 25 a 52.

---

## 7. Cómo repetirlo todo

Con el circuito levantado según la [sección 5 del README](../README.md#5-levantar-el-entorno):

```bash
# Sin infraestructura
python pruebas/prueba_logica_batch.py
python pruebas/prueba_conciliacion.py
python pruebas/prueba_websocket.py

# Con el entorno arriba
python pruebas/prueba_latencia.py
python pruebas/prueba_recuperacion.py

docker compose run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit \
  spark-streaming --master "local[2]" /opt/spark/pruebas/prueba_logica_streaming.py

for p in deduplicacion eventos_tardios malformados; do
  docker compose run --rm --no-deps -e CRIPTO_KAFKA_EXTERNO=kafka:29092 \
    -e CRIPTO_ES=http://elasticsearch:9200 \
    --entrypoint python productor -u /pruebas/prueba_$p.py
done
```

Las tres últimas publican con símbolos propios (`TESTDEDUP`, `TESTLATE`, `TESTMAL`) para
no contaminar las series reales. Para limpiarlos después:

```bash
for s in TESTDEDUP TESTLATE TESTMAL; do
  curl -s -X POST "localhost:9200/cripto-nrt_*/_delete_by_query?refresh=true" \
    -H 'Content-Type: application/json' -d "{\"query\":{\"term\":{\"simbolo\":\"$s\"}}}"
done
```
