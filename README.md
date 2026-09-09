# Pipeline batch + near real-time de mercado cripto

**Trabajo final · Ingeniería de Datos · Universidad San Francisco de Quito**
Estéfano Galarza · Manuel Pillapa · Septiembre 2026

Pipeline de datos con **dos flujos sobre el mismo dominio** —uno batch y uno near
real-time— que además se **concilian entre sí**: se mide con un número si lo que el flujo
rápido reportó en vivo coincide con lo que el flujo lento confirma después.

> **Alcance.** Prototipo exploratorio académico. Las fuentes son APIs públicas gratuitas
> o datos simulados. No hay conexión con ningún sistema corporativo ni se reproducen sus
> estructuras o reglas. Las credenciales del entorno son ficticias y solo válidas en
> local: sin TLS, sin gestión de secretos, sin respaldos.

---

## Estado

Este README describe el proyecto completo. El estado real, entregable por entregable, está
en **[AVANCE.md](AVANCE.md)**.

| Parte | Estado |
|---|---|
| Camino batch — módulo `comun/` y DAGs 01–05 | Ejecutado de punta a punta, con idempotencia verificada |
| Camino near real-time | Funcionando. Más de un millón de trades del exchange real indexados |
| Entorno `docker compose` | Completo, con plantillas y tablero provisionados al arrancar |
| Conciliación (DAG 05) | 9 ventanas conciliadas contra el mercado real |
| Alertas | Generadas en Spark hacia `alertas.precio`, como fija el contrato |
| Tablero de Kibana | 7 paneles, se importa solo al levantar el entorno |
| Pruebas | 11 de las 12 del plan, ejecutadas y documentadas |
| Pendiente | Capturas, vídeo de respaldo, P12 (arranque en limpio) y los dos ensayos |

**Lo que se puede ejecutar sin levantar nada** está en la sección [Probar la lógica sin
infraestructura](#4-probar-la-lógica-sin-infraestructura); para el circuito completo, ver
[Levantar el entorno](#5-levantar-el-entorno).

---

## 1. El problema

Un operador de mesa necesita dos cosas incompatibles en un solo flujo:

- **Ahora mismo:** qué pasa con el precio en los últimos segundos, con latencia de pocos
  segundos, para reaccionar a movimientos bruscos.
- **Con perspectiva:** cómo se comporta el activo en semanas, con series completas y
  consistentes, para decidir posiciones.

El primero exige un flujo que sacrifica completitud por latencia. El segundo, uno que
sacrifica latencia por exactitud. El proyecto implementa ambos y los compara.

---

## 2. Arquitectura

```
  FUENTES                    BUS / INGESTA          PROCESAMIENTO NRT           ALMACENAMIENTO      SERVICIO
──────────────────────────────────────────────────────────────────────────────────────────────────────────

 WebSocket trades ─┐
                   ├─→ productor ─→ Kafka                                                        ┌→ KIBANA
 Simulador ────────┘   (clave =     trades.crudo ──→ Spark Structured Streaming                  │  índice
                        símbolo)          │          · valida esquema                            │  cripto-*
                                          │          · dedup por id_trade                        │
                                          │          · ventana 1 min + watermark 30 s            │
                                          │          · VWAP, OHLC, volatilidad                   │
                                          │                     │                                │
                                          │          ┌──────────┴──────────┐                     │
                                          │          ▼                     ▼                     │
                                          │   Kafka metricas.1min   Kafka alertas.precio         │
                                          └──────────┴─────────┬───────────┘                     │
                                                               │                                 │
 HTTP POST :8088 (control) ────────────────────────────────────┤                                 │
 TCP :5000 (logs) ─────────────────────────────────────────────┼→ LOGSTASH → ELASTICSEARCH ──────┤
 http_poller :60s (referencia) ────────────────────────────────┤   (único escritor hacia ES)     │
                                                               │                                 │
 API REST klines ─┐                                            │                                 │
 CSV catálogo ────┴→ AIRFLOW → bronce → plata → MySQL ─ NDJSON ─┘                                 │
                     DAG 01-04  (Parquet)   (dim + hechos)                                        │
                                                     │                                            │
                     AIRFLOW DAG 05 ─ conciliación ──┴─ consulta agregada a ES + lectura de MySQL ┘
```

### Decisiones y su porqué

| Decisión | Motivo |
|---|---|
| **Kafka como bus compartido** | Desacopla ingesta de procesamiento, permite reprocesar desde un offset y absorbe picos. Sin él, Spark y Logstash competirían por el mismo WebSocket |
| **Spark escribe a Kafka, no a Elasticsearch ni a MySQL** | Elimina el conector Elasticsearch-Spark y el driver JDBC, las dos dependencias más frágiles. El job queda como fuente Kafka → agregación → destino Kafka |
| **Logstash como único escritor hacia Elasticsearch** | Un solo lugar donde se define el mapeo de campos hacia ES |
| **MySQL para el modelo dimensional, Elasticsearch para la línea de tiempo** | Cada almacén hace lo que sabe hacer: MySQL da claves foráneas e idempotencia; Elasticsearch da series temporales y búsqueda |
| **Airflow con LocalExecutor** | Elimina Redis, worker y triggerer. Con Elasticsearch, Kafka y Spark en la misma máquina, el recurso escaso es la RAM |
| **Spark en `local[2]`, un solo contenedor** | Ahorra ~2 GB frente a master + worker. La UI de la aplicación sigue en el puerto 4040 |
| **Parquet en bronce y plata** | Columnar, comprimido y **lleva el esquema dentro**: un CSV devuelve todo como texto y obliga a que cada lector vuelva a decidir qué es número y qué es fecha |

---

## 3. Fuentes de datos

| # | Fuente | Flujo | Tecnología | Propósito |
|---|---|---|---|---|
| F1 | Trades del exchange | NRT | WebSocket → Kafka | Insumo de las ventanas |
| F2 | Simulador de trades | NRT | Proceso local → Kafka | Respaldo y motor de las pruebas |
| F3 | Velas históricas | Batch | API REST paginada | Serie diaria, referencia de la conciliación |
| F4 | Catálogo de activos | Batch | CSV semilla versionado | Dimensión `dim_activo` |
| F5 | Precio de referencia | Batch | `http_poller` de Logstash | Segundo punto de comparación |
| F6 | Eventos de control | NRT | HTTP POST al 8088 | Los DAGs reportan sus hitos |
| F7 | Logs de componentes | NRT | Socket TCP al 5000 | Observabilidad del propio pipeline |

### F1 y F2: dos productores para el mismo mensaje

F1 y F2 emiten **exactamente el mismo evento** y se eligen con una variable de entorno:

```powershell
# Mercado real (por defecto)
docker compose up -d productor

# Simulador: sin red, y para las pruebas de carga y deduplicación
$env:CRIPTO_FUENTE_TRADES="simulador"; docker compose up -d productor
```

Lo único que los distingue en el dato es el campo `origen`: `exchange_ws` o `simulador`.

**F2 es obligatorio, no opcional.** Si el exchange bloquea la IP o no hay internet, la
demostración se cae. Con el simulador el pipeline es reproducible, las pruebas son
deterministas (semilla fija) y se pueden inyectar defectos para probar la cuarentena. Si
el exchange no responde, el productor cae solo a F2, lo avisa por consola y publica un
evento `ops_control`.

> **Regla del proyecto:** el pipeline debe funcionar de punta a punta **sin internet**. Lo
> que solo funciona con la API real, no está terminado.

**Pero la conciliación es la excepción a esa regla, y por un motivo de fondo.** Comparar
el VWAP del streaming contra el cierre de la vela del batch solo mide algo si **ambos
lados leen el mismo mercado**. Contra el simulador, la comparación mide la distancia entre
las constantes escritas a mano del simulador y el precio real: medido, −20 %, +36 % y
+40 % de desviación. Por eso cada métrica de ventana lleva `origen_datos` —de qué fuente
salieron sus trades— y el DAG 05 solo concilia las que valen `exchange_ws`.

El pipeline entero **funciona** sin internet. Lo que no puede hacer sin internet es
demostrar que sus dos flujos coinciden, porque no habría con qué compararlos.

**F6 y F7 hacen que el pipeline se observe a sí mismo.** Cada DAG publica por HTTP su
inicio, fin, filas procesadas y filas en cuarentena; el productor y Spark emiten sus logs
por TCP. En Kibana queda una vista de operación junto a la de negocio.

---

## 4. Probar la lógica sin infraestructura

Lo único que hace falta es Python con `pandas`, `numpy`, `pyarrow` y `requests`.

```powershell
python pruebas\prueba_logica_batch.py
```

También sin infraestructura, la traducción del formato del exchange al contrato:

```powershell
python pruebas\prueba_websocket.py
```

40 comprobaciones en 8 bloques, sin red, sin Kafka y sin necesidad de tener instalado
`websocket-client`. Cubre los tres detalles del formato del exchange que fallan **en
silencio**: precio y cantidad llegan como cadena y no como número; el símbolo va en
minúsculas en el canal y en mayúsculas en el campo; y `ts_evento` sale de `T`, la hora del
trade, no de `E`, la hora del evento.

### Inventario de pruebas

| Prueba | Qué verifica | Necesita |
|---|---|---|
| `prueba_logica_batch.py` | 42 comprobaciones: reglas de calidad, transformaciones, indicadores | Python |
| `prueba_websocket.py` | 40 comprobaciones: traducción exchange → contrato | Python |
| `prueba_conciliacion.py` | 30 comprobaciones: aritmética y veredictos, con respuesta de ES guardada | Python |
| `prueba_logica_streaming.py` | 7 casos: VWAP, OHLC, volatilidad, ventanas, `origen_datos`, campos de salida | Contenedor de Spark |
| `prueba_latencia.py` | Latencia por etapas, con percentiles | Entorno levantado con datos |
| `prueba_carga.py` | Rendimiento del productor y deduplicación | Entorno levantado |

La de Spark corre dentro de su contenedor, con `spark-submit` y no con `python`:

```powershell
docker compose run --rm --no-deps -v "${PWD}/pruebas:/pruebas" `
    spark-streaming /opt/spark/bin/spark-submit /pruebas/prueba_logica_streaming.py
```

**Importa `calcular_metricas` del job en vez de reimplementarlo.** Es la diferencia entre
probar el código que corre en producción y probar una copia que solo existe en la prueba;
la versión anterior hacía lo segundo y pasaba aunque el job estuviera roto.

> **Aviso:** `prueba_carga.py` publica trades del simulador en el mismo topic que usa el
> pipeline. Si se ejecuta con el productor real en marcha, contamina las ventanas de esa
> hora con `origen_datos = exchange_ws+simulador` y la conciliación las descartará.
> Ejecutarla con el productor detenido.

Tarda segundos y ejecuta 42 comprobaciones en 12 bloques. La más importante es el bloque
2: verifica que **cada defecto que inyecta el generador sea detectado por la regla que le
corresponde**. Sin esa correspondencia, una regla puede romperse y las pruebas seguir en
verde porque otra atrapa el defecto por casualidad.

Para comprobar la conectividad con la API pública:

```powershell
python -c "import sys; sys.path.insert(0,'dags'); from comun import clientes_api; print(clientes_api.descargar_klines('BTCUSDT', dias=5))"
```

Si la API no responde, la función cae automáticamente a la serie sintética y lo dice en
el log. Las velas sintéticas se marcan con `origen = sintetico` para que **nunca se
confundan con datos reales**, ni en el reporte ni en los paneles.

---

## 5. Levantar el entorno

**Antes de levantar este entorno hay que detener los otros dos** (`taller-airflow-5dags` y
`kafka-spark-zeppelin`). Con 16 GB no caben en simultáneo.

### Orden de arranque

El orden importa. `productor` y `spark-streaming` declaran `restart: on-failure`, así que
levantarlos antes que Kafka no falla con un mensaje claro: los deja **reintentando en
bucle** contra un bus que no existe, llenando la consola de trazas que no dicen cuál era
el problema.

```bash
# 1. Bases de datos y orquestador
docker compose up -d postgres mysql airflow-init
docker compose up -d airflow-webserver airflow-scheduler

# 2. Bus de eventos (kafka-init crea los topics y termina)
docker compose up -d zookeeper kafka kafka-init kafka-ui

# 3. Almacenamiento y visualización (elasticsearch-init aplica la plantilla y termina)
docker compose up -d elasticsearch elasticsearch-init kibana logstash

# 4. Solo cuando lo anterior está sano: productores y procesamiento
docker compose ps            # comprobar que no queda ninguno reiniciándose
docker compose up -d productor spark-streaming
```

Para apagar sin perder nada: `docker compose stop`. **`docker compose down -v` borra los
volúmenes**, y con ellos los índices de Elasticsearch, las tablas de MySQL y los tableros
de Kibana.

### Verificar que el circuito está cerrado

```bash
# Trades y métricas entrando
curl -s "http://localhost:9200/_cat/indices/cripto-*?v&h=index,docs.count&s=index"

# Qué fuente alimenta los trades: exchange_ws o simulador
curl -s "http://localhost:9200/cripto-nrt_trade-*/_search?size=0" \
  -H 'Content-Type: application/json' \
  -d '{"aggs":{"por_origen":{"terms":{"field":"origen"}}}}'

# Salida de Spark directamente del topic
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic metricas.1min --from-beginning --max-messages 3
```

En Kibana (`http://localhost:5602/app/discover`), el patrón de índice es `cripto-*`.

### Ejecutar las pruebas que necesitan infraestructura

Las que no la necesitan están en la [sección 4](#4-probar-la-lógica-sin-infraestructura).

```bash
# Lógica de Spark: DENTRO del contenedor, que es donde están pyspark y Java.
# Con spark-submit y no con python3: pyspark vive en /opt/spark/python y solo
# spark-submit lo pone en el PYTHONPATH. `--no-deps` evita arrastrar a Kafka,
# que esta prueba no necesita.
docker compose run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit \
  spark-streaming --master "local[2]" /opt/spark/pruebas/prueba_logica_streaming.py

# Latencia de extremo a extremo (p50/p95/p99) — requiere Elasticsearch con datos
python pruebas/prueba_latencia.py

# Carga sobre el bus — requiere Kafka
python pruebas/prueba_carga.py
```

### Puertos

Puertos, elegidos para no chocar con los entornos de los talleres anteriores, que ya
ocupan 8080, 8081, 3306, 3307 y 5432. **Son los del `docker-compose.yml`, verificados con
el entorno levantado:**

| Servicio | Puerto en el host | Puerto dentro de la red de Docker |
|---|---|---|
| Kibana | **5602** | 5601 |
| Elasticsearch | 9200 | 9200 |
| Logstash — beats / HTTP / TCP | 5044 / **8089** / 5000 | 5044 / 8088 / 5000 |
| Airflow UI | 8092 | 8080 |
| Kafka UI | 8093 | 8080 |
| Kafka | 9095 | 29092 |
| MySQL | 3308 | 3306 |
| Spark — UI de la aplicación | 4040 | 4040 |

**Las dos columnas no son un adorno.** Kafka anuncia dos listeners y hay que usar el
correcto según dónde se ejecute el código: `kafka:29092` desde dentro de la red de Docker,
`localhost:9095` desde Windows. Lo mismo con Logstash: los DAGs le envían al 8088 porque
corren dentro de la red; desde el host el mismo input está en el 8089.

---

## 6. Estructura del repositorio

```
├── PLAN.md                     Plan de ejecución de la semana
├── AVANCE.md                   Estado real y bitácora de problemas resueltos
├── docker-compose.yml          Entorno completo: batch, NRT y ELK
├── Dockerfile.airflow          Airflow + pyarrow
├── Dockerfile.spark            Spark + los JAR del conector de Kafka horneados
├── Dockerfile.productor        Productor + websocket-client y kafka-python
├── contratos/
│   └── CONTRATO_DATOS.md       Interfaz entre el camino batch y el NRT
├── sql/                        DDL. Fuente única del esquema de MySQL
├── ingesta_streaming/          ── Camino NRT
│   ├── cliente_websocket.py    F1: exchange real; traducción pura al contrato
│   ├── simulador_trades.py     F2: respaldo sin red
│   ├── productor_kafka.py      Elige fuente y publica en trades.crudo
│   └── observabilidad.py       Eventos ops_control del productor
├── procesamiento_streaming/
│   ├── esquemas_spark.py       Esquema estricto del trade
│   ├── job_metricas_ventana.py Ventanas de 1 min con watermark y dedup
│   └── reglas_alertas.py       Alertas (decisión de diseño abierta)
├── logstash/
│   └── pipeline_cripto/        Único escritor hacia Elasticsearch
├── elasticsearch/
│   └── plantillas/cripto.json  Tipos explícitos, no mapeo dinámico
├── kibana/
│   └── tableros.ndjson         Tableros exportados
├── dags/                       ── Camino batch
│   ├── comun/                  Una capa por archivo
│   │   ├── config.py           Parámetros, umbrales, conexiones
│   │   ├── utilidades.py       Lote_id, rutas, NDJSON, tiempo en UTC
│   │   ├── zonas.py            Acceso a las zonas de datos (Parquet)
│   │   ├── clientes_api.py     Descarga REST y respaldo sintético
│   │   ├── reglas_calidad.py   Reglas R01–R08, funciones puras
│   │   ├── transformaciones.py Normalización e indicadores
│   │   ├── repositorio.py      Único punto de acceso a MySQL
│   │   └── observabilidad.py   Publicación a Logstash
│   ├── dag_01_ingesta_batch.py
│   ├── dag_02_calidad.py
│   ├── dag_03_transformacion.py
│   ├── dag_04_carga_mysql.py
│   └── dag_05_conciliacion.py  Compara el flujo NRT contra el batch
├── datos_semilla/              Catálogo de activos, versionado
├── pruebas/                    Tres corren sin infraestructura; cinco necesitan el entorno
├── docs/
│   ├── ARQUITECTURA.md         Papel de cada servicio
│   ├── arquitectura.html       Diagrama interactivo (compilado del JSON)
│   ├── DECISIONES.md           Decisiones de diseño y su porqué
│   ├── PRUEBAS.md              Las doce pruebas y sus resultados
│   ├── GUION_EXPOSICION.md     Guion cronometrado
│   ├── GUIA_CAPTURAS.md        Qué capturar y con qué estado
│   └── REGLAS_NEGOCIO.md       Catálogo de reglas, fórmulas y supuestos
├── capturas/                   Evidencia para el documento y el respaldo de la demo
└── datos/                      Se crea al ejecutar; no se versiona
    └── bronce/ cuarentena/ plata/ exportado/ checkpoints/
```

**Principio de organización.** Los DAGs y el job de Spark **solo orquestan y
transforman**. Ninguna regla de negocio ni SQL vive dentro de ellos. Las reglas son
funciones puras, lo que permite probarlas en segundos sin levantar infraestructura.

---

## 7. Modelo de datos

### MySQL

| Tabla | Contenido |
|---|---|
| `dim_activo` | Dimensión. Un registro por activo negociado |
| `hechos_ohlcv_diario` | Hechos. Una vela diaria por activo y fecha, con indicadores |
| `control_lotes` | Bitácora: estado y métricas de cada corrida |
| `conciliacion` | Comparación entre el flujo NRT y el batch |

### Elasticsearch

Convención de índices, heredada del Taller 2: `cripto-{tipo_fuente}-YYYY.MM.dd`, con un
patrón único `cripto-*` en Kibana anclado a `@timestamp`. Así los registros del histórico
batch conviven en la misma línea de tiempo que los eventos NRT.

`tipo_fuente` admite: `nrt_trade`, `nrt_metrica`, `nrt_alerta`, `batch_ohlcv`,
`batch_referencia`, `batch_conciliacion`, `ops_control`, `ops_log`.

**Con plantilla de índice explícita, no mapeo dinámico.** Si Elasticsearch infiere los
tipos, un precio puede caer como `text` y las agregaciones del panel fallan sin error
visible.

---

## 8. Documentación

| Documento | Contenido |
|---|---|
| [PLAN.md](PLAN.md) | Alcance, arquitectura, reparto, cronograma, riesgos |
| [AVANCE.md](AVANCE.md) | Estado por entregable y bitácora de problemas resueltos |
| [docs/ARQUITECTURA.md](docs/ARQUITECTURA.md) | Papel de cada servicio y qué pasaría si no estuviera |
| [docs/arquitectura.html](docs/arquitectura.html) | Diagrama interactivo, cuatro vistas guiadas. Se abre en el navegador |
| [docs/DECISIONES.md](docs/DECISIONES.md) | Cada decisión de diseño con su contexto y lo que costó |
| [docs/PRUEBAS.md](docs/PRUEBAS.md) | Las doce pruebas, con resultados reales y cómo repetirlas |
| [docs/GUION_EXPOSICION.md](docs/GUION_EXPOSICION.md) | Guion cronometrado de 20 minutos y preguntas probables |
| [docs/GUIA_CAPTURAS.md](docs/GUIA_CAPTURAS.md) | Qué capturar, con qué estado y para qué |
| [contratos/CONTRATO_DATOS.md](contratos/CONTRATO_DATOS.md) | Esquemas, tipos, unidades y nombres prohibidos |
| [docs/REGLAS_NEGOCIO.md](docs/REGLAS_NEGOCIO.md) | Catálogo de reglas, fórmulas y supuestos |
