# Plan de ejecución — Pipeline batch + near real-time de mercado cripto

> **Estado actual del trabajo: [AVANCE.md](AVANCE.md).** Este documento es el plan; aquel
> dice qué está hecho, qué falta y qué problemas han aparecido por el camino.

**Trabajo final · Ingeniería de Datos · Universidad San Francisco de Quito**
**Equipo: Estéfano Galarza y Manuel Pillapa · Plazo: 7 días · Septiembre 2026**

> **Alcance.** Prototipo exploratorio académico. Todas las fuentes son APIs públicas
> gratuitas o datos simulados. No hay conexión con ningún sistema corporativo ni se
> reproducen sus estructuras o reglas. Las credenciales del entorno son ficticias y
> válidas solo en local: sin TLS, sin gestión de secretos, sin respaldos.

---

## 1. La idea rectora: no empezamos de cero

El equipo llega con **dos pipelines que ya funcionan**, y el proyecto final los une en
lugar de reemplazarlos:

| Activo existente | Qué aporta | Dueño |
|---|---|---|
| **Taller 2 — Logstash + Elasticsearch + Kibana** | Ingesta multi-fuente (archivo, `http_poller`, HTTP, TCP), filtros condicionales por `type`, enrutamiento a índices, patrón de índice unificado en Kibana | Estéfano |
| **Taller Airflow — 5 DAGs encadenados** | Orquestación, zonas de datos, reglas de calidad con ramificación, modelo dimensional en MySQL, carga idempotente por lote, pruebas de lógica sin infraestructura | Manuel |
| **Entorno de clase Kafka + Spark** | `docker-compose` probado con Kafka, Kafka UI y Spark 3.5.6 | Ambos |

Lo único genuinamente nuevo es **una pieza**: el job de Spark Structured Streaming que
agrega por ventanas. Todo lo demás es extensión de código que ya corre. Ese es el motivo
por el que un proyecto de esta ambición cabe en siete días.

**La pieza que faltaba, dicha con precisión.** El Taller 2 ingiere en near real-time pero
procesa evento a evento: parsea, limpia campos y enruta. No agrega. El enunciado del
trabajo final pide explícitamente *procesamiento en tiempo real*, y ahí es donde entra
Spark: ventana temporal, marca de agua para eventos tardíos, deduplicación y punto de
control. Nada más.

---

## 2. Qué construimos y por qué

### 2.1 El problema

Un operador de mesa cripto necesita dos cosas incompatibles en un solo flujo:

- **Ahora mismo**: qué pasa con el precio en los últimos segundos, con latencia de pocos
  segundos, para reaccionar a movimientos bruscos.
- **Con perspectiva**: cómo se comporta el activo en semanas, con series completas y
  consistentes, para decidir posiciones.

El primero exige un flujo **near real-time** que sacrifica completitud por latencia. El
segundo exige un flujo **batch** que sacrifica latencia por exactitud. Implementamos
ambos sobre el mismo dominio y —esta es la parte que nadie suele hacer— **los
conciliamos**: comprobamos con un número si lo que el flujo rápido dijo en vivo coincide
con lo que el flujo lento confirma después.

Esto retoma y responde el "aprendizaje principal" con el que cierra el Taller 2: *un
esquema mixto batch/NRT es viable siempre que se cuide el mapeo de campos y la forma en
que se comparan los tiempos entre ambos flujos*. El proyecto final convierte esa frase
en una tabla medida.

### 2.2 Preguntas que responde

| Usuario | Pregunta | Flujo |
|---|---|---|
| Operador | ¿El precio se movió más de X% en el último minuto? | NRT (alertas) |
| Operador | ¿VWAP y volumen del minuto en curso? | NRT (ventanas Spark) |
| Analista | ¿Retorno y volatilidad de los últimos 30 días? | Batch (MySQL) |
| Analista | ¿Qué activos concentran el volumen? | Batch (modelo dimensional) |
| Ingeniero de datos | ¿El flujo rápido está midiendo bien? | Conciliación (ambos) |
| Ingeniero de datos | ¿El pipeline está sano? | Logs por TCP y eventos de control por HTTP |

### 2.3 Supuestos declarados

1. Los datos provienen de un exchange público; no representan operación real de nadie.
2. El VWAP por ventana se calcula solo con los trades recibidos. Si el WebSocket pierde
   mensajes, el valor es aproximado. **Eso es precisamente lo que mide la conciliación.**
3. Los precios están en USDT y se tratan como USD, sin ajuste cambiario.
4. Las ventanas usan la marca de tiempo del **evento** (hora del exchange), no la de
   ingesta.
5. No hay reglas de negocio propietarias. Los indicadores son de dominio público —VWAP,
   OHLC, SMA, retorno porcentual, desviación estándar móvil— y quedan documentados con su
   fórmula en `docs/REGLAS_NEGOCIO.md`.

---

## 3. Arquitectura

### 3.1 Vista general

```
  FUENTES                      BUS / INGESTA           PROCESAMIENTO NRT              ALMACENAMIENTO       SERVICIO
──────────────────────────────────────────────────────────────────────────────────────────────────────────────────

 WebSocket trades ─┐
                   ├─→ productor ─→ Kafka                                                                ┌→ KIBANA
 Simulador ────────┘   (clave =     trades.crudo ──→ Spark Structured Streaming                          │  índice
                        símbolo)          │          · valida esquema                                    │  cripto-*
                                          │          · dedup por id_trade                                │  refresco 5 s
                                          │          · ventana 1 min + watermark 30 s                    │
                                          │          · VWAP, OHLC, volatilidad                           │
                                          │                     │                                        │
                                          │          ┌──────────┴──────────┐                             │
                                          │          ▼                     ▼                             │
                                          │   Kafka metricas.1min   Kafka alertas.precio                 │
                                          │          │                     │                             │
                                          └──────────┴─────────┬───────────┘                             │
                                                               │                                         │
 HTTP POST :8088 (control) ────────────────────────────────────┤                                         │
 TCP :5000 (logs) ─────────────────────────────────────────────┼──→ LOGSTASH ──→ ELASTICSEARCH ──────────┤
 http_poller :60s (precio referencia) ─────────────────────────┤    (único        cripto-nrt-trades       │
                                                               │     escritor     cripto-nrt-metricas     │
 API REST klines ─┐                                            │     hacia ES)    cripto-nrt-alertas      │
 CSV catálogo ────┴→ AIRFLOW ─→ bronce ─→ plata ─→ MySQL ──────┘                  cripto-batch-ohlcv      │
                     DAG 01-04   (Parquet)  (dim_activo,  NDJSON                  cripto-ops-*            │
                                             hechos_ohlcv, exportado                                      │
                                             control_lotes)                                               │
                                                     │                                                    │
                     AIRFLOW DAG 05 — conciliación ──┴─ consulta agregada a Elasticsearch (REST)          │
                                                        + lectura de MySQL → tabla `conciliacion` ────────┘
```

### 3.2 Decisiones de arquitectura y su porqué

| Decisión | Motivo |
|---|---|
| **Kafka como bus compartido** | Desacopla ingesta de procesamiento, permite reprocesar desde un offset y absorbe picos. Sin él, Spark y Logstash competirían por el mismo WebSocket. |
| **Spark escribe a Kafka, no a Elasticsearch ni a MySQL** | Elimina el conector Elasticsearch-Spark y el driver JDBC, las dos dependencias más frágiles. El job queda como fuente Kafka → agregación → destino Kafka: el caso mejor documentado. |
| **Logstash como único escritor hacia Elasticsearch** | Es el rol que ya cumple en el Taller 2. Se le agregan tres `input { kafka {} }`; no se reescribe. Un solo lugar donde se define el mapeo de campos hacia ES. |
| **MySQL para el modelo dimensional, Elasticsearch para la línea de tiempo** | Cada almacén hace lo que sabe hacer: MySQL da claves foráneas, unicidad e idempotencia por lote; Elasticsearch da series temporales, búsqueda y Kibana. Justificar dos almacenes por su función es mejor argumento que forzar uno solo. |
| **Airflow con LocalExecutor, no Celery** | Elimina Redis, worker y triggerer: tres contenedores menos. Con Elasticsearch, Kafka y Spark en la misma máquina, el recurso escaso es la RAM, no el paralelismo. |
| **Spark en modo `local[2]` en un solo contenedor** | Ahorra ~2 GB frente a master + worker separados. La interfaz de la aplicación sigue disponible en el puerto 4040 para mostrarla en la exposición. |
| **Patrón de índice único `cripto-*` en Kibana** | Es el hallazgo de la sección 5 del Taller 2: batch y NRT conviven en la misma línea de tiempo anclada a `@timestamp`. Aquí por fin tiene contenido que comparar. |
| **Conciliación desde Airflow, no desde Spark** | Airflow ya sabe hablar con MySQL y consultar Elasticsearch es una petición REST. Poner la conciliación en Spark obligaría a que el job leyera de dos almacenes en vivo. |

### 3.3 Zonas de datos

| Zona | Formato | Contenido | Dueño |
|---|---|---|---|
| `datos/bronce/` | Parquet, particionado por fecha y símbolo | Klines tal como llegan de la API | Manuel |
| `datos/cuarentena/` | Parquet + `motivo_rechazo` | Registros que fallan las reglas de calidad | Manuel |
| `datos/plata/` | Parquet | OHLCV normalizado, tipado, sin duplicados, con indicadores | Manuel |
| `datos/exportado/` | NDJSON | Salida del batch que consume Logstash con `input { file {} }` | Manuel |
| MySQL `cripto` | Tablas | `dim_activo`, `hechos_ohlcv_diario`, `control_lotes`, `conciliacion` | Manuel |
| Elasticsearch `cripto-*` | Índices diarios | Trades, métricas, alertas, OHLCV batch y operación | Estéfano |
| `datos/checkpoints/` | Interno de Spark | Offsets y estado de las ventanas | Estéfano |

---

## 4. Fuentes de datos

Se conservan los **cuatro tipos de input** del Taller 2 y se agrega Kafka. No es
decorativo: cada uno cumple una función distinta en el pipeline.

| # | Fuente | Tipo de flujo | Tecnología | Propósito |
|---|---|---|---|---|
| F1 | Trades del exchange | **NRT** | WebSocket → Kafka | Evento a evento, insumo de las ventanas |
| F2 | Simulador de trades | **NRT** | Proceso local → Kafka | Respaldo obligatorio y motor de las pruebas |
| F3 | Velas históricas (klines) | **Batch** | API REST paginada, DAG diario | Serie diaria oficial, referencia de la conciliación |
| F4 | Catálogo de activos | **Batch** | CSV semilla versionado | Dimensión `dim_activo` |
| F5 | Precio de referencia | **Batch** | `http_poller` de Logstash cada 60 s | Segundo punto de comparación de precio, reusa el input del Taller 2 |
| F6 | Eventos de control del pipeline | **NRT** | HTTP POST al puerto 8088 | Los DAGs y el productor reportan hitos; reusa el input del Taller 2 |
| F7 | Logs de los componentes | **NRT** | Socket TCP puerto 5000 | Observabilidad del propio pipeline; reusa el input del Taller 2 |

**F2 es obligatorio, no opcional.** Si el exchange bloquea la IP, si el aula no tiene
internet o si la API cambia, la demo se cae. Con el simulador el pipeline es
reproducible, las pruebas son deterministas (semilla fija) y se pueden inyectar defectos
a voluntad para probar la cuarentena.

> **Regla de equipo:** el pipeline debe funcionar de punta a punta **sin internet**. Lo
> que solo funciona con la API real, no está terminado.

**F6 y F7 son el toque que aprovecha mejor el Taller 2.** En vez de dejar los inputs HTTP
y TCP como demostración vacía, se usan para que el propio pipeline se observe a sí mismo:
cada DAG publica inicio, fin, filas procesadas y filas en cuarentena por HTTP; el
productor y el job de Spark emiten sus logs por TCP. En Kibana queda una vista de
operación junto a la de negocio. Eso es material directo para la sección de resultados.

---

## 5. Contrato de datos — **se cierra el Día 1, entre los dos**

Es la interfaz entre las dos personas. Si no queda cerrada el primer día, se bloquean
mutuamente el resto de la semana. Vive en `contratos/CONTRATO_DATOS.md`, en `sql/` y en
las plantillas de índice de Elasticsearch. **Cualquier cambio se avisa antes de subirlo.**

### 5.1 Evento de trade — Kafka `trades.crudo`, clave = símbolo

```json
{
  "id_evento":          "uuid v4",
  "simbolo":            "BTCUSDT",
  "id_trade":           123456789,
  "precio":             63250.10,
  "cantidad":           0.0031,
  "importe_usdt":       196.07,
  "comprador_es_maker": false,
  "ts_evento":          "2026-09-08T14:03:11.482Z",
  "ts_ingesta":         "2026-09-08T14:03:11.617Z",
  "origen":             "exchange_ws | simulador",
  "tipo_fuente":        "nrt_trade"
}
```

`id_trade` es la clave de deduplicación. `ts_evento` es la que gobierna las ventanas.
`ts_ingesta − ts_evento` da la latencia de la primera etapa.

El campo `tipo_fuente` cumple el mismo papel que `type` en el Taller 2: **es el que
decide el filtro condicional y el índice destino en Logstash.** Valores admitidos:
`nrt_trade`, `nrt_metrica`, `nrt_alerta`, `batch_ohlcv`, `batch_referencia`,
`ops_control`, `ops_log`.

### 5.2 Métrica por ventana — Kafka `metricas.1min`

```json
{
  "simbolo": "BTCUSDT", "ventana_inicio": "...", "ventana_fin": "...",
  "n_trades": 412, "volumen_base": 3.81, "volumen_usdt": 241030.55,
  "precio_apertura": 63180.0, "precio_maximo": 63310.5,
  "precio_minimo": 63102.2, "precio_cierre": 63250.1,
  "vwap": 63248.77, "volatilidad_pct": 0.33,
  "ts_procesado": "...", "origen": "spark_streaming", "tipo_fuente": "nrt_metrica"
}
```

### 5.3 Tablas de MySQL — archivo `sql/02_tablas.sql`

| Tabla | Clave primaria | Escribe | Lee |
|---|---|---|---|
| `dim_activo` | `id_activo` | Manuel (DAG 04) | DAG 05, informe |
| `hechos_ohlcv_diario` | `(id_activo, fecha)` | Manuel (DAG 04) | DAG 05, Kibana vía NDJSON |
| `control_lotes` | `lote_id` | Manuel (DAG 01-04) | Informe |
| `conciliacion` | `(simbolo, fecha_hora)` | Manuel (DAG 05) | Kibana vía NDJSON |

`hechos_ohlcv_diario`: `id_activo, fecha, apertura, maximo, minimo, cierre,
volumen_base, volumen_usdt, n_trades, retorno_pct, sma_7, sma_30, volatilidad_30d,
lote_id`.

`conciliacion`: `simbolo, fecha_hora, vwap_streaming, n_trades_streaming, cierre_batch,
n_trades_batch, desviacion_pct, cobertura_pct, veredicto, lote_id`.

### 5.4 Índices de Elasticsearch

Se mantiene la convención del Taller 2, cambiando el prefijo:
`cripto-{tipo_fuente}-YYYY.MM.dd`. Patrón único en Kibana: `cripto-*`, anclado a
`@timestamp`.

**Plantilla de índice explícita, no mapeo dinámico.** En el Taller 2 el mapeo se infirió
solo; con precios y volúmenes eso lleva a que un campo caiga como texto y las
agregaciones fallen. Se define una plantilla que fije `precio`, `vwap` y `volumen_*` como
`double`, y `simbolo` y `tipo_fuente` como `keyword`.

### 5.5 Mapa de puertos — fijado el Día 1

Los entornos que ya tienen ocupan 8080, 8081, 3306, 3307 y 5432. Este compose los evita.

| Servicio | Puerto host | Notas |
|---|---|---|
| Elasticsearch | 9200 | Igual que en el Taller 2 |
| Kibana | 5601 | Igual que en el Taller 2 |
| Logstash — beats | 5044 | Igual que en el Taller 2 |
| Logstash — HTTP | 8088 | Igual que en el Taller 2 |
| Logstash — TCP | 5000 | Igual que en el Taller 2 |
| Kafka (externo) | 9095 | Interno `kafka:29092` |
| Kafka UI | 8093 | |
| Airflow UI | 8092 | |
| MySQL | 3308 | 3306 y 3307 ya ocupados |
| Spark — UI de la aplicación | 4040 | Solo mientras corre el job |
| Postgres — metadatos de Airflow | *sin publicar* | No se accede desde el host |

**Antes de levantar este compose hay que detener los otros dos** (`taller-airflow-5dags`
y `kafka-spark-zeppelin`). Con 16 GB no caben en simultáneo.

---

## 6. Reparto del trabajo

Reparto **por flujo**, alineado con lo que cada uno ya domina.

### Estéfano — Camino near real-time, ingesta y visualización

| Entregable | Listo cuando | Reuso |
|---|---|---|
| `ingesta_streaming/simulador_trades.py` | Genera eventos válidos con semilla fija, tasa configurable e inyección de defectos | Nuevo |
| `ingesta_streaming/productor_kafka.py` | Se conecta al WebSocket, reconecta tras caída y cae al simulador si no hay red | Nuevo |
| Topics de Kafka | `trades.crudo`, `metricas.1min`, `alertas.precio` creados por el compose, particionados por símbolo | Compose de clase |
| `procesamiento_streaming/job_metricas_ventana.py` | Ventana *tumbling* de 1 min, watermark 30 s, dedup por `id_trade`, checkpoint en volumen | **Nuevo — la pieza crítica** |
| `procesamiento_streaming/reglas_alertas.py` | Regla de variación de precio parametrizada, publica en `alertas.precio` | Nuevo |
| `logstash/pipeline/logstash.conf` | Cinco inputs (3 Kafka + HTTP + TCP + `http_poller` + `file`), filtros por `tipo_fuente`, enrutamiento a `cripto-*` | **Extensión del Taller 2** |
| `elasticsearch/plantillas/*.json` | Plantilla de índice con tipos explícitos, aplicada al arrancar | Extensión |
| Tableros de Kibana | Exportados como NDJSON y provisionados, no creados a mano. Mínimo 5 paneles | **Extensión del Taller 2** |
| `pruebas/prueba_latencia.py` | Reporta p50/p95/p99 de la latencia extremo a extremo | Nuevo |

### Manuel — Camino batch, almacén dimensional y conciliación

| Entregable | Listo cuando | Reuso |
|---|---|---|
| `sql/01_esquemas.sql`, `sql/02_tablas.sql` | El compose crea las tablas al arrancar, sin pasos manuales | Extensión del taller |
| `dags/comun/clientes_api.py` | Descarga klines paginados con reintentos y respeta el límite de tasa | Nuevo |
| `dag_01_ingesta_batch` | Escribe Parquet en `bronce/` particionado; repetir no duplica | **Adaptación del DAG 01** |
| `dag_02_calidad` | Reglas R01–R06 documentadas, ramifica a promoción o bloqueo del lote | **Adaptación del DAG 02** |
| `dag_03_transformacion` | Genera `plata/` con retorno, SMA-7, SMA-30 y volatilidad 30d | **Adaptación del DAG 03** |
| `dag_04_carga_mysql` | Carga dimensión y hechos de forma idempotente por `lote_id`; exporta NDJSON a `datos/exportado/` | **Adaptación del DAG 04** |
| `dag_05_conciliacion` | Consulta agregada a Elasticsearch por REST + lectura de MySQL, escribe `conciliacion` y publica el reporte por HTTP a Logstash | Nuevo |
| `comun/observabilidad.py` | Helper que publica hitos de los DAGs al puerto 8088 y logs al 5000 | Nuevo, pequeño |
| `pruebas/prueba_logica_batch.py` | Corre sin Airflow en segundos; cada defecto inyectado lo detecta su regla | **Adaptación de `prueba_logica.py`** |
| `docs/REGLAS_NEGOCIO.md` | Cada indicador con fórmula, supuesto y límite | Extensión |

### Trabajo conjunto, obligatorio

- **Día 1 completo** — contrato de datos, DDL, plantillas de índice, mapa de puertos,
  `docker-compose.yml` unificado y **la imagen de Spark con el conector de Kafka
  horneado**.
- **Día 3, media jornada** — el job de Spark en pareja. Es el componente nuevo, el de
  mayor riesgo, y el único que ninguno de los dos ha escrito antes. Además los dos tienen
  que poder defenderlo en la exposición.
- **Día 5 completo** — integración, batería de pruebas y capturas.
- **Días 6 y 7** — documento, diagrama, guion y ensayos cronometrados.

### Reglas de colaboración

1. Rama por persona (`nrt/estefano`, `batch/manuel`), *pull request* a `main`. Cada uno
   revisa el PR del otro: es la única forma de que ambos puedan defender el proyecto
   completo.
2. `docker-compose.yml`, `sql/`, `contratos/` y `logstash.conf` son **territorio
   compartido**: se avisa antes de tocarlos.
3. `docs/DECISIONES.md` se escribe **mientras** se trabaja. Documentación y diseño pesan
   40% de la nota combinados.
4. Todo problema resuelto se anota con **síntoma, causa real y solución**. Los
   evaluadores preguntan justamente eso; el README del taller de Airflow ya tiene el
   formato y funcionó.

---

## 7. Cronograma de 7 días

Inicio lunes 7 de septiembre de 2026.

### Día 1 — Lunes 7 · Andamiaje *(los dos, jornada completa)*

- Crear el repositorio, copiar el `docker-compose.yml` y el `logstash.conf` del Taller 2
  como punto de partida, y el compose de Kafka/Spark de clase.
- Unificar ambos en un solo `docker-compose.yml` con el mapa de puertos del punto 5.5.
- Cerrar el contrato de datos: `contratos/`, `sql/` y plantillas de índice.
- **Construir la imagen de Spark con el conector de Kafka incluido en el `Dockerfile`.**
  No usar `--packages` en tiempo de ejecución: descarga por Ivy en cada arranque y exige
  internet. Verificar el mismo día que el contenedor arranca sin red.
- Dibujar el diagrama de arquitectura: sirve para el documento y para la exposición.

> **Criterio de salida:** `docker compose up -d` deja todos los servicios en *healthy*,
> Kibana abre, `SHOW TABLES` en MySQL lista las cuatro tablas y el contenedor de Spark
> arranca offline. Si esto no está el lunes, el resto de la semana se comprime.

### Día 2 — Martes 8 · Ingesta

- **Estéfano**: simulador, productor a Kafka, topics creados, flujo visible en Kafka UI.
  Extender `logstash.conf` con el primer `input { kafka {} }` de `trades.crudo` e
  indexar en `cripto-nrt-trades-*`.
- **Manuel**: DAG 01 con cliente REST paginado y escritura a `bronce/`; catálogo semilla
  en `datos_semilla/`.

> **Salida:** hay Parquet en `bronce/` y trades visibles en Kibana Discover.

### Día 3 — Miércoles 9 · Ventanas *(mañana en pareja)*

- **Mañana, los dos**: job de Spark Structured Streaming. Fuente Kafka, dedup, ventana de
  1 min con watermark, agregaciones, destino Kafka `metricas.1min`.
- **Tarde, Estéfano**: input de Kafka para `metricas.1min` en Logstash, índice
  `cripto-nrt-metricas-*`, primer panel de Kibana.
- **Tarde, Manuel**: DAG 02 (calidad, con ramificación) y DAG 03 (transformación a
  `plata/`).

> **Salida:** un panel de Kibana con el VWAP por minuto que se mueve solo.

### Día 4 — Jueves 10 · Cierre de cada camino

- **Estéfano**: reglas de alertas, topic `alertas.precio`, inputs HTTP y TCP conectados
  a la observabilidad del pipeline, tableros de Kibana completos y exportados.
- **Manuel**: DAG 04 (carga dimensional idempotente + exportación NDJSON), encadenamiento
  01→02→03→04, helper de observabilidad publicando a Logstash.

> **Salida:** el batch corre encadenado en verde, sus filas aparecen en
> `cripto-batch-ohlcv-*`, y el tablero muestra batch y NRT en la misma línea de tiempo.

### Día 5 — Viernes 11 · Integración y pruebas *(los dos)*

- DAG 05 de conciliación: agregar `cripto-nrt-metricas-*` a la granularidad del batch vía
  consulta REST a Elasticsearch, comparar contra `hechos_ohlcv_diario`, escribir
  `conciliacion`.
- Batería completa de la sección 8, con evidencia guardada.
- Capturas siguiendo `docs/GUIA_CAPTURAS.md`.

> **Salida:** tabla `conciliacion` poblada, con la desviación explicada.

### Día 6 — Sábado 12 · Documentación

- `README.md` que permita arrancar en menos de 15 minutos desde cero.
- `docs/ARQUITECTURA.md`, `REGLAS_NEGOCIO.md`, `DECISIONES.md`, `PRUEBAS.md`.
- Presentación según el guion de la sección 9.
- **Grabar el video de respaldo de la demo** (90 s).
- **Ensayo 1**, cronometrado.

### Día 7 — Domingo 13 · Cierre

- Arranque en limpio: `docker compose down -v` y levantar siguiendo **solo** el README,
  sin memoria ni atajos. Lo que falle, se corrige o se documenta.
- **Ensayo 2** con preguntas cruzadas: cada uno interroga la parte del otro.
- Etiquetar `v1.0`, verificar que el repositorio no lleva datos pesados ni credenciales,
  entregar.

---

## 8. Pruebas y validación

| # | Prueba | Cómo | Criterio |
|---|---|---|---|
| P1 | Lógica sin infraestructura | `python pruebas/prueba_logica_batch.py` | Cada defecto inyectado lo detecta su regla |
| P2 | Idempotencia del batch | Ejecutar el DAG 04 dos veces con el mismo lote | El conteo de filas no cambia |
| P3 | Dos lotes el mismo día | Dos corridas seguidas | Ambas en éxito, sin colisión de claves primarias |
| P4 | Deduplicación en NRT | Reenviar 1 000 trades ya procesados | `metricas.1min` no varía |
| P5 | Recuperación ante fallo | `docker compose restart spark-streaming` a mitad de flujo | Retoma desde el checkpoint, sin huecos ni duplicados |
| P6 | Eventos tardíos | Inyectar trades con `ts_evento` de hace 20 s y de hace 90 s | Los de 20 s entran en su ventana; los de 90 s se descartan por el watermark, y se explica por qué |
| P7 | Datos malformados | 5 % de eventos inválidos desde el simulador | Van a cuarentena; ni Spark ni Logstash se caen |
| P8 | Latencia extremo a extremo | `ts_evento` frente a `@timestamp` en ES, sobre 10 min de flujo | Reportar p50, p95, p99. No fijamos umbral: lo medimos y lo explicamos |
| P9 | Carga | Simulador a 500, 1 000 y 2 000 eventos/s | Hasta dónde aguanta y dónde crece el *lag* del consumidor en Kafka UI |
| P10 | Conciliación batch ↔ NRT | DAG 05 sobre un período con ambos flujos | Desviación del VWAP dentro del rango esperado; las discrepancias se explican |
| P11 | Mapeo de campos en ES | Consultar el mapeo del índice tras la primera indexación | `precio`, `vwap` y `volumen_*` son `double`, no `text`. Es el riesgo que el Taller 2 dejó abierto |
| P12 | Arranque desde cero | `down -v` y seguir solo el README | Funciona sin intervención extra |

**P5, P6, P8 y P10 son las que diferencian el proyecto.** Casi nadie mide latencia real,
demuestra recuperación ante fallo ni explica qué hace con un evento que llega tarde.

---

## 9. Exposición — 20 minutos + 5 de preguntas

Diez minutos por persona. Guion en `docs/GUION_EXPOSICION.md`.

| Min | Contenido | Quién |
|---|---|---|
| 0–2 | El problema y por qué exige dos flujos | Manuel |
| 2–5 | Arquitectura sobre el diagrama: fuentes, bus, zonas, dos almacenes y el porqué de cada elección | Manuel |
| 5–9 | Camino batch: DAGs, reglas de calidad, modelo dimensional, idempotencia | Manuel |
| 9–10 | Un problema concreto del batch: síntoma, causa real, solución | Manuel |
| 10–13 | Camino NRT: Kafka, ventana, watermark, dedup, checkpointing; Logstash como escritor único | Estéfano |
| 13–16 | **Demo en vivo**: Kibana moviéndose, disparar una alerta con el simulador, reiniciar Spark y mostrar que retoma | Estéfano |
| 16–18 | Pruebas de resiliencia y latencia medida | Estéfano |
| 18–20 | Conciliación batch ↔ NRT, resultados, limitaciones y qué faltaría para producción | Ambos |

**Reglas de la demo.** Todo levantado y con datos **antes** de empezar. Video de respaldo
de 90 s grabado el Día 6. Nada que dependa del internet del aula.

**Preparación de preguntas.** Cada uno escribe cinco preguntas duras sobre la parte del
otro y las hace en el ensayo del Día 7. Las probables: por qué Kafka si Logstash ya
ingiere; qué pasa con un evento que llega tarde; entrega exactamente-una-vez o
al-menos-una-vez y por qué; por qué dos almacenes y no uno; por qué Parquet y no CSV;
cómo escalaría a diez veces el volumen; qué costaría llevarlo a producción.

---

## 10. Riesgos y mitigaciones

| Riesgo | Prob. | Mitigación |
|---|---|---|
| El conector Kafka de Spark no resuelve por Ivy y el job no arranca | **Alta** | Horneado en el `Dockerfile` el Día 1 y verificado sin red ese mismo día. Es el riesgo número uno del proyecto |
| El exchange bloquea la IP o no hay internet en la demo | Alta | Simulador obligatorio desde el Día 2; prueba sin red el Día 7 |
| Elasticsearch infiere mal el mapeo y las agregaciones fallan | Alta | Plantilla de índice con tipos explícitos, prueba P11 |
| 16 GB no alcanzan con todo levantado | Media | Docker Desktop a 11-12 GB; Spark en `local[2]`; Airflow con LocalExecutor; ES con heap de 1 GB, Logstash 512 MB; 3 símbolos como máximo |
| Choque de puertos con los entornos anteriores | Alta | Mapa del punto 5.5; detener los otros compose antes de levantar este |
| El job de Spark consume más días de lo previsto | Media | Está en pareja el Día 3 y tiene fecha límite el Día 4. **Plan B declarado:** si al cierre del Día 4 no funciona, la agregación pasa a un *transform* continuo de Elasticsearch y se documenta el cambio con su motivo |
| El contrato de datos cambia a mitad de semana | Media | Se cierra el Día 1; todo cambio se avisa; el DDL está versionado |
| La documentación se deja para el final | Alta | `docs/DECISIONES.md` se escribe mientras se trabaja |
| La demo en vivo falla | Media | Video de respaldo del Día 6 |

---

## 11. Estructura del repositorio

```
proyecto-final-pipeline-cripto/
├── README.md                       Arranque en <15 min desde cero
├── PLAN.md                         Este documento
├── docker-compose.yml              Entorno completo unificado
├── .env.example                    Plantilla de variables, sin secretos
├── Dockerfile.airflow              Airflow + dependencias fijadas
├── Dockerfile.spark                Spark + conector de Kafka horneado
├── contratos/
│   ├── CONTRATO_DATOS.md           Interfaz entre los dos caminos
│   ├── esquema_trade.json
│   └── esquema_metrica.json
├── sql/
│   ├── 01_esquemas.sql
│   └── 02_tablas.sql
├── elasticsearch/
│   └── plantillas/cripto.json      Tipos explícitos, no mapeo dinámico
├── logstash/
│   ├── pipeline/logstash.conf      Extensión del Taller 2
│   └── config/                     Ajustes de heap y de la instancia
├── kibana/
│   └── tableros.ndjson             Tableros exportados, provisionados
├── ingesta_streaming/              ── Estéfano
│   ├── config.py
│   ├── cliente_websocket.py
│   ├── productor_kafka.py
│   └── simulador_trades.py
├── procesamiento_streaming/        ── Estéfano
│   ├── esquemas_spark.py
│   ├── job_metricas_ventana.py
│   └── reglas_alertas.py
├── dags/                           ── Manuel
│   ├── comun/                      Una capa por archivo
│   │   ├── config.py
│   │   ├── clientes_api.py
│   │   ├── zonas.py
│   │   ├── reglas_calidad.py
│   │   ├── transformaciones.py
│   │   ├── repositorio.py
│   │   ├── observabilidad.py
│   │   └── conciliacion.py
│   ├── dag_01_ingesta_batch.py
│   ├── dag_02_calidad.py
│   ├── dag_03_transformacion.py
│   ├── dag_04_carga_mysql.py
│   └── dag_05_conciliacion.py
├── datos_semilla/catalogo_activos.csv
├── pruebas/
│   ├── prueba_logica_batch.py
│   ├── prueba_logica_streaming.py
│   ├── prueba_latencia.py
│   └── prueba_carga.py
├── docs/
│   ├── ARQUITECTURA.md
│   ├── REGLAS_NEGOCIO.md
│   ├── DECISIONES.md
│   ├── PRUEBAS.md
│   ├── GUIA_CAPTURAS.md
│   └── GUION_EXPOSICION.md
├── capturas/
└── datos/                          Generado al ejecutar; no se versiona
    ├── bronce/ cuarentena/ plata/ exportado/ checkpoints/
```

**Principio de organización** —el mismo del taller de Airflow, que funcionó: los DAGs y
el job de Spark **solo orquestan y transforman**. Ninguna regla de negocio ni SQL vive
dentro de ellos. Las reglas son funciones puras en `dags/comun/` y en `reglas_alertas.py`,
lo que permite probarlas en segundos sin levantar infraestructura.

---

## 12. Cobertura de la rúbrica

| Criterio | Peso | Qué lo cubre |
|---|---|---|
| Diseño y arquitectura | 20% | Dos flujos con justificación explícita, bus desacoplado, dos almacenes con función distinta, zonas bronce/plata, contrato versionado, `DECISIONES.md` |
| Implementación técnica | 20% | Ventanas con watermark, dedup, checkpointing, idempotencia por lote, cuarentena, plantillas de índice explícitas, pruebas ejecutables |
| Uso de tecnologías | 20% | Kafka, Spark Structured Streaming, Airflow, Logstash, Elasticsearch, Kibana, MySQL, Parquet, Docker Compose — cada una con un motivo, ninguna decorativa |
| Documentación y presentación | 20% | README reproducible, seis documentos en `docs/`, diagrama, guion cronometrado, dos ensayos, video de respaldo |
| Resultados y validación | 10% | Doce pruebas con evidencia; latencia p50/p95/p99; conciliación cuantificada; prueba de eventos tardíos |
| Innovación y creatividad | 10% | La conciliación batch↔NRT, el pipeline que se observa a sí mismo por HTTP y TCP, la medición real de latencia, la recuperación ante fallo y el simulador con inyección de defectos |

---

## 13. Alcance excluido

Se declara, no se disimula:

- Almacenamiento de objetos tipo S3 (MinIO). El data lake es una carpeta con Parquet.
- Formatos de tabla transaccionales (Iceberg, Delta Lake).
- dbt para las transformaciones.
- Registro de esquemas (Schema Registry) y Avro. El contrato es un JSON Schema versionado
  y validado en el productor.
- Autenticación, gestión de secretos, TLS y respaldos. Elasticsearch corre con la
  seguridad desactivada, igual que en el Taller 2: aceptable en local, no replicable fuera.
- Integración continua en GitHub Actions.
- Modelos de predicción sobre el flujo.

**Qué haría falta para producción** —va en el cierre de la exposición: evaluación de
arquitectura, registro de esquemas gestionado, autenticación y secretos, alta
disponibilidad de Kafka y Elasticsearch, monitoreo y alertamiento, pruebas automatizadas
de la orquestación, infraestructura como código, soporte y continuidad. Nada de eso está
en el prototipo, y decirlo explícitamente es parte del rigor.

---

## 14. Primeros tres pasos, ahora

1. Estéfano comparte el repositorio del Taller 2 completo: `docker-compose.yml`,
   `logstash.conf` y los tableros de Kibana si los exportó.
2. Crear el repositorio en GitHub y clonarlo los dos.
3. Bloquear el lunes 7 completo en la agenda. Es el único día que no se puede trabajar
   por separado, y de él depende que la semana alcance.
