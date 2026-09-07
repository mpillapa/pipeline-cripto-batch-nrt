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

Este README describe el proyecto completo. **No todo está construido todavía**; el estado
real, entregable por entregable, está en **[AVANCE.md](AVANCE.md)**.

| Parte | Estado |
|---|---|
| Camino batch — módulo `comun/` y DAGs 01–04 | Escrito y probado sin Airflow |
| Camino near real-time | Pendiente |
| Entorno `docker compose` | Pendiente (sesión del Día 1) |
| Conciliación (DAG 05) | Pendiente, depende del flujo NRT |

**Lo que sí se puede ejecutar hoy** está en la sección [Probar la lógica sin
infraestructura](#probar-la-lógica-sin-infraestructura).

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

**F2 es obligatorio, no opcional.** Si el exchange bloquea la IP o no hay internet, la
demostración se cae. Con el simulador el pipeline es reproducible, las pruebas son
deterministas (semilla fija) y se pueden inyectar defectos para probar la cuarentena.

> **Regla del proyecto:** el pipeline debe funcionar de punta a punta **sin internet**. Lo
> que solo funciona con la API real, no está terminado.

**F6 y F7 hacen que el pipeline se observe a sí mismo.** Cada DAG publica por HTTP su
inicio, fin, filas procesadas y filas en cuarentena; el productor y Spark emiten sus logs
por TCP. En Kibana queda una vista de operación junto a la de negocio.

---

## 4. Probar la lógica sin infraestructura

Lo único que hace falta es Python con `pandas`, `numpy`, `pyarrow` y `requests`.

```powershell
python pruebas\prueba_logica_batch.py
```

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

> **Pendiente.** El `docker-compose.yml` se construye en la sesión del Día 1, fusionando
> el entorno ELK del Taller 2 con el de Kafka y Spark. Esta sección se completa entonces.

Puertos previstos, elegidos para no chocar con los entornos de los talleres anteriores,
que ya ocupan 8080, 8081, 3306, 3307 y 5432:

| Servicio | Puerto |
|---|---|
| Kibana | 5601 |
| Elasticsearch | 9200 |
| Logstash — beats / HTTP / TCP | 5044 / 8088 / 5000 |
| Airflow UI | 8092 |
| Kafka UI | 8093 |
| Kafka (externo) | 9095 |
| MySQL | 3308 |
| Spark — UI de la aplicación | 4040 |

**Antes de levantar este entorno hay que detener los otros dos** (`taller-airflow-5dags` y
`kafka-spark-zeppelin`). Con 16 GB no caben en simultáneo.

---

## 6. Estructura del repositorio

```
├── PLAN.md                     Plan de ejecución de la semana
├── AVANCE.md                   Estado real y bitácora de problemas resueltos
├── contratos/
│   └── CONTRATO_DATOS.md       Interfaz entre el camino batch y el NRT
├── sql/                        DDL. Fuente única del esquema de MySQL
├── dags/
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
│   └── dag_04_carga_mysql.py
├── datos_semilla/              Catálogo de activos, versionado
├── pruebas/                    Pruebas ejecutables sin infraestructura
├── docs/
│   └── REGLAS_NEGOCIO.md       Catálogo de reglas, fórmulas y supuestos
└── datos/                      Se crea al ejecutar; no se versiona
    └── bronce/ cuarentena/ plata/ exportado/
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
| [contratos/CONTRATO_DATOS.md](contratos/CONTRATO_DATOS.md) | Esquemas, tipos, unidades y nombres prohibidos |
| [docs/REGLAS_NEGOCIO.md](docs/REGLAS_NEGOCIO.md) | Catálogo de reglas, fórmulas y supuestos |

---

## 9. Limitaciones conocidas

- **Prototipo, no producción.** Llevarlo a producción exigiría evaluar arquitectura,
  registro de esquemas gestionado, autenticación y secretos, alta disponibilidad de Kafka
  y Elasticsearch, monitoreo, pruebas automatizadas de la orquestación, infraestructura
  como código, soporte y continuidad.
- **Credenciales en claro** en el compose. Aceptable en local; no replicar fuera.
- **Elasticsearch con la seguridad desactivada**, igual que en el Taller 2.
- **Sin pruebas automatizadas de los DAGs.** Las pruebas cubren la lógica de negocio, no
  la orquestación.
- **El VWAP del flujo NRT es aproximado por construcción**: se calcula solo con los trades
  recibidos. Eso no es un defecto oculto, es precisamente lo que mide la conciliación.
- **Los datos no representan la operación de nadie.** Son cotizaciones públicas de un
  exchange, o series sintéticas cuando no hay red.
