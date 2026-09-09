# Avance del proyecto

Estado real de cada entregable y bitácora de lo que se ha ido descubriendo.
Se actualiza **al cerrar cada entregable**, no al final del día: un avance que se
escribe de memoria tres días después pierde justo lo que sirve, que es el detalle
del problema y cómo se resolvió.

Alimenta directamente `docs/DECISIONES.md` y la parte de la exposición donde hay que
explicar *cómo resolvieron problemas específicos*.

**Última actualización:** 9 de septiembre de 2026
**Plan de referencia:** [PLAN.md](PLAN.md)

Leyenda: `LISTO` · `EN CURSO` · `PENDIENTE` · `BLOQUEADO`

---

## 1. Resumen

| Camino | Progreso | Comentario |
|---|---|---|
| Andamiaje conjunto | **Hecho** | Compose unificado levantado y verificado con los dos caminos corriendo encima |
| Batch — Manuel | **Funcionando** | Corrió de punta a punta con datos reales, dos veces, con idempotencia verificada. Los cinco DAGs, incluido el 05 de conciliación, ejecutados |
| NRT — Estéfano | **Funcionando** | Fusionado y corregido el 8 de septiembre. Trades y métricas llegan a Elasticsearch |
| Integración de los dos caminos | **Circuito cerrado** | Los tres flujos indexados: batch, trades y métricas de ventana |
| Conciliación entre flujos | **Midiendo el mercado real** | Fuente F1 (WebSocket) implementada el 9 de septiembre. Los dos flujos leen el mismo exchange, que es lo que hace que la comparación signifique algo |
| Documentación | Adelantada | Plan, contrato, reglas de negocio, README y este archivo |

**Estado del circuito, recontado el 9 de septiembre a las 16:10 UTC**, con las once
pruebas ejecutables pasadas y el tablero construido:

| Índice en Elasticsearch | Documentos | Origen |
|---|---|---|
| `cripto-nrt_trade-2026.09.09` | 1 213 057 | Exchange y simulador → Kafka → Logstash |
| `cripto-nrt_metrica-2026.09.09` | 658 | Trades → Kafka → **Spark** → Kafka → Logstash |
| `cripto-batch_ohlcv` | 1 092 | Airflow → Parquet → MySQL → NDJSON → Logstash |
| `cripto-batch_conciliacion` | 9 | DAG 05, comparando los dos flujos |
| `cripto-ops_control-2026.09.09` | 10 | El pipeline observándose a sí mismo |
| `cripto-nrt_alerta-2026.09.09` | 0 | Spark → Kafka → Logstash. Vacío porque el mercado está tranquilo: verificado bajando el umbral |
| `cripto-desconocido-2026.09.09` | 3 | Lo que Logstash no supo enrutar, marcado y no descartado |

Reparto de los trades por fuente: `exchange_ws` **1 151 534**, `simulador` 104 309.

> **Los tres documentos «desconocidos» son una buena señal, no un problema.** Son mensajes
> que Logstash no pudo parsear —dos los inyectó la prueba de malformados, y el tercero es
> un JSON truncado de una corrida anterior— y llevan las etiquetas `_jsonparsefailure` y
> `sin_tipo_fuente`. Descartarlos los haría invisibles y el síntoma sería «faltan datos»
> sin ninguna pista.

Las métricas de Spark cumplen el contrato campo por campo. Comprobación aritmética sobre
una ventana real: `volumen_usdt / volumen_base` = 351 251,94 / 103,2919 = 3400,58, que
coincide con el `vwap` publicado. Y `volatilidad_pct` = (3406,76 − 3393,26) / 3400,58 ×
100 = 0,397, que coincide con el valor emitido.

**Con la fuente real, 9 de septiembre a las 01:31 UTC.** Primera ventana alimentada por el
exchange en vez del simulador, contra el cierre de la vela horaria del batch:

| Símbolo | VWAP de Spark | Cierre real del batch | Desviación | Trades en 1 min |
|---|---|---|---|---|
| BTCUSDT | 78 756,73 | 78 774,65 | 0,02 % | 1 122 |
| ETHUSDT | 2 492,68 | 2 496,28 | 0,14 % | 688 |
| SOLUSDT | 103,53 | 103,78 | 0,24 % | 169 |

Las tres por debajo del umbral de 0,5 % del contrato. Compárese con lo que daba el
simulador el día anterior — −20 %, +36 % y +40 % — y con su volumen: BTC registra ahora
1 122 trades en **un minuto**, frente a los ~6 700 por **hora** del simulador.

Comprobación aritmética de la ventana de BTC: 878 433,62 / 11,15376 = 78 756,7, que
coincide con el `vwap` publicado.

---

## 2. Andamiaje conjunto — Día 1

| Entregable | Estado | Notas |
|---|---|---|
| Repositorio en GitHub | LISTO | `mpillapa/pipeline-cripto-batch-nrt`. Tres ramas publicadas: `main`, `batch/manuel` y `nrt/estefano` |
| `docker-compose.yml` unificado | LISTO | Completo y levantado: Postgres, MySQL con el DDL montado, Airflow, Zookeeper, Kafka con sus topics, Kafka UI, Elasticsearch, Kibana, Logstash, Spark y el productor. Los dos caminos han corrido encima |
| `Dockerfile.spark` con conector de Kafka horneado | LISTO | Los cuatro JAR dentro de la imagen. **Deja de ser el riesgo número uno**: el job arranca sin descargar nada |
| `Dockerfile.airflow` | LISTO | Imagen propia con `pyarrow`, construida y en uso (`pipeline-cripto-airflow:2.10.4`) |
| `Dockerfile.productor` | LISTO | Imagen del productor, con `websocket-client` y `kafka-python` |
| `requisitos/airflow.txt` | LISTO | Sin versiones fijadas: las decide el archivo de restricciones |
| `.env.example` | LISTO | Valores ficticios, incluidos los límites de memoria de Elasticsearch y Logstash |
| Mapa de puertos | LISTO | Sección 5 del README, verificado con el entorno arriba |
| Conexiones de Airflow en el compose | LISTO | `AIRFLOW_CONN_MYSQL_CRIPTO` y `AIRFLOW_CONN_FS_CRIPTO`, por variable de entorno y no en la base de metadatos: por eso **no aparecen en `airflow connections list`** y sí funcionan. La segunda la usa el `FileSensor` del DAG 03; **no sirve `fs_default`**, que solo existe si la base se inicializa con `--load-default-connections` |
| Montaje de `datos_semilla/` en el contenedor | LISTO | El DAG 04 lo lee desde `/opt/airflow/datos_semilla` |
| Persistencia de Elasticsearch | LISTO | Volumen `elasticsearch-datos`. Añadido el 9 de septiembre, ver bitácora: hasta entonces los índices vivían en la capa del contenedor |
| `contratos/CONTRATO_DATOS.md` | EN CURSO | Aplicado por los dos caminos, pero la **sección 10 sigue redactada como cinco decisiones abiertas** cuando las cinco ya se resolvieron en el código tal y como estaban propuestas: validación en el productor, 3 particiones, retención de 24 h, `ops_log` a Elasticsearch y los tres símbolos. Falta pasarlas al cuerpo del contrato y borrar esa sección |
| `sql/01_esquemas.sql` y `sql/02_tablas.sql` | LISTO | Cuatro tablas. Fuente única del esquema |
| Plantilla de índice de Elasticsearch | LISTO | 24 campos con tipos explícitos, aplicada al arrancar por `elasticsearch-init` |
| Diagrama de arquitectura | LISTO | `docs/arquitectura.html`, interactivo y con cuatro vistas guiadas. La fuente versionada es el JSON; el HTML se compila con archify y no se edita a mano |

---

## 3. Camino batch — Manuel

| Entregable | Estado | Notas |
|---|---|---|
| `comun/config.py` | LISTO | Parámetros, umbrales, conexiones |
| `comun/utilidades.py` | LISTO | Lote_id, rutas, NDJSON, tiempo en UTC |
| `comun/zonas.py` | LISTO | Parquet por zona y por lote |
| `comun/clientes_api.py` | LISTO | Descarga paginada, reintentos y respaldo sintético |
| `comun/reglas_calidad.py` | LISTO | R01–R08, funciones puras |
| `comun/transformaciones.py` | LISTO | Normalización, indicadores, dimensión |
| `comun/repositorio.py` | LISTO | Único acceso a MySQL, cargas idempotentes |
| `comun/observabilidad.py` | LISTO | Publicación a Logstash por HTTP y TCP |
| `comun/conciliacion.py` | LISTO | Consulta agregada a Elasticsearch, comparación y veredictos |
| `pruebas/prueba_conciliacion.py` | LISTO | 7 bloques, 30 comprobaciones, con respuesta de Elasticsearch simulada |
| `pruebas/prueba_logica_batch.py` | LISTO | 12 bloques, 42 comprobaciones, todas pasan |
| `dag_01_ingesta_batch` | LISTO | Una tarea de descarga por símbolo, en paralelo |
| `dag_02_calidad` | LISTO | Bifurcación promover/bloquear, cuarentena con motivo |
| `dag_03_transformacion` | LISTO | Sensor + transformación + verificación de la zona plata |
| `dag_04_carga_mysql` | LISTO | Carga idempotente + exportación NDJSON para Logstash |
| `dag_05_conciliacion` | LISTO | Ya no concilia contra el vacío: 6 documentos en `cripto-batch_conciliacion`, comparando ventanas de Spark alimentadas por el exchange contra el cierre de la vela del batch |
| `datos_semilla/catalogo_activos.csv` | LISTO | Diez activos con nombre y categoría |
| `docs/REGLAS_NEGOCIO.md` | LISTO | R01–R08, T01–T03, fórmulas, supuestos y parámetros |
| `README.md` | LISTO | Marca explícitamente lo que aún no existe |
| `.gitignore` | LISTO | Ignora zonas de datos, `__pycache__`, `.env` y artefactos |

**Verificado sin infraestructura:**

| Comprobación | Resultado |
|---|---|
| `pruebas/prueba_logica_batch.py` | 42 de 42 |
| `pruebas/prueba_conciliacion.py` | 30 de 30 |
| Flujo bronce → cuarentena → plata sobre Parquet real | Correcto |
| Tipos y precisión tras el viaje a Parquet | `float64`, `int64`, `str`; precio de 8 decimales idéntico |
| Nulos convertidos a `None` para MySQL | Correcto |
| API pública desde la red del equipo | Responde con datos reales |

**Verificado con el entorno levantado — 6 de septiembre:**

El camino batch corrió **de punta a punta con datos reales**, dos veces.

| Comprobación | Resultado |
|---|---|
| Construcción de la imagen de Airflow | Correcta |
| `airflow dags list-import-errors` | Sin errores |
| DAGs detectados | Los cuatro |
| DDL ejecutado sin pasos manuales | Las cuatro tablas creadas al arrancar MySQL |
| Conexiones `mysql_cripto` y `fs_cripto` | Resuelven desde las variables de entorno |
| Pipeline encadenado 01 → 02 → 03 → 04 | Los cuatro DAGs en `success` |
| Datos cargados | 1092 velas, 3 activos, 364 días por símbolo |
| Calidad sobre datos reales | 1092 evaluadas, 0 rechazadas, tasa 0,00 % |
| Zonas en disco | bronce, plata, cuarentena y exportado, con Parquet y NDJSON |
| **Segunda corrida del mismo día** | Los cuatro DAGs en `success` |
| **Idempotencia** | **1092 filas tras dos corridas, no 2184** |
| DAG 05 sin flujo NRT levantado | `success` con 0 horas y el motivo en el reporte |
| **Rama de bloqueo del DAG 02** | `bloquear_lote` en `success`, `promover_lote` y `disparar_dag_03` en `skipped`, cadena detenida |
| Cuarentena del lote bloqueado | 259 de 600 filas apartadas, tasa 43,17 %, `cuarentena.parquet` en disco |
| Reglas disparadas por los defectos inyectados | R01: 48, R02: 150, R03: 112, R04: 53, R07: 56 |
| Bitácora `control_lotes` | Tres estados distintos: `CARGADO`, `CONCILIADO`, `BLOQUEADO` |

La clasificación de volatilidad produce además una distribución coherente sin haber sido
calibrada contra estos datos: BTC no tiene ningún día `ALTA`, mientras que ETH y SOL sí
(28 y 47 días respectivamente). Los cortes de `config.py` resultan razonables tal como
estaban, y se eligieron antes de ver los datos.

**El camino batch está completo y probado en las dos ramas.** Lo único que falta por
verificar es lo que depende del flujo NRT: que la conciliación produzca filas reales.

---

## 4. Camino NRT — Estéfano

> **Nota sobre los estados.** Se reserva `LISTO` para lo que se ha **ejecutado y
> comprobado**; `ESCRITO` es código que existe y compila pero que todavía no se ha visto
> funcionar. La distinción no es burocracia: en la revisión del 8 de septiembre varias
> piezas marcadas como listas resultaron no completar el circuito, y un registro que no
> distingue las dos cosas deja de servir para saber qué falta.

| Entregable | Estado | Notas |
|---|---|---|
| `ingesta_streaming/simulador_trades.py` | LISTO | Ejecutado. `id_trade` corregido a contador entero por símbolo: era aleatorio y eso hacía **imposible** probar la deduplicación |
| `ingesta_streaming/cliente_websocket.py` | LISTO | Fuente F1. Conexión al exchange real y traducción pura al contrato. 285 422 trades con `origen=exchange_ws` indexados |
| `ingesta_streaming/observabilidad.py` | LISTO | Eventos `ops_control` del productor, con la biblioteca estándar |
| `ingesta_streaming/productor_kafka.py` | LISTO | Elige fuente por `CRIPTO_FUENTE_TRADES`, con caída al simulador y aviso. Las dos fuentes se consumen como generadores con la misma interfaz |
| Topics de Kafka | LISTO | Los crea `kafka-init`. Verificados con tráfico real de las dos fuentes |
| `procesamiento_streaming/job_metricas_ventana.py` | LISTO | 516 métricas emitidas y verificadas aritméticamente contra el `vwap` publicado. Reescrito el 8 de septiembre: emitía 5 campos de los 10 del contrato, sin `volumen_usdt` ni `n_trades`, que son los que necesita la conciliación. Ver bitácora |
| `procesamiento_streaming/esquemas_spark.py` | LISTO | Ampliado de 5 a los 11 campos del contrato |
| `logstash/pipeline_cripto/logstash.conf` | LISTO | Tenía 1 input de 5. Añadidos `metricas.1min`, `alertas.precio`, el `file` del NDJSON batch, y los del 8088 y 5000. Los cinco flujos han indexado |
| Plantilla de índice de Elasticsearch | LISTO | El JSON estaba bien, pero nada lo aplicaba. Añadido el servicio `elasticsearch-init` |
| `pruebas/prueba_websocket.py` | LISTO | 8 bloques, todas pasan. No necesita red ni Kafka |
| Tableros de Kibana | LISTO | 5 patrones de índice, 7 visualizaciones y 1 tablero, provisionados al arrancar por `kibana-init`. Refresco de 30 s, ventana de 6 h |
| `procesamiento_streaming/reglas_alertas.py` | LISTO | Reescrito como reglas puras: umbrales y clasificación de severidad, sin Spark y con pruebas. El job importa los umbrales de aquí |
| `pruebas/prueba_latencia.py` | LISTO | Ejecutada. Tres etapas medidas; la 3 exigió añadir el sello `ts_indexado` en Logstash. p50 de 162 ms en el tramo hasta Logstash |
| `pruebas/prueba_logica_streaming.py` | LISTO | **Ejecutada por fin el 9 de septiembre: 7 de 7.** Faltaban dos cosas para poder correrla, ver bitácora: montar `pruebas/` en el contenedor y lanzarla con `spark-submit` en vez de `python3` |
| `pruebas/prueba_carga.py` | LISTO | Reescrita para medir el **lag del consumidor** y no la tasa de inyección. Aguanta 500, 1 000 y 2 000 ev/s |
| `Dockerfile.spark` | LISTO | Los cuatro JAR horneados. **Resuelve el riesgo número uno del proyecto**: el job arrancó sin descargar nada |
| Servicios ELK en el compose | LISTO | Elasticsearch, Kibana y Logstash 7.17.10. `spark-streaming` no tenía `command`: corregido. Elasticsearch no tenía volumen: corregido el 9 de septiembre |
| Alertas | LISTO | Se generan en Spark y se publican en `alertas.precio`, como dice el contrato. Verificadas de punta a punta hasta `cripto-nrt_alerta-*`, campo por campo |

---

## 4.bis Qué queda por hacer

Los dos caminos funcionan, las once pruebas ejecutables pasan y el tablero está
construido. Lo que falta es **recoger evidencia y ensayar**.

| Pendiente | De quién | Notas |
|---|---|---|
| Capturas de `capturas/` | Los dos | Catorce fichas en [docs/GUIA_CAPTURAS.md](docs/GUIA_CAPTURAS.md), por orden de importancia. Necesita el circuito con 30 min de datos |
| Vídeo de respaldo de 90 s | Los dos | Guion en la guía de capturas. Sin voz: se narra en directo |
| **P12 — arranque desde cero** | Los dos | `down -v` y levantar solo con el README. **Va al final**: borra la evidencia de todo lo demás |
| Ensayo 1, cronometrado | Los dos | Día 6. Objetivo: caber en 20 minutos |
| Ensayo 2, con preguntas cruzadas | Los dos | Día 7. Cinco preguntas duras sobre la parte del otro |
| Etiquetar `v1.0` y entregar | Manuel | Comprobar que el repositorio no lleva datos pesados ni credenciales |

**Dos anomalías conocidas, ninguna bloqueante:**

- El lote `L20260907_020115` figura como `CONCILIADO` con `filas_cargadas` en NULL. Los
  datos están; la bitácora quedó a medias.
- `main` sigue en el commit del camino batch. Para la entrega, el tag debería salir de
  `main`, no de una rama de trabajo.

---

## 5. Bitácora de hallazgos y decisiones

### 2026-09-09 (tarde) · Cinco defectos que solo aparecen cuando se ejecutan las pruebas

Jornada de cerrar pendientes. Todo lo que sigue estaba escrito y se daba por bueno.

**1. Spark llevaba horas sin emitir una sola ventana, y nada lo indicaba.**
59 reinicios acumulados. La causa: el checkpoint se montaba en `./datos/checkpoints`, y
`airflow-init` hace `chown -R ${AIRFLOW_UID:-50000}:0` sobre todo `datos/`. Spark corre
como uid 185, así que **cada inicialización de Airflow dejaba al job sin permiso de
escritura sobre su propio estado**. Moría con `FileNotFoundException ... (Permission
denied)` y `restart: on-failure` lo levantaba para que volviera a morir.

Es un conflicto entre los dos caminos que ninguno de los dos podía ver por separado. El
checkpoint vive ahora en el volumen nombrado `spark-checkpoints`: es estado interno del
motor, no una zona de datos, y no tiene por qué estar en el árbol que Airflow reclama.

**2. La prueba de carga medía el `sleep` del simulador, no Kafka.**
Reportaba 34 eventos/s con un objetivo de 2000. `generar_trade()` incluye un
`time.sleep(0.01–0.05)` para imitar latencia de red, que promedia 30 ms y **topa la
generación en unos 30 eventos por segundo**. La prueba concluía que el bus no daba más.
Ahora `generar_trade(latencia_simulada=False)` lo desactiva, y el productor alcanza las
tres tasas.

Con eso arreglado apareció el defecto de fondo: la prueba medía **la tasa de inyección**,
es decir el productor contra sí mismo. Si el proceso logra empujar 2000 ev/s imprime
éxito, aunque Logstash vaya cinco minutos por detrás. Reescrita para medir el **lag del
consumidor**, que es lo que pide el criterio del plan. Resultado: aguanta las tres tasas;
20 000 mensajes acumulados se drenan en 4 segundos.

**3. La etapa 3 de la latencia no era «no medible», solo faltaba un campo.**
`prueba_latencia.py` declaraba el tramo Kafka → Logstash → Elasticsearch como hueco. Se
resolvió con una línea en el filtro de Logstash: un `ruby` que sella `ts_indexado` con la
hora de entrada. **No sirve `@timestamp`**, porque Logstash lo deriva de `ts_evento` y la
resta daría cero siempre — que es exactamente lo que hacía la primera versión de la
prueba, la que reportaba 0,00 ms en los tres percentiles. Medido: p50 162 ms, p95 532 ms.

**4. La plantilla de índice solo cubría el flujo NRT.**
Los 24 campos declarados eran todos del camino rápido. **Todo el batch se mapeaba
dinámicamente**, y por eso `veredicto` había quedado como `text`: el panel de conciliación
—el entregable estrella— no podía agrupar por él, y salía vacío sin ningún error. Es el
mismo fallo silencioso que la plantilla existe para evitar, esta vez por omisión y no por
un campo suelto. Añadidos 20 campos del batch y 7 de las alertas: de 25 a 52.

De paso se corrigió `origen`, que seguía como `text` en el índice del día. Se reindexó en
vez de dejarlo: 998 982 documentos, 0 fallos, y el reparto por fuente intacto
(`exchange_ws` 894 533, `simulador` 104 449). La nota anterior decía que no se corregía
para no perder la evidencia; reindexar la conserva, así que no había tal disyuntiva.

**5. El DAG exportaba 9 filas y Elasticsearch recibía 6.**
El input `file` de Logstash va en modo `tail` y recuerda por inodo hasta dónde leyó cada
archivo. El DAG 05 escribía siempre `conciliacion.ndjson`, así que al sobrescribirlo
Logstash **retomaba desde el desplazamiento anterior** en vez de leer el contenido nuevo.
Sin error, sin aviso: simplemente faltaban filas en el índice del que lee el tablero.

Arreglado en los DAG 04 y 05 con una marca de tiempo en el nombre del archivo. Las 9 filas
están indexadas.

**Lo que tienen en común los cinco.** Ninguno daba error. Un job que se reinicia, una
prueba que devuelve un número plausible, un panel vacío, un índice con menos filas de las
esperadas. **El patrón del proyecto entero es el fallo silencioso**, y es lo que conviene
llevar a la exposición: no la lista de defectos, sino que todos se parecen.

---

### 2026-09-09 (tarde) · Las alertas se generan en Spark, no en Kibana

Era la última decisión de diseño abierta. Se resuelve **como decía el contrato**: la
alerta se publica en el topic `alertas.precio` y la emite el job.

La implementación anterior creaba una regla en Kibana Alerting por API, y además no habría
funcionado: declaraba `rule_type_id: metrics.alert.threshold` con parámetros de `es_query`,
y apuntaba a `localhost:5602`, que no resuelve desde dentro de la red de Docker.

**El motivo de fondo no es formal.** Una regla de Kibana vive dentro de Kibana: no es un
dato, no viaja por el bus, no se puede reprocesar ni conciliar, y desaparece si alguien
reconstruye la instancia. Publicada en Kafka, la alerta es un evento como cualquier otro,
con su `id_alerta`, y Logstash la indexa por el mismo camino que trades y métricas. Con
esto **los cinco flujos del contrato están cerrados**.

**Se deriva de las ventanas ya agregadas, no de un segundo recorrido del stream.** Una
ventana que supera el umbral produce a la vez su métrica y su alerta, así que las dos
cuentan lo mismo. Son dos `writeStream` sobre el mismo DataFrame, y cada uno necesita su
propio checkpoint: compartirlo hace que se pisen los offsets.

**El umbral está calibrado contra el mercado, no elegido a ojo.** La volatilidad por minuto
de BTC/ETH/SOL tiene mediana 0,13 % y p90 0,30 %, así que el 0,50 % del contrato casi nunca
saltaría. El compose lo baja a 0,25 %: suena en los minutos movidos y calla en los
tranquilos. Una alerta que no suena nunca no se puede demostrar.

**La regla vive en `reglas_alertas.py`, no en el job.** Umbrales y clasificación de
severidad en Python puro, con sus pruebas. El job importa los umbrales y construye la
expresión de columna equivalente. Se duplica la lógica a propósito —una UDF de Python por
fila serializa entre la JVM y el intérprete, y eso pesa en un stream— y por eso hay una
prueba que compara las dos expresiones en los bordes: es lo que impide que se separen.

---

### 2026-09-09 (tarde) · El tablero de Kibana, y por qué estaba vacío

`kibana/tableros.ndjson` exportaba dos objetos: un patrón de índice y un dashboard **sin
paneles**. El entregable de visualización no existía.

Construido: 5 patrones de índice, 7 visualizaciones y 1 tablero. Los paneles cuentan la
historia del proyecto en el orden en que se explica —salud del circuito, reparto por
fuente y conciliación arriba; VWAP y trades en medio; volatilidad y serie del batch
abajo—, con refresco de 30 s y ventana de 6 horas.

**Se provisiona solo.** El servicio `kibana-init` importa el NDJSON al arrancar, igual que
`elasticsearch-init` con la plantilla. Sin eso, el tablero sería un archivo que hay que
acordarse de importar a mano: estaría en el repositorio y no en la demo.

El `healthcheck` de Kibana no puede ser `/api/status`: responde antes de que los saved
objects estén listos, y la importación falla con «Kibana server is not ready yet». Se
comprueba contra `_export`, que da 200 solo cuando de verdad se puede escribir.

---

### 2026-09-09 · Elasticsearch guardaba 135 MB de evidencia donde un `down` los borra

Revisando por qué el productor llevaba media hora escupiendo errores en la consola
aparecieron dos cosas, una ruidosa y sin importancia y otra callada y grave.

**La ruidosa.** Kafka, Zookeeper, Elasticsearch, Kibana y Logstash estaban parados con
código de salida 143 —SIGTERM, un apagado limpio, no una caída—, mientras `productor` y
`spark-streaming` seguían levantados. Los dos declaran `restart: on-failure`, así que
llevaban 26 minutos reintentando contra un bus que no existía:

```
Conectando a Kafka en kafka:29092...
kafka.errors.NoBrokersAvailable: NoBrokersAvailable
```

No hubo ningún error de código. Fue orden de arranque: se levantaron los productores sin
levantar antes el bus y el ELK. La lección práctica es que `restart: on-failure` sobre un
servicio que depende de otro **convierte una dependencia no satisfecha en un traceback
repetido cada pocos segundos**, y ese ruido esconde lo que sí importa.

**La callada.** `docker inspect elasticsearch_cripto` devolvía `Mounts: []`. El servicio
no declaraba ningún volumen, de modo que los índices vivían en la capa escribible del
contenedor. Eso sobrevive a un `stop` —por eso nadie lo había notado— y **lo borra
cualquier `down`**, que es justo la orden que uno teclea sin pensar al terminar el día.
Lo que estaba en juego: 318 581 trades, 516 métricas de ventana, los 1 092 documentos del
batch y las 6 filas de conciliación. 135 MB que no se regeneran, porque incluyen las
ventanas del exchange real de una franja horaria que ya pasó.

El `.gitignore` llevaba desde el principio una regla para `elasticsearch/datos/`, es decir
que el volumen **se había dado por hecho sin llegar a escribirse**. Ignorar una ruta que
nadie crea no da ningún error: la regla parecía la prueba de que el volumen existía.

**Rescate y arreglo.** El contenedor estaba parado, no eliminado, así que los datos aún
estaban ahí. `docker cp` desde el contenedor parado, volcado a un volumen nombrado nuevo
con `chown -R 1000:0`, y el servicio recreado apuntando a él. Los 14 shards recuperados.

**Volumen nombrado y no `./elasticsearch/datos`.** Un bind mount en Windows le entrega al
contenedor un directorio cuyo propietario no es el uid 1000 con el que corre
Elasticsearch, y el nodo no arranca por permisos. Es la razón por la que la regla del
`.gitignore` no habría funcionado ni escribiéndola.

**Kibana no necesita volumen propio**, aunque parezca que sí: sus tableros y patrones de
índice se guardan en el índice `.kibana` dentro de Elasticsearch. Con este volumen quedan
cubiertos los dos.

---

### 2026-09-09 · La prueba de Spark no era inejecutable, era inalcanzable

`prueba_logica_streaming.py` llevaba días marcada como *escrita pero sin ejecutar*, con el
motivo «importa `pyspark`, que no está instalado en la máquina; hay que correrla dentro
del contenedor». El motivo era correcto y la conclusión no: dentro del contenedor tampoco
corría, por dos razones distintas que se descubren una detrás de otra.

**El contenedor no veía el archivo.** `spark-streaming` montaba `./procesamiento_streaming`
y `./datos/checkpoints`, y nada más. `pruebas/` no estaba montado, así que el único sitio
donde la prueba podía correr era el único sitio donde no existía. Añadido
`./pruebas:/opt/spark/pruebas:ro`.

**`python3` no encuentra pyspark; `spark-submit` sí.** En la imagen oficial de Spark,
pyspark vive en `/opt/spark/python` y no está instalado como paquete del sistema. Lanzar
`python3 la_prueba.py` da `ModuleNotFoundError: No module named 'pyspark'` incluso con
Spark entero en la imagen, porque es `spark-submit` quien arma el `PYTHONPATH`. El comando
que funciona:

```bash
docker compose run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit \
  spark-streaming --master "local[2]" /opt/spark/pruebas/prueba_logica_streaming.py
```

`--no-deps` importa: sin él, Docker levanta Kafka y espera a que esté sano para una prueba
que no toca Kafka.

**Resultado: 7 de 7.** Cubren lo que había que cubrir: que el VWAP pondera por cantidad y
no es el promedio simple, que `volatilidad_pct` es rango relativo y no desviación típica,
que OHLC sale del orden temporal y no del de llegada, que `ventana_inicio` es inclusivo y
`ventana_fin` exclusivo, y que `origen_datos` distingue la fuente —que es de lo que
depende la regla C01 de la conciliación.

**Lo que esto deja como lección**: una prueba que nadie ha conseguido ejecutar no es
evidencia de nada, por muy escrita que esté. Las otras dos que siguen sin correr
—`prueba_latencia` y `prueba_carga`— están exactamente en esa situación.

---

### 2026-09-09 · Dos pruebas que pasaban sin probar nada

Al ejecutar por primera vez las dos pruebas que nunca se habían corrido, ninguna falló.
Las dos estaban rotas.

**`prueba_latencia.py` medía 0,00 ms en p50, p95 y p99.** No era una latencia excelente:
era una resta de un valor consigo mismo. Medía `@timestamp − ts_evento`, y Logstash **fija
`@timestamp` a partir de `ts_evento`**:

```
date { match => ["ts_evento", "ISO8601"] target => "@timestamp" }
```

La diferencia es cero por construcción, en cualquier máquina y con cualquier volumen. Un
resultado que además es el que uno querría ver, lo que lo hace más difícil de cuestionar.

**`prueba_logica_streaming.py` decía "aplicamos la misma lógica del job principal" y a
continuación la copiaba** dentro del propio archivo de prueba. Dos consecuencias:

1. Si el job cambiaba, la prueba seguía pasando. No probaba el job: probaba una copia del
   job que solo existía dentro de la prueba.
2. La copia ni siquiera era fiel. Deduplicaba con `dropDuplicates(["id_trade"])`, y el job
   usa `dropDuplicatesWithinWatermark(["simbolo", "id_trade"])`. Los datos de ejemplo no
   tenían ningún `id_trade` repetido entre símbolos distintos, así que las dos versiones
   daban el mismo resultado y la diferencia nunca se manifestaba. Además declaraba
   `id_trade` como `StringType`, cuando el contrato lo define como `long`.

**El patrón común es el mismo que el del índice con guion medio y el del campo mapeado
como `text`:** ninguno de los tres da error. Todos devuelven un resultado plausible. En
una prueba eso es peor que en el código, porque la prueba es justamente lo que debería
avisar.

**Qué se hizo**

`prueba_latencia.py` se reescribió para medir solo lo que los campos permiten medir:

| Etapa | Cálculo | Resultado |
|---|---|---|
| 1. Exchange → productor | `ts_ingesta − ts_evento` | p95 62 ms, **58,6 % de valores negativos** |
| 2. Cierre de ventana → métrica | `ts_procesado − ventana_fin` | p50 87 s |
| 3. Kafka → Logstash → ES | — | **No medible**, se declara como hueco |

El 58,6 % de valores negativos no es un defecto: es la medida de que **las dos marcas
vienen de relojes distintos**. `ts_evento` lo pone el exchange y `ts_ingesta` el
contenedor, que va unos 12 ms adelantado. Una latencia negativa es imposible, así que esa
columna cuantifica cuánto contamina el desfase. Contra el simulador, donde ambas marcas
salen del mismo reloj, es 0 %. La prueba ahora informa ese porcentaje siempre, en vez de
esconderlo dentro de una mediana.

Los 87 s de la etapa 2 se descomponen y cuadran con el diseño: 30 s de watermark, más
30 s porque **el watermark de Spark va un micro-batch por detrás** —el que se aplica en un
lote es el máximo `ts_evento` visto en el anterior—, más hasta 30 s de espera al trigger.
Total esperado 60–90 s. El motor no va atrasado. Para bajarlo, el parámetro con más efecto
es `CRIPTO_INTERVALO_LOTE`, porque interviene en dos de los tres sumandos.

**Para arreglar la prueba de Spark hubo que hacer el job testeable.** `agregar()` se
separó en `preparar()` (watermark y deduplicación, que exigen un flujo) y
`calcular_metricas()` (ventana y agregaciones, que funciona igual sobre un DataFrame
estático). El `.agg()` no cambia, así que el esquema del estado tampoco y el checkpoint
sigue siendo válido.

La prueba ahora importa `calcular_metricas` y ejerce la aritmética que corre en
producción: **7 casos, todos pasan**. Cubre el VWAP ponderado (comprobando explícitamente
que *no* coincide con el promedio simple), el OHLC con las filas desordenadas a propósito,
la volatilidad como rango relativo, la separación por símbolo, los límites de ventana, los
tres valores de `origen_datos` incluido el mixto, y los 16 campos del contrato en la
salida.

**La etapa 3 queda pendiente y sin inventar.** Para medirla, Logstash tiene que sellar la
hora de indexación en un campo propio (`ruby { code => "event.set('ts_indexado', ...)" }`).
No se aplicó todavía porque reiniciar Logstash habría interrumpido la indexación durante
la hora que se estaba midiendo para la conciliación.

---

### 2026-09-09 · La conciliación contra el mercado real, y por qué `DESVIADO` es la respuesta correcta

Primera conciliación con la fuente F1 activa. La hora 01:00–02:00 UTC, con el productor
real corriendo desde las 01:29 —o sea, **media hora de las dos**:

| Símbolo | VWAP streaming | Cierre batch | Desviación | Cobertura | Veredicto |
|---|---|---|---|---|---|
| BTCUSDT | 78 840,86 | 78 891,50 | **−0,064 %** | 44,98 % | DESVIADO |
| ETHUSDT | 2 496,86 | 2 497,80 | **−0,038 %** | 44,91 % | DESVIADO |
| SOLUSDT | 103,74 | 103,79 | **−0,052 %** | 55,92 % | DESVIADO |

**La desviación es de cuatro centésimas de punto porcentual**, diez veces por debajo del
umbral de 0,5 %. El veredicto `DESVIADO` sale enteramente de la cobertura: 45 % contra un
mínimo del 60 %, porque el flujo en vivo solo escuchó 31 de los 60 minutos.

**Este resultado es el que valida el diseño de la métrica, no el que lo cuestiona.** La
conciliación está diciendo dos cosas a la vez y las está separando bien: *el precio que
calcula el streaming coincide con el del exchange* y *no escuché la hora entera, así que
no te fíes del todo*. Un solo número no podría decir ambas. Es exactamente el motivo por
el que el contrato sostiene que `cobertura_pct` importa más que `desviacion_pct`.

La comprobación aritmética cuadra: 41 721 / 92 751 = 44,98 %, y
(78 840,86 − 78 891,50) / 78 891,50 × 100 = −0,064 %.

**El contraste queda registrado en la propia tabla**, que conserva la hora anterior con el
simulador. Una sola consulta a `conciliacion` muestra las dos poblaciones:

| Hora | Fuente | Desviación BTC | Cobertura |
|---|---|---|---|
| 00:00 | Simulador | −20,03 % | 8,08 % |
| 01:00 | Exchange real | −0,064 % | 44,98 % |

Tres órdenes de magnitud de diferencia en la desviación, con el mismo código a ambos
lados. Es la mejor evidencia que tiene el proyecto de que la conciliación mide algo real,
y conviene llevarla así a la exposición: **las dos filas juntas**, no solo la buena.

---

### 2026-09-09 · El índice que no existía por un guion, y la conciliación que medía el simulador

Dos hallazgos encadenados. El primero explica por qué la conciliación devolvía cero; el
segundo, por qué al arreglarlo los números seguían sin significar nada.

**1. `cripto-nrt-metrica-*` contra `cripto-nrt_metrica-*`.** El nombre del índice sale del
campo `tipo_fuente`, que el contrato define con **guion bajo** (`nrt_metrica`). En
`config.py` estaba escrito con **guion medio**. Elasticsearch no devuelve error ante un
patrón que no casa con ningún índice: devuelve `count: 0` con `0 shards`, que es
exactamente lo que devolvería un índice real y vacío. La conciliación reportaba
`SIN_DATOS` y no había forma de distinguirlo de "el streaming no ha escrito todavía".

Corregido en `config.py`, `CONTRATO_DATOS.md` y `README.md`. **La lección no es el
guion:** es que el nombre del índice se derive de un valor del contrato y aun así se
escriba a mano en otro sitio. Cualquier nombre que se teclee dos veces acaba divergiendo.

**2. La conciliación funcionaba, pero comparaba dos mercados distintos.** Corregido el
nombre, el DAG 05 escribió sus tres filas. Estos fueron los números:

| Símbolo | VWAP streaming | Cierre batch | Desviación | Cobertura | Veredicto |
|---|---|---|---|---|---|
| BTCUSDT | 62 998,12 | 78 774,65 | −20,03 % | 8,08 % | DESVIADO |
| ETHUSDT | 3 399,99 | 2 496,28 | +36,20 % | 10,05 % | DESVIADO |
| SOLUSDT | **145,00** | 103,78 | +39,72 % | 33,45 % | DESVIADO |

El `145,00` clavado de SOL es la pista: es exactamente la constante
`SIMBOLOS["SOLUSDT"]["precio_base"]` del simulador. El simulador genera precios como una
caminata de ±0,2 % alrededor de tres constantes escritas a mano (63 000, 3 400, 145) que
no tienen relación con el mercado. La conciliación estaba midiendo, con toda corrección,
la distancia entre esas constantes y el precio real.

**El mecanismo estaba bien; la comparación no.** Y la cobertura decía lo mismo por otra
vía: el simulador produce unos 6 700 trades por hora contra los 82 550 reales de BTC.

---

### 2026-09-09 · Fuente F1: productor de WebSocket contra el exchange real

La conclusión del hallazgo anterior es que **la conciliación solo mide algo si los dos
flujos leen el mismo mercado**. Eso obliga a la fuente F1 del contrato, que estaba
pendiente. Implementada.

**Qué se añadió**

| Archivo | Papel |
|---|---|
| `ingesta_streaming/cliente_websocket.py` | Conexión al exchange y traducción al contrato |
| `ingesta_streaming/observabilidad.py` | Eventos `ops_control` del productor, con la biblioteca estándar |
| `pruebas/prueba_websocket.py` | 40 comprobaciones de la traducción, sin red ni Kafka |

`productor_kafka.py` pasa a elegir fuente por `CRIPTO_FUENTE_TRADES` (`websocket` por
defecto, `simulador` para trabajar sin red y para las pruebas de carga y deduplicación).
Las dos fuentes se consumen como generadores con la misma interfaz, así que el bucle de
publicación no sabe cuál está usando.

**La traducción está separada de la conexión, y eso es lo importante.** `traducir_trade()`
es una función pura: recibe el payload del exchange y devuelve el evento del contrato. Se
prueba sin socket, sin Kafka y sin la librería instalada — la importación de
`websocket-client` está dentro de `abrir_flujo()` justamente para eso.

**Los tres detalles del formato del exchange que rompen en silencio:**

- **Precio y cantidad llegan como cadena**, no como número: el exchange lo hace para no
  perder precisión al serializar. Sin convertirlos, Spark recibe texto donde su esquema
  declara `double` y la columna sale **nula sin ningún error**.
- **El símbolo va en minúsculas en el nombre del canal** (`btcusdt@trade`) y en mayúsculas
  en el campo `simbolo`. En mayúsculas el socket conecta correctamente y no llega ni un
  mensaje: no hay error, solo silencio.
- **`ts_evento` sale de `T`** (hora del trade), no de `E` (hora del evento). Se parecen y
  difieren en decenas de milisegundos, que es justo la magnitud que mide la prueba de
  latencia.

Cada uno tiene su comprobación en `prueba_websocket.py`, con el motivo escrito al lado.

**Verificado contra el exchange real.** Conexión TCP+TLS desde la red de Docker, y seis
trades traducidos: BTC 78 666,01, ETH 2 490,17 — precios del mercado, no constantes. Los
`id_trade` llegan consecutivos (6666798256, 257, 258…), lo que confirma que son únicos y
crecientes por símbolo, que es lo que la deduplicación de Spark da por supuesto. La
latencia de ingesta (`ts_ingesta − ts_evento`) sale en **9 ms**.

**La caída al simulador es ruidosa a propósito.** Si el exchange no responde, el productor
cae a F2 (salvo `CRIPTO_RESPALDO_SIMULADOR=false`), avisa por consola y publica un evento
`ops_control`. Pero lo que de verdad protege el análisis posterior no es el aviso: es que
cada trade lleve su `origen`. Un aviso se pierde en un log; el campo viaja con el dato.

**Limitación declarada.** Este flujo depende de que el exchange sea alcanzable el día de
la demo. No hay forma de quitar esa dependencia sin volver a datos inventados. Por eso se
conserva el simulador, y por eso el `origen` es obligatorio.

---

### 2026-09-09 · `origen_datos`: sin este campo, la conciliación no es auditable

Tener dos fuentes crea un problema nuevo: **una vez agregada la ventana, ya no se sabe de
dónde salieron sus trades.** El campo `origen` de la métrica vale `spark_streaming`
— dice quién la calculó, no qué la alimentó. Sin distinguirlos, cualquiera podría
conciliar ventanas del simulador contra velas reales y volver al mismo error, ahora sin
la pista del `145,00`.

Se añade `origen_datos` a la métrica, y el DAG 05 solo concilia
`origen_datos = exchange_ws` (`CONCILIACION_ORIGEN_DATOS`).

**Se agrega con `min` y `max`, no con `collect_set`.** Las agregaciones de colección no
son fiables en agregaciones de streaming. Con solo dos valores posibles, comparar el
mínimo con el máximo distingue exactamente los tres casos: `exchange_ws`, `simulador` o
`exchange_ws+simulador` cuando la ventana cae en el minuto del cambio de fuente. Es una
solución más pequeña y sin nada que pueda fallar en tiempo de ejecución.

**Efecto secundario deseado:** las métricas generadas antes de este cambio no tienen el
campo, y un filtro `term` las excluye por sí solo. No hay que borrar nada.

**Dos cosas que hubo que arreglar para que el campo funcionara:**

**La plantilla de índice no lo declaraba.** `elasticsearch/plantillas/cripto.json` solo
declaraba nueve campos. Un campo nuevo se mapea dinámicamente como `text` con subcampo
`.keyword`, y un filtro `term` sobre `text` **no encuentra nada y no da error**: es el
mismo fallo silencioso que la plantilla explícita existe para evitar, reproducido dentro
del propio archivo que debía evitarlo. Se declararon los 23 campos del contrato. Como el
índice del día ya existía, se le aplicó el mapeo con `PUT _mapping` antes de que llegara
el primer documento con el campo; un despliegue desde cero no necesita ese paso.

**Queda un desajuste conocido en los índices de hoy, y se deja a propósito.** El índice
`cripto-nrt_trade-2026.09.09` se creó *antes* de que la plantilla declarara `origen`, así
que ahí ese campo quedó como `text` con subcampo `.keyword`. En los índices que se creen a
partir de ahora será `keyword` a secas. Consecuencia práctica: una agregación sobre los
trades de hoy necesita `origen.keyword`, y sobre los de mañana, `origen`.

No se corrige porque las dos salidas son borrar el índice o reindexarlo, y el índice de
hoy contiene la mezcla de las dos fuentes, que es precisamente la evidencia del cambio:

| `origen` | Trades indexados |
|---|---|
| `simulador` | 33 159 |
| `exchange_ws` | 20 658 |

Con el índice del día siguiente el problema desaparece solo. Lo que sí conviene es no
construir paneles de Kibana contra `origen.keyword`, porque dejarán de funcionar mañana.
En las **métricas** no ocurre: ahí `origen_datos` se declaró con `PUT _mapping` antes de
que llegara el primer documento que lo llevaba, así que es `keyword` desde el principio.

**El checkpoint de Spark quedó incompatible.** Añadir `min(origen)` y `max(origen)`
cambia el esquema del estado de la agregación, y Spark se niega a reanudar desde un
checkpoint cuyo esquema no coincide. Hubo que apartarlo
(`datos/checkpoints/metricas` → `metricas_simulador_20260909`). **Esto vale para
cualquier cambio futuro en el `.agg()`**, no solo para este: tocar la lista de
agregaciones obliga a descartar el estado acumulado. Es una restricción del motor, no un
defecto, pero conviene saberla antes de una demo y no durante.

---

### 2026-09-08 · Primera corrida del circuito completo, y cinco defectos que solo aparecen ejecutando

Con los dos caminos integrados y todo levantado —Kafka, Spark, Elasticsearch, Kibana,
Logstash, Airflow y MySQL— el circuito se cerró. Los cinco problemas de abajo se
descubrieron **uno detrás de otro**, cada uno tapando al siguiente, y ninguno era visible
leyendo la configuración.

#### 1. El códec `json_lines` no funciona con el input `file`

**Síntoma.** Logstash arrancaba sin ningún error, detectaba el archivo, registraba en su
`sincedb` que lo había leído entero —505 169 bytes— y en Elasticsearch no aparecía ni un
documento.

**Causa.** El input `file` ya trocea el archivo por saltos de línea y entrega cada línea
**sin** el salto final. El códec `json_lines` vuelve a buscar un delimitador que ya no
está, se queda la línea en el búfer esperándolo, y no emite nunca.

**Solución.** `codec => "json"` para archivos. `json_lines` solo sirve donde el flujo sí
trae los saltos, como el input `tcp`.

**Por qué es el peor tipo de fallo:** todos los indicadores dicen que funciona. El archivo
se detecta, el `sincedb` avanza hasta el final, no hay excepciones. Solo falta el
resultado.

#### 2. El `sincedb` sobrevive a un `restart`

**Síntoma.** Tras corregir el códec, seguía sin indexar nada.

**Causa.** El `sincedb` ya marcaba los archivos como leídos por completo. Borrarlo dentro
del contenedor y hacer `docker compose restart` **no sirve**: Logstash vuelca el `sincedb`
a disco al recibir la señal de parada, así que el apagado ordenado lo restaura.

**Solución.** `docker compose up -d --force-recreate logstash`, que crea un contenedor
nuevo con la capa de escritura vacía.

#### 3. Un índice por cada día de la serie: más de 300 índices

**Síntoma.** Al empezar a indexar el batch aparecieron cientos de índices
`cripto-batch_ohlcv-AAAA.MM.DD`, con uno a tres documentos cada uno.

**Causa.** El sufijo `%{+YYYY.MM.dd}` se calcula sobre `@timestamp`, y para una vela
`@timestamp` es **su** fecha, de hasta un año atrás, no la de ingesta. La convención
heredada del Taller 2 sirve para un flujo continuo, donde todo cae en el día actual; para
una carga histórica de 364 días es una fragmentación absurda.

**Solución.** Los tipos batch van a un índice único sin fecha; los NRT y de operación
conservan el índice diario, donde sí tiene sentido. La dimensión temporal vive en
`@timestamp`, que es lo que usa Kibana: la fecha en el nombre del índice es una
conveniencia de particionado físico, no un requisito del modelo.

#### 4. El batch era idempotente en MySQL pero no en Elasticsearch

**Síntoma.** Detectado al revisar el punto anterior, antes de que causara daño.

**Causa.** Sin `document_id`, Elasticsearch asigna un identificador aleatorio a cada
documento. Cada corrida del DAG 04 añadía otros 1092 documentos. A la tercera corrida los
paneles habrían mostrado el triple de volumen, **sin ningún error**.

**Solución.** `document_id` derivado de la clave natural, `id_activo_fecha`, la misma que
usa el `ON DUPLICATE KEY UPDATE` de MySQL. La idempotencia tiene que valer en los dos
almacenes, no solo en el relacional.

#### 5. `kafka-python` 2.0.2 no funciona en Python 3.12

**Síntoma.** El contenedor del productor moría al arrancar con
`ModuleNotFoundError: No module named 'kafka.vendor.six.moves'`.

**Causa.** La librería incluye una copia propia de `six` que usa un mecanismo de
importación que Python 3.12 eliminó.

**Solución.** Imagen base `python:3.11-slim`. Descartadas `kafka-python-ng`, un fork menos
conocido, y `confluent-kafka`, que obligaría a reescribir el productor.

#### Un falso positivo, para no repetirlo

Antes de encontrar el problema real dimos por roto Logstash porque el índice
`cripto-nrt_metrica-*` no aparecía. **No estaba roto: consultamos demasiado pronto.** Una
ventana de un minuto con watermark de 30 segundos y `outputMode("append")` no emite nada
hasta que el watermark garantiza que la ventana ya no puede recibir eventos tardíos: unos
dos minutos y medio desde el primer trade. Conviene tenerlo presente en la demostración,
porque el panel tarda ese tiempo en moverse por primera vez y parece que no funciona.

### 2026-09-08 · Integración de los dos caminos: seis defectos que impedían cerrar el circuito

Al fusionar `nrt/estefano` en `batch/manuel` y revisar la rama antes de probarla. La rama
estaba construida sobre la nuestra, así que la fusión fue limpia; el problema no era de
control de versiones sino de **contrato**.

**El defecto grave: el job de Spark emitía 5 de los 10 campos del contrato.** Publicaba
`inicio_ventana`, `fin_ventana`, `vwap`, `volatilidad_real` y `volumen_total`. Faltaban
`volumen_usdt`, `n_trades` y `tipo_fuente`.

Sin `volumen_usdt` no se puede agregar las 60 ventanas de un minuto a una hora ponderando
por volumen. Sin `n_trades` no hay `cobertura_pct`. **La conciliación —la pieza que
justifica tener dos flujos— habría devuelto cero**, y lo habría hecho en silencio: cero
horas conciliadas es un resultado legítimo cuando el flujo NRT está callado, así que nada
habría delatado que el problema era un campo ausente.

Los otros cinco:

| Defecto | Consecuencia |
|---|---|
| `spark-streaming` sin `command` en el compose | El contenedor arrancaba, parecía sano y no ejecutaba nada |
| `dropDuplicates(["id_trade"])` con watermark | Sobre una columna que no es la de tiempo de evento, el watermark **no limpia el estado**: Spark guarda todos los ids vistos para siempre. En la prueba de carga a 2000 ev/s el job se queda sin memoria. Corregido a `dropDuplicatesWithinWatermark(["simbolo","id_trade"])` |
| Checkpoint en `/opt/spark/work-dir/checkpoints` | Ahí se monta el **código**. El volumen previsto para checkpoints quedaba sin usar, y recrear el contenedor perdería el estado, con lo que la prueba de recuperación ante fallo no demostraría nada |
| Logstash con 1 input de 5 | Faltaba el `file` del NDJSON batch. **El volumen ya estaba montado**: el camino batch llevaba dos días escribiendo 1092 registros por corrida y nadie los leía |
| `id_trade` aleatorio y como texto | Nunca se repetía, así que la deduplicación era **imposible de probar**. La prueba P4 consiste precisamente en reenviar trades ya procesados |

**Lo que hace interesante este bloque para la exposición** no es la lista de defectos, sino
que ninguno se detecta leyendo el código de un solo lado. Cada mitad era razonable por
separado; lo que fallaba era la costura. Es el argumento a favor de haber escrito el
contrato de datos primero: sin él no habría habido contra qué comparar, y los seis
defectos habrían aparecido el día de la demostración.

**Lección concreta:** un contrato escrito no basta si nada lo verifica. Convendría una
prueba que valide el mensaje del job contra el JSON Schema, igual que
`prueba_logica_batch.py` valida las reglas de calidad.

### 2026-09-06 · El DAG 05 tumbaba toda la cadena batch si Elasticsearch no estaba

**Cómo apareció.** Ejecutando el DAG 05 recién escrito, con el flujo NRT todavía
inexistente. Los tres `conciliar_<simbolo>` fallaron y arrastraron al resto del DAG.

**Causa.** `consultar_metricas_nrt` trataba igual dos situaciones distintas: que el índice
no exista todavía (404, devolvía vacío) y que Elasticsearch no responda (excepción de
conexión, lanzaba `RuntimeError`).

**Por qué era grave y no un detalle de la prueba.** El DAG 04 dispara al 05 con
`wait_for_completion=True`. Un fallo en el 05 hace fallar al 04, que hace fallar al 03, y
así hasta el 01. **Todo el pipeline batch se pondría en rojo porque la otra mitad del
proyecto no está corriendo**, que es algo que no le corresponde. Y peor: pasaría cada vez
que Estéfano reiniciara su stack.

**Solución.** Tres situaciones, tres tratamientos:

| Situación | Qué significa | Qué se hace |
|---|---|---|
| 404 | El flujo NRT existe pero no ha escrito nada | Cero horas, sigue |
| Error de conexión | La otra mitad no está levantada | `FlujoNrtNoDisponible`, el DAG lo captura, avisa por `ops_log` y sigue |
| Cualquier otro error HTTP | Consulta mal formada o mapeo inesperado | **Falla.** Es un defecto real y disfrazarlo de "no hay datos" lo escondería |

El reporte distingue `NRT_NO_DISPONIBLE` de `SIN_METRICAS`: sin ese campo, un reporte con
cero horas no diría si el flujo estuvo callado o si Elasticsearch estaba caído, y son dos
problemas distintos.

**Lección para el informe.** Al acoplar dos pipelines con `wait_for_completion`, hay que
decidir explícitamente qué fallos del hijo son fallos del padre. Por defecto lo son todos,
y casi nunca es lo que uno quiere.

### 2026-09-06 · Una función que habría comparado horas contra días

**Cómo apareció.** Al escribir `comun/conciliacion.py`. En `repositorio.py` existía una
función `velas_horarias(simbolo, desde, hasta)`, escrita pensando en el DAG 05.

**El problema.** Consultaba `hechos_ohlcv_diario`, que es una tabla **diaria** por diseño.
Habría devuelto velas diarias con nombre de horarias, y la conciliación habría comparado
el VWAP de **una hora** contra el cierre de **un día entero**. Sin ningún error: solo un
número de desviación plausible que no significa nada. Es el peor tipo de defecto, porque
el resultado se ve bien.

**Solución.** La referencia horaria se descarga en el momento
(`conciliacion.obtener_referencia_batch`) y **no se persiste**. La conciliación solo cubre
las pocas horas en que el flujo NRT estuvo corriendo, así que no hace falta guardarlas, y
mezclar dos granularidades en una tabla de hechos es la forma más rápida de que un conteo
posterior salga mal sin que nadie lo note. La función se conserva lanzando
`NotImplementedError` con el motivo, para que si alguien la busca encuentre la explicación
en vez del hueco.

**Detalle que salió gratis.** El VWAP horario ponderado por volumen no necesita ninguna
fórmula especial ni un script en Elasticsearch:

    vwap_hora = suma(volumen_usdt) / suma(volumen_base)

porque `volumen_usdt` de cada ventana ya es suma(precio × cantidad) y `volumen_base` es
suma(cantidad). Dos sumas simples y una división. Promediar los 60 valores de `vwap` sin
ponderar habría sido incorrecto: daría el mismo peso a un minuto con dos operaciones que a
uno con dos mil.

### 2026-09-06 · Decisiones tomadas al escribir el compose

Quedan aquí para no volver a discutirlas y para poder justificarlas en la exposición.

**Creación automática de topics desactivada** (`KAFKA_AUTO_CREATE_TOPICS_ENABLE: false`).
Con la opción activada, un error de tipeo en el nombre de un topic crea uno nuevo y vacío
en vez de fallar. El síntoma es un productor que publica sin error y un consumidor que no
recibe nada, y se pierde media hora buscando el motivo. Los topics se crean explícitamente
en el servicio `kafka-init`, versionados con sus particiones y su retención.

**Dos listeners en Kafka, no uno.** Los contenedores resuelven el nombre `kafka`, pero un
script lanzado desde Windows resuelve `localhost`. Con un solo listener anunciado, uno de
los dos casos falla siempre. `INTERNO` en `kafka:29092` y `EXTERNO` en `localhost:9095`.

**El DDL se monta en `/docker-entrypoint-initdb.d` de MySQL.** Se ejecuta solo la primera
vez, con el volumen vacío. Es lo que hace que el esquema no requiera ningún paso manual, y
también la razón de que modificar `sql/` después obligue a `docker compose down -v`.

**Dependencias instaladas en una imagen propia, no con
`_PIP_ADDITIONAL_REQUIREMENTS`.** Esa variable reinstala en cada arranque de cada
contenedor, exige internet siempre y, al no fijar versiones, cada arranque puede traer una
distinta. Es la misma lección del punto 2 del historial de correcciones del taller
anterior.

**Instalación contra el archivo de restricciones oficial de Airflow.** Sin él, pip puede
actualizar cualquier dependencia transitiva para satisfacer un paquete nuevo, y el
resultado típico es un Airflow que ya no arranca.

Cada entrada: qué pasó, por qué pasó, qué se hizo. Es el material de la sección de
problemas resueltos de la exposición.

### 2026-09-06 · La última vela de la API está incompleta

**Cómo apareció.** Ejecutando `descargar_klines` contra la API real, no leyendo la
documentación. La serie de cinco días traía `n_trades` de entre 1,2 y 4,4 millones en
los días cerrados, y 120 706 en el último.

**Causa.** La API devuelve también el periodo en curso, que todavía no ha cerrado. Esa
vela **pasa todas las reglas de calidad**: su OHLC es coherente, los precios son
positivos y el volumen encaja con el precio. Es válida en forma y falsa en contenido.

**Por qué importa más de lo que parece.** Rompe dos cosas a la vez. La carga deja de ser
idempotente en la práctica, porque el cierre de esa vela cambia cada vez que se ejecuta
el DAG y dos corridas del mismo día producen resultados distintos. Y como las medias
móviles son acumulativas, ese valor parcial contamina los treinta días siguientes de
`sma_30` y `volatilidad_30d`.

**Solución.** `_a_marco` compara el cierre teórico de cada vela contra el reloj y
descarta las que aún no cerraron. Ninguna regla de calidad podía detectarlo mirando la
fila: hace falta información externa al dato.

### 2026-09-06 · Idempotencia por clave natural, no por lote

**Cómo apareció.** Al portar `repositorio.py` del taller de Airflow. El patrón de allí
era `DELETE WHERE lote_id = ...` seguido de `INSERT`.

**Por qué aquí habría fallado.** Es el mismo error del punto 6.3 del README del taller,
reapareciendo en otro dominio. Dos corridas distintas cubren **a propósito** los mismos
días: la descarga trae 365 días hacia atrás cada vez. El borrado acotado por `lote_id`
no toca las filas del lote anterior, que ya ocupan las claves primarias
`(id_activo, fecha)`, y el `INSERT` reventaría con `Duplicate entry`. El síntoma habría
sido que la primera corrida siempre funciona y la segunda siempre falla.

**Solución.** `ON DUPLICATE KEY UPDATE` sobre la clave natural. El `lote_id` se actualiza
junto con los valores, así que la fila queda atribuida a la corrida más reciente que la
escribió.

### 2026-09-06 · R01 no reconocía un campo vacío que había pasado por Parquet

**Cómo apareció.** Ejecutando el flujo completo bronce → cuarentena → plata sobre
archivos Parquet reales. El conteo de rechazos por regla salió
`{'R02': 8, 'R03': 8, 'R04': 6, 'R07': 2}`: **R01 no aparecía**, aunque uno de los cinco
defectos que inyecta el generador es precisamente un campo vacío.

**Causa.** Un `None` escrito en una columna numérica de Parquet vuelve como `NaN`, no
como `None`. R01 comprobaba `is None` o cadena vacía, y `str(nan)` devuelve `"nan"`, que
no es ninguna de las dos.

**Por qué importa aunque no se pierda ningún dato.** La fila igual se rechazaba, porque
R02 y R03 la atrapaban al no poder convertir el valor. Pero el motivo que quedaba escrito
en cuarentena era *"precio ilegible"* en vez de *"campo obligatorio vacío"*. Un motivo de
rechazo incorrecto es peor que ninguno: manda a quien revisa a buscar el problema donde
no está. Y las pruebas seguían en verde porque pasaban diccionarios con `None` directo,
sin el viaje por Parquet que ocurre en la realidad.

**Solución.** Función `_esta_vacio` que reconoce las tres formas de la ausencia de dato:
`None`, cadena vacía y `NaN` (detectado con `valor != valor`, cierto solo para NaN). Se
añadieron dos casos a la prueba, con `float("nan")` y con la cadena `"nan"`. Tras el
arreglo el conteo pasa a `{'R01': 4, 'R02': 8, ...}`.

**Lección para el informe.** Una prueba unitaria que construye sus datos a mano no ve los
defectos que introduce el formato de almacenamiento. Hace falta al menos una prueba que
haga el viaje de ida y vuelta por el medio real.

### 2026-09-06 · La API pública responde desde la red del equipo

**Comprobación.** `descargar_klines('BTCUSDT', dias=5)` devolvió datos reales, con origen
`exchange_rest`.

**Qué cambia.** Baja el riesgo de la demo, pero **no elimina el respaldo sintético**. La
regla de que el pipeline funcione sin internet se mantiene: que hoy haya red no dice
nada sobre la red del aula el día de la exposición.

---

## 6. Bloqueos actuales

Ninguno bloquea la entrega. Lo que queda son límites declarados, no cosas por arreglar.

| Antes bloqueaba | Estado |
|---|---|
| ~~Contrato sin revisar~~ | **Cerrado.** Sección 10 reescrita: las cinco decisiones, resueltas y verificadas |
| ~~Código del Taller 2 fuera del repo~~ | **Resuelto.** Compose unificado |
| ~~El DAG 05 necesita métricas NRT~~ | **Resuelto.** Concilia contra el mercado real |
| ~~Los índices se pierden con un `down`~~ | **Resuelto.** Volumen `elasticsearch-datos` |
| ~~Nada escribe en `alertas.precio`~~ | **Resuelto.** Las emite Spark |
| ~~El tablero está vacío~~ | **Resuelto.** 7 paneles, provisionados al arrancar |
| ~~Pruebas escritas y nunca ejecutadas~~ | **Resuelto.** Once ejecutadas; P12 queda para el Día 7 a propósito |
| ~~El checkpoint de Spark sin permisos~~ | **Resuelto.** Volumen `spark-checkpoints` |
| ~~Faltaban filas de conciliación en ES~~ | **Resuelto.** Nombre de archivo con marca por corrida |

**Límites que se quedan, y se declaran en la exposición:**

| Límite | Por qué no se elimina |
|---|---|
| La demo depende de que el exchange sea alcanzable | No hay forma de quitarlo sin volver a datos inventados. Si no responde, el productor cae al simulador con `origen=simulador` y la conciliación excluye esas ventanas en vez de dar un número falso |
| Entrega al-menos-una-vez, no exactamente-una-vez | Con deduplicación por `id_trade` basta para el caso. No es lo mismo, y se dice |
| Un solo nodo de Kafka y de Elasticsearch | Es un prototipo en una máquina de 16 GB |
| Las pruebas cubren la lógica, no la orquestación | No hay pruebas automatizadas de los DAGs |
