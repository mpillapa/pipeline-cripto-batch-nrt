# Avance del proyecto

Estado real de cada entregable y bitácora de lo que se ha ido descubriendo.
Se actualiza **al cerrar cada entregable**, no al final del día: un avance que se
escribe de memoria tres días después pierde justo lo que sirve, que es el detalle
del problema y cómo se resolvió.

Alimenta directamente `docs/DECISIONES.md` y la parte de la exposición donde hay que
explicar *cómo resolvieron problemas específicos*.

**Última actualización:** 6 de septiembre de 2026
**Plan de referencia:** [PLAN.md](PLAN.md)

Leyenda: `LISTO` · `EN CURSO` · `PENDIENTE` · `BLOQUEADO`

---

## 1. Resumen

| Camino | Progreso | Comentario |
|---|---|---|
| Andamiaje conjunto (Día 1) | PENDIENTE | Depende de la sesión del lunes 7 |
| Batch — Manuel | Adelantado | `comun/` y los DAGs 01–04 escritos y probados. Falta el DAG 05, que depende del flujo NRT |
| NRT — Estéfano | PENDIENTE | Sin iniciar; se apoya en el Taller 2 |
| Documentación | Adelantada | Plan, contrato, reglas de negocio, README y este archivo |

---

## 2. Andamiaje conjunto — Día 1

| Entregable | Estado | Notas |
|---|---|---|
| Repositorio en GitHub | LISTO | `mpillapa/pipeline-cripto-batch-nrt`, con la base subida. Falta agregar a Estéfano como colaborador |
| `docker-compose.yml` unificado | EN CURSO | **Mitad escrita y validada** con `docker compose config`: Postgres, MySQL con el DDL montado, Airflow (init, webserver, scheduler, cli), Zookeeper, Kafka, creación de topics y Kafka UI. Falta pegar Elasticsearch, Kibana, Logstash, Spark y el productor, en el bloque marcado al final del archivo |
| `Dockerfile.spark` con conector de Kafka horneado | PENDIENTE | **Riesgo número uno del proyecto.** Verificar sin red el mismo lunes |
| `Dockerfile.airflow` | LISTO | Imagen propia con `pyarrow`, instalado contra el archivo de restricciones oficial de Airflow. Sin construir todavía |
| `requisitos/airflow.txt` | LISTO | Sin versiones fijadas: las decide el archivo de restricciones |
| `.env.example` | LISTO | Valores ficticios, incluidos los límites de memoria de Elasticsearch y Logstash |
| Mapa de puertos | LISTO | Sección 5.5 del plan |
| Conexiones de Airflow en el compose | PENDIENTE | Hacen falta dos: `mysql_cripto` y `fs_cripto`. La segunda la usa el `FileSensor` del DAG 03; **no sirve `fs_default`**, que solo existe si la base se inicializa con `--load-default-connections` |
| Montaje de `datos_semilla/` en el contenedor | PENDIENTE | El DAG 04 lo lee desde `/opt/airflow/datos_semilla` |
| `contratos/CONTRATO_DATOS.md` | EN CURSO | Borrador escrito. **No cerrado hasta que Estéfano lo revise** |
| `sql/01_esquemas.sql` y `sql/02_tablas.sql` | LISTO | Cuatro tablas. Fuente única del esquema |
| Plantilla de índice de Elasticsearch | PENDIENTE | Estéfano. Tipos explícitos, no mapeo dinámico |
| Diagrama de arquitectura | PENDIENTE | Sirve para el documento y la exposición |

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
| `comun/conciliacion.py` | PENDIENTE | Consulta agregada a Elasticsearch |
| `pruebas/prueba_logica_batch.py` | LISTO | 12 bloques, 42 comprobaciones, todas pasan |
| `dag_01_ingesta_batch` | LISTO | Una tarea de descarga por símbolo, en paralelo |
| `dag_02_calidad` | LISTO | Bifurcación promover/bloquear, cuarentena con motivo |
| `dag_03_transformacion` | LISTO | Sensor + transformación + verificación de la zona plata |
| `dag_04_carga_mysql` | LISTO | Carga idempotente + exportación NDJSON para Logstash |
| `dag_05_conciliacion` | PENDIENTE | Necesita que el flujo NRT esté escribiendo |
| `datos_semilla/catalogo_activos.csv` | LISTO | Diez activos con nombre y categoría |
| `docs/REGLAS_NEGOCIO.md` | LISTO | R01–R08, T01–T03, fórmulas, supuestos y parámetros |
| `README.md` | LISTO | Marca explícitamente lo que aún no existe |
| `.gitignore` | LISTO | Ignora zonas de datos, `__pycache__`, `.env` y artefactos |

**Verificado hasta ahora, sin Airflow:**

| Comprobación | Resultado |
|---|---|
| Sintaxis de los 14 archivos Python | Compilan |
| `pruebas/prueba_logica_batch.py` | 42 de 42 |
| Flujo bronce → cuarentena → plata sobre Parquet real | Correcto |
| Tipos y precisión tras el viaje a Parquet | `float64`, `int64`, `str`; precio de 8 decimales idéntico |
| Nulos convertidos a `None` para MySQL | Correcto |
| API pública desde la red del equipo | Responde con datos reales |

Los DAGs solo tienen verificada la sintaxis: importarlos exige Airflow, que solo existe
dentro del contenedor. Se prueban de verdad el Día 2.

---

## 4. Camino NRT — Estéfano

| Entregable | Estado | Notas |
|---|---|---|
| `ingesta_streaming/simulador_trades.py` | PENDIENTE | Obligatorio, no opcional |
| `ingesta_streaming/productor_kafka.py` | PENDIENTE | Con caída al simulador si no hay red |
| Topics de Kafka | PENDIENTE | Creados por el compose, no a mano |
| `procesamiento_streaming/job_metricas_ventana.py` | PENDIENTE | **Pieza nueva. En pareja el Día 3** |
| `procesamiento_streaming/reglas_alertas.py` | PENDIENTE | |
| `logstash/pipeline/logstash.conf` | PENDIENTE | Extensión del Taller 2 con tres inputs de Kafka |
| Plantillas de índice | PENDIENTE | |
| Tableros de Kibana | PENDIENTE | Exportados como NDJSON, no creados a mano |
| `pruebas/prueba_latencia.py` | PENDIENTE | |

---

## 4.bis Qué puede arrancar Estéfano ahora mismo

Todo lo de esta lista está especificado y **no depende de nada más**. Se puede trabajar
en el entorno del Taller 2, sin esperar al `docker-compose.yml` unificado.

| Se puede empezar ya | Dónde está la especificación |
|---|---|
| `simulador_trades.py` | Esquema del evento completo: contrato, sección 3 |
| `productor_kafka.py` | Topic `trades.crudo`, clave = `simbolo`: contrato, sección 3 |
| Input de Kafka en `logstash.conf` para `trades.crudo` | Convención `tipo_fuente` → índice: contrato, sección 1 |
| Input `file` para el NDJSON del batch | Ruta, codec y patrón: contrato, sección 6.bis |
| Plantillas de índice de Elasticsearch | Tipos de cada campo: contrato, secciones 2, 3, 4 y 6 |
| Primeros paneles de Kibana sobre `cripto-nrt-trade-*` | |
| Esqueleto del job de Spark | Esquema de salida ya fijado: contrato, sección 4 |

**Lo que NO puede cerrar todavía:**

| Bloqueado | Por qué |
|---|---|
| Las cinco decisiones abiertas del contrato | Sección 10 del contrato. Son suyas: validación en productor o en Spark, particiones, retención, destino de `ops_log`, símbolos definitivos |
| El punto de montaje del volumen compartido | Airflow escribe el NDJSON y Logstash tiene que ver esa misma carpeta. Es la única dependencia física entre los dos caminos, y se acuerda el Día 1 |
| Prueba de extremo a extremo | Necesita el compose unificado |

**Lo primero que conviene que haga es el simulador, no el productor del WebSocket.** Con
el simulador funcionando, todo lo demás —Kafka, Logstash, Spark, Kibana— se puede
desarrollar y probar sin depender de que el exchange responda. El productor real se
enchufa después, contra un pipeline que ya funciona.

---

## 5. Bitácora de hallazgos y decisiones

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

| Bloqueo | Afecta a | Se resuelve |
|---|---|---|
| Contrato de datos sin revisar por Estéfano | Ambos caminos | Sesión del Día 1 |
| Código del Taller 2 aún no está en el repo | `docker-compose.yml`, Logstash | Estéfano lo comparte |
| El DAG 05 necesita métricas NRT en Elasticsearch | `dag_05_conciliacion` | Día 4, cuando el flujo NRT escriba |
