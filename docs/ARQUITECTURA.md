# Arquitectura — qué hace cada pieza y por qué está

El **diagrama** del circuito completo está en la [sección 2 del README](../README.md#2-arquitectura),
y las **decisiones de diseño con su justificación**, en la tabla que lo sigue. Este
documento responde otra pregunta, la que suele caer en la exposición: *para qué sirve cada
servicio y qué pasaría si no estuviera*.

El estado real de cada pieza —lo que se ha ejecutado y lo que solo está escrito— vive en
[AVANCE.md](../AVANCE.md), no aquí.

---

## 1. Camino near real-time

### Kafka y Zookeeper — `kafka`, `zookeeper`, `kafka-init`, `kafka-ui`

Bus de mensajería desacoplado, patrón publicación/suscripción. Es el amortiguador central
del streaming: el topic `trades.crudo` recibe el flujo continuo de eventos, particionado
por `simbolo`, que es lo que garantiza el orden **dentro de cada símbolo**.

`kafka-init` crea los topics con sus particiones al arrancar y termina; `kafka-ui`
(puerto 8093) permite auditar topics, particiones y offsets sin entrar al contenedor.

**Sin Kafka**, Spark y Logstash competirían por el mismo WebSocket, y una caída de
cualquiera de los dos perdería eventos que no se pueden pedir otra vez.

> **Los dos listeners no son un adorno.** Kafka anuncia `kafka:29092` para quien está
> dentro de la red de Docker y `localhost:9095` para quien corre desde Windows. Usar el
> equivocado da `NoBrokersAvailable`, que no dice nada sobre cuál era el problema.

### Productor de trades — `ingesta_streaming/`

Emisor de eventos. Elige fuente según `CRIPTO_FUENTE_TRADES`:

| Fuente | Archivo | `origen` del evento |
|---|---|---|
| F1 — exchange real por WebSocket | `cliente_websocket.py` | `exchange_ws` |
| F2 — simulador local | `simulador_trades.py` | `simulador` |

Las dos se consumen como generadores con la misma interfaz, así que el bucle de
publicación de `productor_kafka.py` no sabe cuál está usando. Si el exchange no responde,
cae a F2, avisa por consola y publica un evento `ops_control`.

**La traducción está separada de la conexión**: `traducir_trade()` es una función pura que
recibe el payload del exchange y devuelve el evento del contrato, y por eso se puede
probar sin socket, sin Kafka y sin la librería instalada.

**El campo `origen` viaja con cada trade, y eso es deliberado.** Un aviso en consola se
pierde; el campo permite después saber qué alimentó cada ventana y excluir de la
conciliación lo que no vino del mercado real.

### Spark Structured Streaming — `spark-streaming`

La única pieza genuinamente nueva del proyecto. Consume `trades.crudo`, valida contra el
esquema estricto de `esquemas_spark.py`, deduplica por `id_trade`, aplica una marca de
agua de 30 segundos para los eventos tardíos y agrupa en ventanas fijas de 1 minuto para
calcular VWAP, OHLC, volatilidad, volumen y número de trades. Publica el resultado en
`metricas.1min`.

**Escribe a Kafka y no a Elasticsearch ni a MySQL**, lo que elimina las dos dependencias
más frágiles —el conector ES-Spark y el driver JDBC— y deja el job como Kafka → agregación
→ Kafka.

> **Tocar el `.agg()` invalida el checkpoint.** Cambiar la lista de agregaciones cambia el
> esquema del estado, y Spark se niega a reanudar desde un checkpoint que no coincide. Hay
> que apartarlo. Conviene saberlo antes de una demo y no durante.

### Logstash — `logstash_cripto`

**Único escritor hacia Elasticsearch**, por diseño: un solo lugar donde se decide cómo
aterriza cada campo. Consume cinco entradas —`trades.crudo`, `metricas.1min`,
`alertas.precio`, el NDJSON que exporta el batch, y los puertos HTTP 8088 y TCP 5000 por
los que el pipeline se reporta a sí mismo— y enruta a un índice u otro según `tipo_fuente`.

### Elasticsearch — `elasticsearch_cripto`

Repositorio de series temporales. Guarda los eventos con tipos explícitos declarados en
`elasticsearch/plantillas/cripto.json`, que aplica el servicio `elasticsearch-init` al
arrancar.

**La plantilla no es opcional.** Sin ella, Elasticsearch infiere los tipos del primer
documento que llegue; un precio que caiga como `text` hace que las agregaciones del panel
fallen **sin ningún error visible**: el panel sale vacío y nada explica por qué.

Los índices persisten en el volumen `elasticsearch-datos`. Guarda también los objetos de
Kibana, porque Kibana los almacena en el índice `.kibana` y no en disco propio.

### Kibana — `kibana_cripto`

Exploración en vivo (Discover, puerto 5602), tableros y reglas de alerta.

> Las reglas de alerta necesitan `XPACK_ENCRYPTEDSAVEDOBJECTS_ENCRYPTIONKEY` definida en
> el contenedor: sin esa clave, guardar una regla falla. Y la ventana de evaluación tiene
> que ser más ancha que el retraso de indexación, o la regla evalúa antes de que el
> documento exista y no encuentra nada.

---

## 2. Camino batch

### Airflow — `airflow-webserver`, `airflow-scheduler`, `postgres`

Orquesta cinco DAGs encadenados: ingesta desde la API pública, reglas de calidad con
bifurcación a cuarentena, transformación a la zona plata, carga dimensional a MySQL y
conciliación contra el flujo NRT.

Corre con **LocalExecutor**, lo que elimina Redis, worker y triggerer. Con Elasticsearch,
Kafka y Spark en la misma máquina, el recurso escaso es la RAM.

**Ninguna regla de negocio vive dentro de los DAGs.** Los DAGs orquestan; las reglas son
funciones puras en `dags/comun/`, una capa por archivo, y por eso se prueban en segundos
sin levantar nada.

> Las conexiones se inyectan por variable de entorno (`AIRFLOW_CONN_MYSQL_CRIPTO`,
> `AIRFLOW_CONN_FS_CRIPTO`) y no en la base de metadatos, así que **no aparecen en
> `airflow connections list`** y sin embargo funcionan.

### Zonas de datos — Parquet

`bronce` guarda lo descargado tal cual, `cuarentena` lo que no pasó las reglas con su
motivo, `plata` lo normalizado y con indicadores. Parquet y no CSV porque **lleva el
esquema dentro**: un CSV devuelve todo como texto y obliga a cada lector a volver a
decidir qué es número y qué es fecha.

### MySQL — `mysql`

Modelo dimensional: una dimensión de activos y las tablas de hechos, más la bitácora
`control_lotes`. La carga es **idempotente por clave natural**, no por lote, de modo que
volver a correr un lote no duplica filas.

Cada almacén hace lo que sabe hacer: MySQL da claves foráneas e idempotencia,
Elasticsearch da series temporales y búsqueda.

---

## 3. El punto donde los dos caminos se tocan

Dos veces, y conviene tenerlas localizadas porque son las únicas dependencias físicas
entre los dos trabajos:

1. **El NDJSON del batch.** El DAG 04 lo escribe en una carpeta que Logstash monta y lee.
   Es un archivo, no una llamada: si Logstash está caído, el archivo espera.
2. **La conciliación.** El DAG 05 consulta a Elasticsearch las métricas de ventana y las
   compara contra el cierre de la vela del batch. Solo concilia las que llevan
   `origen_datos = exchange_ws`, porque comparar ventanas del simulador contra velas
   reales daría un número sin significado.

---

## 4. Alcance y límites

Prototipo exploratorio académico. Todas las fuentes son APIs públicas gratuitas o datos
simulados; no hay conexión con ningún sistema corporativo ni se reproducen sus estructuras
o reglas. Las credenciales son ficticias y válidas solo en local: sin TLS, sin gestión de
secretos, sin respaldos. Las limitaciones conocidas están enumeradas en la
[sección 9 del README](../README.md#9-limitaciones-conocidas).
