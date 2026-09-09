# Decisiones de diseño

Cada decisión con su contexto, lo que se descartó y lo que costó. Las que se tomaron
*después* de que algo fallara llevan el síntoma que las provocó: son las que mejor
explican el diseño, porque el problema que resuelven es comprobable.

La bitácora cronológica está en [AVANCE.md](../AVANCE.md) sección 5. Este documento la
reordena por tema y se queda con la conclusión.

Índice:

1. [Forma del sistema](#1-forma-del-sistema) — por qué dos flujos, por qué un bus
2. [Fronteras entre componentes](#2-fronteras-entre-componentes) — quién escribe dónde
3. [Corrección de los datos](#3-correccion-de-los-datos) — idempotencia, calidad, tipos
4. [La conciliación](#4-la-conciliacion) — la pieza propia del proyecto
5. [Operación del entorno](#5-operacion-del-entorno) — reproducibilidad y arranque
6. [Decisiones que se revirtieron](#6-decisiones-que-se-revirtieron)

---

## 1. Forma del sistema

### D01 · Dos flujos sobre el mismo dominio, no uno

**Contexto.** Un operador necesita dos cosas incompatibles: qué pasa en los últimos
segundos, y cómo se comporta el activo en semanas. La primera exige sacrificar
completitud por latencia; la segunda, latencia por exactitud.

**Decisión.** Implementar los dos caminos por separado y **compararlos con un número**, en
vez de buscar un único flujo intermedio que no sirva bien para ninguna de las dos cosas.

**Consecuencia.** Aparece un entregable que no estaba en el enunciado —la conciliación— y
que es lo que distingue al proyecto. También aparece su coste: dos caminos que mantener y
un contrato de datos que respetar entre ambos.

**Alternativa descartada.** Un solo flujo NRT con ventanas grandes. Habría dado latencia
mala y exactitud mediocre, y no habría nada que conciliar.

### D02 · Kafka como bus compartido

**Contexto.** Tanto Spark como Logstash necesitan los mismos trades.

**Decisión.** Los dos consumen del topic `trades.crudo`; ninguno habla con el WebSocket.

**Por qué.** Sin el bus, **Spark y Logstash competirían por la misma conexión al
exchange**, y una caída de cualquiera de los dos perdería eventos que no se pueden volver
a pedir: un WebSocket no tiene historia. Con Kafka, un consumidor caído retoma desde su
offset.

**Coste aceptado.** Un servicio más y Zookeeper detrás.

### D03 · Spark escribe a Kafka, no a Elasticsearch ni a MySQL

**Decisión.** El job es fuente Kafka → agregación → destino Kafka. Nada más.

**Por qué.** Escribir desde Spark a Elasticsearch exige el conector `elasticsearch-hadoop`;
a MySQL, el driver JDBC. Son **las dos dependencias más frágiles** del stack y las que
más suelen romper una demo. Publicando en `metricas.1min` el job no necesita ninguna de
las dos, y quien las necesite —Logstash— ya las tiene resueltas.

**Consecuencia.** Un salto más en el recorrido del dato, a cambio de que el job de Spark
solo dependa de Kafka.

---

## 2. Fronteras entre componentes

### D04 · Logstash es el único proceso que escribe en Elasticsearch

**Decisión.** Cinco entradas distintas —`trades.crudo`, `metricas.1min`, `alertas.precio`,
el NDJSON del batch y los puertos HTTP 8088 y TCP 5000— desembocan en un solo escritor.

**Por qué.** Un único lugar donde se decide cómo aterriza cada campo. Con dos escritores,
el mapeo se define en dos sitios y basta que uno mande un `precio` como texto para que las
agregaciones del panel dejen de funcionar.

### D05 · Dos almacenes con funciones distintas

| Almacén | Para qué | Lo que aporta |
|---|---|---|
| MySQL | Modelo dimensional y bitácora de lotes | Claves foráneas, claves primarias naturales, idempotencia |
| Elasticsearch | Línea de tiempo y búsqueda | Agregaciones temporales, consultas por rango |

**Por qué no uno solo.** Elasticsearch no da integridad referencial y MySQL no da
agregaciones temporales sobre cientos de miles de documentos con la latencia que necesita
un tablero. Cada uno hace lo que sabe hacer.

### D06 · Ninguna regla de negocio dentro de los DAGs ni del job de Spark

**Decisión.** Los DAGs orquestan; las reglas son funciones puras en `dags/comun/`, una
capa por archivo.

**Consecuencia comprobable.** Las pruebas de lógica corren en segundos sin levantar
infraestructura: 42 comprobaciones del batch y 30 de la conciliación, sin Airflow, sin
MySQL y sin Elasticsearch.

---

## 3. Corrección de los datos

### D07 · Idempotencia por clave natural, no por lote

**Síntoma que la provocó.** El patrón heredado del taller anterior era
`DELETE WHERE lote_id = ...` seguido de `INSERT`.

**Por qué habría fallado aquí.** La descarga trae 365 días hacia atrás **en cada
corrida**, así que dos lotes distintos cubren los mismos días a propósito. El borrado
acotado por `lote_id` no toca las filas del lote anterior, que ya ocupan la clave primaria
`(id_activo, fecha)`, y el `INSERT` reventaría con `Duplicate entry`. El síntoma habría
sido inconfundible y tardío: **la primera corrida siempre funciona, la segunda siempre
falla.**

**Decisión.** `ON DUPLICATE KEY UPDATE` sobre la clave natural. El `lote_id` se actualiza
con los valores, de modo que cada fila queda atribuida a la última corrida que la escribió.

### D08 · La última vela de la API se descarta

**Síntoma.** `n_trades` de entre 1,2 y 4,4 millones en los días cerrados, y 120 706 en el
último.

**Causa.** La API devuelve también el periodo en curso. Esa vela **pasa todas las reglas
de calidad**: su OHLC es coherente, los precios son positivos, el volumen encaja. Es
válida en forma y falsa en contenido.

**Por qué importa más de lo que parece.** Rompe dos cosas a la vez. La carga deja de ser
idempotente en la práctica, porque el cierre de esa vela cambia entre corridas del mismo
día. Y como las medias móviles son acumulativas, el valor parcial contamina los treinta
días siguientes de `sma_30` y `volatilidad_30d`.

**Decisión.** Comparar el cierre teórico de cada vela contra el reloj y descartar las que
aún no cerraron. **Ninguna regla de calidad podía detectarlo mirando la fila**: hace falta
información externa al dato. Es el argumento de por qué la calidad no se reduce a
validaciones de formato.

### D09 · Tipos explícitos en Elasticsearch, no mapeo dinámico

**Decisión.** Los 24 campos del contrato declarados en una plantilla que aplica
`elasticsearch-init` al arrancar.

**Por qué.** Con mapeo dinámico, Elasticsearch infiere los tipos del primer documento que
llegue. Un `precio` que caiga como `text` hace que las agregaciones del panel fallen **sin
ningún error visible**: el panel sale vacío y nada explica por qué.

**Y aun así falló una vez.** Al añadir `origen_datos`, la plantilla no lo declaraba. El
campo se mapeó dinámicamente como `text`, y un filtro `term` sobre `text` **no encuentra
nada y no da error**. Es el mismo fallo silencioso que la plantilla existe para evitar,
reproducido dentro del archivo que debía evitarlo. La lección: la plantilla hay que
ampliarla en el mismo commit que añade el campo.

### D10 · El `origen` viaja con cada dato

**Contexto.** Con dos fuentes posibles, después de agregar una ventana ya no se sabe de
dónde salieron sus trades.

**Decisión.** Cada trade lleva `origen` (`exchange_ws` o `simulador`) y cada métrica lleva
`origen_datos`, propagado con `min` y `max` sobre el origen de sus trades.

**Por qué `min`/`max` y no `collect_set`.** Las agregaciones de colección no son fiables en
streaming. Con dos valores posibles, comparar el mínimo con el máximo distingue
exactamente los tres casos: `exchange_ws`, `simulador`, o la mezcla cuando la ventana cae
en el minuto del cambio de fuente. Es más pequeño y no tiene nada que pueda fallar en
ejecución.

**Por qué no basta un aviso en consola.** Un aviso se pierde en un log; el campo viaja con
el dato y se puede filtrar tres días después.

---

## 4. La conciliación

### D11 · La cobertura entra en el veredicto, no es solo informativa

**Decisión.** `COINCIDE` exige desviación dentro del margen **y** cobertura suficiente.
Cualquier otro caso es `DESVIADO`.

**Por qué.** Con una cobertura del 30 %, una desviación pequeña no demuestra que el
streaming acierte: demuestra que los trades que se perdieron eran parecidos a los que sí
se vieron, que es otra cosa. Marcar eso como `COINCIDE` sería declarar un éxito que no se
ha medido.

**El caso que lo valida.** Con el productor real corriendo 31 de los 60 minutos de la
hora:

| Símbolo | Desviación | Cobertura | Veredicto |
|---|---|---|---|
| BTCUSDT | −0,064 % | 44,98 % | DESVIADO |
| ETHUSDT | −0,038 % | 44,91 % | DESVIADO |
| SOLUSDT | −0,052 % | 55,92 % | DESVIADO |

La desviación es diez veces menor que el umbral, y aun así el veredicto es `DESVIADO`.
**Eso es lo correcto**: la métrica está diciendo dos cosas a la vez y separándolas bien
—*el precio coincide* y *no escuché la hora entera*—. Un solo número no podría decir las
dos.

### D12 · Solo se concilia lo que vino del mercado real

**Decisión.** El DAG 05 filtra `origen_datos = exchange_ws`.

**Por qué.** Comparar ventanas del simulador contra velas reales del batch da un número
sin significado. Lo comprobado, con el mismo código a ambos lados:

| Hora | Fuente | Desviación BTC | Cobertura |
|---|---|---|---|
| 00:00 | Simulador | −20,03 % | 8,08 % |
| 01:00 | Exchange real | −0,064 % | 44,98 % |

Tres órdenes de magnitud. Es la mejor evidencia de que la conciliación mide algo real, y
por eso conviene enseñar **las dos filas juntas**, no solo la buena.

**Efecto secundario aprovechado.** Las métricas anteriores al cambio no tienen el campo, y
un filtro `term` las excluye por sí solo. No hubo que borrar nada.

### D13 · Un fallo del flujo NRT no tumba el camino batch

**Síntoma.** Los tres `conciliar_<simbolo>` fallaron con el flujo NRT inexistente y
arrastraron al resto del DAG.

**Por qué era grave.** El DAG 04 dispara al 05 con `wait_for_completion=True`. Un fallo en
el 05 hace fallar al 04, y de ahí hacia atrás hasta el 01. **Todo el pipeline batch en
rojo porque la otra mitad del proyecto no está corriendo**, que es algo que no le
corresponde — y pasaría cada vez que alguien reiniciara su stack.

**Decisión.** Tres situaciones, tres tratamientos:

| Situación | Significa | Qué se hace |
|---|---|---|
| 404 | El flujo NRT existe y no ha escrito nada | Cero horas, sigue |
| Error de conexión | La otra mitad no está levantada | Se avisa por `ops_log` y sigue |
| Otro error HTTP | Consulta mal formada o mapeo inesperado | **Falla**: es un defecto real y disfrazarlo lo escondería |

**Lección general.** Al acoplar dos pipelines con `wait_for_completion`, hay que decidir
explícitamente qué fallos del hijo son fallos del padre. Por defecto lo son todos, y casi
nunca es lo que uno quiere.

---

## 5. Operación del entorno

### D14 · El conector de Kafka va horneado en la imagen de Spark

**Decisión.** Los cuatro JAR dentro del `Dockerfile.spark`, no `--packages` en tiempo de
ejecución.

**Por qué.** `--packages` resuelve por Ivy en cada arranque y exige internet siempre. Era
**el riesgo número uno declarado del proyecto**: el escenario de la demo sin red con el
job que no arranca. Horneado, el job arrancó sin descargar nada.

### D15 · Creación automática de topics desactivada

**Por qué.** Con `KAFKA_AUTO_CREATE_TOPICS_ENABLE` activado, un error de tipeo en el
nombre crea un topic nuevo y vacío en vez de fallar. El síntoma es un productor que
publica sin error y un consumidor que no recibe nada. Los topics se crean explícitamente
en `kafka-init`, versionados con sus particiones y su retención.

### D16 · Dos listeners de Kafka anunciados

**Por qué.** Los contenedores resuelven el nombre `kafka`; un script lanzado desde Windows
resuelve `localhost`. Con un solo listener anunciado, uno de los dos casos falla siempre.
`kafka:29092` dentro de la red, `localhost:9095` desde el host. Lo mismo con Logstash:
8088 dentro, 8089 desde fuera.

### D17 · Dependencias en una imagen propia, no `_PIP_ADDITIONAL_REQUIREMENTS`

**Por qué.** Esa variable reinstala en cada arranque de cada contenedor, exige internet
siempre y, al no fijar versiones, cada arranque puede traer una distinta. La instalación
va contra el archivo de restricciones oficial de Airflow: sin él, pip puede actualizar
cualquier dependencia transitiva y el resultado típico es un Airflow que ya no arranca.

### D18 · Parquet en las zonas de datos

**Por qué.** Columnar, comprimido y **lleva el esquema dentro**. Un CSV devuelve todo como
texto y obliga a que cada lector vuelva a decidir qué es número y qué es fecha; el
proyecto ya tuvo un fallo por un campo vacío que cambió de forma al pasar por Parquet, y
con CSV habría tenido uno por columna.

### D19 · Airflow con LocalExecutor y Spark en `local[2]`

**Por qué.** Con Elasticsearch, Kafka y Spark en la misma máquina de 16 GB, el recurso
escaso es la RAM. LocalExecutor elimina Redis, worker y triggerer; `local[2]` en un solo
contenedor ahorra unos 2 GB frente a master más worker, y la interfaz de la aplicación
sigue en el 4040, que es lo que se enseña.

**Límite declarado.** No es una configuración de producción y no pretende serlo.

### D20 · Elasticsearch sobre un volumen nombrado

**Síntoma.** `docker inspect` devolvía `Mounts: []`. Los índices vivían en la capa
escribible del contenedor: sobreviven a un `stop` —por eso nadie lo notó durante días— y
**los borra cualquier `down`**.

**Decisión.** Volumen `elasticsearch-datos`, con los 135 MB existentes migrados.

**Por qué volumen nombrado y no `./elasticsearch/datos`.** Un bind mount en Windows entrega
al contenedor un directorio cuyo propietario no es el uid 1000 con el que corre
Elasticsearch, y el nodo no arranca por permisos.

**Detalle útil.** Kibana no necesita volumen propio: guarda tableros y patrones de índice
en el índice `.kibana`, dentro de Elasticsearch.

---

## 6. Decisiones que se revirtieron

Las que se cambiaron a mitad de camino, con el motivo. Sirven para responder *qué
cambiarían si volvieran a empezar*.

| Decisión inicial | Por qué se cambió |
|---|---|
| `id_trade` aleatorio en el simulador | Hacía **imposible** probar la deduplicación: sin identificadores repetibles no hay duplicado que detectar. Pasó a contador entero por símbolo |
| El job de Spark emitía 5 campos | Faltaban `volumen_usdt` y `n_trades`, que son justo los que necesita la conciliación. Reescrito a los 10 del contrato |
| Las métricas se conciliaban vinieran de donde vinieran | Comparaba el simulador contra el mercado real. Se añadió `origen_datos` y el filtro del DAG 05 |
| Elasticsearch sin volumen | Ver D20 |
| `prueba_logica_streaming` se daba por no ejecutable | No era inejecutable, era **inalcanzable**: el contenedor de Spark no montaba `pruebas/`. Montado, pasa 7 de 7 |

---

## 7. Lo que costaría llevarlo a producción

Se declara aquí porque es la pregunta previsible del tribunal y porque el alcance del
proyecto es explícitamente un prototipo.

- **Esquemas gestionados**: un registro de esquemas en vez de un contrato en Markdown y
  una plantilla JSON mantenidos a mano.
- **Seguridad**: TLS en Kafka y Elasticsearch, autenticación, gestión de secretos. Hoy las
  credenciales están en claro en el compose y Elasticsearch corre sin seguridad.
- **Alta disponibilidad**: Kafka con réplicas, Elasticsearch con más de un nodo. Hoy ambos
  son de un solo nodo, y el estado de Spark vive en un checkpoint local.
- **Entrega exactamente-una-vez de punta a punta**: hoy es al-menos-una-vez con
  deduplicación por `id_trade`, que es suficiente para el caso pero no equivalente.
- **Operación**: monitoreo, alertas sobre el propio pipeline, infraestructura como código,
  pruebas automatizadas de la orquestación —hoy las pruebas cubren la lógica, no los DAGs—,
  soporte y continuidad.
