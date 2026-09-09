# Contrato de datos

> **Estado: BORRADOR.** Redactado por Manuel el 6 de septiembre de 2026. **No está
> cerrado hasta que Estéfano lo revise en la sesión del Día 1.** Hasta entonces, ningún
> componente debe darse por definitivo contra este documento.

Este archivo es la interfaz entre los dos caminos del pipeline. Todo lo que cruza de un
dueño al otro está aquí: nombres de campos, tipos, unidades, claves y semántica.

**Regla de cambio.** Modificar este documento exige avisar a la otra persona antes de
subir el cambio. Un campo renombrado en silencio rompe el otro camino sin error visible:
el dato simplemente llega vacío.

---

## 1. Convención transversal: `tipo_fuente`

Cumple el mismo papel que el campo `type` del Taller 2: **es el discriminante que decide
el filtro condicional en Logstash y el índice destino en Elasticsearch.** Todo evento que
entre al pipeline, venga de donde venga, lo lleva.

| Valor | Origen | Flujo | Índice destino |
|---|---|---|---|
| `nrt_trade` | Productor WebSocket o simulador | NRT | `cripto-nrt_trade-YYYY.MM.dd` |
| `nrt_metrica` | Job de Spark | NRT | `cripto-nrt_metrica-YYYY.MM.dd` |
| `nrt_alerta` | Job de Spark | NRT | `cripto-nrt_alerta-YYYY.MM.dd` |
| `batch_ohlcv` | Airflow DAG 04 | Batch | `cripto-batch_ohlcv` (sin fecha) |
| `batch_referencia` | `http_poller` de Logstash | Batch | `cripto-batch_referencia-YYYY.MM.dd` |
| `batch_conciliacion` | Airflow DAG 05 | Batch | `cripto-batch_conciliacion` |
| `ops_control` | DAGs y productor, por HTTP al 8088 | NRT | `cripto-ops_control-YYYY.MM.dd` |
| `ops_log` | Componentes, por TCP al 5000 | NRT | `cripto-ops_log-YYYY.MM.dd` |

---

## 2. Convenciones de tipos y unidades

Se declaran una vez y aplican a todos los mensajes. La mayoría de los desencuentros entre
dos flujos vienen de aquí, no de los nombres de campo.

| Concepto | Convención | Motivo |
|---|---|---|
| Marcas de tiempo | ISO 8601 con `Z`, en **UTC**, milisegundos | El exchange publica en UTC. Convertir a hora local en la ingesta hace que dos flujos que corren en momentos distintos no se puedan comparar |
| Precios | `double`, en USDT, sin separador de miles | Se tratan como USD sin ajuste cambiario (supuesto declarado) |
| Volumen base | `double`, en unidades del activo (BTC, ETH…) | |
| Volumen cotizado | `double`, en USDT | El campo se llama `volumen_usdt`, nunca `volumen` a secas |
| Símbolo | `keyword` en mayúsculas, sin separador: `BTCUSDT` | Es clave de partición en Kafka y de agrupación en ambos flujos |
| Porcentajes | `double` en puntos porcentuales: `0.33` significa 0,33 % | Escrito una sola vez aquí porque es el error clásico: 0,33 % contra 33 % |
| Nulos | `null`, nunca `""` ni `0` | Un cero es un dato; un vacío es la ausencia de dato |

**Decisión sobre la marca de tiempo de las ventanas.** Se usa `ts_evento` (hora del
exchange), no `ts_ingesta`. Si se usara la de ingesta, una pausa de red movería trades a
la ventana equivocada y la conciliación mediría el retraso de la red en lugar del precio.

---

## 3. Evento de trade — Kafka `trades.crudo`

Clave del mensaje: `simbolo`. Lee: Spark y Logstash.

Hay **dos** productores que emiten este mismo mensaje y se eligen con
`CRIPTO_FUENTE_TRADES`:

| Fuente | Módulo | `origen` | Para qué |
|---|---|---|---|
| F1, exchange real | `ingesta_streaming/cliente_websocket.py` | `exchange_ws` | Modo por defecto. Es la única con la que la conciliación del DAG 05 significa algo |
| F2, simulador | `ingesta_streaming/simulador_trades.py` | `simulador` | Trabajar sin red, y las pruebas de carga y de deduplicación, que necesitan controlar la tasa y los `id_trade` |

Si el exchange no responde, el productor cae a F2 salvo que
`CRIPTO_RESPALDO_SIMULADOR=false`. La caída es **ruidosa**: se avisa por consola y se
publica un evento `ops_control`. Aun así, lo que garantiza que nadie se confunda después
es el campo `origen` de cada evento.

```json
{
  "id_evento":          "9f1c0f4e-2f7a-4b0e-9d0b-3f1a6c9e2b77",
  "tipo_fuente":        "nrt_trade",
  "simbolo":            "BTCUSDT",
  "id_trade":           123456789,
  "precio":             63250.10,
  "cantidad":           0.0031,
  "importe_usdt":       196.07,
  "comprador_es_maker": false,
  "ts_evento":          "2026-09-07T14:03:11.482Z",
  "ts_ingesta":         "2026-09-07T14:03:11.617Z",
  "origen":             "exchange_ws"
}
```

| Campo | Tipo | Obligatorio | Notas |
|---|---|---|---|
| `id_evento` | `string` (UUID v4) | Sí | Identifica el mensaje, no el trade. Dos reenvíos del mismo trade tienen `id_evento` distinto |
| `id_trade` | `long` | Sí | **Clave de deduplicación.** Único por símbolo en el exchange |
| `precio` | `double` | Sí | > 0 |
| `cantidad` | `double` | Sí | > 0, en unidades del activo base |
| `importe_usdt` | `double` | Sí | `precio * cantidad`, precalculado para que Spark no lo repita por evento |
| `comprador_es_maker` | `boolean` | Sí | Permite separar presión compradora de vendedora |
| `ts_evento` | `string` ISO 8601 UTC | Sí | Hora del exchange. Gobierna las ventanas |
| `ts_ingesta` | `string` ISO 8601 UTC | Sí | Hora en que el productor lo recibió. `ts_ingesta − ts_evento` = latencia de la etapa 1 |
| `origen` | `string` | Sí | `exchange_ws` o `simulador` |

**`origen` no es decorativo.** Es lo que permite decir en la exposición qué parte de los
datos vino de la API real y qué parte del simulador, y separar ambas poblaciones en
Kibana. Un panel que mezcla las dos sin poder distinguirlas no es evidencia de nada.

---

## 4. Métrica por ventana — Kafka `metricas.1min`

Escribe: Spark. Lee: Logstash y, agregado vía Elasticsearch, el DAG 05.

```json
{
  "tipo_fuente":     "nrt_metrica",
  "simbolo":         "BTCUSDT",
  "ventana_inicio":  "2026-09-07T14:03:00.000Z",
  "ventana_fin":     "2026-09-07T14:04:00.000Z",
  "n_trades":        412,
  "volumen_base":    3.81,
  "volumen_usdt":    241030.55,
  "precio_apertura": 63180.00,
  "precio_maximo":   63310.50,
  "precio_minimo":   63102.20,
  "precio_cierre":   63250.10,
  "vwap":            63248.77,
  "volatilidad_pct": 0.33,
  "ts_procesado":    "2026-09-07T14:04:03.180Z",
  "origen":          "spark_streaming",
  "origen_datos":    "exchange_ws"
}
```

**Definiciones, para que ambos calculen lo mismo:**

- `vwap` = `sum(precio * cantidad) / sum(cantidad)` sobre los trades de la ventana.
  **No** es el promedio simple de precios.
- `volatilidad_pct` = `(precio_maximo − precio_minimo) / vwap * 100`. Es rango relativo,
  no desviación estándar. Se elige por ser calculable de forma incremental en una ventana
  corta.
- `ventana_inicio` es inclusivo, `ventana_fin` exclusivo. Un trade exactamente en
  `14:04:00.000` pertenece a la ventana siguiente.
- `ts_procesado − ventana_fin` es la latencia de procesamiento, y es lo que sube cuando
  Spark se atrasa.
- `origen` dice **quién calculó** la métrica y siempre vale `spark_streaming`.
  `origen_datos` dice **de dónde venían los trades** que la alimentaron: se agrega desde
  el campo `origen` de los eventos de la ventana y vale `exchange_ws`, `simulador` o
  `exchange_ws+simulador` si la ventana mezcla ambas fuentes —lo que solo ocurre en el
  minuto en que se cambia de fuente.

**`origen_datos` es lo que hace auditable la conciliación.** El DAG 05 solo concilia las
ventanas con `origen_datos = exchange_ws` (parámetro `CONCILIACION_ORIGEN_DATOS`), porque
comparar el VWAP del simulador contra la vela real del exchange no mide el pipeline: mide
la distancia entre las constantes del simulador y el mercado. Medido el 9/9/2026 con el
simulador: −20 %, +36 % y +40 % de desviación con cobertura del 8 %, 10 % y 33 %.

---

## 5. Alerta — Kafka `alertas.precio`

Escribe: Spark. Lee: Logstash.

```json
{
  "tipo_fuente":     "nrt_alerta",
  "id_alerta":       "uuid v4",
  "simbolo":         "BTCUSDT",
  "regla":           "variacion_precio_1min",
  "umbral_pct":      0.50,
  "valor_pct":       0.83,
  "ventana_inicio":  "2026-09-07T14:03:00.000Z",
  "ventana_fin":     "2026-09-07T14:04:00.000Z",
  "severidad":       "ALTA",
  "detalle":         "Variacion de 0.83% supera el umbral de 0.50%",
  "ts_generada":     "2026-09-07T14:04:03.210Z"
}
```

`severidad`: `BAJA`, `MEDIA`, `ALTA`. El corte va en la configuración del job, no aquí.

---

## 6. Vela diaria — MySQL `hechos_ohlcv_diario` y `cripto-batch_ohlcv`

Escribe: Manuel (DAG 04). El mismo registro va a MySQL y, exportado como NDJSON, a
Elasticsearch vía Logstash.

| Campo | Tipo MySQL | Tipo ES | Notas |
|---|---|---|---|
| `id_activo` | `VARCHAR(20)` | `keyword` | FK a `dim_activo` |
| `simbolo` | `VARCHAR(20)` | `keyword` | Redundante con `dim_activo`, se incluye en el NDJSON porque Elasticsearch no hace joins |
| `fecha` | `DATE` | `date` | Día UTC de la vela |
| `apertura`, `maximo`, `minimo`, `cierre` | `DECIMAL(20,8)` | `double` | En USDT |
| `volumen_base` | `DECIMAL(24,8)` | `double` | Unidades del activo |
| `volumen_usdt` | `DECIMAL(24,8)` | `double` | |
| `n_trades` | `INT` | `integer` | Número de operaciones del día según el exchange |
| `retorno_pct` | `DECIMAL(10,4)` | `double` | `(cierre − cierre_previo) / cierre_previo * 100` |
| `sma_7`, `sma_30` | `DECIMAL(20,8)` | `double` | Media móvil del cierre. `null` mientras no haya suficientes días |
| `volatilidad_30d` | `DECIMAL(10,4)` | `double` | Desviación estándar de `retorno_pct` en 30 días |
| `nivel_volatilidad` | `VARCHAR(10)` | `keyword` | `BAJA`, `MEDIA`, `ALTA` |
| `lote_id` | `VARCHAR(20)` | `keyword` | Trazabilidad hasta la corrida que lo cargó |

`DECIMAL(20,8)` y no `FLOAT`: ocho decimales cubren la precisión de precios de activos de
bajo valor unitario, y en MySQL el decimal exacto evita que dos sumas del mismo conjunto
den resultados distintos.

**Diferencia deliberada con la métrica de streaming.** Aquí `volatilidad_30d` sí es
desviación estándar, mientras que `volatilidad_pct` del streaming es rango relativo. Son
dos medidas distintas con nombres distintos a propósito. **No se comparan entre sí en la
conciliación**; lo que se compara es `vwap` contra `cierre` y los conteos de trades.

---

## 6.bis Entrega del batch a Logstash — el punto donde los dos caminos se tocan

El DAG 04 no escribe en Elasticsearch. Deja un archivo y Logstash lo recoge. Esta sección
es el contrato de ese traspaso.

| Aspecto | Valor |
|---|---|
| Ruta que escribe el batch | `datos/exportado/<lote_id>/ohlcv.ndjson` |
| Ruta dentro del contenedor de Airflow | `/opt/airflow/datos/exportado/<lote_id>/ohlcv.ndjson` |
| Patrón que debe vigilar Logstash | `<montaje>/exportado/*/ohlcv.ndjson` |
| Formato | Un objeto JSON por línea, sin indentación |
| Codec de Logstash | `json_lines` |
| `tipo_fuente` de cada documento | `batch_ohlcv` |
| Índice destino | `cripto-batch_ohlcv` (sin fecha) |
| Campo temporal para `@timestamp` | `fecha_hora`, en ISO 8601 UTC |

**Un archivo nuevo por lote, no uno que se sobrescribe.** El input `file` de Logstash
lleva un registro de hasta dónde leyó cada archivo (`sincedb`). Si el batch reescribiera
siempre la misma ruta, Logstash vería un archivo que encoge y crece, y el comportamiento
sería impredecible: puede reindexar todo o no indexar nada. Con una ruta por lote, cada
archivo se escribe una vez y no vuelve a cambiar.

**El documento lleva `simbolo`, aunque en MySQL viva solo en la dimensión.** Elasticsearch
no hace joins: un documento con `id_activo` y sin `simbolo` sería inútil en Kibana.

**Por qué `fecha_hora` y no `fecha`.** La vela diaria tiene fecha, no hora, pero
Elasticsearch necesita una marca temporal completa para anclar `@timestamp`. El DAG 04
envía `<fecha>T00:00:00.000Z`, que es el momento de apertura de la vela.

**Pendiente del Día 1:** acordar el punto de montaje del volumen compartido. Airflow
escribe en su `/opt/airflow/datos` y Logstash tiene que ver esa misma carpeta desde su
propio contenedor. Es la única dependencia física entre los dos caminos.

---

## 7. Conciliación — MySQL `conciliacion`

Escribe: Manuel (DAG 05). Es el resultado que responde si los dos flujos coinciden.

| Campo | Tipo | Notas |
|---|---|---|
| `simbolo` | `VARCHAR(20)` | |
| `fecha_hora` | `DATETIME` | Granularidad de comparación: **hora**, no día |
| `vwap_streaming` | `DECIMAL(20,8)` | Agregado de `cripto-nrt_metrica-*` a nivel hora, ponderado por volumen |
| `n_trades_streaming` | `INT` | Suma de `n_trades` de las 60 ventanas de esa hora |
| `cierre_batch` | `DECIMAL(20,8)` | Cierre de la vela horaria del batch |
| `n_trades_batch` | `INT` | Trades de esa hora según el exchange |
| `desviacion_pct` | `DECIMAL(10,4)` | `(vwap_streaming − cierre_batch) / cierre_batch * 100` |
| `cobertura_pct` | `DECIMAL(10,4)` | `n_trades_streaming / n_trades_batch * 100` |
| `veredicto` | `VARCHAR(20)` | `COINCIDE`, `DESVIADO`, `SIN_DATOS` |
| `lote_id` | `VARCHAR(20)` | |

**Por qué la granularidad es horaria.** Las ventanas del streaming son de un minuto y la
vela oficial más fina que trae el batch sin explotar el volumen de la API es la horaria.
Comparar minuto a minuto exigiría descargar 1 440 velas por símbolo y por día.

**`cobertura_pct` importa más que `desviacion_pct`.** Dice qué porcentaje de los trades
reales alcanzó a ver el flujo en vivo. Una desviación pequeña con cobertura del 40 % no
significa que el streaming esté bien: significa que se perdió más de la mitad de los
datos y aun así el promedio salió parecido.

**Solo se concilian las ventanas con `origen_datos = exchange_ws`.** Es el requisito que
hace que la comparación tenga sentido: los dos lados tienen que estar midiendo el mismo
mercado. Contra el simulador, la desviación mide la distancia entre unas constantes
escritas a mano y el precio real, no la exactitud del pipeline. El parámetro es
`CONCILIACION_ORIGEN_DATOS`; ponerlo a `None` desactiva el filtro y solo sirve para
depurar.

---

## 8. Eventos de operación — HTTP 8088 y TCP 5000

Reutilizan los inputs que el Taller 2 ya tiene configurados. Sirven para que el pipeline
se observe a sí mismo.

**Control (HTTP POST a `http://logstash:8088`):**

```json
{
  "tipo_fuente":  "ops_control",
  "componente":   "dag_02_calidad",
  "evento":       "fin_tarea",
  "lote_id":      "L20260907_143012",
  "estado":       "PROMOVIDO",
  "metricas":     {"filas_evaluadas": 1095, "filas_validas": 1042, "tasa_rechazo": 0.0484},
  "ts":           "2026-09-07T14:31:02.004Z"
}
```

**Log (línea JSON por socket TCP al 5000):**

```json
{"tipo_fuente":"ops_log","componente":"productor_kafka","nivel":"WARN","mensaje":"Reconexion al WebSocket tras 3 intentos","ts":"2026-09-07T14:12:44.881Z"}
```

`nivel`: `DEBUG`, `INFO`, `WARN`, `ERROR`.

**Estos canales no pueden bloquear al emisor.** Si Logstash está caído, publicar un
evento de control tiene que fallar en silencio y seguir. Un DAG que se cae porque no pudo
reportar su propia telemetría es peor que un DAG sin telemetría.

---

## 9. Nombres reservados y prohibidos

Para evitar el problema que el propio Taller 2 identifica como su principal riesgo —el
mapeo de campos entre flujos:

- **Prohibido** `volumen` a secas. Siempre `volumen_base` o `volumen_usdt`.
- **Prohibido** `timestamp` y `time`. Siempre `ts_*`, `fecha` o `ventana_*`.
- **Prohibido** `type`. El discriminante es `tipo_fuente`.
- **Prohibido** `value`, `data`, `info`. No dicen nada y colisionan con los campos que
  agrega Logstash.
- `@timestamp` lo pone Logstash a partir del campo temporal de cada tipo de fuente. **No
  se envía desde el origen.**

---

## 10. Pendiente de acordar en la sesión del Día 1

Lo que este borrador deja abierto porque depende de Estéfano:

1. ¿El productor valida el JSON Schema antes de publicar, o publica todo y Spark filtra?
   Validar en el productor deja Kafka limpio; validar en Spark permite cuarentena también
   en NRT. **Propuesta: validar en el productor y descartar con log a `ops_log`.**
2. Número de particiones de `trades.crudo`. **Propuesta: 3, una por símbolo**, para que
   el orden por símbolo quede garantizado.
3. Retención de los topics. **Propuesta: 24 horas**, suficiente para reprocesar en las
   pruebas sin llenar el disco.
4. ¿`ops_log` sale también a Elasticsearch o solo a la consola? **Propuesta: a
   Elasticsearch**, es la mitad del argumento de observabilidad.
5. Confirmar los tres símbolos. **Propuesta: `BTCUSDT`, `ETHUSDT`, `SOLUSDT`** — el
   primero de alto volumen, el último bastante menor, para que las ventanas no se vean
   todas iguales en la demo.
