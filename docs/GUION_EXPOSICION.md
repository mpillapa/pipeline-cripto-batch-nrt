# Guion de la exposición

**20 minutos + 5 de preguntas.** Diez minutos por persona.
Estéfano Galarza · Manuel Pillapa

Los tiempos son acumulados y están medidos para leerse en voz alta sin prisa. Si un bloque
se pasa, el siguiente recorta: **el minuto 18 no se negocia**, porque la conciliación es lo
que distingue al proyecto y no puede quedarse fuera.

---

## Antes de empezar

- [ ] Circuito levantado **y con datos** desde al menos 30 minutos antes. Una ventana de
      Spark tarda 60–90 s en salir; un tablero recién arrancado está vacío.
- [ ] Los otros dos entornos de talleres **parados**. Con 16 GB no caben.
- [ ] Kibana abierto en el tablero, rango «últimas 6 horas», refresco cada 30 s.
- [ ] Terminal con el directorio del proyecto, fuente grande.
- [ ] **Vídeo de respaldo de 90 s** abierto en otra pestaña.
- [ ] Nada que dependa del internet del aula: si el exchange no responde, el productor cae
      al simulador solo. Decirlo en voz alta si pasa — es parte del diseño, no un
      imprevisto.

---

## Minutos 0–2 · El problema *(Manuel)*

Un operador de mesa necesita dos cosas **incompatibles en un solo flujo**:

- *ahora mismo*: qué hace el precio en los últimos segundos, para reaccionar;
- *con perspectiva*: cómo se comporta el activo en semanas, para decidir posiciones.

El primero sacrifica completitud por latencia. El segundo, latencia por exactitud.

> **La frase que enmarca todo:** no elegimos uno. Construimos los dos y después **medimos
> con un número si dicen lo mismo**. Eso último es lo que no estaba en el enunciado.

---

## Minutos 2–5 · Arquitectura *(Manuel)*

Sobre `docs/arquitectura.html`, vista **«Circuito completo»**.

Recorrer el flujo una sola vez, de izquierda a derecha, y detenerse en tres decisiones:

| Decisión | En una frase |
|---|---|
| **Kafka como bus compartido** | Sin él, Spark y Logstash competirían por la misma conexión al exchange, y un WebSocket no tiene historia: lo que se pierde no se puede volver a pedir. |
| **Spark escribe a Kafka, no a Elasticsearch ni a MySQL** | Elimina el conector ES-Spark y el driver JDBC, las dos dependencias más frágiles. El job solo depende de Kafka. |
| **Logstash, único escritor hacia Elasticsearch** | Un solo sitio donde se decide cómo aterriza cada campo. |

Cambiar a la vista **«Conciliación entre flujos»** y dejarla puesta: es donde va a volver
la exposición al final.

---

## Minutos 5–9 · Camino batch *(Manuel)*

Cinco DAGs encadenados. Enseñar el grafo en Airflow, no el código.

- **DAG 01–02**: descarga y reglas de calidad R01–R08, con bifurcación a cuarentena. Todo
  registro rechazado lleva **su motivo**.
- **DAG 03–04**: zona plata e indicadores, carga dimensional a MySQL.
- **Idempotencia por clave natural**, no por lote — se detalla en el minuto siguiente.

Enseñar en la terminal, en vivo:

```bash
docker compose exec mysql mysql -ucripto -pcripto cripto \
  -e "SELECT lote_id, estado FROM control_lotes; SELECT COUNT(*) FROM hechos_ohlcv_diario;"
```

**Tres lotes, 1 092 filas.** El conteo no se mueve aunque se vuelva a cargar.

---

## Minutos 9–10 · Un problema concreto del batch *(Manuel)*

**Elegir uno solo y contarlo entero**: síntoma → causa → solución. El mejor es la última
vela, porque el defecto es invisible para cualquier regla de calidad.

> **Síntoma.** La serie traía entre 1,2 y 4,4 millones de trades por día, y 120 706 en el
> último.
>
> **Causa.** La API devuelve también el periodo en curso, que aún no ha cerrado. Esa vela
> **pasa todas las reglas de calidad**: su OHLC es coherente, los precios son positivos, el
> volumen encaja. Es válida en forma y falsa en contenido.
>
> **Por qué importaba el doble.** La carga dejaba de ser idempotente —el cierre cambia
> entre corridas del mismo día— y, como las medias móviles son acumulativas, ese valor
> parcial contaminaba los treinta días siguientes de `sma_30`.
>
> **Solución.** Comparar el cierre teórico de cada vela contra el reloj y descartar las que
> no han cerrado. **Ninguna regla podía detectarlo mirando la fila**: hacía falta
> información externa al dato.

*Alternativa si hay poco tiempo:* la idempotencia por clave natural — el síntoma habría
sido «la primera corrida siempre funciona, la segunda siempre falla».

---

## Minutos 10–13 · Camino NRT *(Estéfano)*

Del trade al agregado. Cuatro conceptos, uno por minuto escaso:

1. **Dos fuentes, un mismo mensaje.** F1 es el exchange real por WebSocket; F2 el
   simulador. Se eligen con una variable de entorno y lo único que las distingue en el dato
   es el campo `origen`. Si el exchange no responde, el productor cae a F2 y lo avisa.
2. **Ventana de 1 minuto con watermark de 30 s.** El watermark acota cuánto estado guarda
   el motor: es lo que permite emitir la ventana una sola vez, ya cerrada.
3. **Deduplicación por `id_trade`.** Kafka entrega al-menos-una-vez.
4. **Checkpointing.** Offsets y estado sobreviven al reinicio.

> **El detalle que suele gustar:** duplicar todos los trades por igual **no cambia el
> VWAP**, porque es una media ponderada. Si solo se mira el VWAP, la falta de deduplicación
> es invisible. Lo que la delata es `n_trades` — y es justo el campo con el que la
> conciliación calcula la cobertura.

---

## Minutos 13–16 · Demo en vivo *(Estéfano)*

**El orden importa: lo más vistoso primero, por si hay que cortar.**

1. **Kibana moviéndose** (40 s). Tablero con refresco de 30 s. Señalar el panel *Reparto
   de trades por fuente*: casi 900 000 del exchange real frente a 100 000 del simulador.
2. **Disparar una alerta** (60 s). Bajar el umbral y enseñar la alerta apareciendo en
   `cripto-nrt_alerta-*`:
   ```bash
   CRIPTO_UMBRAL_ALERTA_PCT=0.02 docker compose up -d spark-streaming
   ```
   Decir por qué la alerta se publica en Kafka y no se queda en Kibana: **así es un dato**
   —con su `id_alerta`, indexado, reprocesable— y no una configuración que vive dentro de
   una herramienta.
3. **Reiniciar Spark y mostrar que retoma** (60 s):
   ```bash
   docker compose restart spark-streaming
   ```
   Mientras arranca, contar qué se está demostrando. Después, enseñar que la serie de
   ventanas no tiene huecos.

> **Si algo falla: pasar al vídeo sin disculparse.** Está grabado para eso. Perder treinta
> segundos pidiendo perdón cuesta más que el fallo.

---

## Minutos 16–18 · Resiliencia y latencia medida *(Estéfano)*

Sobre `docs/PRUEBAS.md`, sin leerlo entero.

| Etapa | p50 | p95 |
|---|---|---|
| Exchange → productor | 1 185 ms | 1 400 ms |
| Productor → Logstash | **162 ms** | 532 ms |
| Cierre de ventana → métrica | 84 s | 112 s |

**Los 84 segundos son la suma esperada, no un atasco:** 30 s de watermark + 30 s porque el
watermark va un micro-batch por detrás + 0–30 s hasta el trigger.

Carga: **aguanta 2 000 eventos/s**; 20 000 mensajes acumulados se drenan en 4 segundos.

> **La honestidad aquí puntúa.** Decir que esta prueba **medía 0,00 ms** hasta que se
> reescribió, porque restaba `@timestamp` menos `ts_evento` y Logstash *deriva* el primero
> del segundo: era una resta contra sí misma. Una prueba que no puede fallar no demuestra
> nada. Es el tipo de error que un tribunal reconoce y agradece que se cuente.

---

## Minutos 18–20 · Conciliación, límites y producción *(los dos)*

**El cierre. Enseñar las dos filas juntas, nunca solo la buena:**

| Hora | Fuente | Desviación BTC | Cobertura | Veredicto |
|---|---|---|---|---|
| 00:00 | Simulador | −20,03 % | 8,08 % | DESVIADO |
| 01:00 | Exchange real | **−0,064 %** | 44,98 % | DESVIADO |
| 02:00 | Exchange real | 0,086 % | 100 % | **COINCIDE** |

Tres órdenes de magnitud de diferencia, **con el mismo código a ambos lados**.

Y el punto fino, que es el que demuestra que se entendió el problema:

> La fila de las 01:00 tiene una desviación **diez veces menor que el umbral** y aun así
> sale `DESVIADO`. Es lo correcto: la cobertura era del 45 % porque el flujo en vivo solo
> escuchó media hora. La métrica dice dos cosas a la vez y las separa —*el precio coincide*
> y *no escuché la hora entera*—. Un solo número no podría decir ambas.

**Límites, dichos por nosotros antes de que los pregunten:** un solo nodo de Kafka y de
Elasticsearch, credenciales en claro, sin TLS, entrega al-menos-una-vez con deduplicación
—no exactamente-una-vez—, y las pruebas cubren la lógica, no la orquestación.

**Qué costaría producción:** registro de esquemas gestionado, seguridad y secretos, alta
disponibilidad, monitoreo, infraestructura como código, soporte y continuidad.

---

## Preguntas probables, con la respuesta en una frase

| Pregunta | Respuesta |
|---|---|
| ¿Por qué Kafka si Logstash ya ingiere? | Porque Spark y Logstash necesitan los mismos trades, y un WebSocket no se puede leer dos veces ni reproducir. |
| ¿Qué pasa con un evento que llega tarde? | Dentro del watermark de 30 s entra en su ventana; más tarde se descarta. Está probado con las dos tandas, a 20 s y a 90 s. |
| ¿Exactamente-una-vez o al-menos-una-vez? | Al-menos-una-vez con deduplicación por `id_trade`. Suficiente para el caso, y no es lo mismo: lo decimos. |
| ¿Por qué dos almacenes? | MySQL da claves foráneas e idempotencia; Elasticsearch da series temporales y agregaciones. Ninguno hace bien lo del otro. |
| ¿Por qué Parquet y no CSV? | Lleva el esquema dentro. Un CSV devuelve todo como texto y cada lector vuelve a adivinar qué es número y qué es fecha. |
| ¿Cómo escalaría a diez veces el volumen? | Más particiones en `trades.crudo` y más instancias de Logstash en el mismo grupo. El techo medido hoy es del consumidor, no del bus. |
| ¿Y si el exchange no responde el día de la demo? | El productor cae al simulador, lo marca con `origen=simulador`, y la conciliación excluye esas ventanas en vez de dar un número falso. |
| ¿Por qué la conciliación da DESVIADO con 0,06 % de desviación? | Porque la cobertura era del 45 %. Una desviación pequeña con cobertura baja no demuestra que el streaming acierte. |

---

## Reparto y ensayos

| Quién | Minutos | Total |
|---|---|---|
| Manuel | 0–10 y 18–20 | ~11 min |
| Estéfano | 10–18 y 18–20 | ~9 min |

- **Ensayo 1** (Día 6): cronometrado, sin interrupciones. Objetivo: caber en 20 minutos.
- **Ensayo 2** (Día 7): cada uno prepara **cinco preguntas duras sobre la parte del otro**.
  Quien no sepa responder algo de su mitad, lo estudia esa noche.
