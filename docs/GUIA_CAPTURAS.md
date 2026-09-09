# Guía de capturas

Qué fotografiar, con qué estado y para qué sirve cada una. Las capturas son el **respaldo
de la demo**: si el día de la exposición algo no levanta, esto es lo que se enseña.

Van en `capturas/`, con el nombre que indica cada ficha. Esa carpeta **no está en el
`.gitignore`**: las capturas se versionan, porque son evidencia del trabajo.

---

## Antes de capturar

- [ ] Circuito levantado y con **al menos 30 minutos** de datos. Una ventana de Spark tarda
      60–90 s en salir, y un tablero recién arrancado sale vacío.
- [ ] El productor leyendo el **mercado real** (`CRIPTO_FUENTE_TRADES=websocket`, que es el
      valor por defecto). Una captura alimentada por el simulador vale mucho menos.
- [ ] Los cinco DAGs ejecutados al menos una vez, con el 05 en verde.
- [ ] Navegador **sin pestañas personales** ni marcadores visibles, zoom al 100 %.
- [ ] Terminal con fuente grande: se va a proyectar.

**Formato:** PNG, ventana completa del navegador o de la terminal, sin recortar los
títulos. Una captura sin contexto no se puede explicar tres días después.

---

## Las capturas, por orden de importancia

Si solo diera tiempo a cinco, son las cinco primeras.

### 1 · `01-tablero-completo.png` — el tablero en marcha

**Dónde:** Kibana → Dashboards → *Pipeline cripto - batch y near real-time*.
**Estado:** rango «últimas 6 horas», los siete paneles con datos.

Es la captura que resume el proyecto entero. Que se vea el reloj de refresco activo.

### 2 · `02-conciliacion-dos-filas.png` — el resultado que distingue al proyecto

**Dónde:** terminal.

```bash
docker compose exec mysql mysql -ucripto -pcripto cripto \
  -e "SELECT simbolo, fecha_hora, desviacion_pct, cobertura_pct, veredicto
      FROM conciliacion ORDER BY fecha_hora, simbolo;"
```

**Tiene que verse la hora del simulador (−20 %) junto a la del exchange (−0,064 %).** Las
dos juntas, nunca solo la buena: el contraste con el mismo código a ambos lados es la
evidencia de que la conciliación mide algo real.

### 3 · `03-reparto-por-fuente.png` — las dos fuentes

**Dónde:** panel *Reparto de trades por fuente*, ampliado a pantalla completa.

Casi 900 000 trades del exchange real frente a unos 100 000 del simulador. Demuestra que
F1 quedó enchufada y que el campo `origen` viaja con el dato.

### 4 · `04-vwap-por-minuto.png` — la serie que produce Spark

**Dónde:** panel *VWAP por minuto y símbolo*, pantalla completa.

Tres líneas moviéndose. Es la pieza genuinamente nueva del proyecto.

### 5 · `05-airflow-dags.png` — la orquestación en verde

**Dónde:** Airflow (`localhost:8092`) → vista de DAGs.
**Estado:** los cinco en verde, con el 05 ejecutado.

---

### 6 · `06-latencia.png` — los percentiles

```bash
python pruebas/prueba_latencia.py
```

Que se vean las **tres etapas**, incluida la 3 con su p50 de 162 ms. Y, si cabe en la
misma captura, el bloque que explica de dónde salen los 84 s de la etapa 2.

### 7 · `07-carga.png` — el resumen de P9

```bash
docker compose stop productor
docker compose run --rm --no-deps -e CRIPTO_KAFKA_EXTERNO=kafka:29092 \
  --entrypoint python productor -u /pruebas/prueba_carga.py
docker compose up -d productor
```

Capturar la **tabla de resumen**: las tres tasas con AGUANTA.

### 8 · `08-recuperacion.png` — P5

```bash
python pruebas/prueba_recuperacion.py
```

Los tres OK: volvió a emitir, no recalculó, sin huecos.

### 9 · `09-eventos-tardios.png` — P6

La tanda de 20 s entrando y la de 90 s descartada. Es una de las cuatro pruebas que el
plan considera diferenciales.

### 10 · `10-alerta-generada.png` — el quinto flujo

Con el umbral bajado, un documento de `cripto-nrt_alerta-*` en Discover, con todos sus
campos visibles: `id_alerta`, `regla`, `severidad`, `umbral_pct`, `valor_pct`.

### 11 · `11-kafka-ui-topics.png` — el bus

**Dónde:** Kafka UI (`localhost:8093`) → Topics.

Los tres topics con sus particiones y offsets. Que se vea el lag del grupo de Logstash
cerca de cero.

### 12 · `12-arquitectura.png` — el diagrama

`docs/arquitectura.html` en la vista *Circuito completo*. Sirve para las diapositivas.

### 13 · `13-pruebas-logica.png` — las pruebas que corren sin nada

Las tres suites puras seguidas, con su «todas las comprobaciones pasan».

### 14 · `14-arranque-limpio.png` — P12, el Día 7

La única que **no se puede tomar antes**: exige `docker compose down -v`, que borra toda la
evidencia anterior. Va al final, cuando el resto ya esté capturado.

---

## Vídeo de respaldo — 90 segundos

Se graba el Día 6, después de las capturas. Es el seguro contra un fallo en vivo.

| Segundos | Qué se ve |
|---|---|
| 0–20 | El tablero moviéndose, con el reloj de refresco |
| 20–40 | Bajar el umbral y la alerta apareciendo en Discover |
| 40–70 | `docker compose restart spark-streaming` y la serie retomando sin huecos |
| 70–90 | La consulta de conciliación con las dos filas |

**Sin voz.** Se narra en directo mientras se reproduce: así el vídeo sirve aunque haya que
enseñarlo a otro ritmo, y no hay que sincronizar nada.

Guardar como `capturas/respaldo-demo.mp4`. Comprobar que se reproduce **sin conexión** y
en el portátil que se va a llevar, no solo en el que se grabó.
