# Reglas de negocio del camino batch

Catálogo de todo lo que el pipeline decide sobre los datos: qué se considera una vela
válida, qué se calcula a partir de ella y con qué umbrales.

**Por qué existe este documento.** Una regla escondida dentro de una tarea de Airflow
solo se puede leer abriendo el DAG, no se puede probar sin levantar Airflow y no se puede
reutilizar. Aquí está el catálogo; en el código, cada regla es una función pura que se
ejecuta con Python a secas.

> **Supuestos generales.** Las reglas se apoyan en propiedades aritméticas de una vela
> OHLCV y en tolerancias elegidas para este trabajo académico. No provienen de ningún
> manual, procedimiento ni sistema de terceros, y no deben tomarse como válidas fuera de
> este ejercicio. Todos los indicadores son de dominio público.

---

## 1. Dónde vive cada cosa

| Qué | Archivo | Cómo se prueba |
|---|---|---|
| Umbrales y parámetros | [`dags/comun/config.py`](../dags/comun/config.py) | Se leen, no se prueban |
| Reglas de calidad R01–R08 | [`dags/comun/reglas_calidad.py`](../dags/comun/reglas_calidad.py) | `pruebas/prueba_logica_batch.py`, bloques 1–5 y 11 |
| Transformaciones T01–T03 | [`dags/comun/transformaciones.py`](../dags/comun/transformaciones.py) | Bloques 6–10 |
| Decisión de promoción | `reglas_calidad.decidir_promocion` | Bloque 5 |

Ningún DAG contiene reglas ni SQL. Los DAGs solo orquestan.

---

## 2. Reglas de calidad

Se aplican en el **DAG 02**, sobre la zona bronce. Cada regla recibe una fila y devuelve
un texto de error o `None`.

**Se evalúan todas las reglas, no se corta en la primera que falla.** Para corregir un
dato es más útil saber todo lo que tiene mal de una vez que descubrirlo de uno en uno.

### R01 — Campos obligatorios

Ninguno de estos puede venir vacío: `simbolo`, `fecha`, `apertura`, `maximo`, `minimo`,
`cierre`, `volumen_base`, `volumen_usdt`, `n_trades`.

**Qué cuenta como vacío.** Tres formas de lo mismo, y hay que reconocer las tres:
`None`, cadena vacía y `NaN`. La tercera es la que importa en este pipeline: la zona
bronce es Parquet, así que **todo nulo numérico llega a la regla como `NaN`**, no como
`None`. Ver la entrada del 6 de septiembre en [AVANCE.md](../AVANCE.md): la primera
versión de esta regla no lo contemplaba y escribía un motivo de rechazo equivocado.

### R02 — Coherencia OHLC

El máximo y el mínimo deben contener a la apertura y al cierre:

```
maximo >= minimo
maximo >= max(apertura, cierre)
minimo <= min(apertura, cierre)
```

**Es la propiedad que define una vela.** Durante el periodo el precio tocó un máximo y un
mínimo, y tanto el primer precio como el último están entre ambos. Una vela que no lo
cumple está corrupta, venga de donde venga.

Esta regla detecta el error de transporte más silencioso: **columnas desplazadas o
intercambiadas**. Si el máximo y el mínimo se cambian de sitio al parsear, todos los
valores siguen siendo positivos y plausibles; ninguna regla de rango lo nota y esta sí.

### R03 — Precios positivos

Apertura, máximo, mínimo y cierre deben ser mayores que cero.

**Un precio de cero no es un precio bajo:** es la ausencia del dato codificada como
número. Dejarla pasar arruina cualquier media móvil que la incluya y produce una división
por cero en el cálculo del retorno.

### R04 — Volúmenes no negativos

`volumen_base`, `volumen_usdt` y `n_trades` no pueden ser negativos.

**Se admite el cero**, a diferencia de R03: un periodo sin operaciones es un dato
legítimo, sobre todo en activos de bajo volumen.

### R05 — Símbolo en catálogo

El símbolo debe pertenecer a `config.SIMBOLOS`. Protege contra una respuesta de la API
que traiga un par distinto del solicitado, por ejemplo tras un cambio en el endpoint.

### R06 — Fecha plausible

La fecha debe ser legible y no estar en el futuro.

**Una vela con fecha futura significa que se está interpretando mal la marca de tiempo de
la API**, casi siempre por confundir segundos con milisegundos. El síntoma típico es una
fecha del año 56000.

### R07 — Coherencia de volumen

`volumen_usdt` debe parecerse a `volumen_base × precio_medio`, con
`precio_medio = (maximo + minimo) / 2`, dentro de una tolerancia del
**±25 %** (`config.TOLERANCIA_VOLUMEN_PCT`).

**No se exige igualdad, y la tolerancia es amplia a propósito.** El volumen cotizado real
se acumula operación a operación, con el precio de cada una; aquí solo se dispone del
OHLC, así que lo mejor que se puede estimar es el producto por el punto medio. La regla
busca detectar un desfase de columnas o un factor de escala equivocado —un valor mil veces
mayor lo detecta— no validar la aritmética del exchange. Un valor un 10 % distinto no lo
detecta, y no debe.

### R08 — Unicidad

No puede haber dos velas con el mismo `(simbolo, fecha)` dentro del lote. Se conserva la
primera aparición y se rechazan las siguientes: lo contrario descartaría el registro
original por culpa del duplicado.

**Se evalúa sobre el lote completo, no fila a fila**, porque no se puede decidir mirando
un solo registro.

**Aquí importa más que en otros dominios.** Dos velas del mismo día para el mismo símbolo
no solo duplican una fila: descuadran todas las medias móviles calculadas después.

---

## 3. Qué se hace con lo rechazado

Las filas rechazadas van a `datos/cuarentena/<lote_id>/cuarentena.parquet` con una
columna extra `motivo_rechazo` que lista **todos** los errores encontrados, separados
por `|`.

**No se descartan.** Sin el motivo escrito al lado del registro, nadie puede corregir el
origen del problema. Y el motivo tiene que ser el correcto: uno equivocado es peor que
ninguno, porque manda a quien revisa a buscar donde no está.

---

## 4. Decisión de promoción

El **DAG 02** decide si el lote sigue adelante. Dos criterios independientes; basta uno
para bloquear.

| Criterio | Umbral | Parámetro | Por qué |
|---|---|---|---|
| Tasa de rechazo | > 15 % | `UMBRAL_RECHAZO` | La fuente está entregando datos malos; procesarlos produciría indicadores engañosos |
| Filas válidas | < 60 | `MINIMO_FILAS_VALIDAS` | Aunque la tasa sea buena, sin suficiente historia las medias móviles de 30 días salen todas nulas y el lote no aporta nada |

**Un lote bloqueado no es un error del pipeline.** Es un resultado válido que se registra
en `control_lotes` con estado `BLOQUEADO`. Por eso la bifurcación es un
`BranchPythonOperator` y no una excepción: si se lanzara un error, Airflow reintentaría
una tarea que va a fallar siempre, porque los datos no van a mejorar solos.

---

## 5. Transformaciones

Se aplican en el **DAG 03**, sobre las filas que superaron la calidad. Producen la zona
plata.

### T01 — Normalización

Por fila, sin necesidad de ver el resto del lote:

| Campo | Qué se hace |
|---|---|
| `simbolo` | Sin espacios, en mayúsculas |
| `id_activo` | Se deriva como `ACT-<simbolo>` |
| `fecha` | ISO `YYYY-MM-DD`, en UTC |
| Precios y volúmenes | `float` redondeado a 8 decimales |
| `n_trades` | Entero |

**Por qué importa.** Sin normalizar, `' btcusdt '` y `'BTCUSDT'` se agrupan como dos
activos distintos en cualquier conteo posterior, y la unión con la dimensión falla en
silencio dejando el hecho sin activo.

**Por qué `id_activo` se deriva y no se inventa.** Así el DAG 04 puede unir hechos y
dimensión sin mantener un catálogo intermedio en memoria.

### T02 — Indicadores

Se calculan **por símbolo**, sobre la serie ordenada por `(simbolo, fecha)`.

| Indicador | Fórmula | Ventana |
|---|---|---|
| `retorno_pct` | `(cierre − cierre_anterior) / cierre_anterior × 100` | 1 día |
| `sma_7` | Media móvil simple del cierre | 7 días |
| `sma_30` | Media móvil simple del cierre | 30 días |
| `volatilidad_30d` | Desviación estándar muestral de `retorno_pct` | 30 días |

**El orden no es cosmético.** Todos estos indicadores son acumulativos. Con las filas
desordenadas, la media móvil mezcla días sin ningún aviso y el resultado parece razonable
aunque sea falso. Se ordena una sola vez, en `normalizar_lote`, y el resto da ese orden
por hecho. El DAG 03 lo verifica explícitamente antes de cargar.

**Por qué `groupby` por símbolo.** Calcularlos sobre el marco completo mezclaría el último
día de BTC con el primero de ETH y produciría un retorno absurdo justo en el límite entre
las dos series.

**Por qué los primeros días quedan nulos.** `min_periods` es igual a la ventana. Sin eso,
pandas devuelve una media parcial desde el primer día, y una "media de 30 días" calculada
con 3 días es un número que no significa lo que dice. Rellenar con cero sería peor:
inventaría un dato. No es que el retorno haya sido nulo, es que no hay día anterior con el
cual compararlo.

### T03 — Clasificación de volatilidad

| Etiqueta | Condición | Parámetro |
|---|---|---|
| `BAJA` | `volatilidad_30d <= 2.0` | `CORTE_VOLATILIDAD_BAJA` |
| `MEDIA` | `<= 5.0` | `CORTE_VOLATILIDAD_MEDIA` |
| `ALTA` | `> 5.0` | |
| *(nulo)* | Sin volatilidad calculada | |

Los valores están en puntos porcentuales de desviación estándar diaria a 30 días. **Son
cortes elegidos para este trabajo, no un estándar del sector.**

**Existe porque un panel con `BAJA / MEDIA / ALTA` se lee de un vistazo y uno con
`2.7431` no.**

**Una serie sin historia suficiente queda sin etiqueta, no como `BAJA`.** No es una serie
tranquila: es una serie desconocida, y son dos cosas distintas.

---

## 6. Reglas que no son de fila

Tres controles que ninguna regla de calidad puede hacer, porque necesitan información que
no está en el registro.

### La vela en curso se descarta

La API devuelve también el periodo que aún no ha cerrado. Esa vela **pasa todas las
reglas**: su OHLC es coherente, los precios son positivos y el volumen encaja. Es válida
en forma y falsa en contenido, porque su cierre es el precio de este instante y su volumen
una fracción del real.

Se detecta comparando el cierre teórico de la vela contra el reloj, en
`clientes_api._a_marco`. Ver la entrada del 6 de septiembre en [AVANCE.md](../AVANCE.md).

### Huecos en la serie

El DAG 02 reporta los días faltantes por símbolo. Es **informativo**: un hueco no
invalida las velas que sí llegaron, pero explica una media móvil que se ve rara. Un hueco
no es una fila mala, es una fila que no está, y por eso ninguna regla de fila puede verlo.

### Verificación de la zona plata

El DAG 03 comprueba, antes de que nada llegue a MySQL: que el orden por `(simbolo, fecha)`
se mantenga, que las columnas coincidan con las que espera la tabla de hechos, y que no
haya valores infinitos.

**Los indicadores son el punto donde un error deja de ser evidente.** Una vela mal formada
la detecta el DAG 02; una media móvil calculada sobre filas desordenadas produce números
perfectamente plausibles y completamente falsos.

---

## 7. Reglas de carga

### Idempotencia por clave natural

La carga de `hechos_ohlcv_diario` usa `ON DUPLICATE KEY UPDATE` sobre
`(id_activo, fecha)`, **no** `DELETE WHERE lote_id` seguido de `INSERT`.

Dos corridas distintas cubren a propósito los mismos días: la descarga trae 365 días hacia
atrás cada vez. Un borrado acotado por `lote_id` no tocaría las filas del lote anterior,
que ya ocupan esas claves primarias, y el `INSERT` fallaría con `Duplicate entry`. El
síntoma sería que la primera corrida siempre funciona y la segunda siempre falla.

El `lote_id` se actualiza junto con los valores: la fila queda atribuida a la corrida más
reciente que la escribió.

### La dimensión va antes que los hechos

La clave foránea lo exige. Un hecho cuyo activo aún no existe hace fallar el `INSERT`
entero.

### Un símbolo ausente del catálogo entra igual

Con el nombre derivado del par. **Es preferible una dimensión incompleta a una carga de
hechos que falla por clave foránea.**

---

## 7.bis Regla de conciliación

### C01 — Solo se concilia lo que es comparable

**Una ventana del streaming solo se compara contra la vela del batch si sus trades vinieron
del mercado real** (`origen_datos = exchange_ws`). Las ventanas alimentadas por el
simulador se excluyen de la conciliación.

**Por qué es una regla y no un detalle técnico.** La conciliación afirma algo concreto: que
los dos flujos, midiendo lo mismo por caminos distintos, llegan al mismo número. Eso exige
que los dos estén midiendo lo mismo. El simulador genera precios como una caminata de
±0,2 % alrededor de tres constantes escritas a mano, así que compararlo contra la vela real
del exchange no mide el pipeline: mide la distancia entre esas constantes y el mercado.

Medido el 9 de septiembre de 2026, la misma lógica de conciliación sobre las dos fuentes:

| Fuente | Desviación BTC | Desviación ETH | Desviación SOL | Cobertura |
|---|---|---|---|---|
| Simulador | −20,03 % | +36,20 % | +39,72 % | 8 – 33 % |
| Exchange real | 0,02 % | 0,14 % | 0,24 % | — |

**Sin esta regla, el veredicto `DESVIADO` es ambiguo**, y esa ambigüedad es el verdadero
problema: no se puede distinguir "el streaming pierde datos o calcula mal" de "estás
comparando datos inventados contra datos reales". Un número que puede significar dos cosas
opuestas no sirve para validar nada.

**Implementación.** El job de Spark agrega el campo `origen` de los trades de cada ventana
y publica el resultado como `origen_datos`. El DAG 05 lo usa como filtro en la consulta a
Elasticsearch. Las métricas anteriores a este cambio no llevan el campo, y el filtro las
excluye por sí solo, que es lo correcto: son del simulador.

Poner `CONCILIACION_ORIGEN_DATOS` a `None` desactiva el filtro. Solo sirve para depurar.

---

## 8. Parámetros, en un solo sitio

Todos viven en [`dags/comun/config.py`](../dags/comun/config.py) y se pueden cambiar sin
tocar ninguna regla.

| Parámetro | Valor | Qué controla |
|---|---|---|
| `SIMBOLOS` | BTCUSDT, ETHUSDT, SOLUSDT | Qué se descarga y qué acepta R05 |
| `DIAS_HISTORIA` | 365 | Profundidad de la serie |
| `UMBRAL_RECHAZO` | 0,15 | Corte de la bifurcación del DAG 02 |
| `MINIMO_FILAS_VALIDAS` | 60 | Segundo criterio de bloqueo |
| `TOLERANCIA_VOLUMEN_PCT` | 25,0 | Holgura de R07 |
| `VENTANA_SMA_CORTA` / `LARGA` | 7 / 30 | Medias móviles |
| `VENTANA_VOLATILIDAD` | 30 | Desviación estándar |
| `CORTE_VOLATILIDAD_BAJA` / `MEDIA` | 2,0 / 5,0 | Etiquetas de T03 |
| `CONCILIACION_DESVIACION_ACEPTABLE` | 0,5 % | Veredicto del DAG 05 |
| `CONCILIACION_COBERTURA_MINIMA` | 60 % | Veredicto del DAG 05 |
| `CONCILIACION_ORIGEN_DATOS` | `exchange_ws` | Qué ventanas entran en la conciliación (regla C01) |
