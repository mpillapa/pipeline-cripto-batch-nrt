-- ---------------------------------------------------------------------------
-- Modelo del camino batch + resultado de la conciliacion.
--
-- Cuatro tablas:
--   dim_activo           Dimension. Un registro por activo negociado.
--   hechos_ohlcv_diario  Tabla de hechos. Una vela diaria por activo y fecha.
--   control_lotes        Bitacora: estado y metricas de cada corrida.
--   conciliacion         Comparacion entre el flujo NRT y el flujo batch.
--
-- Ver contratos/CONTRATO_DATOS.md para el significado de cada campo.
-- ---------------------------------------------------------------------------

USE cripto;

-- ---------------------------------------------------------------------------
-- DIMENSION
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_activo (
    id_activo           VARCHAR(20)  NOT NULL,
    simbolo             VARCHAR(20)  NOT NULL,
    activo_base         VARCHAR(10)  NOT NULL,
    activo_cotizacion   VARCHAR(10)  NOT NULL,
    nombre              VARCHAR(80)  NOT NULL,
    categoria           VARCHAR(40),
    estado              VARCHAR(20)  NOT NULL DEFAULT 'ACTIVO',
    actualizado_en      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id_activo),
    -- El simbolo es la clave natural: es lo que viaja por Kafka y lo que usa el
    -- exchange. id_activo es la clave sustituta. Ambos deben ser unicos, pero
    -- solo uno puede ser la primaria.
    UNIQUE KEY uq_simbolo (simbolo)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ---------------------------------------------------------------------------
-- HECHOS
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hechos_ohlcv_diario (
    id_activo           VARCHAR(20)    NOT NULL,
    fecha               DATE           NOT NULL,
    lote_id             VARCHAR(20)    NOT NULL,

    -- DECIMAL y no DOUBLE: el decimal exacto evita que dos sumas del mismo
    -- conjunto de filas den resultados distintos segun el orden. Ocho decimales
    -- cubren la precision de activos de bajo valor unitario.
    apertura            DECIMAL(20,8)  NOT NULL,
    maximo              DECIMAL(20,8)  NOT NULL,
    minimo              DECIMAL(20,8)  NOT NULL,
    cierre              DECIMAL(20,8)  NOT NULL,
    volumen_base        DECIMAL(24,8)  NOT NULL,
    volumen_usdt        DECIMAL(24,8)  NOT NULL,
    n_trades            INT            NOT NULL,

    -- Indicadores derivados (DAG 03). Admiten NULL a proposito: los primeros
    -- dias de la serie no tienen historia suficiente para una media movil de 30
    -- dias. Rellenarlos con 0 seria inventar un dato.
    retorno_pct         DECIMAL(10,4),
    sma_7               DECIMAL(20,8),
    sma_30              DECIMAL(20,8),
    volatilidad_30d     DECIMAL(10,4),
    nivel_volatilidad   VARCHAR(10),

    cargado_en          TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,

    -- Clave compuesta natural. Sin esto, re-ejecutar el DAG 04 duplicaria la
    -- serie completa y todos los indicadores saldrian mal.
    PRIMARY KEY (id_activo, fecha),
    KEY idx_lote (lote_id),
    KEY idx_fecha (fecha),
    CONSTRAINT fk_ohlcv_activo
        FOREIGN KEY (id_activo) REFERENCES dim_activo (id_activo)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ---------------------------------------------------------------------------
-- BITACORA DE LOTES
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS control_lotes (
    lote_id             VARCHAR(20)   NOT NULL,
    estado              VARCHAR(20)   NOT NULL,
    simbolos            VARCHAR(200),
    filas_descargadas   INT,
    filas_validas       INT,
    filas_rechazadas    INT,
    tasa_rechazo        DECIMAL(6,4),
    filas_cargadas      INT,
    actualizado_en      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (lote_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ---------------------------------------------------------------------------
-- CONCILIACION NRT <-> BATCH
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS conciliacion (
    simbolo             VARCHAR(20)    NOT NULL,
    -- Granularidad horaria, no diaria ni por minuto. Ver seccion 7 del contrato.
    fecha_hora          DATETIME       NOT NULL,
    lote_id             VARCHAR(20)    NOT NULL,

    vwap_streaming      DECIMAL(20,8),
    n_trades_streaming  INT,
    cierre_batch        DECIMAL(20,8),
    n_trades_batch      INT,

    desviacion_pct      DECIMAL(10,4),
    -- Que porcentaje de los trades reales alcanzo a ver el flujo en vivo.
    -- Es la metrica que da sentido a la desviacion: una desviacion pequena con
    -- cobertura baja no significa que el streaming este midiendo bien.
    cobertura_pct       DECIMAL(10,4),

    veredicto           VARCHAR(20)    NOT NULL,
    evaluado_en         TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (simbolo, fecha_hora),
    KEY idx_veredicto (veredicto)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
