"""Modulo compartido por los DAGs del camino batch.

Una capa por archivo:

    config.py          Parametros del entorno: rutas, conexiones, umbrales.
    utilidades.py      Lote_id, rutas por lote, lectura/escritura, logs.
    zonas.py           Acceso a las zonas de datos (bronce, cuarentena, plata).
    clientes_api.py    Descarga desde la API REST del exchange.
    reglas_calidad.py  Reglas de validacion R01..R07, funciones puras.
    transformaciones.py Normalizacion e indicadores, funciones puras.
    repositorio.py     Unico punto de acceso a MySQL.
    observabilidad.py  Publicacion de eventos de control y logs a Logstash.
    conciliacion.py    Comparacion entre el flujo NRT y el flujo batch.

Los DAGs solo orquestan: no contienen reglas de negocio ni SQL. Eso permite
ejecutar y probar la logica sin levantar Airflow.
"""
