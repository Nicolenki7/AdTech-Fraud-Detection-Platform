# Ruta abierta · qué queda y qué falta

## Estado original (se conserva)

Este proyecto se construyó con **Snowflake + Snowpark**: SQL avanzado para el scoring, Python para las
transformaciones y un dashboard en Streamlit. La configuración se documenta en `config.py.example`
y el despliegue en `DEPLOY.md`.

## Por qué no hay un port ejecutable todavía

No hay datos versionados en el repositorio: la tabla de eventos vive en Snowflake y se accede con credenciales.
Sin datos no hay informe, y no voy a publicar gráficos de un dataset que nadie puede inspeccionar.

## Camino concreto para reabrirlo

1. **Generar un dataset sintético reproducible**: un `gen_events.py` con semilla fija que produzca eventos
   de impresiones y clics con una tasa de fraude conocida (por ejemplo 2 %). Al declararlo sintético,
   las métricas del modelo se vuelven defendibles.
2. **Portar el SQL** de Snowflake a DuckDB: casi todo es SQL estándar; lo específico de Snowpark
   (UDFs en Python) se traduce a funciones de DuckDB o a una columna calculada.
3. **Fijar el scoring** con una matriz de confusión versionada: precisión, recall y falsos positivos.
   En fraude, el falso positivo tiene costo comercial real y hay que mostrarlo, no esconderlo.
4. **Reemplazar Streamlit por Superset** si se busca stack 100 % abierto, o mantenerlo: Streamlit ya es open source.
5. **Test de contrato** sobre el esquema de eventos, como en `ETL_Egatur_INE_Esp`.

## Referencia ya funcionando

`Tech_GlobalStore_Retail_Analysis-Fabric` tiene el patrón resuelto en `stack-abierto/`: modelo en DuckDB,
vistas de KPIs y siete controles de calidad que fallan con código de error si el dato cambia.

## Lo que sí se sostiene hoy

El diseño del pipeline, la lógica de detección y la separación entre capa de scoring y capa de presentación
son decisiones de arquitectura que no dependen de Snowflake.
