# Plan de ejecucion

## Estado actual
- Pipeline completo: `src/sql/01_setup_environment.sql`, `02_sql_filtering.sql`, `03_model_evaluation.sql` + `src/python/feature_engineering.py` y `udf_deployment.py`.
- Dashboard Streamlit (`dashboard/app.py`), arquitectura documentada y guia de despliegue.

## Proximos pasos
1. Sustituir la ruta local que quedo en `DEPLOY.md` (linea 20) por una ruta relativa.
2. Fixtures pequenas y test de columnas de `feature_engineering` con pytest.
3. Parametrizar los umbrales de deteccion en `config.py` en lugar de constantes.
4. CI con mocks de Snowflake (sin credenciales reales).

## Criterio de aceptacion
- `pytest` verde en CI sin cuenta de Snowflake; `compileall src` limpio.

## Riesgos
- Sin dataset sintetico versionado, los tests dependen de la plataforma.
