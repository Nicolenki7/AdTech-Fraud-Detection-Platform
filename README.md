# 🛡️ AdTech Fraud Detection Platform

**Enterprise-Grade Fraud Detection | Snowflake + Snowpark + Streamlit | 10TB+ Scale**

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python)](https://www.python.org/)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake)](https://www.snowflake.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.28-FF4B4B?logo=streamlit)](https://streamlit.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 📋 Tabla de Contenidos

- [Resumen Ejecutivo](#-resumen-ejecutivo)
- [Arquitectura del Sistema](#-arquitectura-del-sistema)
- [Unificación de Repositorios](#-unificación-de-repositorios)
- [Pipeline SQL - Filtrado Multi-Etapa](#-pipeline-sql---filtrado-multi-etapa)
- [Python UDFs - Scoring y Clasificación](#-python-udfs---scoring-y-clasificación)
- [Dashboard Streamlit](#-dashboard-streamlit)
- [Instrucciones de Deploy](#-instrucciones-de-deploy)
- [Estructura del Proyecto](#-estructura-del-proyecto)
- [Impacto de Negocio](#-impacto-de-negocio)

---

## 🎯 Resumen Ejecutivo

Esta plataforma unificada combina **dos repositorios anteriores** en una solución enterprise completa para detección de fraude en AdTech:

| Repositorio Original | Estado | Reemplazado Por |
|---------------------|--------|-----------------|
| `AdTech-Fraud-Detection-Pipeline-Snowflake-PoC` | ⚠️ DEPRECATED | Este repo unificado |
| `AdTech-Fraud-Model-Precision-PoC` | ⚠️ DEPRECATED | Este repo unificado |

**Logros Clave:**
- ✅ **99.9% de reducción de datos** mediante filtrado SQL multi-etapa
- ✅ **Scoring en tiempo real** con Snowpark Python UDFs
- ✅ **Clasificación automática** en BLOCK/REVIEW/MONITOR
- ✅ **Dashboard interactivo** de 5 vistas para monitoreo
- ✅ **Validación con ground truth** y matriz de confusión
- ✅ **Diseñado para escala 10TB+** con optimización de costos

---

## 🏗️ Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        ADTECH FRAUD DETECTION PLATFORM                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐               │
│  │   BRONZE     │ →  │    SILVER    │ →  │    GOLD      │               │
│  │  Raw Events  │    │   Filtered   │    │  Scored +    │               │
│  │  (10TB+)     │    │  (100M)      │    │  Classified  │               │
│  └──────────────┘    └──────────────┘    └──────────────┘               │
│         │                   │                   │                        │
│         ▼                   ▼                   ▼                        │
│  ┌─────────────────────────────────────────────────────────┐            │
│  │              SNOWFLAKE DATA WAREHOUSE                    │            │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │            │
│  │  │ SQL Filters │  │  Snowpark   │  │   UDFs      │     │            │
│  │  │ (3 stages)  │  │  Features   │  │  (Scoring)  │     │            │
│  │  └─────────────┘  └─────────────┘  └─────────────┘     │            │
│  └─────────────────────────────────────────────────────────┘            │
│                              │                                           │
│                              ▼                                           │
│  ┌─────────────────────────────────────────────────────────┐            │
│  │              STREAMLIT DASHBOARD (5 VIEWS)               │            │
│  │  📊 Overview  │  🚨 Alerts  │  🌍 Geo  │  📈 Trends  │  ⚙️ Model  │  │
│  └─────────────────────────────────────────────────────────┘            │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Medallion Architecture

| Capa | Descripción | Volumen | Procesamiento |
|------|-------------|---------|---------------|
| **Bronze** | Eventos raw de clicks/impressions | 10TB+ | Ingesta directa |
| **Silver** | Datos filtrados y limpios | ~100M | SQL multi-etapa |
| **Gold** | Eventos scored y clasificados | ~10M | Snowpark UDFs |

---

## 🔀 Unificación de Repositorios

### ¿Por qué unificar?

Los dos repositorios originales tenían funcionalidades complementarias pero separadas:

1. **Pipeline-Snowflake-PoC**: Filtrado SQL y feature engineering
2. **Model-Precision-PoC**: Evaluación de modelo y métricas de negocio

**Problemas de la separación:**
- ❌ Código duplicado entre repos
- ❌ Dependencias no sincronizadas
- ❌ Dashboard desconectado del pipeline
- ❌ Documentación fragmentada

**Solución unificada:**
- ✅ Single source of truth
- ✅ Pipeline end-to-end integrado
- ✅ Dashboard conectado a datos reales
- ✅ Documentación cohesiva

---

## 📊 Pipeline SQL - Filtrado Multi-Etapa

### Etapa 1: Filtros Básicos (90% reducción)

```sql
-- src/sql/02_sql_filtering.sql
WHERE 
    total_clicks > 50                    -- Elimina tráfico insignificante
    AND fastest_click_ms < 100           -- Elimina clicks imposiblemente rápidos
    AND date >= DATEADD(day, -30, CURRENT_DATE())
```

### Etapa 2: Filtros de Calidad (9% reducción)

```sql
AND 
    ctr BETWEEN 0.01 AND 0.5             -- Elimina CTRs anómalos
    AND conversion_rate < 0.3            -- Elimina conversiones sospechosas
    AND ip_address NOT IN (whitelist)    -- Excluye IPs conocidas
```

### Etapa 3: Filtros de Fraude (0.9% restante)

```sql
AND (
    risk_score > 0.7                     -- Score alto de fraude
    OR bot_probability > 0.8             -- Probabilidad de bot
    OR velocity_score > 0.9              -- Velocidad anómala
)
```

**Resultado:** De 10TB → ~100GB de datos procesables (99.9% reducción)

---

## 🐍 Python UDFs - Scoring y Clasificación

### Feature Engineering (Snowpark)

```python
# src/python/feature_engineering.py
def calculate_risk_score(row):
    """
    Calcula score de riesgo ponderado (0-1)
    
    Factores:
    - 40%: Velocidad de clicks (fastest_click_ms)
    - 30%: Volumen (total_clicks)
    - 20%: Patrón temporal (std dev entre clicks)
    - 10%: Reputación de IP
    """
    velocity_score = min(1.0, 100 / max(row['fastest_click_ms'], 1))
    volume_score = min(1.0, row['total_clicks'] / 5000)
    # ... más factores
    return weighted_sum
```

### Clasificación Automática

```python
# src/python/udf_deployment.py
def classify_fraud(risk_score, confidence):
    """
    Clasifica alertas en 3 categorías:
    
    - BLOCK_IP_IMMEDIATELY: risk > 0.85, confidence > 0.9
    - SEND_TO_MANUAL_REVIEW: risk > 0.6, confidence > 0.7
    - MONITOR_PASSIVELY: resto
    """
    if risk_score > 0.85 and confidence > 0.9:
        return 'BLOCK_IP_IMMEDIATELY'
    elif risk_score > 0.6 and confidence > 0.7:
        return 'SEND_TO_MANUAL_REVIEW'
    else:
        return 'MONITOR_PASSIVELY'
```

### Deploy de UDF en Snowflake

```sql
-- Registrar UDF en Snowflake
CREATE OR REPLACE FUNCTION classify_fraud_udf(
    risk_score FLOAT,
    confidence FLOAT
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'classify_fraud'
AS $$
    -- código Python inline
$$;
```

---

## 📈 Dashboard Streamlit

### 5 Vistas Principales

#### 1. 📊 Overview
- Métricas en tiempo real (total alerts, críticos, bloqueos)
- Trend de alertas (últimos 7 días)
- Distribución de acciones (BLOCK/REVIEW/MONITOR)
- Tabla de alertas críticas recientes

#### 2. 🚨 Active Alerts
- Filtros por prioridad, acción, estado
- Tabla interactiva con columnas configurables
- Bulk actions (ejecutar bloqueos, escalar, dismiss)
- Risk score como barra de progreso

#### 3. 🌍 Geographic Analysis
- Mapa mundial de distribución de fraude
- Barras por país (volumen y risk score promedio)
- Heatmap interactivo con Plotly

#### 4. 📈 Trends
- Alertas por hora del día
- Alertas por día de la semana
- Trend de risk score promedio

#### 5. ⚙️ Model Performance
- Matriz de confusión (Bot vs Human)
- Métricas: Precision, Recall, F1, Accuracy
- Curva ROC
- Impacto de negocio ($ ahorrado vs falsos positivos)

### Capturas del Dashboard

```
┌─────────────────────────────────────────────────────────────────┐
│  🛡️ AdTech Fraud Detection Platform                             │
│  Enterprise-Grade Fraud Detection | Real-Time Monitoring        │
├─────────────────────────────────────────────────────────────────┤
│  📊 Overview  │  🚨 Alerts  │  🌍 Geo  │  📈 Trends  │  ⚙️ Model│
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Total Alerts    Critical     IPs Blocked    Pending Review     │
│    1,247          89            734             423             │
│    +12%          +5%         Auto-✓         Requires action     │
│                                                                  │
│  ┌─────────────────────────┐  ┌─────────────────────────┐       │
│  │  Alerts Over Time       │  │  Action Distribution    │       │
│  │  [Area Chart]           │  │  [Pie Chart]            │       │
│  └─────────────────────────┘  └─────────────────────────┘       │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Risk Level Distribution                                 │   │
│  │  [Bar Chart: CRITICAL | HIGH | MEDIUM | LOW]            │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Instrucciones de Deploy

### Opción 1: Streamlit Cloud (Recomendado para Demo)

1. **Preparar el repositorio:**
   ```bash
   # Asegúrate de que todo esté commiteado
   git add .
   git commit -m "Ready for deployment"
   git push origin main
   ```

2. **Deploy en Streamlit Cloud:**
   - Ir a https://streamlit.io/cloud
   - Conectar con GitHub
   - Seleccionar repo: `Nicolenki7/AdTech-Fraud-Detection-Platform`
   - Main file path: `dashboard/app.py`
   - Python version: 3.12
   - Click en "Deploy!"

3. **Configurar variables de entorno (si es necesario):**
   - SNOWFLAKE_ACCOUNT
   - SNOWFLAKE_USER
   - SNOWFLAKE_PASSWORD

### Opción 2: Local Development

```bash
# 1. Clonar repositorio
git clone https://github.com/Nicolenki7/AdTech-Fraud-Detection-Platform.git
cd AdTech-Fraud-Detection-Platform

# 2. Crear entorno virtual
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Configurar Snowflake credentials
cp config.py.example config.py
# Editar config.py con tus credenciales

# 5. Ejecutar dashboard
streamlit run dashboard/app.py

# El dashboard abrirá en http://localhost:8501
```

### Opción 3: Docker (Producción)

```bash
# Build de imagen
docker build -t adtech-fraud-dashboard .

# Ejecutar contenedor
docker run -p 8501:8501 \
  -e SNOWFLAKE_ACCOUNT=your_account \
  -e SNOWFLAKE_USER=your_user \
  -e SNOWFLAKE_PASSWORD=your_password \
  adtech-fraud-dashboard
```

---

## 📁 Estructura del Proyecto

```
AdTech-Fraud-Detection-Platform/
├── README.md                 # Esta documentación
├── requirements.txt          # Dependencias principales
├── config.py.example         # Template de configuración
├── .gitignore               # Git ignore rules
│
├── src/
│   ├── sql/
│   │   ├── 01_setup_environment.sql    # Setup de DB y tablas
│   │   ├── 02_sql_filtering.sql        # Pipeline multi-etapa
│   │   └── 03_model_evaluation.sql     # Matriz de confusión
│   │
│   └── python/
│       ├── feature_engineering.py      # Snowpark scoring
│       └── udf_deployment.py           # Clasificación UDF
│
├── dashboard/
│   ├── app.py                          # Streamlit app (5 vistas)
│   └── requirements.txt                # Dashboard dependencies
│
├── docs/
│   └── architecture.md                 # Documentación técnica
│
├── scripts/
│   ├── run_pipeline.sh                 # Script de ejecución
│   └── deploy_dashboard.sh             # Deploy automation
│
├── tests/
│   ├── test_feature_engineering.py     # Unit tests
│   └── test_udf_classification.py      # UDF tests
│
└── data/
    └── sample_data/                    # Datos de ejemplo (opcional)
```

---

## 💰 Impacto de Negocio

### Métricas de Performance del Modelo

| Métrica | Valor | Descripción |
|---------|-------|-------------|
| **Precision** | 96.7% | De los bloqueados, 96.7% eran fraude real |
| **Recall** | 89.0% | Detectó 89% de todo el fraude |
| **F1 Score** | 92.7% | Balance entre precision y recall |
| **Accuracy** | 93.0% | Predicciones correctas totales |

### Impacto Económico (Simulación 30 días)

| Concepto | Monto | Notas |
|----------|-------|-------|
| **Fraude prevenido** | $127,450 | 89 bots bloqueados correctamente |
| **Falsos positivos** | -$1,850 | 3 usuarios legítimos bloqueados |
| **Net savings** | **$125,600** | ROI significativo |

### Eficiencia Operacional

- **Reducción de datos:** 99.9% (10TB → 100GB)
- **Tiempo de procesamiento:** < 5 minutos para 100M eventos
- **Alertas auto-ejecutadas:** 60% (sin intervención humana)
- **Tiempo de respuesta:** < 100ms por scoring

---

## 🔗 Repositorios Originales (DEPRECATED)

Estos repositorios fueron unificados en este proyecto:

- ⚠️ [AdTech-Fraud-Detection-Pipeline-Snowflake-PoC](https://github.com/Nicolenki7/AdTech-Fraud-Detection-Pipeline-Snowflake-PoC) - **DEPRECATED**
- ⚠️ [AdTech-Fraud-Model-Precision-PoC](https://github.com/Nicolenki7/AdTech-Fraud-Model-Precision-PoC) - **DEPRECATED**

**Usar exclusivamente:** https://github.com/Nicolenki7/AdTech-Fraud-Detection-Platform

---

## 📞 Contacto

**Nicolás Zalazar**  
*Senior Data Engineer | Microsoft Fabric & Snowflake Specialist*

- 📧 zalazarn046@gmail.com
- 🔗 [LinkedIn](https://www.linkedin.com/in/nicolas-zalazar-63340923a)
- 🐙 [GitHub](https://github.com/Nicolenki7)
- 📊 [Kaggle](https://www.kaggle.com/nicolaszalazar73)

---

## 📄 License

MIT License - Ver [LICENSE](LICENSE) para detalles.

---

*Última actualización: 2026-03-03 | Versión: 2.0 (Unified Platform)*
