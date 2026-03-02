# 🚀 Guía de Deploy - AdTech Fraud Detection Platform

Esta guía te llevará paso a paso para desplegar el dashboard en Streamlit Cloud.

> 🎉 **Demo en vivo:** https://adtech-fraud-detection-platform.streamlit.app/

---

## ✅ Pre-requisitos

1. Cuenta de GitHub (ya la tienes: @Nicolenki7)
2. Cuenta de Streamlit Cloud (gratis): https://streamlit.io/cloud
3. Este repositorio ya está en GitHub: `Nicolenki7/AdTech-Fraud-Detection-Platform`

---

## 📋 Paso 1: Verificar que el código esté en GitHub

```bash
cd /home/nicolas/.openclaw/workspace/github-audit/AdTech-Fraud-Detection-Platform

# Verificar estado
git status

# Si hay cambios pendientes:
git add .
git commit -m "docs: README completo con guía de deploy"
git push origin main
```

---

## 📋 Paso 2: Deploy en Streamlit Cloud

### 2.1 Ir a Streamlit Cloud
- URL: https://streamlit.io/cloud
- Click en **"New app"**

### 2.2 Conectar GitHub
- Seleccionar **"GitHub"** como fuente
- Autorizar a Streamlit si es la primera vez
- Buscar tu repositorio: `Nicolenki7/AdTech-Fraud-Detection-Platform`

### 2.3 Configurar el Deploy

| Campo | Valor |
|-------|-------|
| **Main file path** | `dashboard/app.py` |
| **Branch** | `main` |
| **Python version** | `3.12` |

### 2.4 Variables de Entorno (Opcional)

Si vas a conectar a Snowflake real, agrega estas variables:

| Name | Value |
|------|-------|
| `SNOWFLAKE_ACCOUNT` | tu-account.snowflakecomputing.com |
| `SNOWFLAKE_USER` | tu_usuario |
| `SNOWFLAKE_PASSWORD` | tu_password |
| `SNOWFLAKE_DATABASE` | ADTECH_FRAUD |
| `SNOWFLAKE_SCHEMA` | FRAUD_DETECTION |

> **Nota:** Para demo, el dashboard usa datos mock generados internamente. No necesitas Snowflake para probarlo.

### 2.5 Click en "Deploy!"

Streamlit Cloud va a:
1. Clonar tu repositorio
2. Instalar dependencias de `dashboard/requirements.txt`
3. Ejecutar `dashboard/app.py`

### 2.6 Obtener URL Pública

Una vez deployado, vas a recibir una URL como:
```
https://nicolenki7-adtech-fraud-detection-platform-dashboard-app-xyz123.streamlit.app
```

> 🎉 **URL actual de tu app:** https://adtech-fraud-detection-platform.streamlit.app/

¡Esta es tu app pública! Podés compartirla en LinkedIn, portfolio, etc.

---

## 🔧 Paso 3: Solución de Problemas

### Error: "ModuleNotFoundError: No module named 'plotly'"

**Causa:** Las dependencias no se instalaron correctamente.

**Solución:** Verificar que `dashboard/requirements.txt` exista y tenga:

```txt
streamlit>=1.28.0
plotly>=5.17.0
pandas>=2.0.0
numpy>=1.24.0
```

### Error: "App crashed after deployment"

**Causa:** Error en el código de `app.py`.

**Solución:**
1. Click en "Manage app" → "Logs"
2. Revisar el error en los logs
3. Corregir el código localmente
4. Hacer push a GitHub
5. Streamlit Cloud redeploya automáticamente

### Error: "Timeout waiting for app to start"

**Causa:** El dashboard tarda mucho en cargar.

**Solución:**
- El dashboard usa `@st.cache_data` para cachear datos
- Verificar que la función `load_mock_data()` tenga el decorador
- Reducir la cantidad de datos mock si es necesario

---

## 🎨 Paso 4: Personalización (Opcional)

### Cambiar el título de la app

En `dashboard/app.py`, modificar:

```python
st.set_page_config(
    page_title="TU TÍTULO PERSONALIZADO",
    page_icon="🛡️",
    # ...
)
```

### Cambiar colores del tema

En el CSS custom (línea ~45), modificar los colores:

```css
.metric-card {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    /* Cambiar estos colores */
}
```

### Agregar tu logo

En el header, agregar:

```python
st.image("https://tu-logo-url.com/logo.png", width=100)
```

---

## 📊 Paso 5: Conectar a Datos Reales (Producción)

Para conectar a Snowflake real en lugar de datos mock:

### 5.1 Modificar `load_mock_data()`

Reemplazar con consulta real:

```python
@st.cache_data(ttl=300)
def load_real_data():
    from snowflake.snowpark import Session
    
    # Configurar conexión
    session = Session.builder.configs({
        "account": st.secrets["SNOWFLAKE_ACCOUNT"],
        "user": st.secrets["SNOWFLAKE_USER"],
        "password": st.secrets["SNOWFLAKE_PASSWORD"],
        "database": "ADTECH_FRAUD",
        "schema": "FRAUD_DETECTION"
    }).create()
    
    # Consultar datos
    df = session.table("GOLD_FRAUD_ALERTS").to_pandas()
    return df
```

### 5.2 Configurar Secrets en Streamlit Cloud

1. Ir a "Manage app" → "Secrets"
2. Agregar en formato TOML:

```toml
[snowflake]
account = "tu-account"
user = "tu_usuario"
password = "tu_password"
database = "ADTECH_FRAUD"
schema = "FRAUD_DETECTION"
```

---

## ✅ Checklist Final

- [ ] Código en GitHub (rama main)
- [ ] `dashboard/requirements.txt` actualizado
- [ ] Deploy en Streamlit Cloud completado
- [ ] URL pública obtenida
- [ ] Dashboard carga sin errores
- [ ] Todas las 5 vistas funcionan
- [ ] Datos se muestran correctamente

---

## 🔗 Links Útiles

- **Streamlit Docs:** https://docs.streamlit.io
- **Streamlit Cloud:** https://streamlit.io/cloud
- **Plotly Graph Gallery:** https://plotly.com/python/
- **Tu Repo:** https://github.com/Nicolenki7/AdTech-Fraud-Detection-Platform

---

## 🆘 Soporte

Si tenés problemas:

1. Revisar logs en Streamlit Cloud ("Manage app" → "Logs")
2. Probar localmente: `streamlit run dashboard/app.py`
3. Verificar dependencias: `pip install -r dashboard/requirements.txt`

---

*Guía creada: 2026-03-03 | Versión: 1.0*
