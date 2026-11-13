"""Construye docs/report.html a partir de la muestra sintética agregada.

Uso: python src/python/generate_sample.py && python src/python/build_report.py
"""
import pathlib
import sys

import pandas as pd

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from report import escribir, num

ROOT = pathlib.Path(__file__).resolve().parents[2]
d = pd.read_csv(ROOT / "data" / "sample" / "alertas_resumen.csv")

filas = []
for _, r in d.iterrows():
    filas.append({"a": "todos", "g": "volumen_" + r.dimension, "k": str(r.clave), "v": int(r.alertas), "u": ""})
    filas.append({"a": "todos", "g": "bloqueo_" + r.dimension, "k": str(r.clave), "v": float(r.bloqueo_pct), "u": "%"})

pais = d[d.dimension == "pais"].sort_values("bloqueo_pct", ascending=False)
tabla = [[str(r.clave), num(int(r.alertas)), f"{num(r.bloqueo_pct, 1)} %", num(r.riesgo_medio, 3)]
         for _, r in pais.iterrows()]

escribir(
    ROOT / "docs" / "report.html",
    eyebrow="AdTech · detección de fraude (datos sintéticos)",
    titulo="Alertas de fraude: qué se bloquea y qué se revisa",
    pregunta=("Un sistema de detección no sirve por cuántas alertas levanta, sino por lo que hace con "
              "ellas: cuántas bloquea solo, cuántas manda a revisión humana y en qué países se concentra. "
              "Este informe agrega la muestra que genera el propio repo con una semilla fija."),
    kpis=[("Alertas", num(int(d[d.dimension == "pais"].alertas.sum())), "muestra sintética"),
          ("Países", num(d[d.dimension == "pais"].clave.nunique()), "origen simulado"),
          ("Riesgo medio", num(d[d.dimension == "pais"].riesgo_medio.mean(), 3), "escala 0,2-1"),
          ("Bloqueo automático", f"{num(d[d.dimension == 'pais'].bloqueo_pct.mean(), 1)} %", "media por país")],
    filtros=None,
    graficos=[
        {"id": "vp", "grupo": "volumen_pais", "titulo": "Alertas por país de origen",
         "subtitulo": "volumen simulado, reparto uniforme por diseño", "unidad": ""},
        {"id": "bp", "grupo": "bloqueo_pais", "titulo": "Bloqueo automático por país",
         "subtitulo": "% de las alertas de ese país que se bloquean sin revisión", "unidad": "%"},
        {"id": "bpr", "grupo": "bloqueo_prioridad", "titulo": "Bloqueo automático por prioridad",
         "subtitulo": "cuanto más crítica la alerta, más se bloquea", "unidad": "%"},
    ],
    filas=filas,
    tabla={"titulo": "Detalle por país", "subtitulo": "ordenado por tasa de bloqueo",
           "cabeceras": ["País", "Alertas", "Bloqueo", "Riesgo medio"], "filas": tabla},
    metodo=["Los datos son sintéticos: los genera src/python/generate_sample.py con seed=42, igual que el dashboard.",
            "El script agrega 500 alertas por país y por prioridad y versiona el resumen en data/sample/alertas_resumen.csv.",
            "Bloqueo = alertas con acción BLOCK_IP_IMMEDIATELY / alertas de ese grupo.",
            "El informe lee el CSV agregado, no vuelve a simular: así el HTML y el CSV no pueden divergir."],
    limitaciones=["Datos sintéticos: sirven para mostrar el pipeline y el panel, no describen fraude real.",
                  "El reparto por país es uniforme por construcción, así que el volumen no dice nada del mercado.",
                  "Con 500 alertas y 8 países, cada tasa se mueve ~3 puntos por caso.",
                  "No hay serie temporal: las marcas de tiempo del dashboard son horas relativas, no fechas."],
    fuente="data/sample/alertas_resumen.csv (sintético, seed 42)",
    metodo_sub="El resumen agregado es lo único que el informe consume.",
    lim_sub="Lo que una muestra simulada no puede afirmar.")
print("docs/report.html:", (ROOT / "docs" / "report.html").stat().st_size, "bytes")
