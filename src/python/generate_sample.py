"""Genera la muestra sintética de alertas que alimenta docs/report.html.

El dashboard del repo (dashboard/app.py) trabaja con datos simulados con
np.random.seed(42). Este script reproduce ese mismo escenario con el seed fijo,
lo agrega por país, prioridad y acción, y escribe un resumen pequeño.

Los datos son SINTÉTICOS: no provienen de tráfico real ni de Snowflake.

Uso: python src/python/generate_sample.py [--seed 42] [--n 500]
"""
import argparse
import csv
import random
import pathlib

ACCIONES = ["BLOCK_IP_IMMEDIATELY", "SEND_TO_MANUAL_REVIEW", "MONITOR_PASSIVELY"]
PESOS_ACCION = [0.6, 0.3, 0.1]
PRIORIDADES = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
PESOS_PRIORIDAD = [0.15, 0.35, 0.35, 0.15]
PAISES = ["US", "CN", "RU", "BR", "IN", "DE", "GB", "FR"]


def generar(n=500, seed=42):
    rnd = random.Random(seed)
    filas = []
    for i in range(n):
        filas.append({
            "alert_id": f"ALERT-{i:06d}",
            "pais": rnd.choice(PAISES),
            "prioridad": rnd.choices(PRIORIDADES, PESOS_PRIORIDAD)[0],
            "accion": rnd.choices(ACCIONES, PESOS_ACCION)[0],
            "risk_score": round(0.2 + 0.8 * rnd.betavariate(2, 3), 4),
        })
    return filas


def resumen(filas):
    """Una fila por país y por prioridad: lo que consume el informe."""
    out = []
    for pais in PAISES:
        grupo = [f for f in filas if f["pais"] == pais]
        if not grupo:
            continue
        bloqueo = sum(1 for f in grupo if f["accion"] == "BLOCK_IP_IMMEDIATELY") / len(grupo)
        riesgo = sum(f["risk_score"] for f in grupo) / len(grupo)
        out.append({"dimension": "pais", "clave": pais, "alertas": len(grupo),
                    "bloqueo_pct": round(100 * bloqueo, 1), "riesgo_medio": round(riesgo, 3)})
    for pri in PRIORIDADES:
        grupo = [f for f in filas if f["prioridad"] == pri]
        if not grupo:
            continue
        bloqueo = sum(1 for f in grupo if f["accion"] == "BLOCK_IP_IMMEDIATELY") / len(grupo)
        riesgo = sum(f["risk_score"] for f in grupo) / len(grupo)
        out.append({"dimension": "prioridad", "clave": pri, "alertas": len(grupo),
                    "bloqueo_pct": round(100 * bloqueo, 1), "riesgo_medio": round(riesgo, 3)})
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--n", type=int, default=500)
    a = ap.parse_args()
    raiz = pathlib.Path(__file__).resolve().parents[2]
    destino = raiz / "data" / "sample" / "alertas_resumen.csv"
    destino.parent.mkdir(parents=True, exist_ok=True)
    filas = resumen(generar(a.n, a.seed))
    with destino.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["dimension", "clave", "alertas", "bloqueo_pct", "riesgo_medio"])
        w.writeheader()
        w.writerows(filas)
    print(destino, len(filas), "filas")
