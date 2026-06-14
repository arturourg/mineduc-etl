"""
MINEDUC ETL Pipeline — Análisis de Matrícula Escolar Chile
===========================================================
Pipeline ETL que descarga datos públicos del Ministerio de Educación
de Chile, los transforma con pandas y genera insights accionables.

Dataset: Matrícula por establecimiento (datos.mineduc.cl)
Autor: Arturo Urra Galdames
"""

import os
import sqlite3
import urllib.request
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────

DB_PATH       = "outputs/mineduc.db"
DATA_DIR      = "data/raw"
PROCESSED_DIR = "data/processed"

# Dataset público MINEDUC — matrícula por establecimiento 2023
# Fuente: https://datosabiertos.mineduc.cl
DATA_URL = (
    "https://datosabiertos.mineduc.cl/wp-content/uploads/2024/04/"
    "20240408_Matrícula_unica_2023_20240408.csv"
)
LOCAL_CSV = os.path.join(DATA_DIR, "matricula_2023.csv")

# ─── EXTRACCIÓN ───────────────────────────────────────────────────────────────

def extract(url: str, local_path: str) -> str:
    """Descarga el CSV si no existe localmente (caché simple)."""
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(local_path):
        print(f"[EXTRACT] Usando caché local: {local_path}")
        return local_path
    print(f"[EXTRACT] Descargando datos desde {url} ...")
    urllib.request.urlretrieve(url, local_path)
    print(f"[EXTRACT] Descarga completa → {local_path}")
    return local_path


def load_csv(path: str) -> pd.DataFrame:
    """Lee el CSV con encoding latin-1 (estándar para archivos MINEDUC)."""
    df = pd.read_csv(path, encoding="latin-1", sep=";", low_memory=False)
    print(f"[EXTRACT] {len(df):,} filas | {len(df.columns)} columnas")
    return df


# ─── TRANSFORMACIÓN ───────────────────────────────────────────────────────────

COLUMNAS_RELEVANTES = {
    "RBD":              "rbd",
    "NOM_RBD":          "nombre_escuela",
    "NOM_COM_RBD":      "comuna",
    "NOM_REG_RBD_A":    "region",
    "COD_DEPE2":        "dependencia_cod",
    "COD_ENSE":         "nivel_cod",
    "GEN_ALU":          "genero_cod",
    "MRUN":             "matricula",
}

DEPENDENCIA_MAP = {
    1: "Municipal",
    2: "Part. Subvencionado",
    3: "Part. Pagado",
    4: "Corp. Adm. Delegada",
    5: "Servicio Local",
}

NIVEL_MAP = {
    10: "Parvularia",
    20: "Diferencial",
    60: "Primaria (1°-6°)",
    63: "Primaria (1°-6°)",
    65: "Primaria (1°-6°)",
    67: "Primaria (1°-6°)",
    70: "Secundaria (7°-8°)",
    73: "Secundaria (7°-8°)",
    74: "Media HC",
    72: "Media TP",
    310: "Educación de Adultos",
    999: "Otro",
}


def transform(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia, filtra y enriquece el dataset de matrícula:
    - Selecciona columnas relevantes
    - Renombra y normaliza
    - Mapea códigos a etiquetas legibles
    - Elimina nulos críticos
    """
    # Filtrar solo columnas que existan en el CSV
    cols_presentes = {k: v for k, v in COLUMNAS_RELEVANTES.items() if k in df_raw.columns}
    df = df_raw[list(cols_presentes.keys())].copy()
    df = df.rename(columns=cols_presentes)

    # Mapear dependencia y nivel
    if "dependencia_cod" in df.columns:
        df["dependencia"] = df["dependencia_cod"].map(DEPENDENCIA_MAP).fillna("Otro")
    if "nivel_cod" in df.columns:
        df["nivel"] = df["nivel_cod"].map(NIVEL_MAP).fillna("Otro")
    if "genero_cod" in df.columns:
        df["genero"] = df["genero_cod"].map({1: "Masculino", 2: "Femenino"}).fillna("No especificado")

    # Eliminar filas sin RBD o matrícula
    df = df.dropna(subset=["rbd"])

    # Limpiar strings
    for col in ["nombre_escuela", "comuna", "region"]:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()

    df["procesado_en"] = datetime.utcnow().isoformat()

    print(f"[TRANSFORM] {len(df):,} registros limpios")
    return df


def aggregate(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Genera tablas de agregación para análisis."""
    aggs = {}

    # 1. Matrícula total por región
    if "region" in df.columns and "matricula" in df.columns:
        aggs["por_region"] = (
            df.groupby("region")["matricula"]
            .count()
            .reset_index()
            .rename(columns={"matricula": "total_matriculas"})
            .sort_values("total_matriculas", ascending=False)
        )

    # 2. Distribución por dependencia
    if "dependencia" in df.columns:
        aggs["por_dependencia"] = (
            df["dependencia"]
            .value_counts()
            .reset_index()
            .rename(columns={"count": "total"})
        )

    # 3. Distribución por nivel educativo
    if "nivel" in df.columns:
        aggs["por_nivel"] = (
            df["nivel"]
            .value_counts()
            .reset_index()
            .rename(columns={"count": "total"})
        )

    # 4. Distribución por género
    if "genero" in df.columns:
        aggs["por_genero"] = (
            df["genero"]
            .value_counts()
            .reset_index()
            .rename(columns={"count": "total"})
        )

    return aggs


# ─── CARGA ────────────────────────────────────────────────────────────────────

def load(df: pd.DataFrame, aggs: dict, db_path: str) -> None:
    """Carga datos limpios y agregaciones en SQLite."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        df.to_sql("matricula", conn, if_exists="replace", index=False)
        for nombre, tabla in aggs.items():
            tabla.to_sql(f"agg_{nombre}", conn, if_exists="replace", index=False)
    print(f"[LOAD] Base de datos guardada en {db_path}")


# ─── REPORTE Y VISUALIZACIONES ────────────────────────────────────────────────

def report(aggs: dict, db_path: str) -> None:
    """Genera 3 visualizaciones desde las tablas de agregación."""
    os.makedirs("outputs", exist_ok=True)

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle(
        "Matrícula Escolar Chile 2023 — Análisis MINEDUC",
        fontsize=14, fontweight="bold"
    )

    # ── Gráfico 1: Top 10 regiones ──
    if "por_region" in aggs:
        top_reg = aggs["por_region"].head(10)
        sns.barplot(
            data=top_reg, x="total_matriculas", y="region",
            palette="Blues_d", ax=axes[0]
        )
        axes[0].set_title("Top 10 regiones por matrícula")
        axes[0].set_xlabel("Total registros")
        axes[0].set_ylabel("")

    # ── Gráfico 2: Por dependencia ──
    if "por_dependencia" in aggs:
        dep = aggs["por_dependencia"]
        axes[1].pie(
            dep["total"], labels=dep["dependencia"],
            autopct="%1.1f%%",
            colors=["#003f5c", "#2f4b7c", "#665191", "#a05195", "#d45087"],
            startangle=140,
        )
        axes[1].set_title("Distribución por tipo de dependencia")

    # ── Gráfico 3: Por nivel educativo ──
    if "por_nivel" in aggs:
        niv = aggs["por_nivel"].head(8)
        sns.barplot(
            data=niv, x="total", y="nivel",
            palette="Greens_d", ax=axes[2]
        )
        axes[2].set_title("Distribución por nivel educativo")
        axes[2].set_xlabel("Total registros")
        axes[2].set_ylabel("")

    plt.tight_layout()
    plt.savefig("outputs/reporte_mineduc.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("[REPORT] Gráfico guardado en outputs/reporte_mineduc.png")

    # Leer insights desde SQLite y mostrar en consola
    with sqlite3.connect(db_path) as conn:
        df_full = pd.read_sql("SELECT * FROM matricula LIMIT 5", conn)
    print("\n── MUESTRA DE DATOS PROCESADOS ──")
    print(df_full.to_string(index=False))


# ─── PIPELINE PRINCIPAL ───────────────────────────────────────────────────────

def run_pipeline():
    print("=" * 55)
    print("  MINEDUC ETL PIPELINE — Matrícula Escolar Chile 2023")
    print("=" * 55)

    csv_path = extract(DATA_URL, LOCAL_CSV)
    df_raw   = load_csv(csv_path)
    df_clean = transform(df_raw)
    aggs     = aggregate(df_clean)
    load(df_clean, aggs, DB_PATH)
    report(aggs, DB_PATH)

    print("\n✅ Pipeline completado exitosamente.")
    print(f"   Base de datos : {DB_PATH}")
    print(f"   Reporte       : outputs/reporte_mineduc.png")


if __name__ == "__main__":
    run_pipeline()
