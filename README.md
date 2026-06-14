# 🏫 MINEDUC ETL Pipeline — Matrícula Escolar Chile

> **Pipeline ETL** que procesa datos abiertos del Ministerio de Educación de Chile para analizar la distribución de matrícula escolar por región, dependencia y nivel educativo.

---

## 📌 Descripción / Description

**ES:** Pipeline de datos que descarga, limpia y transforma el dataset público de matrícula escolar del MINEDUC (2023). Genera una base de datos SQLite con tablas de agregación y visualizaciones automáticas con insights sobre el sistema educativo chileno.

**EN:** Data pipeline that downloads, cleans, and transforms Chile's Ministry of Education public enrollment dataset (2023). Produces a SQLite database with aggregation tables and automated visualizations with insights about the Chilean educational system.

---

## 🏗️ Arquitectura del pipeline

```
datos.mineduc.cl (CSV público)
         │
         ▼
[EXTRACT]   urllib → descarga + caché local
         │
         ▼
[TRANSFORM] pandas → limpieza, mapeo de códigos, normalización
         │
         ▼
[LOAD]      SQLite → tabla `matricula` + 4 tablas de agregación
         │
         ▼
[REPORT]    matplotlib/seaborn → 3 visualizaciones en PNG
```

---

## 🛠️ Stack técnico / Tech stack

| Capa | Tecnología |
|------|-----------|
| Extracción | `urllib` + datos abiertos MINEDUC |
| Transformación | `pandas` |
| Almacenamiento | `SQLite` |
| Visualización | `matplotlib`, `seaborn` |
| Lenguaje | Python 3.10+ |
| Fuente de datos | [datosabiertos.mineduc.cl](https://datosabiertos.mineduc.cl) |

---

## ⚙️ Instalación y uso

```bash
# 1. Clonar el repositorio
git clone https://github.com/arturourg/mineduc-etl-pipeline.git
cd mineduc-etl-pipeline

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Ejecutar el pipeline
python src/pipeline.py
```

> El dataset (~60MB) se descarga automáticamente la primera vez y se guarda en `data/raw/` para ejecuciones siguientes.

---

## 📊 Insights generados / Generated insights

El pipeline produce 3 visualizaciones automáticas:

**1. Top 10 regiones por matrícula** — identifica las regiones con mayor número de registros.

**2. Distribución por tipo de dependencia** — compara el peso del sector municipal, subvencionado y privado.

**3. Distribución por nivel educativo** — muestra qué niveles concentran más matrícula (parvularia, básica, media, adultos).

---

## 📁 Estructura del proyecto

```
mineduc-etl-pipeline/
├── src/
│   └── pipeline.py          # Pipeline ETL completo
├── data/
│   ├── raw/                 # CSV descargado (generado al correr)
│   └── processed/           # (reservado para versiones futuras)
├── outputs/                 # DB SQLite + PNG (generado al correr)
├── requirements.txt
└── README.md
```

---

## 📂 Tablas en SQLite

| Tabla | Descripción |
|-------|-------------|
| `matricula` | Dataset completo limpio |
| `agg_por_region` | Matrícula agregada por región |
| `agg_por_dependencia` | Distribución por tipo de administración |
| `agg_por_nivel` | Distribución por nivel educativo |
| `agg_por_genero` | Distribución por género |

---

## 🗺️ Fuente de datos

**Ministerio de Educación de Chile — Datos Abiertos**  
Dataset: *Matrícula por Estudiante 2023*  
URL: https://datosabiertos.mineduc.cl  
Licencia: Datos públicos, uso libre con atribución.

---

## 👤 Autor

**Arturo Urra Galdames** — [GitHub](https://github.com/arturourg) · [LinkedIn](https://linkedin.com/in/arturourragaldames)
