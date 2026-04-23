import requests
import pandas as pd
from datetime import datetime, timezone
import os
import time
import schedule
import base64

API_KEY      = os.environ.get("TOMTOM_API_KEY", "")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO  = "wgonzales1/Incidentes_railway"
OUTPUT_FILE  = "data/incidentes.csv"

ZONAS = {
    "Antofagasta": {"bbox": (-70.45, -23.75, -70.35, -23.55), "region_codigo": 2},
}

COLUMNAS_SCHEMA = {
    "id": "object", "x": "float64", "y": "float64",
    "end_x": "float64", "end_y": "float64",
    "incident_type": "object", "descripcion": "object", "title": "object",
    "severity": "int64", "delay_seconds": "float64",
    "is_road_closed": "bool", "is_traffic_jam": "bool",
    "fecha_inicio": "object", "fecha_fin": "object",
    "last_modified": "float64", "region": "object", "region_codigo": "int64",
    "ciudad": "object", "comuna": "object", "created_at": "object",
    "ingestion_batch_id": "object", "duracion_minutos": "float64",
    "hora_del_dia": "int64", "dia_semana": "object",
    "current_speed_kmh": "float64", "free_flow_speed_kmh": "float64",
    "speed_ratio": "float64", "road_name": "object",
    "from_street": "object", "to_street": "object",
    "length_meters": "float64", "cause_severity": "float64",
    "traffic_road_category": "float64", "cluster_size": "float64",
    "flow_confidence": "float64", "current_travel_time_seconds": "float64",
    "free_flow_travel_time_seconds": "float64", "road_type": "float64",
    "functional_road_class": "object", "flow_segment_coordinates": "object",
    "road_closure_type": "float64", "flow_zoom_level_used": "float64",
    "flow_has_coverage": "object",
}

def consultar_zona(nombre_zona, config):
    min_lon, min_lat, max_lon, max_lat = config["bbox"]
    fields = (
        "{incidents{type,geometry{type,coordinates},"
        "properties{id,iconCategory,magnitudeOfDelay,"
        "events{description,code,iconCategory},"
        "startTime,endTime,from,to,length,delay,"
        "roadNumbers,lastReportTime,"
        "aci{probabilityOfOccurrence,numberOfReports,lastReportTime}}}}"
    )
    url = (
        f"https://api.tomtom.com/traffic/services/5/incidentDetails"
        f"?key={API_KEY}&bbox={min_lon},{min_lat},{max_lon},{max_lat}"
        f"&fields={fields}&language=es-ES&timeValidityFilter=present"
    )
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json().get("incidents", [])
    except Exception as e:
        print(f"Error consultando {nombre_zona}: {e}")
        return []

def parsear_incidentes(incidents, nombre_zona, region_codigo):
    rows = []
    ts = datetime.now(timezone.utc)
    batch_id = f"batch_{int(ts.timestamp())}_0"

    for item in incidents:
        props = item.get("properties", {})
        geometry = item.get("geometry", {})
        coords = geometry.get("coordinates", [])
        geo_type = geometry.get("type", "")

        if geo_type == "Point":
            x, y = coords[0], coords[1]
            end_x = end_y = None
            flow_coords = None
        elif geo_type == "LineString" and coords:
            x, y = coords[0][0], coords[0][1]
            end_x, end_y = coords[-1][0], coords[-1][1]
            flow_coords = "LINESTRING(" + ", ".join(f"{c[0]} {c[1]}" for c in coords) + ")"
        else:
            x = y = end_x = end_y = flow_coords = None

        events = props.get("events", [{}])
        first_event = events[0] if events else {}
        descripcion = first_event.get("description", "Sin descripción")
        road_numbers = props.get("roadNumbers", [])
        road_name = "|".join(road_numbers) if road_numbers else None

        fecha_inicio_str = props.get("startTime")
        fecha_fin_str = props.get("endTime")
        duracion_minutos = None
        if fecha_inicio_str and fecha_fin_str:
            try:
                fi = datetime.fromisoformat(fecha_inicio_str.replace("Z", "+00:00"))
                ff = datetime.fromisoformat(fecha_fin_str.replace("Z", "+00:00"))
                duracion_minutos = round((ff - fi).total_seconds() / 60, 2)
            except:
                pass

        mag = props.get("magnitudeOfDelay", 0)

        rows.append({
            "id": props.get("id"),
            "x": x, "y": y, "end_x": end_x, "end_y": end_y,
            "incident_type": props.get("iconCategory"),
            "descripcion": descripcion,
            "title": f"{props.get('from', '')} → {props.get('to', '')}",
            "severity": mag,
            "delay_seconds": props.get("delay"),
            "is_road_closed": (mag == 4),
            "is_traffic_jam": False,
            "fecha_inicio": fecha_inicio_str,
            "fecha_fin": fecha_fin_str,
            "last_modified": None,
            "region": nombre_zona,
            "region_codigo": region_codigo,
            "ciudad": nombre_zona,
            "comuna": None,
            "created_at": ts.isoformat(),
            "ingestion_batch_id": batch_id,
            "duracion_minutos": duracion_minutos,
            "hora_del_dia": ts.hour,
            "dia_semana": ts.strftime("%A"),
            "current_speed_kmh": None, "free_flow_speed_kmh": None,
            "speed_ratio": None, "road_name": road_name,
            "from_street": props.get("from"),
            "to_street": props.get("to"),
            "length_meters": props.get("length"),
            "cause_severity": None, "traffic_road_category": None,
            "cluster_size": None, "flow_confidence": None,
            "current_travel_time_seconds": None,
            "free_flow_travel_time_seconds": None,
            "road_type": None, "functional_road_class": None,
            "flow_segment_coordinates": flow_coords,
            "road_closure_type": None, "flow_zoom_level_used": None,
            "flow_has_coverage": None,
        })
    return rows

def aplicar_schema(df):
    for col in COLUMNAS_SCHEMA:
        if col not in df.columns:
            df[col] = None
    df = df[list(COLUMNAS_SCHEMA.keys())]
    for col, dtype in COLUMNAS_SCHEMA.items():
        try:
            if dtype == "bool":
                df[col] = df[col].fillna(False).astype(bool)
            elif dtype == "int64":
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")
            elif dtype == "float64":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(str).where(df[col].notna(), other=None)
        except:
            pass
    return df

def exportar_a_github():
    if not GITHUB_TOKEN:
        print("  ⚠️ Sin GITHUB_TOKEN, saltando exportación")
        return
    try:
        with open(OUTPUT_FILE, "rb") as f:
            contenido = base64.b64encode(f.read()).decode()

        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/data/incidentes.csv"

        # Obtener SHA del archivo actual en GitHub
        r = requests.get(url, headers=headers)
        sha = r.json().get("sha", "") if r.status_code == 200 else ""

        payload = {
            "message": f"datos {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}",
            "content": contenido,
            "sha": sha
        }
        r = requests.put(url, headers=headers, json=payload)
        if r.status_code in (200, 201):
            print(f"  📤 CSV exportado a GitHub ({r.status_code})")
        else:
            print(f"  ⚠️ Error exportando a GitHub: {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"  ⚠️ Excepción exportando: {e}")

# Contador de ciclos para exportar cada 6 ciclos (≈2 horas)
ciclo = 0

def recolectar():
    global ciclo
    ciclo += 1
    ts = datetime.now(timezone.utc)
    print(f"[{ts.strftime('%Y-%m-%d %H:%M:%S')}] Ciclo {ciclo} — Consultando API...")

    nuevos = []
    for nombre_zona, config in ZONAS.items():
        incidents = consultar_zona(nombre_zona, config)
        filas = parsear_incidentes(incidents, nombre_zona, config["region_codigo"])
        nuevos.extend(filas)
        print(f"  [{nombre_zona}] {len(filas)} incidentes")

    df_nuevo = pd.DataFrame(nuevos) if nuevos else pd.DataFrame(columns=list(COLUMNAS_SCHEMA.keys()))
    df_nuevo = aplicar_schema(df_nuevo)

    os.makedirs("data", exist_ok=True)
    if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
        df_viejo = pd.read_csv(OUTPUT_FILE)
        df_total = pd.concat([df_viejo, df_nuevo], ignore_index=True)
        df_total = df_total.drop_duplicates(subset=["id", "ingestion_batch_id"])
    else:
        df_total = df_nuevo

    df_total = aplicar_schema(df_total)
    df_total.to_csv(OUTPUT_FILE, index=False)
    print(f"  Total guardado: {len(df_total)} filas")

    # Exportar a GitHub cada 6 ciclos (~2 horas)
    if ciclo % 1 == 0:
        print("  Exportando a GitHub...")
        exportar_a_github()

# Correr inmediatamente
recolectar()

# Programar cada 20 minutos
schedule.every(5).minutes.do(recolectar)

print("Scheduler activo. Recolectando cada 20(5) minutos, exportando a GitHub cada 2(5min) horas...")
while True:
    schedule.run_pending()
    time.sleep(1)
