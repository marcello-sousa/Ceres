# server.py
from mcp.server.fastmcp import FastMCP
from client_geo import OpenMeteoClient
import json
import os


# Criar um servidor MCP
mcp = FastMCP("Demo")

METEO_DIR = "meteo_data" 

@mcp.tool()
def get_weather_forecast(
    city: str | None = None,
    state: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    forecast_days: int = 7,
) -> dict:
    client = OpenMeteoClient()

    forecast = client.get_forecast_auto(
        city=city,
        state=state,
        county=None,
        country_code="BR",
        latitude=latitude,
        longitude=longitude,
        timezone="America/Sao_Paulo",
        forecast_days=forecast_days,
        language="pt",
    )

    # Decide o "nome da pasta" de forma segura:
    # - se veio city, usa city
    # - se não veio (lat/lon), cria pasta por coordenadas
    if city:
        folder_name = city.lower().strip().replace(" ", "_")
    else:
        folder_name = f"lat_{latitude}_lon_{longitude}".replace(".", "_").replace("-", "m")

    # Create directory for this city
    path = os.path.join(METEO_DIR, folder_name)
    os.makedirs(path, exist_ok=True)

    file_path = os.path.join(path, "city.json")

    # Try to load existing meteo info
    try:
        with open(file_path, "r", encoding="utf-8") as json_file:
            meteo_info = json.load(json_file)
    except (FileNotFoundError, json.JSONDecodeError):
        meteo_info = {}

    # Store/update:
    # - se você quiser salvar "o último forecast" sobrescrevendo:
    meteo_info["latest"] = forecast

    # - opcional: manter histórico (por timestamp do current.time)
    ts = (forecast.get("current") or {}).get("time") or "unknown_time"
    meteo_info.setdefault("history", {})
    meteo_info["history"][ts] = forecast

    # Save back to disk
    with open(file_path, "w", encoding="utf-8") as json_file:
        json.dump(meteo_info, json_file, ensure_ascii=False, indent=2)

    return forecast




if __name__ == "__main__":
    mcp.run(transport="stdio")