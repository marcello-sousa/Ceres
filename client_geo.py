# client_geo.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Dict, List
import requests


@dataclass(frozen=True)
class GeoResult:
    name: str
    latitude: float
    longitude: float
    country_code: str
    admin1: Optional[str] = None  # estado
    admin2: Optional[str] = None  # mesorregião/condado (varia)
    timezone: Optional[str] = None
    population: Optional[int] = None


class OpenMeteoClient:
    GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"
    FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

    def __init__(self, timeout_s: float = 15.0):
        self.session = requests.Session()
        self.timeout_s = timeout_s

    # ---------------------------
    # Helpers
    # ---------------------------
    @staticmethod
    def _norm(s: Optional[str]) -> str:
        return (s or "").strip().lower()

    @staticmethod
    def _validate_latlon(lat: float, lon: float) -> None:
        if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
            raise ValueError(
                f"Coordenadas inválidas: lat={lat}, lon={lon}. "
                "O Open-Meteo exige latitude [-90,90] e longitude [-180,180]."
            )

    def _rank_results(
        self,
        results: List[Dict[str, Any]],
        *,
        state: Optional[str],
        county: Optional[str],
        country_code: Optional[str],
    ) -> List[Dict[str, Any]]:
        # 1) filtra por país se veio
        if country_code:
            cc = self._norm(country_code)
            results = [r for r in results if self._norm(r.get("country_code")) == cc] or results

        # 2) filtra por estado (admin1) se veio
        if state:
            st = self._norm(state)
            filtered = [r for r in results if self._norm(r.get("admin1")) == st]
            if filtered:
                results = filtered

        # 3) filtra por admin2/condado se veio (nem sempre existe, mas ajuda quando existe)
        if county:
            ct = self._norm(county)
            filtered = [r for r in results if self._norm(r.get("admin2")) == ct]
            if filtered:
                results = filtered

        # 4) ordena por população desc (quando disponível)
        def pop(r: Dict[str, Any]) -> int:
            p = r.get("population")
            try:
                return int(p) if p is not None else -1
            except Exception:
                return -1

        results.sort(key=pop, reverse=True)
        return results

    # ---------------------------
    # Geocoding (cidade -> lista de candidatos)
    # ---------------------------
    def geocode_candidates(
        self,
        city: str,
        *,
        country_code: str = "BR",
        language: str = "pt",
        count: int = 10,
    ) -> List[GeoResult]:
        params = {
            "name": city,
            "count": count,
            "language": language,
            "format": "json",
            "countryCode": country_code,  # bom default pro Brasil
        }
        resp = self.session.get(self.GEOCODE_URL, params=params, timeout=self.timeout_s)
        resp.raise_for_status()
        data = resp.json()

        raw = data.get("results") or []
        out: List[GeoResult] = []
        for r in raw:
            lat = float(r["latitude"])
            lon = float(r["longitude"])
            # não falha aqui; só ignora inválidos (muito raro)
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                continue
            out.append(
                GeoResult(
                    name=str(r.get("name", city)),
                    latitude=lat,
                    longitude=lon,
                    country_code=str(r.get("country_code", country_code)),
                    admin1=r.get("admin1"),
                    admin2=r.get("admin2"),
                    timezone=r.get("timezone"),
                    population=r.get("population"),
                )
            )

        return out

    # Resolve 1 melhor candidato
    def geocode_city(
        self,
        city: str,
        *,
        state: Optional[str] = None,      # ex: "São Paulo" ou "Rio de Janeiro"
        county: Optional[str] = None,     # opcional
        country_code: str = "BR",
        language: str = "pt",
        count: int = 10,
    ) -> GeoResult:
        params = {
            "name": city,
            "count": count,
            "language": language,
            "format": "json",
            "countryCode": country_code,
        }
        resp = self.session.get(self.GEOCODE_URL, params=params, timeout=self.timeout_s)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results") or []
        if not results:
            raise ValueError(f"Nenhum resultado de geocoding para: {city!r}")

        ranked = self._rank_results(results, state=state, county=county, country_code=country_code)
        chosen = ranked[0]

        lat = float(chosen["latitude"])
        lon = float(chosen["longitude"])
        self._validate_latlon(lat, lon)

        return GeoResult(
            name=str(chosen.get("name", city)),
            latitude=lat,
            longitude=lon,
            country_code=str(chosen.get("country_code", country_code)),
            admin1=chosen.get("admin1"),
            admin2=chosen.get("admin2"),
            timezone=chosen.get("timezone"),
            population=chosen.get("population"),
        )

    # ---------------------------
    # Forecast (lat/lon -> previsão)
    # ---------------------------
    def get_forecast(
        self,
        latitude: float,
        longitude: float,
        *,
        timezone: str = "America/Sao_Paulo",
        forecast_days: int = 7,
        language: str = "pt",
        current: str = "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m,wind_direction_10m",
        hourly: str = "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        daily: str = "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,sunrise,sunset",
    ) -> Dict[str, Any]:
        latitude = float(latitude)
        longitude = float(longitude)
        self._validate_latlon(latitude, longitude)

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "timezone": timezone,
            "forecast_days": forecast_days,
            "language": language,
            "current": current,
            "hourly": hourly,
            "daily": daily,
        }
        resp = self.session.get(self.FORECAST_URL, params=params, timeout=self.timeout_s)
        resp.raise_for_status()
        return resp.json()

    # ---------------------------
    # Modo "auto": cidade OU lat/lon
    # ---------------------------
    def get_forecast_auto(
        self,
        *,
        city: Optional[str] = None,
        state: Optional[str] = None,
        county: Optional[str] = None,
        country_code: str = "BR",
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        timezone: str = "America/Sao_Paulo",
        forecast_days: int = 7,
        language: str = "pt",
    ) -> Dict[str, Any]:
        # Se veio lat/lon, usa direto
        if latitude is not None and longitude is not None:
            data = self.get_forecast(
                latitude, longitude,
                timezone=timezone,
                forecast_days=forecast_days,
                language=language,
            )
            data["_resolved_location"] = {
                "mode": "latlon",
                "latitude": float(latitude),
                "longitude": float(longitude),
                "timezone": timezone,
            }
            return data

        # Senão, exige city
        if not city:
            raise ValueError("Informe (latitude e longitude) OU city.")

        geo = self.geocode_city(
            city,
            state=state,
            county=county,
            country_code=country_code,
            language=language,
            count=10,
        )
        tz = geo.timezone or timezone

        data = self.get_forecast(
            geo.latitude,
            geo.longitude,
            timezone=tz,
            forecast_days=forecast_days,
            language=language,
        )
        data["_resolved_location"] = {
            "mode": "city",
            "name": geo.name,
            "admin1": geo.admin1,
            "admin2": geo.admin2,
            "country_code": geo.country_code,
            "latitude": geo.latitude,
            "longitude": geo.longitude,
            "timezone": tz,
            "population": geo.population,
        }
        return data

