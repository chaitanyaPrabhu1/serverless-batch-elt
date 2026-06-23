import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "lambda"))

import common  # noqa: E402


# A canned Open-Meteo "current" response for one city.
def fake_api_response(temperature=12.3, humidity=68, code=3):
    return {
        "latitude": 51.5,
        "longitude": -0.12,
        "timezone": "GMT",
        "current_units": {"time": "iso8601", "temperature_2m": "°C"},
        "current": {
            "time": "2024-01-01T13:00",
            "temperature_2m": temperature,
            "apparent_temperature": temperature - 1.5,
            "relative_humidity_2m": humidity,
            "precipitation": 0.0,
            "wind_speed_10m": 4.2,
            "wind_direction_10m": 210,
            "pressure_msl": 1013.2,
            "cloud_cover": 75,
            "weather_code": code,
            "is_day": 1,
        },
    }


@pytest.fixture
def cities():
    return [
        {"city_id": "london", "city_name": "London", "country": "GB",
         "latitude": 51.5074, "longitude": -0.1278, "timezone": "Europe/London"},
        {"city_id": "tokyo", "city_name": "Tokyo", "country": "JP",
         "latitude": 35.6762, "longitude": 139.6503, "timezone": "Asia/Tokyo"},
    ]


@pytest.fixture
def local_backend(tmp_path):
    return common.LocalBackend(str(tmp_path / "lake"))
