import requests

BASE_URL = "https://api.frankfurter.app"


class SourceUnavailableError(Exception):
    """Raised when api.frankfurter.app cannot be reached."""


def fetch_latest_rates(base: str = "USD") -> dict:
    try:
        response = requests.get(f"{BASE_URL}/latest", params={"from": base}, timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise SourceUnavailableError(
            "Could not reach api.frankfurter.app — check your internet connection"
        ) from exc
    return response.json()
