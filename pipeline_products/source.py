import requests

BASE_URL = "https://fakestoreapi.com"


class SourceUnavailableError(Exception):
    """Raised when fakestoreapi.com cannot be reached."""


def fetch_products() -> list[dict]:
    try:
        response = requests.get(f"{BASE_URL}/products", timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise SourceUnavailableError(
            "Could not reach fakestoreapi.com/products — check your internet connection"
        ) from exc
    return response.json()


def fetch_carts() -> list[dict]:
    try:
        response = requests.get(f"{BASE_URL}/carts", timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise SourceUnavailableError(
            "Could not reach fakestoreapi.com/carts — check your internet connection"
        ) from exc
    return response.json()
