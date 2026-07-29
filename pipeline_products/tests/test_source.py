from unittest.mock import Mock, patch

import pytest
import requests

import source


def test_fetch_products_returns_parsed_json():
    fake_response = Mock()
    fake_response.json.return_value = [{"id": 1, "title": "Widget"}]
    fake_response.raise_for_status.return_value = None

    with patch("source.requests.get", return_value=fake_response) as mock_get:
        result = source.fetch_products()

    assert result == [{"id": 1, "title": "Widget"}]
    mock_get.assert_called_once_with("https://fakestoreapi.com/products", timeout=10)


def test_fetch_carts_returns_parsed_json():
    fake_response = Mock()
    fake_response.json.return_value = [{"id": 100, "userId": 5}]
    fake_response.raise_for_status.return_value = None

    with patch("source.requests.get", return_value=fake_response) as mock_get:
        result = source.fetch_carts()

    assert result == [{"id": 100, "userId": 5}]
    mock_get.assert_called_once_with("https://fakestoreapi.com/carts", timeout=10)


def test_fetch_products_raises_source_unavailable_on_network_error():
    with patch("source.requests.get", side_effect=requests.ConnectionError("boom")):
        with pytest.raises(source.SourceUnavailableError):
            source.fetch_products()
