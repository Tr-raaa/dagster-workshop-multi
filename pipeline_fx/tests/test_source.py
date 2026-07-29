from unittest.mock import Mock, patch

import pytest
import requests

import source


def test_fetch_latest_rates_returns_parsed_json():
    fake_response = Mock()
    fake_response.json.return_value = {"base": "USD", "rates": {"EUR": 0.9, "GBP": 0.8}}
    fake_response.raise_for_status.return_value = None

    with patch("source.requests.get", return_value=fake_response) as mock_get:
        result = source.fetch_latest_rates(base="USD")

    assert result == {"base": "USD", "rates": {"EUR": 0.9, "GBP": 0.8}}
    mock_get.assert_called_once_with(
        "https://api.frankfurter.app/latest", params={"from": "USD"}, timeout=10
    )


def test_fetch_latest_rates_raises_source_unavailable_on_network_error():
    with patch("source.requests.get", side_effect=requests.ConnectionError("boom")):
        with pytest.raises(source.SourceUnavailableError):
            source.fetch_latest_rates()
