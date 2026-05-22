import requests
from datetime import datetime, UTC


class USDConverter:
    def __init__(self):
        self._cache = {}
        self._currency_map = {
            "Euro": "EUR",
            "Shekel": "ILS",
            "US Dollar": "USD",
            "Yuan": "CNY",
            "Swiss Franc": "CHF",
            "Canadian Dollar": "CAD",
            "Brazil Real": "BRL",
            "Mexican Peso": "MXN",
            "Saudi Riyal": "SAR",
            "Ruble": "RUB",
            "Rupee": "INR",
            "Australian Dollar": "AUD",
            "Yen": "JPY",
            "UK Pound": "GBP",
        }

    def _convert_to_usd(self, timestamp, currency, amount):
        if currency == "US Dollar":
            return amount
        
        date = datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%d")
        cache_key = (date, currency)
        if cache_key not in self._cache:
            self._cache[cache_key] = self._fetch_conversion_rate(date, currency)
        
        conversion_rate = self._cache[cache_key]
        return amount * conversion_rate

    def _fetch_conversion_rate(self, date, currency):
        currency_code = self._currency_map.get(currency)
        if not currency_code:
            raise ValueError(f"Unsupported currency: {currency}")
        
        response = requests.get(
            f"https://api.frankfurter.dev/v2/{date}",
            params={
                "base": currency_code,
                "symbols": "USD",
            },
            timeout=10,
        )

        response.raise_for_status()

        return response.json()["rates"]["USD"]
