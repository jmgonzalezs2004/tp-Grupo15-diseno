from datetime import datetime, UTC, timedelta
import json
import logging
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class USDConverter:
    def __init__(self):
        self._cache: dict[str, dict[str, float]] = {
            "2022-09-01": { "BTC": 19793.1 },
            "2022-09-02": { "BTC": 199999.0 }, # Huh ?
            "2022-09-03": { "BTC": 19831.4 },
            "2022-09-04": { "BTC": 19952.7 },
            "2022-09-05": { "BTC": 20126.1 },
        }
        self._currency_to_code = {
            "Australian Dollar": "AUD",
            "Brazil Real": "BRL",
            "Canadian Dollar": "CAD",
            "Swiss Franc": "CHF",
            "Yuan": "CNY",
            "Euro": "EUR",
            "UK Pound": "GBP",
            "Shekel": "ILS",
            "Rupee": "INR",
            "Yen": "JPY",
            "Mexican Peso": "MXN",
            "Ruble": "RUB",
            "Saudi Riyal": "SAR",
            "US Dollar": "USD",
            "Bitcoin": "BTC",
        }
        self._warmup("2022-09-01", "2022-09-05")

    def _fetch_conversion_rate(self, date, currency_code: str) -> float:
        """
        Fetch given conversion rate for a given date by making an API request.
        """

        if currency_code == "":
            raise ValueError(f"Unsupported currency: {currency_code}")

        params = urlencode({
            "date": date,
            "base": currency_code,
            "quotes": "USD",
        })

        url = f"https://api.frankfurter.dev/v2/rates?{params}"
        request = Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        })
        try:
            with urlopen(request, timeout=30) as response:
                if response.status != 200:
                    raise ValueError(f"HTTP error: {response.status}")
                data = json.loads(response.read().decode("utf-8"))
        except HTTPError as e:
            raise ValueError(f"HTTP error: {e.code}") from e
        except URLError as e:
            raise ValueError(f"Connection error: {e.reason}") from e

        if not data:
            raise ValueError(f"No conversion rate found for {currency_code} on {date}")

        return 1 / data[0]["rate"]
    
    def _fetch_multiple_conversion_rates(self, date) -> dict[str, float]:
        """
        Fetch given conversion rates for a given date by making a single API request.
        """

        currency_codes = ",".join(self._currency_to_code.values())

        params = urlencode({
            "date": date,
            "base": "USD",
            "quotes": currency_codes,
        })

        url = f"https://api.frankfurter.dev/v2/rates?{params}"
        request = Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        })
        try:
            with urlopen(request, timeout=30) as response:
                if response.status != 200:
                    raise ValueError(f"HTTP error: {response.status}")
                data = json.loads(response.read().decode("utf-8"))
        except HTTPError as e:
            raise ValueError(f"HTTP error: {e.code}") from e
        except URLError as e:
            raise ValueError(f"Connection error: {e.reason}") from e

        if not data:
            raise ValueError(f"No conversion rates found for {currency_codes} on {date}")

        conv_rates = {}
        for rate_data in data:
            quote = rate_data["quote"]
            rate = rate_data["rate"]
            if rate == 0:
                continue
            conv_rates[quote] = 1 / rate

        return conv_rates

    def _warmup(self, from_date: str, to_date: str):
        """
        Fetch conversion rates for all supported currencies for a given date range and cache them.
        """

        start_date = datetime.strptime(from_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(to_date, "%Y-%m-%d").date()

        if start_date > end_date:
            raise ValueError("from_date must be lower or equal to to_date")

        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            conv_rates = self._fetch_multiple_conversion_rates(date_str)
            self._cache[date_str] = { **self._cache.get(date_str, {}), **conv_rates }
            current_date += timedelta(days=1)

    def convert_to_usd(self, timestamp, currency: str, amount: float):
        if currency == "US Dollar":
            return amount
        
        currency_code = self._currency_to_code.get(currency, "")
        date_str = datetime.fromtimestamp(timestamp, tz=UTC).strftime("%Y-%m-%d")
        if date_str not in self._cache:
            self._cache[date_str] = {}
        if currency_code not in self._cache[date_str]:
            try: 
                self._cache[date_str][currency_code] = self._fetch_conversion_rate(date_str, currency_code)
            except ValueError as e:
                logging.error(f"Error fetching conversion rate for {currency} on {date_str}: {e}")
                return None

        conversion_rate = self._cache[date_str][currency_code]
        return amount * conversion_rate
