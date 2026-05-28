from datetime import datetime, UTC, timedelta
import json
import logging
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from common.protocol.common_enums import Currency


class USDConverter:
    def __init__(self):
        # Bitcoin values are not available from API
        self._cache: dict[tuple[str, Currency], float] = {
            ("2022-09-01", Currency.BITCOIN): 19793.1,
            ("2022-09-02", Currency.BITCOIN): 199999.0, # Huh ?
            ("2022-09-03", Currency.BITCOIN): 19831.4,
            ("2022-09-04", Currency.BITCOIN): 19952.7,
            ("2022-09-05", Currency.BITCOIN): 20126.1,
        }
        
    def _fetch_conversion_rate(self, date, currency: Currency):
        currency_code = currency.code
        logging.info(f"Fetch conversion rate for {currency_code} at {date}")
        if currency_code == "":
            raise ValueError(f"Unsupported currency: {currency.label}")

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
            with urlopen(request, timeout=10) as response:
                if response.status != 200:
                    raise ValueError(f"HTTP error: {response.status}")
                data = json.loads(response.read().decode("utf-8"))
        except HTTPError as e:
            raise ValueError(f"HTTP error: {e.code}") from e
        except URLError as e:
            raise ValueError(f"Connection error: {e.reason}") from e

        if not data:
            raise ValueError(f"No conversion rate found for {currency_code} on {date}")
        return data[0]["rate"]
    
    def warmup(self, from_date: str, to_date: str):
        start_date = datetime.strptime(from_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(to_date, "%Y-%m-%d").date()

        if start_date > end_date:
            raise ValueError("from_date must be lower or equal to to_date")

        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            for currency in Currency:
                if currency.code == "" or currency == Currency.US_DOLLAR:
                    continue
                cache_key = (date_str, currency)
                self._cache[cache_key] = self._fetch_conversion_rate(date_str, currency)
            current_date += timedelta(days=1)

    def convert_to_usd(self, timestamp, currency: Currency, amount: float):
        if currency == Currency.US_DOLLAR:
            return amount
        
        date_str = datetime.fromtimestamp(timestamp, tz=UTC).strftime("%Y-%m-%d")
        cache_key = (date_str, currency)
        if cache_key not in self._cache:
            self._cache[cache_key] = self._fetch_conversion_rate(date_str, currency)
        
        conversion_rate = self._cache[cache_key]
        return amount * conversion_rate
