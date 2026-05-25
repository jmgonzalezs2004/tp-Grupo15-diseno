import requests
from datetime import datetime, UTC


class USDConverter:
    def __init__(self):
        self._conversion_rates = {
        "2022/09/01": {
            "Australian Dollar": 1.4644,
            "Brazil Real": 5.1805,
            "Canadian Dollar": 1.314,
            "Swiss Franc": 0.97999,
            "Yuan": 6.9,
            "Euro": 1.0002,
            "UK Pound": 0.86272,
            "Shekel": 3.3535,
            "Rupee": 79.543,
            "Yen": 139.34,
            "Mexican Peso": 20.189,
            "Ruble": 60.367,
            "Saudi Riyal": 3.75,
            "US Dollar": 1.0,
            "Bitcoin": 19793.1,
        },

        "2022/09/02": {
            "Australian Dollar": 1.4691,
            "Brazil Real": 5.2035,
            "Canadian Dollar": 1.3141,
            "Swiss Franc": 0.98175,
            "Yuan": 6.9035,
            "Euro": 1.0011,
            "UK Pound": 0.86468,
            "Shekel": 3.3755,
            "Rupee": 79.719,
            "Yen": 140.11,
            "Mexican Peso": 20.085,
            "Ruble": 60.427,
            "Saudi Riyal": 3.75,
            "US Dollar": 1.0,
            "Bitcoin": 199999.0,
        },

        "2022/09/03": {
            "Australian Dollar": 1.4691,
            "Brazil Real": 5.2056,
            "Canadian Dollar": 1.3138,
            "Swiss Franc": 0.98207,
            "Yuan": 6.9046,
            "Euro": 1.0013,
            "UK Pound": 0.86478,
            "Shekel": 3.3791,
            "Rupee": 79.75,
            "Yen": 140.17,
            "Mexican Peso": 20.081,
            "Ruble": 60.471,
            "Saudi Riyal": 3.75,
            "US Dollar": 1.0,
            "Bitcoin": 19831.4,
        },

        "2022/09/04": {
            "Australian Dollar": 1.4695,
            "Brazil Real": 5.2082,
            "Canadian Dollar": 1.3139,
            "Swiss Franc": 0.98219,
            "Yuan": 6.9047,
            "Euro": 1.0013,
            "UK Pound": 0.8649,
            "Shekel": 3.3815,
            "Rupee": 79.754,
            "Yen": 140.22,
            "Mexican Peso": 20.084,
            "Ruble": 60.461,
            "Saudi Riyal": 3.75,
            "US Dollar": 1.0,
            "Bitcoin": 19952.7,
        },

        "2022/09/05": {
            "Australian Dollar": 1.4722,
            "Brazil Real": 5.1786,
            "Canadian Dollar": 1.3142,
            "Swiss Franc": 0.98273,
            "Yuan": 6.9216,
            "Euro": 1.0068,
            "UK Pound": 0.86813,
            "Shekel": 3.4006,
            "Rupee": 79.816,
            "Yen": 140.49,
            "Mexican Peso": 20.018,
            "Ruble": 60.737,
            "Saudi Riyal": 3.75,
            "US Dollar": 1.0,
            "Bitcoin": 20126.1,
        },
    }

    def convert_to_usd(self, timestamp, currency, amount):
        if currency == "US Dollar":
            return amount
        
        date_str = datetime.fromtimestamp(timestamp, tz=UTC).strftime("%Y/%m/%d")
        if date_str not in self._conversion_rates:
            return None

        if currency not in self._conversion_rates[date_str]:
            return None

        rate = self._conversion_rates[date_str][currency]
        return amount / rate
