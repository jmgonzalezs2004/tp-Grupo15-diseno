from enum import IntEnum


class PaymentFormat(IntEnum):
    ACH = 1
    CHEQUE = 2
    CREDIT_CARD = 3
    WIRE = 4
    CASH = 5
    REINVESTMENT = 6
    BITCOIN = 7
    OTHER = 64

    @staticmethod
    def from_str(format: str) -> "PaymentFormat":
        mapper = {
            "ACH": PaymentFormat.ACH,
            "CHEQUE": PaymentFormat.CHEQUE,
            "CREDIT CARD": PaymentFormat.CREDIT_CARD,
            "WIRE": PaymentFormat.WIRE,
            "CASH": PaymentFormat.CASH,
            "REINVESTMENT": PaymentFormat.REINVESTMENT,
            "BITCOIN": PaymentFormat.BITCOIN,
        }
        return mapper.get(format.upper(), PaymentFormat.OTHER)
    
    @staticmethod
    def to_str(format_id: int) -> str:
        mapper = {
            PaymentFormat.ACH.value: "ACH",
            PaymentFormat.CHEQUE.value: "Cheque",
            PaymentFormat.CREDIT_CARD.value: "Credit Card",
            PaymentFormat.WIRE.value: "Wire",
            PaymentFormat.CASH.value: "Cash",
            PaymentFormat.REINVESTMENT.value: "Reinvestment",
            PaymentFormat.BITCOIN.value: "Bitcoin",
        }
        return mapper.get(format_id, "OTHER")
    
class Currency(IntEnum):
    AU_DOLLAR = 1
    BR_REAL = 2
    CA_DOLLAR = 3
    SWISS_FRANC = 4
    YUAN = 5
    EURO = 6
    UK_POUND = 7
    SHEKEL = 8
    RUPEE = 9
    JP_YEN = 10
    MX_PESO = 11
    RUBLE = 12
    SAUDI_RUYAL = 13
    US_DOLLAR = 14
    BITCOIN = 15
    OTHER = 64

    @staticmethod
    def from_str(format: str) -> "Currency":
        mapper = {
            "AUSTRALIAN DOLLAR": Currency.AU_DOLLAR,
            "BRAZIL REAL": Currency.BR_REAL,
            "CANADIAN DOLLAR": Currency.CA_DOLLAR,
            "SWISS FRANC": Currency.SWISS_FRANC,
            "YUAN": Currency.YUAN,
            "EURO": Currency.EURO,
            "UK POUND": Currency.UK_POUND,
            "SHEKEL": Currency.SHEKEL,
            "RUPEE": Currency.RUPEE,
            "YEN": Currency.JP_YEN,
            "MEXICAN PESO": Currency.MX_PESO,
            "RUBLE": Currency.RUBLE,
            "SAUDI RIYAL": Currency.SAUDI_RUYAL,
            "US DOLLAR": Currency.US_DOLLAR,
            "BITCOIN": Currency.BITCOIN,
        }
        return mapper.get(format.upper(), Currency.OTHER)
    
    @property
    def label(self) -> str:
        mapper = {
            Currency.AU_DOLLAR.value: "Australian Dollar",
            Currency.BR_REAL.value: "Brazil Real",
            Currency.CA_DOLLAR.value: "Canadian Dollar",
            Currency.SWISS_FRANC.value: "Swiss Franc",
            Currency.YUAN.value: "Yuan",
            Currency.EURO.value: "Euro",
            Currency.UK_POUND.value: "UK Pound",
            Currency.SHEKEL.value: "Shekel",
            Currency.RUPEE.value: "Rupee",
            Currency.JP_YEN.value: "Yen",
            Currency.MX_PESO.value: "Mexican Peso",
            Currency.RUBLE.value: "Ruble",
            Currency.SAUDI_RUYAL.value: "Saudi Riyal",
            Currency.US_DOLLAR.value: "US Dollar",
            Currency.BITCOIN.value: "Bitcoin",
        }
        return mapper.get(self, "OTHER")
    
    @property
    def code(self) -> str:
        mapper = {
            Currency.AU_DOLLAR.value: "AUD",
            Currency.BR_REAL.value: "BRL",
            Currency.CA_DOLLAR.value: "CAD",
            Currency.SWISS_FRANC.value: "CHF",
            Currency.YUAN.value: "CNY",
            Currency.EURO.value: "EUR",
            Currency.UK_POUND.value: "GBP",
            Currency.SHEKEL.value: "ILS",
            Currency.RUPEE.value: "INR",
            Currency.JP_YEN.value: "JPY",
            Currency.MX_PESO.value: "MXN",
            Currency.RUBLE.value: "RUB",
            Currency.SAUDI_RUYAL.value: "SAR",
            Currency.US_DOLLAR.value: "USD",
        }
        return mapper.get(self, "")
    
    @staticmethod
    def from_code(code: str) -> "Currency":
        mapper = {
            "AUD": Currency.AU_DOLLAR,
            "BRL": Currency.BR_REAL,
            "CAD": Currency.CA_DOLLAR,
            "CHF": Currency.SWISS_FRANC,
            "CNY": Currency.YUAN,
            "EUR": Currency.EURO,
            "GBP": Currency.UK_POUND,
            "ILS": Currency.SHEKEL,
            "INR": Currency.RUPEE,
            "JPY": Currency.JP_YEN,
            "MXN": Currency.MX_PESO,
            "RUB": Currency.RUBLE,
            "SAR": Currency.SAUDI_RUYAL,
            "USD": Currency.US_DOLLAR,
        }
        return mapper.get(code.upper(), Currency.OTHER)
