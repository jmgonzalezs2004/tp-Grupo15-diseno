from enum import IntEnum

class PaymentFormat(IntEnum):
    ACH = 1
    WIRE = 2
    OTHER = 64
    # ...

    @staticmethod
    def from_str(format: str) -> "PaymentFormat":
        mapper = {
            "ACH": PaymentFormat.ACH,
            "WIRE": PaymentFormat.WIRE
            # ...
        }
        return mapper.get(format.upper(), PaymentFormat.OTHER)
    
class Currency(IntEnum):
    US_DOLLAR = 1
    EURO = 2
    YUAN = 3
    MEXICAN_PESO = 4
    BITCOIN = 5
    # ...

    OTHER = 64

    @staticmethod
    def from_str(format: str) -> "Currency":
        mapper = {
            "US DOLLAR": Currency.US_DOLLAR,
            "EURO": Currency.EURO
            # ...
        }
        return mapper.get(format.upper(), Currency.OTHER)