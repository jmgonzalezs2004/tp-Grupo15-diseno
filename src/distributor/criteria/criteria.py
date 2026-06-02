from datetime import datetime, timedelta

from common.protocol.common_enums import Currency, PaymentFormat
from common.protocol.internal_messages import Transaction


class FilterCriteria:
    def check(self, transaction: Transaction) -> bool:
        raise NotImplementedError()
    
class AmountLowerCriteria:
    def __init__(self, amount: float):
        self.amount = amount

    def check(self, transaction: Transaction) -> bool:
        return transaction.amount < self.amount
    
class CurrencyCriteria:
    def __init__(self, currency: Currency):
        self.currency = currency

    def check(self, transaction: Transaction) -> bool:
        return transaction.currency_id == self.currency

class PaymentFormatCriteria:
    def __init__(self, payment_formats: list[PaymentFormat]):
        self.formats = payment_formats

    def check(self, transaction: Transaction) -> bool:
        return transaction.payment_format_id in self.formats
    
class DateCriteria:
    def __init__(self, date_from: str, date_to: str, inclusive=True):
        dt_date_from = datetime.strptime(date_from, "%Y-%m-%d")
        dt_date_to = datetime.strptime(date_to, "%Y-%m-%d")
        if inclusive:
            dt_date_to = dt_date_to + timedelta(days=1) - timedelta(seconds=1)
        self.timestamp_from = int(dt_date_from.timestamp())
        self.timestamp_to = int(dt_date_to.timestamp())

    def check(self, transaction: Transaction) -> bool:
        return self.timestamp_from <= transaction.timestamp <= self.timestamp_to

class AndCriteria:
    def __init__(self, lhs: FilterCriteria, rhs: FilterCriteria):
        self.lhs = lhs
        self.rhs = rhs

    def check(self, transaction: Transaction) -> bool:
        return self.lhs.check(transaction) and self.rhs.check(transaction)
    

def build_criteria_for_query(query: int):
    match query:
        case 1:
            return AndCriteria(
                CurrencyCriteria(Currency.US_DOLLAR),
                AmountLowerCriteria(50.0)
            )
        case 2:
            return CurrencyCriteria(Currency.US_DOLLAR)
        case 3:
            return AndCriteria(
                CurrencyCriteria(Currency.US_DOLLAR),
                DateCriteria("2022-09-01", "2022-09-15")
            )
        case 4:
            return AndCriteria(
                CurrencyCriteria(Currency.US_DOLLAR),
                DateCriteria("2022-09-01", "2022-09-05"),
            )
        case 5:
            return AndCriteria(
                PaymentFormatCriteria([PaymentFormat.WIRE, PaymentFormat.ACH]),
                DateCriteria("2022-09-01", "2022-09-05"),
            )
        case _:
            raise ValueError(f"Query {query} not supported")
