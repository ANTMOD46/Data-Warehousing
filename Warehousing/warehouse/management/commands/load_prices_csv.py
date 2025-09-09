import csv
from datetime import date
from decimal import Decimal
from django.core.management.base import BaseCommand, CommandError
from warehouse.models import Symbol, DateDim, PriceDailyFact

class Command(BaseCommand):
    help = "Load daily prices from CSV (columns: date,open,high,low,close,adj_close,volume,ticker)"

    def add_arguments(self, parser):
        parser.add_argument("--csv", required=True)

    def handle(self, *args, **opts):
        path = opts["csv"]
        created = 0
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                d = date.fromisoformat(row["date"])
                dd = DateDim.objects.get(date=d)
                sym = Symbol.objects.get(ticker=row["ticker"])
                obj, is_new = PriceDailyFact.objects.get_or_create(
                    symbol=sym, date=dd,
                    defaults=dict(
                        open=Decimal(row["open"] or "0"),
                        high=Decimal(row["high"] or "0"),
                        low=Decimal(row["low"] or "0"),
                        close=Decimal(row["close"] or "0"),
                        adj_close=Decimal(row.get("adj_close") or "0"),
                        volume=int(row["volume"] or 0),
                        source="csv",
                    )
                )
                created += int(is_new)
        self.stdout.write(self.style.SUCCESS(f"Inserted {created} rows"))
