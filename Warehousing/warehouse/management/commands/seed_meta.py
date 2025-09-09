from django.core.management.base import BaseCommand
from warehouse.models import Exchange, Sector, Symbol

class Command(BaseCommand):
    help = "Seed exchange/sector/symbol examples"

    def handle(self, *args, **opts):
        nasdaq, _ = Exchange.objects.get_or_create(code="NASDAQ", defaults={"name": "NASDAQ"})
        setth, _ = Exchange.objects.get_or_create(code="SET", defaults={"name": "Stock Exchange of Thailand"})

        tech, _ = Sector.objects.get_or_create(code="TECH", defaults={"name": "Technology"})
        energy, _ = Sector.objects.get_or_create(code="ENERGY", defaults={"name": "Energy"})

        Symbol.objects.get_or_create(ticker="AAPL", exchange=nasdaq, defaults={
            "name": "Apple Inc.", "sector": tech, "currency": "USD"
        })
        Symbol.objects.get_or_create(ticker="PTTEP.BK", exchange=setth, defaults={
            "name": "PTT Exploration and Production", "sector": energy, "currency": "THB"
        })

        self.stdout.write(self.style.SUCCESS("Seeded meta data"))
