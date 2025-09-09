from datetime import date, timedelta
from django.core.management.base import BaseCommand
from warehouse.models import DateDim

class Command(BaseCommand):
    help = "Preload dim_date (รายปี)"

    def add_arguments(self, parser):
        parser.add_argument("--start", type=str, required=True)  # YYYY-MM-DD
        parser.add_argument("--end", type=str, required=True)

    def handle(self, *args, **opts):
        start = date.fromisoformat(opts["start"])
        end = date.fromisoformat(opts["end"])
        cur = start
        created = 0
        while cur <= end:
            dow = cur.isoweekday()  # Mon=1
            obj, is_new = DateDim.objects.get_or_create(
                date=cur,
                defaults=dict(
                    year=cur.year,
                    quarter=((cur.month - 1)//3) + 1,
                    month=cur.month,
                    day=cur.day,
                    day_of_week=dow,
                    is_weekend=(dow >= 6),
                )
            )
            created += int(is_new)
            cur += timedelta(days=1)
        self.stdout.write(self.style.SUCCESS(f"Created {created} dates"))
