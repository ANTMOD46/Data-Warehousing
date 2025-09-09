from django.core.management.base import BaseCommand, CommandError
from django.utils.dateparse import parse_date
from warehouse.ch import ch_client
from warehouse.models import PriceDailyFact

class Command(BaseCommand):
    help = "Sync PriceDailyFact (Postgres) into ClickHouse price_daily"

    def add_arguments(self, parser):
        parser.add_argument("--since", type=str, default=None, help="YYYY-MM-DD (เลือกช่วงวันที่)")
        parser.add_argument("--ticker", type=str, default=None, help="จำกัดเฉพาะสัญลักษณ์")

    def handle(self, *args, **opts):
        dfrom = parse_date(opts["since"]) if opts["since"] else None
        ticker = (opts["ticker"] or "").upper().strip()

        qs = (PriceDailyFact.objects
              .select_related("symbol", "date")
              .order_by("date__date"))
        if dfrom:
            qs = qs.filter(date__date__gte=dfrom)
        if ticker:
            qs = qs.filter(symbol__ticker=ticker)

        # แปลงเป็นชุดแถวสำหรับ bulk insert
        rows = []
        for p in qs.iterator(chunk_size=5000):
            rows.append((
                p.date.date,            # Date
                p.symbol.ticker,        # String
                p.open or 0,
                p.high or 0,
                p.low or 0,
                p.close or 0,
                (p.adj_close or p.close or 0),
                int(p.volume or 0),
                p.source or "pg",
                # load_ts ให้ DEFAULT now() ก็ได้; ส่ง None เพื่อใช้ DEFAULT
                None
            ))

        if not rows:
            self.stdout.write("No rows to sync.")
            return

        cl = ch_client()
        # ใช้ ReplacingMergeTree → แทรกซ้ำทับด้วยเวอร์ชันล่าสุด (query ใช้ FINAL หรือ argMax)
        cl.execute("""
            INSERT INTO market.price_daily
            (date, symbol, open, high, low, close, adj_close, volume, source, load_ts)
            VALUES
        """, rows)

        self.stdout.write(self.style.SUCCESS(f"Synced {len(rows)} rows to ClickHouse"))
