import os
import time
from datetime import date
from decimal import Decimal
import requests

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from warehouse.models import Symbol, PriceDailyFact, DateDim  # ปรับให้ตรงกับโปรเจคคุณ


def ensure_date_dim(d: date):
    """คืน (DateDim) โดยสร้างให้ถ้ายังไม่มี"""
    q = DateDim.objects.filter(date=d)
    if q.exists():
        return q.get()
    dow = d.isoweekday()
    return DateDim.objects.create(
        date=d,
        year=d.year,
        quarter=((d.month - 1) // 3) + 1,
        month=d.month,
        day=d.day,
        day_of_week=dow,
        is_weekend=(dow >= 6),
    )


class Command(BaseCommand):
    help = "Load/Upsert daily OHLCV from Alpha Vantage (supports DAILY & DAILY_ADJUSTED with fallback)."

    def add_arguments(self, parser):
        parser.add_argument("--ticker", required=True, help="เช่น AAPL, PTTEP.BK")
        parser.add_argument("--since", type=str, default=None, help="YYYY-MM-DD (โหลดเฉพาะ >= วันนี้)")
        parser.add_argument("--source", type=str, default="alpha", help="ค่า default=alpha")
        parser.add_argument("--adjusted", action="store_true",
                            help="พยายามใช้ TIME_SERIES_DAILY_ADJUSTED (ถ้า premium เท่านั้น)")
        parser.add_argument("--api-key", dest="api_key", default=None,
                            help="Alpha Vantage API key (ถ้าไม่ส่ง จะอ่านจาก .env/ENV)")

    def handle(self, *args, **opts):
        # ---- อ่านคีย์: CLI > .env > ENV
        api_key = (
            opts.get("api_key")
            or os.getenv("ALPHA_VANTAGE_KEY")
        )
        if not api_key:
            raise CommandError("ยังไม่มี API key: ส่ง --api-key หรือ ตั้ง ALPHA_VANTAGE_KEY ใน .env/ENV")

        ticker = opts["ticker"].upper()
        since = date.fromisoformat(opts["since"]) if opts["since"] else None
        prefer_adjusted = bool(opts["adjusted"])

        # ---- เตรียม Symbol
        symbol, _ = Symbol.objects.get_or_create(ticker=ticker, defaults={"name": ticker})

        # ---- สร้างฟังก์ชันเรียก API + backoff 1 ครั้งกรณีชน rate limit
        def fetch(function_name, attempt=1):
            url = (
                "https://www.alphavantage.co/query"
                f"?function={function_name}&symbol={ticker}&outputsize=full&apikey={api_key}"
            )
            r = requests.get(url, timeout=60)
            try:
                data = r.json()
            except Exception:
                data = {"_raw": r.text}

            # จับข้อความ rate limit
            note = (data.get("Note") or data.get("Information") or "").lower()
            if "frequency" in note or "please visit" in note or "premium" in note or "thank you for using alpha vantage" in note:
                if attempt == 1:
                    # backoff 20s ครั้งเดียว
                    self.stdout.write(self.style.WARNING("ชน rate limit/premium: รอ 20 วินาทีแล้วลองใหม่..."))
                    time.sleep(20)
                    return fetch(function_name, attempt=2)
            return r.status_code, data

        # ---- เลือก endpoint ตามธง --adjusted; ถ้าเจอ premium จะ fallback
        func = "TIME_SERIES_DAILY_ADJUSTED" if prefer_adjusted else "TIME_SERIES_DAILY"
        status, data = fetch(func)

        # ถ้าพยายาม adjusted แล้วเจอ premium → fallback เป็น DAILY
        info_txt = (data.get("Information") or data.get("Note") or "").lower()
        if ("premium" in info_txt or "subscribe" in info_txt) and func == "TIME_SERIES_DAILY_ADJUSTED":
            self.stdout.write(self.style.WARNING(
                "Alpha Vantage แจ้งว่า Adjusted เป็น premium → สลับไปใช้ TIME_SERIES_DAILY อัตโนมัติ"
            ))
            status, data = fetch("TIME_SERIES_DAILY")

        ts_key = "Time Series (Daily)"
        if status != 200 or ts_key not in data:
            raise CommandError(f"ไม่พบ {ts_key}. ข้อความตอบกลับ: {str(data)[:500]}")

        series = data[ts_key]

        # utilities map ค่าทั้ง adjusted/non-adjusted
        def dec(row, *keys):
            for k in keys:
                v = row.get(k)
                if v not in (None, ""):
                    try:
                        return Decimal(v)
                    except Exception:
                        pass
            return None

        def to_int(row, *keys):
            for k in keys:
                v = row.get(k)
                if v not in (None, ""):
                    try:
                        # บางครั้งได้เป็นสตริงทศนิยม -> cast float -> int
                        return int(float(v))
                    except Exception:
                        pass
            return None

        # ---- DQ check เบื้องต้น
        def valid_ohlcv(o, h, l, c, vol):
            nums = [x for x in (o, h, l, c) if x is not None]
            if any(x < 0 for x in nums):
                return False
            if vol is not None and vol < 0:
                return False
            if all(v is not None for v in (h, l)):
                if l > h:
                    return False
            # ถ้ามีครบ o,h,l,c ให้ตรวจช่วงด้วย
            if all(v is not None for v in (o, h, l, c)):
                if h < max(o, c, l) or l > min(o, c, h):
                    return False
            return True

        inserted = updated = 0
        with transaction.atomic():
            for ds, row in series.items():
                d = date.fromisoformat(ds)
                if since and d < since:
                    continue

                dd = ensure_date_dim(d)

                o = dec(row, "1. open")
                h = dec(row, "2. high")
                l = dec(row, "3. low")
                c = dec(row, "4. close")
                adj = dec(row, "5. adjusted close", "4. close")  # ถ้าไม่มี adjusted ใช้ close
                vol = to_int(row, "6. volume", "5. volume")       # DAILY ใช้ "5. volume"

                if not valid_ohlcv(o, h, l, c, vol):
                    # ข้ามแถวที่ไม่ผ่าน DQ
                    self.stdout.write(self.style.WARNING(f"ข้าม {ds}: ไม่ผ่าน DQ (OHLCV ผิดปกติ)"))
                    continue

                defaults = dict(
                    open=o, high=h, low=l, close=c, adj_close=adj,
                    volume=vol, source=opts["source"],
                )

                obj, is_new = PriceDailyFact.objects.update_or_create(
                    symbol=symbol, date=dd, defaults=defaults
                )
                inserted += int(is_new)
                updated += int(not is_new)

        self.stdout.write(self.style.SUCCESS(
            f"Ticker {ticker}: inserted={inserted}, updated={updated}"
        ))
