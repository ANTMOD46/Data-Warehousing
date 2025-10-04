import json
from multiprocessing.dummy import connection
from django.shortcuts import render
import requests
import os
from django.http import Http404, HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from warehouse.models import EconDailyFact, Symbol, PriceDailyFact, DateDim, Exchange, Sector,EconomicIndicator
from datetime import date
from decimal import Decimal
import time
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import io
import matplotlib
import matplotlib.pyplot as plt
import subprocess, sys
import time

API_KEY = os.getenv('T39H2CDWWA09NDT6')  # API Key ของคุณจาก Alpha Vantage
ALPHAVANTAGE_API_KEY = "<T39H2CDWWA09NDT6>"



def home(request):
    """หน้าแรกอธิบายการใช้งานคร่าว ๆ และปุ่มลัดไปฟังก์ชันหลัก"""
    return render(request, "home.html")


def build_api_symbol(ticker: str, exchange_code: str | None) -> str:
    t = ticker.upper()
    ex = (exchange_code or "").upper()

    # ตัวอย่างคร่าว ๆ (ปรับตาม exchange จริงในระบบคุณ)
    if ex in {"BK", "SET", "TH"}:
        return f"{t}.BK"              # ไทย
    if ex in {"BSE", "BO"}:
        return f"{t}.BSE"             # อินเดีย (BSE) บาง provider ใช้ .BO
    if ex in {"NSE", "NS"}:
        return f"{t}.NS"              # อินเดีย (NSE)
    # ดั้งเดิม: ถ้าไม่รู้จัก exchange ก็คืน ticker ตรงๆ
    return t

def fetch_alpha_daily_adjusted(api_symbol: str):
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": api_symbol,
        "apikey": ALPHAVANTAGE_API_KEY,
        "outputsize": "full",      # สำคัญ!
        "datatype": "json",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    series = data.get("Time Series (Daily)") or {}
    # แปลงเป็น list เรียงเก่า->ใหม่
    rows = []
    for dstr, vals in sorted(series.items()):
        rows.append({
            "date": dt.date.fromisoformat(dstr),
            "open": float(vals["1. open"]),
            "high": float(vals["2. high"]),
            "low": float(vals["3. low"]),
            "close": float(vals["4. close"]),
            "adj_close": float(vals.get("5. adjusted close", vals["4. close"])),
            "volume": int(vals["6. volume"]),
        })
    return rows

def load_into_pg(ticker: str, exchange_code: str | None = None, source="alpha"):
    api_symbol = build_api_symbol(ticker, exchange_code)

    rows = fetch_alpha_daily_adjusted(api_symbol)
    if not rows:
        raise RuntimeError(f"No data from API for {api_symbol}")

    # 2) freshness guard: ข้อมูลล่าสุดต้องไม่เก่ากว่า 60 วัน
    max_date = rows[-1]["date"]
    if max_date < (dt.date.today() - dt.timedelta(days=60)):
        raise RuntimeError(f"Stale feed: latest={max_date} for {api_symbol}")

    with connection.cursor() as cur:
        # หา symbol_id, date_id mapping
        cur.execute("SELECT id FROM dim_symbol WHERE lower(ticker)=lower(%s)", [ticker])
        res = cur.fetchone()
        if not res:
            raise RuntimeError(f"Ticker {ticker} not found in dim_symbol")
        symbol_id = res[0]

        # (ทางเลือก) ลบของเก่าก่อน สำหรับ refresh ชัด ๆ
        cur.execute("DELETE FROM fact_price_daily WHERE symbol_id=%s", [symbol_id])

        # insert เป็น batch
        for r in rows:
            cur.execute("""
                INSERT INTO fact_price_daily
                (symbol_id, date_id, open, high, low, close, adj_close, volume, source, load_ts)
                VALUES (
                  %s,
                  (SELECT id FROM dim_date WHERE date=%s),
                  %s,%s,%s,%s,%s,%s,%s, now()
                )
                ON CONFLICT (symbol_id, date_id) DO UPDATE SET
                  open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                  close=EXCLUDED.close, adj_close=EXCLUDED.adj_close,
                  volume=EXCLUDED.volume, source=EXCLUDED.source,
                  load_ts=EXCLUDED.load_ts
            """, [
                symbol_id, r["date"],
                r["open"], r["high"], r["low"], r["close"], r["adj_close"], r["volume"],
                source
            ])

            
# ---------- Helpers ----------
def ensure_date_dim(d: date) -> DateDim:
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
        is_weekend=(dow >= 6)
    )

def ensure_exchange(exchange_code: str) -> Exchange:
    obj, _ = Exchange.objects.get_or_create(code=exchange_code, defaults={"name": exchange_code})
    return obj

def ensure_sector(sector_code: str) -> Sector:
    obj, _ = Sector.objects.get_or_create(code=sector_code, defaults={"name": sector_code})
    return obj

def ensure_indicator(code: str, name: str = "", unit: str = "") -> EconomicIndicator:
    obj, created = EconomicIndicator.objects.get_or_create(
        code=code, defaults={"name": name or code, "unit": unit or ""}
    )
    # อัปเดตชื่อ/หน่วยถ้ามีข้อมูลใหม่
    changed = False
    if name and obj.name != name:
        obj.name = name; changed = True
    if unit and obj.unit != unit:
        obj.unit = unit; changed = True
    if changed:
        obj.save(update_fields=["name", "unit"])
    return obj

def backoff_fetch(url: str, tries: int = 2):
    """เรียก API; ถ้าเจอ Note/Information (rate limit) ให้หน่วงแล้วลองใหม่ 1 ครั้ง"""
    for i in range(tries):
        r = requests.get(url, timeout=60)
        try:
            data = r.json()
        except Exception:
            data = {"_raw": r.text}
        note = (data.get("Note") or data.get("Information") or "").lower()
        if ("thank you for using alpha vantage" in note) or ("frequency" in note):
            if i + 1 < tries:
                time.sleep(20)
                continue
        return r.status_code, data
    return r.status_code, data



def get_stock_data(symbol_ticker):
    """ดึงข้อมูลจาก Alpha Vantage API"""
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol_ticker}&apikey={API_KEY}&outputsize=full'
    
    try:
        response = requests.get(url)
        data = response.json()

        # ตรวจสอบว่า API limit ถูกใช้งานหรือไม่
        if "Note" in data:
            # ถ้า API limit ถูกใช้งาน ให้รอ 1 นาที
            time.sleep(60)  # หน่วงเวลา 1 นาที
            return get_stock_data(symbol_ticker)  # ลองดึงใหม่หลังจากหน่วงเวลา

        # ตรวจสอบข้อมูลที่ได้จาก API
        if 'Time Series (Daily)' not in data:
            return None  # ถ้าไม่พบข้อมูลให้ return None

        return data['Time Series (Daily)']
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None


@csrf_exempt
def load_stock_data(request, symbol_ticker):
    if request.method == "GET":
        symbol = symbol_ticker.upper()
        time_series = get_stock_data(symbol)
        if not time_series:
            return JsonResponse({"error": "Data not found or API limit reached."}, status=400)

        exchange = ensure_exchange("NASDAQ")
        sector = ensure_sector("TECHNOLOGY")
        symbol_obj, created = Symbol.objects.get_or_create(
            ticker=symbol,
            defaults={
                "name": symbol,
                "exchange": exchange,
                "sector": sector,
                "currency": "USD",
            }
        )

        inserted = 0
        updated = 0
        for date_str, stats in time_series.items():
            stock_date = date.fromisoformat(date_str)
            date_obj = ensure_date_dim(stock_date)

            open_price = Decimal(stats["1. open"])
            high_price = Decimal(stats["2. high"])
            low_price = Decimal(stats["3. low"])
            close_price = Decimal(stats["4. close"])
            adj_close = Decimal(stats.get("5. adjusted close", close_price))
            volume = int(stats.get("6. volume") or stats.get("5. volume", 0))

            price_data, created = PriceDailyFact.objects.update_or_create(
                symbol=symbol_obj,
                date=date_obj,
                defaults={
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price,
                    'adj_close': adj_close,
                    'volume': volume,
                    'source': "alpha_vantage",
                }
            )

            if created:
                inserted += 1
            else:
                updated += 1

        # 🔽 ตรงนี้คือจุดเพิ่ม: trigger sync ไป ClickHouse
        try:
            subprocess.run(
                ["wsl.exe", "bash", "-lc", f"/home/exit/sync_symbol.sh {symbol}"],
                check=True, capture_output=True, text=True
            )
        except subprocess.CalledProcessError as e:
            print("CH sync failed:", e.stdout, e.stderr, file=sys.stderr)

        return JsonResponse({
            "message": f"Data for {symbol} processed. Inserted: {inserted}, Updated: {updated}"
        }, status=200)
    

def get_economic_data():
    """ดึงข้อมูลหลายตัวชี้วัดเศรษฐกิจพร้อมกันจาก Alpha Vantage"""
    # สร้าง URLs สำหรับแต่ละฟังก์ชัน
    urls = {
        "CPI": f"https://www.alphavantage.co/query?function=CPI&apikey={API_KEY}",
        "REAL_GDP": f"https://www.alphavantage.co/query?function=REAL_GDP&apikey={API_KEY}",
        "UNEMPLOYMENT": f"https://www.alphavantage.co/query?function=UNEMPLOYMENT&apikey={API_KEY}",
    }

    data = {}
    # เรียก API ทั้ง 3 ตัว
    for key, url in urls.items():
        try:
            response = requests.get(url)
            response_data = response.json()

            if 'Note' in response_data or 'Information' in response_data:
                data[key] = {"error": "API limit reached or issue with the data."}
            else:
                data[key] = response_data
        except Exception as e:
            data[key] = {"error": str(e)}

    return data

@csrf_exempt
def get_economic_indicators(request):
    """API สำหรับดึงข้อมูลเศรษฐกิจทั้งหมดพร้อมกัน"""
    if request.method == "GET":
        # ดึงข้อมูลทั้งหมด
        economic_data = get_economic_data()

        return JsonResponse(economic_data, status=200)

# ---------- Economic Indicators ----------
def parse_econ_payload(payload: dict):
    name = payload.get("name") or payload.get("Name") or ""
    unit = payload.get("unit") or payload.get("Unit") or ""
    rows = []
    items = payload.get("data") or payload.get("Data") or payload.get("Historical Data")
    if isinstance(items, list):
        for row in items:
            ds = row.get("date") or row.get("Date")
            val = row.get("value") or row.get("Value")
            if not ds:
                continue
            try:
                d = date.fromisoformat(ds)
            except Exception:
                continue
            try:
                v = Decimal(str(val)) if val not in (None, "", "None") else None
            except Exception:
                v = None
            rows.append((d, v))
    return name, unit, rows

@csrf_exempt
def load_economic_indicators(request):
    """
    ดึง + บันทึกตัวชี้วัดเศรษฐกิจจาก Alpha Vantage → Postgres
    แล้ว 'ซิงก์ไป ClickHouse ทันที' สำหรับแต่ละ indicator (เรียก WSL: /home/exit/sync_econ.sh <CODE>)

    พารามิเตอร์:
      - ?codes=CPI,REAL_GDP,UNEMPLOYMENT (คั่นด้วย comma)
      - หรือใช้ซ้ำหลายตัว: ?code=CPI&code=REAL_GDP
      - ?sync=0  เพื่อไม่ trigger sync ทันที (ให้ cron จัดการเอง)

    ค่าดีฟอลต์เมื่อไม่ส่ง codes: ["CPI", "REAL_GDP", "UNEMPLOYMENT"]
    """
    if request.method != "GET":
        return JsonResponse({"error": "GET only"}, status=405)

    # ----- อ่านพารามิเตอร์ -----
    default_codes = ["CPI", "REAL_GDP", "UNEMPLOYMENT"]

    # รองรับทั้ง ?codes=... และ ?code=... ที่ซ้ำหลายตัว
    codes_param = request.GET.get("codes")
    codes_list  = request.GET.getlist("code")

    codes = []
    if codes_param:
        codes.extend([c.strip().upper() for c in codes_param.split(",") if c.strip()])
    if codes_list:
        codes.extend([c.strip().upper() for c in codes_list if c.strip()])

    if not codes:
        codes = default_codes

    # กันซ้ำและคงลำดับเดิม
    seen = set()
    uniq_codes = []
    for c in codes:
        if c not in seen:
            seen.add(c)
            uniq_codes.append(c)

    # ควบคุมว่าจะ sync ทันทีไหม (ดีฟอลต์: sync)
    do_sync = request.GET.get("sync", "1") != "0"

    result = {
        "inserted": 0,
        "updated": 0,
        "by_indicator": {}
    }

    for code in uniq_codes:
        url = f"https://www.alphavantage.co/query?function={code}&apikey={API_KEY}"
        status, payload = backoff_fetch(url)

        # ถ้า key หาย/ติดลิมิต/ต้อง premium → ข้ามตัวนี้ไป
        if status != 200:
            result["by_indicator"][code] = {"error": f"http {status}"}
            continue

        name, unit, rows = parse_econ_payload(payload)
        if not rows:
            note = payload.get("Note") or payload.get("Information") or payload.get("Error Message") or str(payload)[:300]
            result["by_indicator"][code] = {"error": note}
            continue

        indicator = ensure_indicator(code, name=name, unit=unit)

        ins = 0
        upd = 0
        for d, v in rows:
            # d คือ datetime.date (หรือ string 'YYYY-MM-DD'), v คือ Decimal/float
            the_date = d if isinstance(d, date) else date.fromisoformat(str(d))
            dd = ensure_date_dim(the_date)
            obj, created = EconDailyFact.objects.update_or_create(
                indicator=indicator,
                date=dd,
                defaults={
                    "value": Decimal(str(v)),
                    "source": "alpha"
                }
            )
            if created:
                ins += 1
            else:
                upd += 1

        result["inserted"] += ins
        result["updated"] += upd
        result["by_indicator"][code] = {
            "name": indicator.name,
            "unit": indicator.unit,
            "rows_in_payload": len(rows),
            "inserted": ins,
            "updated": upd,
        }

        # ----- Trigger sync ไป ClickHouse ทันทีต่อ indicator -----
        if do_sync:
            try:
                proc = subprocess.run(
                    ["wsl.exe", "bash", "-lc", f"/home/exit/sync_econ.sh {code}"],
                    check=True, capture_output=True, text=True
                )
                result["by_indicator"][code]["ch_sync"] = "ok"
                # (เลือกได้) เก็บ stdout สั้นๆ ไว้ดูดีบัก
                if proc.stdout:
                    result["by_indicator"][code]["ch_sync_out"] = proc.stdout.strip().splitlines()[-1][:200]
            except subprocess.CalledProcessError as e:
                result["by_indicator"][code]["ch_sync"] = "failed"
                result["by_indicator"][code]["ch_sync_err"] = (e.stderr or e.stdout or "").strip()[:400]
                
                if code != uniq_codes[-1]:
                    time.sleep(15) 


    return JsonResponse(result, status=200)


# from django.shortcuts import render
# from .models import EconDailyFact

# def economic_indicators_view(request):
#     # ดึงข้อมูลเศรษฐกิจจากฐานข้อมูล (ตัวอย่างข้อมูล CPI)
#     data = EconDailyFact.objects.all()  # หรือทำ query ที่ต้องการ

#     # ส่งข้อมูลไปยัง template
#     return render(request, 'economic_indicators.html', {'data': data})
from .models import EconDailyFact, EconomicIndicator
def economic_indicators_view(request):
    
    # 1. ดึงข้อมูล CPI
    cpi_indicator = EconomicIndicator.objects.get(code='CPI') # ดึง Indicator object ของ CPI
    cpi_data = EconDailyFact.objects.filter(indicator=cpi_indicator).order_by('-date')

    # 2. ดึงข้อมูล GDP
    gdp_indicator = EconomicIndicator.objects.get(code='REAL_GDP')
    gdp_data = EconDailyFact.objects.filter(indicator=gdp_indicator).order_by('-date')
    
    # 3. ดึงข้อมูล UNEMPLOYMENT
    unemp_indicator = EconomicIndicator.objects.get(code='UNEMPLOYMENT')
    unemp_data = EconDailyFact.objects.filter(indicator=unemp_indicator).order_by('-date')

    context = {
        'cpi_data': cpi_data,
        'gdp_data': gdp_data,
        'unemp_data': unemp_data,
    }
    
    return render(request, 'economic_indicators.html', context)

# ---------- ClickHouse กล้า 
#clickhouse

# from django.http import JsonResponse
# from django.db import connections


# def stock_chart_page(request, ticker: str):
#     # 1) ดึงรายชื่อหุ้นสำหรับ dropdown แบบไม่ซ้ำ
#     with connections["clickhouse"].cursor() as cur:
#         cur.execute("""
#             SELECT DISTINCT s.ticker
#             FROM market.dim_symbol AS s
#             /* ถ้าต้องการเฉพาะตัวที่มีราคาจริง ให้ใช้ INNER JOIN fact แทน และยังคง DISTINCT */
#             -- INNER JOIN market.fact_price_daily f ON f.symbol_id = s.id
#             WHERE s.is_active = 1
#             ORDER BY s.ticker
#         """)
#         symbol_rows = cur.fetchall()
#     symbols = [r[0] for r in symbol_rows]

#     # หาก ticker ที่ขอมายังไม่มีในรายการ ให้โยน 404 (กันสะกดผิด)
#     if ticker not in symbols:
#         # ถ้าต้องการรองรับก็ข้ามเงื่อนไขนี้ได้
#         pass

#     # 2) ดึงข้อมูล “วันละ 1 แถว” และเอาเฉพาะ 30 วันล่าสุด
#     sql = """
#       SELECT
#         d.date,
#         argMax(f.open,      f.load_ts)  AS open,
#         argMax(f.high,      f.load_ts)  AS high,
#         argMax(f.low,       f.load_ts)  AS low,
#         argMax(f.close,     f.load_ts)  AS close,
#         argMax(f.adj_close, f.load_ts)  AS adj_close,
#         argMax(f.volume,    f.load_ts)  AS volume
#       FROM market.fact_price_daily f
#       JOIN market.dim_date  d ON d.id = f.date_id
#       JOIN market.dim_symbol s ON s.id = f.symbol_id
#       WHERE s.ticker = %(ticker)s
#       GROUP BY d.date
#       ORDER BY d.date DESC
#       LIMIT 30
#     """
#     with connections["clickhouse"].cursor() as cur:
#         cur.execute(sql, {"ticker": ticker})
#         rows = cur.fetchall()

#     if not rows:
#         raise Http404(f"No data for ticker={ticker}")

#     # เรียงจากเก่า→ใหม่เพื่อให้กราฟอ่านง่าย
#     rows.reverse()

#     labels   = [r[0].isoformat() for r in rows]
#     opens    = [float(r[1]) if r[1] is not None else None for r in rows]
#     highs    = [float(r[2]) if r[2] is not None else None for r in rows]
#     lows     = [float(r[3]) if r[3] is not None else None for r in rows]
#     closes   = [float(r[4]) if r[4] is not None else None for r in rows]
#     adjclose = [float(r[5]) if r[5] is not None else None for r in rows]
#     volumes  = [int(r[6])   if r[6] is not None else None for r in rows]

#     context = {
#         "ticker": ticker,
#         "symbols": symbols,  # ส่งไปใช้กับ dropdown
#         "labels_json":   json.dumps(labels),
#         "opens_json":    json.dumps(opens),
#         "highs_json":    json.dumps(highs),
#         "lows_json":     json.dumps(lows),
#         "closes_json":   json.dumps(closes),
#         "adjclose_json": json.dumps(adjclose),
#         "volumes_json":  json.dumps(volumes),
#     }
#     return render(request, "stock_chart.html", context)



# มดปิด
# from django.shortcuts import render
# from django.db import connections
# from django.http import Http404
# import json

# def stock_chart_page(request, ticker: str):
#     # 1) ดึงรายชื่อหุ้นสำหรับ dropdown แบบไม่ซ้ำ
#     with connections["clickhouse"].cursor() as cur:
#         cur.execute("""
#             SELECT DISTINCT s.ticker
#             FROM market.dim_symbol AS s
#             WHERE s.is_active = 1
#               AND s.id IN (SELECT DISTINCT symbol_id FROM market.fact_price_daily)
#             ORDER BY s.ticker
#         """)
#         symbols = [row[0] for row in cur.fetchall()]

#     # ถ้า ticker ไม่อยู่ในลิสต์ให้ 404
#     if ticker not in symbols:
#         raise Http404(f"Ticker not found: {ticker}")

#     # 2) เอา 30 วันล่าสุด (วันละ 1 แถว) ใช้ argMax เลือกค่าล่าสุดตาม load_ts
#     sql = """
#         SELECT
#             d.date AS dt,
#             argMax(f.`open`,     f.load_ts) AS open,
#             argMax(f.high,       f.load_ts) AS high,
#             argMax(f.low,        f.load_ts) AS low,
#             argMax(f.`close`,    f.load_ts) AS close,
#             argMax(f.adj_close,  f.load_ts) AS adj_close,
#             argMax(f.volume,     f.load_ts) AS volume
#         FROM market.fact_price_daily AS f
#         INNER JOIN market.dim_date AS d ON d.id = f.date_id
#         WHERE f.symbol_id IN (
#             SELECT id FROM market.dim_symbol
#             WHERE ticker = %(ticker)s AND is_active = 1
#         )
#         GROUP BY dt
#         ORDER BY dt DESC
#         LIMIT 30
#     """
#     with connections["clickhouse"].cursor() as cur:
#         cur.execute(sql, {"ticker": ticker})
#         rows = cur.fetchall()

#     if not rows:
#         raise Http404(f"No data for ticker={ticker}")

#     # เรียงเก่า->ใหม่ให้กราฟอ่านง่าย
#     rows.reverse()

#     labels   = [r[0].isoformat() for r in rows]
#     opens    = [float(r[1]) if r[1] is not None else None for r in rows]
#     highs    = [float(r[2]) if r[2] is not None else None for r in rows]
#     lows     = [float(r[3]) if r[3] is not None else None for r in rows]
#     closes   = [float(r[4]) if r[4] is not None else None for r in rows]
#     adjclose = [float(r[5]) if r[5] is not None else None for r in rows]
#     volumes  = [int(r[6])   if r[6] is not None else None for r in rows]

#     context = {
#         "ticker": ticker,
#         "symbols": symbols,
#         "labels_json":   json.dumps(labels),
#         "opens_json":    json.dumps(opens),
#         "highs_json":    json.dumps(highs),
#         "lows_json":     json.dumps(lows),
#         "closes_json":   json.dumps(closes),
#         "adjclose_json": json.dumps(adjclose),
#         "volumes_json":  json.dumps(volumes),
#     }
#     return render(request, "stock_chart.html", context)





from django.shortcuts import render
from django.db import connections
from django.http import Http404, JsonResponse
import json
import pandas as pd
from datetime import timedelta

def stock_chart_page(request, ticker: str):
    """
    แสดงกราฟหุ้นจากข้อมูลจริง (ClickHouse)
    พร้อม RSI, MACD, Timeframe filter (1M, 3M, 1Y, 5Y)
    """
    timeframe = request.GET.get("period", "1M")  # default = 1 เดือน

    # 🕒 Map ระยะเวลาเป็นจำนวนวัน
    period_map = {
        "1D": 1,
        "1W": 7,
        "1M": 30,
        "3M": 90,
        "1Y": 365,
        "5Y": 365 * 5,
    }
    days = period_map.get(timeframe, 30)

    # 1) ดึงรายชื่อหุ้นทั้งหมด (จริง)
    with connections["clickhouse"].cursor() as cur:
        cur.execute("""
            SELECT DISTINCT s.ticker
            FROM market.dim_symbol AS s
            WHERE s.is_active = 1
              AND s.id IN (SELECT DISTINCT symbol_id FROM market.fact_price_daily)
            ORDER BY s.ticker
        """)
        symbols = [r[0] for r in cur.fetchall()]

    if ticker not in symbols:
        raise Http404(f"Ticker not found: {ticker}")

    # 2) ดึงข้อมูลย้อนหลังตาม period
    sql = f"""
        SELECT
            d.date AS dt,
            argMax(f.open,  f.load_ts) AS open,
            argMax(f.high,  f.load_ts) AS high,
            argMax(f.low,   f.load_ts) AS low,
            argMax(f.close, f.load_ts) AS close,
            argMax(f.volume,f.load_ts) AS volume
        FROM market.fact_price_daily AS f
        INNER JOIN market.dim_date AS d ON d.id = f.date_id
        WHERE f.symbol_id IN (
            SELECT id FROM market.dim_symbol WHERE ticker = %(ticker)s AND is_active = 1
        )
        GROUP BY dt
        ORDER BY dt DESC
        LIMIT {days + 20}
    """

    with connections["clickhouse"].cursor() as cur:
        cur.execute(sql, {"ticker": ticker})
        rows = cur.fetchall()

    if not rows:
        raise Http404(f"No data found for {ticker}")

    df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"]).sort_values("date")

    # ✅ 1) แปลง date ให้เป็น datetime จริง
    df["date"] = pd.to_datetime(df["date"])

    # ✅ 2) แปลง numeric columns ให้เป็น float/int
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")


    # === RSI (14) ===
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))

    # === MACD (12,26,9) ===
    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    df["macd"] = ema12 - ema26
    df["signal"] = df["macd"].ewm(span=9, adjust=False).mean()

    # === คำนวณข้อมูลสถิติจริง ===
    closes = df["close"].tolist()
    highs = df["high"].tolist()
    lows = df["low"].tolist()
    volumes = df["volume"].tolist()

    if len(closes) >= 2:
        diff = closes[-1] - closes[-2]
        pct = (diff / closes[-2]) * 100 if closes[-2] else 0
        change_text = f"{diff:+.2f} ({pct:+.2f}%)"
    else:
        change_text = "-"

    high_52w = max(highs) if highs else None
    low_52w = min(lows) if lows else None
    avg_volume = (sum(volumes) / len(volumes) / 1_000_000) if volumes else None

    # ส่งค่าไป Template
    context = {
        "ticker": ticker,
        "symbols": symbols,
        "timeframe": timeframe,
        "labels_json": json.dumps(df["date"].dt.strftime("%Y-%m-%d").tolist()),
        "closes_json": json.dumps(df["close"].round(2).tolist()),
        "volumes_json": json.dumps(df["volume"].astype(int).tolist()),
        "rsi_json": json.dumps(df["rsi"].round(2).fillna(0).tolist()),
        "macd_json": json.dumps(df["macd"].round(4).fillna(0).tolist()),
        "signal_json": json.dumps(df["signal"].round(4).fillna(0).tolist()),
        "change_text": change_text,
        "high_52w": high_52w,
        "low_52w": low_52w,
        "avg_volume": avg_volume,
        "periods": ["1M", "3M", "1Y", "5Y"],   # ✅ เพิ่มตรงนี้
    }

    return render(request, "stock_chart.html", context)









###การทำงานกับ Google Gemini API
import os
import google.generativeai as genai
from django.db import connections
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
import json


genai.configure(api_key="AIzaSyCv3HKt9M-SkeAv9Tk3JhJpmr7uH6s9j-A")
model = genai.GenerativeModel("gemini-2.5-flash")

def fetch_price_data(ticker: str, days: int = 30):
    """
    ดึง OHLCV วันละ 1 แถวล่าสุด (เลือกค่าล่าสุดด้วย argMax ตาม load_ts)
    """
    sql = """
      SELECT
        d.date,
        argMax(f.open,      f.load_ts) AS open,
        argMax(f.high,      f.load_ts) AS high,
        argMax(f.low,       f.load_ts) AS low,
        argMax(f.close,     f.load_ts) AS close,
        argMax(f.adj_close, f.load_ts) AS adj_close,
        argMax(f.volume,    f.load_ts) AS volume
      FROM market.fact_price_daily f
      JOIN market.dim_symbol s ON s.id=f.symbol_id
      JOIN market.dim_date   d ON d.id=f.date_id
      WHERE lower(s.ticker) = %(ticker)s
      GROUP BY d.date
      ORDER BY d.date DESC
      LIMIT %(days)s
    """
    with connections["clickhouse"].cursor() as cur:
        cur.execute(sql, {"ticker": ticker.lower(), "days": days})
        rows = cur.fetchall()

    # เรียงเก่า -> ใหม่
    return rows[::-1]

def fetch_econ_data(code: str, months: int = 12):
    sql = """
      SELECT d.date, f.value
      FROM market.fact_econ_daily f
      JOIN market.dim_econ_indicator i ON i.id=f.indicator_id
      JOIN market.dim_date d ON d.id=f.date_id
      WHERE lower(i.code) = %(code)s
      ORDER BY d.date DESC
      LIMIT %(months)s
    """
    with connections["clickhouse"].cursor() as cur:
        cur.execute(sql, {"code": code.lower(), "months": months})
        rows = cur.fetchall()
    return rows[::-1]

# def ask_ai_about_stock(ticker: str):
#     prices = fetch_price_data(ticker)
#     table_txt = "\n".join([f"{d} O:{o} H:{h} L:{l} C:{c} V:{v}" for d,o,h,l,c,v in prices])

#     prompt = f"""
#     ข้อมูลราคาหุ้น {ticker} 30 วันล่าสุด:
#     {table_txt}

#     ช่วยวิเคราะห์แนวโน้มสั้นๆ ว่ามีสัญญาณบวกหรือลบ?
#     """
#     resp = model.generate_content(prompt)
#     return resp.text

# def ask_ai_about_econ(indicator: str):
#     econ = fetch_econ_data(indicator)
#     table_txt = "\n".join([f"{d} {val}" for d,val in econ])

#     prompt = f"""
#     ข้อมูลเศรษฐกิจ ({indicator}) 12 เดือนล่าสุด:
#     {table_txt}

#     ช่วยสรุปว่ามีแนวโน้มขึ้นหรือลง และมีนัยสำคัญต่อเศรษฐกิจไทยหรือโลกอย่างไร?
#     """
#     resp = model.generate_content(prompt)
#     return resp.text



# from django.http import JsonResponse

# def ai_stock_analysis(request, ticker: str):
#     text = ask_ai_about_stock(ticker)
#     return JsonResponse({"ticker": ticker, "analysis": text})

# def ai_econ_analysis(request, code: str):
#     text = ask_ai_about_econ(code)
#     return JsonResponse({"indicator": code, "analysis": text})








@csrf_exempt
def ai_prompt_page(request):
    """
    แสดงหน้าเว็บที่มีช่องให้พิมพ์ prompt แล้วส่งไปให้ Gemini
    """
    if request.method == "POST":
        try:
            data = json.loads(request.body.decode())
            user_prompt = data.get("prompt", "")
            if not user_prompt:
                return JsonResponse({"error": "empty prompt"}, status=400)

            resp = model.generate_content(user_prompt)
            return JsonResponse({"response": resp.text})
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)

    # GET → render หน้าเว็บ
    return render(request, "ai_prompt.html")




def ch_all_tickers():
    with connections["clickhouse"].cursor() as cur:
        cur.execute("""
            SELECT DISTINCT s.ticker
            FROM market.dim_symbol s
            WHERE s.is_active = 1
            ORDER BY s.ticker
        """)
        return [r[0] for r in cur.fetchall()]










# ต้องมั่นใจว่ามีการ import connections จาก django.db
# from django.db import connections 

def ch_fetch_price(ticker: str, days: int = 30):
    """
    ดึง OHLCV โดยใช้เทคนิค Zero-JOIN (หลาย Query) เพื่อหลีกเลี่ยงข้อจำกัด Multiple JOINs ของ ClickHouse
    """
    
    with connections["clickhouse"].cursor() as cur:
        
        # 1. A. Query 1: Get Symbol ID (Zero JOIN)
        cur.execute(
            "SELECT id FROM market.dim_symbol WHERE lower(ticker) = %(ticker)s", 
            {"ticker": ticker.lower()}
        )
        symbol_row = cur.fetchone()
        if not symbol_row:
            # ควรจัดการ Error 404 ใน Django view ที่เรียกฟังก์ชันนี้
            raise Exception(f"Ticker '{ticker}' not found in dim_symbol.")
        symbol_id = symbol_row[0]
        
        # 1. B. Query 2: Get Date IDs and the actual date (Zero JOIN)
        # ดึงวันที่จริงและ ID ของวันที่ที่ต้องการ (เช่น 30 วันล่าสุด)
        date_sql = f"""
        SELECT 
            id, date 
        FROM market.dim_date 
        WHERE date >= today() - INTERVAL %(days)s DAY 
        ORDER BY date ASC
        """
        cur.execute(date_sql, {"days": days})
        date_rows = cur.fetchall()
        
        if not date_rows:
            return [] # ไม่พบข้อมูลวันที่ในช่วงนี้
            
        # สร้าง Dictionary สำหรับแปลง ID เป็นวันที่จริงใน Python
        date_lookup = {row[0]: row[1] for row in date_rows}
        date_id_filter = ", ".join(map(str, date_lookup.keys()))
        
        
        # 2. Query 3: Main Fact Data (Zero JOIN)
        # ใช้ symbol_id และ date_id เป็นตัวกรองโดยตรงกับ Fact Table
        main_sql = f"""
        SELECT
            f.date_id,
            argMax(f.open, f.load_ts) AS open,
            argMax(f.high, f.load_ts) AS high,
            argMax(f.low, f.load_ts) AS low,
            argMax(f.close, f.load_ts) AS close,
            argMax(f.adj_close, f.load_ts) AS adj_close,
            argMax(f.volume, f.load_ts) AS volume
        FROM market.fact_price_daily f
        WHERE f.symbol_id = %(symbol_id)s
          AND f.date_id IN ({date_id_filter}) 
        GROUP BY f.date_id
        ORDER BY f.date_id ASC
        """
        
        # เนื่องจากเราต้องใช้ date_id_filter เป็น string ใน SQL Query 
        # จึงต้องส่ง symbol_id แยกไป
        cur.execute(main_sql, {"symbol_id": symbol_id})
        fact_rows = cur.fetchall()
        
    # 3. Combine results in Python (แทนการ JOIN)
    final_rows = []
    for date_id, open_p, high_p, low_p, close_p, adj_close_p, volume_p in fact_rows:
        date_str = date_lookup.get(date_id)
        if date_str:
            # นำวันที่จริงที่ได้จาก Dictionary มาใส่เป็นคอลัมน์แรก
            final_rows.append((date_str, open_p, high_p, low_p, close_p, adj_close_p, volume_p))

    # โค้ดนี้จะคืนค่าเรียงเก่า -> ใหม่ (ASC) ตามที่ต้องการแล้ว
    return final_rows # [(date, open, high, low, close, adj_close, volume), ...]

def ch_fetch_econ(code: str, npoints: int = 12):
    """
    ดึงค่าตัวชี้วัดเศรษฐกิจ 12 จุดล่าสุด (วันละ 1 แถว) เรียงเก่า→ใหม่
    """
    sql = """
      SELECT
        d.date,
        argMax(f.value, f.load_ts) AS value
      FROM market.fact_econ_daily f
      JOIN market.dim_econ_indicator i ON i.id=f.indicator_id
      JOIN market.dim_date d ON d.id=f.date_id
      WHERE lower(i.code) = %(code)s
      GROUP BY d.date
      ORDER BY d.date DESC
      LIMIT %(n)s
    """
    with connections["clickhouse"].cursor() as cur:
        cur.execute(sql, {"code": code.lower(), "n": npoints})
        rows = cur.fetchall()
    rows.reverse()
    return rows  # [(date, value), ...]

# ---------- Page: เลือกหุ้น + prompt + ผล AI ----------
def ai_stock_page(request):
    symbols = ch_all_tickers()
    # preselect ตัวแรกถ้ามี
    return render(request, "ai_stock.html", {"symbols": symbols, "default_days": 30})








# ---------- API: วิเคราะห์หุ้นด้วยข้อมูลจาก ClickHouse + prompt ผู้ใช้ ----------
from django.http import JsonResponse, Http404
from django.views.decorators.csrf import csrf_exempt
import json

import google.generativeai as genai
# ต้องกำหนด API Key ให้ถูกต้อง
genai.configure(api_key="AIzaSyCv3HKt9M-SkeAv9Tk3JhJpmr7uH6s9j-A") 
model = genai.GenerativeModel("gemini-2.5-flash") # หรือชื่อโมเดลอื่นที่คุณใช้








# เดิมใช้ดีมดปิด
# import json
# from django.http import JsonResponse
# from django.views.decorators.csrf import csrf_exempt
# from datetime import date, timedelta
# from typing import List, Tuple, Any


# @csrf_exempt
# def ai_analyze_stock(request):
#     if request.method != "POST":
#         return JsonResponse({"error": "POST only"}, status=405)

#     # 1. Parse body (ไม่มีการเปลี่ยนแปลง)
#     try:
#         payload = json.loads(request.body.decode())
#     except Exception:
#         return JsonResponse({"error": "invalid json body"}, status=400)

#     ticker      = (payload.get("ticker") or "").strip()
#     days        = int(payload.get("days") or 30)
#     user_prompt = (payload.get("prompt") or "").strip()
#     econ_code   = (payload.get("econ_code") or "").strip()

#     if not ticker:
#         return JsonResponse({"error": "ticker is required"}, status=400)

#     rows = []
#     # 2. ดึงจาก CH (พร้อมจัดการข้อผิดพลาดในการเชื่อมต่อ/คิวรี)
#     try:
#         rows = ch_fetch_price(ticker, days=days)
#     except Exception as e:
#         # ดักจับข้อผิดพลาดในการเชื่อมต่อฐานข้อมูลหรือข้อผิดพลาดที่ไม่คาดคิด
#         return JsonResponse({"error": f"Database fetch error: {e}"}, status=500)
        
#     if not rows:
#         # แก้ Http404 ให้เป็น JsonResponse 404
#         return JsonResponse({"error": f"No data for ticker={ticker}"}, status=404)

#     # แปลงเป็นข้อความ (ไม่มีการเปลี่ยนแปลง)
#     price_lines = []
#     for d, o, h, l, c, adjc, v in rows:
#         o = float(o) if o is not None else None
#         h = float(h) if h is not None else None
#         l = float(l) if l is not None else None
#         c = float(c) if c is not None else None
#         adjc = float(adjc) if adjc is not None else None
#         v = int(v) if v is not None else 0
#         price_lines.append(f"{d} O:{o} H:{h} L:{l} C:{c} AdjC:{adjc} V:{v}")
#     price_block = "\n".join(price_lines)

#     econ_block = ""
#     if econ_code:
#         try:
#             econ = ch_fetch_econ(econ_code, npoints=12)
#             if econ:
#                 econ_lines = [f"{d} {float(val) if val is not None else 'null'}" for d, val in econ]
#                 econ_block = "\n\nตัวชี้วัดเศรษฐกิจ ({}) 12 จุดล่าสุด:\n{}".format(econ_code, "\n".join(econ_lines))
#         except Exception as e:
#             # ดักจับข้อผิดพลาดในการดึงข้อมูลเศรษฐกิจ แต่ไม่ทำให้ระบบ crash
#             econ_block = f"\n\n[Warning: Failed to fetch econ data ({e})]"


#     # 3. prompt สำหรับ Gemini (ใช้ตัวแปรทั้งหมดให้ถูกต้อง)
#     SYSTEM_PROMPT = (
#         # ... (ส่วน SYSTEM_PROMPT ที่คุณกำหนดไว้ด้านบนของไฟล์) ...
#     ) # สมมติว่าคุณกำหนด SYSTEM_PROMPT ไว้เป็น Global/Module Variable แล้ว

#     # ต้องกำหนดค่าเริ่มต้นของ prompt ก่อนการใช้งาน
#     prompt = (
#         f"{SYSTEM_PROMPT}" 
#         f"--- ข้อมูลสำหรับการวิเคราะห์ ---\n"
#         f"ข้อมูลราคาหุ้น {ticker} {days} วันล่าสุด (เรียงเก่า→ใหม่):\n"
#         f"{price_block}\n"
#         f"{econ_block}"
#         f"--- คำสั่งวิเคราะห์ ---\n"
#         f"โปรดวิเคราะห์ราคาหุ้น **{ticker}** โดยสรุปประเด็นต่อไปนี้:\n"
#         f"1. **แนวโน้มหลัก (Trend)** ในช่วง {days} วันที่ผ่านมา (เช่น ขาขึ้น/ลง, Sideways)\n"
#         f"2. **แนวรับและแนวต้าน (Support/Resistance)** ที่สำคัญจากข้อมูลที่เห็น\n"
#         f"3. **ข้อสังเกตเฉพาะ** จากปริมาณการซื้อขาย (Volume) และการเคลื่อนไหวของราคา (Volatility)\n"
#         f"4. **การสรุปและข้อควรพิจารณา** ในเชิงของความเสี่ยงและโมเมนตัม\n"
#     )

#     if user_prompt:
#         prompt += f"\n--- คำขอเพิ่มเติมจากผู้ใช้ ---\n"
#         prompt += f"{user_prompt}\n"
#         prompt += f"โปรดรวมการวิเคราะห์ที่ผู้ใช้ต้องการเข้ากับการวิเคราะห์หลักข้างต้นด้วย\n"


#     # 4. เรียก Gemini (จัดการข้อผิดพลาดเฉพาะที่นี่)
#     try:
#         resp = model.generate_content(prompt)
#         answer = resp.text
#     except Exception as e:
#         # นี่คือ Try/Catch สำหรับการติดต่อ Gemini/OpenAI API โดยเฉพาะ
#         return JsonResponse({"error": f"Gemini API error: {e}"}, status=500)

#     # 5. response
#     return JsonResponse({
#         "ticker": ticker,
#         "days": days,
#         "used_econ": econ_code or None,
#         "analysis": answer,
       
#     }, status=200)


@csrf_exempt
def ai_analyze_stock(request):
    if request.method != "POST":
        return JsonResponse({"error": "POST only"}, status=405)

    try:
        payload = json.loads(request.body.decode())
    except Exception:
        return JsonResponse({"error": "invalid json body"}, status=400)

    ticker      = (payload.get("ticker") or "").strip()
    days        = int(payload.get("days") or 30)
    user_prompt = (payload.get("prompt") or "").strip()
    econ_code   = (payload.get("econ_code") or "").strip()

    if not ticker:
        return JsonResponse({"error": "ticker is required"}, status=400)

    # --- 1️⃣ ดึงข้อมูลหุ้นจาก ClickHouse ---
    try:
        rows = ch_fetch_price(ticker, days=days)
    except Exception as e:
        return JsonResponse({"error": f"Database fetch error: {e}"}, status=500)

    if not rows:
        return JsonResponse({"error": f"No data for ticker={ticker}"}, status=404)

    price_lines = []
    for d, o, h, l, c, adjc, v in rows:
        o = float(o) if o is not None else None
        h = float(h) if h is not None else None
        l = float(l) if l is not None else None
        c = float(c) if c is not None else None
        adjc = float(adjc) if adjc is not None else None
        v = int(v) if v is not None else 0
        price_lines.append(f"{d} O:{o} H:{h} L:{l} C:{c} AdjC:{adjc} V:{v}")
    price_block = "\n".join(price_lines)

    # --- 2️⃣ ดึงข้อมูลเศรษฐกิจจาก Postgres ORM ---
    from warehouse.models import EconDailyFact, EconomicIndicator

    econ_info = {}
    for code in ["CPI", "REAL_GDP", "UNEMPLOYMENT"]:
        indicator = EconomicIndicator.objects.filter(code=code).first()
        if indicator:
            latest = (
                EconDailyFact.objects.filter(indicator=indicator)
                .select_related("date")
                .order_by("-date__date")
                .first()
            )
            if latest:
                econ_info[code] = {
                    "name": indicator.name,
                    "unit": indicator.unit,
                    "value": float(latest.value),
                    "date": latest.date.date.strftime("%Y-%m-%d")
                }

    econ_summary = "\n".join([
        f"• {econ_info[c]['name']} ({c}) = {econ_info[c]['value']} {econ_info[c]['unit']} ({econ_info[c]['date']})"
        for c in econ_info
    ]) or "ไม่มีข้อมูลเศรษฐกิจล่าสุดในฐานข้อมูล"

    # --- 3️⃣ รวม block เศรษฐกิจเพิ่มเติมถ้าผู้ใช้เลือก econ_code ---
    econ_block = ""
    if econ_code:
        if econ_code in econ_info:
            e = econ_info[econ_code]
            econ_block = f"\n\nตัวชี้วัด {econ_code}: {e['value']} {e['unit']} ({e['date']})"
        else:
            econ_block = f"\n\n[Warning: ไม่มีข้อมูล {econ_code} ในฐานข้อมูล]"

    # --- 4️⃣ สร้าง Prompt สำหรับ AI ---
    prompt = f"""
คุณเป็นนักวิเคราะห์ตลาดหุ้นระดับมืออาชีพ

วิเคราะห์ราคาหุ้น {ticker} โดยอ้างอิงจากข้อมูลจริงด้านล่างนี้:

📊 **ข้อมูลเศรษฐกิจล่าสุด**
{econ_summary}

💹 **ข้อมูลราคาหุ้น {ticker} {days} วันล่าสุด (เรียงเก่า→ใหม่):**
{price_block}

{econ_block}

โปรดสรุปในประเด็น:
1. แนวโน้มหลัก (Trend)
2. แนวรับและแนวต้าน (Support / Resistance)
3. ความสัมพันธ์กับภาวะเศรษฐกิจ (CPI, GDP, Unemployment)
4. ปริมาณการซื้อขายและความผันผวน
5. ข้อควรระวังและมุมมองระยะสั้น
"""

    if user_prompt:
        prompt += f"\n\n--- คำขอเพิ่มเติมจากผู้ใช้ ---\n{user_prompt}"

    # --- 5️⃣ เรียก Gemini ---
    try:
        resp = model.generate_content(prompt)
        answer = resp.text
    except Exception as e:
        return JsonResponse({"error": f"Gemini API error: {e}"}, status=500)

    # --- 6️⃣ ส่งผลกลับ ---
    return JsonResponse({
        "ticker": ticker,
        "days": days,
        "econ_data_used": econ_info,
        "analysis": answer
    }, status=200)

