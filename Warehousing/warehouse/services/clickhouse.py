# # <your_app>/services/clickhouse.py
# from django.db import connections

# def last_30d_close(symbol_id: int):
#     sql = """
#       SELECT d.date, f.close
#       FROM market.fact_price_daily f
#       JOIN market.dim_date d ON d.id = f.date_id
#       WHERE f.symbol_id = %(sid)s
#       ORDER BY d.date DESC
#       LIMIT 30
#     """
#     with connections["clickhouse"].cursor() as cur:
#         cur.execute(sql, {"sid": symbol_id})
#         return cur.fetchall()

# def last_30d_close_by_ticker(ticker: str):
#     sql = """
#       SELECT d.date, f.close
#       FROM market.fact_price_daily f
#       JOIN market.dim_date d ON d.id = f.date_id
#       JOIN market.dim_symbol s ON s.id = f.symbol_id
#       WHERE s.ticker = %(ticker)s
#       ORDER BY d.date DESC
#       LIMIT 30
#     """
#     with connections["clickhouse"].cursor() as cur:
#         cur.execute(sql, {"ticker": ticker})
#         return cur.fetchall()
