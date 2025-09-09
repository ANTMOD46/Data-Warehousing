from django.db import models

class DateDim(models.Model):
    """มิติวัน/เวลา (รายวัน) – ปกติจะ preload ล่วงหน้าเป็นปีๆ"""
    date = models.DateField(unique=True)                   # 2025-09-01
    year = models.IntegerField()
    quarter = models.IntegerField()                        # 1..4
    month = models.IntegerField()                          # 1..12
    day = models.IntegerField()
    day_of_week = models.IntegerField()                    # 1=Mon..7=Sun
    is_weekend = models.BooleanField(default=False)

    class Meta:
        db_table = "dim_date"

    def __str__(self):
        return self.date.isoformat()


class Exchange(models.Model):
    """ตลาดหลักทรัพย์/ตลาดซื้อขาย"""
    code = models.CharField(max_length=10, unique=True)    # เช่น NASDAQ, SET
    name = models.CharField(max_length=128)

    class Meta:
        db_table = "dim_exchange"

    def __str__(self):
        return self.code


class Sector(models.Model):
    """หมวดธุรกิจ (ทางเลือก: จะรวมเป็นฟิลด์ใน Symbol ก็ได้)"""
    code = models.CharField(max_length=32, unique=True)    # เช่น TECHNOLOGY
    name = models.CharField(max_length=128)

    class Meta:
        db_table = "dim_sector"

    def __str__(self):
        return self.name


class Symbol(models.Model):
    """สัญลักษณ์หุ้น/หลักทรัพย์"""
    ticker = models.CharField(max_length=24)               # เช่น AAPL, PTTEP.BK
    name = models.CharField(max_length=128, blank=True, default="")
    exchange = models.ForeignKey(Exchange, null=True, blank=True, on_delete=models.SET_NULL)
    sector = models.ForeignKey(Sector, null=True, blank=True, on_delete=models.SET_NULL)
    currency = models.CharField(max_length=8, blank=True, default="USD")
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = "dim_symbol"
        unique_together = [("ticker", "exchange")]         # กันซ้ำข้ามตลาด
        indexes = [
            models.Index(fields=["ticker"]),
        ]

    def __str__(self):
        return self.ticker


class EconomicIndicator(models.Model):
    """มิติตัวชี้วัดเศรษฐกิจ (CPI, GDP, Unemployment)"""
    code = models.CharField(max_length=32, unique=True)    # เช่น CPI, GDP, UNEMP
    name = models.CharField(max_length=128)
    unit = models.CharField(max_length=32, blank=True, default="")   # เช่น %

    class Meta:
        db_table = "dim_econ_indicator"

    def __str__(self):
        return self.code

from decimal import Decimal

class PriceDailyFact(models.Model):
    """ราคาหุ้นรายวัน (OHLCV) – Grain: 1 แถว/สัญลักษณ์/วัน"""
    symbol = models.ForeignKey(Symbol, on_delete=models.CASCADE)
    date = models.ForeignKey(DateDim, on_delete=models.PROTECT)
    open = models.DecimalField(max_digits=18, decimal_places=6, null=True)
    high = models.DecimalField(max_digits=18, decimal_places=6, null=True)
    low = models.DecimalField(max_digits=18, decimal_places=6, null=True)
    close = models.DecimalField(max_digits=18, decimal_places=6, null=True)
    adj_close = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    volume = models.BigIntegerField(null=True)
    source = models.CharField(max_length=32, default="yahoo")   # แหล่งที่มา (yahoo/alpha/…)
    load_ts = models.DateTimeField(auto_now_add=True)           # เวลาโหลดเข้า DWH

    class Meta:
        db_table = "fact_price_daily"
        unique_together = [("symbol", "date")]                  # ห้ามซ้ำ
        indexes = [
            models.Index(fields=["symbol", "date"]),
            models.Index(fields=["date"]),
        ]


class EconDailyFact(models.Model):
    """ค่าตัวชี้วัดเศรษฐกิจรายวัน/รายงวด (ถ้ารายเดือนให้ map มาที่วันที่สิ้นเดือน)"""
    indicator = models.ForeignKey(EconomicIndicator, on_delete=models.CASCADE)
    date = models.ForeignKey(DateDim, on_delete=models.PROTECT)
    value = models.DecimalField(max_digits=18, decimal_places=6, null=True)
    source = models.CharField(max_length=32, default="fred")     # ตัวอย่าง: FRED
    load_ts = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "fact_econ_daily"
        unique_together = [("indicator", "date")]
        indexes = [
            models.Index(fields=["indicator", "date"]),
            models.Index(fields=["date"]),
        ]
