from django.contrib import admin
from .models import *

# ลงทะเบียน Model เพื่อให้แสดงในหน้า Admin
admin.site.register(Symbol)
admin.site.register(Exchange)
admin.site.register(Sector)
admin.site.register(PriceDailyFact)
admin.site.register(DateDim)
admin.site.register(EconomicIndicator)
admin.site.site_header = "Data Warehouse Admin"
admin.site.site_title = "Data Warehouse Admin Portal"