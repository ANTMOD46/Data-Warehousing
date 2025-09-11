from django.urls import path
#from warehouse.views import prices_view, summary_view, last_close_view
from . import views


urlpatterns = [
    path('', views.home, name='home'),
    # path("prices/", prices_view),
    # path("summary/", summary_view),
    # path("last-close/", last_close_view),
    path('load-stock-data/<str:symbol_ticker>/', views.load_stock_data, name='load_stock_data'),
    path('get-economic-indicators/', views.get_economic_indicators, name='get_economic_indicators'),
    path("load-economic-indicators/", views.load_economic_indicators, name="load_economic_indicators"),
    path("ch/chart/<str:ticker>/", views.stock_chart_page, name="ch_stock_chart_page"),
    # path("ai/stock/<str:ticker>/", views.ai_stock_analysis),
    # path("ai/econ/<str:code>/", views.ai_econ_analysis),
    path("ai/prompt/", views.ai_prompt_page, name="ai_prompt"),
    path("ai/stock/", views.ai_stock_page, name="ai_stock_page"),
    path("api/ai/analyze-stock", views.ai_analyze_stock, name="ai_analyze_stock")

]
