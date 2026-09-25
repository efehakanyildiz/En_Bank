# -*- coding: utf-8 -*-
"""
Notebook 4: Delta Lake Bakım, Sıkıştırma ve Optimizasyon (Maintenance)
===================================================================
Bu notebook, veri işlem hattının (pipeline) en sonunda çalışır.
Her gün artımlı (MERGE INTO) yüklemeler sonrası oluşan küçük dosyaları (small files)
birleştirir (Compaction) ve Power BI DirectLake sorgu hızını maksimuma çıkarır.

Fabric Notebook'a doğrudan kopyalayıp çalıştırabilirsiniz.
"""

# %% [markdown]
# # 🧹 Delta Lake Bakım & Optimizasyon (Compaction & Z-ORDER)

# %%
import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print("=" * 60)
print("🚀 DELTA LAKE TABLO BAKIMI VE OPTİMİZASYON BAŞLIYOR")
print("=" * 60)

# Optimize edilecek kritik tablolar
MAINTENANCE_TABLES = [
    {"table": "bronze_yfinance",        "zorder": "ticker, date"},
    {"table": "silver_borsa",            "zorder": "ticker, date"},
    {"table": "silver_makro",            "zorder": "date"},
    {"table": "gold_teknik_analiz",      "zorder": "ticker, date"},
    {"table": "gold_getiri_analizi",     "zorder": "ticker, date"},
    {"table": "gold_makro_dashboard",    "zorder": "date"}
]

start_time = time.time()
optimized_count = 0

for item in MAINTENANCE_TABLES:
    tbl = item["table"]
    zorder_cols = item.get("zorder")
    
    if spark.catalog.tableExists(tbl):
        try:
            print(f"\n📦 {tbl} tablosu optimize ediliyor (Z-ORDER: {zorder_cols})...")
            
            # Z-ORDER ile sorgulama performansını indeksle
            if zorder_cols:
                spark.sql(f"OPTIMIZE {tbl} ZORDER BY ({zorder_cols})")
            else:
                spark.sql(f"OPTIMIZE {tbl}")
                
            optimized_count += 1
            print(f"✅ {tbl} başarıyla sıkıştırıldı (Compacted).")
            
        except Exception as e:
            print(f"⚠️ {tbl} optimizasyon uyarısı: {e}")
    else:
        print(f"ℹ️ {tbl} henüz mevcut değil, atlandı.")

duration = round(time.time() - start_time, 2)
print("\n" + "=" * 60)
print(f"🎉 BAKIM TAMAMLANDI: {optimized_count} tablo optimize edildi. Süre: {duration} saniye.")
print("=" * 60)
