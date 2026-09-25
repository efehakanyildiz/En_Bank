# Fabric Notebook — 03a: Borsa Teknik Analiz & Sinyaller → Gold Katmanı
# ================================================================
# Bu notebook, 'silver_borsa' tablosundan teknik analiz göstergelerini
# (SMA, RSI, Bollinger Bantları, Golden/Death Cross) ve günlük KPI
# özetlerini hesaplar, Delta MERGE INTO ile Gold tablolara yazar.
#
# Pipeline'da: 03b_gold_makro_getiri ile PARALEL çalışır.
# ================================================================

# %% [markdown]
# ## 1. Kurulum & Kütüphaneler

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, stddev, lit, when,
    min as spark_min, max as spark_max,
    row_number, current_timestamp, round as spark_round,
    lag, abs as spark_abs
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

MA_WINDOWS = [7, 20, 50, 200]
RSI_PERIOD = 14
BOLLINGER_WINDOW = 20
BOLLINGER_STD = 2

# %% [markdown]
# ## 2. Teknik Göstergelerin Hesaplanması

# %%
print("=" * 60)
print("📊 GOLD TEKNİK ANALİZ HESAPLANIYOR (03a_gold_teknik_analiz)")
print("=" * 60)

df_borsa = spark.table("silver_borsa")
print(f"📦 Okunan silver_borsa satır sayısı: {df_borsa.count()}")

w_ticker = Window.partitionBy("ticker").orderBy("date")

# 2a. Hareketli Ortalamalar (SMA)
df_gold = df_borsa.alias("base")
for window_size in MA_WINDOWS:
    w_ma = w_ticker.rowsBetween(-(window_size - 1), 0)
    col_name = f"sma_{window_size}"
    df_gold = df_gold.withColumn(col_name, spark_round(avg("close").over(w_ma), 4))

# 2b. RSI (Relative Strength Index)
df_gold = df_gold.withColumn("price_change", col("close") - lag("close", 1).over(w_ticker))
df_gold = df_gold \
    .withColumn("gain", when(col("price_change") > 0, col("price_change")).otherwise(0)) \
    .withColumn("loss", when(col("price_change") < 0, spark_abs(col("price_change"))).otherwise(0))

w_rsi = w_ticker.rowsBetween(-(RSI_PERIOD - 1), 0)
df_gold = df_gold \
    .withColumn("avg_gain", avg("gain").over(w_rsi)) \
    .withColumn("avg_loss", avg("loss").over(w_rsi)) \
    .withColumn("rs", when(col("avg_loss") > 0, col("avg_gain") / col("avg_loss")).otherwise(100)) \
    .withColumn("rsi", spark_round(100 - (100 / (1 + col("rs"))), 2))

# 2c. Bollinger Bantları
w_bb = w_ticker.rowsBetween(-(BOLLINGER_WINDOW - 1), 0)
df_gold = df_gold \
    .withColumn("bb_middle", spark_round(avg("close").over(w_bb), 4)) \
    .withColumn("bb_std", stddev("close").over(w_bb)) \
    .withColumn("bb_upper", spark_round(col("bb_middle") + (BOLLINGER_STD * col("bb_std")), 4)) \
    .withColumn("bb_lower", spark_round(col("bb_middle") - (BOLLINGER_STD * col("bb_std")), 4))

# 2d. Alım / Satım Sinyalleri
df_gold = df_gold \
    .withColumn("prev_sma_50", lag("sma_50", 1).over(w_ticker)) \
    .withColumn("prev_sma_200", lag("sma_200", 1).over(w_ticker)) \
    .withColumn("signal",
        when(
            (col("sma_50").isNotNull()) & (col("sma_200").isNotNull()) &
            (col("sma_50") > col("sma_200")) & (col("prev_sma_50") <= col("prev_sma_200")),
            lit("GOLDEN_CROSS")
        ).when(
            (col("sma_50").isNotNull()) & (col("sma_200").isNotNull()) &
            (col("sma_50") < col("sma_200")) & (col("prev_sma_50") >= col("prev_sma_200")),
            lit("DEATH_CROSS")
        ).when(
            (col("sma_50").isNotNull()) & (col("sma_200").isNotNull()) &
            (col("sma_50") > col("sma_200")),
            lit("BULLISH")
        ).when(
            (col("sma_50").isNotNull()) & (col("sma_200").isNotNull()) &
            (col("sma_50") < col("sma_200")),
            lit("BEARISH")
        ).otherwise(lit("NEUTRAL"))
    )

df_teknik = df_gold.select(
    "date", "ticker", "company_name",
    "open", "high", "low", "close", "volume",
    "daily_change", "daily_change_pct",
    "sma_7", "sma_20", "sma_50", "sma_200",
    "rsi",
    "bb_upper", "bb_middle", "bb_lower",
    "signal",
    current_timestamp().alias("gold_timestamp")
)

# %%
# gold_teknik_analiz tablosuna MERGE INTO
if not spark.catalog.tableExists("gold_teknik_analiz"):
    print("📦 gold_teknik_analiz tablosu oluşturuluyor (İlk Yükleme)...")
    df_teknik.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_teknik_analiz")
else:
    print("🔄 gold_teknik_analiz tablosuna MERGE INTO yapılıyor...")
    dt_teknik = DeltaTable.forName(spark, "gold_teknik_analiz")
    (
        dt_teknik.alias("target")
        .merge(
            df_teknik.alias("source"),
            "target.ticker = source.ticker AND target.date = source.date"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

print(f"✅ gold_teknik_analiz güncellendi — Toplam: {spark.table('gold_teknik_analiz').count()} satır")

# %% [markdown]
# ## 3. KPI Özet Tablosu (Snapshot)

# %%
w_latest = Window.partitionBy("ticker").orderBy(col("date").desc())

df_perf = df_borsa \
    .withColumn("close_5d_ago", lag("close", 5).over(w_ticker)) \
    .withColumn("close_22d_ago", lag("close", 22).over(w_ticker)) \
    .withColumn("weekly_change_pct",
        when(col("close_5d_ago") > 0,
            spark_round(((col("close") - col("close_5d_ago")) / col("close_5d_ago")) * 100, 2)
        )
    ) \
    .withColumn("monthly_change_pct",
        when(col("close_22d_ago") > 0,
            spark_round(((col("close") - col("close_22d_ago")) / col("close_22d_ago")) * 100, 2)
        )
    )

df_kpi = df_perf \
    .withColumn("rn", row_number().over(w_latest)) \
    .filter(col("rn") == 1) \
    .select(
        "date", "ticker", "company_name", "close", "volume",
        "daily_change", "daily_change_pct", "weekly_change_pct", "monthly_change_pct",
        current_timestamp().alias("gold_timestamp")
    )

df_kpi.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_kpi_ozet")
print(f"✅ gold_kpi_ozet yazıldı: {spark.table('gold_kpi_ozet').count()} satır")

print("\n🎉 03a_gold_teknik_analiz başarıyla tamamlandı!")
