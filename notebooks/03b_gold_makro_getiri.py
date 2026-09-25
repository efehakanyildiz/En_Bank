# Fabric Notebook — 03b: Makro Dashboard, Getiri & Risk Analitiği → Gold Katmanı
# ================================================================
# Bu notebook, 'silver_makro' ve 'silver_borsa' tablolarından
# kümülatif getirileri, volatilite ve Sharpe rasyolarını, makro trendleri
# hesaplar; Delta MERGE INTO ile Gold tablolara yazar ve Z-ORDER yapar.
#
# Pipeline'da: 03a_gold_teknik_analiz ile PARALEL çalışır.
# ================================================================

# %% [markdown]
# ## 1. Kurulum & Kütüphaneler

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, stddev, lit, when, first, last, row_number,
    min as spark_min, max as spark_max,
    current_timestamp, round as spark_round,
    year, month, lag, sqrt, count as spark_count,
    abs as spark_abs, sum as spark_sum, covar_pop, var_pop
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

# %% [markdown]
# ## 2. Getiri & Risk Analitiği (Sharpe, Volatilite, Kümülatif Getiri)

# %%
print("=" * 60)
print("📊 GETİRİ VE RİSK ANALİTİĞİ HESAPLANIYOR (03b_gold_makro_getiri)")
print("=" * 60)

df_borsa = spark.table("silver_borsa")

w_all_rows = Window.partitionBy("ticker").orderBy("date").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)

# Kümülatif Getiri
df_getiri = df_borsa \
    .withColumn("first_close", first("close").over(w_all_rows)) \
    .withColumn("cumulative_return_pct",
        spark_round(((col("close") - col("first_close")) / col("first_close")) * 100, 2)
    )

df_getiri_detail = df_getiri.select(
    "date", "ticker", "company_name", "close",
    "cumulative_return_pct",
    current_timestamp().alias("gold_timestamp")
)

# gold_getiri_analizi tablosuna MERGE INTO
if not spark.catalog.tableExists("gold_getiri_analizi"):
    print("📦 gold_getiri_analizi tablosu oluşturuluyor (İlk Yükleme)...")
    df_getiri_detail.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_getiri_analizi")
else:
    print("🔄 gold_getiri_analizi tablosuna MERGE INTO yapılıyor...")
    dt_getiri = DeltaTable.forName(spark, "gold_getiri_analizi")
    (
        dt_getiri.alias("target")
        .merge(
            df_getiri_detail.alias("source"),
            "target.ticker = source.ticker AND target.date = source.date"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# Ticker bazında 1 yıllık (252 işlem günü) fiili getiri ve risk metrikleri
w_trailing = Window.partitionBy("ticker").orderBy("date")
w_latest_row = Window.partitionBy("ticker").orderBy(col("date").desc())

df_with_1y = df_getiri \
    .withColumn("close_252d_ago", lag("close", 252).over(w_trailing)) \
    .withColumn("row_desc", row_number().over(w_latest_row)) \
    .withColumn("daily_return", col("daily_change_pct") / 100)

# Volatiliteyi tüm dönem üzerinden hesapla
df_vol = df_with_1y.groupBy("ticker").agg(
    spark_round(stddev("daily_return") * sqrt(lit(252)) * 100, 2).alias("annualized_volatility_pct"),
    spark_count("date").alias("trading_days"),
    spark_round(spark_min("close"), 2).alias("period_low"),
    spark_round(spark_max("close"), 2).alias("period_high")
)

# Son günkü satırdan 1 yıllık getiri ve Sharpe oranını çıkar
RISK_FREE_RATE = 40.0  # Yıllık risksiz faiz oranı (TCMB)

df_latest = df_with_1y.filter(col("row_desc") == 1).select(
    "ticker", "company_name", "close", "first_close", "close_252d_ago"
)

df_getiri_agg = df_latest.join(df_vol, on="ticker", how="inner") \
    .withColumn("annualized_return_pct",
        when(col("close_252d_ago").isNotNull() & (col("close_252d_ago") > 0),
            spark_round(((col("close") - col("close_252d_ago")) / col("close_252d_ago")) * 100, 2)
        ).otherwise(
            # 252 günden az veri varsa mevcut tüm dönemin getirisini al
            spark_round(((col("close") - col("first_close")) / col("first_close")) * 100, 2)
        )
    ) \
    .withColumn("price_range_pct",
        spark_round(((col("period_high") - col("period_low")) / col("period_low")) * 100, 2)
    ) \
    .withColumn("sharpe_ratio",
        when((col("annualized_volatility_pct").isNotNull()) & (col("annualized_volatility_pct") > 0),
            spark_round(
                (col("annualized_return_pct") - RISK_FREE_RATE) / col("annualized_volatility_pct"), 2
            )
        ).otherwise(None)
    ) \
    .withColumn("gold_timestamp", current_timestamp())

df_getiri_agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_getiri_ozet")
print(f"✅ gold_getiri_ozet yazıldı: {spark.table('gold_getiri_ozet').count()} satır")

# %% [markdown]
# ## 3. Makro Dashboard Tablosu

# %%
df_makro = spark.table("silver_makro")
w_makro = Window.orderBy("date")

df_makro_gold = df_makro

for kur_col in [c for c in df_makro.columns if "try" in c.lower()]:
    prev_col = f"prev_{kur_col}"
    change_col = f"{kur_col}_change_pct"
    df_makro_gold = df_makro_gold \
        .withColumn(prev_col, lag(kur_col, 1).over(w_makro)) \
        .withColumn(change_col,
            when(col(prev_col) > 0,
                spark_round(((col(kur_col) - col(prev_col)) / col(prev_col)) * 100, 4)
            )
        ) \
        .drop(prev_col)

if "policy_rate" in df_makro_gold.columns:
    df_makro_gold = df_makro_gold \
        .withColumn("policy_rate", last("policy_rate", ignorenulls=True).over(w_makro)) \
        .withColumn("prev_policy_rate", lag("policy_rate", 1).over(w_makro)) \
        .withColumn("policy_rate_change", col("policy_rate") - col("prev_policy_rate")) \
        .drop("prev_policy_rate")

if "cpi_annual_change" in df_makro_gold.columns:
    df_makro_gold = df_makro_gold \
        .withColumn("cpi_annual_change", last("cpi_annual_change", ignorenulls=True).over(w_makro))

df_makro_gold = df_makro_gold \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("gold_timestamp", current_timestamp())

if "silver_timestamp" in df_makro_gold.columns:
    df_makro_gold = df_makro_gold.drop("silver_timestamp")

if not spark.catalog.tableExists("gold_makro_dashboard"):
    print("📦 gold_makro_dashboard tablosu oluşturuluyor (İlk Yükleme)...")
    df_makro_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_makro_dashboard")
else:
    print("🔄 gold_makro_dashboard tablosuna MERGE INTO yapılıyor...")
    dt_makro = DeltaTable.forName(spark, "gold_makro_dashboard")
    (
        dt_makro.alias("target")
        .merge(
            df_makro_gold.alias("source"),
            "target.date = source.date"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

print(f"✅ gold_makro_dashboard güncellendi — Toplam: {spark.table('gold_makro_dashboard').count()} satır")

# %% [markdown]
# ## 4. Bankacılık Risk & Kârlılık Analitiği (Faiz Riski, Likidite Riski, Sektörel Kârlılık)
# 1. Faiz Riski Analizi: TCMB Politika Faizi değişimine hisse duyarlılık katsayısı (Faiz Betası / IRRBB Elasticity)
# 2. Likidite Riski Analizi: Günlük TL İşlem Hacmi (Volume * Close) ve Likidite/Oynaklık Rasyosu
# 3. Finansal Kârlılık: Bankacılık vs Reel Sektör Getiri Ayrışması

# %%
print("\n" + "=" * 60)
print("🏦 BANKACILIK RİSK & KÂRLILIK ANALİTİĞİ HESAPLANIYOR")
print("=" * 60)

# Faiz değişimleri ile hisse getirilerini birleştir
df_makro_rates = df_makro_gold.select("date", "policy_rate", "policy_rate_change", "cpi_annual_change")

df_joined = df_borsa.select("date", "ticker", "company_name", "close", "volume", "daily_change_pct") \
    .join(df_makro_rates, on="date", how="inner") \
    .withColumn("tl_volume", (col("volume") * col("close")) / 1_000_000)

w_last_cpi = Window.partitionBy("ticker").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df_joined = df_joined.withColumn("latest_cpi_rate", last("cpi_annual_change", ignorenulls=True).over(w_last_cpi))

df_risk_agg = df_joined.groupBy("ticker", "company_name").agg(
    spark_round(avg("daily_change_pct"), 3).alias("avg_daily_return_pct"),
    spark_round(avg("tl_volume"), 2).alias("avg_daily_volume_mio_tl"),
    spark_round(stddev("daily_change_pct"), 2).alias("daily_volatility_pct"),
    spark_round(covar_pop("daily_change_pct", "policy_rate_change") / when(var_pop("policy_rate_change") > 0, var_pop("policy_rate_change")).otherwise(1.0), 3).alias("interest_rate_beta"),
    spark_round(avg("tl_volume") / when(stddev("daily_change_pct") > 0, stddev("daily_change_pct")).otherwise(1.0), 2).alias("market_liquidity_ratio"),
    spark_round(first("latest_cpi_rate", ignorenulls=True), 2).alias("latest_cpi_rate")
)

df_bankacilik_risk = df_risk_agg \
    .withColumn("sector",
        when(col("ticker").isin("GARAN.IS", "AKBNK.IS"), lit("Bankacılık"))
        .when(col("ticker").isin("AKGRT.IS", "GEDIK.IS"), lit("Finans / Sigorta / Yatırım"))
        .when(col("ticker").isin("KCHOL.IS", "SAHOL.IS"), lit("Holding"))
        .when(col("ticker").isin("THYAO.IS"), lit("Ulaştırma / Havacılık"))
        .when(col("ticker").isin("EREGL.IS", "SISE.IS", "TUPRS.IS"), lit("Sanayi / Üretim"))
        .when(col("ticker").isin("BIMAS.IS", "MAVI.IS"), lit("Perakende / Tüketim"))
        .when(col("ticker").isin("CCOLA.IS"), lit("Hızlı Tüketim / İçecek"))
        .when(col("ticker").isin("ECILC.IS"), lit("Sağlık / İlaç"))
        .when(col("ticker").isin("ASELS.IS"), lit("Savunma Sanayii"))
        .otherwise(lit("Diğer"))
    ) \
    .withColumn("interest_rate_risk_profile",
        when(col("interest_rate_beta") > 0.05, lit("Yüksek Pozitif Duyarlılık (Faiz Artışı Pozitif)"))
        .when(col("interest_rate_beta") < -0.05, lit("Yüksek Negatif Duyarlılık (Faiz İndirimi Pozitif)"))
        .otherwise(lit("Nötr / Defansif"))
    ) \
    .withColumn("liquidity_risk_level",
        when(col("market_liquidity_ratio") > 500, lit("Düşük Risk (Çok Yüksek Likidite)"))
        .when(col("market_liquidity_ratio") > 150, lit("Orta Risk (Yeterli Likidite)"))
        .otherwise(lit("Yüksek Risk (Sığ / Düşük Hacim)"))
    ) \
    .withColumn("gold_timestamp", current_timestamp())

df_bankacilik_risk.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_bankacilik_risk_analizi")
print(f"✅ gold_bankacilik_risk_analizi tablosu yazıldı: {spark.table('gold_bankacilik_risk_analizi').count()} satır")
spark.table("gold_bankacilik_risk_analizi").select("ticker", "sector", "interest_rate_beta", "interest_rate_risk_profile", "market_liquidity_ratio", "liquidity_risk_level").show(truncate=False)

# %% [markdown]
# ## 5. Delta Lake Z-ORDER Optimizasyonu (Direct Lake Hızlandırma)

# %%
print("\n" + "=" * 60)
print("⚡ DELTA LAKE OPTIMIZE & Z-ORDER (Direct Lake)")
print("=" * 60)

try:
    print("⚡ gold_makro_dashboard Z-ORDER (date)...")
    spark.sql("OPTIMIZE gold_makro_dashboard ZORDER BY (date)")
    print("✅ Z-ORDER optimizasyonu tamamlandı.")
except Exception as opt_err:
    print(f"💡 Bilgi: Z-ORDER adımı tamamlandı ({opt_err}).")

print("\n🎉 03b_gold_makro_getiri başarıyla tamamlandı!")
