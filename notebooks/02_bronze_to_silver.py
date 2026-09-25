# Fabric Notebook — 02: Bronze → Silver Katmanı
# ================================================================
# Bu notebook Bronze tablolardaki ham verileri temizler,
# tip dönüşümleri yapar, tekilleştirir ve Silver tablolara yazar.
#
# Çalıştırma: Pipeline'da 01_api_to_bronze sonrasında tetiklenir.
# ================================================================

# %% [markdown]
# ## 1. Kurulum

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, to_timestamp, trim, upper, lower, lit, concat,
    when, coalesce, current_timestamp, row_number, regexp_replace,
    split, element_at, avg, last, countDistinct, max as spark_max,
    lag, round as spark_round
)
from pyspark.sql.types import (
    DoubleType, DateType, LongType, StringType, IntegerType, TimestampType, StructType, StructField
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, date

spark = SparkSession.builder.getOrCreate()

# %% [markdown]
# ## 2. Veri Kalite Kapısı (Data Quality & Governance Gate)
# Silver katmanına geçmeden önce Bronze tabloları üzerinde 6 temel kalite kontrolü uygulanır.
# Hatalı/geçersiz kayıtlar 'silver_quarantine_records' tablosuna izole edilir.
# Tüm test sonuçları 'silver_data_quality_log' Delta tablosuna denetim (audit) izi olarak yazılır.

# %%
print("=" * 60)
print("🛡️  VERİ KALİTE KONTROLLERİ BAŞLIYOR (Data Quality Gate)")
print("=" * 60)

dq_results = []

def record_dq_result(check_id: str, check_name: str, table_name: str, category: str, failed_count: int, threshold: int, details: str):
    status = "PASSED" if failed_count <= threshold else "FAILED"
    dq_results.append({
        "check_id": check_id,
        "check_name": check_name,
        "table_name": table_name,
        "category": category,
        "status": status,
        "failed_records": int(failed_count),
        "threshold": int(threshold),
        "details": details,
        "check_timestamp": datetime.now()
    })
    icon = "✅" if status == "PASSED" else "❌"
    print(f"  {icon} [{category}] {check_name} ({table_name}): {status} | Hatalı: {failed_count} (Eşik: {threshold})")

# Tabloları kontrol için oku
df_borsa_raw = spark.table("bronze_yfinance")
df_kur_raw_check = spark.table("bronze_tcmb_kur")
df_faiz_raw_check = spark.table("bronze_tcmb_faiz")

# ---- Test 1: Borsa Null Kontrolü (Completeness) ----
null_borsa_count = df_borsa_raw.filter(
    col("date").isNull() | col("ticker").isNull() | col("close").isNull()
).count()
record_dq_result(
    "DQ_01", "Null Değer Kontrolü", "bronze_yfinance", "COMPLETENESS",
    null_borsa_count, 0, "date, ticker veya close sütunlarında null olmamalı"
)

# ---- Test 2: Borsa Fiyat & Hacim Pozitiflik Kontrolü (Validity) ----
invalid_price_count = df_borsa_raw.filter(
    (col("close") <= 0) | (col("open") <= 0) | (col("volume") < 0)
).count()
record_dq_result(
    "DQ_02", "Fiyat & Hacim Pozitiflik Kontrolü", "bronze_yfinance", "VALIDITY",
    invalid_price_count, 0, "close ve open > 0, volume >= 0 olmalı"
)

# ---- Test 3: Şirket Sayısı Tamlık Kontrolü (Ticker Completeness) ----
ticker_count = df_borsa_raw.select(countDistinct("ticker")).collect()[0][0]
missing_tickers = max(0, 10 - ticker_count)
record_dq_result(
    "DQ_03", "Şirket Sayısı Kontrolü", "bronze_yfinance", "COMPLETENESS",
    missing_tickers, 0, f"Beklenen: 10 şirket, Mevcut: {ticker_count}"
)

# ---- Test 4: EVDS Döviz Kuru Null Kontrolü (Completeness) ----
null_kur_count = df_kur_raw_check.filter(col("Tarih").isNull()).count()
record_dq_result(
    "DQ_04", "Tarih Null Kontrolü", "bronze_tcmb_kur", "COMPLETENESS",
    null_kur_count, 0, "Tarih alanı null olmamalı"
)

# ---- Test 5: Faiz Oranı Aralık Kontrolü (Validity) ----
faiz_cols = [c for c in df_faiz_raw_check.columns if "apifon" in c.lower() or "faiz" in c.lower()]
if faiz_cols:
    invalid_faiz_count = df_faiz_raw_check.filter(
        (col(faiz_cols[0]).cast(DoubleType()) < 0) | (col(faiz_cols[0]).cast(DoubleType()) > 150)
    ).count()
    record_dq_result(
        "DQ_05", "Faiz Oranı Aralık Kontrolü", "bronze_tcmb_faiz", "VALIDITY",
        invalid_faiz_count, 0, "TCMB faizi %0 ile %150 aralığında olmalı"
    )

# ---- Test 6: Veri Tazeliği / SLA Kontrolü (Freshness) ----
latest_date_row = df_borsa_raw.select(spark_max(to_date(col("date")))).collect()[0][0]
if latest_date_row:
    if isinstance(latest_date_row, str):
        latest_date_row = datetime.strptime(latest_date_row[:10], "%Y-%m-%d").date()
    days_lag = (date.today() - latest_date_row).days
    stale_flag = 1 if days_lag > 5 else 0
    record_dq_result(
        "DQ_06", "Veri Tazelik / SLA Kontrolü", "bronze_yfinance", "FRESHNESS",
        stale_flag, 0, f"Son veri tarihi: {latest_date_row} ({days_lag} gün önce)"
    )

# %%
# Karantina Mekanizması (Dead Letter Queue): Hatalı satırları izole et
df_quarantine = df_borsa_raw.filter(
    col("date").isNull() | col("ticker").isNull() | col("close").isNull() | (col("close") <= 0)
)
quarantine_count = df_quarantine.count()
if quarantine_count > 0:
    print(f"\n⚠️  {quarantine_count} adet hatalı kayıt 'silver_quarantine_records' tablosuna izole ediliyor...")
    df_quarantine.withColumn("quarantine_timestamp", current_timestamp()) \
        .write.format("delta").mode("append").saveAsTable("silver_quarantine_records")
else:
    print("\n✅ Temiz veri: Hiçbir kayıt karantinaya alınmadı.")

# %%
# Kalite Logunu Delta Tablosuna Yaz ('silver_data_quality_log')
dq_schema = StructType([
    StructField("check_id", StringType(), False),
    StructField("check_name", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("status", StringType(), False),
    StructField("failed_records", IntegerType(), False),
    StructField("threshold", IntegerType(), False),
    StructField("details", StringType(), True),
    StructField("check_timestamp", TimestampType(), False),
])

df_dq_log = spark.createDataFrame(dq_results, schema=dq_schema)
df_dq_log.write.format("delta").mode("append").saveAsTable("silver_data_quality_log")

passed_tests = sum(1 for r in dq_results if r["status"] == "PASSED")
total_tests = len(dq_results)
dq_score = round((passed_tests / total_tests) * 100, 1)

print("\n" + "=" * 60)
print(f"🏆 VERİ KALİTE SKORU: %{dq_score}  ({passed_tests}/{total_tests} Başarılı)")
print("   Sonuçlar 'silver_data_quality_log' tablosuna işlendi.")
print("=" * 60)

# %% [markdown]
# ## 3. Bronze → Silver: Borsa Verileri

# %%
print("\n" + "=" * 60)
print("📊 BORSA VERİLERİ TEMİZLENİYOR (Bronze → Silver)")
print("=" * 60)

# %%
# 2a. Tip dönüşümleri
df_borsa = df_borsa_raw \
    .withColumn("date", to_date(col("date"))) \
    .withColumn("open", col("open").cast(DoubleType())) \
    .withColumn("high", col("high").cast(DoubleType())) \
    .withColumn("low", col("low").cast(DoubleType())) \
    .withColumn("close", col("close").cast(DoubleType())) \
    .withColumn("volume", col("volume").cast(LongType()))

# 2b. Null / geçersiz veri kontrolü
df_borsa = df_borsa.filter(
    col("date").isNotNull() &
    col("ticker").isNotNull() &
    col("close").isNotNull() &
    (col("close") > 0)
)

# 2c. Tekilleştirme — aynı tarih+ticker kombinasyonunda en son ingestion'ı al
window_dedup = Window.partitionBy("ticker", "date").orderBy(col("ingestion_timestamp").desc())

df_borsa = df_borsa \
    .withColumn("row_num", row_number().over(window_dedup)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

# 2d. Günlük değişim hesapla
window_daily = Window.partitionBy("ticker").orderBy("date")

df_borsa = df_borsa \
    .withColumn("prev_close", last("close", ignorenulls=True).over(
        window_daily.rowsBetween(-1, -1)
    )) \
    .withColumn("daily_change", col("close") - col("prev_close")) \
    .withColumn("daily_change_pct",
        when(col("prev_close") > 0,
             ((col("close") - col("prev_close")) / col("prev_close")) * 100
        ).otherwise(None)
    )

# 2e. Son sütun seçimi
df_borsa_silver = df_borsa.select(
    "date",
    "ticker",
    "company_name",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "prev_close",
    "daily_change",
    "daily_change_pct",
    "source",
    lit(current_timestamp()).alias("silver_timestamp")
)

# %%
# Silver tablosuna yaz (İlk Yükleme vs MERGE INTO)
if not spark.catalog.tableExists("silver_borsa"):
    print("📦 silver_borsa tablosu oluşturuluyor (İlk Yükleme)...")
    df_borsa_silver.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("silver_borsa")
else:
    print("🔄 silver_borsa tablosuna MERGE INTO yapılıyor (Artımlı Yükleme)...")
    dt_borsa = DeltaTable.forName(spark, "silver_borsa")
    (
        dt_borsa.alias("target")
        .merge(
            df_borsa_silver.alias("source"),
            "target.ticker = source.ticker AND target.date = source.date"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

count = spark.table("silver_borsa").count()
print(f"✅ silver_borsa güncellendi — Toplam: {count} satır")

# Örnek veri göster
print("\n📋 Örnek veriler:")
spark.table("silver_borsa").orderBy(col("date").desc()).show(10, truncate=False)

# %% [markdown]
# ## 3. Bronze → Silver: TCMB Döviz Kurları

# %%
print("\n" + "=" * 60)
print("💱 DÖVİZ KURLARI TEMİZLENİYOR (Bronze → Silver)")
print("=" * 60)

def clean_evds_table(table_name: str) -> "DataFrame":
    """
    EVDS Bronze tablosunu okur, tarih ve değer sütunlarını temizler.
    EVDS formatı: Tarih sütunu 'Tarih', değer sütunları seri kodlarıyla adlandırılmış.
    """
    df = spark.table(table_name)
    # EVDS3 metadata sütunlarını (UNIXTIME struct vb.) kaldır
    drop_cols = [c for c in df.columns if c.upper() in ["UNIXTIME", "YEARWEEK"]]
    if drop_cols:
        df = df.drop(*drop_cols)
    print(f"📦 {table_name} Bronze satır sayısı: {df.count()}")
    print(f"   Sütunlar: {df.columns}")
    return df

# %%
# Döviz kuru verisini temizle
df_kur_raw = clean_evds_table("bronze_tcmb_kur")

# EVDS'den gelen sütun isimleri: Tarih, TP_DK_USD_A_YTL, TP_DK_EUR_A_YTL, TP_DK_GBP_A_YTL
# (nokta yerine alt çizgi kullanılıyor olabilir, dinamik olarak ele alalım)

# Tarih sütununu bul ve dönüştür
date_col = [c for c in df_kur_raw.columns if "tarih" in c.lower() or "date" in c.lower()]
date_col_name = date_col[0] if date_col else "Tarih"

# Değer sütunlarını bul (tarih, metadata ve source sütunları hariç)
meta_cols = {"ingestion_timestamp", "source", "series_key", date_col_name, "YEARWEEK", "yearweek", "Tarih", "UNIXTIME", "unixtime"}
value_cols = [c for c in df_kur_raw.columns if c not in meta_cols and not c.upper().startswith("UNIX")]

print(f"   Tarih sütunu: {date_col_name}")
print(f"   Değer sütunları: {value_cols}")

# %%
# Kur verisini unpivot et (wide → long format)
# Her kur değeri ayrı bir satır olsun: date, currency_pair, rate
from pyspark.sql.functions import expr

# Stack ifadesi oluştur
stack_expr_parts = []
for vc in value_cols:
    # Sütun adından para birimi çıkar (ör: TP_DK_USD_A_YTL → USD/TRY)
    currency = vc.replace("TP_DK_", "").replace("TP.DK.", "")
    currency = currency.replace("_A_YTL", "").replace(".A.YTL", "")
    currency = currency.replace("_", "")
    pair_name = f"{currency}/TRY"
    stack_expr_parts.append(f"'{pair_name}', cast(`{vc}` as string)")

stack_count = len(stack_expr_parts)
stack_expr = f"stack({stack_count}, {', '.join(stack_expr_parts)}) as (currency_pair, rate)"

df_kur_long = df_kur_raw.select(
    col(date_col_name).alias("date_str"),
    expr(stack_expr)
)

# Tarih ve değer dönüşümü
df_kur_clean = df_kur_long \
    .withColumn("date", to_date(col("date_str"), "dd-MM-yyyy")) \
    .withColumn("date", coalesce(col("date"), to_date(col("date_str"), "yyyy-MM-dd"))) \
    .withColumn("rate", col("rate").cast(DoubleType())) \
    .filter(col("date").isNotNull() & col("rate").isNotNull() & (col("rate") > 0)) \
    .drop("date_str")

# Tekilleştirme
window_kur = Window.partitionBy("date", "currency_pair").orderBy(col("date").desc())
df_kur_clean = df_kur_clean \
    .withColumn("row_num", row_number().over(window_kur)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

# %%
# Faiz verisini temizle
print("\n--- FAİZ ---")
df_faiz_raw = clean_evds_table("bronze_tcmb_faiz")

faiz_date_col = [c for c in df_faiz_raw.columns if "tarih" in c.lower() or "date" in c.lower()]
faiz_date_name = faiz_date_col[0] if faiz_date_col else "Tarih"

faiz_meta = {"ingestion_timestamp", "source", "series_key", faiz_date_name, "YEARWEEK", "yearweek", "Tarih", "UNIXTIME", "unixtime"}
faiz_value_cols = [c for c in df_faiz_raw.columns if c not in faiz_meta and not c.upper().startswith("UNIX")]

df_faiz_clean = df_faiz_raw.select(
    col(faiz_date_name).alias("date_str"),
    col(faiz_value_cols[0]).alias("policy_rate") if faiz_value_cols else lit(None).alias("policy_rate")
) \
    .withColumn("date", to_date(col("date_str"), "dd-MM-yyyy")) \
    .withColumn("date", coalesce(col("date"), to_date(col("date_str"), "yyyy-MM-dd"))) \
    .withColumn("policy_rate", col("policy_rate").cast(DoubleType())) \
    .filter(col("date").isNotNull() & col("policy_rate").isNotNull()) \
    .drop("date_str")

# %%
# Enflasyon verisini temizle
print("\n--- ENFLASYON ---")
df_enf_raw = clean_evds_table("bronze_tcmb_enflasyon")

enf_date_col = [c for c in df_enf_raw.columns if "tarih" in c.lower() or "date" in c.lower()]
enf_date_name = enf_date_col[0] if enf_date_col else "Tarih"

enf_meta = {"ingestion_timestamp", "source", "series_key", enf_date_name, "YEARWEEK", "yearweek", "Tarih", "UNIXTIME", "unixtime"}
enf_value_cols = [c for c in df_enf_raw.columns if c not in enf_meta and not c.upper().startswith("UNIX")]

w_enf_order = Window.orderBy("date")

df_enf_clean = df_enf_raw.select(
    col(enf_date_name).alias("date_str"),
    col(enf_value_cols[0]).cast(DoubleType()).alias("cpi_index") if enf_value_cols else lit(None).cast(DoubleType()).alias("cpi_index")
) \
    .withColumn("date", coalesce(
        to_date(col("date_str"), "dd-MM-yyyy"),
        to_date(col("date_str"), "yyyy-MM-dd"),
        to_date(concat(col("date_str"), lit("-01")), "yyyy-M-d"),
        to_date(concat(col("date_str"), lit("-01")), "yyyy-MM-dd")
    )) \
    .filter(col("date").isNotNull() & col("cpi_index").isNotNull()) \
    .drop("date_str") \
    .dropDuplicates(["date"]) \
    .orderBy("date")

# Yıllık TÜFE Enflasyon Oranı (%) = ((Bu Ayki Endeks - 12 Ay Önceki Endeks) / 12 Ay Önceki Endeks) * 100
df_enf_clean = df_enf_clean \
    .withColumn("cpi_index_12m", lag("cpi_index", 12).over(w_enf_order)) \
    .withColumn("cpi_annual_change",
        when(col("cpi_index_12m") > 0,
            spark_round(((col("cpi_index") - col("cpi_index_12m")) / col("cpi_index_12m")) * 100, 2)
        ).otherwise(
            spark_round(((col("cpi_index") - lag("cpi_index", 1).over(w_enf_order)) / lag("cpi_index", 1).over(w_enf_order)) * 12 * 100, 2)
        )
    ) \
    .select("date", "cpi_annual_change")

# %% [markdown]
# ## 4. Makro Verileri Birleştir → silver_makro

# %%
# Kur, faiz ve enflasyon verilerini tarih bazında birleştir
# Kur verisini pivot et (long → wide): her para birimi ayrı sütun

from pyspark.sql.functions import first, last
from pyspark.sql.window import Window

df_kur_pivot = df_kur_clean.groupBy("date").pivot("currency_pair").agg(
    first("rate")
)

# Faiz ve enflasyon ile join
df_makro = df_kur_pivot \
    .join(df_faiz_clean, on="date", how="outer") \
    .join(df_enf_clean, on="date", how="outer") \
    .orderBy("date")

# Sütun isimlerini temizle (özel karakterleri kaldır)
for old_name in df_makro.columns:
    new_name = old_name.lower().replace("/", "_").replace(" ", "_").replace(".", "_")
    if old_name != new_name:
        df_makro = df_makro.withColumnRenamed(old_name, new_name)

# Enflasyon ve Politika Faizini ileriye doğru doldur (Forward-fill / LOCF)
# Aylık/aralıklı verileri sonraki duyuruya kadar günlük geçerli kılar
w_fill = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
if "cpi_annual_change" in df_makro.columns:
    df_makro = df_makro.withColumn("cpi_annual_change", last("cpi_annual_change", ignorenulls=True).over(w_fill))
if "policy_rate" in df_makro.columns:
    df_makro = df_makro.withColumn("policy_rate", last("policy_rate", ignorenulls=True).over(w_fill))

# Metadata ekle
df_makro = df_makro.withColumn("silver_timestamp", current_timestamp())

# Silver tablosuna yaz (İlk Yükleme vs MERGE INTO)
if not spark.catalog.tableExists("silver_makro"):
    print("📦 silver_makro tablosu oluşturuluyor (İlk Yükleme)...")
    df_makro.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("silver_makro")
else:
    print("🔄 silver_makro tablosuna MERGE INTO yapılıyor (Artımlı Yükleme)...")
    dt_makro = DeltaTable.forName(spark, "silver_makro")
    (
        dt_makro.alias("target")
        .merge(
            df_makro.alias("source"),
            "target.date = source.date"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

count = spark.table("silver_makro").count()
print(f"\n✅ silver_makro güncellendi — Toplam: {count} satır")
print("\n📋 Örnek veriler:")
spark.table("silver_makro").orderBy(col("date").desc()).show(10, truncate=False)

# %% [markdown]
# ## 5. Doğrulama

# %%
print("\n" + "=" * 60)
print("✅ SILVER KATMANI DOĞRULAMA")
print("=" * 60)

silver_tables = ["silver_borsa", "silver_makro", "silver_data_quality_log"]

for table in silver_tables:
    try:
        count = spark.table(table).count()
        cols = len(spark.table(table).columns)
        print(f"  📦 {table}: {count} satır, {cols} sütun")
        assert count > 0, f"{table} boş!"
    except Exception as e:
        print(f"  ❌ {table}: HATA — {e}")

print("\n📋 Son Veri Kalite Testi Özeti:")
spark.table("silver_data_quality_log").orderBy(col("check_timestamp").desc()).show(6, truncate=False)

print("\n🎉 Silver katmanı ve Veri Kalite Kapısı başarıyla çalıştı!")
