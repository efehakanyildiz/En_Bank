# Fabric Notebook — 01b: TCMB EVDS Veri Çekimi → Bronze Katmanı
# ================================================================
# Bu notebook, TCMB EVDS API'sinden Döviz Kurları (USD, EUR, GBP),
# Politika Fonlama Faizi (AOFM) ve TÜFE Enflasyon verilerini artımlı
# (CDC) çeker ve Delta Lake MERGE INTO ile Bronze tablolara yazar.
#
# Pipeline'da: 01a_ingest_yfinance ile PARALEL çalışır.
# ================================================================

# %% [markdown]
# ## 1. Kütüphaneler & Kurulum

# %%
import requests
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col, max as spark_max, to_date
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

# %% [markdown]
# ## 2. Konfigürasyon

# %%
EVDS_API_KEY = "wPttLJcMae"
EVDS_BASE_URL = "https://evds3.tcmb.gov.tr/igmevdsms-dis//"

EVDS_SERIES = {
    "kur": {
        "series": "TP.DK.USD.A.YTL-TP.DK.EUR.A.YTL-TP.DK.GBP.A.YTL",
        "table": "bronze_tcmb_kur",
    },
    "faiz": {
        "series": "TP.APIFON4",
        "table": "bronze_tcmb_faiz",
    },
    "enflasyon": {
        "series": "TP.FG.J0",
        "table": "bronze_tcmb_enflasyon",
    },
}

EVDS_DEFAULT_START = "01-01-2020"

# %% [markdown]
# ## 3. Artımlı Tarih Tespiti ve EVDS Çekim Fonksiyonları

# %%
def get_evds_start_date(table_name: str, default_start: str = "01-01-2020") -> str:
    """
    EVDS tablosundaki son tarihi kontrol eder.
    Tablo mevcut ve doluysa son tarihten 7 gün öncesini döndürür.
    """
    try:
        if spark.catalog.tableExists(table_name):
            cnt = spark.table(table_name).count()
            if cnt > 0:
                max_dt_row = spark.table(table_name) \
                    .select(spark_max(to_date(col("Tarih"), "dd-MM-yyyy"))) \
                    .collect()[0][0]
                if max_dt_row:
                    if isinstance(max_dt_row, str):
                        max_dt_row = datetime.strptime(max_dt_row[:10], "%Y-%m-%d").date()
                    start_dt = max_dt_row - timedelta(days=7)
                    return start_dt.strftime("%d-%m-%Y")
    except Exception as e:
        print(f"⚠️ {table_name} için son tarih okunamadı: {e}")
    return default_start

def fetch_evds_data(series_code: str, start_date: str, api_key: str) -> pd.DataFrame:
    today = datetime.now().strftime("%d-%m-%Y")
    headers = {"key": api_key}
    url = f"{EVDS_BASE_URL}series={series_code}&startDate={start_date}&endDate={today}&type=json"

    print(f"📥 EVDS serisi çekiliyor: {series_code}")
    print(f"   Aralık: {start_date} — {today}")

    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != 200:
            print(f"   ❌ HTTP Hatası: {response.status_code}")
            return pd.DataFrame()

        data = response.json()
        if isinstance(data, dict) and "items" in data and data["items"]:
            df = pd.DataFrame(data["items"])
            if "UNIXTIME" in df.columns:
                df = df.drop(columns=["UNIXTIME"])
            print(f"   ✅ {len(df)} satır çekildi!")
            return df
        elif isinstance(data, list) and len(data) > 0:
            df = pd.DataFrame(data)
            if "UNIXTIME" in df.columns:
                df = df.drop(columns=["UNIXTIME"])
            print(f"   ✅ {len(df)} satır çekildi!")
            return df
        else:
            print(f"   ⚠️ Veri bulunamadı veya boş liste döndü.")
            return pd.DataFrame()
    except Exception as e:
        print(f"   ❌ Bağlantı hatası: {e}")
        return pd.DataFrame()

def write_evds_to_bronze(df: pd.DataFrame, table_name: str, series_key: str):
    if df.empty:
        print(f"⚠️  {table_name} için yeni veri yok — atlanıyor.")
        return

    sdf = spark.createDataFrame(df)
    sdf = sdf.withColumn("ingestion_timestamp", current_timestamp())
    sdf = sdf.withColumn("source", lit("tcmb_evds"))
    sdf = sdf.withColumn("series_key", lit(series_key))

    if not spark.catalog.tableExists(table_name):
        print(f"📦 {table_name} tablosu oluşturuluyor (İlk Yükleme)...")
        sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    else:
        print(f"🔄 {table_name} tablosuna MERGE INTO uygulanıyor...")
        dt_evds = DeltaTable.forName(spark, table_name)
        (
            dt_evds.alias("target")
            .merge(
                sdf.alias("source"),
                "target.Tarih = source.Tarih"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    row_count = spark.table(table_name).count()
    print(f"✅ {table_name} güncellendi — Toplam: {row_count} satır")

# %% [markdown]
# ## 4. Tüm EVDS Serilerini Çek ve Bronze'a Yaz

# %%
print("=" * 60)
print("🏛️  TCMB EVDS VERİ ÇEKİMİ BAŞLIYOR (01b_ingest_tcmb)")
print("=" * 60)

for key, config in EVDS_SERIES.items():
    tbl = config["table"]
    evds_start = get_evds_start_date(tbl, EVDS_DEFAULT_START)
    print(f"\n--- {key.upper()} (Başlangıç: {evds_start}) ---")
    evds_df = fetch_evds_data(
        series_code=config["series"],
        start_date=evds_start,
        api_key=EVDS_API_KEY,
    )
    write_evds_to_bronze(evds_df, tbl, key)

print("\n🎉 01b_ingest_tcmb başarıyla tamamlandı!")
