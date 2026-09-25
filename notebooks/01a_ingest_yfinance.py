# Fabric Notebook — 01a: Borsa Veri Çekimi (yfinance) → Bronze Katmanı
# ================================================================
# Bu notebook, BIST hisse senetlerinin OHLCV verilerini artımlı (CDC)
# olarak çeker ve Delta Lake MERGE INTO ile 'bronze_yfinance' tablosuna yazar.
#
# Pipeline'da: 01b_ingest_tcmb ile PARALEL çalışır.
# ================================================================

# %% [markdown]
# ## 1. Kütüphaneler & Kurulum

# %%
import sys
import subprocess
from datetime import datetime, timedelta

try:
    import yfinance as yf
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "yfinance", "--quiet"])
    import yfinance as yf

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, LongType
)
from pyspark.sql.functions import lit, current_timestamp, max as spark_max
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

# %% [markdown]
# ## 2. Konfigürasyon

# %%
TICKERS = [
    "THYAO.IS", "GARAN.IS", "AKBNK.IS", "SISE.IS", "EREGL.IS",
    "KCHOL.IS", "ASELS.IS", "BIMAS.IS", "TUPRS.IS", "SAHOL.IS",
    "CCOLA.IS", "MAVI.IS", "AKGRT.IS", "GEDIK.IS", "ECILC.IS",
]

TICKER_NAMES = {
    "THYAO.IS": "Türk Hava Yolları",
    "GARAN.IS": "Garanti BBVA",
    "AKBNK.IS": "Akbank",
    "SISE.IS":  "Şişecam",
    "EREGL.IS": "Ereğli Demir Çelik",
    "KCHOL.IS": "Koç Holding",
    "ASELS.IS": "ASELSAN",
    "BIMAS.IS": "BİM",
    "TUPRS.IS": "Tüpraş",
    "SAHOL.IS": "Sabancı Holding",
    "CCOLA.IS": "Coca-Cola İçecek",
    "MAVI.IS":  "Mavi Giyim",
    "AKGRT.IS": "Aksigorta",
    "GEDIK.IS": "Gedik Yatırım",
    "ECILC.IS": "Eczacıbaşı İlaç",
}

YFINANCE_PERIOD = "5y"
YFINANCE_INTERVAL = "1d"

# %% [markdown]
# ## 3. Artımlı Tarih Tespiti (Watermarking)

# %%
def get_ticker_start_date(ticker: str) -> str:
    """
    Her ticker için ayrı ayrı bronze_yfinance tablosunu kontrol eder.
    Ticker tabloda varsa ve 10'dan fazla satırı varsa son tarihten 5 gün öncesini döndürür (Artımlı).
    Ticker yeni eklenmişse veya az verisi varsa None döner (5 Yıllık Tam Yükleme).
    """
    try:
        if spark.catalog.tableExists("bronze_yfinance"):
            df_t = spark.table("bronze_yfinance").filter(col("ticker") == ticker)
            if df_t.count() > 10:
                max_dt = df_t.select(spark_max("date")).collect()[0][0]
                if max_dt:
                    if isinstance(max_dt, str):
                        max_dt = datetime.strptime(str(max_dt)[:10], "%Y-%m-%d")
                    start_dt = max_dt - timedelta(days=5)
                    return start_dt.strftime("%Y-%m-%d")
    except Exception as e:
        print(f"⚠️ {ticker} son tarih kontrolü uyarısı: {e}")
    return None

def fetch_yfinance_data(tickers: list, period: str = "5y", interval: str = "1d") -> pd.DataFrame:
    all_data = []

    for ticker in tickers:
        try:
            ticker_start = get_ticker_start_date(ticker)
            if ticker_start:
                print(f"📥 {ticker} verisi çekiliyor (Artımlı: {ticker_start} itibarıyla)...")
                stock = yf.Ticker(ticker)
                df = stock.history(start=ticker_start, interval=interval)
            else:
                print(f"📥 {ticker} verisi çekiliyor (5 Yıllık Tam Yükleme)...")
                stock = yf.Ticker(ticker)
                df = stock.history(period=period, interval=interval)

            if df.empty:
                print(f"⚠️  {ticker} için yeni veri bulunamadı, atlanıyor.")
                continue

            df = df.reset_index()
            df["ticker"] = ticker
            df["company_name"] = TICKER_NAMES.get(ticker, ticker)

            # Sütun isimlerini standartlaştır
            df.columns = [c.lower().replace(" ", "_") for c in df.columns]

            cols = ["date", "ticker", "company_name", "open", "high", "low", "close", "volume"]
            available_cols = [c for c in cols if c in df.columns]
            df = df[available_cols]

            all_data.append(df)
            print(f"✅ {ticker}: {len(df)} satır çekildi.")

        except Exception as e:
            print(f"❌ {ticker} hatası: {e}")
            continue

    if not all_data:
        print("⚠️ Hiçbir ticker'dan yeni veri gelmedi (piyasa kapalı veya veriler güncel).")
        return pd.DataFrame()

    combined = pd.concat(all_data, ignore_index=True)
    if "date" in combined.columns:
        combined["date"] = pd.to_datetime(combined["date"]).dt.tz_localize(None)

    return combined

# %% [markdown]
# ## 4. Veri Çekimi ve MERGE INTO

# %%
print("=" * 60)
print("📊 BORSA VERİ ÇEKİMİ BAŞLIYOR (01a_ingest_yfinance)")
print("=" * 60)

yfinance_pdf = fetch_yfinance_data(TICKERS, period=YFINANCE_PERIOD, interval=YFINANCE_INTERVAL)

if not yfinance_pdf.empty:
    print(f"\n📊 Yeni çekilen satır sayısı: {len(yfinance_pdf)}")
    
    yfinance_schema = StructType([
        StructField("date", TimestampType(), True),
        StructField("ticker", StringType(), True),
        StructField("company_name", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
    ])

    yfinance_sdf = spark.createDataFrame(yfinance_pdf, schema=yfinance_schema)
    yfinance_sdf = yfinance_sdf.withColumn("ingestion_timestamp", current_timestamp())
    yfinance_sdf = yfinance_sdf.withColumn("source", lit("yfinance"))

    if not spark.catalog.tableExists("bronze_yfinance"):
        print("📦 bronze_yfinance tablosu oluşturuluyor (İlk Yükleme)...")
        yfinance_sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_yfinance")
    else:
        print("🔄 bronze_yfinance tablosuna MERGE INTO uygulanıyor...")
        dt_yf = DeltaTable.forName(spark, "bronze_yfinance")
        (
            dt_yf.alias("target")
            .merge(
                yfinance_sdf.alias("source"),
                "target.ticker = source.ticker AND target.date = source.date"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    row_count = spark.table("bronze_yfinance").count()
    print(f"✅ bronze_yfinance güncellendi — Toplam: {row_count} satır")
else:
    print("⚠️ Yeni satır bulunmadığından mevcut bronze_yfinance korundu.")

print("\n🎉 01a_ingest_yfinance başarıyla tamamlandı!")
