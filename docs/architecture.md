# 🏗️ Mimari Dokümanı — Canlı Finans Data Pipeline

## Genel Bakış

Bu proje, Microsoft Fabric platformu üzerinde çalışan, uçtan uca otomatik bir
finans veri pipeline'ıdır. Medallion mimarisi kullanılarak veri kalitesi katman
katman artırılır ve Power BI ile canlı bir dashboard sunulur.

## Veri Akışı Diyagramı

```
  ┌─────────────┐     ┌─────────────┐
  │  yfinance   │     │ TCMB EVDS   │
  │  (Borsa)    │     │ (Makro)     │
  └──────┬──────┘     └──────┬──────┘
         │                   │
         ▼                   ▼
  ┌──────────────────────────────────┐
  │     01_api_to_bronze.py          │
  │     Notebook 1: Veri Çekimi      │
  └──────────────┬───────────────────┘
                 │
                 ▼
  ┌──────────────────────────────────┐
  │  BRONZE (Delta Tables)           │
  │  ├── bronze_yfinance             │
  │  ├── bronze_tcmb_kur             │
  │  ├── bronze_tcmb_faiz            │
  │  └── bronze_tcmb_enflasyon       │
  └──────────────┬───────────────────┘
                 │
                 ▼
  ┌──────────────────────────────────┐
  │     02_bronze_to_silver.py       │
  │     Notebook 2: Temizleme        │
  └──────────────┬───────────────────┘
                 │
                 ▼
  ┌──────────────────────────────────┐
  │  SILVER (Delta Tables)           │
  │  ├── silver_borsa                │
  │  └── silver_makro                │
  └──────────────┬───────────────────┘
                 │
                 ▼
  ┌──────────────────────────────────┐
  │     03_silver_to_gold.py         │
  │     Notebook 3: Analiz           │
  └──────────────┬───────────────────┘
                 │
                 ▼
  ┌──────────────────────────────────┐
  │  GOLD (Delta Tables)             │
  │  ├── gold_teknik_analiz          │
  │  ├── gold_kpi_ozet               │
  │  ├── gold_getiri_analizi         │
  │  ├── gold_getiri_ozet            │
  │  └── gold_makro_dashboard        │
  └──────────────┬───────────────────┘
                 │
                 ▼
  ┌──────────────────────────────────┐
  │  POWER BI (DirectLake)           │
  │  └── Yönetici Dashboard          │
  └──────────────────────────────────┘
```

## Katman Detayları

### Bronze Katmanı (Ham Veri)
- **Amaç:** API'lerden gelen verileri olduğu gibi saklamak
- **Format:** Delta Lake
- **Strateji:** Her pipeline çalışmasında `overwrite` — tüm tarihsel veriyi yeniden çeker
- **Metadata:** `ingestion_timestamp`, `source` sütunları eklenir

### Silver Katmanı (Temiz Veri)
- **Amaç:** Tip dönüşümleri, null handling, tekilleştirme, standardizasyon
- **İşlemler:**
  - Tarih string → DateType
  - Fiyat string → DoubleType
  - Aynı tarih+ticker'da son ingestion'ı al (dedup)
  - EVDS wide format → long format (unpivot)
  - Kur, faiz, enflasyon → `silver_makro` tablosunda birleştirilir
  - Günlük değişim hesaplanır

### Gold Katmanı (İş Verisi)
- **Amaç:** Doğrudan Power BI'da kullanılacak analitik tablolar
- **İçerik:**
  - **Teknik Analiz:** SMA(7/20/50/200), RSI(14), Bollinger(20,2σ), Golden/Death Cross
  - **KPI Özet:** Son günün günlük/haftalık/aylık performansı
  - **Getiri Analizi:** Kümülatif getiri, yıllık getiri, Sharpe oranı, volatilite
  - **Makro Dashboard:** Kur trend, faiz değişim, enflasyon

## Zamanlama & Orkestrasyon

```
Pipeline: pipeline_gunluk_guncelleme
Tetikleme: Her gün saat 19:00 (TSİ) — BIST kapanışından sonra

Adım 1: 01_api_to_bronze     (~2-5 dk)
    ↓ (başarılıysa)
Adım 2: 02_bronze_to_silver   (~1-3 dk)
    ↓ (başarılıysa)
Adım 3: 03_silver_to_gold     (~2-5 dk)
    ↓ (başarılıysa)
Adım 4: Semantic Model Refresh (~1 dk)
```

**Toplam tahmini süre:** 6-14 dakika

## Power BI Entegrasyonu

- **Bağlantı modu:** DirectLake
- **Avantaj:** Veri kopyalanmaz, doğrudan OneLake'ten okunur
- **Güncelleme:** Delta tablo değiştiğinde otomatik algılanır (reframing)
- **Garanti:** Pipeline sonundaki Semantic Model Refresh adımı

## Güvenlik Notları

- EVDS API key'i notebook'a hardcode etmek yerine Key Vault kullanılmalı
- Fabric workspace'e sadece yetkili kullanıcılar erişmeli
- Power BI raporları paylaşırken RLS (Row Level Security) düşünülebilir
