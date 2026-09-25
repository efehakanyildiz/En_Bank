# -*- coding: utf-8 -*-
"""
Proje Konfigürasyonu
====================
Tüm notebook'lar bu dosyadan ortak ayarları okur.
Fabric Notebook'a kopyalarken bu değerleri doğrudan kullanabilirsiniz.
"""

# ==============================================================================
# 1. BORSA (yfinance) AYARLARI
# ==============================================================================

# BIST'ten takip edilecek 15 şirket (Şirket Müşterileri Entegre Edildi)
TICKERS = [
    "THYAO.IS",   # Türk Hava Yolları
    "GARAN.IS",   # Garanti BBVA
    "AKBNK.IS",   # Akbank
    "SISE.IS",    # Şişecam
    "EREGL.IS",   # Ereğli Demir Çelik
    "KCHOL.IS",   # Koç Holding
    "ASELS.IS",   # ASELSAN
    "BIMAS.IS",   # BİM
    "TUPRS.IS",   # Tüpraş
    "SAHOL.IS",   # Sabancı Holding
    # Firmanın Çalıştığı Müşteri Şirketleri
    "CCOLA.IS",   # Coca-Cola İçecek
    "MAVI.IS",    # Mavi Giyim
    "AKGRT.IS",   # Aksigorta
    "GEDIK.IS",   # Gedik Yatırım
    "ECILC.IS",   # Eczacıbaşı İlaç
]

# Şirket bilgileri (dashboard görsellerinde kullanılır)
TICKER_INFO = {
    "THYAO.IS": {"name": "Türk Hava Yolları",   "sector": "Havacılık"},
    "GARAN.IS": {"name": "Garanti BBVA",        "sector": "Bankacılık"},
    "AKBNK.IS": {"name": "Akbank",              "sector": "Bankacılık"},
    "SISE.IS":  {"name": "Şişecam",             "sector": "Cam/Kimya"},
    "EREGL.IS": {"name": "Ereğli Demir Çelik",  "sector": "Demir/Çelik"},
    "KCHOL.IS": {"name": "Koç Holding",         "sector": "Holding"},
    "ASELS.IS": {"name": "ASELSAN",             "sector": "Savunma"},
    "BIMAS.IS": {"name": "BİM",                "sector": "Perakende"},
    "TUPRS.IS": {"name": "Tüpraş",             "sector": "Enerji"},
    "SAHOL.IS": {"name": "Sabancı Holding",     "sector": "Holding"},
    "CCOLA.IS": {"name": "Coca-Cola İçecek",    "sector": "Hızlı Tüketim/İçecek"},
    "MAVI.IS":  {"name": "Mavi Giyim",          "sector": "Perakende/Moda"},
    "AKGRT.IS": {"name": "Aksigorta",           "sector": "Sigortacılık/Finans"},
    "GEDIK.IS": {"name": "Gedik Yatırım",       "sector": "Sermaye Piyasaları/Finans"},
    "ECILC.IS": {"name": "Eczacıbaşı İlaç",     "sector": "Sağlık/İlaç"},
}

# Tarihsel veri çekme periyodu
YFINANCE_PERIOD = "5y"       # Son 5 yıl
YFINANCE_INTERVAL = "1d"     # Günlük veri

# ==============================================================================
# 2. TCMB EVDS AYARLARI
# ==============================================================================

# API Key — Fabric'te Key Vault'tan çekilmeli, burada placeholder
EVDS_API_KEY = "wPttLJcMae"

# EVDS3 API endpoint (evds2 Ocak 2026'da emekli oldu)
EVDS_BASE_URL = "https://evds3.tcmb.gov.tr/igmevdsms-dis//"

# EVDS Seri Kodları
EVDS_SERIES = {
    # Döviz Kurları
    "kur": {
        "series": "TP.DK.USD.A.YTL-TP.DK.EUR.A.YTL-TP.DK.GBP.A.YTL",
        "table": "bronze_tcmb_kur",
        "description": "USD/TRY, EUR/TRY, GBP/TRY Döviz Kurları",
    },
    # Faiz Oranları (TCMB Ağırlıklı Ortalama Fonlama Maliyeti / AOFM)
    "faiz": {
        "series": "TP.APIFON4",
        "table": "bronze_tcmb_faiz",
        "description": "TCMB Ağırlıklı Ortalama Fonlama Maliyeti (Faiz)",
    },
    # Enflasyon (TÜFE)
    "enflasyon": {
        "series": "TP.FG.J0",
        "table": "bronze_tcmb_enflasyon",
        "description": "Tüketici Fiyat Endeksi (TÜFE) — Yıllık % Değişim",
    },
}

# Tarih aralığı (DD-MM-YYYY formatında, EVDS bunu bekler)
EVDS_START_DATE = "01-01-2020"
EVDS_END_DATE = ""  # Boş bırakılırsa bugünün tarihini kullanır

# ==============================================================================
# 3. LAKEHOUSE AYARLARI
# ==============================================================================

# Lakehouse adı (Fabric'te oluşturacağınız Lakehouse ile aynı olmalı)
LAKEHOUSE_NAME = "finans_lakehouse"

# Tablo isimleri
TABLES = {
    # Bronze
    "bronze_yfinance":       "bronze_yfinance",
    "bronze_tcmb_kur":       "bronze_tcmb_kur",
    "bronze_tcmb_faiz":      "bronze_tcmb_faiz",
    "bronze_tcmb_enflasyon": "bronze_tcmb_enflasyon",
    # Silver
    "silver_borsa":          "silver_borsa",
    "silver_makro":          "silver_makro",
    # Gold
    "gold_teknik_analiz":    "gold_teknik_analiz",
    "gold_kpi_ozet":         "gold_kpi_ozet",
    "gold_getiri_analizi":   "gold_getiri_analizi",
    "gold_makro_dashboard":  "gold_makro_dashboard",
}

# ==============================================================================
# 4. TEKNİK ANALİZ PARAMETRELERİ
# ==============================================================================

# Hareketli Ortalama pencereleri (gün)
MA_WINDOWS = [7, 20, 50, 200]

# RSI penceresi
RSI_PERIOD = 14

# Bollinger Bantları
BOLLINGER_WINDOW = 20
BOLLINGER_STD = 2

# ==============================================================================
# 5. PIPELINE AYARLARI
# ==============================================================================

# Pipeline zamanlaması (bilgi amaçlı — asıl zamanlama Fabric Data Factory'de yapılır)
PIPELINE_SCHEDULE = "Her gün saat 19:00 (TSİ)"
PIPELINE_TIMEZONE = "Europe/Istanbul"
