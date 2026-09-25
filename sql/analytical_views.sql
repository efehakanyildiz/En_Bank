-- ================================================================
-- Microsoft Fabric SQL Analytics Endpoint — Analitik SQL Görünümleri
-- ================================================================
-- Bu script, Fabric Lakehouse üzerindeki Gold tablolarını kullanarak
-- Power BI ve analistlerin tüketimine hazır T-SQL View'ları oluşturur.
--
-- Çalıştırma: Fabric Lakehouse -> Sağ üstten "SQL analytics endpoint"
--             seçip New SQL Query penceresinde çalıştırın.
-- ================================================================

-- ----------------------------------------------------------------
-- 1. VIEW: BIST Hisseleri vs Enflasyon & Dolar (Reel Getiri Analizi)
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_bist_reel_getiri AS
WITH SonGetiri AS (
    SELECT 
        ticker,
        company_name,
        annualized_return_pct,
        annualized_volatility_pct,
        sharpe_ratio
    FROM gold_getiri_ozet
),
SonMakro AS (
    SELECT TOP 1
        cpi_annual_change AS enflasyon_tfe_yillik,
        policy_rate AS tcmb_politika_faizi
    FROM gold_makro_dashboard
    WHERE cpi_annual_change IS NOT NULL
    ORDER BY [date] DESC
)
SELECT 
    g.ticker AS [Ticker],
    g.company_name AS [Şirket],
    g.annualized_return_pct AS [Hisse_Yıllık_Nominal_Getiri_%],
    m.enflasyon_tfe_yillik AS [Yıllık_TÜFE_Enflasyon_%],
    -- Fisher Denklemi: (1 + Nominal) / (1 + Enflasyon) - 1
    ROUND(((1.0 + (g.annualized_return_pct / 100.0)) / (1.0 + (m.enflasyon_tfe_yillik / 100.0)) - 1.0) * 100.0, 2) AS [Enflasyona_Göre_Reel_Getiri_%],
    m.tcmb_politika_faizi AS [TCMB_Politika_Faizi_%],
    -- Risksiz Faize Göre Fazla Getiri (Excess Return over Risk-Free Rate)
    ROUND(g.annualized_return_pct - m.tcmb_politika_faizi, 2) AS [Faiz_Üstü_Fazla_Getiri_%],
    g.annualized_volatility_pct AS [Yıllık_Volatilite_%],
    g.sharpe_ratio AS [Sharpe_Rasyosu],
    -- 3 Kademeli Değerlendirme (Reel Getiri + Sharpe Skoru)
    CASE 
        WHEN ((1.0 + (g.annualized_return_pct / 100.0)) / (1.0 + (m.enflasyon_tfe_yillik / 100.0)) - 1.0) * 100.0 > 15.0 AND g.sharpe_ratio > 0.3 THEN 'Enflasyon Üstü Güçlü Getiri'
        WHEN ((1.0 + (g.annualized_return_pct / 100.0)) / (1.0 + (m.enflasyon_tfe_yillik / 100.0)) - 1.0) * 100.0 > 0.0 THEN 'Enflasyon Üstü Sınırlı Getiri'
        ELSE 'Enflasyon Altı Negatif Getiri'
    END AS [Reel_Getiri_Durumu]
FROM SonGetiri g
CROSS JOIN SonMakro m;
GO

-- ----------------------------------------------------------------
-- 2. VIEW: Teknik Alım / Satım & Alarm Sinyalleri
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_teknik_alim_sinyalleri AS
WITH EnSonTeknik AS (
    SELECT 
        [date],
        ticker,
        company_name,
        [close],
        daily_change_pct,
        rsi,
        sma_50,
        sma_200,
        bb_lower,
        bb_upper,
        signal,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY [date] DESC) as rn
    FROM gold_teknik_analiz
)
SELECT 
    [date] AS [Tarih],
    ticker AS [Ticker],
    company_name AS [Sirket],
    [close] AS [Kapanis_Fiyati],
    daily_change_pct AS [Gunluk_Degisim_%],
    rsi AS [RSI_14],
    sma_50 AS [SMA_50],
    sma_200 AS [SMA_200],
    signal AS [Trend_Sinyali],
    CASE 
        WHEN rsi < 35 THEN 'AŞIRI SATIM (Alım Fırsatı)'
        WHEN rsi > 70 THEN 'AŞIRI ALIM (Kar Satışı Düşün)'
        ELSE 'NÖTR'
    END AS [RSI_Durumu],
    CASE 
        WHEN [close] <= bb_lower THEN 'Bollinger Alt Bandı (Dip Seviye)'
        WHEN [close] >= bb_upper THEN 'Bollinger Üst Bandı (Zirve Seviye)'
        ELSE 'Kanal İçi'
    END AS [Bollinger_Durumu]
FROM EnSonTeknik
WHERE rn = 1;
GO

-- ----------------------------------------------------------------
-- 3. VIEW: Sektör Bazlı Performans ve Ağırlık Kıyaslaması
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_sektor_performans AS
WITH SektorAtama AS (
    SELECT 
        ticker,
        company_name,
        daily_change_pct,
        weekly_change_pct,
        monthly_change_pct,
        CASE 
            WHEN ticker IN ('GARAN.IS', 'AKBNK.IS') THEN 'Bankacılık'
            WHEN ticker IN ('AKGRT.IS', 'GEDIK.IS') THEN 'Finans & Sigorta & Yatırım'
            WHEN ticker IN ('KCHOL.IS', 'SAHOL.IS') THEN 'Holding'
            WHEN ticker IN ('THYAO.IS') THEN 'Ulaştırma & Havacılık'
            WHEN ticker IN ('EREGL.IS', 'SISE.IS', 'TUPRS.IS') THEN 'Sanayi & İmalat'
            WHEN ticker IN ('BIMAS.IS', 'MAVI.IS') THEN 'Perakende & Moda'
            WHEN ticker IN ('CCOLA.IS') THEN 'Hızlı Tüketim & İçecek'
            WHEN ticker IN ('ECILC.IS') THEN 'Sağlık & İlaç'
            WHEN ticker IN ('ASELS.IS') THEN 'Savunma Sanayii'
            ELSE 'Diğer'
        END AS Sektor
    FROM gold_kpi_ozet
)
SELECT 
    Sektor,
    COUNT(ticker) AS [Sirket_Sayisi],
    ROUND(AVG(daily_change_pct), 2) AS [Sektor_Gunluk_Ortalama_Getiri_%],
    ROUND(AVG(weekly_change_pct), 2) AS [Sektor_Haftalik_Ortalama_Getiri_%],
    ROUND(AVG(monthly_change_pct), 2) AS [Sektor_Aylik_Ortalama_Getiri_%],
    STRING_AGG(ticker, ', ') AS [Sektordeki_Hisseler]
FROM SektorAtama
GROUP BY Sektor;
GO

-- ----------------------------------------------------------------
-- 4. VIEW: Veri Kalite & Yönetişim Sağlık Özeti (Data Governance)
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_veri_kalite_ozeti AS
WITH SonTestler AS (
    SELECT 
        check_id,
        check_name,
        table_name,
        category,
        [status],
        failed_records,
        threshold,
        details,
        check_timestamp,
        CAST(check_timestamp AS DATE) AS test_tarihi,
        DENSE_RANK() OVER (ORDER BY check_timestamp DESC) as test_run_id
    FROM silver_data_quality_log
)
SELECT 
    check_id AS [Test_ID],
    check_name AS [Test_Adi],
    table_name AS [Hedef_Tablo],
    category AS [Kategori],
    [status] AS [Durum],
    failed_records AS [Hatali_Kayit_Sayisi],
    threshold AS [Kabul_Edilebilir_Esik],
    details AS [Detaylar],
    check_timestamp AS [Son_Calisma_Zamani]
FROM SonTestler
WHERE test_run_id = 1;
GO

-- ----------------------------------------------------------------
-- 5. VIEW: Bankacılık Faiz Riski & Likidite Göstergeleri (ALM / IRRBB)
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_bankacilik_risk_ve_faiz_duyarliligi AS
SELECT 
    ticker AS [Ticker],
    company_name AS [Sirket],
    sector AS [Sektor],
    interest_rate_beta AS [Faiz_Duyarlilik_Betasi],
    interest_rate_risk_profile AS [Faiz_Risk_Profili],
    avg_daily_volume_mio_tl AS [Gunluk_Ortalama_Hacim_Milyon_TL],
    daily_volatility_pct AS [Gunluk_Volatilite_%],
    market_liquidity_ratio AS [Piyasa_Likidite_Rasyosu],
    liquidity_risk_level AS [Likidite_Risk_Seviyesi],
    avg_daily_return_pct AS [Ortalama_Gunluk_Getiri_%]
FROM gold_bankacilik_risk_analizi;
GO


