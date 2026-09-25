-- ================================================================
-- Microsoft Fabric SQL Analytics Endpoint — Ad-hoc Analiz Sorguları
-- ================================================================
-- Bu sorgular, veri analistleri ve portföy yöneticilerinin
-- hızlı kararlar alması için hazırlanmış gelişmiş T-SQL sorgularıdır.
-- ================================================================

-- 1. SORGU: Son 30 Günde En Yüksek Sharpe Oranına Sahip İlk 5 Şirket
SELECT TOP 5
    ticker,
    company_name,
    annualized_return_pct AS [Yillik_Getiri_%],
    annualized_volatility_pct AS [Volatilite_%],
    sharpe_ratio AS [Sharpe_Rasyosu]
FROM gold_getiri_ozet
ORDER BY sharpe_ratio DESC;

-- 2. SORGU: Son 1 Yılda Dolar Bazlı En Çok Kazandıran Şirketler (Window & Join)
WITH SonBorsa AS (
    SELECT 
        ticker,
        company_name,
        [date],
        [close],
        FIRST_VALUE([close]) OVER (PARTITION BY ticker ORDER BY [date] ASC) as ilk_fiyat,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY [date] DESC) as rn
    FROM gold_teknik_analiz
    WHERE [date] >= DATEADD(year, -1, GETDATE())
),
SonDolar AS (
    SELECT 
        TRY_USD,
        FIRST_VALUE(TRY_USD) OVER (ORDER BY [date] ASC) as ilk_dolar,
        ROW_NUMBER() OVER (ORDER BY [date] DESC) as rn
    FROM gold_makro_dashboard
    WHERE [date] >= DATEADD(year, -1, GETDATE())
)
SELECT 
    b.ticker AS Ticker,
    b.company_name AS Sirket,
    ROUND(((b.[close] - b.ilk_fiyat) / b.ilk_fiyat) * 100, 2) AS [TL_Bazli_1Y_Getiri_%],
    ROUND(((d.TRY_USD - d.ilk_dolar) / d.ilk_dolar) * 100, 2) AS [USD_TRY_1Y_Artis_%],
    ROUND((((b.[close] / d.TRY_USD) - (b.ilk_fiyat / d.ilk_dolar)) / (b.ilk_fiyat / d.ilk_dolar)) * 100, 2) AS [Dolar_Bazli_Net_Getiri_%]
FROM SonBorsa b
CROSS JOIN (SELECT TOP 1 * FROM SonDolar WHERE rn = 1) d
WHERE b.rn = 1
ORDER BY [Dolar_Bazli_Net_Getiri_%] DESC;

-- 3. SORGU: Günlük Alım Fırsatları (RSI < 40 ve SMA 50 > SMA 200)
SELECT 
    ticker,
    company_name,
    [close] AS Fiyat,
    rsi AS RSI_Gostergesi,
    signal AS Trend_Sinyali,
    [date] AS Sinyal_Tarihi
FROM gold_teknik_analiz
WHERE [date] = (SELECT MAX([date]) FROM gold_teknik_analiz)
  AND rsi < 40
  AND signal = 'GOLDEN_CROSS'
ORDER BY rsi ASC;
