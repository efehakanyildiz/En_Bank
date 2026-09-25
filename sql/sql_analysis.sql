-- Microsoft Fabric SQL Analytics Endpoint — 10 Temel İş Analitiği Sorgusu

-- 1. Enflasyonu Yenen Şampiyon Şirketler (15 şirket arasında enflasyonu en çok aşan ilk 5 şirket hangisi)
SELECT TOP 5
    Ticker,
    Şirket,
    [Hisse_Yıllık_Nominal_Getiri_%],
    [Yıllık_TÜFE_Enflasyon_%],
    [Enflasyona_Göre_Reel_Getiri_%],
    [Sharpe_Rasyosu],
    [Reel_Getiri_Durumu]
FROM vw_bist_reel_getiri
ORDER BY [Enflasyona_Göre_Reel_Getiri_%] DESC;


-- 2. 5 Özel Müşteri Şirketimizin Güncel Durum Karnesi (Danışmanlık müşterisi olan 5 şirketin son fiyat ve teknik sinyalleri nedir)
SELECT 
    Sirket AS [Müşteri_Şirket],
    Ticker,
    [Kapanis_Fiyati] AS [Son_Fiyat_TL],
    [Gunluk_Degisim_%],
    [RSI_14],
    [Trend_Sinyali],
    [Bollinger_Durumu]
FROM vw_teknik_alim_sinyalleri
WHERE Sirket IN ('Coca-Cola İçecek', 'Mavi Giyim', 'Eczacıbaşı İlaç', 'Aksigorta', 'Gedik Yatırım')
ORDER BY [Gunluk_Degisim_%] DESC;


-- 3. Kelepir ve Potansiyel Alım Fırsatı Veren Hisseler (RSI göstergesi 35 altında olan aşırı satılmış hisseler hangileri)
SELECT 
    Ticker,
    Sirket,
    [Kapanis_Fiyati] AS [Güncel_Fiyat_TL],
    [RSI_14],
    [RSI_Durumu],
    [Trend_Sinyali]
FROM vw_teknik_alim_sinyalleri
WHERE [RSI_14] < 35
ORDER BY [RSI_14] ASC;


-- 4. Şirketlerin Risk Kademelerine Göre Dağılımı (Hisselerden hangileri defansif sakin, hangileri agresif oynak)
SELECT 
    Ticker,
    company_name AS [Şirket],
    annualized_volatility_pct AS [Yıllık_Volatilite_%],
    CASE 
        WHEN annualized_volatility_pct < 45 THEN 'Düşük Risk (Defansif)'
        WHEN annualized_volatility_pct BETWEEN 45 AND 55 THEN 'Dengeli / Orta Risk'
        ELSE 'Yüksek Risk (Agresif Oynak)'
    END AS [Risk_Sınıfı],
    annualized_return_pct AS [Yıllık_Nominal_Getiri_%]
FROM gold_getiri_ozet
ORDER BY annualized_volatility_pct ASC;


-- 5. Sektör Bazında Ortalama Getiri ve Hacim Liderliği (BIST'te hangi sektörler daha karlı ve piyasa likiditesinin ne kadarına sahip)
SELECT 
    Sektor,
    COUNT(DISTINCT Sirket) AS [Şirket_Sayısı],
    ROUND(AVG(Ortalama_Gunluk_Getiri_%), 2) AS [Ortalama_Günlük_Getiri_%],
    ROUND(SUM(Gunluk_Ortalama_Hacim_Milyon_TL), 1) AS [Toplam_Günlük_Hacim_Milyon_TL]
FROM vw_bankacilik_risk_ve_faiz_duyarliligi
GROUP BY Sektor
ORDER BY [Toplam_Günlük_Hacim_Milyon_TL] DESC;


-- 6. TCMB Politika Faizi vs Enflasyon Makası (Merkez Bankası faizi ile yıllık enflasyon arasındaki reel fark son aylarda ne oldu)
SELECT TOP 10
    FORMAT([date], 'yyyy-MM') AS [Yıl_Ay],
    ROUND(AVG(policy_rate), 2) AS [Ortalama_Politika_Faizi_%],
    ROUND(AVG(cpi_annual_change), 2) AS [Ortalama_Yıllık_TÜFE_%],
    ROUND(AVG(policy_rate) - AVG(cpi_annual_change), 2) AS [Net_Reel_Faiz_Farkı]
FROM silver_makro
WHERE cpi_annual_change IS NOT NULL AND policy_rate IS NOT NULL
GROUP BY FORMAT([date], 'yyyy-MM')
ORDER BY [Yıl_Ay] DESC;


-- 7. Risksiz Faiz Üstü Ekstra Kazanç Sağlayanlar (TCMB politika faizini geçip yatırımcısına ekstra prim kazandıran şirketler kimler)
SELECT 
    Ticker,
    Şirket,
    [Hisse_Yıllık_Nominal_Getiri_%],
    [TCMB_Politika_Faizi_%],
    [Faiz_Üstü_Fazla_Getiri_%],
    [Sharpe_Rasyosu]
FROM vw_bist_reel_getiri
WHERE [Faiz_Üstü_Fazla_Getiri_%] > 0
ORDER BY [Faiz_Üstü_Fazla_Getiri_%] DESC;


-- 8. Piyasa Likidite Şampiyonları (Günlük işlem hacmi en yüksek ilk 5 hisse senedi hangisi)
SELECT TOP 5
    Ticker,
    Sirket,
    Sektor,
    ROUND(Gunluk_Ortalama_Hacim_Milyon_TL, 1) AS [Günlük_Ortalama_Hacim_Milyon_TL],
    Likidite_Risk_Seviyesi
FROM vw_bankacilik_risk_ve_faiz_duyarliligi
ORDER BY Gunluk_Ortalama_Hacim_Milyon_TL DESC;


-- 9. Çift Hareketli Ortalamanın Üzerindeki Güçlü Boğa Hisseleri (Hem 50 günlük hem 200 günlük ortalamasının üstündeki hisseler hangileri)
SELECT 
    Ticker,
    Sirket,
    [Kapanis_Fiyati],
    [SMA_50],
    [SMA_200],
    ROUND([Kapanis_Fiyati] - [SMA_50], 2) AS [SMA50_Mesafe],
    [Trend_Sinyali]
FROM vw_teknik_alim_sinyalleri
WHERE [Kapanis_Fiyati] > [SMA_50] AND [Kapanis_Fiyati] > [SMA_200]
ORDER BY [SMA50_Mesafe] DESC;


-- 10. Dolar Kuru Artışını Aşan Şirketler (Döviz Bazında Koruma) (1 yılda Dolar kuru yaklaşık %35 arttı; dolardan daha çok kazandıran şirketler hangileri)
SELECT 
    Ticker,
    Şirket,
    [Hisse_Yıllık_Nominal_Getiri_%] AS [TL_Getiri_%],
    35.0 AS [Tahmini_USD_TRY_Yıllık_Artış_%],
    ROUND([Hisse_Yıllık_Nominal_Getiri_%] - 35.0, 2) AS [Dolar_Üstü_Reel_Fark_%],
    [Reel_Getiri_Durumu]
FROM vw_bist_reel_getiri
WHERE [Hisse_Yıllık_Nominal_Getiri_%] > 35.0
ORDER BY [Dolar_Üstü_Reel_Fark_%] DESC;
