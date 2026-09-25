# -*- coding: utf-8 -*-
"""
Notebook 0: Borsa Takvim ve Tatil Denetimi (BIST Market Calendar Gate)
====================================================================
Bu notebook, FinOps (Maliyet Optimizasyonu) amacıyla pipeline'ın en başında çalışır.
Eğer bugün hafta sonu (Cumartesi/Pazar) veya Borsa İstanbul'un kapalı olduğu resmi
bir tatil günü ise, gereksiz API çağrısı ve bulut kaynağı harcamamak için pipeline'ı
temiz bir şekilde durdurur.

Fabric Notebook'a doğrudan kopyalayıp çalıştırabilirsiniz.
"""

# %% [markdown]
# # 🏛️ Borsa İstanbul (BIST) Takvim & Tatil Denetimi (FinOps)

# %%
from datetime import datetime, date

print("=" * 60)
print("📅 BIST PİYASA TAKVİMİ DENETİMİ BAŞLIYOR (00_market_calendar_check)")
print("=" * 60)

today = datetime.now()
day_of_week = today.weekday()  # 0: Pazartesi ... 4: Cuma, 5: Cumartesi, 6: Pazar
today_str = today.strftime("%Y-%m-%d")

# Sabit Resmi Tatiller (Ay-Gün)
FIXED_HOLIDAYS = [
    "01-01",  # Yılbaşı
    "04-23",  # Ulusal Egemenlik ve Çocuk Bayramı
    "05-01",  # Emek ve Dayanışma Günü
    "05-19",  # Atatürk'ü Anma, Gençlik ve Spor Bayramı
    "07-15",  # Demokrasi ve Milli Birlik Günü
    "08-30",  # Zafer Bayramı
    "10-29",  # Cumhuriyet Bayramı
]

is_weekend = day_of_week in [5, 6]
is_fixed_holiday = today.strftime("%m-%d") in FIXED_HOLIDAYS

print(f"🕒 Sistem Zamanı: {today_str} ({today.strftime('%A')})")

status = "MARKET_OPEN"
reason = "BIST İşlem Günü (Piyasa Açık)"

if is_weekend:
    status = "MARKET_CLOSED"
    reason = "Hafta Sonu (Piyasa Kapalı)"
elif is_fixed_holiday:
    status = "MARKET_CLOSED"
    reason = "Resmi Tatil (BIST İşleme Kapalı)"

print(f"📊 Durum: {status} — Gerekçe: {reason}")

# Microsoft Fabric Notebook Exit (Data Factory'ye parametre döner)
try:
    # Fabric ortamındaysa Data Factory'ye sonucu ilet
    import notebookutils
    notebookutils.notebook.exit(status)
except Exception:
    try:
        import mssparkutils
        mssparkutils.notebook.exit(status)
    except Exception:
        pass

print("=" * 60)
if status == "MARKET_OPEN":
    print("🚀 Piyasa açık! İşlem hattı sonraki adımlara devam edebilir.")
else:
    print("⏸️  FinOps Devrede: Gereksiz işlem yapılmaması için pipeline durduruldu.")
print("=" * 60)
