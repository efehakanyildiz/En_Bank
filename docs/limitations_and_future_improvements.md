# 🔍 Mevcut Kısıtlar ve Geliştirilebilecek Noktalar (Limitations & Future Roadmap)

Bu doküman, platformun mevcut sürümündeki mimari kısıtları ve gelecek sürümlerde (V2) uygulanabilecek optimizasyon alanlarını özetlemektedir.

---

### 1. Merkezi Şirket Boyut Tablosunun (Star Schema) Eksikliği
* **Mevcut Kısıt:**
  Lakehouse üzerindeki analitik görünümler (`vw_bist_reel_getiri`, `vw_teknik_alim_sinyalleri`, `gold_teknik_analiz`) birbirinden bağımsız olarak hazırlanmıştır ve şirket adları farklı tablolarda `Şirket`, `Sirket` ve `company_name` sütunları altında taşınmaktadır. Power BI rapor sayfalarında tek bir merkezi boyut tablosu bulunmadığı için, bazı görsellerin sayfa düzeyindeki filtrelere otomatik tepki vermesi zorlaşmakta ve görsel düzeyinde (Visual-level) filtreleme yapılması gerekmektedir.
* **Geliştirilebilecek Nokta:**
  Semantic model içerisine tekil şirket kodlarından oluşan bir **`dim_sirket` (Company Dimension)** tablosu eklenerek tam bir **Yıldız Şema (Star Schema)** kurulmalıdır. Böylece tek bir merkezi dilimleyici ile sayfadaki tüm kart ve grafikler $1 \rightarrow \infty$ ilişkisi üzerinden senkronize çalışacaktır.

---

### 2. Dinamik Tarih Boyutu ve Borsa İş Günü Takvimi
* **Mevcut Kısıt:**
  Tarih filtreleri doğrudan olgu (Fact) tablolarındaki ham `date` sütunundan beslenmektedir. Bu durum, DirectLake üzerinde çeyreklik (Q1, Q2) veya mali dönem bazlı hiyerarşik filtreleme kabiliyetini ve BIST işlem günlerine göre dinamik kaydırmayı sınırlamaktadır.
* **Geliştirilebilecek Nokta:**
  Borsa İstanbul seans takvimini, resmi tatilleri, hafta sonlarını ve çeyreklik mali dönemleri içeren zengin bir **`dim_tarih` (Date Dimension)** tablosunun veri ambarına entegre edilmesi.

---

### 3. Gün İçi Canlı Veri Akışının (Intraday Streaming) Bulunmaması
* **Mevcut Kısıt:**
  Platform şu anda günlük seans kapanışı sonrası (Batch - 18:30) periyoduyla çalışmaktadır. Gün içi seans hareketlerini (dakikalık ve anlık fiyat dalgalanmalarını) kapsamamaktadır.
* **Geliştirilebilecek Nokta:**
  Microsoft Fabric **Real-Time Analytics (Eventstream & KQL Database)** kullanılarak seans saatleri boyunca anlık işlem verisinin mikro-batch veya streaming olarak akıtılması.

---

### 4. Satır Düzeyinde Güvenlik (Row-Level Security - RLS) İhtiyacı
* **Mevcut Kısıt:**
  Raporu açan her kullanıcı, 15 hisse senedinin ve danışmanlık portföyündeki 5 müşteri şirketinin tamamına ait tüm verilere erişebilmektedir.
* **Geliştirilebilecek Nokta:**
  Kurumsal yönetim standartlarına uyum amacıyla, her danışman veya analist grubunun yalnızca kendi sorumlu olduğu müşteri şirketlerini görmesini sağlayacak dinamik bir **Row-Level Security (RLS)** yetkilendirme katmanının kurulması.
