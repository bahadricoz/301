import streamlit as st
import pandas as pd
import io

# Sayfa yapılandırması
st.set_page_config(
    page_title="İkas 301 Yönlendirme Aracı",
    page_icon="🔄",
    layout="wide"
)

# Başlık
st.title("🔄 İkas Geçişi Toplu 301 Yönlendirme Aracı")
st.markdown("---")

# Açıklama
st.markdown("""
### Kullanım Talimatları
1. **Eski Ürün/Kategori Verisi**: Eski platformdan dışa aktarılan CSV dosyasını yükleyin
2. **İkas Ürün/Kategori Verisi**: İkas'tan dışa aktarılan CSV dosyasını yükleyin
3. **Eşleştirme Tipi**: Hangi alan üzerinden eşleştirme yapılacağını seçin
4. **İşle**: Verileri işleyip 301 yönlendirme listesini oluşturun
""")
st.markdown("---")

# Dosya yükleme bölümleri
col1, col2 = st.columns(2)

with col1:
    st.subheader("📤 Eski Ürün/Kategori Verisi")
    eski_dosyalar = st.file_uploader(
        "Eski platformdan CSV dosyası yükleyin (birden fazla dosya seçebilirsiniz)",
        type=['csv'],
        key='eski',
        accept_multiple_files=True,
        help="En az 3 sütun içermeli: Eşleştirme Anahtarı, Eski URL Yolu, Ürün Adı/Başlığı. Birden fazla dosya seçebilirsiniz, hepsi otomatik birleştirilir."
    )

with col2:
    st.subheader("📥 İkas Ürün/Kategori Verisi")
    ikas_dosyalar = st.file_uploader(
        "İkas'tan CSV dosyası yükleyin (birden fazla dosya seçebilirsiniz)",
        type=['csv'],
        key='ikas',
        accept_multiple_files=True,
        help="En az 3 sütun içermeli: Eşleştirme Anahtarı, Yeni URL Yolu, Ürün Adı/Başlığı. Birden fazla dosya seçebilirsiniz, hepsi otomatik birleştirilir."
    )

# Opsiyonel Blog/Sayfa verisi
st.subheader("📝 Blog/Sayfa Verisi (Opsiyonel)")
blog_dosya = st.file_uploader(
    "Blog/Sayfa yönlendirmeleri için CSV dosyası yükleyin",
    type=['csv'],
    key='blog',
    help="Eski Blog/Sayfa URL'leri ve karşılık gelen İkas URL'leri"
)

st.markdown("---")

# Eşleştirme seçeneği
st.subheader("⚙️ Eşleştirme Ayarları")
eslesme_tipi = st.selectbox(
    "Eşleştirme yapılacak alan:",
    options=['SKU/Barkod', 'Ürün Adı/Başlığı'],
    help="Hangi alan üzerinden eski ve yeni verilerin eşleştirileceğini seçin"
)

st.markdown("---")


def temizle_url(url):
    """URL'den domain kısmını kaldırıp sadece slug yapısını döndürür"""
    if pd.isna(url) or url == '':
        return ''
    
    url = str(url).strip()
    
    # http:// veya https:// ile başlıyorsa domain'i kaldır
    if url.startswith('http://') or url.startswith('https://'):
        # Domain kısmını bul ve kaldır
        parts = url.split('/', 3)
        if len(parts) > 3:
            url = '/' + parts[3]
        else:
            url = '/'
    
    # Başında / yoksa ekle
    if not url.startswith('/'):
        url = '/' + url
    
    # Sondaki / karakterini kaldır (root path hariç)
    if len(url) > 1 and url.endswith('/'):
        url = url.rstrip('/')
    
    return url


def normalize_turkish(text):
    """Türkçe karakterleri normalize eder"""
    import unicodedata
    text = str(text)
    # Önce Türkçe karakterleri değiştir (NFKD'den önce)
    replacements = {
        'İ': 'i', 'ı': 'i', 'I': 'i',
        'ğ': 'g', 'Ğ': 'g',
        'ü': 'u', 'Ü': 'u',
        'ş': 's', 'Ş': 's',
        'ö': 'o', 'Ö': 'o',
        'ç': 'c', 'Ç': 'c'
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    # Sonra normalize et ve lowercase yap
    text = unicodedata.normalize('NFKD', text)
    text = text.lower().strip()
    # Combining karakterleri kaldır
    text = ''.join(c for c in text if not unicodedata.combining(c))
    return text


def sutun_secici(df, anahtar_kelimeler, dosya_adi):
    """DataFrame'den uygun sütunu bulmaya çalışır"""
    # Önce tam eşleşme dene (Türkçe karakter desteği ile)
    for anahtar in anahtar_kelimeler:
        for col in df.columns:
            col_normalized = normalize_turkish(col)
            anahtar_normalized = normalize_turkish(anahtar)
            # Tam eşleşme (en önemli - öncelikli)
            if col_normalized == anahtar_normalized:
                return col
    
    # Kısa kelimeler için (ad, isim, name gibi) sadece tam eşleşme kabul et (Türkçe karakter desteği ile)
    kisa_kelimeler = ['ad', 'isim', 'name', 'title']
    for anahtar in anahtar_kelimeler:
        if normalize_turkish(anahtar) in kisa_kelimeler:
            for col in df.columns:
                col_normalized = normalize_turkish(col)
                anahtar_normalized = normalize_turkish(anahtar)
                # Kısa kelimeler için sadece tam eşleşme
                if col_normalized == anahtar_normalized:
                    return col
    
    # Çok kelimeli eşleşme kontrolü (örn: "ürün adı" için "Ürün Adı" sütunu)
    for anahtar in anahtar_kelimeler:
        if ' ' in anahtar:  # Çok kelimeli anahtar
            anahtar_kelimeleri = normalize_turkish(anahtar).split()
            for col in df.columns:
                col_normalized = normalize_turkish(col)
                # Sütun adı tüm anahtar kelimeleri içeriyorsa
                if all(kelime in col_normalized for kelime in anahtar_kelimeleri):
                    if 'metafield' not in col_normalized and 'linked' not in col_normalized and 'metadata' not in col_normalized:
                        return col
    
    # Tam eşleşme bulunamazsa içerme kontrolü yap (ama kısa kelimeler için değil)
    kisa_kelimeler = ['ad', 'isim', 'name', 'title']
    for anahtar in anahtar_kelimeler:
        # Kısa kelimeler için içerme kontrolü yapma, sadece tam eşleşme kabul et
        if normalize_turkish(anahtar) in kisa_kelimeler:
            continue
        for col in df.columns:
            col_normalized = normalize_turkish(col)
            anahtar_normalized = normalize_turkish(anahtar)
            # Sütun adı anahtar kelimeyi içeriyor ama "metafield", "metadata", "linked" değilse
            if anahtar_normalized in col_normalized and 'metafield' not in col_normalized and 'linked' not in col_normalized and 'metadata' not in col_normalized:
                return col
    
    # Bulunamazsa kullanıcıya göster ve seç
    st.warning(f"⚠️ {dosya_adi} dosyasında '{', '.join(anahtar_kelimeler[:3])}...' içeren sütun bulunamadı.")
    return st.selectbox(
        f"{dosya_adi} - Uygun sütunu seçin:",
        options=df.columns.tolist(),
        key=f"select_{dosya_adi}_{anahtar_kelimeler[0]}"
    )


def isle_veriyi(eski_df, ikas_df, eslesme_tipi):
    """Ana işleme fonksiyonu"""
    
    try:
        # Eşleştirme anahtarı belirleme
        if eslesme_tipi == 'SKU/Barkod':
            eski_anahtar_kelimeler = ['sku', 'barkod', 'barcode', 'code', 'kod']
            ikas_anahtar_kelimeler = ['sku', 'barkod', 'barcode', 'code', 'kod']
        else:  # Ürün Adı/Başlığı
            eski_anahtar_kelimeler = ['title', 'name', 'ürün', 'urun', 'başlık', 'baslik', 'ad']
            # İkas için önce "İsim" sütununu bul, sonra diğerleri
            ikas_anahtar_kelimeler = ['isim', 'ad', 'ürün adı', 'ürün ismi', 'ürün adi', 'name', 'title', 'başlık', 'baslik', 'ürün', 'urun']
        
        # URL yolu anahtarları
        eski_url_kelimeler = ['handle', 'slug', 'url', 'path', 'yol', 'link']
        ikas_url_kelimeler = ['slug', 'url', 'handle', 'path', 'yol', 'link']
        
        # Sütunları bul
        eski_anahtar_col = sutun_secici(eski_df, eski_anahtar_kelimeler, "Eski Veri - Eşleştirme Anahtarı")
        eski_url_col = sutun_secici(eski_df, eski_url_kelimeler, "Eski Veri - URL Yolu")
        
        ikas_anahtar_col = sutun_secici(ikas_df, ikas_anahtar_kelimeler, "İkas Veri - Eşleştirme Anahtarı")
        ikas_url_col = sutun_secici(ikas_df, ikas_url_kelimeler, "İkas Veri - URL Yolu")
        
        # Standart isimlendirme
        eski_df_isleme = eski_df[[eski_anahtar_col, eski_url_col]].copy()
        eski_df_isleme.columns = ['Anahtar', 'Eski_URL']
        
        ikas_df_isleme = ikas_df[[ikas_anahtar_col, ikas_url_col]].copy()
        ikas_df_isleme.columns = ['Anahtar', 'Yeni_URL']
        
        # Boş değerleri temizle
        eski_df_isleme = eski_df_isleme.dropna(subset=['Anahtar', 'Eski_URL'])
        ikas_df_isleme = ikas_df_isleme.dropna(subset=['Anahtar', 'Yeni_URL'])
        
        # Anahtarları string'e çevir ve normalize et
        eski_df_isleme['Anahtar'] = eski_df_isleme['Anahtar'].astype(str).str.strip().str.lower()
        ikas_df_isleme['Anahtar'] = ikas_df_isleme['Anahtar'].astype(str).str.strip().str.lower()
        
        # URL'leri temizle
        eski_df_isleme['Eski_URL'] = eski_df_isleme['Eski_URL'].apply(temizle_url)
        ikas_df_isleme['Yeni_URL'] = ikas_df_isleme['Yeni_URL'].apply(temizle_url)
        
        # Inner join ile birleştir (sadece tam eşleşenler)
        birlesik_df = pd.merge(
            eski_df_isleme,
            ikas_df_isleme,
            on='Anahtar',
            how='inner'
        )
        
        st.info(f"✅ Toplam {len(birlesik_df)} adet eşleşme bulundu.")
        
        # Sadece slug'ı farklı olanları filtrele
        farkli_slug_df = birlesik_df[birlesik_df['Eski_URL'] != birlesik_df['Yeni_URL']].copy()
        
        st.info(f"🔄 Bunlardan {len(farkli_slug_df)} adedinde URL değişikliği var (yönlendirme gerekiyor).")
        
        if len(farkli_slug_df) == 0:
            st.warning("⚠️ Yönlendirme gerektiren kayıt bulunamadı. Tüm URL'ler aynı!")
            return None
        
        # İkas 301 formatına dönüştür
        sonuc_df = pd.DataFrame({
            'Source Path': farkli_slug_df['Eski_URL'],
            'Target Path': farkli_slug_df['Yeni_URL'],
            'Status Code': 301
        })
        
        # Tekrar eden kayıtları kaldır
        sonuc_df = sonuc_df.drop_duplicates(subset=['Source Path'])
        
        return sonuc_df
        
    except Exception as e:
        st.error(f"❌ Hata oluştu: {str(e)}")
        return None


def isle_blog_veriyi(blog_df):
    """Blog/Sayfa verilerini işler"""
    try:
        # İlk iki sütunu al (genellikle eski URL ve yeni URL)
        if len(blog_df.columns) < 2:
            st.error("❌ Blog/Sayfa dosyası en az 2 sütun içermelidir!")
            return None
        
        # Sütun isimlerini kontrol et
        eski_col = blog_df.columns[0]
        yeni_col = blog_df.columns[1]
        
        blog_isleme = blog_df[[eski_col, yeni_col]].copy()
        blog_isleme.columns = ['Eski_URL', 'Yeni_URL']
        
        # Boş değerleri temizle
        blog_isleme = blog_isleme.dropna()
        
        # URL'leri temizle
        blog_isleme['Eski_URL'] = blog_isleme['Eski_URL'].apply(temizle_url)
        blog_isleme['Yeni_URL'] = blog_isleme['Yeni_URL'].apply(temizle_url)
        
        # Farklı olanları filtrele
        blog_isleme = blog_isleme[blog_isleme['Eski_URL'] != blog_isleme['Yeni_URL']]
        
        # İkas 301 formatına dönüştür
        sonuc_df = pd.DataFrame({
            'Source Path': blog_isleme['Eski_URL'],
            'Target Path': blog_isleme['Yeni_URL'],
            'Status Code': 301
        })
        
        # Tekrar eden kayıtları kaldır
        sonuc_df = sonuc_df.drop_duplicates(subset=['Source Path'])
        
        st.info(f"✅ Blog/Sayfa: {len(sonuc_df)} adet yönlendirme hazırlandı.")
        
        return sonuc_df
        
    except Exception as e:
        st.error(f"❌ Blog/Sayfa verisi işlenirken hata oluştu: {str(e)}")
        return None


# Birden fazla dosyayı birleştiren fonksiyon
def dosyalari_birlestir(dosyalar, dosya_tipi):
    """Birden fazla CSV dosyasını birleştirir"""
    if not dosyalar or len(dosyalar) == 0:
        return None
    
    dataframes = []
    for i, dosya in enumerate(dosyalar):
        try:
            df = pd.read_csv(dosya)
            dataframes.append(df)
            st.success(f"✅ {dosya_tipi} - Dosya {i+1} ({dosya.name}): {len(df)} kayıt yüklendi")
        except Exception as e:
            st.warning(f"⚠️ {dosya_tipi} - Dosya {i+1} ({dosya.name}) okunurken hata: {str(e)}")
            continue
    
    if len(dataframes) == 0:
        return None
    
    # Tüm DataFrame'leri birleştir
    birlesik_df = pd.concat(dataframes, ignore_index=True)
    # Tekrar eden kayıtları kaldır
    birlesik_df = birlesik_df.drop_duplicates()
    
    return birlesik_df


# İşle butonu
if st.button("🚀 Verileri İşle ve 301 Listesi Oluştur", type="primary"):
    
    # Zorunlu dosyaların kontrolü
    if not eski_dosyalar or len(eski_dosyalar) == 0 or not ikas_dosyalar or len(ikas_dosyalar) == 0:
        st.error("❌ Lütfen en az 'Eski Ürün/Kategori Verisi' ve 'İkas Ürün/Kategori Verisi' dosyalarını yükleyin!")
    else:
        with st.spinner("⏳ Veriler işleniyor..."):
            try:
                # CSV dosyalarını oku ve birleştir
                eski_df = dosyalari_birlestir(eski_dosyalar, "Eski veri")
                ikas_df = dosyalari_birlestir(ikas_dosyalar, "İkas veri")
                
                if eski_df is None or ikas_df is None:
                    st.error("❌ Dosyalar okunurken bir hata oluştu!")
                else:
                    st.info(f"📊 Eski veri toplam: {len(eski_df)} kayıt (birleştirilmiş)")
                    st.info(f"📊 İkas veri toplam: {len(ikas_df)} kayıt (birleştirilmiş)")
                
                # Ana işlemi yap
                sonuc_df = isle_veriyi(eski_df, ikas_df, eslesme_tipi)
                
                # Blog/Sayfa verisi varsa işle
                blog_sonuc_df = None
                if blog_dosya is not None:
                    blog_df = pd.read_csv(blog_dosya)
                    st.success(f"✅ Blog/Sayfa veri: {len(blog_df)} kayıt yüklendi")
                    blog_sonuc_df = isle_blog_veriyi(blog_df)
                
                # Sonuçları birleştir
                if sonuc_df is not None or blog_sonuc_df is not None:
                    
                    # Tüm sonuçları birleştir
                    tum_sonuclar = []
                    if sonuc_df is not None:
                        tum_sonuclar.append(sonuc_df)
                    if blog_sonuc_df is not None:
                        tum_sonuclar.append(blog_sonuc_df)
                    
                    nihai_sonuc = pd.concat(tum_sonuclar, ignore_index=True)
                    
                    # Tekrar eden Source Path'leri kaldır (ilk kaydı tut)
                    nihai_sonuc = nihai_sonuc.drop_duplicates(subset=['Source Path'], keep='first')
                    
                    st.markdown("---")
                    st.subheader("✅ İşlem Başarılı!")
                    st.success(f"🎯 Toplam {len(nihai_sonuc)} adet 301 yönlendirme oluşturuldu")
                    
                    # Sonuçları göster
                    st.dataframe(nihai_sonuc, use_container_width=True)
                    
                    # İstatistikler
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Toplam Yönlendirme", len(nihai_sonuc))
                    with col2:
                        if sonuc_df is not None:
                            st.metric("Ürün/Kategori", len(sonuc_df))
                        else:
                            st.metric("Ürün/Kategori", 0)
                    with col3:
                        if blog_sonuc_df is not None:
                            st.metric("Blog/Sayfa", len(blog_sonuc_df))
                        else:
                            st.metric("Blog/Sayfa", 0)
                    
                    # CSV'ye dönüştür
                    csv_buffer = io.StringIO()
                    nihai_sonuc.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
                    csv_data = csv_buffer.getvalue()
                    
                    # İndirme butonu
                    st.download_button(
                        label="📥 301 Listesini İndir (CSV)",
                        data=csv_data,
                        file_name="ikas_301_yonlendirmeleri.csv",
                        mime="text/csv",
                        type="primary"
                    )
                    
                    st.markdown("---")
                    st.info("💡 İndirilen CSV dosyasını doğrudan İkas admin paneline yükleyebilirsiniz.")
                
            except Exception as e:
                st.error(f"❌ Genel hata oluştu: {str(e)}")
                st.exception(e)

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: gray;'>
    <p>İkas Geçişi 301 Yönlendirme Aracı | Versiyon 1.0</p>
    <p><small>⚠️ Önemli: URL'ler sadece slug formatında (/) üretilir, domain bilgisi içermez.</small></p>
</div>
""", unsafe_allow_html=True)

