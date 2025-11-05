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
    eski_dosya = st.file_uploader(
        "Eski platformdan CSV dosyası yükleyin",
        type=['csv'],
        key='eski',
        help="En az 3 sütun içermeli: Eşleştirme Anahtarı, Eski URL Yolu, Ürün Adı/Başlığı"
    )

with col2:
    st.subheader("📥 İkas Ürün/Kategori Verisi")
    ikas_dosya = st.file_uploader(
        "İkas'tan CSV dosyası yükleyin",
        type=['csv'],
        key='ikas',
        help="En az 3 sütun içermeli: Eşleştirme Anahtarı, Yeni URL Yolu, Ürün Adı/Başlığı"
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


def sutun_secici(df, anahtar_kelimeler, dosya_adi):
    """DataFrame'den uygun sütunu bulmaya çalışır"""
    for anahtar in anahtar_kelimeler:
        for col in df.columns:
            if anahtar.lower() in col.lower():
                return col
    
    # Bulunamazsa kullanıcıya göster ve seç
    st.warning(f"⚠️ {dosya_adi} dosyasında '{', '.join(anahtar_kelimeler)}' içeren sütun bulunamadı.")
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
            eski_anahtar_kelimeler = ['ürün', 'urun', 'name', 'title', 'başlık', 'baslik', 'ad']
            ikas_anahtar_kelimeler = ['ürün', 'urun', 'name', 'title', 'başlık', 'baslik', 'ad']
        
        # URL yolu anahtarları
        eski_url_kelimeler = ['url', 'slug', 'path', 'yol', 'link', 'handle']
        ikas_url_kelimeler = ['url', 'slug', 'path', 'yol', 'link', 'handle']
        
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


# İşle butonu
if st.button("🚀 Verileri İşle ve 301 Listesi Oluştur", type="primary"):
    
    # Zorunlu dosyaların kontrolü
    if eski_dosya is None or ikas_dosya is None:
        st.error("❌ Lütfen en az 'Eski Ürün/Kategori Verisi' ve 'İkas Ürün/Kategori Verisi' dosyalarını yükleyin!")
    else:
        with st.spinner("⏳ Veriler işleniyor..."):
            try:
                # CSV dosyalarını oku
                eski_df = pd.read_csv(eski_dosya)
                ikas_df = pd.read_csv(ikas_dosya)
                
                st.success(f"✅ Eski veri: {len(eski_df)} kayıt yüklendi")
                st.success(f"✅ İkas veri: {len(ikas_df)} kayıt yüklendi")
                
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

