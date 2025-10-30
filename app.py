# app.py
# -*- coding: utf-8 -*-

import re
import io
import json
import time
import math
import html
import logging
import unicodedata
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import requests
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup, Tag
from difflib import SequenceMatcher

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; IkasMigrator/1.0; +https://ikas.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

def fetch_html(url: str, timeout: int = 20, max_retries: int = 3, sleep_between: float = 1.0) -> Optional[str]:
    last_exc: Optional[Exception] = None
    session = requests.Session()
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
            if resp.status_code >= 200 and resp.status_code < 400 and "text/html" in resp.headers.get("Content-Type", ""):
                return resp.text
            else:
                logging.warning("Non-OK response %s for %s", resp.status_code, url)
        except Exception as exc:
            last_exc = exc
            logging.warning("Attempt %d failed for %s: %s", attempt, url, exc)
        time.sleep(sleep_between * attempt)
    logging.error("Failed to fetch %s after %d attempts. Last error: %s", url, max_retries, last_exc)
    return None

PRICE_CLEAN_RE = re.compile(r"[^0-9,.\-]")

def clean_price_to_number(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    t = text.strip()
    if not t:
        return None
    t = PRICE_CLEAN_RE.sub("", t)
    if "," in t and "." in t:
        t = t.replace(".", "").replace(",", ".")
    else:
        if "," in t and t.count(",") == 1 and len(t.split(",")[-1]) in (1, 2):
            t = t.replace(",", ".")
        else:
            t = t.replace(",", "")
    try:
        return float(t)
    except ValueError:
        return None

def slugify(value: str) -> str:
    if not value:
        return ""
    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode("ascii")
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = value.strip("-")
    value = re.sub(r"-{2,}", "-", value)
    return value

def _absolute_url(base_url: str, maybe_url: str) -> str:
    try:
        return urljoin(base_url, maybe_url)
    except Exception:
        return maybe_url

def _same_domain(url_a: str, url_b: str) -> bool:
    try:
        pa = urlparse(url_a)
        pb = urlparse(url_b)
        return pa.netloc == pb.netloc
    except Exception:
        return False

def _should_exclude_url(href: str) -> bool:
    """Check if URL should be excluded (checkout, cart, login, etc.)"""
    if not href:
        return True
    href_l = href.lower()
    exclude_patterns = [
        "/checkout",
        "/cart",
        "/sepet",
        "/login",
        "/giris",
        "/register",
        "/uye-ol",
        "/logout",
        "/cikis",
        "/search",
        "/ara",
        "/filter",
        "/compare",
        "/karsilastir",
        "/wishlist",
        "/favori",
        "/profile",
        "/profil",
        "/account",
        "/hesap",
        "/payment",
        "/odeme",
        "/api/",
        "/ajax/",
        "/ajax",
        "/json/",
        ".json",
        ".xml",
        ".rss",
        "#",
        "?print=",
        "?export=",
    ]
    return any(pattern in href_l for pattern in exclude_patterns)

def _determine_page_type(url: str, soup: BeautifulSoup) -> str:
    """Determine if page is product, category, or static page"""
    url_l = url.lower()
    path = urlparse(url_l).path
    
    # Product patterns
    product_patterns = ["/urun-", "/urun/", "/product/", "/p-", "/p/", "/detay-", "/detail/"]
    if any(p in path for p in product_patterns):
        return "product"
    
    # Blog patterns
    blog_patterns = ["/blog", "/haber", "/news", "/article"]
    if any(p in path for p in blog_patterns):
        return "blog"

    # Category patterns
    category_patterns = ["/kategori", "/category", "/katalog", "/catalog"]
    if any(p in path for p in category_patterns):
        return "category"
    
    # Check HTML for product indicators
    if soup:
        if soup.find("meta", property="og:type", content=lambda x: x and "product" in x.lower()):
            return "product"
        if soup.find("meta", property="og:type", content=lambda x: x and ("article" in x.lower() or "blog" in x.lower())):
            return "blog"
        if soup.find("div", class_=re.compile(r"product", re.I)) or soup.find(id=re.compile(r"product", re.I)):
            return "product"
        if soup.find("nav", class_=re.compile(r"breadcrumb", re.I)) and "/urun" in path:
            return "product"
    
    # Static pages (hakkimizda, iletisim, vs.)
    return "page"

def _normalize_sku(value: Optional[str]) -> str:
    if not value:
        return ""
    v = str(value).strip()
    v = re.sub(r"\s+", "", v)
    return v.upper()

def _normalize_title(value: Optional[str]) -> str:
    if not value:
        return ""
    v = str(value).lower()
    v = re.sub(r"[^a-z0-9çğıöşü\s]", " ", v)
    v = re.sub(r"\s+", " ", v).strip()
    return v

@dataclass
class JsonLdProduct:
    name: Optional[str] = None
    sku: Optional[str] = None
    description_html: Optional[str] = None
    price: Optional[float] = None
    currency: Optional[str] = None
    image: Optional[str] = None
    images: Optional[List[str]] = None
    brand: Optional[str] = None
    barcode: Optional[str] = None
    category_path: Optional[str] = None

def parse_ld_json_product(soup: BeautifulSoup, base_url: str) -> JsonLdProduct:
    candidates: List[Dict[str, Any]] = []
    for script in soup.find_all("script", type=lambda t: t and "ld+json" in t):
        try:
            raw = script.string or script.text or ""
            if not raw.strip():
                continue
            data = json.loads(raw)
            if isinstance(data, dict):
                candidates.append(data)
            elif isinstance(data, list):
                candidates.extend([d for d in data if isinstance(d, dict)])
        except Exception:
            continue

    product_node: Optional[Dict[str, Any]] = None
    for node in candidates:
        t = node.get("@type") or node.get("@type".lower())
        if isinstance(t, list):
            if any(str(i).lower() == "product" for i in t):
                product_node = node
                break
        elif isinstance(t, str) and t.lower() == "product":
            product_node = node
            break
        if "@graph" in node and isinstance(node["@graph"], list):
            for sub in node["@graph"]:
                if isinstance(sub, dict) and str(sub.get("@type", "")).lower() == "product":
                    product_node = sub
                    break

    if not product_node:
        return JsonLdProduct()

    name = product_node.get("name")
    sku = product_node.get("sku") or product_node.get("mpn")
    description = product_node.get("description")
    if isinstance(description, dict) and "text" in description:
        description = description.get("text")

    image_val = product_node.get("image")
    image_url = None
    image_list: List[str] = []
    if isinstance(image_val, list) and len(image_val) > 0:
        image_list = [
            _absolute_url(base_url, str(u)) for u in image_val if isinstance(u, (str,))
        ]
        image_url = image_list[0] if image_list else None
    elif isinstance(image_val, str):
        image_url = _absolute_url(base_url, image_val)
        image_list = [image_url]

    price_val = None
    currency = None
    offers = product_node.get("offers")
    if isinstance(offers, list) and offers:
        offers = offers[0]
    if isinstance(offers, dict):
        price_val = offers.get("price") or offers.get("lowPrice") or offers.get("highPrice")
        currency = offers.get("priceCurrency")
    price_num = clean_price_to_number(str(price_val)) if price_val is not None else None

    # Brand (may be string or object)
    brand_node = product_node.get("brand")
    brand_name: Optional[str] = None
    if isinstance(brand_node, dict):
        brand_name = brand_node.get("name")
    elif isinstance(brand_node, str):
        brand_name = brand_node

    # Barcodes may appear as gtin13/gtin14/gtin
    barcode = (
        product_node.get("gtin13")
        or product_node.get("gtin14")
        or product_node.get("gtin12")
        or product_node.get("gtin8")
        or product_node.get("isbn")
    )

    # Category via breadcrumbs or category property
    category_path = None
    category_val = product_node.get("category")
    if isinstance(category_val, str):
        category_path = category_val

    return JsonLdProduct(
        name=name if isinstance(name, str) else None,
        sku=sku if isinstance(sku, str) else None,
        description_html=description if isinstance(description, str) else None,
        price=price_num,
        currency=currency if isinstance(currency, str) else None,
        image=image_url,
        images=image_list or None,
        brand=brand_name if isinstance(brand_name, str) else None,
        barcode=str(barcode) if barcode else None,
        category_path=category_path,
    )

def find_product_name(soup: BeautifulSoup) -> Optional[str]:
    for sel in [
        "h1.product-title",
        "h1.product_name",
        "h1#productName",
        "h1[itemprop='name']",
        "h1",
        ".product-title",
        ".productName",
        "meta[property='og:title']",
    ]:
        el = soup.select_one(sel)
        if el:
            if isinstance(el, Tag) and el.name == "meta":
                content = el.get("content")
                if content:
                    return content.strip()
            txt = el.get_text(" ", strip=True)
            if txt:
                return txt
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    return None

def find_price_text(soup: BeautifulSoup) -> Optional[str]:
    for sel in [
        ".product-price .price",
        ".product-price .new",
        ".price .current",
        ".productPrice",
        "[itemprop='price']",
        ".price",
        ".newPrice",
        "meta[itemprop='price']",
        "meta[property='product:price:amount']",
    ]:
        el = soup.select_one(sel)
        if el:
            if el.name == "meta":
                content = el.get("content")
                if content:
                    return content.strip()
            txt = el.get_text(" ", strip=True)
            if txt:
                return txt
    text = soup.get_text(" ", strip=True)
    m = re.search(r"([0-9\.\,]+)\s*(TL|₺|TRY)?", text, flags=re.IGNORECASE)
    if m:
        return m.group(0)
    return None

def find_sku(soup: BeautifulSoup) -> Optional[str]:
    for sel in [
        "[itemprop='sku']",
        ".product-sku",
        "#productSku",
        ".sku",
        "span:contains('SKU')",
        "span:contains('Stok Kodu')",
    ]:
        try:
            el = soup.select_one(sel)
        except Exception:
            el = None
        if el:
            txt = el.get_text(" ", strip=True) if hasattr(el, "get_text") else (el.get("content") or "")
            txt = re.sub(r"(?i)\b(SKU|Stok Kodu|Model)\b[:\s]*", "", txt).strip()
            if txt:
                return txt
    el = soup.find(attrs={"itemprop": "sku"})
    if el:
        if el.has_attr("content"):
            return el["content"].strip()
        return el.get_text(" ", strip=True)
    return None

def find_description_html(soup: BeautifulSoup) -> Optional[str]:
    for sel in [
        "#productDescription",
        ".product-description",
        ".product-desc",
        "[itemprop='description']",
        ".tab-content .desc",
        ".aciklama",
        ".tab-content .tab-pane.active",
        "#tabProductDesc",
        "#tab-description",
        "[id*='description']",
        ".product-detail .tab-content",
    ]:
        el = soup.select_one(sel)
        if el:
            html_str = str(el)
            if html_str:
                return html_str
    candidates = soup.find_all(["div", "section"], limit=20)
    best = ""
    for c in candidates:
        text_len = len(c.get_text(" ", strip=True))
        if text_len > len(best):
            best = str(c)
    return best or None

def _extract_img_src(img: Tag) -> Optional[str]:
    # Prefer high-res attributes
    for key in ("data-zoom-image", "data-large", "data-src", "src"):
        val = img.get(key)
        if val:
            return val
    # Parse srcset
    srcset = img.get("srcset")
    if srcset:
        first = srcset.split(",")[0].strip().split(" ")[0]
        if first:
            return first
    return None

def find_main_image_url(soup: BeautifulSoup, base_url: str, ld: Optional[JsonLdProduct]) -> Optional[str]:
    if ld and ld.image:
        return _absolute_url(base_url, ld.image)
    meta = soup.find("meta", property="og:image") or soup.find("meta", attrs={"name": "og:image"})
    if meta and meta.get("content"):
        return _absolute_url(base_url, meta["content"])
    for sel in [
        "#productImage",
        ".product-image img",
        ".gallery img",
        "img[itemprop='image']",
        ".swiper .swiper-slide img",
        "#productImages img",
        ".product-detail .swiper img",
    ]:
        img = soup.select_one(sel)
        if img:
            src = _extract_img_src(img)
            if src:
                return _absolute_url(base_url, src)
    imgs = soup.find_all("img")
    best_url = None
    best_w = 0
    for img in imgs:
        src = _extract_img_src(img) or ""
        if not src:
            continue
        w = 0
        try:
            w = int(img.get("width") or 0)
        except Exception:
            w = 0
        if w > best_w:
            best_w = w
            best_url = src
    return _absolute_url(base_url, best_url) if best_url else None

def find_all_image_urls(soup: BeautifulSoup, base_url: str, ld: Optional[JsonLdProduct]) -> List[str]:
    # Prefer JSON-LD list
    if ld and ld.images:
        return list(dict.fromkeys([_absolute_url(base_url, u) for u in ld.images]))

    urls: List[str] = []
    # Common gallery selectors
    gallery_selectors = [
        ".product-images img",
        ".product-gallery img",
        ".gallery img",
        "#productImages img",
        ".swiper .swiper-slide img",
        ".product-detail .swiper img",
        "img[data-zoom-image]",
        "img[itemprop='image']",
    ]
    for sel in gallery_selectors:
        for img in soup.select(sel):
            src = _extract_img_src(img)
            if src:
                urls.append(_absolute_url(base_url, src))
    # Fallback to og:image
    meta = soup.find("meta", property="og:image")
    if meta and meta.get("content"):
        urls.append(_absolute_url(base_url, meta["content"]))
    # De-dup preserving order
    return list(dict.fromkeys(urls))

def find_brand(soup: BeautifulSoup, ld: Optional[JsonLdProduct]) -> Optional[str]:
    if ld and ld.brand:
        return ld.brand
    candidates = [
        "[itemprop='brand']",
        ".brand-name",
        ".product-brand",
        "#brandName",
        "span:contains('Marka')",
    ]
    for sel in candidates:
        try:
            el = soup.select_one(sel)
        except Exception:
            el = None
        if el:
            txt = el.get_text(" ", strip=True)
            if txt:
                txt = re.sub(r"(?i)\b(Marka|Brand)\b[:\s]*", "", txt).strip()
                if txt:
                    return txt
    return None

def find_barcode(soup: BeautifulSoup, ld: Optional[JsonLdProduct]) -> Optional[str]:
    if ld and ld.barcode:
        return ld.barcode
    # Try common microdata/meta names
    for sel in [
        "[itemprop='gtin13']",
        "[itemprop='gtin14']",
        "[itemprop='barcode']",
        "meta[itemprop='gtin13']",
        "meta[name='barcode']",
        ".barcode",
        "span:contains('Barkod')",
    ]:
        try:
            el = soup.select_one(sel)
        except Exception:
            el = None
        if el:
            val = el.get("content") if hasattr(el, "get") else None
            text_val = el.get_text(" ", strip=True) if hasattr(el, "get_text") else None
            out = (val or text_val or "").strip()
            out = re.sub(r"(?i)\b(Barkod|Barcode|GTIN|EAN)\b[:\s]*", "", out)
            if out:
                return out
    return None

def find_category_path(soup: BeautifulSoup, ld: Optional[JsonLdProduct]) -> Optional[str]:
    if ld and ld.category_path:
        return ld.category_path
    # Breadcrumbs
    crumbs: List[str] = []
    for sel in [
        ".breadcrumb a",
        "nav.breadcrumb a",
        "[itemtype*='BreadcrumbList'] [itemprop='name']",
    ]:
        for el in soup.select(sel):
            txt = el.get_text(" ", strip=True)
            if txt:
                crumbs.append(txt)
    crumbs = [c for c in crumbs if c and c.lower() not in ("anasayfa", "home")]
    if crumbs:
        return " > ".join(crumbs)
    return None

def find_meta_title(soup: BeautifulSoup) -> Optional[str]:
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return og["content"].strip()
    return None

def find_meta_description(soup: BeautifulSoup) -> Optional[str]:
    for name in ("description", "og:description"):
        el = soup.find("meta", attrs={"name": name}) or soup.find("meta", property=name)
        if el and el.get("content"):
            return el["content"].strip()
    return None

def scrape_ideasoft_product(url: str) -> Optional[Dict[str, Any]]:
    html_str = fetch_html(url)
    if not html_str:
        return None
    soup = BeautifulSoup(html_str, "html.parser")
    ld = parse_ld_json_product(soup, url)

    name = ld.name or find_product_name(soup) or ""
    price = ld.price
    if price is None:
        price_text = find_price_text(soup)
        price = clean_price_to_number(price_text)
    sku = ld.sku or find_sku(soup) or ""
    description_html = ld.description_html or find_description_html(soup) or ""
    # Images
    image_url = find_main_image_url(soup, url, ld) or ""
    image_urls = find_all_image_urls(soup, url, ld)
    # Extra fields
    brand = find_brand(soup, ld) or ""
    barcode = find_barcode(soup, ld) or ""
    category_path = find_category_path(soup, ld) or ""
    meta_title = find_meta_title(soup) or ""
    meta_description = find_meta_description(soup) or ""

    name = html.unescape(name).strip()
    description_html = description_html.strip()
    image_url = image_url.strip()
    s = slugify(name)

    return {
        "source_url": url,
        "name": name,
        "slug": s,
        "price": price if price is not None else "",
        "sku": sku,
        "description_html": description_html,
        "main_image_url": image_url,
        "currency": ld.currency or "TRY",
        "image_urls": image_urls,
        "brand": brand,
        "barcode": barcode,
        "category_path": category_path,
        "meta_title": meta_title,
        "meta_description": meta_description,
    }

def _discover_all_links_from_page(base_url: str, html_str: str) -> Tuple[Set[str], Optional[str]]:
    """Discover all internal links from a page, excluding checkout/cart/login etc."""
    soup = BeautifulSoup(html_str, "html.parser")
    found: Set[str] = set()
    
    # Get all anchor tags
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if not href:
            continue
        abs_url = _absolute_url(base_url, href)
        # Exclude external links and unwanted pages
        if _same_domain(base_url, abs_url) and not _should_exclude_url(abs_url):
            # Normalize: remove query params and fragments
            try:
                p = urlparse(abs_url)
                clean_url = urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
                if clean_url and clean_url != base_url:
                    found.add(clean_url)
            except Exception:
                pass

    # Pagination
    next_link = None
    next_candidates = [
        "a[rel='next']",
        "link[rel='next']",
        ".pagination a.next",
        "a.next",
        "a[aria-label='Next']",
    ]
    for sel in next_candidates:
        el = soup.select_one(sel)
        if el:
            candidate = el.get("href") or el.get("content")
            if candidate:
                next_link = _absolute_url(base_url, candidate)
                break

    return found, next_link

def find_all_page_links(start_url: str, max_pages: int = 100) -> List[Dict[str, Any]]:
    """Find all pages (products, categories, static pages) from the site."""
    start_html = fetch_html(start_url)
    if not start_html:
        return []

    to_visit: List[str] = [start_url]
    visited: Set[str] = set()
    all_pages: List[Dict[str, Any]] = []
    pages = 0

    while to_visit and pages < max_pages:
        current = to_visit.pop(0)
        if current in visited or _should_exclude_url(current):
            continue
        visited.add(current)
        pages += 1

        html_str = start_html if current == start_url else fetch_html(current)
        if not html_str:
            continue

        soup = BeautifulSoup(html_str, "html.parser")
        page_type = _determine_page_type(current, soup)
        
        # Get page title/name
        title = None
        if soup.title:
            title = soup.title.string.strip() if soup.title.string else None
        if not title:
            og_title = soup.find("meta", property="og:title")
            if og_title and og_title.get("content"):
                title = og_title["content"].strip()
        if not title:
            h1 = soup.find("h1")
            if h1:
                title = h1.get_text(" ", strip=True)
        if not title:
            title = urlparse(current).path.strip("/").split("/")[-1].replace("-", " ").title()

        # Extract slug from URL path
        path = urlparse(current).path.strip("/")
        url_slug = path.split("/")[-1] if path else ""
        slug = slugify(title) if title else url_slug

        all_pages.append({
            "url": current,
            "title": title or "",
            "slug": slug,
            "type": page_type,
        })

        # Discover more links
        found_here, next_link = _discover_all_links_from_page(current, html_str)
        new_links = [u for u in found_here if u not in visited and u not in to_visit]
        to_visit.extend(new_links)
        
        if next_link and next_link not in visited and next_link not in to_visit:
            to_visit.append(next_link)

    return all_pages

st.set_page_config(page_title="IdeaSoft -> İkas 301 Redirect", page_icon="🔄", layout="wide")

st.title("🔄 IdeaSoft -> İkas 301 Redirect Oluşturucu")
st.caption("Domain'deki tüm sayfalar için 301 redirect CSV dosyası oluşturur ve İkas ürünleriyle eşler.")

with st.expander("Nasıl kullanırım?", expanded=False):
    st.markdown(
        "- Ana sayfa URL'si girin (örn. `https://magaza.com`).\n"
        "- 'Başlat' butonuna basın.\n"
        "- Tüm sayfalar (ürünler, kategoriler, statik sayfalar) taranır.\n"
        "- Checkout, sepet, login gibi sayfalar otomatik olarak hariç tutulur.\n"
        "- İkas formatında 301 redirect CSV dosyası indirilir."
    )

input_url = st.text_input("IdeaSoft Ana Sayfa URL", placeholder="https://magaza.com")
ikas_site_url = st.text_input("İkas Site URL (opsiyonel - doğrulama)", placeholder="https://shop.myikas.com")
ikas_csv = st.file_uploader("İkas Ürün CSV (opsiyonel, eşleştirme doğruluğunu artırır)", type=["csv", "CSV"]) 

start = st.button("Başlat", type="primary")

if start:
    if not input_url or not input_url.strip():
        st.error("Lütfen geçerli bir IdeaSoft URL'si girin.")
        st.stop()

    base_url = input_url.strip()
    
    # Ensure base URL doesn't end with /
    if base_url.endswith("/"):
        base_url = base_url[:-1]

    st.info("🔍 Site taranıyor, tüm sayfalar toplanıyor...")
    progress = st.progress(0)
    status = st.empty()
    
    try:
        pages = find_all_page_links(base_url, max_pages=200)
    except Exception as exc:
        st.error(f"Hata oluştu: {exc}")
        logging.exception("Sayfa toplama hatası")
        st.stop()

    progress.progress(1.0)
    status.empty()

    if not pages:
        st.warning("Hiç sayfa bulunamadı. Farklı bir URL deneyin.")
        st.stop()

    st.success(f"✅ {len(pages)} sayfa bulundu!")

    # Build Ikas lookup indexes if provided
    ikas_df: Optional[pd.DataFrame] = None
    sku_to_slug: Dict[str, str] = {}
    barcode_to_slug: Dict[str, str] = {}
    slug_set: Set[str] = set()
    title_to_slug: List[Tuple[str, str]] = []  # (normalized_title, slug)
    if ikas_csv is not None:
        try:
            ikas_df = pd.read_csv(ikas_csv, dtype=str, encoding="utf-8-sig")
        except Exception:
            ikas_csv.seek(0)
            ikas_df = pd.read_csv(ikas_csv, dtype=str, encoding="utf-8")
        ikas_df = ikas_df.fillna("")
        for _, r in ikas_df.iterrows():
            slug_set.add(str(r.get("Slug", "")).strip())
            sku_n = _normalize_sku(r.get("SKU"))
            if sku_n:
                sku_to_slug[sku_n] = str(r.get("Slug", "")).strip()
            bar_list = str(r.get("Barkod Listesi", "")).strip()
            if bar_list:
                for b in str(bar_list).split(";"):
                    b_n = _normalize_sku(b)
                    if b_n:
                        barcode_to_slug[b_n] = str(r.get("Slug", "")).strip()
            title_norm = _normalize_title(r.get("İsim"))
            if title_norm:
                title_to_slug.append((title_norm, str(r.get("Slug", "")).strip()))

    # Create 301 redirect mapping
    redirects_rows: List[Dict[str, Any]] = []
    diagnostics: List[Dict[str, Any]] = []

    for page in pages:
        old_url = page["url"]
        page_type = page["type"]
        title = page["title"]
        slug = page["slug"]
        
        # Determine target path format (strict matching; default empty)
        target_path = ""

        sku = ""
        barcode = ""
        confidence = 0.0
        reason = ""
        if page_type == "product":
            # Fetch once to try to get SKU/Barcode and improve match
            try:
                html_str = fetch_html(old_url)
                if html_str:
                    soup = BeautifulSoup(html_str, "html.parser")
                    sku = _normalize_sku(find_sku(soup) or "")
                    barcode = _normalize_sku(find_barcode(soup, None) or "")
            except Exception:
                pass

            # 1) Match by SKU (exact)
            if sku and sku in sku_to_slug:
                target_path = f"/urun/{sku_to_slug[sku]}"
                confidence = 1.0
                reason = "sku"
            # 2) Match by Barcode (exact)
            elif barcode and barcode in barcode_to_slug:
                target_path = f"/urun/{barcode_to_slug[barcode]}"
                confidence = 0.98
                reason = "barcode"
            else:
                # No safe match; leave empty
                target_path = ""
                reason = "unmatched"
        elif page_type == "category":
            # Ikas format: /kategori-adi (no /kategori/ prefix)
            target_path = f"/{slug}"
            reason = "category"
        elif page_type == "blog":
            target_path = f"/blog/{slug}"
            reason = "blog"
        elif page_type == "page":
            target_path = f"/pages/{slug}"
            reason = "page"
        else:
            # Other non-product pages
            target_path = ""
            reason = "non_product"

        # Verify target URL on Ikas site if provided
        existence = "unknown"
        if ikas_site_url:
            try:
                base = ikas_site_url.rstrip("/")
                check_url = base + target_path
                r = requests.head(check_url, headers=DEFAULT_HEADERS, timeout=10, allow_redirects=True)
                if r.status_code in (200, 301, 302):
                    existence = "ok"
                else:
                    r2 = requests.get(check_url, headers=DEFAULT_HEADERS, timeout=10)
                    existence = "ok" if r2.status_code == 200 else f"status_{r2.status_code}"
            except Exception:
                existence = "error"

        # Slugs for CSV output
        try:
            # Use original IdeaSoft path with leading '/'
            from_path = urlparse(old_url).path
            if not from_path.startswith("/"):
                from_path = "/" + from_path
            from_slug = from_path.strip("/").split("/")[-1] if from_path else ""
        except Exception:
            from_slug = slug
        try:
            to_path = urlparse(target_path).path if target_path else ""
            to_path = to_path if to_path.startswith("/") else ("/" + to_path if to_path else "")
            to_slug = to_path.strip("/").split("/")[-1] if to_path else ""
        except Exception:
            to_slug = slug

        redirects_rows.append({
            "from_url": old_url,
            "to_url": target_path,
            "from_slug": from_slug,
            "to_slug": to_slug,
            "title": title,
            "sku": sku,
            "type": page_type,
        })

        diagnostics.append({
            "from_url": old_url,
            "to_url": target_path,
            "type": page_type,
            "reason": reason,
            "confidence": round(confidence, 3),
            "exists_on_ikas": existence,
        })

    redirects_df = pd.DataFrame(redirects_rows)
    diagnostics_df = pd.DataFrame(diagnostics)
    
    # Show summary
    st.info(f"📊 **Özet:** {len([p for p in pages if p['type'] == 'product'])} ürün, "
            f"{len([p for p in pages if p['type'] == 'category'])} kategori, "
            f"{len([p for p in pages if p['type'] == 'page'])} statik sayfa")

    # Convert to exact Ikas 301 format
    # Ikas wants paths; ensure leading '/'
    kaynak_list = []
    for v, p in zip(redirects_df.get("from_slug", redirects_df["from_url"]), redirects_df.get("from_url")):
        # Prefer full path if available in from_url; else build from slug
        path = urlparse(p).path if isinstance(p, str) and p else ""
        if path:
            k = path if path.startswith("/") else "/" + path
        else:
            s = v or ""
            k = f"/{s}" if s and not s.startswith("/") else (s or "")
        kaynak_list.append(k)

    yon_list = []
    for v in redirects_df.get("to_slug", []):
        s = v or ""
        yon_list.append((f"/{s}" if s and not s.startswith("/") else s))

    ikas301 = pd.DataFrame({
        "ID": ["" for _ in range(len(redirects_df))],
        "Kaynak Adres": kaynak_list,
        "Yönlendirilecek Adres": yon_list if yon_list else ["" for _ in range(len(redirects_df))],
        "Geçici Yönlendirme (302)": ["false" for _ in range(len(redirects_df))],
        "Silindi mi?": ["false" for _ in range(len(redirects_df))],
    })

    def df_to_bytesio_csv(df: pd.DataFrame) -> io.BytesIO:
        buf = io.BytesIO()
        df.to_csv(buf, index=False, encoding="utf-8-sig")
        buf.seek(0)
        return buf

    redirects_buf = df_to_bytesio_csv(ikas301)

    st.success("✅ 301 redirect dosyası hazır!")

    st.download_button(
        label="📥 ikas_301.csv İndir",
        data=redirects_buf,
        file_name="ikas_301.csv",
        mime="text/csv",
        type="primary",
    )
    
    # Show preview table
    with st.expander("📋 Önizleme (İlk 10 satır)", expanded=False):
        st.dataframe(ikas301.head(10), use_container_width=True)

    # Diagnostics section
    if not diagnostics_df.empty:
        low_df = diagnostics_df[(diagnostics_df["type"] == "product") & ((diagnostics_df["confidence"] < 0.8) | (diagnostics_df["exists_on_ikas"] != "ok"))]
        if not low_df.empty:
            st.warning(f"⚠️ İnceleme gerekli: {len(low_df)} ürün için düşük güven veya İkas'ta bulunamadı.")
            st.dataframe(low_df.head(20), use_container_width=True)
            low_buf = io.BytesIO()
            low_df.to_csv(low_buf, index=False, encoding="utf-8-sig")
            low_buf.seek(0)
            st.download_button(
                label="📥 inceleme_gerekenler.csv",
                data=low_buf,
                file_name="inceleme_gerekenler.csv",
                mime="text/csv",
            )