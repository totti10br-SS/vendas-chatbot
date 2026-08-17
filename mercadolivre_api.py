"""
Mercado Livre — via ScrapingAnt scraping das páginas de ofertas
Publisher ID: ot20260326074822
"""

import os
import re
import sys
import json
import random
import hashlib
import time
import requests

ML_PUBLISHER_ID = os.getenv("ML_PUBLISHER_ID", "ot20260326074822")
SCRAPINGANT_KEY = os.getenv("SCRAPINGANT_KEY", "")
ZENROWS_KEY     = os.getenv("ZENROWS_KEY", "")
SCRAPERAPI_KEY  = os.getenv("SCRAPERAPI_KEY", "")
PRECO_MINIMO    = float(os.getenv("PRECO_MINIMO", "50.00"))
PRECO_MAXIMO    = float(os.getenv("PRECO_MAXIMO", "3000.00"))
DESCONTO_MINIMO = int(os.getenv("DESCONTO_MINIMO", "20"))
TINYURL_API_TOKEN = os.getenv("TINYURL_API_TOKEN", "5E6O0b6FW8c5FDRCSjjo1TBl4VO0JtmUwgDgtVr7opF1vCMzdu5NCD1f7T5k")

URLS_BUSCA = [
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1648", "Computacao"),
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1051", "Celulares"),
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1000", "Eletronicos"),
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1066", "TVs"),
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1039", "Video Games"),
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1002", "Audio"),
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1648&q=gabinete%20gamer", "Gabinetes"),
]

URLS_BUSCA_EXTRA = [
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1648&q=notebook", "Notebooks"),
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1648&q=monitor+gamer", "Monitores Gamer"),
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1648&q=ssd+nvme", "SSDs"),
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1648&q=placa+de+video", "Placas de Video"),
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1051&q=smartphone+samsung", "Samsung"),
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1051&q=smartphone+motorola", "Motorola"),
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1051&q=xiaomi", "Xiaomi"),
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1000&q=smartwatch", "Smartwatch"),
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1000&q=fone+bluetooth", "Fones"),
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1000&q=caixa+de+som+bluetooth", "Caixas de Som"),
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1066&q=smart+tv+55", "Smart TV 55"),
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1066&q=smart+tv+65", "Smart TV 65"),
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1039&q=controle+gamer", "Controles"),
    ("https://www.mercadolivre.com.br/ofertas?category=MLB1002&q=soundbar", "Soundbar"),
]

PALAVRAS_BLOQUEADAS = [
    "guincho", "girafa", "talha", "macaco hidraulico", "compressor",
    "furadeira", "parafusadeira", "martelo", "serra", "esmerilhadeira",
    "andaime", "escada", "carrinho de mao", "empilhadeira",
    "lente de projecao", "lente spot", "refletor par", "moving head",
    "canhao de luz", "strobo", "maquina de fumaca",
    "pneu", "rodas", "amortecedor", "escapamento", "farol",
    "cortador de grama", "vaso de planta", "mangueira",
    "churrasqueira", "fogao", "geladeira", "lava-roupa",
    "maquina de lavar", "secadora", "lava-loucas",
    "sofa", "colchao", "cama", "guarda-roupa", "estante",
    "tapete", "cortina", "persiana",
    "roupa", "roupas", "vestido", "camisa", "camiseta", "blusa",
    "calca", "bermuda", "saia", "jaqueta", "casaco",
    "sapato", "tenis", "sandalia", "chinelo", "bota",
    "bolsa", "mochila", "mala", "carteira", "cinto",
    "bola de futebol", "bola gigante", "brinquedo", "brinquedos",
    "boneca", "carrinho de brinquedo",
    "bicicleta", "patins", "skate", "patinete infantil",
    "suplemento", "creatina", "whey protein", "vitamina",
    "remedio", "medicamento",
    "perfume", "shampoo", "creme", "maquiagem", "batom",
    "telescopio", "luneta", "microscopio",
    "racao", "aquario", "gaiola",
    "livro", "album de figurinha", "figurinha",
    "caderno", "agenda",
]

PALAVRAS_TECH = [
    "notebook", "laptop", "pc", "computador", "desktop",
    "celular", "smartphone", "iphone", "samsung", "xiaomi", "motorola",
    "tv", "smart tv", "televisao", "televisão",
    "monitor", "tela", "display",
    "tablet", "ipad",
    "fone", "headset", "headphone", "earphone", "caixa de som",
    "soundbar", "speaker",
    "teclado", "mouse", "mousepad",
    "ssd", "hd externo", "pendrive", "memoria ram", "processador",
    "placa de video", "placa mae", "gabinete", "fonte atx",
    "roteador", "repetidor wifi", "modem",
    "camera", "webcam", "impressora",
    "carregador", "cabo usb", "hub usb",
    "controle", "joystick", "videogame", "playstation", "xbox", "nintendo",
    "smartwatch", "relogio inteligente",
    "drone", "gopro", "action cam",
    "power bank", "nobreak",
    "ar condicionado", "ventilador tower", "purificador de ar",
    "fritadeira air fryer", "cafeteira", "liquidificador",
]


def log(msg):
    print(msg, flush=True)
    sys.stdout.flush()


def produto_valido(nome):
    nome_lower = nome.lower()
    for p in PALAVRAS_BLOQUEADAS:
        if p in nome_lower:
            log(f"  ML bloqueado ({p}): {nome[:50]}")
            return False
    return True


def produto_e_tech(nome):
    nome_lower = nome.lower()
    return any(p in nome_lower for p in PALAVRAS_TECH)


def encurtar_link(url_longa):
    """Encurta via TinyURL API v2 com token."""
    try:
        r = requests.post(
            "https://api.tinyurl.com/create",
            headers={
                "Authorization": f"Bearer {TINYURL_API_TOKEN}",
                "Content-Type": "application/json",
            },
            json={"url": url_longa, "domain": "tinyurl.com"},
            timeout=8
        )
        if r.status_code == 200:
            data = r.json()
            short = data.get("data", {}).get("tiny_url", "")
            if short.startswith("http"):
                return short
    except Exception as e:
        log(f"  TinyURL erro: {e}")
    return url_longa


def gerar_link_afiliado(url):
    separador = "&" if "?" in url else "?"
    return f"{url}{separador}matt_tool={ML_PUBLISHER_ID}"


def scraper_fetch(url):
    """Busca HTML via ScrapingAnt com fallback para ZenRows e ScraperAPI."""
    if SCRAPINGANT_KEY:
        try:
            params = {
                "url":           url,
                "x-api-key":     SCRAPINGANT_KEY,
                "proxy_country": "BR",
                "browser":       "false",
            }
            r = requests.get("https://api.scrapingant.com/v2/general", params=params, timeout=60)
            log(f"  ScrapingAnt {r.status_code} → {url[:60]}")
            if r.status_code == 200:
                return r.text
            log(f"  ScrapingAnt erro: {r.text[:100]}")
        except Exception as e:
            log(f"  ScrapingAnt erro: {e}")

    if ZENROWS_KEY:
        try:
            params = {
                "url":           url,
                "apikey":        ZENROWS_KEY,
                "js_render":     "false",
                "antibot":       "true",
                "premium_proxy": "true",
                "proxy_country": "br",
            }
            r = requests.get("https://api.zenrows.com/v1/", params=params, timeout=60)
            log(f"  ZenRows {r.status_code} → {url[:60]}")
            if r.status_code == 200:
                return r.text
        except Exception as e:
            log(f"  ZenRows erro: {e}")

    if SCRAPERAPI_KEY:
        try:
            payload = {
                "api_key":      SCRAPERAPI_KEY,
                "url":          url,
                "country_code": "br",
                "render":       "false",
            }
            r = requests.get("https://api.scraperapi.com", params=payload, timeout=60)
            log(f"  ScraperAPI {r.status_code} → {url[:60]}")
            if r.status_code == 200:
                return r.text
        except Exception as e:
            log(f"  ScraperAPI erro: {e}")

    return None


def extrair_produtos_html(html):
    """Extrai lista de produtos do JSON embutido na página ML."""
    if not html:
        return []
    log(f"  -> HTML: {len(html)} chars")
    try:
        # Tenta encontrar o JSON de resultados no HTML
        for marker in ['"results":[{', '"items":[{', '"polycard_payload"']:
            idx = html.find(marker)
            if idx > 0:
                start = html.rfind('[', 0, idx + 15)
                if start < 0:
                    continue
                depth = 0
                end = start
                for i, c in enumerate(html[start:], start):
                    if c == '[':
                        depth += 1
                    elif c == ']':
                        depth -= 1
                        if depth == 0:
                            end = i + 1
                            break
                    if i - start > 500000:
                        break
                try:
                    data = json.loads(html[start:end])
                    if isinstance(data, list) and len(data) > 0:
                        log(f"  -> {len(data)} itens extraidos (marker: {marker})")
                        return data
                except Exception:
                    continue
    except Exception as e:
        log(f"  -> Erro extração JSON: {e}")
    log(f"  -> JSON não encontrado no HTML")
    return []


def processar_item(item):
    """Processa item extraído do JSON da página ML."""
    try:
        if not isinstance(item, dict):
            return None

        card = item.get("card", {})
        if not card:
            return None

        metadata   = card.get("metadata", {}) or {}
        components = card.get("components", []) or []
        pictures   = card.get("pictures", []) or []

        item_id  = metadata.get("id", "")
        url_prod = metadata.get("url", "")
        if url_prod and not url_prod.startswith("http"):
            url_prod = "https://" + url_prod
        if not url_prod:
            return None

        nome       = ""
        preco      = 0.0
        preco_orig = 0.0
        desconto   = 0
        imagem     = ""
        mais_vendido = False
        frete_ok   = False

        for comp in components:
            if not isinstance(comp, dict):
                continue
            ctype = comp.get("type", "").lower()
            cdata = comp.get(ctype, {})
            if not isinstance(cdata, dict):
                cdata = {}

            if ctype == "title":
                nome = cdata.get("text", "") or nome

            elif ctype == "price":
                curr = cdata.get("current_price", {}) or {}
                prev = cdata.get("previous_price", {}) or {}
                preco      = float(curr.get("value", 0) or 0)
                preco_orig = float(prev.get("value", 0) or 0)

            elif ctype == "shipping":
                frete_txt_comp = cdata.get("text", "")
                if "grátis" in frete_txt_comp.lower() or "gratis" in frete_txt_comp.lower():
                    frete_ok = True

            elif ctype in ("image", "picture", "gallery"):
                imagem = cdata.get("url", "") or cdata.get("src", "") or imagem

            elif ctype == "highlight":
                txt = cdata.get("text", "").lower()
                if "mais vendido" in txt or "best seller" in txt:
                    mais_vendido = True

        # Imagem via pictures
        if not imagem and isinstance(pictures, dict):
            try:
                pics_list = pictures.get("pictures", [])
                if pics_list and isinstance(pics_list[0], dict):
                    pic_id = pics_list[0].get("id", "")
                    if pic_id:
                        imagem = f"https://http2.mlstatic.com/D_NQ_NP_{pic_id}-F.jpg"
            except Exception:
                imagem = ""

        if not nome:
            nome = metadata.get("title", "")
        nome = nome.strip()
        if not nome:
            return None
        if not produto_valido(nome):
            return None
        if not produto_e_tech(nome):
            log(f"  ML nao-tech: {nome[:50]}")
            return None

        if preco <= 0 or preco < PRECO_MINIMO or preco > PRECO_MAXIMO:
            return None

        if preco_orig > preco and desconto == 0:
            desconto = int((1 - preco / preco_orig) * 100)

        if desconto < DESCONTO_MINIMO:
            return None

        frete_txt = "✅ Frete grátis" if frete_ok else "🚚 Frete a calcular"

        try:
            from mercadolivre_link import gerar_link_afiliado_ml
            link_curto = gerar_link_afiliado_ml(url_prod, item_id) or encurtar_link(gerar_link_afiliado(url_prod))
        except Exception:
            link_curto = encurtar_link(gerar_link_afiliado(url_prod))

        log(f"  ✅ {nome[:45]} | R${preco} | {desconto}% {'⭐' if mais_vendido else ''}")

        return {
            "nome":           nome,
            "preco":          round(preco, 2),
            "preco_original": round(preco_orig, 2) if preco_orig > preco else 0,
            "desconto":       desconto,
            "loja":           "MERCADOLIVRE",
            "frete":          frete_txt,
            "link_afiliado":  link_curto,
            "imagem_url":     imagem,
            "score":          3 if mais_vendido else 1,
            "fontes":         ["mercadolivre"],
        }
    except Exception as e:
        import traceback
        log(f"  ML item erro: {e} | {traceback.format_exc()[-300:]}")
        return None


def buscar_todos_produtos():
    if not SCRAPINGANT_KEY and not ZENROWS_KEY and not SCRAPERAPI_KEY:
        log("ML: nenhuma chave de scraping configurada")
        return []

    log("ML ScrapingAnt: iniciando busca...")
    todos       = []
    vistos      = set()
    total_bruto = 0

    urls = random.sample(URLS_BUSCA, min(4, len(URLS_BUSCA)))

    for url, nome in urls:
        try:
            log(f"ML buscando: {nome}")
            html  = scraper_fetch(url)
            items = extrair_produtos_html(html)
            total_bruto += len(items)

            if items and len(todos) == 0:
                processar_item._logged = False

            for item in items:
                p = processar_item(item)
                if p:
                    chave = hashlib.md5(p["nome"].encode()).hexdigest()
                    if chave not in vistos:
                        vistos.add(chave)
                        todos.append(p)
            time.sleep(2)
        except Exception as e:
            log(f"ML erro {nome}: {e}")
            continue

    log(f"Mercado Livre (ScrapingAnt): {total_bruto} brutos → {len(todos)} validos")
    return todos


def buscar_profundo():
    """Busca profunda ML — todas as URLs normais + extras."""
    if not SCRAPINGANT_KEY and not ZENROWS_KEY and not SCRAPERAPI_KEY:
        log("ML profundo: nenhuma chave de scraping configurada")
        return []

    log("ML BUSCA PROFUNDA iniciada...")
    todos       = []
    vistos      = set()
    total_bruto = 0

    todas_urls = URLS_BUSCA + URLS_BUSCA_EXTRA

    for url, nome in todas_urls:
        try:
            log(f"ML profundo buscando: {nome}")
            html  = scraper_fetch(url)
            items = extrair_produtos_html(html)
            total_bruto += len(items)
            for item in items:
                p = processar_item(item)
                if p:
                    chave = hashlib.md5(p["nome"].encode()).hexdigest()
                    if chave not in vistos:
                        vistos.add(chave)
                        todos.append(p)
            time.sleep(1)
        except Exception as e:
            log(f"ML profundo erro {nome}: {e}")
            continue

    log(f"ML BUSCA PROFUNDA: {total_bruto} brutos → {len(todos)} validos")
    return todos
