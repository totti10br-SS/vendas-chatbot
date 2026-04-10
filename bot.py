"""
OlhaissoTech Bot v6.0
- AliExpress API oficial (AppKey: 530504)
- Shopee API oficial (AppID: 18307831002)
- Mercado Livre API pública (Publisher: ot20260326074822)
- Amazon Best Sellers
- Google Trends BR
- TikTok Creative Center
- Reddit gadgets
- Score inteligente por cruzamento de fontes
- Imagem 1080x1080 com logo
- SQLite para evitar repetição de produtos
"""

import os
import time
import hashlib
import logging
import sqlite3
import schedule
import requests
import textwrap
from io import BytesIO
from datetime import datetime, timedelta
from PIL import Image, ImageDraw, ImageFont
from aliexpress_api import buscar_todos_produtos as buscar_aliexpress
from shopee_api import buscar_todos_produtos as buscar_shopee
from mercadolivre_api import buscar_todos_produtos as buscar_ml

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("OlhaissoTech")

# ============================================================
# CONFIGURAÇÕES
# ============================================================
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "8258862380:AAGCr--OpycbKXp6KeqJCU1_piyu4kRl4bk")
TELEGRAM_CHANNEL = os.getenv("TELEGRAM_CHANNEL", "@olhaissotech")
AMAZON_TAG       = os.getenv("AMAZON_TAG", "olhaissotech-20")
PRECO_MAXIMO     = float(os.getenv("PRECO_MAXIMO", "800"))
DESCONTO_MINIMO  = int(os.getenv("DESCONTO_MINIMO", "20"))
POSTS_POR_CICLO  = int(os.getenv("POSTS_POR_CICLO", "8"))
HORARIOS            = ["07:30", "10:00", "12:30", "15:00", "17:30", "20:30", "22:30", "01:00"]
HORARIOS_MISTO      = ["12:30", "20:30"]  # Ciclo misto: metade smartphones + metade monitores
HORARIOS_MONITOR    = ["15:00"]           # Ciclo dedicado apenas monitores
DB_PATH          = os.getenv("DB_PATH", "/data/olhaissotech.db")

# Evolution API — WhatsApp
EVOLUTION_URL      = os.getenv("EVOLUTION_URL", "https://evolution-api-production-b1df.up.railway.app")
EVOLUTION_APIKEY   = os.getenv("EVOLUTION_APIKEY", "A05E4CD20532-4B74-BA78-7FC09B26F2B0")
EVOLUTION_INSTANCE = os.getenv("EVOLUTION_INSTANCE", "OlhaissOTech")
WHATSAPP_GROUP_ID  = os.getenv("WHATSAPP_GROUP_ID", "120363409953330235@g.us")

# Quantos dias manter um produto no histórico antes de poder repetir
DIAS_SEM_REPETIR = int(os.getenv("DIAS_SEM_REPETIR", "2"))  # mantido por compatibilidade
HORAS_SEM_REPETIR = int(os.getenv("HORAS_SEM_REPETIR", str(DIAS_SEM_REPETIR * 24)))

KEYWORDS_NICHO = [
    "gadget", "fone", "carregador", "teclado", "mouse", "câmera",
    "smartwatch", "speaker", "bluetooth", "usb", "led", "robô",
    "aspirador", "fritadeira", "airfryer", "projetor", "drone",
    "cabo", "hub", "power bank", "earphone", "headset", "webcam",
    "luminária", "lâmpada", "tomada inteligente", "smart home",
    "impressora", "tablet", "celular", "notebook", "monitor",
    "suporte", "cooler", "rgb", "gamer", "pen drive", "ssd"
]

# ============================================================
# CORES
# ============================================================
COR_FUNDO         = (17, 17, 17)
COR_CARD          = (26, 26, 26)
COR_LARANJA       = (255, 107, 26)
COR_LARANJA_CLARO = (255, 154, 74)
COR_VERDE         = (0, 187, 68)
COR_BRANCO        = (255, 255, 255)
COR_CINZA         = (136, 136, 136)
COR_CINZA_ESCURO  = (45, 45, 45)

# ============================================================
# BANCO DE DADOS SQLite
# ============================================================

def init_db():
    """Cria o banco de dados e tabelas se não existirem."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS postados (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            hash      TEXT UNIQUE NOT NULL,
            nome      TEXT,
            preco     REAL,
            loja      TEXT,
            postado_em TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()
    log.info(f"SQLite iniciado: {DB_PATH}")


def ja_postado(hash_produto):
    """Verifica se produto foi postado nas últimas HORAS_SEM_REPETIR horas."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        limite = (datetime.now() - timedelta(hours=HORAS_SEM_REPETIR)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute("SELECT id FROM postados WHERE hash = ? AND postado_em > ?", (hash_produto, limite))
        resultado = c.fetchone()
        conn.close()
        return resultado is not None
    except Exception as e:
        log.error(f"SQLite erro ao verificar: {e}")
        return False


def registrar_post(produto):
    """Registra produto postado no banco."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        hash_p = hashlib.md5(produto["nome"].encode()).hexdigest()
        agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("""
            INSERT OR REPLACE INTO postados (hash, nome, preco, loja, postado_em)
            VALUES (?, ?, ?, ?, ?)
        """, (hash_p, produto["nome"][:200], produto.get("preco", 0),
              produto.get("loja", ""), agora))
        conn.commit()
        conn.close()
    except Exception as e:
        log.error(f"SQLite erro ao registrar: {e}")


def limpar_historico_antigo():
    """Remove registros mais antigos que HORAS_SEM_REPETIR * 2 horas."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        limite = (datetime.now() - timedelta(hours=HORAS_SEM_REPETIR * 2)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute("DELETE FROM postados WHERE postado_em < ?", (limite,))
        removidos = c.rowcount
        conn.commit()
        conn.close()
        if removidos > 0:
            log.info(f"SQLite: {removidos} registros antigos removidos")
    except Exception as e:
        log.error(f"SQLite erro ao limpar: {e}")


def contar_postados():
    """Retorna total de produtos no histórico."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM postados")
        total = c.fetchone()[0]
        conn.close()
        return total
    except:
        return 0

# ============================================================
# HELPERS
# ============================================================

def fmt_preco(valor):
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_economia(orig, atual):
    eco = round(orig - atual, 2)
    return fmt_preco(eco) if eco > 0 else None


def carregar_fonte(tamanho, negrito=False):
    nomes = [
        f"/usr/share/fonts/truetype/dejavu/DejaVuSans{'-Bold' if negrito else ''}.ttf",
        f"/usr/share/fonts/truetype/liberation/LiberationSans{'-Bold' if negrito else '-Regular'}.ttf",
    ]
    for nome in nomes:
        try:
            return ImageFont.truetype(nome, tamanho)
        except:
            continue
    return ImageFont.load_default()


def badge_score(score):
    if score >= 3:
        return ("🔥 VIRAL AGORA", COR_LARANJA)
    elif score == 2:
        return ("📈 TENDÊNCIA", (0, 150, 200))
    else:
        return ("💰 OFERTA", COR_VERDE)


def desenhar_logo(draw, x, y, tamanho=34):
    r = tamanho // 2
    esp = tamanho + 6
    for i in range(2):
        cx = x + i * esp + r
        cy = y + r
        draw.ellipse([cx-r, cy-r, cx+r, cy+r], outline=COR_LARANJA, width=3)
        ri = int(r * 0.55)
        draw.ellipse([cx-ri, cy-ri, cx+ri, cy+ri], fill=COR_LARANJA_CLARO)
        rp = int(r * 0.25)
        draw.ellipse([cx-rp, cy-rp, cx+rp, cy+rp], fill=COR_FUNDO)
        rb = int(r * 0.14)
        draw.ellipse([cx+int(r*0.12)-rb, cy-int(r*0.2)-rb,
                      cx+int(r*0.12)+rb, cy-int(r*0.2)+rb], fill=COR_BRANCO)

# ============================================================
# GERADOR DE IMAGEM
# ============================================================

def gerar_imagem(produto):
    W, H = 1080, 1080
    img = Image.new("RGB", (W, H), COR_FUNDO)
    draw = ImageDraw.Draw(img)

    # ── TOPO: badge único grande centralizado ──────────────────
    label_score, cor_score = badge_score(produto.get("score", 0))
    f_badge = carregar_fonte(54, negrito=True)
    bw, bh = 580, 90
    bx = (W - bw) // 2
    draw.rounded_rectangle([bx, 24, bx+bw, 24+bh], radius=45, fill=cor_score)
    bb = draw.textbbox((0,0), label_score, font=f_badge)
    draw.text((bx+(bw-(bb[2]-bb[0]))//2, 24+(bh-(bb[3]-bb[1]))//2), label_score, font=f_badge, fill=COR_BRANCO)

    # ── BADGES LOJA + DESCONTO lado a lado ────────────────────
    f_sub_badge = carregar_fonte(44, negrito=True)
    loja = produto.get("loja", "ALIEXPRESS")
    desc = produto.get("desconto", 0)

    # Badge loja (esquerda)
    draw.rounded_rectangle([50, 130, 50+300, 130+72], radius=36, fill=(40,40,40))
    bb2 = draw.textbbox((0,0), loja, font=f_sub_badge)
    draw.text((50+(300-(bb2[2]-bb2[0]))//2, 130+(72-(bb2[3]-bb2[1]))//2), loja, font=f_sub_badge, fill=COR_LARANJA)

    # Badge desconto (direita)
    if desc > 0:
        desc_txt = f"-{desc}% OFF"
        draw.rounded_rectangle([W-50-300, 130, W-50, 130+72], radius=36, fill=COR_VERDE)
        bb3 = draw.textbbox((0,0), desc_txt, font=f_sub_badge)
        draw.text((W-50-300+(300-(bb3[2]-bb3[0]))//2, 130+(72-(bb3[3]-bb3[1]))//2), desc_txt, font=f_sub_badge, fill=COR_BRANCO)

    # ── FOTO DO PRODUTO ────────────────────────────────────────
    img_url = produto.get("imagem_url", "")
    prod_img = None
    if img_url:
        try:
            r = requests.get(img_url, timeout=8)
            prod_img = Image.open(BytesIO(r.content)).convert("RGBA")
            prod_img = prod_img.resize((600, 460), Image.LANCZOS)
        except:
            prod_img = None

    if prod_img:
        img.paste(prod_img, ((W-600)//2, 215), prod_img)
    else:
        draw.rounded_rectangle([240, 215, 840, 675], radius=20, fill=COR_CINZA_ESCURO)
        f_ico = carregar_fonte(100)
        draw.text((540, 445), "📦", font=f_ico, fill=(70,70,70), anchor="mm")

    # ── ÁREA DE TEXTO: fundo escuro arredondado ────────────────
    draw.rounded_rectangle([32, 690, W-32, H-120], radius=28, fill=(22,22,22))

    y = 706

    # 👀 OlhaissO
    f_marca = carregar_fonte(50, negrito=True)
    marca_txt = "👀 OlhaissO"
    bb_m = draw.textbbox((0,0), marca_txt, font=f_marca)
    draw.text(((W-(bb_m[2]-bb_m[0]))//2, y), marca_txt, font=f_marca, fill=COR_LARANJA)
    y += 66

    # Nome do produto
    f_nome = carregar_fonte(48, negrito=True)
    nome = produto.get("nome", "")

    # Detecta se é Oferta Premium do Canal — banner especial
    PREFIXO_PREMIUM = "🏆 OFERTA PREMIUM DO CANAL"
    if nome.startswith(PREFIXO_PREMIUM):
        # Banner vermelho com texto azul negrito
        COR_VERMELHO    = (200, 20, 20)
        COR_AZUL_BRIGHT = (30, 120, 255)
        f_premium = carregar_fonte(46, negrito=True)
        banner_txt = "🏆 OFERTA PREMIUM DO CANAL"
        bb_p = draw.textbbox((0, 0), banner_txt, font=f_premium)
        bw_p = bb_p[2] - bb_p[0]
        bh_p = bb_p[3] - bb_p[1]
        pad_x, pad_y = 30, 12
        rx1 = (W - bw_p) // 2 - pad_x
        rx2 = (W + bw_p) // 2 + pad_x
        draw.rounded_rectangle([rx1, y, rx2, y + bh_p + pad_y * 2], radius=16, fill=COR_VERMELHO)
        draw.text(((W - bw_p) // 2, y + pad_y), banner_txt, font=f_premium, fill=COR_AZUL_BRIGHT)
        y += bh_p + pad_y * 2 + 10
        # Resto do nome sem o prefixo
        nome_real = nome[len(PREFIXO_PREMIUM):].strip().lstrip("\n").strip()
        linhas = textwrap.wrap(nome_real, width=32)[:2]
    else:
        linhas = textwrap.wrap(nome, width=32)[:2]

    for linha in linhas:
        bb = draw.textbbox((0,0), linha, font=f_nome)
        draw.text(((W-(bb[2]-bb[0]))//2, y), linha, font=f_nome, fill=COR_BRANCO)
        y += 60

    y += 8

    # Preço original riscado
    preco_orig = produto.get("preco_original", 0)
    preco = produto.get("preco", 0)
    if preco_orig > preco:
        f_old = carregar_fonte(40)
        txt_old = f"De {fmt_preco(preco_orig)}"
        bb = draw.textbbox((0,0), txt_old, font=f_old)
        tw = bb[2]-bb[0]
        x_old = (W-tw)//2
        draw.text((x_old, y), txt_old, font=f_old, fill=COR_CINZA)
        meio_y = y + (bb[3]-bb[1])//2
        draw.line([(x_old, meio_y), (x_old+tw, meio_y)], fill=COR_CINZA, width=2)
        y += 54

    # Preço atual — destaque máximo
    f_preco = carregar_fonte(88, negrito=True)
    txt_preco = fmt_preco(preco)
    bb = draw.textbbox((0,0), txt_preco, font=f_preco)
    draw.text(((W-(bb[2]-bb[0]))//2, y), txt_preco, font=f_preco, fill=COR_LARANJA)
    y += 96

    # Frete
    frete = produto.get("frete", "")
    if frete:
        f_frete = carregar_fonte(40)
        bb = draw.textbbox((0,0), frete, font=f_frete)
        draw.text(((W-(bb[2]-bb[0]))//2, y), frete, font=f_frete, fill=COR_VERDE)

    # ── RODAPÉ LARANJA ─────────────────────────────────────────
    draw.rounded_rectangle([32, H-108, W-32, H-28], radius=30, fill=COR_LARANJA)
    desenhar_logo(draw, 58, H-96, tamanho=32)
    f_cta = carregar_fonte(38, negrito=True)
    cta = "OlhaissoTech — Link na bio e no Telegram!"
    bb = draw.textbbox((0,0), cta, font=f_cta)
    draw.text(((W-(bb[2]-bb[0]))//2 + 20, H-84), cta, font=f_cta, fill=COR_BRANCO)

    path = f"/tmp/oferta_{hashlib.md5(nome.encode()).hexdigest()[:8]}.jpg"
    img.save(path, "JPEG", quality=93)
    return path


# ============================================================
# TELEGRAM
# ============================================================


def escapar_html(texto):
    """Escapa caracteres especiais do HTML do Telegram."""
    return texto.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def montar_caption(produto):
    nome   = escapar_html(produto.get("nome", ""))
    preco  = produto.get("preco", 0)
    orig   = produto.get("preco_original", 0)
    desc   = produto.get("desconto", 0)
    link   = produto.get("link_afiliado", "")
    loja   = produto.get("loja", "")
    frete  = produto.get("frete", "")
    score  = produto.get("score", 0)
    fontes = produto.get("fontes", [])

    eco = fmt_economia(orig, preco) if orig > preco else None

    # Badge de urgência por score
    if score >= 3:
        badge = "🔥 <b>VIRAL AGORA</b>"
    elif score == 2:
        badge = "📈 <b>TENDÊNCIA</b>"
    else:
        badge = "💰 <b>OFERTA DO DIA</b>"

    # Badge de loja
    loja_badge = {"ALIEXPRESS": "🛍️ AliExpress", "SHOPEE": "🧡 Shopee", "AMAZON": "📦 Amazon"}.get(loja, loja)

    txt  = f"👀 <b>OlhaissO</b> — {badge}\n"
    txt += f"━━━━━━━━━━━━━━━━━━━━\n\n"
    txt += f"<b>{nome}</b>\n\n"

    if desc > 0:
        txt += f"🏷️ <b>{desc}% OFF</b>"
        if eco:
            txt += f"  |  Economia de <b>{eco}</b>"
        txt += "\n"

    txt += f"\n💵 De <s>{fmt_preco(orig)}</s> por apenas\n"
    txt += f"💰 <b>{fmt_preco(preco)}</b>\n\n"

    txt += f"{loja_badge}"
    if frete:
        txt += f"  •  {frete}"
    txt += "\n"

    if fontes:
        labels = {"google": "Google Trends", "tiktok": "TikTok", "reddit": "Reddit", "amazon": "Amazon", "aliexpress": "AliExpress", "shopee": "Shopee"}
        txt += f"📊 Em alta: {' · '.join([labels.get(f,f) for f in fontes])}\n"

    txt += f"\n🛒 <a href=\"{link}\"><b>COMPRAR AGORA — CLIQUE AQUI</b></a>\n"
    txt += f"\n━━━━━━━━━━━━━━━━━━━━\n"
    txt += f"<i>👀 OlhaissoTech | Gadgets com o melhor preço</i>"
    return txt


def postar_telegram(produto, imagem_path):
    caption = montar_caption(produto)
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    img_url = produto.get("imagem_url", "")

    # Tenta 1: foto real do produto via URL
    if img_url:
        try:
            r = requests.post(url, data={
                "chat_id": TELEGRAM_CHANNEL,
                "photo": img_url,
                "caption": caption,
                "parse_mode": "HTML",
            }, timeout=30)
            if r.status_code == 200:
                return True
            log.warning(f"Imagem URL falhou ({r.status_code}), tentando imagem gerada...")
        except Exception as e:
            log.warning(f"Imagem URL exceção: {e}, tentando imagem gerada...")

    # Tenta 2: imagem gerada localmente (PIL)
    try:
        with open(imagem_path, "rb") as f:
            r = requests.post(url, data={
                "chat_id": TELEGRAM_CHANNEL,
                "caption": caption,
                "parse_mode": "HTML",
            }, files={"photo": f}, timeout=30)
        if r.status_code == 200:
            return True
        log.error(f"Telegram erro imagem gerada: {r.text[:200]}")
    except Exception as e:
        log.error(f"Telegram exceção imagem gerada: {e}")

    # Sem imagem = não publica, produto será registrado no banco e pulado
    log.warning(f"Produto pulado por falha de imagem: {produto.get('nome','')[:50]}")
    return False


def fazer_upload_imagem(imagem_path):
    """Faz upload da imagem gerada para imgbb e retorna URL pública."""
    IMGBB_KEY = os.getenv("IMGBB_KEY", "")
    if not IMGBB_KEY:
        log.warning("imgbb: IMGBB_KEY não configurada!")
        return None
    try:
        import base64
        with open(imagem_path, "rb") as f:
            img_b64 = base64.b64encode(f.read()).decode("utf-8")
        r = requests.post(
            "https://api.imgbb.com/1/upload",
            data={"key": IMGBB_KEY, "image": img_b64},
            timeout=20
        )
        log.info(f"imgbb status: {r.status_code}")
        if r.status_code == 200:
            url = r.json()["data"]["url"]
            log.info(f"imgbb URL: {url}")
            return url
        log.warning(f"imgbb erro: {r.text[:200]}")
    except Exception as e:
        log.warning(f"imgbb upload falhou: {e}")
    return None


def postar_whatsapp(produto, imagem_path):
    """Posta no grupo WhatsApp via Evolution API. Falha silenciosa — não afeta o Telegram."""
    if not EVOLUTION_URL or not WHATSAPP_GROUP_ID:
        return

    try:
        nome    = produto.get("nome", "")
        preco   = produto.get("preco", 0)
        orig    = produto.get("preco_original", 0)
        desc    = produto.get("desconto", 0)
        link    = produto.get("link_afiliado", "")
        loja    = produto.get("loja", "")
        img_url = produto.get("imagem_url", "")

        loja_label = {"ALIEXPRESS": "🛍️ AliExpress", "SHOPEE": "🧡 Shopee", "AMAZON": "📦 Amazon"}.get(loja, loja)
        badge = "🔥 VIRAL AGORA" if produto.get("score", 0) >= 3 else "📈 TENDÊNCIA" if produto.get("score", 0) == 2 else "💰 OFERTA DO DIA"

        def fmt(v):
            return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        # Caption limpo para WhatsApp — sem HTML
        texto  = f"👀 *OlhaissO* — {badge}\n"
        texto += f"━━━━━━━━━━━━━━━━━━━━\n\n"
        texto += f"*{nome}*\n\n"
        if desc > 0:
            eco = round(orig - preco, 2)
            texto += f"🏷️ *{desc}% OFF*  |  Economia de *{fmt(eco)}*\n"
        if orig > preco:
            texto += f"\n💵 De {fmt(orig)} por apenas\n"
        texto += f"💰 *{fmt(preco)}*\n\n"
        texto += f"{loja_label}\n"
        texto += f"\n🛒 *COMPRAR AGORA:*\n{link}\n"
        texto += f"\n━━━━━━━━━━━━━━━━━━━━\n"
        texto += f"👀 OlhaissoTech | Gadgets com o melhor preço"

        headers = {
            "apikey": EVOLUTION_APIKEY,
            "Content-Type": "application/json",
        }

        # Tenta com imagem via URL do produto
        if img_url:
            payload = {
                "number": WHATSAPP_GROUP_ID,
                "mediatype": "image",
                "mimetype": "image/jpeg",
                "caption": texto,
                "media": img_url,
            }
            r = requests.post(
                f"{EVOLUTION_URL}/message/sendMedia/{EVOLUTION_INSTANCE}",
                json=payload, headers=headers, timeout=30
            )
            if r.status_code in (200, 201):
                log.info("✅ WhatsApp postado com imagem!")
                return
            log.warning(f"WhatsApp imagem falhou ({r.status_code}), postando só texto...")

        # Fallback: posta só o texto sem imagem
        payload_txt = {
            "number": WHATSAPP_GROUP_ID,
            "text": texto,
        }
        r = requests.post(
            f"{EVOLUTION_URL}/message/sendText/{EVOLUTION_INSTANCE}",
            json=payload_txt, headers=headers, timeout=30
        )
        if r.status_code in (200, 201):
            log.info("✅ WhatsApp postado (só texto)!")
        else:
            log.warning(f"WhatsApp texto falhou: {r.text[:150]}")

    except Exception as e:
        log.warning(f"WhatsApp exceção (ignorada): {e}")

def buscar_trends_google():
    try:
        from pytrends.request import TrendReq
        pt = TrendReq(hl="pt-BR", tz=-180, timeout=(10, 25))
        termos = []
        for i in range(0, len(KEYWORDS_NICHO), 5):
            lote = KEYWORDS_NICHO[i:i+5]
            try:
                pt.build_payload(lote, geo="BR", timeframe="now 1-d")
                dados = pt.interest_over_time()
                if dados.empty:
                    continue
                media = dados[lote].mean()
                for t in lote:
                    if media.get(t, 0) > 40:
                        termos.append(t)
                time.sleep(1.5)
            except:
                continue
        log.info(f"Google Trends: {len(termos)} termos")
        return termos
    except:
        return []


def buscar_reddit_gadgets():
    headers = {"User-Agent": "OlhaissoTechBot/6.0"}
    termos = []
    for sub in ["gadgets", "BuyItForLife"]:
        try:
            r = requests.get(f"https://www.reddit.com/r/{sub}/hot.json?limit=25", headers=headers, timeout=10)
            if r.status_code != 200:
                continue
            for post in r.json().get("data", {}).get("children", []):
                titulo = post["data"].get("title", "").lower()
                if post["data"].get("score", 0) < 500:
                    continue
                for kw in KEYWORDS_NICHO:
                    if kw in titulo:
                        termos.append(kw)
            time.sleep(1)
        except:
            continue
    return list(set(termos))


def buscar_tiktok_trending():
    try:
        url = "https://ads.tiktok.com/creative_radar_api/v1/top_product/list"
        params = {"period": 7, "page": 1, "limit": 20, "region": "BR", "category_id": 0}
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://ads.tiktok.com/business/creativecenter/"}
        r = requests.get(url, params=params, headers=headers, timeout=10)
        if r.status_code == 200:
            termos = []
            for item in r.json().get("data", {}).get("list", []):
                nome = item.get("product_name", "").lower()
                for kw in KEYWORDS_NICHO:
                    if kw in nome:
                        termos.append(kw)
            return list(set(termos))
    except:
        pass
    return []


def buscar_amazon_best_sellers():
    categorias = [
        ("Eletrônicos", "https://www.amazon.com.br/gp/bestsellers/electronics"),
        ("Informática", "https://www.amazon.com.br/gp/bestsellers/computers"),
    ]
    headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "pt-BR,pt;q=0.9"}
    produtos = []
    for nome_cat, url in categorias:
        try:
            r = requests.get(url, headers=headers, timeout=12)
            if r.status_code != 200:
                continue
            from html.parser import HTMLParser
            class P(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self.items = []
                    self._t = False
                    self._p = False
                    self._c = {}
                def handle_starttag(self, tag, attrs):
                    cls = dict(attrs).get("class", "")
                    if "p13n-sc-truncate" in cls: self._t = True
                    if "p13n-sc-price" in cls: self._p = True
                def handle_data(self, data):
                    data = data.strip()
                    if not data: return
                    if self._t and len(data) > 8:
                        self._c["nome"] = data; self._t = False
                    if self._p and "R$" in data:
                        self._c["preco_txt"] = data
                        if "nome" in self._c:
                            self.items.append(dict(self._c)); self._c = {}
                        self._p = False
            p = P(); p.feed(r.text)
            for item in p.items[:8]:
                try:
                    preco = float(item["preco_txt"].replace("R$","").replace(".","").replace(",",".").strip())
                except:
                    preco = 0
                if preco <= 0 or preco > PRECO_MAXIMO: continue
                asin = hashlib.md5(item["nome"].encode()).hexdigest()[:10]
                produtos.append({
                    "nome": item["nome"], "preco": preco,
                    "preco_original": round(preco * 1.3, 2), "desconto": 23,
                    "loja": "AMAZON", "frete": "✅ Frete grátis Prime",
                    "link_afiliado": f"https://www.amazon.com.br/dp/{asin}?tag={AMAZON_TAG}",
                    "imagem_url": "", "score": 1, "fontes": ["amazon"],
                })
            time.sleep(2)
        except:
            continue
    return produtos


def calcular_score(produto, tg, tt, tr):
    score = produto.get("score", 0)
    nome = produto.get("nome", "").lower()
    fontes = produto.get("fontes", [])
    for kw in KEYWORDS_NICHO:
        if kw not in nome: continue
        if kw in tg and "google" not in fontes: score += 1; fontes.append("google")
        if kw in tt and "tiktok" not in fontes: score += 1; fontes.append("tiktok")
        if kw in tr and "reddit" not in fontes: score += 1; fontes.append("reddit")
    produto["score"] = score
    produto["fontes"] = fontes
    return produto


def produtos_mock():
    return [
        {"nome": "Fone Bluetooth TWS 5.3 com cancelamento de ruído", "preco": 72.90, "preco_original": 189.90, "desconto": 62, "loja": "ALIEXPRESS", "frete": "🚢 Frete grátis", "link_afiliado": "https://s.click.aliexpress.com/e/_exemplo", "imagem_url": "", "score": 3, "fontes": ["aliexpress", "google", "tiktok"]},
        {"nome": "Carregador GaN 65W turbo 3 portas USB-C", "preco": 89.90, "preco_original": 189.00, "desconto": 52, "loja": "ALIEXPRESS", "frete": "🚢 Frete grátis", "link_afiliado": "https://s.click.aliexpress.com/e/_exemplo2", "imagem_url": "", "score": 2, "fontes": ["aliexpress", "tiktok"]},
        {"nome": "Aspirador robô Wi-Fi com mapeamento automático", "preco": 249.90, "preco_original": 499.90, "desconto": 50, "loja": "AMAZON", "frete": "✅ Frete grátis Prime", "link_afiliado": "https://amzn.to/exemplo", "imagem_url": "", "score": 3, "fontes": ["amazon", "google", "reddit"]},
    ]



KEYWORDS_SMARTPHONE = [
    "smartphone", "telefone", "celular", "iphone", "samsung galaxy",
    "redmi", "poco", "motorola moto", "realme", "xiaomi mi",
]

MAX_POR_TEMA = int(os.getenv("MAX_POR_TEMA", "2"))

TEMAS = [
    ("smartphone",   ["smartphone", "celular", "iphone", "redmi", "poco", "motorola moto", "realme", "samsung galaxy"]),
    ("notebook",     ["notebook", "laptop"]),
    ("fone",         ["fone", "earbuds", "earphone", "headset", "headphone"]),
    ("smartwatch",   ["smartwatch", "smart watch", "relogio inteligente", "band "]),
    ("carregador",   ["carregador", "power bank", "powerbank", "gan charger"]),
    ("mouse",        ["mouse "]),
    ("teclado",      ["teclado", "keyboard"]),
    ("caixa de som", ["caixa de som", "bluetooth speaker"]),
    ("monitor",      ["monitor "]),
    ("aspirador",    ["aspirador", "robot vacuum"]),
    ("fritadeira",   ["fritadeira", "air fryer", "airfryer"]),
    ("projetor",     ["projetor", "projector"]),
    ("hub usb",      ["hub usb", "docking station"]),
    ("ssd",          ["ssd", "hd externo", "pendrive"]),
    ("webcam",       ["webcam"]),
    ("led",          ["fita led", "led strip", "luminaria", "lampada"]),
]

def hora_atual_str():
    return datetime.now().strftime("%H:%M")

def horario_dentro_de(horarios, tolerancia_min=30):
    """Verifica se a hora atual está dentro da tolerância de algum horário da lista."""
    agora = datetime.now()
    for h in horarios:
        hh, mm = map(int, h.split(":"))
        alvo = agora.replace(hour=hh, minute=mm, second=0, microsecond=0)
        diff = abs((agora - alvo).total_seconds() / 60)
        if diff <= tolerancia_min:
            return True
    return False

def permitir_misto():
    return horario_dentro_de(HORARIOS_MISTO)

def permitir_monitor_dedicado():
    return horario_dentro_de(HORARIOS_MONITOR)

KEYWORDS_MONITOR = [
    "monitor ", "monitor gamer", "monitor 4k", "monitor ips",
    "monitor curvo", "monitor portatil", "monitor led",
    "monitor 144hz", "monitor 165hz", "monitor 240hz",
    "tela monitor", "display monitor",
]

def montar_ciclo_misto(produtos):
    """
    Ciclo misto (12:30 e 20:30): metade smartphones + metade monitores.
    Cada metade = POSTS_POR_CICLO // 2 produtos.
    """
    metade = POSTS_POR_CICLO // 2
    smartphones = [p for p in produtos if any(kw in p.get("nome", "").lower() for kw in KEYWORDS_SMARTPHONE)]
    monitores   = [p for p in produtos if any(kw in p.get("nome", "").lower() for kw in KEYWORDS_MONITOR)]
    outros      = [p for p in produtos if p not in smartphones and p not in monitores]

    log.info(f"🔀 CICLO MISTO — {metade} smartphone(s) + {metade} monitor(es)")
    log.info(f"   Disponíveis: {len(smartphones)} smartphones | {len(monitores)} monitores")

    # Pega metade de cada, completa com outros se faltar
    resultado = smartphones[:metade] + monitores[:metade]
    faltando = POSTS_POR_CICLO - len(resultado)
    if faltando > 0:
        resultado += outros[:faltando]
        if faltando > 0:
            log.info(f"   Completado com {min(faltando, len(outros))} produto(s) genérico(s)")
    return resultado

def filtrar_ciclo_especial(produtos):
    """
    Horário misto   → retorna metade smartphones + metade monitores.
    Horário monitor → retorna apenas monitores.
    Horário normal  → remove smartphones e monitores do ciclo.
    """
    if permitir_misto():
        return montar_ciclo_misto(produtos)

    if permitir_monitor_dedicado():
        log.info("🖥️ CICLO DEDICADO — apenas Monitores")
        resultado = [p for p in produtos if any(kw in p.get("nome", "").lower() for kw in KEYWORDS_MONITOR)]
        log.info(f"🖥️ {len(resultado)} monitor(es) disponíveis para o ciclo")
        return resultado

    # Ciclo normal — remove smartphones e monitores
    antes = len(produtos)
    resultado = []
    for p in produtos:
        nome_lower = p.get("nome", "").lower()
        if any(kw in nome_lower for kw in KEYWORDS_SMARTPHONE):
            continue
        if any(kw in nome_lower for kw in KEYWORDS_MONITOR):
            continue
        resultado.append(p)
    removidos = antes - len(resultado)
    if removidos > 0:
        log.info(f"🔒 {removidos} produto(s) reservados para ciclos especiais")
    return resultado

def detectar_tema(nome):
    nome_lower = nome.lower()
    for tema, keywords in TEMAS:
        if any(kw in nome_lower for kw in keywords):
            return tema
    return "outros"

def limitar_por_tema(produtos):
    """Limita MAX_POR_TEMA produtos por tema no ciclo."""
    contagem = {}
    resultado = []
    pulados = []
    for p in produtos:
        tema = detectar_tema(p.get("nome", ""))
        count = contagem.get(tema, 0)
        if tema == "outros" or count < MAX_POR_TEMA:
            contagem[tema] = count + 1
            resultado.append(p)
        else:
            pulados.append(tema)
    if pulados:
        log.info(f"🎯 Limite por tema ({MAX_POR_TEMA}/tema): {len(pulados)} removido(s) — {', '.join(set(pulados))}")
    return resultado

def montar_pipeline():
    log.info("=== Pipeline v6.0 iniciado ===")
    tg = buscar_trends_google()
    tt = buscar_tiktok_trending()
    tr = buscar_reddit_gadgets()
    log.info(f"Tendências — Google: {len(tg)} | TikTok: {len(tt)} | Reddit: {len(tr)}")

    log.info("Buscando AliExpress API...")
    produtos_ali = buscar_aliexpress()

    log.info("Buscando Shopee API...")
    produtos_shopee = buscar_shopee()

    log.info("Buscando Mercado Livre API...")
    produtos_ml = buscar_ml()

    log.info("Buscando Amazon Best Sellers...")
    produtos_amazon = buscar_amazon_best_sellers()

    # Calcula cota proporcional por fonte (33% cada)
    cota = POSTS_POR_CICLO // 3
    sobra = POSTS_POR_CICLO - (cota * 3)  # ex: 6//3=2, sobra=0; 8//3=2, sobra=2

    log.info(f"Rodízio proporcional: {cota} AliExpress | {cota} Shopee | {cota + sobra} ML (+ sobra)")

    # Aplica score em todos
    todos_raw = produtos_ali + produtos_shopee + produtos_ml + produtos_amazon
    for p in todos_raw:
        calcular_score(p, tg, tt, tr)

    if not todos_raw:
        log.warning("Sem produtos — usando mock")
        todos_raw = produtos_mock()

    # Filtra/organiza por ciclo especial ou normal
    todos_raw = filtrar_ciclo_especial(todos_raw)

    # Filtra por preço e desconto
    todos_raw = [p for p in todos_raw if p.get("preco", 999) <= PRECO_MAXIMO and p.get("desconto", 0) >= DESCONTO_MINIMO]

    # Remove já postados
    antes = len(todos_raw)
    todos_raw = [p for p in todos_raw if not ja_postado(hashlib.md5(p["nome"].encode()).hexdigest())]
    filtrados = antes - len(todos_raw)
    if filtrados > 0:
        log.info(f"SQLite: {filtrados} produtos já postados recentemente removidos")

    # Separa por fonte e ordena por score
    def filtrar_fonte(lista, loja):
        return sorted([p for p in lista if p.get("loja") == loja], key=lambda x: x.get("score", 0), reverse=True)

    pool_ali    = filtrar_fonte(todos_raw, "ALIEXPRESS")
    pool_shopee = filtrar_fonte(todos_raw, "SHOPEE")
    pool_ml     = filtrar_fonte(todos_raw, "MERCADOLIVRE")
    pool_outros = [p for p in todos_raw if p.get("loja") not in ("ALIEXPRESS", "SHOPEE", "MERCADOLIVRE")]

    # Monta fila proporcional — intercala as fontes
    fila = []
    idx_ali = idx_shopee = idx_ml = 0

    # Intercala: 1 Ali, 1 Shopee, 1 ML repetindo
    ordem = ["ALIEXPRESS", "SHOPEE", "MERCADOLIVRE"] * (POSTS_POR_CICLO + 3)
    for loja_alvo in ordem:
        if len(fila) >= POSTS_POR_CICLO * 2:
            break
        if loja_alvo == "ALIEXPRESS" and idx_ali < len(pool_ali):
            fila.append(pool_ali[idx_ali]); idx_ali += 1
        elif loja_alvo == "SHOPEE" and idx_shopee < len(pool_shopee):
            fila.append(pool_shopee[idx_shopee]); idx_shopee += 1
        elif loja_alvo == "MERCADOLIVRE" and idx_ml < len(pool_ml):
            fila.append(pool_ml[idx_ml]); idx_ml += 1

    # Completa com outros se faltar
    for p in pool_outros:
        if len(fila) >= POSTS_POR_CICLO * 2:
            break
        fila.append(p)

    # Remove duplicatas mantendo ordem
    vistos = set()
    resultado = []
    for p in fila:
        chave = hashlib.md5(p["nome"].encode()).hexdigest()
        if chave not in vistos:
            vistos.add(chave)
            resultado.append(p)

    # Limita MAX_POR_TEMA produtos por tema no ciclo
    resultado = limitar_por_tema(resultado)

    lojas_log = {}
    for p in resultado[:POSTS_POR_CICLO]:
        lojas_log[p.get("loja","")] = lojas_log.get(p.get("loja",""), 0) + 1

    log.info(f"Pipeline: {len(resultado)} produtos prontos ({contar_postados()} no historico)")
    log.info(f"Distribuicao: {lojas_log}")
    for p in resultado[:8]:
        log.info(f"  [{p['score']}pts][{p['loja']}] {p['nome'][:40]} | {fmt_preco(p['preco'])}")
    return resultado


# ============================================================
# CICLO PRINCIPAL
# ============================================================

def ciclo():
    log.info(f"\n{'='*50}")
    log.info(f"Ciclo — {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    limpar_historico_antigo()
    produtos = montar_pipeline()
    postou = 0
    tentativas = 0
    max_tentativas = 3  # tenta até 3 vezes buscar mais produtos se faltar

    while postou < POSTS_POR_CICLO and tentativas < max_tentativas:
        if not produtos:
            tentativas += 1
            if tentativas < max_tentativas:
                log.warning(f"Sem produtos disponíveis, buscando novamente (tentativa {tentativas})...")
                produtos = montar_pipeline()
                continue
            else:
                log.warning("Sem produtos suficientes após 3 tentativas.")
                break

        produto = produtos.pop(0)
        log.info(f"Postando [{produto['score']}pts]: {produto['nome'][:50]}")
        imagem = gerar_imagem(produto)
        ok = postar_telegram(produto, imagem)
        if ok:
            registrar_post(produto)
            postou += 1
            log.info("✅ Postado!")
            postar_whatsapp(produto, imagem)
        else:
            registrar_post(produto)
            log.warning("⏭️ Pulado e registrado (falha de imagem)")
        time.sleep(10)

    log.info(f"Ciclo concluído — {postou} post(s)\n")


def main():
    init_db()
    log.info("🤖 OlhaissoTech Bot v6.0 iniciado!")
    log.info(f"📢 Canal: {TELEGRAM_CHANNEL}")
    log.info(f"⏰ Horários: {', '.join(HORARIOS)}")
    log.info(f"📦 Posts por ciclo: {POSTS_POR_CICLO}")
    log.info(f"🎯 Máx por tema: {MAX_POR_TEMA}")
    log.info(f"🗓️ Sem repetir por: {HORAS_SEM_REPETIR} horas\n")
    for h in HORARIOS:
        schedule.every().day.at(h).do(ciclo)
    log.info("⏳ Aguardando próximo horário agendado...")
    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
