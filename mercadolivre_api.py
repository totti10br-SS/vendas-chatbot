"""
Mercado Livre Affiliates — integração via API pública
Publisher ID: ot20260326074822
Busca produtos com desconto nas categorias de tech do ML Brasil
"""

import os
import random
import hashlib
import time
import requests

ML_PUBLISHER_ID = os.getenv("ML_PUBLISHER_ID", "ot20260326074822")
ML_BASE_URL     = "https://api.mercadolibre.com"

PRECO_MINIMO    = float(os.getenv("PRECO_MINIMO", "50.00"))
PRECO_MAXIMO    = float(os.getenv("PRECO_MAXIMO", "3000.00"))
DESCONTO_MINIMO = int(os.getenv("DESCONTO_MINIMO", "20"))

# Categorias de tech do ML Brasil
CATEGORIAS = [
    # Informática
    ("MLB1648", "Computação"),
    ("MLB1676", "Monitores e Acessórios"),
    ("MLB1693", "Componentes de PC"),
    ("MLB1672", "Impressoras e Scanners"),
    # Celulares e Telefones
    ("MLB1051", "Celulares e Smartphones"),
    ("MLB1052", "Acessórios para Celulares"),
    # Eletrônicos
    ("MLB1000", "Eletrônicos"),
    ("MLB1002", "Áudio e Video"),
    ("MLB1132", "Câmeras e Acessórios"),
    # TVs
    ("MLB1066", "TVs e Vídeo"),
    # Games
    ("MLB1039", "Video Games"),
    ("MLB10215", "Consoles e Jogos"),
    # Casa inteligente / gadgets
    ("MLB1574", "Casa Inteligente"),
]

# Keywords para busca textual (complementar às categorias)
KEYWORDS = [
    "monitor gamer 144hz",
    "monitor 4k",
    "notebook gamer",
    "processador intel",
    "processador amd ryzen",
    "memoria ram ddr4",
    "memoria ram ddr5",
    "ssd nvme",
    "placa de video",
    "smartphone samsung",
    "iphone",
    "xiaomi redmi",
    "motorola edge",
    "fone bluetooth",
    "headset gamer",
    "teclado mecanico",
    "mouse gamer",
    "webcam full hd",
    "smart tv 4k",
    "tv qled",
    "tv oled",
    "playstation 5",
    "xbox series",
    "nintendo switch",
    "controle gamer",
    "robo aspirador",
    "airfryer",
    "caixa de som bluetooth",
    "smartwatch",
    "power bank",
]

PALAVRAS_BLOQUEADAS = [
    # Esportes fora do nicho
    "bola de futebol", "bola gigante", "bola pvc", "bola praia",
    "brinquedo", "brinquedos", "jogos ao ar livre", "esporte ao ar livre",
    "football net", "soccer net", "goal net", "rede de futebol",
    "boxing glove", "luva de boxe", "yoga mat", "haltere", "dumbbell",
    "bicicleta", "bike", "skate", "patins", "raquete",
    # Moda e vestuário
    "roupa", "roupas", "vestido", "camisa", "camiseta", "calça",
    "sapato", "sandália", "bolsa", "carteira", "chapéu",
    "peruca", "extensão cabelo",
    # Ferramentas
    "furadeira", "parafusadeira", "martelo", "serra",
    "multímetro", "multimetro", "clamp meter",
    # Fogo / churrasco
    "churrasqueira", "fogueira", "grelha", "espeto",
    # Jardinagem
    "cortador de grama", "vaso de planta", "mangueira jardim",
    # Suplementos e saúde
    "suplemento", "creatina", "whey protein", "vitamina",
    "remédio", "medicamento", "farmácia",
]


def produto_valido(nome):
    nome_lower = nome.lower()
    for palavra in PALAVRAS_BLOQUEADAS:
        if palavra in nome_lower:
            return False
    return True


def encurtar_link(url_longa):
    try:
        r = requests.get(
            f"https://tinyurl.com/api-create.php?url={url_longa}",
            timeout=5
        )
        if r.status_code == 200 and r.text.startswith("https://"):
            return r.text.strip()
    except:
        pass
    return url_longa


def gerar_link_afiliado(url_produto):
    """Adiciona parâmetro de afiliado na URL do produto."""
    separador = "&" if "?" in url_produto else "?"
    return f"{url_produto}{separador}matt_tool={ML_PUBLISHER_ID}"


def buscar_por_categoria(categoria_id, limit=10):
    """Busca produtos com desconto em uma categoria do ML."""
    try:
        params = {
            "category":    categoria_id,
            "sort":        "best_seller",
            "limit":       limit,
            "offset":      random.randint(0, 40),
        }
        r = requests.get(
            f"{ML_BASE_URL}/sites/MLB/search",
            params=params,
            timeout=15,
            headers={"User-Agent": "OlhaissoTechBot/1.0"}
        )
        if r.status_code != 200:
            return []
        return r.json().get("results", [])
    except Exception as e:
        print(f"ML busca categoria {categoria_id} erro: {e}")
        return []


def buscar_por_keyword(keyword, limit=10):
    """Busca produtos por palavra-chave no ML."""
    try:
        params = {
            "q":      keyword,
            "sort":   "best_seller",
            "limit":  limit,
            "offset": random.randint(0, 20),
        }
        r = requests.get(
            f"{ML_BASE_URL}/sites/MLB/search",
            params=params,
            timeout=15,
            headers={"User-Agent": "OlhaissoTechBot/1.0"}
        )
        if r.status_code != 200:
            return []
        return r.json().get("results", [])
    except Exception as e:
        print(f"ML busca keyword '{keyword}' erro: {e}")
        return []


def processar_item(item):
    """Converte item da API ML no formato padrão do bot."""
    try:
        nome = item.get("title", "").strip()
        if not nome:
            return None

        preco = float(item.get("price", 0) or 0)
        preco_orig = float(item.get("original_price") or preco)

        if preco <= 0:
            return None
        if preco < PRECO_MINIMO or preco > PRECO_MAXIMO:
            return None

        desconto = 0
        if preco_orig > preco:
            desconto = int((1 - preco / preco_orig) * 100)

        if desconto < DESCONTO_MINIMO:
            return None

        if not produto_valido(nome):
            print(f"  ML bloqueado: {nome[:50]}")
            return None

        link_original = item.get("permalink", "")
        if not link_original:
            return None

        # Verifica se tem frete grátis
        shipping = item.get("shipping", {})
        frete_gratis = shipping.get("free_shipping", False)
        frete_txt = "✅ Frete grátis" if frete_gratis else "🚚 Frete a calcular"

        # Imagem
        thumbnail = item.get("thumbnail", "")
        imagem = thumbnail.replace("I.jpg", "O.jpg") if thumbnail else ""

        link_afiliado = gerar_link_afiliado(link_original)
        link_curto    = encurtar_link(link_afiliado)

        return {
            "nome":           nome,
            "preco":          round(preco, 2),
            "preco_original": round(preco_orig, 2),
            "desconto":       desconto,
            "loja":           "MERCADOLIVRE",
            "frete":          frete_txt,
            "link_afiliado":  link_curto,
            "imagem_url":     imagem,
            "score":          1,
            "fontes":         ["mercadolivre"],
        }
    except Exception as e:
        print(f"ML processar item erro: {e}")
        return None


def buscar_todos_produtos():
    todos   = []
    vistos  = set()

    # Busca por categorias
    for cat_id, cat_nome in CATEGORIAS:
        try:
            items = buscar_por_categoria(cat_id, limit=8)
            for item in items:
                p = processar_item(item)
                if p:
                    chave = hashlib.md5(p["nome"].encode()).hexdigest()
                    if chave not in vistos:
                        vistos.add(chave)
                        todos.append(p)
            time.sleep(1)
        except Exception as e:
            print(f"ML categoria {cat_nome} erro: {e}")
            continue

    # Busca por keywords
    keywords_shuffle = random.sample(KEYWORDS, min(15, len(KEYWORDS)))
    for keyword in keywords_shuffle:
        try:
            items = buscar_por_keyword(keyword, limit=5)
            for item in items:
                p = processar_item(item)
                if p:
                    chave = hashlib.md5(p["nome"].encode()).hexdigest()
                    if chave not in vistos:
                        vistos.add(chave)
                        todos.append(p)
            time.sleep(1)
        except Exception as e:
            print(f"ML keyword '{keyword}' erro: {e}")
            continue

    print(f"Mercado Livre API: {len(todos)} produtos encontrados")
    return todos
