"""
Shopee Affiliates API — integração oficial
AppID: 18307831002
Assinatura: SHA256(AppId + Timestamp + Payload + Secret)
"""

import os
import random
import hashlib
import time
import json
import requests

SHOPEE_APP_ID = "18307831002"
SHOPEE_SECRET = "5TCZ4KND77VOJV5QNUX7PMYKTVPF23XT"
SHOPEE_URL    = "https://open-api.affiliate.shopee.com.br/graphql"

CATEGORIAS = [
    # Originais — tech e gamer
    "fone bluetooth",
    "smartwatch",
    "carregador gan",
    "teclado gamer",
    "mouse sem fio",
    "caixa de som bluetooth",
    "power bank",
    "webcam full hd",
    "hub usb",
    "projetor mini",
    "aspirador portatil",
    "aspirador robo wifi",
    "robo aspirador inteligente",
    "robo aspirador mop",
    "fritadeira airfryer",
    "tomada inteligente",
    "luminaria led",
    "fita led rgb",
    "headset gamer",
    "suporte notebook",
    "relogio inteligente",
    "controle gamer",
    # Informática e upgrade
    "ssd nvme m2",
    "ssd 1tb interno",
    "ssd 512gb",
    "memoria ram ddr4",
    "memoria ram ddr5",
    "ram 4gb ddr4",
    "ram 8gb ddr4",
    "ram 16gb",
    "ram 32gb",
    "ram 8gb ddr5",
    "processador intel",
    "processador amd ryzen",
    # Monitores
    "monitor gamer 144hz",
    "monitor gamer 165hz",
    "monitor gamer 240hz",
    "monitor 4k",
    "monitor curvo",
    "monitor 27 polegadas",
    "monitor 24 polegadas",
    "monitor led full hd",
    "monitor ultrawide",
    "monitor portatil",
    # Celulares e Smartphones
    "smartphone android",
    "smartphone 5g",
    "celular xiaomi",
    "celular samsung",
    "celular motorola",
    "poco smartphone",
    "redmi smartphone",
    "celular barato bom",
    "smartphone camera 108mp",
    "celular gamer",
    # Acessórios de celular
    "capinha celular",
    "pelicula celular",
    "carregador rapido celular",
    "cabo usb-c",
    "suporte celular carro",
    "selfie ring light",
    # Marcas premium
    "xiaomi fone",
    "xiaomi smartwatch",
    "baseus carregador",
    "baseus cabo",
    "anker power bank",
    "jbl caixa som",
    "jbl fone",
    "redragon mouse",
    "redragon teclado",
    "logitech mouse",
    "philips luminaria",
    "mondial fritadeira",
    # Virais e tendência — tech apenas
    "gadget criativo",
    "gadget util dia a dia",
    "setup gamer barato",
    "produto tendencia tech",
]

PRECO_MINIMO    = float(os.getenv("PRECO_MINIMO", "50.00"))
PRECO_MAXIMO    = float(os.getenv("PRECO_MAXIMO", "3000.00"))
DESCONTO_MINIMO = int(os.getenv("DESCONTO_MINIMO", "20"))

PALAVRAS_BLOQUEADAS = [
    "separador de tela", "manutenção", "desmontagem", "reparo", "solda",
    "placa mãe", "cabo flex", "ferramenta de", "kit de reparo",
    "separador lcd", "substituição", "peça de reposição", "conserto",
    "chave de fenda", "alicate", "pinça", "estação de solda",
    "repair", "maintenance", "soldering", "pcb", "lcd separator",
    "rework", "fixture", "jig", "spare part", "replacement part",
    "motherboard", "flex cable", "digitizer", "screen separator",
    "wholesale", "bulk", "lot of", "pcs lot", "oem", "odm",
    "wig", "hair extension", "nail art", "eyelash",
    "fishing", "hunting", "medical", "surgical", "dental",
    # Eletrônica DIY / fora do nicho
    "arduino", "esp32", "esp8266", "raspberry", "diy kit",
    "servo motor", "sensor module", "breadboard", "robô kit",
    "robot kit", "open source", "programavel", "programmable robot",
    # Esportes e artigos esportivos fora do nicho
    "football net", "soccer net", "goal net", "training net", "sport net",
    "quarterback", "arremesso", "rede de futebol", "rede esportiva",
    "rede de alvo", "golha de futebol", "gol dobravel", "gol portatil",
    "rede de gol", "trave dobravel", "mini gol", "chute ao gol",
    "chute a gol", "trave de futebol", "gol de futebol",
    "football target", "throwing target", "pitching net",
    "tennis net", "volleyball net", "badminton net",
    "soccer goal", "football goal", "pop up goal",
    "nailon futebol", "rede nailon", "nailon esporte",
    "playground futebol", "quintal futebol", "treinamento futebol",
    "boxing glove", "luva de boxe", "saco de pancada",
    "bicicleta", "bike", "cycling", "ciclismo",
    "skate", "patins", "roller skate",
    "equipamento esportivo", "sport equipment",
    "yoga mat", "tapete yoga", "colchonete",
    "haltere", "dumbbell", "kettlebell", "barbell",
    "elastico musculacao", "resistance band",
    "corda de pular", "jump rope",
    "mochila esportiva", "sport backpack",
    "golfe", "golf", "taco de golfe",
    "raquete", "racket", "racquet",
    "capacete bike", "joelheira", "cotoveleira",
    # Brinquedos e esportes ao ar livre — fora do nicho
    "bola de futebol", "bola gigante", "bola pvc", "bola praia",
    "bola verão", "brinquedo", "brinquedos", "toy", "toys",
    "jogos ao ar livre", "jogos esportivos", "esporte ao ar livre",
    "gramado esporte", "queda gramado", "presente jogos",
    "pvc ball", "giant ball", "beach ball", "outdoor game",
    "kite", "pipa", "pião", "bambolê", "frisbee",
    # Ferramentas de medição e elétrica — fora do nicho
    "multímetro", "multimetro", "alicate amperímetro", "alicate amperimetro",
    "medidor de braçadeira", "medidor de bracadeira", "clamp meter",
    "digital clamp", "amperímetro", "amperimetro", "voltímetro", "voltimetro",
    "capacitância", "capacitancia", "medidor de temperatura",
    "testador de cabo", "testador elétrico", "testador eletrico",
    "osciloscópio", "osciloscopio", "analisador de espectro",
    "medidor de energia", "wattímetro", "wattimetro",
    "detector de tensão", "detector de tensao",
    "termômetro infravermelho", "termometro infravermelho",
    # Ferramentas gerais
    "furadeira", "parafusadeira", "chave inglesa", "martelo",
    "serra", "esmerilhadeira", "lixadeira",
    # Fogo, churrasqueira, camping fora do nicho
    "fogueira", "fogão", "fogao", "churrasqueira", "churrasco",
    "fire pit", "campfire", "fireplace", "lareira",
    "suporte para fogueira", "base para fogueira", "queimador",
    "grelha", "espeto", "carvão", "carvaoo", "braseiro",
    "camping stove", "camp fire", "outdoor fire",
    "retrátil ajustável aço inoxidável", "retratil ajustavel",
    "fogão solo", "ranger yukon", "fogueira solo",
    # Jardinagem e ao ar livre
    "cortador de grama", "podadora", "regador",
    "vaso de planta", "adubo", "fertilizante",
    "mangueira jardim", "aspersor",
    # Moda e vestuário — BLOQUEIO TOTAL
    "suéter", "sueter", "pulôver", "pulover", "blusa", "blusão",
    "malha", "tricô", "tricot", "knit", "knitwear",
    "algodão macio", "algodao macio", "bordado de urso", "bordado floral",
    "gola redonda", "gola v", "gola alta", "decote v",
    "manga longa", "manga curta", "manga 3/4",
    "top feminino", "top de", "para mulheres", "para homens",
    "feminino", "masculino", "unissex moda",
    "casual confortável", "casual confortavel",
    "blazer", "vestido", "camisa", "camiseta", "blusa", "calça",
    "saia", "shorts", "jaqueta", "casaco", "moletom", "agasalho",
    "roupa", "roupas", "moda", "fashion", "clothing", "dress",
    "shirt", "pants", "jacket", "coat", "sweater", "hoodie",
    "t-shirt", "jeans", "legging", "bermuda", "pijama", "lingerie",
    "sutiã", "calcinha", "cueca", "meia calça", "underwear",
    "feminino retrô", "abotoamento",
    "coleção primavera", "coleção verão", "coleção outono",
    "estilo napoleão", "assimétrico", "decote", "tecido",
    "sapato", "sandália", "chinelo", "calçado",
    "bolsa feminina", "carteira feminina", "cinto moda",
    "chapéu moda", "boné fashion", "óculos de sol moda",
    "peruca", "extensão cabelo", "aplique cabelo",
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


def gerar_assinatura(app_id, timestamp, payload_str, secret):
    """Assinatura correta: SHA256(AppId + Timestamp + Payload + Secret)"""
    fator = app_id + str(timestamp) + payload_str + secret
    return hashlib.sha256(fator.encode("utf-8")).hexdigest()


def buscar_produtos_shopee(keyword, limit=10):
    """Busca produtos via Shopee Affiliate API GraphQL."""
    try:
        query = """query getProducts($keyword: String!, $limit: Int!, $page: Int!) {
  productOfferV2(
    listType: 0,
    sortType: 2,
    keyword: $keyword,
    limit: $limit,
    page: $page
  ) {
    nodes {
      productName
      priceMin
      priceMax
      priceDiscountRate
      imageUrl
      offerLink
      productLink
      commissionRate
    }
    pageInfo { hasNextPage }
  }
}"""

        variables = {"keyword": keyword, "limit": limit, "page": random.randint(1, 3)}
        body = {
            "query": query,
            "operationName": "getProducts",
            "variables": variables
        }

        # Payload como string compacta para assinatura
        payload_str = json.dumps(body, separators=(",", ":"))
        timestamp = int(time.time())
        sign = gerar_assinatura(SHOPEE_APP_ID, timestamp, payload_str, SHOPEE_SECRET)

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"SHA256 Credential={SHOPEE_APP_ID},Timestamp={timestamp},Signature={sign}",
        }

        r = requests.post(SHOPEE_URL, data=payload_str, headers=headers, timeout=15)

        if r.status_code != 200:
            print(f"Shopee HTTP erro {r.status_code}: {r.text[:200]}")
            return []

        data = r.json()

        # Verifica erros GraphQL
        if "errors" in data:
            print(f"Shopee GraphQL erro: {data['errors']}")
            return []

        nodes = data.get("data", {}).get("productOfferV2", {}).get("nodes", []) or []
        produtos = []

        for item in nodes:
            try:
                preco = float(str(item.get("priceMin", "0")).replace(",", "."))
                desc_raw = item.get("priceDiscountRate", "0") or "0"
                desconto = int(str(desc_raw).replace("%", "").strip() or 0)
            except:
                continue

            if preco < PRECO_MINIMO or preco > PRECO_MAXIMO:
                continue
            if desconto < DESCONTO_MINIMO:
                continue

            nome = item.get("productName", "")
            link_original = item.get("offerLink") or item.get("productLink", "")
            imagem = item.get("imageUrl", "")

            if not nome or not link_original:
                continue

            if not produto_valido(nome):
                print(f"  Shopee bloqueado: {nome[:50]}")
                continue

            preco_orig = round(preco / (1 - desconto / 100), 2) if desconto > 0 else round(preco * 1.3, 2)
            link = encurtar_link(link_original)

            produtos.append({
                "nome": nome,
                "preco": round(preco, 2),
                "preco_original": preco_orig,
                "desconto": desconto,
                "loja": "SHOPEE",
                "frete": "✅ Frete grátis",
                "link_afiliado": link,
                "imagem_url": imagem,
                "score": 1,
                "fontes": ["shopee"],
            })

        return produtos

    except Exception as e:
        print(f"Shopee erro ({keyword}): {e}")
        return []


def buscar_todos_produtos():
    import hashlib as _h
    todos = []
    vistos = set()
    monitor_portatil_count = 0

    for keyword in CATEGORIAS:
        try:
            produtos = buscar_produtos_shopee(keyword, limit=5)
            for p in produtos:
                chave = _h.md5(p["nome"].encode()).hexdigest()
                if chave not in vistos:
                    # Limita monitor portátil a 1 por rodada
                    nome_lower = p["nome"].lower()
                    eh_portatil = any(kw in nome_lower for kw in ["monitor portátil", "monitor portatil", "portable monitor", "monitor usb-c"])
                    if eh_portatil:
                        if monitor_portatil_count >= 1:
                            continue
                        monitor_portatil_count += 1
                    vistos.add(chave)
                    todos.append(p)
            time.sleep(1)
        except Exception as e:
            print(f"Erro em {keyword}: {e}")
            continue

    print(f"Shopee API: {len(todos)} produtos encontrados")
    return todos
