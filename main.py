import os
import logging
import json
import pickle
import io
import re
import base64
import calendar
import threading

os.environ.setdefault('TZ', 'America/Sao_Paulo')
try:
    import time; time.tzset()
except AttributeError:
    pass

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel
from typing import List, Optional
import httpx
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
app = FastAPI()
app.add_middleware(CORSMiddleware,
    allow_origins=["https://web-production-91aff.up.railway.app"],
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"])

FILE_ID    = os.environ.get("DRIVE_FILE_ID", "")
CLAUDE_KEY = os.environ.get("CLAUDE_API_KEY", "")
MEUDANFE_KEY = "0c1588f4-f90e-4711-8b39-87be9a1581da"

FILIAIS_VALIDAS = {"ITAP", "BJESUS", "PORC"}

# ─────────────────────────────────────────────
#  CACHE EM MEMÓRIA (30 minutos)
# ─────────────────────────────────────────────
_cache_lock = threading.Lock()
_cache = {"df": None, "ts": None, "file_id": None}
CACHE_TTL = 1800  # 30 minutos

def get_drive_service():
    token_bytes = os.environ.get("GOOGLE_TOKEN_PICKLE")
    if not token_bytes:
        raise HTTPException(status_code=500, detail="Token do Google Drive não configurado.")
    creds = pickle.loads(base64.b64decode(token_bytes))
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build('drive', 'v3', credentials=creds)

def _download_df() -> pd.DataFrame:
    """Baixa CSV do Drive e retorna DataFrame tratado."""
    service = get_drive_service()
    req = service.files().get_media(fileId=FILE_ID)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    buf.seek(0)
    df = pd.read_csv(buf, sep=';', encoding='utf-8-sig', low_memory=False)

    # Data
    sample = str(df['DATA_MOVTO'].dropna().iloc[0]) if len(df) > 0 else ''
    use_dayfirst = bool(re.match(r'\d{1,2}/\d{1,2}/\d{2,4}', sample))
    df['DATA_MOVTO'] = pd.to_datetime(df['DATA_MOVTO'], errors='coerce', dayfirst=use_dayfirst)

    # Numéricos
    df['VALOR_LIQUIDO'] = pd.to_numeric(df['VALOR_LIQUIDO'], errors='coerce').fillna(0)
    df['QTDE_PRI']      = pd.to_numeric(df['QTDE_PRI'],      errors='coerce').fillna(0)
    df['QTDE_AUX']      = pd.to_numeric(df.get('QTDE_AUX', 0), errors='coerce').fillna(0)

    # Vendedor sem decimal
    if 'COD_VENDEDOR' in df.columns:
        df['COD_VENDEDOR'] = pd.to_numeric(df['COD_VENDEDOR'], errors='coerce').fillna(0).astype(int).astype(str).str.zfill(4)

    # Filial — garante apenas filiais válidas
    if 'NOME_FILIAL' in df.columns:
        df = df[df['NOME_FILIAL'].isin(FILIAIS_VALIDAS)]

    logging.info(f"[CACHE] CSV baixado: {len(df)} linhas | FILE_ID={FILE_ID}")
    return df

def load_df() -> pd.DataFrame:
    """Retorna DataFrame do cache ou baixa novo se expirado."""
    with _cache_lock:
        agora = time.time()
        if (
            _cache["df"] is not None
            and _cache["file_id"] == FILE_ID
            and _cache["ts"] is not None
            and (agora - _cache["ts"]) < CACHE_TTL
        ):
            return _cache["df"].copy()
        df = _download_df()
        _cache["df"] = df
        _cache["ts"] = agora
        _cache["file_id"] = FILE_ID
        return df.copy()

def invalidar_cache():
    with _cache_lock:
        _cache["df"] = None
        _cache["ts"] = None

# ─────────────────────────────────────────────
#  ETAPA 1 — INTERPRETAR (Claude → JSON de filtro)
# ─────────────────────────────────────────────
async def interpretar_pergunta(pergunta: str, historico: list, df: pd.DataFrame) -> dict:
    """Claude lê a pergunta e devolve um JSON de filtro estruturado."""

    # Contexto do DataFrame para o Claude saber o que existe
    d_min = df['DATA_MOVTO'].min().strftime('%d/%m/%Y') if pd.notna(df['DATA_MOVTO'].min()) else '?'
    d_max = df['DATA_MOVTO'].max().strftime('%d/%m/%Y') if pd.notna(df['DATA_MOVTO'].max()) else '?'
    filiais = sorted(df['NOME_FILIAL'].dropna().unique().tolist()) if 'NOME_FILIAL' in df.columns else []
    hoje = datetime.now().strftime('%d/%m/%Y')

    # Histórico resumido (últimas 6 msgs)
    hist_txt = ""
    for m in historico[-6:]:
        papel = "Usuário" if m["role"] == "user" else "IAF"
        hist_txt += f"{papel}: {m['content'][:200]}\n"

    system_interpret = f"""Você é um interpretador de perguntas sobre vendas. 
Retorne APENAS um JSON válido, sem texto adicional, sem markdown, sem explicações.

CONTEXTO DOS DADOS:
- Período disponível: {d_min} até {d_max}
- Hoje: {hoje}
- Filiais: {', '.join(filiais)}

HISTÓRICO RECENTE DA CONVERSA:
{hist_txt}

PERGUNTA ATUAL: {pergunta}

Retorne JSON com esta estrutura exata:
{{
  "tipo": "resumo_mensal|resumo_diario|ultimas_vendas|detalhe_nota|ranking_clientes|ranking_produtos|ranking_vendedores|comparativo|grafico|cnpj_query|periodo_livre|saudacao|indefinido",
  "data_inicio": "YYYY-MM-DD ou null",
  "data_fim": "YYYY-MM-DD ou null",
  "filial": "ITAP|BJESUS|PORC ou null",
  "cliente": "nome parcial ou null",
  "cnpj_raiz": "8 dígitos ou null",
  "vendedor": "nome parcial ou null",
  "nr_nota": "número ou null",
  "uf": "sigla ou null",
  "precisa_cliente": true|false,
  "precisa_periodo": true|false,
  "comparar_periodo_anterior": true|false,
  "observacao": "qualquer detalhe extra relevante ou null"
}}

REGRAS:
- "hoje" = {hoje}
- "mês passado" = mês anterior ao atual
- "esta semana" = segunda-feira até hoje
- Se período não especificado e tipo for resumo: use o último mês disponível
- Se cliente não especificado e tipo for ultimas_vendas: precisa_cliente=true
- Se nr_nota mencionado: tipo="detalhe_nota"
- Para comparativos entre períodos: tipo="comparativo", coloque ambos os períodos em observacao
- NUNCA invente dados, apenas interprete a pergunta"""

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={"Content-Type": "application/json",
                     "x-api-key": CLAUDE_KEY,
                     "anthropic-version": "2023-06-01"},
            json={"model": "claude-haiku-4-5-20251001",
                  "max_tokens": 300,
                  "system": system_interpret,
                  "messages": [{"role": "user", "content": pergunta}]}
        )
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)

    texto = r.json()["content"][0]["text"].strip()
    # Remove markdown se vier
    texto = re.sub(r'^```json\s*', '', texto)
    texto = re.sub(r'\s*```$', '', texto)
    try:
        return json.loads(texto)
    except Exception:
        logging.warning(f"[INTERPRETAR] JSON inválido: {texto}")
        return {"tipo": "indefinido", "data_inicio": None, "data_fim": None,
                "filial": None, "cliente": None, "cnpj_raiz": None,
                "vendedor": None, "nr_nota": None, "uf": None,
                "precisa_cliente": False, "precisa_periodo": False,
                "comparar_periodo_anterior": False, "observacao": None}

# ─────────────────────────────────────────────
#  ETAPA 2 — CALCULAR (Python faz tudo)
# ─────────────────────────────────────────────
def _aplicar_filtros(df: pd.DataFrame, filtro: dict) -> pd.DataFrame:
    """Aplica filtros do JSON ao DataFrame. Retorna dff limpo."""
    dff = df.copy()

    # Período
    if filtro.get("data_inicio"):
        try:
            d1 = pd.to_datetime(filtro["data_inicio"])
            dff = dff[dff['DATA_MOVTO'] >= d1]
        except: pass
    if filtro.get("data_fim"):
        try:
            d2 = pd.to_datetime(filtro["data_fim"]) + timedelta(days=1)
            dff = dff[dff['DATA_MOVTO'] < d2]
        except: pass

    # Filial
    if filtro.get("filial") and 'NOME_FILIAL' in dff.columns:
        dff = dff[dff['NOME_FILIAL'].str.upper() == filtro["filial"].upper()]

    # UF
    if filtro.get("uf") and 'UF' in dff.columns:
        dff = dff[dff['UF'].str.upper() == filtro["uf"].upper()]

    # Cliente por nome
    if filtro.get("cliente") and 'NOME_CLIENTE' in dff.columns:
        nome = filtro["cliente"]
        # Match progressivo: 20 chars → 10 → 5
        for tam in [20, 10, 5]:
            mask = dff['NOME_CLIENTE'].str.lower().str.contains(nome.lower()[:tam], na=False)
            if mask.sum() > 0:
                dff = dff[mask]
                break

    # Cliente por CNPJ raiz
    if filtro.get("cnpj_raiz") and 'CPF_CGC' in dff.columns:
        raiz = re.sub(r'\D', '', filtro["cnpj_raiz"])[:8]
        col = dff['CPF_CGC'].astype(str).str.replace(r'\D', '', regex=True)
        dff = dff[col.str.startswith(raiz)]

    # Vendedor
    if filtro.get("vendedor") and 'NOM_VENDEDOR' in dff.columns:
        nome_v = filtro["vendedor"]
        for tam in [20, 10, 5]:
            mask = dff['NOM_VENDEDOR'].str.lower().str.contains(nome_v.lower()[:tam], na=False)
            if mask.sum() > 0:
                dff = dff[mask]
                break

    # Nota fiscal
    if filtro.get("nr_nota") and 'NUM_DOCTO' in dff.columns:
        dff = dff[dff['NUM_DOCTO'].astype(str).str.strip() == str(filtro["nr_nota"]).strip()]

    return dff

def _periodo_anterior(filtro: dict) -> tuple:
    """Calcula data_inicio/fim do período anterior equivalente."""
    try:
        d1 = pd.to_datetime(filtro["data_inicio"])
        d2 = pd.to_datetime(filtro["data_fim"])
        delta = d2 - d1 + timedelta(days=1)
        pa_fim = d1 - timedelta(days=1)
        pa_ini = pa_fim - delta + timedelta(days=1)
        return pa_ini, pa_fim
    except:
        return None, None

def calcular(df: pd.DataFrame, filtro: dict) -> dict:
    """Calcula todos os dados necessários. Retorna dict com resultados."""
    tipo = filtro.get("tipo", "indefinido")
    dff = _aplicar_filtros(df, filtro)
    n = len(dff)

    resultado = {
        "tipo": tipo,
        "filtro_aplicado": filtro,
        "n_registros": n,
        "dados": {}
    }

    if n == 0:
        resultado["sem_dados"] = True
        return resultado

    resultado["sem_dados"] = False
    d = resultado["dados"]

    # ── Métricas base sempre calculadas ──
    fat   = round(float(dff['VALOR_LIQUIDO'].sum()), 2)
    kg    = round(float(dff['QTDE_PRI'].sum()), 2)
    cx    = round(kg / 30, 0)
    notas = int(dff['NUM_DOCTO'].nunique()) if 'NUM_DOCTO' in dff.columns else 0
    pm    = round(fat / kg, 2) if kg > 0 else 0
    d_min = dff['DATA_MOVTO'].min()
    d_max = dff['DATA_MOVTO'].max()

    d["faturamento"]  = fat
    d["kg"]           = kg
    d["cx30"]         = int(cx)
    d["notas"]        = notas
    d["preco_medio"]  = pm
    d["periodo_ini"]  = d_min.strftime('%d/%m/%Y') if pd.notna(d_min) else None
    d["periodo_fim"]  = d_max.strftime('%d/%m/%Y') if pd.notna(d_max) else None

    # ── Por filial ──
    if 'NOME_FILIAL' in dff.columns:
        por_filial = dff.groupby('NOME_FILIAL').agg(
            kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum'),
            notas=('NUM_DOCTO','nunique')
        ).sort_values('kg', ascending=False)
        d["por_filial"] = [
            {"filial": idx,
             "kg": round(float(r.kg),2),
             "cx30": int(round(r.kg/30,0)),
             "faturamento": round(float(r.fat),2),
             "pm": round(float(r.fat)/float(r.kg),2) if r.kg > 0 else 0,
             "notas": int(r.notas)}
            for idx, r in por_filial.iterrows()
        ]

    # ── Por dia ──
    por_dia = dff.groupby(dff['DATA_MOVTO'].dt.date).agg(
        kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum'),
        notas=('NUM_DOCTO','nunique')
    ).sort_index()
    d["por_dia"] = [
        {"data": str(idx),
         "kg": round(float(r.kg),2),
         "cx30": int(round(r.kg/30,0)),
         "faturamento": round(float(r.fat),2),
         "pm": round(float(r.fat)/float(r.kg),2) if r.kg > 0 else 0,
         "notas": int(r.notas)}
        for idx, r in por_dia.iterrows()
    ]

    # ── Dias faturados e média diária ──
    dias_fat = len(por_dia)
    d["dias_faturados"]  = dias_fat
    d["media_diaria_kg"] = round(kg / dias_fat, 0) if dias_fat > 0 else 0
    d["media_diaria_fat"] = round(fat / dias_fat, 2) if dias_fat > 0 else 0

    # ── Previsão de fechamento do mês ──
    try:
        ult_data = d_max.date()
        ult_dia_mes = calendar.monthrange(ult_data.year, ult_data.month)[1]
        dias_rest = sum(
            1 for day in range(ult_data.day + 1, ult_dia_mes + 1)
            if ult_data.replace(day=day).weekday() < 5
        )
        d["dias_uteis_restantes"] = dias_rest
        d["previsao_kg"]  = round(kg  + d["media_diaria_kg"]  * dias_rest, 0)
        d["previsao_fat"] = round(fat + d["media_diaria_fat"] * dias_rest, 2)
        d["previsao_cx30"] = int(round(d["previsao_kg"] / 30, 0))
    except:
        d["dias_uteis_restantes"] = 0

    # ── Top clientes ──
    if 'NOME_CLIENTE' in dff.columns:
        top_cli = dff.groupby('NOME_CLIENTE').agg(
            kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum'),
            notas=('NUM_DOCTO','nunique')
        ).sort_values('kg', ascending=False).head(15)
        d["top_clientes"] = [
            {"nome": idx,
             "kg": round(float(r.kg),2),
             "cx30": int(round(r.kg/30,0)),
             "faturamento": round(float(r.fat),2),
             "pm": round(float(r.fat)/float(r.kg),2) if r.kg > 0 else 0,
             "notas": int(r.notas)}
            for idx, r in top_cli.iterrows()
        ]

    # ── Top produtos ──
    if 'DESC_PRODUTO' in dff.columns:
        top_prod = dff.groupby('DESC_PRODUTO').agg(
            kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum')
        ).sort_values('kg', ascending=False).head(15)
        d["top_produtos"] = [
            {"nome": idx,
             "kg": round(float(r.kg),2),
             "cx30": int(round(r.kg/30,0)),
             "faturamento": round(float(r.fat),2),
             "pm": round(float(r.fat)/float(r.kg),2) if r.kg > 0 else 0}
            for idx, r in top_prod.iterrows()
        ]

    # ── Top vendedores ──
    if 'NOM_VENDEDOR' in dff.columns and 'COD_VENDEDOR' in dff.columns:
        top_vend = dff.groupby(['COD_VENDEDOR','NOM_VENDEDOR']).agg(
            kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum'),
            notas=('NUM_DOCTO','nunique')
        ).sort_values('kg', ascending=False).head(20)
        d["top_vendedores"] = [
            {"cod": idx[0], "nome": idx[1],
             "kg": round(float(r.kg),2),
             "cx30": int(round(r.kg/30,0)),
             "faturamento": round(float(r.fat),2),
             "pm": round(float(r.fat)/float(r.kg),2) if r.kg > 0 else 0,
             "notas": int(r.notas)}
            for idx, r in top_vend.iterrows()
        ]

    # ── Por tipo de corte ──
    if 'DESC_DIVISAO2' in dff.columns:
        por_tipo = dff.groupby('DESC_DIVISAO2').agg(
            kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum')
        ).sort_values('kg', ascending=False)
        d["por_tipo_corte"] = [
            {"tipo": idx,
             "kg": round(float(r.kg),2),
             "cx30": int(round(r.kg/30,0)),
             "faturamento": round(float(r.fat),2),
             "pm": round(float(r.fat)/float(r.kg),2) if r.kg > 0 else 0}
            for idx, r in por_tipo.iterrows()
        ]

    # ── Detalhe de nota fiscal ──
    if tipo == "detalhe_nota" and 'NUM_DOCTO' in dff.columns:
        cols_nota = [c for c in ['NUM_DOCTO','DATA_MOVTO','NOME_CLIENTE','NOME_FILIAL',
                                  'NOM_VENDEDOR','COD_PRODUTO','DESC_PRODUTO','DESC_DIVISAO2',
                                  'QTDE_PRI','QTDE_AUX','VALOR_UNITARIO','VALOR_LIQUIDO',
                                  'CHAVE_ACESSO'] if c in dff.columns]
        itens = []
        for _, row in dff[cols_nota].iterrows():
            item = {}
            for c in cols_nota:
                v = row[c]
                if hasattr(v, 'strftime'):
                    item[c] = v.strftime('%d/%m/%Y')
                elif pd.isna(v):
                    item[c] = None
                else:
                    item[c] = v
            itens.append(item)
        d["itens_nota"] = itens
        d["chave_acesso"] = str(dff['CHAVE_ACESSO'].dropna().iloc[0]).strip() if 'CHAVE_ACESSO' in dff.columns and len(dff) > 0 else None

    # ── Últimas vendas de cliente ──
    if tipo == "ultimas_vendas" and filtro.get("cliente"):
        cols_venda = [c for c in ['DATA_MOVTO','NUM_DOCTO','COD_PRODUTO','DESC_PRODUTO',
                                   'QTDE_PRI','QTDE_AUX','VALOR_LIQUIDO','NOME_FILIAL'] if c in dff.columns]
        ultimas = dff[cols_venda].sort_values('DATA_MOVTO', ascending=False).head(20)
        registros = []
        for _, row in ultimas.iterrows():
            item = {}
            for c in cols_venda:
                v = row[c]
                if hasattr(v, 'strftime'):
                    item[c] = v.strftime('%d/%m/%Y')
                elif pd.isna(v):
                    item[c] = None
                else:
                    item[c] = v
            registros.append(item)
        d["ultimas_vendas"] = registros
        if 'NOME_CLIENTE' in dff.columns:
            d["cliente_encontrado"] = dff['NOME_CLIENTE'].iloc[0]

    # ── Comparativo com período anterior ──
    if filtro.get("comparar_periodo_anterior") and filtro.get("data_inicio") and filtro.get("data_fim"):
        pa_ini, pa_fim = _periodo_anterior(filtro)
        if pa_ini:
            filtro_pa = {**filtro, "data_inicio": pa_ini.strftime('%Y-%m-%d'),
                         "data_fim": pa_fim.strftime('%Y-%m-%d'),
                         "comparar_periodo_anterior": False}
            dff_pa = _aplicar_filtros(df, filtro_pa)
            if len(dff_pa) > 0:
                fat_pa = round(float(dff_pa['VALOR_LIQUIDO'].sum()), 2)
                kg_pa  = round(float(dff_pa['QTDE_PRI'].sum()), 2)
                d["comparativo"] = {
                    "periodo_anterior_ini": pa_ini.strftime('%d/%m/%Y'),
                    "periodo_anterior_fim": pa_fim.strftime('%d/%m/%Y'),
                    "faturamento_anterior": fat_pa,
                    "kg_anterior": kg_pa,
                    "var_fat_pct": round((fat - fat_pa) / fat_pa * 100, 1) if fat_pa > 0 else None,
                    "var_kg_pct":  round((kg  - kg_pa)  / kg_pa  * 100, 1) if kg_pa  > 0 else None,
                }

    # ── CNPJ query ──
    if tipo == "cnpj_query" and filtro.get("cliente") and 'CPF_CGC' in df.columns:
        nome_busca = filtro["cliente"]
        mask = df['NOME_CLIENTE'].str.lower().str.contains(nome_busca.lower()[:10], na=False)
        rows = df[mask][['NOME_CLIENTE','CPF_CGC']].dropna()
        rows['raiz'] = rows['CPF_CGC'].astype(str).str.replace(r'\D','',regex=True).str[:8]
        raizes = rows.groupby('raiz')['NOME_CLIENTE'].first().reset_index()
        d["cnpjs"] = [{"nome": r['NOME_CLIENTE'], "cnpj_raiz": r['raiz']} for _, r in raizes.iterrows()]

    return resultado

# ─────────────────────────────────────────────
#  ETAPA 3 — NARRAR (Claude formata o resultado)
# ─────────────────────────────────────────────
async def narrar(pergunta: str, resultado: dict, historico: list, modo: str = "normal") -> str:
    """Claude recebe JSON com dados calculados e formata a resposta."""

    if resultado.get("sem_dados"):
        return "⚠️ Sem dados disponíveis para o período/filtro solicitado."

    personalidade = ""
    if modo == "mengo":
        personalidade = "\nMODO NAÇÃO 🔴⚫: Tempere com referências rubro-negras, mas NUNCA altere os números."
    elif modo == "vasco":
        personalidade = "\nMODO GIGANTE DA COLINA ⬛⬜: Tempere com referências vascaínas, mas NUNCA altere os números."

    dados_json = json.dumps(resultado["dados"], ensure_ascii=False, indent=2)
    tipo = resultado.get("tipo", "")

    system_narrar = f"""Você é o IAF, Analista Comercial Sênior da Frinense Alimentos.{personalidade}

⛔⛔⛔ REGRA ABSOLUTA — NUNCA INVENTAR DADOS ⛔⛔⛔
- USE APENAS os números do JSON abaixo. ZERO EXCEÇÕES.
- NUNCA arredonde, estime ou altere qualquer valor.
- Se um campo não existe no JSON = não existe. Não mencione.
- Filiais válidas: ITAP, BJESUS, PORC. Qualquer outra NÃO EXISTE.
⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔

## FORMATO
- Tom executivo e direto — sem "Olá", "Claro!", "Com prazer"
- Use Markdown: ## títulos, **negrito**, tabelas com | Col |
- Valores: R$ X.XXX.XXX,XX | Kg: X.XXX.XXX kg | Datas: DD/MM/AA
- CX30 = kg/30 — sempre exiba junto com kg
- Toda tabela DEVE ter linha "| **TOTAIS** |" no final
- Finalize com: 💡 **Insight:** [ação ou oportunidade concreta]
- Após tabelas: 📊 **ANÁLISE RÁPIDA:** com 3-4 bullets (🔝 Destaque / 📉 Atenção / 📈 Tendência / 💰 Ticket médio)

## COMPORTAMENTOS POR TIPO
- resumo_mensal / resumo_diario: Mostre KPIs gerais → por filial → por dia → previsão fechamento → top clientes → top produtos
- detalhe_nota: Mostre cabeçalho (filial, cliente, vendedor) + tabela de itens + DANFE se tiver chave_acesso
- ultimas_vendas: Tabela DATA | NR NOTA | PRODUTO | KG | CX | R$ | R$/kg — decrescente, sem totais
- ranking_clientes: Tabela com posição, nome, kg, cx30, faturamento, R$/kg
- ranking_vendedores: Tabela com cod, nome, kg, cx30, faturamento, notas
- comparativo: Mostre período atual vs anterior com variação % em cada métrica
- cnpj_query: Liste clientes com CNPJ raiz formatado

## NOTA FISCAL — FORMATO OBRIGATÓRIO
## NOTA FISCAL [NR] · [DATA]
**Filial:** [F] | **Cliente:** [C] | **Vendedor:** [V]

| # | PRODUTO | COD | DIVISÃO | KG | CX | VALOR | R$/kg |
|---|---------|-----|---------|----|----|-------|-------|
| 1 | ... | ... | ... | ... | ... | ... | ... |
| **TOTAIS** | | | | [soma] | [soma] | [soma] | [pm] |

Se chave_acesso disponível: adicione linha em branco + "DANFE:[chave]"

DADOS CALCULADOS (use SOMENTE estes):
{dados_json}"""

    # Histórico para contexto (últimas 6 msgs)
    msgs = []
    for m in historico[-6:]:
        msgs.append({"role": m["role"], "content": m["content"][:500]})
    msgs.append({"role": "user", "content": pergunta})

    # Max tokens por tipo
    max_tok = 4000 if tipo in ("resumo_mensal", "comparativo") else 2000 if tipo == "detalhe_nota" else 1500

    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={"Content-Type": "application/json",
                     "x-api-key": CLAUDE_KEY,
                     "anthropic-version": "2023-06-01"},
            json={"model": "claude-haiku-4-5-20251001",
                  "max_tokens": max_tok,
                  "system": system_narrar,
                  "messages": msgs}
        )
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)
    return r.json()["content"][0]["text"]

# ─────────────────────────────────────────────
#  MODELOS
# ─────────────────────────────────────────────
class Message(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    messages: List[Message]
    modo: str = "normal"

# ─────────────────────────────────────────────
#  ROUTES — ESTÁTICAS
# ─────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
def root():
    for p in ["menu.html", "/app/menu.html"]:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
    return HTMLResponse("<h1>Menu</h1><a href='/iaf'>IAF</a> | <a href='/ia3'>IA3</a>")

@app.get("/iaf", response_class=HTMLResponse)
def iaf():
    for p in ["index.html", "/app/index.html"]:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
    return HTMLResponse("<h1>IAF</h1>")

# ─────────────────────────────────────────────
#  ROUTES — DASHBOARD
# ─────────────────────────────────────────────
@app.get("/dashboard")
def dashboard():
    try:
        df = load_df()
        hoje = datetime.now().date()
        dia = hoje if (df['DATA_MOVTO'].dt.date == hoje).any() else df['DATA_MOVTO'].dt.date.max()
        df_dia = df[df['DATA_MOVTO'].dt.date == dia]

        fat   = float(df_dia['VALOR_LIQUIDO'].sum())
        kg    = float(df_dia['QTDE_PRI'].sum())
        notas = int(df_dia['NUM_DOCTO'].nunique()) if 'NUM_DOCTO' in df_dia.columns else 0

        ultima_str = "—"
        try:
            ultima = df.dropna(subset=['DATA_MOVTO']).sort_values('DATA_MOVTO').iloc[-1]['DATA_MOVTO']
            ultima_str = ultima.strftime('%d/%m/%Y %H:%M') if pd.notna(ultima) else "—"
        except: pass

        top = (df_dia.groupby('NOME_CLIENTE')
               .agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum'))
               .sort_values('kg', ascending=False).head(10).reset_index())
        top10 = [{"nome": r.NOME_CLIENTE, "kg": round(r.kg,2), "fat": round(r.fat,2)}
                 for r in top.itertuples()]

        df_dia2 = df_dia.copy()
        df_dia2['DESC_DIVISAO2'] = df_dia2['DESC_DIVISAO2'].fillna('').str.strip()
        df_dia2.loc[df_dia2['DESC_DIVISAO2'] == '', 'DESC_DIVISAO2'] = 'SEM CLASS.'
        tipos_grp = (df_dia2.groupby('DESC_DIVISAO2')
                     .agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum'))
                     .sort_values('kg', ascending=False).reset_index())
        tipos = [{"tipo": r.DESC_DIVISAO2, "kg": round(r.kg,2), "fat": round(r.fat,2)}
                 for r in tipos_grp.itertuples()]

        csv_modificado_str = "—"
        try:
            service = get_drive_service()
            meta = service.files().get(fileId=FILE_ID, fields='modifiedTime').execute()
            mod = meta.get('modifiedTime','')
            if mod:
                dt_utc = datetime.strptime(mod, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
                csv_modificado_str = dt_utc.astimezone().strftime('%d/%m/%Y %H:%M')
        except: pass

        meses_pt = ['Janeiro','Fevereiro','Março','Abril','Maio','Junho',
                    'Julho','Agosto','Setembro','Outubro','Novembro','Dezembro']
        return JSONResponse({
            "total_registros": len(df),
            "dia_label":       dia.strftime('%d/%m/%Y'),
            "mes_label":       f"{meses_pt[dia.month-1]}/{dia.year}",
            "fat":             round(fat,2),
            "kg":              round(kg,2),
            "notas":           notas,
            "ultima_nota":     ultima_str,
            "csv_modificado":  csv_modificado_str,
            "top10":           top10,
            "tipos":           tipos
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/cliente/{nome:path}")
def detalhe_cliente(nome: str):
    try:
        df = load_df()
        hoje = datetime.now().date()
        dia = hoje if (df['DATA_MOVTO'].dt.date == hoje).any() else df['DATA_MOVTO'].dt.date.max()
        df_dia = df[df['DATA_MOVTO'].dt.date == dia]
        mask = df_dia['NOME_CLIENTE'].str.upper() == nome.upper()
        if mask.sum() == 0:
            mask = df_dia['NOME_CLIENTE'].str.upper().str.contains(nome.upper()[:20], na=False)
        df_cli = df_dia[mask]
        fat_total = float(df_cli['VALOR_LIQUIDO'].sum())
        kg_total  = float(df_cli['QTDE_PRI'].sum())
        prods = (df_cli.groupby(['DESC_PRODUTO','DESC_DIVISAO2'])
                 .agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum'))
                 .sort_values('kg', ascending=False).reset_index())
        produtos = [{"nome": r.DESC_PRODUTO, "tipo": r.DESC_DIVISAO2, "kg": round(r.kg,2), "fat": round(r.fat,2)}
                    for r in prods.itertuples()]
        return JSONResponse({"nome": nome, "fat_total": round(fat_total,2),
                             "kg_total": round(kg_total,2), "produtos": produtos})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tipo/{tipo}")
def detalhe_tipo(tipo: str):
    try:
        df = load_df()
        hoje = datetime.now().date()
        dia = hoje if (df['DATA_MOVTO'].dt.date == hoje).any() else df['DATA_MOVTO'].dt.date.max()
        df_dia = df[df['DATA_MOVTO'].dt.date == dia]
        df_tipo = df_dia[df_dia['DESC_DIVISAO2'].str.upper() == tipo.upper()]
        fat_total = float(df_tipo['VALOR_LIQUIDO'].sum())
        kg_total  = float(df_tipo['QTDE_PRI'].sum())
        notas     = int(df_tipo['NUM_DOCTO'].nunique())
        pm        = round(fat_total / kg_total, 2) if kg_total > 0 else 0
        clientes = (df_tipo.groupby('NOME_CLIENTE').agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum'))
                    .sort_values('kg', ascending=False).head(10).reset_index())
        produtos = (df_tipo.groupby('DESC_PRODUTO').agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum'))
                    .sort_values('kg', ascending=False).head(10).reset_index())
        return JSONResponse({
            "tipo": tipo, "fat_total": round(fat_total,2), "kg_total": round(kg_total,2),
            "cx_total": round(kg_total/30,0), "notas": notas, "pm": pm,
            "clientes": [{"nome": r.NOME_CLIENTE, "kg": round(r.kg,2), "fat": round(r.fat,2)} for r in clientes.itertuples()],
            "produtos": [{"nome": r.DESC_PRODUTO, "kg": round(r.kg,2), "fat": round(r.fat,2)} for r in produtos.itertuples()]
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────────
#  ROUTE — /chat (nova arquitetura)
# ─────────────────────────────────────────────
@app.post("/chat")
async def chat(req: ChatRequest):
    if not CLAUDE_KEY:
        raise HTTPException(status_code=500, detail="CLAUDE_API_KEY não configurada.")

    ultima = next((m.content for m in reversed(req.messages) if m.role == "user"), "")
    historico = [{"role": m.role, "content": m.content} for m in req.messages]

    # ── Saudação / Quem sou eu ──
    if ultima.startswith('__QUEM_SOU_EU__') or any(x in ultima.lower() for x in ['quem é você','quem e voce','o que você faz','o que voce faz']):
        try:
            df = load_df()
            csv_mod = ultima.split('csv_modificado=')[-1].strip() if 'csv_modificado=' in ultima else '—'
            d_min = df['DATA_MOVTO'].dropna().min().strftime('%d/%m/%Y')
            d_max = df['DATA_MOVTO'].dropna().max().strftime('%d/%m/%Y')
            resposta = "\n".join([
                "## Olá! Sou o IAF — Analista Comercial Frinense",
                "",
                "Fui desenvolvido para analisar os dados comerciais da **Frinense Alimentos**.",
                "",
                "**O que posso fazer:**",
                "- Analisar faturamento e volume por período, filial, vendedor ou cliente",
                "- Identificar top clientes, produtos e regiões",
                "- Detalhar notas fiscais e gerar DANFE em PDF",
                "- Comparar desempenho entre filiais e períodos",
                "- Criar rankings e relatórios comerciais",
                "",
                f"**Período de dados disponível:** {d_min} até {d_max}",
                f"**Total de registros:** {len(df):,}",
                f"**Última atualização:** {csv_mod}",
            ])
        except Exception as e:
            resposta = f"Erro ao carregar informações: {e}"
        return JSONResponse({"content": [{"type": "text", "text": resposta}]})

    # ── Pergunta precisa de cliente? ──
    df = load_df()

    # ETAPA 1 — Interpretar
    try:
        filtro = await interpretar_pergunta(ultima, historico[:-1], df)
    except Exception as e:
        logging.error(f"[INTERPRETAR] erro: {e}")
        filtro = {"tipo": "indefinido"}

    logging.info(f"[FILTRO] {json.dumps(filtro, ensure_ascii=False)}")

    # Se precisa de cliente e não foi informado
    if filtro.get("precisa_cliente") and not filtro.get("cliente") and not filtro.get("cnpj_raiz"):
        return JSONResponse({"content": [{"type": "text", "text":
            "Para qual cliente? Pode informar o nome ou CNPJ raiz (8 dígitos)."}]})

    # Se período indefinido para resumo
    if filtro.get("precisa_periodo") and not filtro.get("data_inicio"):
        return JSONResponse({"content": [{"type": "text", "text":
            "Qual período você quer analisar? Ex: março 2026, esta semana, últimos 30 dias..."}]})

    # Nota não encontrada
    if filtro.get("nr_nota"):
        if 'NUM_DOCTO' in df.columns:
            encontrou = (df['NUM_DOCTO'].astype(str).str.strip() == str(filtro["nr_nota"]).strip()).sum()
            if encontrou == 0:
                return JSONResponse({"content": [{"type": "text", "text":
                    f"❌ Nota **{filtro['nr_nota']}** não encontrada nos dados disponíveis."}]})

    # ETAPA 2 — Calcular
    try:
        resultado = calcular(df, filtro)
    except Exception as e:
        logging.error(f"[CALCULAR] erro: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao calcular dados: {e}")

    # Se sem dados
    if resultado.get("sem_dados"):
        return JSONResponse({"content": [{"type": "text", "text":
            "⚠️ Sem dados disponíveis para o período/filtro solicitado. Verifique o período ou tente outro filtro."}]})

    # ETAPA 3 — Narrar
    try:
        resposta_texto = await narrar(ultima, resultado, historico[:-1], req.modo)
    except Exception as e:
        logging.error(f"[NARRAR] erro: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao narrar: {e}")

    return JSONResponse({
        "id": "iaf-response",
        "type": "message",
        "role": "assistant",
        "content": [{"type": "text", "text": resposta_texto}],
        "model": "iaf-v2",
        "stop_reason": "end_turn"
    })

# ─────────────────────────────────────────────
#  ROUTES — DEBUG
# ─────────────────────────────────────────────
@app.get("/debug-csv")
def debug_csv():
    try:
        df = load_df()
        filiais = df['NOME_FILIAL'].unique().tolist() if 'NOME_FILIAL' in df.columns else []
        meses = sorted(df['DATA_MOVTO'].dt.to_period('M').dropna().unique().astype(str).tolist())
        cache_age = round(time.time() - _cache["ts"], 0) if _cache["ts"] else None
        return JSONResponse({"file_id": FILE_ID, "total_linhas": len(df),
                             "filiais": filiais, "meses": meses,
                             "cache_age_segundos": cache_age})
    except Exception as e:
        return JSONResponse({"erro": str(e)})

@app.get("/cache/invalidar")
def invalidar():
    invalidar_cache()
    return JSONResponse({"status": "cache invalidado"})

@app.get("/health")
def health():
    return {"status": "ok", "sistema": "IAF-v2"}

# ─────────────────────────────────────────────
#  IA3 — preservado integralmente
# ─────────────────────────────────────────────
FILE_ID_IA3 = os.environ.get("DRIVE_FILE_ID_IA3", "")

def load_df_ia3() -> pd.DataFrame:
    if not FILE_ID_IA3:
        raise HTTPException(status_code=500, detail="DRIVE_FILE_ID_IA3 não configurado.")
    service = get_drive_service()
    req = service.files().get_media(fileId=FILE_ID_IA3)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    buf.seek(0)
    df = pd.read_csv(buf, sep=';', encoding='utf-8-sig', low_memory=False)
    sample = str(df['DATASAIDA'].dropna().iloc[0]) if len(df) > 0 else ''
    use_dayfirst = bool(re.match(r'\d{1,2}/\d{1,2}/\d{2,4}', sample))
    df['DATASAIDA'] = pd.to_datetime(df['DATASAIDA'], errors='coerce', dayfirst=use_dayfirst)
    for col in ['TOTVEND','TOTCUSTO','QTDEKG','VALORDESCONTO']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

@app.get("/ia3", response_class=HTMLResponse)
def ia3():
    for p in ["ia3.html", "/app/ia3.html"]:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
    return HTMLResponse("<h1>IA3</h1>")

@app.get("/dashboard-ia3")
def dashboard_ia3():
    try:
        df = load_df_ia3()
        ultimo_mes = df['DATASAIDA'].dt.to_period('M').max()
        df_mes = df[df['DATASAIDA'].dt.to_period('M') == ultimo_mes]
        fat      = float(df_mes['TOTVEND'].sum())
        kg       = float(df_mes['QTDEKG'].sum())   if 'QTDEKG'       in df_mes.columns else 0
        custo    = float(df_mes['TOTCUSTO'].sum())  if 'TOTCUSTO'     in df_mes.columns else 0
        desconto = float(df_mes['VALORDESCONTO'].sum()) if 'VALORDESCONTO' in df_mes.columns else 0
        margem_pct = round((fat - custo) / fat * 100, 1) if fat > 0 else 0
        mes_label  = df_mes['DATASAIDA'].dt.strftime('%m/%Y').iloc[0] if len(df_mes) > 0 else '—'
        top = (df_mes.groupby('NOMECLIENTE').agg(fat=('TOTVEND','sum'), kg=('QTDEKG','sum'))
               .sort_values('fat', ascending=False).head(5).reset_index())
        top5 = [{"nome": r.NOMECLIENTE, "fat": round(r.fat,2), "kg": round(r.kg,2)} for r in top.itertuples()]
        return JSONResponse({"total_registros": len(df), "mes_label": mes_label,
                             "fat": round(fat,2), "kg": round(kg,2),
                             "margem_pct": margem_pct, "desconto": round(desconto,2), "top5": top5})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/cliente-ia3/{nome}")
def cliente_ia3(nome: str, mes: str = None):
    try:
        df = load_df_ia3()
        periodo = pd.Period(mes, freq='M') if mes else df['DATASAIDA'].dt.to_period('M').max()
        df = df[df['DATASAIDA'].dt.to_period('M') == periodo]
        dfc = pd.DataFrame()
        for tam in [25, 15, 8, 5]:
            mask = df['NOMECLIENTE'].str.lower().str.contains(nome.lower()[:tam], na=False)
            dfc = df[mask]
            if len(dfc) > 0: break
        if len(dfc) == 0:
            return JSONResponse({"erro": "Cliente não encontrado"})
        fat_total   = float(dfc['TOTVEND'].sum())
        kg_total    = float(dfc['QTDEKG'].sum())    if 'QTDEKG'        in dfc.columns else 0
        custo_total = float(dfc['TOTCUSTO'].sum())  if 'TOTCUSTO'      in dfc.columns else 0
        desc_total  = float(dfc['VALORDESCONTO'].sum()) if 'VALORDESCONTO' in dfc.columns else 0
        margem_pct  = round((fat_total - custo_total) / fat_total * 100, 1) if fat_total > 0 else 0
        pm          = round(fat_total / kg_total, 2) if kg_total > 0 else 0
        deptos = []
        if 'NOMEDEPARTAMENTO' in dfc.columns:
            por_depto = dfc.groupby('NOMEDEPARTAMENTO').agg(fat=('TOTVEND','sum'), kg=('QTDEKG','sum')).sort_values('fat', ascending=False)
            deptos = [{"nome": idx, "fat": round(float(r.fat),2), "kg": round(float(r.kg),2)} for idx, r in por_depto.iterrows()]
        produtos = []
        if 'DESCRICAOPRODUTO' in dfc.columns:
            por_prod = dfc.groupby('DESCRICAOPRODUTO').agg(fat=('TOTVEND','sum'), kg=('QTDEKG','sum')).sort_values('fat', ascending=False).head(15)
            produtos = [{"nome": idx, "fat": round(float(r.fat),2), "kg": round(float(r.kg),2)} for idx, r in por_prod.iterrows()]
        return JSONResponse({"nome": dfc['NOMECLIENTE'].iloc[0], "fat_total": round(fat_total,2),
                             "kg_total": round(kg_total,2), "margem_pct": margem_pct,
                             "pm": pm, "desc_total": round(desc_total,2),
                             "deptos": deptos, "produtos": produtos})
    except Exception as e:
        return JSONResponse({"erro": str(e)})

@app.post("/chat-ia3")
async def chat_ia3(req: ChatRequest):
    if not CLAUDE_KEY:
        raise HTTPException(status_code=500, detail="CLAUDE_API_KEY não configurada.")
    ultima = next((m.content for m in reversed(req.messages) if m.role == "user"), "")
    try:
        df = load_df_ia3()
        hoje = datetime.now().date()
        ultimo_mes = df['DATASAIDA'].dt.to_period('M').max()
        df_mes = df[df['DATASAIDA'].dt.to_period('M') == ultimo_mes]
        d_max = df['DATASAIDA'].dropna().max()
        data_label = f"Referência: {d_max.strftime('%d/%m/%Y') if hasattr(d_max,'strftime') else d_max}"
        cols_display = [c for c in ['DATASAIDA','NOMEFILIAL','NOMEVENDEDOR','NOMEROTA','NOMECLIENTE',
                                     'CIDADE','UF','DESCRICAOPRODUTO','NOMECURTO','NOMEGRUPO',
                                     'NOMESUBGRUPO','NOMEDEPARTAMENTO','UND','QTDE','QTDEKG',
                                     'TOTVEND','TOTCUSTO','VALORDESCONTO','DESCRICAOCONDPGVENDA'] if c in df_mes.columns]
        sales_data = df_mes[cols_display].to_string(index=False, max_rows=400) if len(df_mes) > 0 else "Sem dados."
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    system = f"""Você é o IA3 — Analista Comercial da empresa 3F.
⛔ NUNCA INVENTAR DADOS — use apenas os dados fornecidos abaixo.
- Responda em português brasileiro, tom executivo
- Use Markdown com tabelas e negrito
- Valores: R$ X.XXX,XX | Kg: X.XXX kg | Datas: DD/MM/AA
- Finalize com 💡 Insight:

DADOS ({data_label}):
{sales_data}"""

    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={"Content-Type":"application/json","x-api-key":CLAUDE_KEY,"anthropic-version":"2023-06-01"},
            json={"model":"claude-haiku-4-5-20251001","max_tokens":1500,"system":system,
                  "messages":[m.dict() for m in req.messages]}
        )
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)
    return r.json()

@app.get("/health-ia3")
def health_ia3():
    return {"status": "ok", "sistema": "IA3"}

# ─────────────────────────────────────────────
#  DANFE
# ─────────────────────────────────────────────
@app.get("/danfe/{chave}")
async def get_danfe(chave: str):
    import asyncio
    if not re.match(r'^\d{44}$', chave):
        raise HTTPException(status_code=400, detail="Chave de acesso inválida.")
    BASE = "https://api.meudanfe.com.br/v2"
    headers = {"Api-Key": MEUDANFE_KEY}
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.get(f"{BASE}/fd/get/da/{chave}", headers=headers)
            if r.status_code == 404 or len(r.content) == 0:
                r2 = await client.put(f"{BASE}/fd/add/{chave}", headers=headers)
                if r2.status_code not in (200, 201, 202):
                    raise HTTPException(status_code=502, detail=f"MeuDanfe erro: {r2.status_code}")
                for _ in range(10):
                    await asyncio.sleep(2)
                    r = await client.get(f"{BASE}/fd/get/da/{chave}", headers=headers)
                    if r.status_code == 200 and len(r.content) > 100:
                        break
                else:
                    raise HTTPException(status_code=504, detail="MeuDanfe: timeout.")
            if r.status_code != 200 or len(r.content) == 0:
                raise HTTPException(status_code=502, detail=f"PDF indisponível ({r.status_code})")
            import base64 as _b64
            try:
                data = r.json()
                pdf_bytes = _b64.b64decode(data["data"]) if isinstance(data, dict) and data.get("format") == "BASE64" else r.content
            except:
                pdf_bytes = r.content
            return Response(content=pdf_bytes, media_type="application/pdf",
                            headers={"Content-Disposition": f"inline; filename=DANFE_{chave}.pdf"})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro DANFE: {e}")
