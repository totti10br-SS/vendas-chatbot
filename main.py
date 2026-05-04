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
from datetime import datetime, timedelta, timezone, date
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel
from typing import List, Optional
import httpx
from google.auth.transport.requests import Request
import starlette.requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
try:
    from weasyprint import HTML as WeasyprintHTML
    WEASYPRINT_OK = True
except Exception:
    WEASYPRINT_OK = False

# Fallback ReportLab (sempre disponível)
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable
from reportlab.lib.enums import TA_RIGHT, TA_CENTER

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
app = FastAPI()
app.add_middleware(CORSMiddleware,
    allow_origins=["https://web-production-91aff.up.railway.app"],
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"])

FILE_ID       = os.environ.get("DRIVE_FILE_ID", "")
CLAUDE_KEY    = os.environ.get("CLAUDE_API_KEY", "")
ELEVENLABS_KEY = os.environ.get("ELEVENLABS_API_KEY", "")
MEUDANFE_KEY  = "0c1588f4-f90e-4711-8b39-87be9a1581da"

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
  "tipo": "resumo_mensal|resumo_diario|ultimas_vendas|ultimos_precos|detalhe_nota|ranking_clientes|ranking_produtos|ranking_vendedores|comparativo|grafico|cnpj_query|periodo_livre|saudacao|indefinido",
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
  "data_inicio_b": "YYYY-MM-DD ou null",
  "data_fim_b": "YYYY-MM-DD ou null",
  "formato": "normal|pdf",
  "tipo_operacao": "PRODUTOS|SERVICOS|TODOS",
  "observacao": "qualquer detalhe extra relevante ou null"
}}

REGRAS:
- "hoje" = {hoje}
- "mês passado" = mês anterior ao atual
- "esta semana" = segunda-feira até hoje
- Se período não especificado e tipo for resumo: use o último mês disponível
- Se cliente não especificado e tipo for ultimas_vendas: precisa_cliente=true
- IMPORTANTE: Se a última mensagem do assistente no histórico for "Para qual cliente?" (ou similar pedindo nome de cliente), e a pergunta atual for apenas um nome/CNPJ, então herde o tipo da penúltima mensagem do usuário e preencha o cliente com o valor informado agora. NÃO mude o tipo.
- Se nr_nota mencionado: tipo="detalhe_nota"
- Se usuário perguntar "última nota", "última nota emitida", "quando foi a última nota", "qual foi a última nota", "me mostra a última nota" para um cliente: tipo="ultimas_vendas", data_inicio=null, data_fim=null (SEM filtro de período — busca em TODO o histórico), precisa_cliente=true se cliente não informado
- Se usuário perguntar sobre PDF, DANFE, nota fiscal, NF, ou detalhe de nota SEM informar número: tipo="detalhe_nota", nr_nota=null
- Se tipo="detalhe_nota" e nr_nota=null: o sistema vai pedir o número automaticamente
- Para comparativos entre dois períodos EXPLÍCITOS (ex: "março 2026 vs março 2025", "abril 2026 com abril 2025"):
  * tipo="comparativo"
  * data_inicio/data_fim = período A (mais recente)
  * data_inicio_b/data_fim_b = período B (mais antigo)
  * comparar_periodo_anterior=false
- Para comparativo com período imediatamente anterior (ex: "vs mês passado"): comparar_periodo_anterior=true
- tipo_operacao="PRODUTOS" por padrão em TODAS as consultas. Somente use "SERVICOS" se o usuário mencionar explicitamente serviços/serviço. Use "TODOS" apenas se pedir ambos juntos.
- Se usuário pedir "em PDF", "relatório PDF", "manda em PDF", "exportar PDF": formato="pdf"
- "últimas vendas", "últimas notas", "histórico de compras": tipo="ultimas_vendas" → mostra itens de notas (data, NF, produto, kg, valor)
- "últimos preços", "preço atual", "quanto paga", "tabela de preços": tipo="ultimos_precos" → mostra preço mais recente por produto; se período não especificado: precisa_periodo=false (o sistema assume 90 dias automaticamente)
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

    # Tipo de operação — padrão PRODUTOS, override apenas se solicitado
    if 'TIPO_OPERACAO' in dff.columns:
        tp = filtro.get("tipo_operacao", "PRODUTOS").upper()
        if tp == "SERVICOS":
            dff = dff[dff['TIPO_OPERACAO'] == 'SERVICOS']
        elif tp != "TODOS":
            dff = dff[dff['TIPO_OPERACAO'] == 'PRODUTOS']
        # se TODOS, não filtra

    return dff

def _periodo_anterior(filtro: dict) -> tuple:
    """Calcula data_inicio/fim do período anterior equivalente."""
    # Se o filtro já tem datas B explícitas, usa elas
    if filtro.get("data_inicio_b") and filtro.get("data_fim_b"):
        try:
            return pd.to_datetime(filtro["data_inicio_b"]), pd.to_datetime(filtro["data_fim_b"])
        except:
            pass
    # Senão calcula período imediatamente anterior
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
        grp_prod_cols = ['DESC_PRODUTO']
        if 'DESC_DIVISAO2' in dff.columns: grp_prod_cols.append('DESC_DIVISAO2')
        if 'DESC_DIVISAO3' in dff.columns: grp_prod_cols.append('DESC_DIVISAO3')
        top_prod = dff.groupby(grp_prod_cols).agg(
            kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum')
        ).sort_values('kg', ascending=False).head(15).reset_index()
        d["top_produtos"] = []
        for _, r in top_prod.iterrows():
            d["top_produtos"].append({
                "nome": str(r['DESC_PRODUTO']),
                "divisao": str(r['DESC_DIVISAO2']) if 'DESC_DIVISAO2' in top_prod.columns else None,
                "tipo_corte": str(r['DESC_DIVISAO3']) if 'DESC_DIVISAO3' in top_prod.columns else None,
                "kg": round(float(r.kg),2),
                "cx30": int(round(r.kg/30,0)),
                "faturamento": round(float(r.fat),2),
                "pm": round(float(r.fat)/float(r.kg),2) if r.kg > 0 else 0
            })

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
        grp_cols_div = ['DESC_DIVISAO2']
        if 'DESC_DIVISAO3' in dff.columns:
            grp_cols_div = ['DESC_DIVISAO2','DESC_DIVISAO3']
        por_tipo = dff.groupby(grp_cols_div).agg(
            kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum')
        ).sort_values('kg', ascending=False).reset_index()
        d["por_tipo_corte"] = []
        for _, r in por_tipo.iterrows():
            item = {
                "tipo": str(r['DESC_DIVISAO2']),
                "tipo_corte": str(r['DESC_DIVISAO3']) if 'DESC_DIVISAO3' in por_tipo.columns else None,
                "kg": round(float(r.kg),2),
                "cx30": int(round(r.kg/30,0)),
                "faturamento": round(float(r.fat),2),
                "pm": round(float(r.fat)/float(r.kg),2) if r.kg > 0 else 0
            }
            d["por_tipo_corte"].append(item)

    # ── Por tipo de movimento (COD_TIPO_MV / DESC_TIPO_MV) ──
    if 'DESC_TIPO_MV' in dff.columns:
        cols_tmv = ['COD_TIPO_MV','DESC_TIPO_MV'] if 'COD_TIPO_MV' in dff.columns else ['DESC_TIPO_MV']
        por_tmv = dff.groupby(cols_tmv).agg(
            kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum'),
            notas=('NUM_DOCTO','nunique') if 'NUM_DOCTO' in dff.columns else ('VALOR_LIQUIDO','count')
        ).sort_values('kg', ascending=False)
        d["por_tipo_movimento"] = []
        for idx, r in por_tmv.iterrows():
            cod  = idx[0] if isinstance(idx, tuple) else ''
            desc = idx[1] if isinstance(idx, tuple) else idx
            d["por_tipo_movimento"].append({
                "cod": str(cod), "desc": str(desc),
                "kg": round(float(r.kg),2), "cx30": int(round(r.kg/30,0)),
                "faturamento": round(float(r.fat),2),
                "pm": round(float(r.fat)/float(r.kg),2) if r.kg > 0 else 0,
                "notas": int(r.notas)
            })

    # ── Detalhe de nota fiscal ──
    if tipo == "detalhe_nota" and 'NUM_DOCTO' in dff.columns:
        cols_nota = [c for c in ['NUM_DOCTO','DATA_MOVTO','NOME_CLIENTE','NOME_FILIAL',
                                  'NOM_VENDEDOR','COD_PRODUTO','DESC_PRODUTO','DESC_DIVISAO2','DESC_DIVISAO3',
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
        if 'NOME_CLIENTE' in dff.columns:
            d["cliente_encontrado"] = dff['NOME_CLIENTE'].iloc[0]

        # Resumo por nota (agrupado)
        if 'NUM_DOCTO' in dff.columns:
            grp = dff.groupby(['DATA_MOVTO','NUM_DOCTO']).agg(
                kg=('QTDE_PRI','sum'),
                fat=('VALOR_LIQUIDO','sum'),
                n_itens=('COD_PRODUTO','count') if 'COD_PRODUTO' in dff.columns else ('VALOR_LIQUIDO','count'),
            ).reset_index().sort_values('DATA_MOVTO', ascending=False).head(30)
            extras = {}
            for col in ['NOME_FILIAL','NOM_VENDEDOR','CHAVE_ACESSO']:
                if col in dff.columns:
                    extras[col] = dff.groupby('NUM_DOCTO')[col].first()
            resumo_notas = []
            for _, r in grp.iterrows():
                nr = r['NUM_DOCTO']
                resumo_notas.append({
                    "data":     r['DATA_MOVTO'].strftime('%d/%m/%Y') if hasattr(r['DATA_MOVTO'],'strftime') else str(r['DATA_MOVTO']),
                    "nr_nota":  str(nr),
                    "filial":   str(extras['NOME_FILIAL'].get(nr,'')) if 'NOME_FILIAL' in extras else '',
                    "vendedor": str(extras['NOM_VENDEDOR'].get(nr,'')) if 'NOM_VENDEDOR' in extras else '',
                    "kg":       round(float(r['kg']),2),
                    "cx30":     int(round(float(r['kg'])/30,0)),
                    "fat":      round(float(r['fat']),2),
                    "pm":       round(float(r['fat'])/float(r['kg']),2) if float(r['kg']) > 0 else 0,
                    "n_itens":  int(r['n_itens']),
                    "chave":    str(extras['CHAVE_ACESSO'].get(nr,'')) if 'CHAVE_ACESSO' in extras else '',
                })
            d["resumo_notas"] = resumo_notas

        # Itens detalhados — max 100 linhas
        cols_item = [col for col in ['DATA_MOVTO','NUM_DOCTO','NOME_FILIAL','NOM_VENDEDOR',
                                     'COD_PRODUTO','DESC_PRODUTO','DESC_DIVISAO2','DESC_DIVISAO3',
                                     'QTDE_PRI','QTDE_AUX','VALOR_UNITARIO','VALOR_LIQUIDO',
                                     'CHAVE_ACESSO'] if col in dff.columns]
        ultimas = dff[cols_item].sort_values('DATA_MOVTO', ascending=False).head(100)
        registros = []
        for _, row in ultimas.iterrows():
            item = {}
            for col in cols_item:
                v = row[col]
                if hasattr(v, 'strftime'):
                    item[col] = v.strftime('%d/%m/%Y')
                elif pd.isna(v):
                    item[col] = None
                else:
                    item[col] = v
            registros.append(item)
        d["itens_detalhados"] = registros

    # ── Últimos preços por produto ──
    if tipo == "ultimos_precos" and "DESC_PRODUTO" in dff.columns:
        cliente_nome = dff['NOME_CLIENTE'].iloc[0] if 'NOME_CLIENTE' in dff.columns and len(dff) > 0 else None
        if cliente_nome:
            d["cliente_encontrado"] = str(cliente_nome)
        # Para cada produto, pega a última nota e o preço praticado
        prod_grp = dff.sort_values('DATA_MOVTO', ascending=False).groupby(['COD_PRODUTO','DESC_PRODUTO']).agg(
            ultima_data=('DATA_MOVTO','first'),
            ultima_nota=('NUM_DOCTO','first') if 'NUM_DOCTO' in dff.columns else ('VALOR_LIQUIDO','count'),
            kg_total=('QTDE_PRI','sum'),
            fat_total=('VALOR_LIQUIDO','sum'),
            ultimo_vl_unit=('VALOR_UNITARIO','first') if 'VALOR_UNITARIO' in dff.columns else ('VALOR_LIQUIDO','first'),
            n_compras=('NUM_DOCTO','nunique') if 'NUM_DOCTO' in dff.columns else ('VALOR_LIQUIDO','count'),
        ).reset_index().sort_values('ultima_data', ascending=False)
        precos = []
        for _, r in prod_grp.iterrows():
            kg_t = float(r['kg_total'])
            fat_t = float(r['fat_total'])
            pm = round(fat_t / kg_t, 2) if kg_t > 0 else 0
            precos.append({
                "cod": str(r['COD_PRODUTO']),
                "produto": str(r['DESC_PRODUTO']),
                "ultima_data": r['ultima_data'].strftime('%d/%m/%Y') if hasattr(r['ultima_data'],'strftime') else str(r['ultima_data']),
                "ultima_nota": str(r['ultima_nota']),
                "ultimo_vl_unit": round(float(r['ultimo_vl_unit']),2),
                "pm_historico": pm,
                "kg_total": round(kg_t,2),
                "n_compras": int(r['n_compras']),
            })
        d["ultimos_precos"] = precos

    # ── Comparativo com período anterior ──
    if (filtro.get("comparar_periodo_anterior") or filtro.get("tipo") == "comparativo") and filtro.get("data_inicio") and filtro.get("data_fim"):
        pa_ini, pa_fim = _periodo_anterior(filtro)
        if pa_ini:
            filtro_pa = {**filtro, "data_inicio": pa_ini.strftime('%Y-%m-%d'),
                         "data_fim": pa_fim.strftime('%Y-%m-%d'),
                         "comparar_periodo_anterior": False}
            dff_pa = _aplicar_filtros(df, filtro_pa)
            if len(dff_pa) > 0:
                fat_pa  = round(float(dff_pa['VALOR_LIQUIDO'].sum()), 2)
                kg_pa   = round(float(dff_pa['QTDE_PRI'].sum()), 2)
                notas_pa = int(dff_pa['NUM_DOCTO'].nunique()) if 'NUM_DOCTO' in dff_pa.columns else 0
                pm_pa   = round(fat_pa / kg_pa, 2) if kg_pa > 0 else 0
                cx_pa   = int(round(kg_pa / 30, 0))
                # Por filial período B
                filial_b = []
                if 'NOME_FILIAL' in dff_pa.columns:
                    grp_b = dff_pa.groupby('NOME_FILIAL').agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum')).sort_values('kg', ascending=False)
                    filial_b = [{"filial": idx, "kg": round(float(r.kg),2), "cx30": int(round(r.kg/30,0)), "faturamento": round(float(r.fat),2), "pm": round(float(r.fat)/float(r.kg),2) if r.kg > 0 else 0} for idx, r in grp_b.iterrows()]
                d["comparativo"] = {
                    "periodo_a_ini": d.get("periodo_ini"),
                    "periodo_a_fim": d.get("periodo_fim"),
                    "periodo_b_ini": pa_ini.strftime('%d/%m/%Y'),
                    "periodo_b_fim": pa_fim.strftime('%d/%m/%Y'),
                    "periodo_a": {"kg": kg, "cx30": int(cx), "faturamento": fat, "pm": pm, "notas": notas, "por_filial": d.get("por_filial",[])},
                    "periodo_b": {"kg": kg_pa, "cx30": cx_pa, "faturamento": fat_pa, "pm": pm_pa, "notas": notas_pa, "por_filial": filial_b},
                    "var_fat_pct": round((fat - fat_pa) / fat_pa * 100, 1) if fat_pa > 0 else None,
                    "var_kg_pct":  round((kg  - kg_pa)  / kg_pa  * 100, 1) if kg_pa  > 0 else None,
                    "var_pm_pct":  round((pm  - pm_pa)  / pm_pa  * 100, 1) if pm_pa  > 0 else None,
                    "var_notas_pct": round((notas - notas_pa) / notas_pa * 100, 1) if notas_pa > 0 else None,
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

    # Limita itens_detalhados para 50 no JSON (evita overflow de tokens)
    dados_narrar = resultado["dados"].copy()
    if "itens_detalhados" in dados_narrar and len(dados_narrar["itens_detalhados"]) > 50:
        dados_narrar["itens_detalhados"] = dados_narrar["itens_detalhados"][:50]
    if "resumo_notas" in dados_narrar and len(dados_narrar["resumo_notas"]) > 30:
        dados_narrar["resumo_notas"] = dados_narrar["resumo_notas"][:30]
    # Passar tipo_operacao para o narrador
    tipo_operacao = resultado.get("filtro_aplicado", {}).get("tipo_operacao", "PRODUTOS")
    dados_narrar["_tipo_operacao"] = tipo_operacao
    dados_json = json.dumps(dados_narrar, ensure_ascii=False, indent=2)
    tipo = resultado.get("tipo", "")

    system_narrar = f"""Você é o IAF — Inteligência Analítica Frinense, analista comercial sênior da Frinense Alimentos.{personalidade}

Sua missão é transformar dados de vendas em informação clara e útil para a equipe comercial. Você conhece profundamente o negócio: filiais de Itaperuna, Bom Jesus e Porciúncula, os produtos J.Beef (dianteiro, traseiro, ponta de agulha, peito, LP), Charque e Resfriadas.

**Seu estilo:**
- Direto, preciso e confiante — como um analista experiente que respeita o tempo de quem pergunta
- Sem rodeios, sem saudações, sem "Com prazer!" — vá direto ao dado
- Quando os números são bons, pode ser assertivo. Quando são ruins, seja neutro e factual
- Respostas CURTAS e objetivas — responda com o mínimo necessário. Sem introduções desnecessárias
- Use linguagem comercial brasileira — não corporativo demais, não informal demais

**Seus limites:**
- Trabalha APENAS com os dados disponíveis no sistema
- Se não tiver o dado, diz claramente: "Essa informação não está disponível na base."
- Não responde perguntas fora do escopo comercial da Frinense

**Contexto do negócio:**
- Filiais: Itaperuna (ITAP), Bom Jesus (BJESUS), Porciúncula (PORC) — sempre use o nome completo
- Produtos: J.Beef (dianteiro, traseiro, ponta de agulha, peito, LP) | Charque | Resfriadas
- Operações: vendas de produtos e serviços (separados)

⛔⛔⛔ REGRA ABSOLUTA — NUNCA INVENTAR DADOS ⛔⛔⛔
- USE APENAS os números do JSON abaixo. ZERO EXCEÇÕES.
- NUNCA arredonde, estime ou altere qualquer valor.
- Se um campo não existe no JSON = não existe. Não mencione.
- Filiais válidas: ITAP, BJESUS, PORC. Qualquer outra NÃO EXISTE.
⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔⛔

## FORMATO
- Tom executivo e direto — sem "Olá", "Claro!", "Com prazer"
- Respostas CURTAS e objetivas — responda com o mínimo necessário. Sem introduções, sem explicações desnecessárias. Vá direto ao dado solicitado.
- Use Markdown: ## títulos, **negrito**, tabelas com | Col |
- Valores: R$ X.XXX.XXX,XX | Kg: X.XXX.XXX kg | Datas: DD/MM/AA
- CX30 = kg/30 — sempre exiba junto com kg
- Nomes de filiais: ITAP = Itaperuna | BJESUS = Bom Jesus | PORC = Porciúncula — sempre use o nome completo, nunca o código
- Toda tabela DEVE ter linha "| **TOTAIS** |" no final
- NÃO inclua análise rápida, bullets de insight ou comentários automáticos — a menos que o usuário peça explicitamente ("analise", "o que você acha", "dê sua opinião")
- Se o usuário pedir resposta "em áudio", "em voz" ou similar: IGNORE essa parte do pedido e responda normalmente em texto — o sistema de áudio é gerenciado pelo frontend automaticamente. NUNCA diga que não consegue gerar áudio.
- Se o usuário pedir análise: finalize com 💡 **Insight:** e 📊 **ANÁLISE RÁPIDA:** com 3-4 bullets
- RESUMO DE VOZ OBRIGATÓRIO: Toda resposta que contenha tabela com totais DEVE terminar com um parágrafo em texto corrido (sem markdown) resumindo os principais números da tabela — ex: "No período, a Sendas faturou R$ 2.538.781,82 com 609 notas e volume de 72.510 kg a R$ 35,01/kg." Esse parágrafo é essencial para leitura em voz alta.
- SERVIÇOS — quando tipo_operacao=SERVICOS: nas tabelas de ranking/resumo por cliente, OMITA as colunas R$/kg e CX30. Mostre apenas: Cliente | Faturamento | Nº Notas

## COMPORTAMENTOS POR TIPO
- resumo_mensal / resumo_diario: Mostre KPIs gerais → por filial → por dia → previsão fechamento → top clientes → top produtos
- detalhe_nota: Mostre cabeçalho (filial, cliente, vendedor) + tabela de itens + DANFE se tiver chave_acesso
- ultimos_precos:
  Use o campo "ultimos_precos" do JSON.
  
  ## ÚLTIMOS PREÇOS · [cliente_encontrado]
  
  | COD | PRODUTO | ÚLTIMA COMPRA | NF | VL UNIT | R$/kg MÉDIO | TOTAL KG | Nº COMPRAS |
  |-----|---------|---------------|-----|---------|-------------|----------|------------|
  [uma linha por produto, ordenado por última data decrescente]
  
  Campos: cod=COD, produto=PRODUTO, ultima_data=ÚLTIMA COMPRA, ultima_nota=NF, ultimo_vl_unit=VL UNIT, pm_historico=R$/kg médio, kg_total=TOTAL KG, n_compras=Nº COMPRAS
  Sem totais. Sem análise automática.

- ultimas_vendas:
  Use "itens_detalhados" do JSON — cada linha é um item de nota fiscal.
  
  ## ÚLTIMAS VENDAS · [cliente_encontrado]
  
  | DATA | NR NOTA | FILIAL | COD PRODUTO | PRODUTO | KG | CX | VL UNIT | R$/kg |
  |------|---------|--------|-------------|---------|----|----|---------|-------|
  [uma linha por item de itens_detalhados, decrescente por DATA_MOVTO]
  
  Campos: DATA_MOVTO=data, NUM_DOCTO=NR NOTA, NOME_FILIAL=filial, COD_PRODUTO=cod, DESC_PRODUTO=produto, QTDE_PRI=kg, QTDE_AUX=cx, VALOR_UNITARIO=vl unit, calcule R$/kg = VALOR_LIQUIDO/QTDE_PRI
  
  Sem linha de totais. Sem análise automática.
  
  CASO ESPECIAL — ÚLTIMA NOTA: Se "resumo_notas" tiver exatamente 1 nota (ou o usuário perguntar "última nota", "quando foi a última nota"), exiba apenas o resumo dessa nota:
  **Nota [nr_nota]** · [data] · [filial]
  **Cliente:** [cliente_encontrado] | **Vendedor:** [vendedor]
  **Faturamento:** R$ [fat] | **Volume:** [kg] kg | **Itens:** [n_itens]
  
  E finalize com a pergunta (em texto corrido, sem markdown):
  "Deseja que eu gere o PDF (DANFE) desta nota? Responda sim para receber o arquivo."
- ranking_clientes: Tabela com posição, nome, kg, cx30, faturamento, R$/kg
- ranking_vendedores: Tabela com cod, nome, kg, cx30, faturamento, notas
- comparativo: 
  Use os campos periodo_a e periodo_b do JSON.
  Mostre tabela:
  | MÉTRICA | [periodo_a_ini ~ periodo_a_fim] | [periodo_b_ini ~ periodo_b_fim] | VAR % |
  |---------|--------------------------------|--------------------------------|-------|
  | Volume (kg) | ... | ... | +X% ⬆ ou -X% ⬇ |
  | CX30 | ... | ... | ... |
  | Faturamento | ... | ... | ... |
  | Preço Médio | ... | ... | ... |
  | Notas | ... | ... | ... |
  Depois mostre comparativo por filial se disponível.
  Use ⬆ verde para positivo e ⬇ vermelho para negativo nas variações.
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
    max_tok = 4000 if tipo in ("resumo_mensal", "comparativo") else 2500 if tipo in ("detalhe_nota", "ultimas_vendas") else 1500

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
def gerar_relatorio_pdf(df: pd.DataFrame, filtro: dict, resultado: dict) -> bytes:
    """Gera PDF para qualquer tipo de relatório."""
    import html as _h

    tipo_rel = filtro.get("tipo", "resumo_mensal")
    dados    = resultado.get("dados", {})
    d1       = filtro.get("data_inicio") or ""
    d2       = filtro.get("data_fim") or ""
    hoje_str = datetime.now().strftime("%d/%m/%Y %H:%M")

    # Período label
    try:
        p1 = datetime.strptime(d1, "%Y-%m-%d").strftime("%d/%m/%Y") if d1 else None
        p2 = datetime.strptime(d2, "%Y-%m-%d").strftime("%d/%m/%Y") if d2 else None
        if p1 and p2:   periodo_label = p1 if p1 == p2 else f"{p1} a {p2}"
        elif p1:        periodo_label = f"a partir de {p1}"
        else:           periodo_label = "Todos os períodos"
    except:
        periodo_label = f"{d1} a {d2}"

    def fmt_brl(v):
        try:
            if v is None: return "R$ 0,00"
            return f"R$ {float(v):,.2f}".replace(",","X").replace(".",",").replace("X",".")
        except: return "R$ 0,00"
    def fmt_kg(v):
        try:
            if v is None: return "0,00 kg"
            return f"{float(v):,.2f} kg".replace(",","X").replace(".",",").replace("X",".")
        except: return "0,00 kg"

    # ── Aplicar filtros no DataFrame ──
    dff = df.copy()
    if d1:
        try: dff = dff[dff["DATA_MOVTO"] >= pd.to_datetime(d1)]
        except: pass
    if d2:
        try: dff = dff[dff["DATA_MOVTO"] < pd.to_datetime(d2) + timedelta(days=1)]
        except: pass
    if filtro.get("filial") and "NOME_FILIAL" in dff.columns:
        dff = dff[dff["NOME_FILIAL"].str.upper() == filtro["filial"].upper()]

    # Filtro cliente
    cliente_label = None
    if filtro.get("cliente") and "NOME_CLIENTE" in dff.columns:
        for tam in [25, 15, 8, 5]:
            mask = dff["NOME_CLIENTE"].str.lower().str.contains(filtro["cliente"].lower()[:tam], na=False)
            if mask.sum() > 0:
                dff = dff[mask]
                cliente_label = dff["NOME_CLIENTE"].iloc[0]
                break
    elif filtro.get("cnpj_raiz") and "CPF_CGC" in dff.columns:
        raiz = re.sub(r"\D","",filtro["cnpj_raiz"])[:8]
        mask = dff["CPF_CGC"].astype(str).str.replace(r"\D","",regex=True).str.startswith(raiz)
        if mask.sum() > 0:
            dff = dff[mask]
            cliente_label = dff["NOME_CLIENTE"].iloc[0] if "NOME_CLIENTE" in dff.columns else raiz

    # Filtro vendedor
    vendedor_label = None
    if filtro.get("vendedor") and "NOM_VENDEDOR" in dff.columns:
        for tam in [20, 10, 5]:
            mask = dff["NOM_VENDEDOR"].str.lower().str.contains(filtro["vendedor"].lower()[:tam], na=False)
            if mask.sum() > 0:
                dff = dff[mask]
                vendedor_label = dff["NOM_VENDEDOR"].iloc[0]
                break

    if len(dff) == 0:
        raise Exception("Sem dados para o filtro solicitado.")

    # ── KPIs recalculados ──
    fat   = round(float(dff["VALOR_LIQUIDO"].sum()), 2)
    kg    = round(float(dff["QTDE_PRI"].sum()), 2) if "QTDE_PRI" in dff.columns else 0
    cx    = int(round(kg / 30, 0))
    notas = int(dff["NUM_DOCTO"].nunique()) if "NUM_DOCTO" in dff.columns else 0
    pm    = round(fat / kg, 2) if kg > 0 else 0

    # ── Labels do cabeçalho ──
    filial_label = filtro.get("filial") or "Todas as Filiais"
    subtitulo_parts = [filial_label]
    if cliente_label: subtitulo_parts.append(f"Cliente: {str(cliente_label)[:40]}")
    if vendedor_label: subtitulo_parts.append(f"Vendedor: {str(vendedor_label)[:30]}")
    subtitulo = " · ".join(subtitulo_parts)

    # ── Título do relatório por tipo ──
    titulos = {
        "resumo_mensal":    "RESUMO MENSAL DE VENDAS",
        "resumo_diario":    "RESUMO DIÁRIO DE VENDAS",
        "ultimas_vendas":   "ÚLTIMAS VENDAS POR NOTA",
        "ultimos_precos":   "ÚLTIMOS PREÇOS POR PRODUTO",
        "ranking_clientes": "RANKING DE CLIENTES",
        "ranking_vendedores": "RANKING DE VENDEDORES",
        "ranking_produtos":  "RANKING DE PRODUTOS",
        "comparativo":       "RELATÓRIO COMPARATIVO",
        "periodo_livre":     "RELATÓRIO DE VENDAS",
    }
    titulo_rel = titulos.get(tipo_rel, "RELATÓRIO DE VENDAS")

    # ── CORPO HTML por tipo ──

    def _tabela(headers, rows, totais=None):
        """Gera HTML de tabela com cabeçalho e opção de linha de totais."""
        ths = "".join(f"<th>{h}</th>" for h in headers)
        trs = ""
        for row in rows:
            tds = "".join(f'<td class="{"valor" if i >= len(headers)-3 else ""}">{_h.escape(str(v))}</td>' for i, v in enumerate(row))
            trs += f"<tr>{tds}</tr>"
        tot_html = ""
        if totais:
            tds_tot = "".join(f'<td class="valor"><strong>{_h.escape(str(v))}</strong></td>' for v in totais)
            tot_html = f'<tr class="total-row">{tds_tot}</tr>'
        return f"""<table>
          <thead><tr>{ths}</tr></thead>
          <tbody>{trs}{tot_html}</tbody>
        </table>"""

    corpo_html = ""
    resumo_tmv_html = ""

    logging.warning(f"[PDF-CORPO] tipo_rel={tipo_rel!r} dados_keys={list(resultado.get('dados',{}).keys())}")
    # ── ULTIMAS VENDAS: itens com filial, data, NF, cod produto, produto, qtde, R$/kg ──
    if tipo_rel in ("ultimas_vendas", "ranking_produtos"):
        cols_item = [col for col in ["DATA_MOVTO","NOME_FILIAL","NUM_DOCTO","COD_PRODUTO",
                                     "DESC_PRODUTO","QTDE_PRI","QTDE_AUX",
                                     "VALOR_UNITARIO","VALOR_LIQUIDO"] if col in dff.columns]
        df_itens = dff[cols_item].sort_values("DATA_MOVTO", ascending=False).head(300)
        linhas_i = ""
        for _, row in df_itens.iterrows():
            data_str = row["DATA_MOVTO"].strftime("%d/%m/%Y") if hasattr(row.get("DATA_MOVTO",None),"strftime") else ""
            qtde_pri = float(row.get("QTDE_PRI",0))
            qtde_aux = float(row.get("QTDE_AUX",0))
            vl_unit  = float(row.get("VALOR_UNITARIO",0))
            vl_liq   = float(row.get("VALOR_LIQUIDO",0))
            pm_item  = round(vl_liq / qtde_pri, 2) if qtde_pri > 0 else 0
            cod      = str(row.get("COD_PRODUTO",""))[:12]
            prod     = str(row.get("DESC_PRODUTO",""))[:40]
            linhas_i += f"<tr><td>{_h.escape(str(row.get('NOME_FILIAL',''))[:6])}</td><td style='white-space:nowrap'>{_h.escape(data_str)}</td><td>{_h.escape(str(row.get('NUM_DOCTO','')))}</td><td>{_h.escape(cod)}</td><td>{_h.escape(prod)}</td><td class='valor'>{qtde_pri:,.0f}</td><td class='valor'>{int(round(qtde_aux,0))}</td><td class='valor'>{fmt_brl(vl_unit)}</td><td class='valor'>{fmt_brl(pm_item)}</td></tr>"
        corpo_html = f"""<table style="table-layout:fixed;width:100%;font-size:7px;">
          <colgroup>
            <col style="width:5%"><col style="width:8%"><col style="width:7%">
            <col style="width:10%"><col style="width:35%">
            <col style="width:8%"><col style="width:5%">
            <col style="width:10%"><col style="width:12%">
          </colgroup>
          <thead><tr>
            <th>FILIAL</th><th>DATA</th><th>NF</th><th>COD</th><th>PRODUTO</th>
            <th style="text-align:right">KG</th><th style="text-align:right">CX</th>
            <th style="text-align:right">VL UNIT</th><th style="text-align:right">R$/KG</th>
          </tr></thead>
          <tbody>{linhas_i}</tbody>
        </table>"""

    # ── ÚLTIMOS PREÇOS por produto ──
    elif tipo_rel == "ultimos_precos":
        # Tenta pegar do resultado pré-calculado
        dados_preco = resultado.get("dados", {}).get("ultimos_precos", [])
        # Se não tiver, calcula direto do dff
        if not dados_preco and "DESC_PRODUTO" in dff.columns:
            prod_grp = dff.sort_values("DATA_MOVTO", ascending=False).groupby(["COD_PRODUTO","DESC_PRODUTO"] if "COD_PRODUTO" in dff.columns else ["DESC_PRODUTO"]).agg(
                ultima_data=("DATA_MOVTO","first"),
                ultima_nota=("NUM_DOCTO","first") if "NUM_DOCTO" in dff.columns else ("VALOR_LIQUIDO","count"),
                kg_total=("QTDE_PRI","sum"),
                fat_total=("VALOR_LIQUIDO","sum"),
                ultimo_vl_unit=("VALOR_UNITARIO","first") if "VALOR_UNITARIO" in dff.columns else ("VALOR_LIQUIDO","first"),
                n_compras=("NUM_DOCTO","nunique") if "NUM_DOCTO" in dff.columns else ("VALOR_LIQUIDO","count"),
            ).reset_index().sort_values("ultima_data", ascending=False)
            dados_preco = []
            for _, r in prod_grp.iterrows():
                kg_t = float(r["kg_total"]); fat_t = float(r["fat_total"])
                pm = round(fat_t/kg_t,2) if kg_t>0 else 0
                cod = str(r["COD_PRODUTO"]) if "COD_PRODUTO" in r.index else ""
                dados_preco.append({
                    "cod": cod, "produto": str(r["DESC_PRODUTO"]),
                    "ultima_data": r["ultima_data"].strftime("%d/%m/%Y") if hasattr(r["ultima_data"],"strftime") else str(r["ultima_data"]),
                    "ultima_nota": str(r["ultima_nota"]),
                    "ultimo_vl_unit": round(float(r["ultimo_vl_unit"]),2),
                    "pm_historico": pm, "kg_total": round(kg_t,2), "n_compras": int(r["n_compras"])
                })
        logging.warning(f"[PDF-PRECO] dados_preco len={len(dados_preco)} dff_interno_len={len(dff)} dff_cols={list(dff.columns)[:8]}")
        if dados_preco:
            linhas_p = ""
            for r in dados_preco:
                linhas_p += f"""<tr>
                    <td>{_h.escape(str(r.get('cod',''))[:12])}</td>
                    <td class="nome">{_h.escape(str(r.get('produto',''))[:45])}</td>
                    <td style="white-space:nowrap">{_h.escape(str(r.get('ultima_data','')))}</td>
                    <td>{_h.escape(str(r.get('ultima_nota','')))}</td>
                    <td class="valor">{fmt_brl(r.get('ultimo_vl_unit',0))}</td>
                    <td class="valor">{fmt_brl(r.get('pm_historico',0))}</td>
                    <td class="valor">{r.get('kg_total',0):,.0f}</td>
                    <td class="valor">{r.get('n_compras',0)}</td>
                </tr>"""
            corpo_html = f"""<table style="table-layout:fixed;width:100%;font-size:7.5px;">
              <colgroup>
                <col style="width:10%"><col style="width:35%"><col style="width:9%">
                <col style="width:8%"><col style="width:10%"><col style="width:10%">
                <col style="width:10%"><col style="width:8%">
              </colgroup>
              <thead><tr>
                <th>COD</th><th>PRODUTO</th><th>ÚLTIMA COMPRA</th><th>NF</th>
                <th style="text-align:right">VL UNIT</th><th style="text-align:right">R$/kg MÉDIO</th>
                <th style="text-align:right">TOTAL KG</th><th style="text-align:right">Nº COMPRAS</th>
              </tr></thead>
              <tbody>{linhas_p}</tbody>
            </table>"""

    # ── RANKING CLIENTES ──
    elif tipo_rel == "ranking_clientes" and "NOME_CLIENTE" in dff.columns:
        cli_grp = dff.groupby("NOME_CLIENTE").agg(kg=("QTDE_PRI","sum"),fat=("VALOR_LIQUIDO","sum"),notas=("NUM_DOCTO","nunique")).sort_values("kg",ascending=False).head(50)
        linhas_c = ""
        for i,(ci,cr) in enumerate(cli_grp.iterrows(),1):
            kc=float(cr["kg"]); fc=float(cr["fat"]); pmc=round(fc/kc,2) if kc>0 else 0
            linhas_c += f"<tr><td>{i}</td><td class='nome'>{_h.escape(str(ci)[:50])}</td><td class='valor'>{kc:,.2f}</td><td class='valor'>{int(round(kc/30,0)):,}</td><td class='valor'>{fmt_brl(fc)}</td><td class='valor'>{fmt_brl(pmc)}</td><td class='valor'>{int(cr['notas'])}</td></tr>"
        corpo_html = f"""<table>
          <thead><tr><th>#</th><th>CLIENTE</th><th style="text-align:right">KG</th><th style="text-align:right">CX30</th><th style="text-align:right">FATURAMENTO</th><th style="text-align:right">R$/KG</th><th style="text-align:right">NOTAS</th></tr></thead>
          <tbody>{linhas_c}<tr class="total-row"><td colspan="2"><strong>TOTAIS</strong></td><td class="valor">{kg:,.2f}</td><td class="valor">{cx:,}</td><td class="valor">{fmt_brl(fat)}</td><td class="valor">{fmt_brl(pm)}</td><td class="valor">{notas}</td></tr></tbody>
        </table>"""

    # ── RANKING VENDEDORES ──
    elif tipo_rel == "ranking_vendedores" and "NOM_VENDEDOR" in dff.columns:
        vend_grp = dff.groupby("NOM_VENDEDOR").agg(kg=("QTDE_PRI","sum"),fat=("VALOR_LIQUIDO","sum"),notas=("NUM_DOCTO","nunique")).sort_values("kg",ascending=False)
        linhas_v = ""
        for i,(vi,vr) in enumerate(vend_grp.iterrows(),1):
            kv=float(vr["kg"]); fv=float(vr["fat"]); pmv=round(fv/kv,2) if kv>0 else 0
            linhas_v += f"<tr><td>{i}</td><td class='nome'>{_h.escape(str(vi)[:40])}</td><td class='valor'>{kv:,.2f}</td><td class='valor'>{int(round(kv/30,0)):,}</td><td class='valor'>{fmt_brl(fv)}</td><td class='valor'>{fmt_brl(pmv)}</td><td class='valor'>{int(vr['notas'])}</td></tr>"
        corpo_html = f"""<table>
          <thead><tr><th>#</th><th>VENDEDOR</th><th style="text-align:right">KG</th><th style="text-align:right">CX30</th><th style="text-align:right">FATURAMENTO</th><th style="text-align:right">R$/KG</th><th style="text-align:right">NOTAS</th></tr></thead>
          <tbody>{linhas_v}<tr class="total-row"><td colspan="2"><strong>TOTAIS</strong></td><td class="valor">{kg:,.2f}</td><td class="valor">{cx:,}</td><td class="valor">{fmt_brl(fat)}</td><td class="valor">{fmt_brl(pm)}</td><td class="valor">{notas}</td></tr></tbody>
        </table>"""

    # ── PADRÃO: relatório de faturamento por tipo de movimento ──
    else:
        if "DESC_TIPO_MV" in dff.columns:
            tipos = sorted(dff["DESC_TIPO_MV"].fillna("SEM TIPO").unique())
            for tipo in tipos:
                df_tipo = dff[dff["DESC_TIPO_MV"].fillna("SEM TIPO") == tipo]
                if len(df_tipo) == 0: continue
                fat_tipo = df_tipo["VALOR_LIQUIDO"].sum()
                n_notas  = df_tipo["NUM_DOCTO"].nunique() if "NUM_DOCTO" in df_tipo.columns else 0
                grp_cols = [col for col in ["DATA_MOVTO","NOME_FILIAL","NUM_DOCTO","NOME_CLIENTE","CIDADE","UF","NOM_VENDEDOR"] if col in df_tipo.columns]
                df_notas = df_tipo.groupby(grp_cols).agg(valor=("VALOR_LIQUIDO","sum")).reset_index().sort_values("DATA_MOVTO", ascending=False)
                linhas = ""
                total_valor = 0
                for _, row in df_notas.iterrows():
                    data_str = row["DATA_MOVTO"].strftime("%d/%m/%Y") if hasattr(row.get("DATA_MOVTO",None),"strftime") else ""
                    valor = float(row.get("valor",0))
                    total_valor += valor
                    linhas += f"""<tr>
                        <td>{_h.escape(data_str)}</td>
                        <td>{_h.escape(str(row.get("NOME_FILIAL",""))[:8])}</td>
                        <td>{_h.escape(str(row.get("NUM_DOCTO","")))}</td>
                        <td class="nome">{_h.escape(str(row.get("NOME_CLIENTE",""))[:45])}</td>
                        <td>{_h.escape(str(row.get("CIDADE",""))[:18])}</td>
                        <td>{_h.escape(str(row.get("UF","")))}</td>
                        <td class="valor">{fmt_brl(valor)}</td>
                        <td>{_h.escape(str(row.get("NOM_VENDEDOR",""))[:22])}</td>
                    </tr>"""
                corpo_html += f"""
                <div class="grupo-header">
                  <span class="grupo-tipo">{_h.escape(str(tipo))}</span>
                  <span class="grupo-stats">{n_notas} notas &middot; {fmt_brl(fat_tipo)}</span>
                </div>
                <table>
                  <thead><tr><th>DATA</th><th>FILIAL</th><th>NF</th><th>CLIENTE</th><th>CIDADE</th><th>UF</th><th>VALOR</th><th>VENDEDOR</th></tr></thead>
                  <tbody>{linhas}
                    <tr class="total-row"><td colspan="6"><strong>TOTAL</strong></td><td class="valor">{fmt_brl(total_valor)}</td><td></td></tr>
                  </tbody>
                </table>"""
        else:
            grp_cols = [col for col in ["DATA_MOVTO","NOME_FILIAL","NUM_DOCTO","NOME_CLIENTE","CIDADE","UF","NOM_VENDEDOR"] if col in dff.columns]
            df_notas = dff.groupby(grp_cols).agg(valor=("VALOR_LIQUIDO","sum")).reset_index().sort_values("DATA_MOVTO", ascending=False)
            linhas = ""; total_valor = 0
            for _, row in df_notas.iterrows():
                data_str = row["DATA_MOVTO"].strftime("%d/%m/%Y") if hasattr(row.get("DATA_MOVTO",None),"strftime") else ""
                valor = float(row.get("valor",0)); total_valor += valor
                linhas += f"""<tr><td>{_h.escape(data_str)}</td><td>{_h.escape(str(row.get("NOME_FILIAL",""))[:8])}</td><td>{_h.escape(str(row.get("NUM_DOCTO","")))}</td><td class="nome">{_h.escape(str(row.get("NOME_CLIENTE",""))[:45])}</td><td>{_h.escape(str(row.get("CIDADE",""))[:18])}</td><td>{_h.escape(str(row.get("UF","")))}</td><td class="valor">{fmt_brl(valor)}</td><td>{_h.escape(str(row.get("NOM_VENDEDOR",""))[:22])}</td></tr>"""
            corpo_html = f"""<table><thead><tr><th>DATA</th><th>FILIAL</th><th>NF</th><th>CLIENTE</th><th>CIDADE</th><th>UF</th><th>VALOR</th><th>VENDEDOR</th></tr></thead><tbody>{linhas}<tr class="total-row"><td colspan="6"><strong>TOTAL</strong></td><td class="valor">{fmt_brl(total_valor)}</td><td></td></tr></tbody></table>"""

    # ── Resumo sintético por tipo de movimento ──
    if "DESC_TIPO_MV" in dff.columns:
        tmv_grp = dff.groupby("DESC_TIPO_MV").agg(fat=("VALOR_LIQUIDO","sum"), notas=("NUM_DOCTO","nunique")).sort_values("fat", ascending=False)
        for tmv_idx, tmv_row in tmv_grp.iterrows():
            pct = round(float(tmv_row["fat"]) / fat * 100, 1) if fat > 0 else 0
            resumo_tmv_html += f"""<tr>
                <td>{_h.escape(str(tmv_idx))}</td>
                <td class="valor">{int(tmv_row["notas"]):,}</td>
                <td class="valor">{fmt_brl(tmv_row["fat"])}</td>
                <td class="valor">{pct:.1f}%</td>
            </tr>"""

    html_content = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"/>
<style>
@page {{ margin: 1.5cm 1.2cm; size: A4 landscape; }}
body {{ font-family: Arial, sans-serif; font-size: 9px; color: #222; margin: 0; }}
.header {{ background: #C8102E; color: #fff; padding: 12px 16px; display: flex; justify-content: space-between; align-items: center; margin-bottom: 14px; }}
.header-left h1 {{ margin: 0; font-size: 16px; letter-spacing: 2px; }}
.header-left p {{ margin: 3px 0 0; font-size: 9px; opacity: .85; }}
.header-right {{ text-align: right; font-size: 9px; line-height: 1.6; }}
.kpis {{ display: flex; gap: 10px; margin-bottom: 16px; }}
.kpi {{ flex: 1; border: 1px solid #ddd; border-top: 3px solid #C8102E; border-radius: 5px; padding: 8px 12px; background: #fafafa; }}
.kpi-label {{ font-size: 8px; color: #999; text-transform: uppercase; letter-spacing: .8px; }}
.kpi-value {{ font-size: 14px; font-weight: bold; color: #111; margin-top: 3px; }}
.grupo-header {{ background: #1A1A1A; color: #fff; padding: 6px 12px; display: flex; justify-content: space-between; border-radius: 4px 4px 0 0; margin-top: 10px; }}
.grupo-tipo {{ font-size: 10px; font-weight: bold; letter-spacing: .5px; }}
.grupo-stats {{ font-size: 9px; color: #F5C800; }}
.resumo-header {{ background: #C8102E; color: #fff; padding: 6px 12px; display: flex; justify-content: space-between; border-radius: 4px 4px 0 0; margin-top: 20px; page-break-before: auto; }}
table {{ width: 100%; border-collapse: collapse; margin-bottom: 0; }}
thead tr {{ background: #f0f0f0; }}
th {{ padding: 5px 6px; text-align: left; font-size: 8px; color: #666; text-transform: uppercase; border-bottom: 2px solid #ddd; white-space: nowrap; }}
td {{ padding: 3px 5px; border-bottom: 1px solid #eee; font-size: 7.5px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }}
tr:nth-child(even) td {{ background: #f9f9f9; }}
.valor {{ text-align: right; font-weight: bold; }}
.nome {{ max-width: 200px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }}
td {{ white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
.total-row td {{ background: #FFF8DC !important; font-weight: bold; border-top: 2px solid #ddd; }}
.footer {{ margin-top: 16px; border-top: 1px solid #ddd; padding-top: 6px; display: flex; justify-content: space-between; font-size: 8px; color: #aaa; }}
</style>
</head>
<body>
<div class="header">
  <div class="header-left">
    <h1>IAF &middot; {_h.escape(titulo_rel)}</h1>
    <p>Frinense Alimentos &middot; {_h.escape(subtitulo)}</p>
  </div>
  <div class="header-right">
    <div><strong>Período:</strong> {periodo_label}</div>
    <div><strong>Gerado em:</strong> {hoje_str}</div>
  </div>
</div>
<div class="kpis">
  <div class="kpi"><div class="kpi-label">Faturamento</div><div class="kpi-value">{fmt_brl(fat)}</div></div>
  <div class="kpi"><div class="kpi-label">Volume</div><div class="kpi-value">{fmt_kg(kg)}</div></div>
  <div class="kpi"><div class="kpi-label">CX30</div><div class="kpi-value">{cx:,}</div></div>
  <div class="kpi"><div class="kpi-label">Notas</div><div class="kpi-value">{notas:,}</div></div>
  <div class="kpi"><div class="kpi-label">R$/kg</div><div class="kpi-value">{fmt_brl(pm)}</div></div>
</div>
{corpo_html}
<div class="resumo-header">
  <span class="grupo-tipo">📊 RESUMO POR TIPO DE MOVIMENTO</span>
  <span class="grupo-stats">Total: {fmt_brl(fat)}</span>
</div>
<table>
  <thead><tr><th>TIPO DE MOVIMENTO</th><th style="text-align:right">NOTAS</th><th style="text-align:right">FATURAMENTO</th><th style="text-align:right">% DO TOTAL</th></tr></thead>
  <tbody>
    {resumo_tmv_html if resumo_tmv_html else f'<tr><td colspan="4">Sem agrupamento por tipo disponível</td></tr>'}
    <tr class="total-row"><td><strong>TOTAL GERAL</strong></td><td class="valor">{notas:,}</td><td class="valor">{fmt_brl(fat)}</td><td class="valor">100,0%</td></tr>
  </tbody>
</table>
<div class="footer">
  <span>IAF &middot; Analista Comercial &middot; Frinense Alimentos</span>
  <span>Gerado automaticamente em {hoje_str}</span>
</div>
</body></html>"""

    if WEASYPRINT_OK:
        return WeasyprintHTML(string=html_content).write_pdf()

    # Fallback: ReportLab
    logging.warning("[PDF] WeasyPrint indisponível, usando ReportLab")
    VERM = colors.HexColor("#C8102E"); AMAR = colors.HexColor("#F5C800")
    PRET = colors.HexColor("#1A1A1A"); CINZ = colors.HexColor("#F5F5F5"); CINZ2 = colors.HexColor("#DDDDDD")
    buf2 = io.BytesIO()
    doc = SimpleDocTemplate(buf2, pagesize=landscape(A4), leftMargin=1.2*cm, rightMargin=1.2*cm, topMargin=1.2*cm, bottomMargin=1.2*cm)
    s_t  = ParagraphStyle("t",  fontName="Helvetica-Bold", fontSize=12, textColor=colors.white)
    s_s  = ParagraphStyle("s",  fontName="Helvetica",      fontSize=8,  textColor=colors.white, alignment=TA_RIGHT)
    s_g  = ParagraphStyle("g",  fontName="Helvetica-Bold", fontSize=9,  textColor=colors.white)
    s_gs = ParagraphStyle("gs", fontName="Helvetica",      fontSize=8,  textColor=AMAR, alignment=TA_RIGHT)
    s_kl = ParagraphStyle("kl", fontName="Helvetica",      fontSize=7,  textColor=colors.grey, alignment=TA_CENTER)
    s_kv = ParagraphStyle("kv", fontName="Helvetica-Bold", fontSize=11, textColor=PRET, alignment=TA_CENTER)
    s_r  = ParagraphStyle("r",  fontName="Helvetica",      fontSize=7,  textColor=colors.grey)
    story2 = []
    ht = Table([[Paragraph(f"IAF · {titulo_rel}", s_t), Paragraph(f"Período: {periodo_label}<br/>{subtitulo}<br/>Gerado em {hoje_str}", s_s)]], colWidths=["60%","40%"])
    ht.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),VERM),("VALIGN",(0,0),(-1,-1),"MIDDLE"),("TOPPADDING",(0,0),(-1,-1),8),("BOTTOMPADDING",(0,0),(-1,-1),8),("LEFTPADDING",(0,0),(-1,-1),10)]))
    story2.append(ht); story2.append(Spacer(1,0.3*cm))
    kpi = Table([[Paragraph("FATURAMENTO",s_kl),Paragraph("VOLUME",s_kl),Paragraph("CX30",s_kl),Paragraph("NOTAS",s_kl),Paragraph("R$/KG",s_kl)],[Paragraph(fmt_brl(fat),s_kv),Paragraph(fmt_kg(kg),s_kv),Paragraph(f"{cx:,}",s_kv),Paragraph(f"{notas:,}",s_kv),Paragraph(fmt_brl(pm),s_kv)]], colWidths=["20%"]*5)
    kpi.setStyle(TableStyle([("BOX",(0,0),(-1,-1),0.5,CINZ2),("INNERGRID",(0,0),(-1,-1),0.5,CINZ2),("BACKGROUND",(0,0),(-1,-1),CINZ),("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),("LINEABOVE",(0,0),(-1,0),2,VERM)]))
    story2.append(kpi); story2.append(Spacer(1,0.4*cm))

    # ── ULTIMOS PREÇOS — ReportLab (sem KPI de vendas) ──
    if tipo_rel == "ultimos_precos":
        # Remover o KPI de vendas que não faz sentido para preços
        story2 = story2[:-2]  # remove kpi e spacer
        dados_preco = resultado.get("dados", {}).get("ultimos_precos", [])
        s_th = ParagraphStyle("th", fontName="Helvetica-Bold", fontSize=7, textColor=colors.HexColor("#666666"))
        s_td = ParagraphStyle("td", fontName="Helvetica", fontSize=7.5, textColor=PRET)
        s_td_r = ParagraphStyle("tdr", fontName="Helvetica", fontSize=7.5, textColor=PRET, alignment=TA_RIGHT)
        cw_p = [2*cm, 8*cm, 2.5*cm, 2.2*cm, 2.8*cm, 2.8*cm, 2.5*cm, 2*cm]
        td_p = [["COD", "PRODUTO", "ÚLT. COMPRA", "NF", "VL UNIT", "R$/kg MÉD.", "TOTAL KG", "Nº COMP."]]
        for r in dados_preco:
            td_p.append([
                str(r.get("cod",""))[:12],
                str(r.get("produto",""))[:50],
                str(r.get("ultima_data","")),
                str(r.get("ultima_nota","")),
                fmt_brl(r.get("ultimo_vl_unit",0)),
                fmt_brl(r.get("pm_historico",0)),
                f"{r.get('kg_total',0):,.0f}",
                str(r.get("n_compras",0)),
            ])
        t_p = Table(td_p, colWidths=cw_p, repeatRows=1)
        t_p.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,0),colors.HexColor("#F0F0F0")),
            ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
            ("FONTSIZE",(0,0),(-1,-1),7.5),
            ("FONTNAME",(0,1),(-1,-1),"Helvetica"),
            ("GRID",(0,0),(-1,-1),0.3,CINZ2),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white,CINZ]),
            ("ALIGN",(4,0),(-1,-1),"RIGHT"),
            ("TOPPADDING",(0,0),(-1,-1),2),
            ("BOTTOMPADDING",(0,0),(-1,-1),2),
            ("LINEABOVE",(0,0),(-1,0),2,VERM),
        ]))
        story2.append(t_p)
        story2.append(Spacer(1,0.2*cm))
        story2.append(HRFlowable(width="100%",thickness=0.5,color=CINZ2))
        story2.append(Paragraph(f"IAF · Analista Comercial Frinense Alimentos · Gerado em {hoje_str}", s_r))
        doc.build(story2); buf2.seek(0)
        return buf2.read()

    cw = [1.8*cm,1.5*cm,1.8*cm,7*cm,3*cm,1*cm,3*cm,4*cm]
    tipos2 = sorted(dff["DESC_TIPO_MV"].fillna("SEM TIPO").unique()) if "DESC_TIPO_MV" in dff.columns else ["SEM TIPO"]
    for tipo2 in tipos2:
        df_t2 = dff[dff["DESC_TIPO_MV"].fillna("SEM TIPO") == tipo2] if "DESC_TIPO_MV" in dff.columns else dff
        if len(df_t2) == 0: continue
        fat_t2 = df_t2["VALOR_LIQUIDO"].sum(); n_t2 = df_t2["NUM_DOCTO"].nunique() if "NUM_DOCTO" in df_t2.columns else 0
        gt = Table([[Paragraph(str(tipo2),s_g),Paragraph(f"{n_t2} notas · {fmt_brl(fat_t2)}",s_gs)]],colWidths=["60%","40%"])
        gt.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),PRET),("TOPPADDING",(0,0),(-1,-1),4),("BOTTOMPADDING",(0,0),(-1,-1),4),("LEFTPADDING",(0,0),(0,0),8)]))
        story2.append(gt)
        gc2 = [col for col in ["DATA_MOVTO","NOME_FILIAL","NUM_DOCTO","NOME_CLIENTE","CIDADE","UF","NOM_VENDEDOR"] if col in df_t2.columns]
        dn2 = df_t2.groupby(gc2).agg(valor=("VALOR_LIQUIDO","sum")).reset_index().sort_values("DATA_MOVTO",ascending=False)
        td2 = [["DATA","FILIAL","NF","CLIENTE","CIDADE","UF","VALOR","VENDEDOR"]]; tot2 = 0
        for _, row in dn2.iterrows():
            ds = row["DATA_MOVTO"].strftime("%d/%m/%Y") if hasattr(row.get("DATA_MOVTO",None),"strftime") else ""
            v2 = float(row.get("valor",0)); tot2 += v2
            td2.append([ds,str(row.get("NOME_FILIAL",""))[:8],str(row.get("NUM_DOCTO","")),str(row.get("NOME_CLIENTE",""))[:45],str(row.get("CIDADE",""))[:18],str(row.get("UF","")),fmt_brl(v2),str(row.get("NOM_VENDEDOR",""))[:22]])
        td2.append(["TOTAL","","","","","",fmt_brl(tot2),""])
        t2 = Table(td2,colWidths=cw,repeatRows=1)
        t2.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),CINZ),("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),7.5),("FONTNAME",(0,1),(-1,-2),"Helvetica"),("BACKGROUND",(0,-1),(-1,-1),colors.HexColor("#FFF8DC")),("FONTNAME",(0,-1),(-1,-1),"Helvetica-Bold"),("GRID",(0,0),(-1,-1),0.3,CINZ2),("ROWBACKGROUNDS",(0,1),(-1,-2),[colors.white,CINZ]),("ALIGN",(6,0),(6,-1),"RIGHT"),("TOPPADDING",(0,0),(-1,-1),2),("BOTTOMPADDING",(0,0),(-1,-1),2)]))
        story2.append(t2); story2.append(Spacer(1,0.3*cm))
    # Resumo sintético ReportLab
    if "DESC_TIPO_MV" in dff.columns:
        gt_res = Table([[Paragraph("RESUMO POR TIPO DE MOVIMENTO",s_g),Paragraph(f"Total: {fmt_brl(fat)}",s_gs)]],colWidths=["60%","40%"])
        gt_res.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),VERM),("TOPPADDING",(0,0),(-1,-1),4),("BOTTOMPADDING",(0,0),(-1,-1),4),("LEFTPADDING",(0,0),(0,0),8)]))
        story2.append(gt_res)
        tmv_grp2 = dff.groupby("DESC_TIPO_MV").agg(fat=("VALOR_LIQUIDO","sum"),notas=("NUM_DOCTO","nunique")).sort_values("fat",ascending=False)
        td_res = [["TIPO DE MOVIMENTO","NOTAS","FATURAMENTO","% TOTAL"]]
        for ti, tr in tmv_grp2.iterrows():
            pct2 = round(float(tr["fat"])/fat*100,1) if fat>0 else 0
            td_res.append([str(ti),f"{int(tr['notas']):,}",fmt_brl(tr["fat"]),f"{pct2:.1f}%"])
        td_res.append(["TOTAL GERAL",f"{notas:,}",fmt_brl(fat),"100,0%"])
        t_res = Table(td_res,colWidths=["55%","10%","20%","15%"],repeatRows=1)
        t_res.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),CINZ),("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),8),("FONTNAME",(0,1),(-1,-2),"Helvetica"),("BACKGROUND",(0,-1),(-1,-1),colors.HexColor("#FFF8DC")),("FONTNAME",(0,-1),(-1,-1),"Helvetica-Bold"),("GRID",(0,0),(-1,-1),0.3,CINZ2),("ALIGN",(1,0),(-1,-1),"RIGHT"),("TOPPADDING",(0,0),(-1,-1),3),("BOTTOMPADDING",(0,0),(-1,-1),3)]))
        story2.append(t_res)
    story2.append(Spacer(1,0.2*cm))
    story2.append(HRFlowable(width="100%",thickness=0.5,color=CINZ2))
    story2.append(Paragraph(f"IAF · Analista Comercial Frinense Alimentos · Gerado em {hoje_str}", s_r))
    doc.build(story2); buf2.seek(0)
    return buf2.read()



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

    # Se o assistente acabou de pedir o cliente e o tipo voltou errado, corrigir pelo histórico
    ultimas_msgs = historico[-4:] if len(historico) >= 4 else historico
    # Se assistente ofereceu PDF da nota e usuário respondeu "sim"
    assist_ofereceu_danfe = any(
        m.get("role") == "assistant" and "DANFE" in str(m.get("content","")) and "Deseja" in str(m.get("content",""))
        for m in ultimas_msgs
    )
    ultima_lower = ultima.lower().strip()
    if assist_ofereceu_danfe and ultima_lower in ("sim", "sim.", "yes", "pode", "pode sim", "quero", "quero sim", "gera", "gera sim"):
        # Buscar nr_nota e chave_acesso da última resposta do assistente
        for msg in reversed(historico[:-1]):
            if msg.get("role") == "assistant":
                txt = msg.get("content","")
                import re as _re
                m_nota = _re.search(r'Nota\s+(\d+)', txt)
                m_chave = _re.search(r'DANFE:(\d{44})', txt)
                if m_nota:
                    filtro["tipo"] = "detalhe_nota"
                    filtro["nr_nota"] = m_nota.group(1)
                if m_chave:
                    filtro["chave_acesso_override"] = m_chave.group(1)
                break

    assist_pediu_cliente = any(
        m.get("role") == "assistant" and "Para qual cliente" in str(m.get("content",""))
        for m in ultimas_msgs
    )
    if assist_pediu_cliente and filtro.get("tipo") in ("indefinido", "ultimas_vendas", None):
        # Buscar tipo na penúltima mensagem do usuário
        msgs_usuario = [m for m in historico if m.get("role") == "user"]
        if len(msgs_usuario) >= 2:
            penultima = msgs_usuario[-2].get("content", "")
            if any(p in penultima.lower() for p in ["preço", "preco", "tabela de preço", "quanto paga"]):
                filtro["tipo"] = "ultimos_precos"
                logging.info("[FILTRO] tipo corrigido para ultimos_precos via histórico")

    # Se precisa de cliente e não foi informado
    if filtro.get("precisa_cliente") and not filtro.get("cliente") and not filtro.get("cnpj_raiz"):
        return JSONResponse({"content": [{"type": "text", "text":
            "Para qual cliente? Pode informar o nome ou CNPJ raiz (8 dígitos)."}]})

    # ultimos_precos sem período → assume últimos 90 dias automaticamente
    if filtro.get("tipo") == "ultimos_precos" and not filtro.get("data_inicio"):
        from datetime import date, timedelta
        hoje = date.today()
        filtro["data_inicio"] = (hoje - timedelta(days=90)).strftime("%Y-%m-%d")
        filtro["data_fim"]    = hoje.strftime("%Y-%m-%d")
        filtro["precisa_periodo"] = False

    # Se período indefinido para resumo
    if filtro.get("precisa_periodo") and not filtro.get("data_inicio"):
        return JSONResponse({"content": [{"type": "text", "text":
            "Qual período você quer analisar? Ex: março 2026, esta semana, últimos 30 dias..."}]})

    # Nota: pede número se não informado
    if filtro.get("tipo") == "detalhe_nota" and not filtro.get("nr_nota"):
        return JSONResponse({"content": [{"type": "text", "text":
            "📄 Claro! Qual o **número da nota fiscal** que deseja consultar?\n\nSe preferir, pode informar também o nome do cliente para eu localizar mais rápido."}]})

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

    # ETAPA 2.5 — Se formato PDF, SEMPRE detectar tipo pela última resposta do assistente
    if filtro.get("formato") == "pdf":
        ultima_resp_assist = ""
        for msg in reversed(historico[:-1]):
            if msg.get("role") == "assistant":
                ultima_resp_assist = str(msg.get("content", "")).lower()
                break

        # LOG: ver exatamente o que chega como última resposta do assistente
        logging.info(f"[PDF-DEBUG] ultima_resp_assist (primeiros 500 chars): {ultima_resp_assist[:500]!r}")
        logging.info(f"[PDF-DEBUG] filtro antes da detecção: tipo={filtro.get('tipo')} cliente={filtro.get('cliente')}")
        logging.info(f"[PDF-DEBUG] total msgs no histórico: {len(historico)}")
        for i, msg in enumerate(historico[-6:]):
            logging.info(f"[PDF-DEBUG] hist[-{6-i}] role={msg.get('role')} content={str(msg.get('content',''))[:100]!r}")

        # Detectar tipo pelo que estava sendo exibido na tela
        tipo_detectado = None
        if any(p in ultima_resp_assist for p in ["últimos preços", "ultimos precos", "r$/kg médio", "última compra", "nº compras"]):
            tipo_detectado = "ultimos_precos"
        elif any(p in ultima_resp_assist for p in ["últimas vendas", "ultimas vendas"]):
            tipo_detectado = "ultimas_vendas"
        elif "ranking de clientes" in ultima_resp_assist or "ranking clientes" in ultima_resp_assist:
            tipo_detectado = "ranking_clientes"
        elif "ranking de vendedores" in ultima_resp_assist or "ranking vendedores" in ultima_resp_assist:
            tipo_detectado = "ranking_vendedores"
        elif "ranking de produtos" in ultima_resp_assist or "ranking produtos" in ultima_resp_assist:
            tipo_detectado = "ranking_produtos"

        if tipo_detectado:
            filtro["tipo"] = tipo_detectado
            # Herdar cliente se não veio
            if not filtro.get("cliente") and not filtro.get("cnpj_raiz"):
                palavras_cmd = ["pdf","preço","preco","venda","ranking","último","ultim","relatorio","relatório","quanto","tabela","histórico","historico","notas","manda","gera","exporta"]
                for msg in reversed(historico[:-1]):
                    if msg.get("role") == "user":
                        txt = msg.get("content", "").strip()
                        if txt and len(txt) < 60 and not any(p in txt.lower() for p in palavras_cmd):
                            filtro["cliente"] = txt
                            break
            # Se ultimos_precos, garantir 90 dias (o bloco anterior não roda de novo)
            if tipo_detectado == "ultimos_precos":
                hoje = date.today()
                filtro["data_inicio"] = (hoje - timedelta(days=90)).strftime("%Y-%m-%d")
                filtro["data_fim"]    = hoje.strftime("%Y-%m-%d")
            logging.warning(f"[PDF] tipo detectado={tipo_detectado} cliente={filtro.get('cliente')} data_inicio={filtro.get('data_inicio')}")
            resultado = calcular(df, filtro)

    if filtro.get("formato") == "pdf":
        try:
            pdf_bytes = gerar_relatorio_pdf(df, filtro, resultado)
            # Nome amigável: IAF_Abril2026_CAMARA.pdf
            try:
                d1_dt = datetime.strptime(filtro.get("data_inicio",""), "%Y-%m-%d")
                meses_pt = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]
                mes_label = meses_pt[d1_dt.month-1] + str(d1_dt.year)
            except:
                mes_label = (filtro.get("data_inicio") or datetime.now().strftime("%Y-%m")).replace("-","")
            cli_label = ""
            if filtro.get("cliente"):
                cli_label = "_" + re.sub(r'[^A-Za-z0-9]','',filtro["cliente"])[:15].upper()
            fil_label = ("_" + filtro["filial"]) if filtro.get("filial") else ""
            filename = f"IAF_{mes_label}{fil_label}{cli_label}.pdf"
            logging.info(f"[PDF] Gerando: {filename}")
            import base64 as _b64
            pdf_b64 = _b64.b64encode(pdf_bytes).decode()
            return JSONResponse({
                "id": "iaf-pdf",
                "type": "message",
                "role": "assistant",
                "content": [{"type": "text", "text":
                    f"📄 Relatório gerado!\nRELATORIO_PDF_BASE64:{pdf_b64}:FILENAME:{filename}"}],
                "model": "iaf-v2",
                "stop_reason": "end_turn"
            })
        except Exception as e:
            logging.error(f"[PDF] erro: {e}")
            return JSONResponse({"content": [{"type": "text", "text":
                f"❌ Erro ao gerar PDF: {str(e)}. Tente sem o PDF por enquanto."}]})

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

@app.post("/tts")
async def tts(req: starlette.requests.Request):
    """Converte texto em áudio via ElevenLabs."""
    if not ELEVENLABS_KEY:
        raise HTTPException(status_code=503, detail="ElevenLabs não configurado.")
    body = await req.json()
    texto = body.get("texto", "").strip()
    voice_id = body.get("voice_id", "4za2kOXGgUd57HRSQ1fn")
    if not texto:
        raise HTTPException(status_code=400, detail="Texto vazio.")
    # Limitar texto (ElevenLabs cobra por char)
    texto = texto[:1500]
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}",
            headers={
                "xi-api-key": ELEVENLABS_KEY,
                "Content-Type": "application/json",
                "Accept": "audio/mpeg"
            },
            json={
                "text": texto,
                "model_id": "eleven_multilingual_v2",
                "voice_settings": {"stability": 0.5, "similarity_boost": 0.75}
            }
        )
    if r.status_code != 200:
        logging.warning(f"[TTS] ElevenLabs status={r.status_code} body={r.text[:300]!r} key_prefix={ELEVENLABS_KEY[:8] if ELEVENLABS_KEY else 'VAZIA'}")
        raise HTTPException(status_code=r.status_code, detail=f"ElevenLabs erro: {r.text[:200]}")
    import base64 as _b64
    audio_b64 = _b64.b64encode(r.content).decode()
    return JSONResponse({"audio_b64": audio_b64})


@app.get("/dash-data")
def dash_data(ano: int = None, mes: int = None, filial: str = None):
    """Endpoint do novo dashboard ampliado — retorna dados filtrados por ano/mês/filial."""
    try:
        df = load_df()
        dff = df.copy()

        # Filtros
        if ano:
            dff = dff[dff['DATA_MOVTO'].dt.year == ano]
        if mes:
            dff = dff[dff['DATA_MOVTO'].dt.month == mes]
        if filial and filial != "TODAS":
            dff = dff[dff['NOME_FILIAL'].str.upper() == filial.upper()]

        if len(dff) == 0:
            return JSONResponse({"erro": "Sem dados para o filtro selecionado."})

        fat   = round(float(dff['VALOR_LIQUIDO'].sum()), 2)
        kg    = round(float(dff['QTDE_PRI'].sum()), 2)
        cx30  = int(round(kg / 30, 0))
        notas = int(dff['NUM_DOCTO'].nunique()) if 'NUM_DOCTO' in dff.columns else 0
        pm    = round(fat / kg, 2) if kg > 0 else 0

        # Anos e meses disponíveis
        anos_disp  = sorted(df['DATA_MOVTO'].dt.year.dropna().unique().tolist(), reverse=True)
        filiais_disp = sorted(df['NOME_FILIAL'].dropna().unique().tolist()) if 'NOME_FILIAL' in df.columns else []

        # Evolução mensal (últimos 12 meses do filtro)
        dff2 = dff.copy()
        dff2['mes_ano'] = dff2['DATA_MOVTO'].dt.to_period('M')
        evol = (dff2.groupby('mes_ano').agg(fat=('VALOR_LIQUIDO','sum'), kg=('QTDE_PRI','sum'))
                .sort_index().tail(13))
        evolucao = [{"mes": str(idx), "fat": round(float(r.fat),2), "kg": round(float(r.kg),2)}
                    for idx, r in evol.iterrows()]

        # Evolução diária (mês atual do filtro ou último mês)
        mes_ref = mes or dff['DATA_MOVTO'].dt.month.max()
        ano_ref = ano or dff['DATA_MOVTO'].dt.year.max()
        df_dia = dff[(dff['DATA_MOVTO'].dt.month == mes_ref) & (dff['DATA_MOVTO'].dt.year == ano_ref)]
        evol_dia = (df_dia.groupby(df_dia['DATA_MOVTO'].dt.day)
                    .agg(fat=('VALOR_LIQUIDO','sum'), kg=('QTDE_PRI','sum'))
                    .sort_index())
        por_dia = [{"dia": int(idx), "fat": round(float(r.fat),2), "kg": round(float(r.kg),2)}
                   for idx, r in evol_dia.iterrows()]

        # Top 10 clientes
        top_cli = (dff.groupby('NOME_CLIENTE').agg(fat=('VALOR_LIQUIDO','sum'), kg=('QTDE_PRI','sum'))
                   .sort_values('fat', ascending=False).head(10).reset_index())
        top_clientes = [{"nome": str(r.NOME_CLIENTE), "fat": round(float(r.fat),2), "kg": round(float(r.kg),2)}
                        for r in top_cli.itertuples()]

        # Por filial
        por_filial = []
        if 'NOME_FILIAL' in dff.columns:
            grp_fil = (dff.groupby('NOME_FILIAL').agg(fat=('VALOR_LIQUIDO','sum'), kg=('QTDE_PRI','sum'))
                       .sort_values('fat', ascending=False).reset_index())
            por_filial = [{"filial": str(r.NOME_FILIAL), "fat": round(float(r.fat),2), "kg": round(float(r.kg),2)}
                          for r in grp_fil.itertuples()]

        # Por divisão (tipo de carne)
        por_divisao = []
        if 'DESC_DIVISAO2' in dff.columns:
            grp_div = (dff.groupby('DESC_DIVISAO2').agg(fat=('VALOR_LIQUIDO','sum'), kg=('QTDE_PRI','sum'))
                       .sort_values('kg', ascending=False).reset_index())
            por_divisao = [{"tipo": str(r.DESC_DIVISAO2), "fat": round(float(r.fat),2), "kg": round(float(r.kg),2)}
                           for r in grp_div.itertuples()]

        # Top 10 produtos
        top_prod = (dff.groupby('DESC_PRODUTO').agg(fat=('VALOR_LIQUIDO','sum'), kg=('QTDE_PRI','sum'))
                    .sort_values('kg', ascending=False).head(10).reset_index())
        top_produtos = [{"nome": str(r.DESC_PRODUTO), "fat": round(float(r.fat),2), "kg": round(float(r.kg),2)}
                        for r in top_prod.itertuples()]

        # Top vendedores
        top_vend = []
        if 'NOM_VENDEDOR' in dff.columns:
            grp_v = (dff.groupby('NOM_VENDEDOR').agg(fat=('VALOR_LIQUIDO','sum'), kg=('QTDE_PRI','sum'))
                     .sort_values('fat', ascending=False).head(10).reset_index())
            top_vend = [{"nome": str(r.NOM_VENDEDOR), "fat": round(float(r.fat),2), "kg": round(float(r.kg),2)}
                        for r in grp_v.itertuples()]

        meses_pt = ['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez']

        return JSONResponse({
            "kpis": {"fat": fat, "kg": kg, "cx30": cx30, "notas": notas, "pm": pm},
            "filtros_disp": {"anos": anos_disp, "filiais": filiais_disp, "meses": meses_pt},
            "evolucao_mensal": evolucao,
            "evolucao_diaria": por_dia,
            "top_clientes": top_clientes,
            "por_filial": por_filial,
            "por_divisao": por_divisao,
            "top_produtos": top_produtos,
            "top_vendedores": top_vend,
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/dash", response_class=HTMLResponse)
def dash_page():
    """Serve o novo dashboard ampliado."""
    try:
        with open("dashboard_new.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="dashboard_new.html não encontrado.")


@app.post("/chat-analitico")
async def chat_analitico(req: ChatRequest):
    """Modo Analítico — usa Sonnet, contexto YoY completo, análises profundas."""
    if not CLAUDE_KEY:
        raise HTTPException(status_code=500, detail="CLAUDE_API_KEY não configurada.")

    ultima = next((m.content for m in reversed(req.messages) if m.role == "user"), "")
    historico = [{"role": m.role, "content": m.content} for m in req.messages]

    df = load_df()

    # Pré-agregar contexto YoY completo
    ctx = {}

    # Evolução mensal 2025 e 2026 por filial
    if 'TIPO_OPERACAO' in df.columns:
        df_prod = df[df['TIPO_OPERACAO'] == 'PRODUTOS']
    else:
        df_prod = df

    df_prod = df_prod.copy()
    df_prod['ano']  = df_prod['DATA_MOVTO'].dt.year
    df_prod['mes']  = df_prod['DATA_MOVTO'].dt.month

    # Evolução mensal geral
    evol = df_prod.groupby(['ano','mes']).agg(
        fat=('VALOR_LIQUIDO','sum'), kg=('QTDE_PRI','sum'),
        notas=('NUM_DOCTO','nunique')
    ).reset_index()
    ctx['evolucao_mensal'] = [
        {"ano": int(r.ano), "mes": int(r.mes),
         "fat": round(float(r.fat),2), "kg": round(float(r.kg),2), "notas": int(r.notas)}
        for _, r in evol.iterrows()
    ]

    # Por filial × ano × mês
    if 'NOME_FILIAL' in df_prod.columns:
        evol_fil = df_prod.groupby(['ano','mes','NOME_FILIAL']).agg(
            fat=('VALOR_LIQUIDO','sum'), kg=('QTDE_PRI','sum')
        ).reset_index()
        ctx['evolucao_por_filial'] = [
            {"ano": int(r.ano), "mes": int(r.mes), "filial": str(r.NOME_FILIAL),
             "fat": round(float(r.fat),2), "kg": round(float(r.kg),2)}
            for _, r in evol_fil.iterrows()
        ]

    # Top 20 clientes com fat por ano
    if 'NOME_CLIENTE' in df_prod.columns:
        top_cli = df_prod.groupby(['NOME_CLIENTE','ano']).agg(
            fat=('VALOR_LIQUIDO','sum'), kg=('QTDE_PRI','sum'), notas=('NUM_DOCTO','nunique')
        ).reset_index()
        top_nomes = df_prod.groupby('NOME_CLIENTE')['VALOR_LIQUIDO'].sum().nlargest(20).index.tolist()
        top_cli = top_cli[top_cli['NOME_CLIENTE'].isin(top_nomes)]
        ctx['top_clientes_yoy'] = [
            {"cliente": str(r.NOME_CLIENTE), "ano": int(r.ano),
             "fat": round(float(r.fat),2), "kg": round(float(r.kg),2), "notas": int(r.notas)}
            for _, r in top_cli.iterrows()
        ]

    # Mix de produtos (divisão2 + divisão3) por ano
    if 'DESC_DIVISAO2' in df_prod.columns:
        grp_mix_cols = ['ano','DESC_DIVISAO2']
        if 'DESC_DIVISAO3' in df_prod.columns:
            grp_mix_cols.append('DESC_DIVISAO3')
        mix = df_prod.groupby(grp_mix_cols).agg(
            fat=('VALOR_LIQUIDO','sum'), kg=('QTDE_PRI','sum')
        ).reset_index()
        ctx['mix_produtos_yoy'] = [
            {"ano": int(r.ano),
             "divisao": str(r.DESC_DIVISAO2),
             "tipo_corte": str(r.DESC_DIVISAO3) if 'DESC_DIVISAO3' in mix.columns else None,
             "fat": round(float(r.fat),2), "kg": round(float(r.kg),2)}
            for _, r in mix.iterrows()
        ]

    # Preço médio mensal por divisão2 + divisão3
    if 'DESC_DIVISAO2' in df_prod.columns:
        grp_pm_cols = ['ano','mes','DESC_DIVISAO2']
        if 'DESC_DIVISAO3' in df_prod.columns:
            grp_pm_cols.append('DESC_DIVISAO3')
        pm = df_prod[df_prod['QTDE_PRI'] > 0].groupby(grp_pm_cols).apply(
            lambda x: round(float(x['VALOR_LIQUIDO'].sum()) / float(x['QTDE_PRI'].sum()), 2)
        ).reset_index(name='pm')
        ctx['preco_medio_mensal'] = [
            {"ano": int(r.ano), "mes": int(r.mes),
             "divisao": str(r.DESC_DIVISAO2),
             "tipo_corte": str(r.DESC_DIVISAO3) if 'DESC_DIVISAO3' in pm.columns else None,
             "pm": float(r.pm)}
            for _, r in pm.iterrows()
        ]

    # Cruzamento cliente × divisão — últimos 6 meses (top 20 clientes)
    if 'NOME_CLIENTE' in df_prod.columns and 'DESC_DIVISAO2' in df_prod.columns:
        data_corte_cross = df_prod['DATA_MOVTO'].max() - pd.DateOffset(months=6)
        df_cross = df_prod[df_prod['DATA_MOVTO'] >= data_corte_cross]
        top_nomes_cli = df_cross.groupby('NOME_CLIENTE')['VALOR_LIQUIDO'].sum().nlargest(20).index.tolist()
        df_cross = df_cross[df_cross['NOME_CLIENTE'].isin(top_nomes_cli)]
        grp_cross_cols = ['ano','mes','NOME_CLIENTE','DESC_DIVISAO2']
        cross = df_cross.groupby(grp_cross_cols).agg(
            fat=('VALOR_LIQUIDO','sum'), kg=('QTDE_PRI','sum')
        ).reset_index()
        ctx['cliente_por_divisao'] = [
            {"ano": int(r.ano), "mes": int(r.mes),
             "cliente": str(r.NOME_CLIENTE),
             "divisao": str(r.DESC_DIVISAO2),
             "fat": round(float(r.fat),2), "kg": round(float(r.kg),2)}
            for _, r in cross.iterrows()
        ]

    ctx_json = json.dumps(ctx, ensure_ascii=False)

    # Meses disponíveis
    anos = sorted(df_prod['ano'].unique().tolist())

    system_analitico = f"""Você é o IAF — Inteligência Analítica Frinense, analista comercial sênior da Frinense Alimentos.

Sua missão é transformar dados de vendas em informação clara e útil para a equipe comercial. Você conhece profundamente o negócio: filiais de Itaperuna, Bom Jesus e Porciúncula, os produtos J.Beef (dianteiro, traseiro, ponta de agulha, peito, LP), Charque e Resfriadas.

**Seu estilo:**
- Direto, preciso e confiante — como um analista experiente que respeita o tempo de quem pergunta
- Sem rodeios, sem saudações, sem "Com prazer!" — vá direto ao dado
- Quando os números são bons, pode ser assertivo. Quando são ruins, seja neutro e factual
- Use linguagem comercial brasileira — não corporativo demais, não informal demais

**Contexto do negócio:**
- Filiais: Itaperuna (ITAP), Bom Jesus (BJESUS), Porciúncula (PORC) — sempre use o nome completo
- Anos disponíveis na base: {anos}
- Hoje: {date.today().strftime('%d/%m/%Y')}

**Modo Analítico — você tem acesso ao histórico completo:**
- Pode comparar períodos, anos, filiais, produtos livremente
- Pode identificar tendências, sazonalidades, crescimentos e quedas
- Pode fazer análises cruzadas (ex: clientes que cresceram/caíram, mix de produtos mudando)
- Use os dados do contexto abaixo para TODAS as análises — NUNCA invente ou estime valores
- Finalize análises com 💡 **Insight** destacando o ponto mais relevante

⛔ NUNCA invente, estime ou arredonde valores. USE APENAS os dados do contexto abaixo.

CONTEXTO ANALÍTICO COMPLETO (evolução YoY, clientes, mix, preços):
{ctx_json}"""

    msgs = []
    for m in historico[-12:]:  # Mais histórico no modo analítico
        msgs.append({"role": m["role"], "content": m["content"][:1000]})
    msgs.append({"role": "user", "content": ultima})

    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={"Content-Type": "application/json",
                     "x-api-key": CLAUDE_KEY,
                     "anthropic-version": "2023-06-01"},
            json={"model": "claude-sonnet-4-20250514",
                  "max_tokens": 4000,
                  "system": system_analitico,
                  "messages": msgs}
        )
    data = r.json()
    if data.get("content") and len(data["content"]) > 0:
        resposta = data["content"][0]["text"]
    else:
        logging.warning(f"[ANALITICO] resposta vazia: {str(data)[:300]}")
        resposta = "❌ Erro na análise — o modelo não retornou resposta. Tente reformular a pergunta."
    return JSONResponse({"content": [{"type": "text", "text": resposta}]})


@app.post("/pdf-analitico")
async def pdf_analitico(req: starlette.requests.Request):
    """Gera PDF a partir de texto de análise do modo analítico."""
    body = await req.json()
    texto = body.get("texto", "").strip()
    titulo = body.get("titulo", "ANÁLISE COMERCIAL IAF")
    if not texto:
        raise HTTPException(status_code=400, detail="Texto vazio.")

    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, HRFlowable
    from reportlab.lib.enums import TA_LEFT, TA_CENTER
    import io, re as _re
    from datetime import date

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2*cm, bottomMargin=2*cm)

    VERM  = colors.HexColor("#CC0000")
    AMAR  = colors.HexColor("#F5C800")
    CINZ  = colors.HexColor("#1a1a1a")
    BRNCO = colors.white
    TEXTO = colors.HexColor("#e8e8e8")

    s_titulo = ParagraphStyle("titulo", fontName="Helvetica-Bold", fontSize=18,
        textColor=AMAR, spaceAfter=4, alignment=TA_CENTER)
    s_sub = ParagraphStyle("sub", fontName="Helvetica", fontSize=10,
        textColor=colors.HexColor("#aaaaaa"), spaceAfter=12, alignment=TA_CENTER)
    s_h2 = ParagraphStyle("h2", fontName="Helvetica-Bold", fontSize=13,
        textColor=AMAR, spaceBefore=14, spaceAfter=6)
    s_h3 = ParagraphStyle("h3", fontName="Helvetica-Bold", fontSize=11,
        textColor=colors.HexColor("#ff6666"), spaceBefore=10, spaceAfter=4)
    s_body = ParagraphStyle("body", fontName="Helvetica", fontSize=10,
        textColor=colors.HexColor("#333333"), leading=15, spaceAfter=6)
    s_bullet = ParagraphStyle("bullet", fontName="Helvetica", fontSize=10,
        textColor=colors.HexColor("#333333"), leading=14, leftIndent=16, spaceAfter=4)
    s_insight = ParagraphStyle("insight", fontName="Helvetica-Bold", fontSize=10,
        textColor=VERM, leading=14, spaceAfter=6, leftIndent=8)
    s_rodape = ParagraphStyle("rodape", fontName="Helvetica", fontSize=8,
        textColor=colors.HexColor("#999999"), alignment=TA_CENTER)

    story = []

    # Cabeçalho
    story.append(Paragraph("IAF · INTELIGÊNCIA ANALÍTICA FRINENSE", s_titulo))
    story.append(Paragraph(f"{titulo} · Gerado em {date.today().strftime('%d/%m/%Y')}", s_sub))
    story.append(HRFlowable(width="100%", thickness=2, color=VERM))
    story.append(Spacer(1, 0.4*cm))

    # Processar texto markdown para parágrafos
    linhas = texto.split("
")
    for linha in linhas:
        linha = linha.rstrip()
        if not linha:
            story.append(Spacer(1, 0.2*cm))
            continue
        # Remove markdown de negrito/itálico para texto limpo
        linha_limpa = _re.sub(r'\*\*([^*]+)\*\*', r'', linha)
        linha_limpa = _re.sub(r'\*([^*]+)\*', r'', linha_limpa)
        linha_limpa = _re.sub(r'`([^`]+)`', r'', linha_limpa)
        if linha.startswith("## "):
            story.append(Paragraph(linha_limpa[3:].upper(), s_h2))
        elif linha.startswith("# "):
            story.append(Paragraph(linha_limpa[2:].upper(), s_h2))
        elif linha.startswith("### "):
            story.append(Paragraph(linha_limpa[4:], s_h3))
        elif linha.startswith("- ") or linha.startswith("• "):
            story.append(Paragraph("• " + linha_limpa[2:], s_bullet))
        elif linha.startswith("💡"):
            story.append(Paragraph(linha_limpa, s_insight))
        elif linha.startswith("|"):
            # Linha de tabela — pular (não renderiza tabelas markdown no PDF)
            continue
        else:
            story.append(Paragraph(linha_limpa, s_body))

    story.append(Spacer(1, 0.5*cm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#cccccc")))
    story.append(Spacer(1, 0.2*cm))
    story.append(Paragraph("IAF · Inteligência Analítica Frinense Alimentos", s_rodape))

    doc.build(story)
    buf.seek(0)

    from fastapi.responses import Response
    return Response(
        content=buf.read(),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="IAF_Analitico_{date.today().strftime("%Y%m%d")}.pdf"'}
    )

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
