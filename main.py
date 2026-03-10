import os
import pickle
import io
import re
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse
from pydantic import BaseModel
from typing import List
import httpx
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

FILE_ID    = os.environ.get("DRIVE_FILE_ID", "")
CLAUDE_KEY = os.environ.get("CLAUDE_API_KEY", "")

# Cache do CSV em memória
_csv_cache = {"data": None, "df": None}

def get_drive_service():
    token_bytes = os.environ.get("GOOGLE_TOKEN_PICKLE")
    if not token_bytes:
        raise HTTPException(status_code=500, detail="Token do Google Drive não configurado.")
    import base64
    creds = pickle.loads(base64.b64decode(token_bytes))
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build('drive', 'v3', credentials=creds)

def load_csv():
    """Carrega CSV do Drive e retorna DataFrame."""
    if _csv_cache["df"] is not None:
        return _csv_cache["df"]
    service = get_drive_service()
    request = service.files().get_media(fileId=FILE_ID)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buffer.seek(0)
    df = pd.read_csv(buffer, sep=';', encoding='utf-8-sig', low_memory=False)
    df['DATA_MOVTO'] = pd.to_datetime(df['DATA_MOVTO'], errors='coerce')
    _csv_cache["df"] = df
    return df

def filter_data(df: pd.DataFrame, pergunta: str) -> pd.DataFrame:
    """Filtra o DataFrame baseado em palavras-chave da pergunta."""
    pergunta_lower = pergunta.lower()
    df_filtered = df.copy()

    # Mapeamento de meses PT
    meses = {
        'janeiro': 1, 'fevereiro': 2, 'março': 3, 'marco': 3,
        'abril': 4, 'maio': 5, 'junho': 6, 'julho': 7,
        'agosto': 8, 'setembro': 9, 'outubro': 10,
        'novembro': 11, 'dezembro': 12
    }

    # Filtro por mês
    for nome, num in meses.items():
        if nome in pergunta_lower:
            df_filtered = df_filtered[df_filtered['DATA_MOVTO'].dt.month == num]
            break

    # Filtro por ano
    anos = re.findall(r'\b(202[0-9])\b', pergunta)
    if anos:
        df_filtered = df_filtered[df_filtered['DATA_MOVTO'].dt.year == int(anos[0])]

    # Filtro "ontem" / "hoje" / "última semana" / "último mês"
    from datetime import datetime, timedelta
    hoje = datetime.now()
    if 'ontem' in pergunta_lower:
        ontem = hoje - timedelta(days=1)
        df_filtered = df_filtered[df_filtered['DATA_MOVTO'].dt.date == ontem.date()]
    elif 'hoje' in pergunta_lower:
        df_filtered = df_filtered[df_filtered['DATA_MOVTO'].dt.date == hoje.date()]
    elif 'última semana' in pergunta_lower or 'ultima semana' in pergunta_lower:
        df_filtered = df_filtered[df_filtered['DATA_MOVTO'] >= hoje - timedelta(days=7)]
    elif 'último mês' in pergunta_lower or 'ultimo mes' in pergunta_lower:
        df_filtered = df_filtered[df_filtered['DATA_MOVTO'] >= hoje - timedelta(days=30)]

    # Filtro por filial
    filiais = {'itap': 'ITAP', 'bjesus': 'BJESUS', 'porc': 'PORC', 'trindade': 'TRINDADE'}
    for key, val in filiais.items():
        if key in pergunta_lower:
            df_filtered = df_filtered[df_filtered['NOME_FILIAL'].str.upper() == val]
            break

    # Filtro por cliente (nome parcial)
    cliente_match = re.search(r'cliente[:\s]+([a-záéíóúâêîôûãõç\s]+)', pergunta_lower)
    if cliente_match:
        nome_cliente = cliente_match.group(1).strip()
        if len(nome_cliente) > 2:
            df_filtered = df_filtered[
                df_filtered['NOME_CLIENTE'].str.lower().str.contains(nome_cliente, na=False)
            ]

    # Filtro por vendedor
    vendedor_match = re.search(r'vendedor[:\s]+([a-záéíóúâêîôûãõç\s]+)', pergunta_lower)
    if vendedor_match:
        nome_vend = vendedor_match.group(1).strip()
        if len(nome_vend) > 2:
            df_filtered = df_filtered[
                df_filtered['NOM_VENDEDOR'].str.lower().str.contains(nome_vend, na=False)
            ]

    # Filtro por produto
    produto_match = re.search(r'produto[:\s]+([a-záéíóúâêîôûãõç\s]+)', pergunta_lower)
    if produto_match:
        nome_prod = produto_match.group(1).strip()
        if len(nome_prod) > 2:
            df_filtered = df_filtered[
                df_filtered['DESC_PRODUTO'].str.lower().str.contains(nome_prod, na=False)
            ]

    # Se filtrou demais ou nada, garante pelo menos os últimos 30 dias
    if len(df_filtered) == 0:
        df_filtered = df[df['DATA_MOVTO'] >= hoje - timedelta(days=30)]

    # Limita a 2000 linhas para não estourar o contexto
    if len(df_filtered) > 2000:
        df_filtered = df_filtered.tail(2000)

    # Mantém apenas colunas essenciais para análise
    colunas_essenciais = [
        'NOME_FILIAL', 'DATA_MOVTO', 'DESC_PRODUTO', 'NOME_CLIENTE',
        'NOM_VENDEDOR', 'QTDE_PRI', 'VALOR_LIQUIDO', 'DESC_DIVISAO2',
        'DESC_DIVISAO3', 'UF', 'CIDADE'
    ]
    colunas_presentes = [c for c in colunas_essenciais if c in df_filtered.columns]
    df_filtered = df_filtered[colunas_presentes]

    return df_filtered

@app.get("/", response_class=HTMLResponse)
def root():
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Analista de Vendas IA</h1>")

@app.get("/vendas")
def get_vendas():
    try:
        service = get_drive_service()
        request = service.files().get_media(fileId=FILE_ID)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        buffer.seek(0)
        # Invalida cache ao atualizar
        _csv_cache["df"] = None
        return StreamingResponse(
            buffer,
            media_type="text/csv",
            headers={"Content-Disposition": "inline; filename=vendas.csv"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class Message(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    messages: List[Message]

@app.post("/chat")
async def chat(req: ChatRequest):
    if not CLAUDE_KEY:
        raise HTTPException(status_code=500, detail="API Key da Claude não configurada.")

    # Pega a última pergunta do usuário
    ultima_pergunta = next(
        (m.content for m in reversed(req.messages) if m.role == "user"), ""
    )

    # Carrega e filtra os dados
    try:
        df = load_csv()
        df_filtrado = filter_data(df, ultima_pergunta)
        sales_data = df_filtrado.to_csv(sep=';', index=False)
        linhas_filtradas = len(df_filtrado)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao carregar dados: {str(e)}")

    SYSTEM_PROMPT = f"""Você é o Jhon, Analista Comercial Sênior da Frinense Alimentos.

PERFIL:
- Especialista em indicadores comerciais com foco em volume de vendas
- Comunicativo mas discreto — vai direto ao ponto, sem rodeios
- Respostas claras, organizadas e sempre orientadas a dados
- Conhece profundamente o mercado de alimentos e frigoríficos
- Prioriza sempre volume de vendas (kg/unidades) antes de valor financeiro
- Nunca inventa dados — se não tiver certeza, diz claramente

EMPRESA:
- Frinense Alimentos — setor frigorífico/alimentos, Brasil
- Filiais: ITAP (Itaperuna), BJESUS (Bom Jesus), PORC (Porciúncula), TRINDADE (Trindade)

REGRAS DE FORMATAÇÃO:
- Responda SEMPRE em português brasileiro
- Use Markdown: ## para títulos de seção, **negrito** para destaques
- Use tabelas Markdown para dados comparativos (| Col | Col |)
- Formate valores monetários como R$ X.XXX,XX
- Formate quantidades com unidade: ex. 15.320 kg
- Sempre finalize com 1 insight relevante ou sugestão de próxima análise
- Seja conciso: respostas objetivas, sem introduções longas

DADOS DISPONÍVEIS ({linhas_filtradas} registros filtrados para esta pergunta):
{sales_data}"""

    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type": "application/json",
                "x-api-key": CLAUDE_KEY,
                "anthropic-version": "2023-06-01"
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 1500,
                "system": SYSTEM_PROMPT,
                "messages": [m.dict() for m in req.messages]
            }
        )

    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    return response.json()

@app.get("/health")
def health():
    return {"status": "healthy"}
