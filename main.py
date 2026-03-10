import os
import pickle
import io
import json
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

def get_drive_service():
    token_bytes = os.environ.get("GOOGLE_TOKEN_PICKLE")
    if not token_bytes:
        raise HTTPException(status_code=500, detail="Token do Google Drive não configurado.")
    import base64
    creds = pickle.loads(base64.b64decode(token_bytes))
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build('drive', 'v3', credentials=creds)

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
    sales_data: str

@app.post("/chat")
async def chat(req: ChatRequest):
    if not CLAUDE_KEY:
        raise HTTPException(status_code=500, detail="API Key da Claude não configurada.")
    
    SYSTEM_PROMPT = """Você é um Analista de Dados Comercial Sênior, bem-humorado e extremamente competente.
Você analisa dados de vendas de uma empresa brasileira do setor de alimentos com filiais em ITAP, BJESUS, PORC e TRINDADE.

REGRAS IMPORTANTES:
- Responda SEMPRE em português brasileiro
- Seja analítico e preciso nos números
- Use formatação clara com tabelas quando necessário (use HTML: <table><th><td>)
- Destaque os insights mais importantes em <strong>negrito</strong>
- Sempre ofereça opções de aprofundamento ao final da resposta
- Seja bem-humorado mas profissional
- Formate valores monetários como R$ X.XXX,XX
- Quando não tiver certeza, diga claramente

Os dados de vendas serão fornecidos em formato CSV com as seguintes colunas principais:
cod_filial, NOME_FILIAL, DESC_TIPO_MV, COD_PRODUTO, DESC_PRODUTO, NOME_CLIENTE, DATA_MOVTO,
NOM_VENDEDOR, QTDE_PRI, VALOR_UNITARIO, VALOR_LIQUIDO, DESC_DIVISAO2, DESC_DIVISAO3, UF, CIDADE"""

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
                "system": SYSTEM_PROMPT + "\n\nDADOS DE VENDAS (CSV):\n" + req.sales_data,
                "messages": [m.dict() for m in req.messages]
            }
        )
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    
    return response.json()

@app.get("/health")
def health():
    return {"status": "healthy"}
