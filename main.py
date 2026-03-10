import os
import pickle
import io
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
import requests
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

app = FastAPI()

# Permite requisições de qualquer origem (o chatbot no Netlify)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ID do arquivo no Google Drive
FILE_ID = os.environ.get("DRIVE_FILE_ID", "1NBiwDGmoJZ2ydO09LhuJjs6FXmQgGEK7")

def get_drive_service():
    """Autentica no Google Drive usando token.pickle salvo como variável de ambiente."""
    token_bytes = os.environ.get("GOOGLE_TOKEN_PICKLE")
    if not token_bytes:
        raise HTTPException(status_code=500, detail="Token do Google Drive não configurado.")
    
    import base64
    creds = pickle.loads(base64.b64decode(token_bytes))
    
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    
    return build('drive', 'v3', credentials=creds)

@app.get("/")
def root():
    return {"status": "ok", "message": "Servidor Analista de Vendas IA"}

@app.get("/vendas")
def get_vendas():
    """Retorna o CSV de vendas do Google Drive."""
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

@app.get("/health")
def health():
    return {"status": "healthy"}
