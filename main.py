import os
os.environ.setdefault('TZ', 'America/Sao_Paulo')
try:
    import time; time.tzset()
except AttributeError:
    pass
import pickle
import io
import re
import pandas as pd
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from typing import List
import httpx
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

FILE_ID    = os.environ.get("DRIVE_FILE_ID", "")
CLAUDE_KEY = os.environ.get("CLAUDE_API_KEY", "")

# Sem cache — sempre busca do Drive diretamente

def get_drive_service():
    token_bytes = os.environ.get("GOOGLE_TOKEN_PICKLE")
    if not token_bytes:
        raise HTTPException(status_code=500, detail="Token do Google Drive não configurado.")
    import base64
    creds = pickle.loads(base64.b64decode(token_bytes))
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build('drive', 'v3', credentials=creds)

def load_df() -> pd.DataFrame:
    """Sempre busca CSV diretamente do Drive — sem cache."""
    service = get_drive_service()
    req = service.files().get_media(fileId=FILE_ID)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    buf.seek(0)
    df = pd.read_csv(buf, sep=';', encoding='utf-8-sig', low_memory=False)
    df['DATA_MOVTO'] = pd.to_datetime(df['DATA_MOVTO'], errors='coerce')
    df['VALOR_LIQUIDO'] = pd.to_numeric(df['VALOR_LIQUIDO'], errors='coerce').fillna(0)
    df['QTDE_PRI']      = pd.to_numeric(df['QTDE_PRI'],      errors='coerce').fillna(0)
    return df

def get_dia_referencia(df: pd.DataFrame):
    """Retorna o último dia com dados (hoje se existir, senão último disponível)."""
    hoje = datetime.now().date()
    if (df['DATA_MOVTO'].dt.date == hoje).any():
        return hoje
    ultimo = df['DATA_MOVTO'].dropna().dt.date.max()
    return ultimo

def filter_for_chat(df: pd.DataFrame, pergunta: str) -> pd.DataFrame:
    """Filtra dados para o chat baseado na pergunta."""
    pl = pergunta.lower()
    dff = df.copy()

    meses = {'janeiro':1,'fevereiro':2,'março':3,'marco':3,'abril':4,'maio':5,
             'junho':6,'julho':7,'agosto':8,'setembro':9,'outubro':10,'novembro':11,'dezembro':12}
    for nome, num in meses.items():
        if nome in pl:
            dff = dff[dff['DATA_MOVTO'].dt.month == num]
            break

    anos = re.findall(r'\b(202[0-9])\b', pergunta)
    if anos:
        dff = dff[dff['DATA_MOVTO'].dt.year == int(anos[0])]

    hoje = datetime.now()
    if 'ontem' in pl:
        dia = (hoje - timedelta(days=1)).date()
        dff = dff[dff['DATA_MOVTO'].dt.date == dia]
    elif 'hoje' in pl:
        dff = dff[dff['DATA_MOVTO'].dt.date == hoje.date()]
    elif 'última semana' in pl or 'ultima semana' in pl:
        dff = dff[dff['DATA_MOVTO'] >= hoje - timedelta(days=7)]
    elif 'último mês' in pl or 'ultimo mes' in pl:
        dff = dff[dff['DATA_MOVTO'] >= hoje - timedelta(days=30)]

    filiais = {'itap':'ITAP','bjesus':'BJESUS','porc':'PORC','trindade':'TRINDADE'}
    for key, val in filiais.items():
        if key in pl:
            dff = dff[dff['NOME_FILIAL'].str.upper() == val]
            break

    m_cliente = re.search(r'cliente[:\s]+([a-záéíóúâêîôûãõç\s]+)', pl)
    if m_cliente and len(m_cliente.group(1).strip()) > 2:
        dff = dff[dff['NOME_CLIENTE'].str.lower().str.contains(m_cliente.group(1).strip(), na=False)]

    # Busca livre por nome de cliente (quando não usa palavra "cliente:")
    # Detecta padrões como "distribuidora uniao", "rca alimentos" etc na pergunta
    elif not any(x in pl for x in ['produto','vendedor','filial','ranking','comparar','top','total','resumo']):
        palavras = [p for p in pl.split() if len(p) > 3 and p not in
                    ['últimas','ultimas','vendas','venda','quais','qual','como','foram','mais','este','essa','esse','para','pela','pelo','mês','mes','ano','2025','2026']]
        if palavras:
            termo = ' '.join(palavras[:3])
            mask = dff['NOME_CLIENTE'].str.lower().str.contains(termo, na=False)
            if mask.sum() > 0:
                dff = dff[mask]

    cols = ['NOME_FILIAL','DATA_MOVTO','NUM_DOCTO','COD_PRODUTO','DESC_PRODUTO','NOME_CLIENTE',
            'NOM_VENDEDOR','QTDE_PRI','VALOR_LIQUIDO','DESC_DIVISAO2','DESC_DIVISAO3']

    # Detecta intenção de "últimas vendas" — já filtrado por cliente acima, limita 15 linhas
    if any(x in pl for x in ['últimas vendas','ultimas vendas','ultima venda','última venda']):
        dff = dff.sort_values('DATA_MOVTO', ascending=False).head(15)
        return dff[[c for c in cols if c in dff.columns]]

    m = re.search(r'vendedor[:\s]+([a-záéíóúâêîôûãõç\s]+)', pl)
    if m and len(m.group(1).strip()) > 2:
        dff = dff[dff['NOM_VENDEDOR'].str.lower().str.contains(m.group(1).strip(), na=False)]

    m = re.search(r'produto[:\s]+([a-záéíóúâêîôûãõç\s]+)', pl)
    if m and len(m.group(1).strip()) > 2:
        dff = dff[dff['DESC_PRODUTO'].str.lower().str.contains(m.group(1).strip(), na=False)]

    if len(dff) == 0:
        dff = df[df['DATA_MOVTO'] >= hoje - timedelta(days=30)]

    if len(dff) > 2000:
        dff = dff.tail(2000)

    return dff[[c for c in cols if c in dff.columns]]

def aggregate_for_summary(dff: pd.DataFrame) -> str:
    """Agrega dados para resumos mensais — evita estouro de tokens."""
    lines = []
    
    # Totais gerais
    total_kg = dff['QTDE_PRI'].sum()
    total_fat = dff['VALOR_LIQUIDO'].sum()
    total_notas = dff['NUM_DOCTO'].nunique()
    preco_medio = total_fat / total_kg if total_kg > 0 else 0
    lines.append(f"## RESUMO GERAL")
    lines.append(f"Total: {total_kg:,.2f} kg | R$ {total_fat:,.2f} | {total_notas} notas | R$ {preco_medio:.2f}/kg")
    lines.append("")

    # Por filial
    lines.append("## POR FILIAL")
    por_filial = dff.groupby('NOME_FILIAL').agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum'), notas=('NUM_DOCTO','nunique')).sort_values('kg', ascending=False)
    for idx, r in por_filial.iterrows():
        pm = r.fat/r.kg if r.kg > 0 else 0
        lines.append(f"{idx}: {r.kg:,.2f} kg | R$ {r.fat:,.2f} | {r.notas} notas | R$ {pm:.2f}/kg")
    lines.append("")

    # Por dia
    lines.append("## POR DIA")
    por_dia = dff.groupby(dff['DATA_MOVTO'].dt.strftime('%d/%m/%y')).agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum'), notas=('NUM_DOCTO','nunique')).sort_index()
    for idx, r in por_dia.iterrows():
        pm = r.fat/r.kg if r.kg > 0 else 0
        lines.append(f"{idx}: {r.kg:,.2f} kg | R$ {r.fat:,.2f} | {r.notas} notas | R$ {pm:.2f}/kg")
    lines.append("")

    # Top 15 clientes
    lines.append("## TOP 15 CLIENTES (por volume)")
    por_cli = dff.groupby('NOME_CLIENTE').agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum')).sort_values('kg', ascending=False).head(15)
    for idx, r in por_cli.iterrows():
        pm = r.fat/r.kg if r.kg > 0 else 0
        lines.append(f"{idx}: {r.kg:,.2f} kg | R$ {r.fat:,.2f} | R$ {pm:.2f}/kg")
    lines.append("")

    # Top 15 produtos
    lines.append("## TOP 15 PRODUTOS (por volume)")
    por_prod = dff.groupby(['COD_PRODUTO','DESC_PRODUTO']).agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum')).sort_values('kg', ascending=False).head(15)
    for idx, r in por_prod.iterrows():
        pm = r.fat/r.kg if r.kg > 0 else 0
        lines.append(f"{idx[1]}: {r.kg:,.2f} kg | R$ {r.fat:,.2f} | R$ {pm:.2f}/kg")
    lines.append("")

    # Top 10 vendedores
    lines.append("## TOP 10 VENDEDORES (por volume)")
    por_vend = dff.groupby('NOM_VENDEDOR').agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum')).sort_values('kg', ascending=False).head(10)
    for idx, r in por_vend.iterrows():
        pm = r.fat/r.kg if r.kg > 0 else 0
        lines.append(f"{idx}: {r.kg:,.2f} kg | R$ {r.fat:,.2f} | R$ {pm:.2f}/kg")
    lines.append("")

    # Por tipo de carne
    lines.append("## POR TIPO DE CARNE (DESC_DIVISAO2)")
    por_tipo = dff.groupby('DESC_DIVISAO2').agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum')).sort_values('kg', ascending=False)
    for idx, r in por_tipo.iterrows():
        pm = r.fat/r.kg if r.kg > 0 else 0
        lines.append(f"{idx}: {r.kg:,.2f} kg | R$ {r.fat:,.2f} | R$ {pm:.2f}/kg")

    return "\n".join(lines)

def is_summary_query(pergunta: str) -> bool:
    """Detecta se é uma pergunta de resumo/análise geral que precisa de agregação."""
    pl = pergunta.lower()
    summary_keywords = [
        'como está','como esta','resumo','análise','analise','comparar','comparativo',
        'ranking','top','total','mês','mes','periodo','período','evolução','evolucao',
        'desempenho','performance','balanço','balanco','visão geral','visao geral',
        'quanto vendeu','quanto foi','quanto faturou'
    ]
    # Não agregar se for busca específica de cliente/produto/nota
    specific_keywords = ['últimas vendas','ultimas vendas','ultima venda','última venda','nota ','nr ']
    if any(x in pl for x in specific_keywords):
        return False
    return any(x in pl for x in summary_keywords)

# ─── ROUTES ───

@app.get("/", response_class=HTMLResponse)
def root():
    p = os.path.join(os.path.dirname(__file__), "index.html")
    if os.path.exists(p):
        with open(p, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse("<h1>Jhon</h1>")

@app.get("/dashboard")
def dashboard():
    """Retorna KPIs + top10 clientes — JSON leve, sem CSV."""
    try:
        df = load_df()
        dia = get_dia_referencia(df)
        df_dia = df[df['DATA_MOVTO'].dt.date == dia]

        fat   = float(df_dia['VALOR_LIQUIDO'].sum())
        kg    = float(df_dia['QTDE_PRI'].sum())
        notas = int(df_dia.shape[0])
        total = int(df.shape[0])

        # Última nota com hora
        ultima_str = "—"
        ultima = df.dropna(subset=['DATA_MOVTO']).sort_values('DATA_MOVTO').iloc[-1]['DATA_MOVTO']
        if pd.notna(ultima):
            ultima_str = ultima.strftime('%d/%m/%Y %H:%M')

        # Top 10 clientes por volume
        top = (df_dia.groupby('NOME_CLIENTE')
               .agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum'))
               .sort_values('kg', ascending=False)
               .head(10)
               .reset_index())
        top10 = [{"nome": r.NOME_CLIENTE, "kg": round(r.kg,2), "fat": round(r.fat,2)}
                 for r in top.itertuples()]

        # Tipos de carne do dia (DESC_DIVISAO2)
        df_dia2 = df_dia.copy()
        df_dia2['DESC_DIVISAO2'] = df_dia2['DESC_DIVISAO2'].fillna('').str.strip()
        df_dia2.loc[df_dia2['DESC_DIVISAO2'] == '', 'DESC_DIVISAO2'] = 'SEM CLASS.'
        tipos_grp = (df_dia2.groupby('DESC_DIVISAO2')
                     .agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum'))
                     .sort_values('kg', ascending=False)
                     .reset_index())
        tipos = [{"tipo": r.DESC_DIVISAO2, "kg": round(r.kg,2), "fat": round(r.fat,2)}
                 for r in tipos_grp.itertuples()]

        dia_label = dia.strftime('%d/%m/%Y')

        return JSONResponse({
            "total_registros": total,
            "dia_label": dia_label,
            "fat": round(fat, 2),
            "kg":  round(kg, 2),
            "notas": notas,
            "ultima_nota": ultima_str,
            "top10": top10,
            "tipos": tipos
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/cliente/{nome}")
def detalhe_cliente(nome: str):
    """Retorna produtos comprados pelo cliente no dia de referência."""
    try:
        df = load_df()
        dia = get_dia_referencia(df)
        df_dia = df[df['DATA_MOVTO'].dt.date == dia]
        df_cli = df_dia[df_dia['NOME_CLIENTE'].str.upper() == nome.upper()]

        fat_total = float(df_cli['VALOR_LIQUIDO'].sum())
        kg_total  = float(df_cli['QTDE_PRI'].sum())

        prods = (df_cli.groupby(['DESC_PRODUTO','DESC_DIVISAO2'])
                 .agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum'))
                 .sort_values('kg', ascending=False)
                 .reset_index())
        produtos = [{"nome": r.DESC_PRODUTO, "tipo": r.DESC_DIVISAO2, "kg": round(r.kg,2), "fat": round(r.fat,2)}
                    for r in prods.itertuples()]

        return JSONResponse({
            "nome": nome,
            "fat_total": round(fat_total,2),
            "kg_total":  round(kg_total,2),
            "produtos": produtos
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class Message(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    messages: List[Message]
    modo: str = "normal"  # normal | mengo | vasco

@app.post("/chat")
async def chat(req: ChatRequest):
    if not CLAUDE_KEY:
        raise HTTPException(status_code=500, detail="CLAUDE_API_KEY não configurada.")

    ultima = next((m.content for m in reversed(req.messages) if m.role == "user"), "")

    try:
        df = load_df()
        dff = filter_for_chat(df, ultima)
        n = len(dff)
        # Para resumos com muitos dados, usa agregação em vez de CSV bruto
        if n > 1500 or is_summary_query(ultima):
            sales_data = aggregate_for_summary(dff)
            data_label = f"DADOS AGREGADOS ({n} registros originais)"
        else:
            dff = dff.copy()
            dff['DATA_MOVTO'] = dff['DATA_MOVTO'].dt.strftime('%d/%m/%y')
            sales_data = dff.to_csv(sep=';', index=False)
            data_label = f"{n} registros"
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao carregar dados: {e}")

    # Personalidade extra por modo
    if req.modo == "mengo":
        personalidade = """
- MODO NAÇÃO ATIVADO 🔴⚫: Você é torcedor fanático do Flamengo! As análises são corretas e profissionais, MAS você tempera com referências rubro-negras
- Compare desempenhos com craques do Mengão: volumes altos = "digno de Gabigol", crescimento = "aceleração de Bruno Henrique", consistência = "solidez de Arrascaeta"
- Momentos históricos: título da Libertadores 2019, Maracanã lotado, "É campeão!"
- Use expressões da torcida: "Que Dia!", "Urubu voou!", "A Nação agradece!"
- Emojis: 🔴⚫🦅 ocasionalmente
- Mas NUNCA sacrifique a precisão dos dados pela empolgação"""
    elif req.modo == "vasco":
        personalidade = """
- MODO GIGANTE DA COLINA ATIVADO ⬛⬜: Você é torcedor apaixonado do Vasco da Gama! As análises são corretas e profissionais, MAS você tempera com referências vascaínas
- Compare desempenhos com ídolos do Vasco: volumes expressivos = "na força de Romário", precisão = "fineza de Juninho Pernambucano", volume crescente = "na raça de Edmundo"
- Momentos históricos: tetracampeão brasileiro, Maracanã, "São Januário é uma festa!"
- Use expressões da torcida: "Gigante!", "Colina Sagrada!", "Vasco é Vasco!"
- Emojis: ⬛⬜⚔️ ocasionalmente
- Mas NUNCA sacrifique a precisão dos dados pela empolgação"""
    else:
        personalidade = ""

    system = f"""Você é o IAF, Analista Comercial Sênior da Frinense Alimentos.
- Especialista em indicadores comerciais, foco em volume de vendas (kg)
- Comunicativo mas direto — sem rodeios, sem introduções longas
- Prioriza volume (kg) antes de valor financeiro
- Nunca inventa dados
- Filiais: ITAP (Itaperuna), BJESUS (Bom Jesus), PORC (Porciúncula), TRINDADE (Trindade)
- Use Markdown: ## títulos, **negrito**, tabelas com | Col |
- Valores: R$ X.XXX,XX | Quantidades: X.XXX,XX kg
- Sempre calcule e exiba o PREÇO MÉDIO (R$/kg) em qualquer análise de produto, cliente ou vendedor — calcule como VALOR_LIQUIDO / QTDE_PRI e formate como R$ X,XX/kg
- Datas sempre no formato DD/MM/AA (ex: 09/03/26)
- Finalize SEMPRE com 1 insight ou sugestão OBRIGATORIAMENTE precedido de "💡 Insight:" em linha separada
- Nos insights: SEMPRE cite o dia específico (DD/MM/AA), número do documento (NR NOTA) e/ou produto quando relevante — seja o mais específico possível. Ex: "💡 Insight: Em 11/03/26, a NR NOTA 35900 de J. BEEF DIANTEIRO teve R$/kg acima da média..."
- Quando perguntado sobre "últimas vendas de um cliente" sem especificar o nome, pergunte qual cliente. Quando o cliente for informado, mostre uma tabela com colunas: DATA | NR NOTA | COD PRODUTO | DESCRIÇÃO | QTDE (kg) | R$/kg — ordenada por data decrescente — limitada aos últimos 15 registros
- Quando identificar que a pergunta envolve um período longo (mensal, trimestral, semestral ou anual), ANTES de responder pergunte ao usuário qual o nível de detalhe desejado, oferecendo opções claras. Exemplo: "Identificei que você quer uma análise mensal. Como prefere ver os dados? 1) Resumo executivo (totais por filial e top clientes) 2) Análise detalhada por dia 3) Ranking completo de produtos e vendedores 4) Comparativo entre filiais". Só processe após a confirmação do usuário.
{personalidade}
DADOS ({data_label}):
{sales_data}"""

    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={"Content-Type":"application/json",
                     "x-api-key":CLAUDE_KEY,
                     "anthropic-version":"2023-06-01"},
            json={"model":"claude-haiku-4-5-20251001",
                  "max_tokens":1500,
                  "system":system,
                  "messages":[m.dict() for m in req.messages]}
        )
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)
    return r.json()

@app.get("/health")
def health():
    return {"status":"ok","cache":"disabled"}

@app.get("/vendas")
def reload_vendas():
    """Confirma que Drive está acessível."""
    try:
        load_df()
        return {"status":"ok","message":"CSV carregado com sucesso"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
