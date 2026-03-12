import os
os.environ.setdefault('TZ', 'America/Sao_Paulo')
try:
    import time; time.tzset()
except AttributeError:
    pass
import pickle
import io
import re
import base64
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
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
    # Detecta formato da data no CSV (DD/MM/YYYY brasileiro vs YYYY-MM-DD ISO)
    sample = df['DATA_MOVTO'].dropna().iloc[0] if len(df) > 0 else ''
    use_dayfirst = bool(re.match(r'\d{1,2}/\d{1,2}/\d{2,4}', str(sample)))
    df['DATA_MOVTO'] = pd.to_datetime(df['DATA_MOVTO'], errors='coerce', dayfirst=use_dayfirst)
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

    hoje = datetime.now()

    # ── Intervalo de datas explícito: "de DD/MM/YYYY a DD/MM/YYYY" ──
    m_range = re.search(r'(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})\s+a[té]?\s+(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})', pl)
    if m_range:
        try:
            d1 = datetime(int(m_range.group(3)) if len(m_range.group(3))==4 else 2000+int(m_range.group(3)),
                          int(m_range.group(2)), int(m_range.group(1)))
            d2 = datetime(int(m_range.group(6)) if len(m_range.group(6))==4 else 2000+int(m_range.group(6)),
                          int(m_range.group(5)), int(m_range.group(4)))
            dff = dff[(dff['DATA_MOVTO'] >= d1) & (dff['DATA_MOVTO'] <= d2 + timedelta(days=1))]
        except:
            pass
        return _finalize_filter(dff, pl)

    # ── Ano explícito ──
    anos = re.findall(r'\b(202[0-9])\b', pergunta)
    ano_ref = int(anos[0]) if anos else hoje.year

    # ── Meses encontrados na pergunta (suporta intervalo: "novembro de 2025 a fevereiro de 2026") ──
    meses_map = {'janeiro':1,'fevereiro':2,'março':3,'marco':3,'abril':4,'maio':5,
                 'junho':6,'julho':7,'agosto':8,'setembro':9,'outubro':10,'novembro':11,'dezembro':12}

    meses_encontrados = []
    for nome, num in meses_map.items():
        for m in re.finditer(nome, pl):
            trecho = pl[m.start():m.start()+30]
            ano_local = re.search(r'202[0-9]', trecho)
            ano_mes = int(ano_local.group()) if ano_local else None
            meses_encontrados.append((m.start(), num, ano_mes))
    meses_encontrados.sort(key=lambda x: x[0])

    if len(meses_encontrados) >= 2:
        # Intervalo entre dois meses (ex: "novembro/2025 a fevereiro/2026")
        _, mes_ini, ano_ini = meses_encontrados[0]
        _, mes_fim, ano_fim = meses_encontrados[-1]
        ano_ini = ano_ini or (int(anos[0]) if anos else hoje.year)
        ano_fim = ano_fim or (int(anos[-1]) if len(anos) > 1 else ano_ini)
        d1 = pd.Timestamp(ano_ini, mes_ini, 1)
        d2 = pd.Timestamp(ano_fim, mes_fim + 1, 1) - timedelta(days=1) if mes_fim < 12 else pd.Timestamp(ano_fim + 1, 1, 1) - timedelta(days=1)
        dff = dff[(dff['DATA_MOVTO'] >= d1) & (dff['DATA_MOVTO'] <= d2)]
        return _finalize_filter(dff, pl)

    elif len(meses_encontrados) == 1:
        _, mes_encontrado, ano_mes = meses_encontrados[0]
        ano_usar = ano_mes or ano_ref
        dff = dff[(dff['DATA_MOVTO'].dt.month == mes_encontrado) &
                  (dff['DATA_MOVTO'].dt.year == ano_usar)]
        return _finalize_filter(dff, pl)

    # ── Trimestre ──
    if 'trimestre' in pl or '1º tri' in pl or 'primeiro trimestre' in pl:
        tri = 1
        if '2º tri' in pl or 'segundo trimestre' in pl: tri = 2
        elif '3º tri' in pl or 'terceiro trimestre' in pl: tri = 3
        elif '4º tri' in pl or 'quarto trimestre' in pl: tri = 4
        mes_ini = (tri - 1) * 3 + 1
        mes_fim = mes_ini + 2
        dff = dff[(dff['DATA_MOVTO'].dt.month >= mes_ini) &
                  (dff['DATA_MOVTO'].dt.month <= mes_fim) &
                  (dff['DATA_MOVTO'].dt.year == ano_ref)]
        return _finalize_filter(dff, pl)

    # ── Semestre ──
    if 'semestre' in pl:
        if '1º sem' in pl or 'primeiro semestre' in pl:
            dff = dff[(dff['DATA_MOVTO'].dt.month <= 6) & (dff['DATA_MOVTO'].dt.year == ano_ref)]
        else:
            dff = dff[(dff['DATA_MOVTO'].dt.month >= 7) & (dff['DATA_MOVTO'].dt.year == ano_ref)]
        return _finalize_filter(dff, pl)

    # ── Ano inteiro ──
    if 'ano' in pl and anos:
        dff = dff[dff['DATA_MOVTO'].dt.year == ano_ref]
        return _finalize_filter(dff, pl)

    # ── Relativos ──
    # "mês passado" / "mes passado" → mês anterior completo
    if any(x in pl for x in ['mês passado','mes passado','mês anterior','mes anterior']):
        primeiro_dia_mes_atual = hoje.replace(day=1)
        ultimo_dia_mes_passado = primeiro_dia_mes_atual - timedelta(days=1)
        primeiro_dia_mes_passado = ultimo_dia_mes_passado.replace(day=1)
        dff = dff[(dff['DATA_MOVTO'] >= pd.Timestamp(primeiro_dia_mes_passado)) &
                  (dff['DATA_MOVTO'] <= pd.Timestamp(ultimo_dia_mes_passado))]

    elif any(x in pl for x in ['este mês','esse mês','este mes','esse mes','mês atual','mes atual']):
        primeiro_dia = hoje.replace(day=1)
        dff = dff[dff['DATA_MOVTO'] >= pd.Timestamp(primeiro_dia)]

    elif any(x in pl for x in ['semana passada','semana anterior']):
        dias_desde_segunda = hoje.weekday()
        ultima_segunda = hoje - timedelta(days=dias_desde_segunda + 7)
        ultimo_domingo = ultima_segunda + timedelta(days=6)
        dff = dff[(dff['DATA_MOVTO'] >= pd.Timestamp(ultima_segunda)) &
                  (dff['DATA_MOVTO'] <= pd.Timestamp(ultimo_domingo) + timedelta(days=1))]

    elif any(x in pl for x in ['esta semana','essa semana','semana atual']):
        dias_desde_segunda = hoje.weekday()
        segunda = hoje - timedelta(days=dias_desde_segunda)
        dff = dff[dff['DATA_MOVTO'] >= pd.Timestamp(segunda)]

    elif any(x in pl for x in ['última semana','ultima semana','ultimos 7 dias','últimos 7 dias']):
        dff = dff[dff['DATA_MOVTO'] >= hoje - timedelta(days=7)]

    elif 'ontem' in pl:
        dia = hoje - timedelta(days=1)
        dff = dff[(dff['DATA_MOVTO'] >= pd.Timestamp(dia.date())) &
                  (dff['DATA_MOVTO'] < pd.Timestamp(hoje.date()))]

    elif 'hoje' in pl:
        dff = dff[dff['DATA_MOVTO'] >= pd.Timestamp(hoje.date())]

    elif any(x in pl for x in ['último mês','ultimo mes','ultimos 30','últimos 30']):
        dff = dff[dff['DATA_MOVTO'] >= hoje - timedelta(days=30)]

    else:
        # ── "últimos N meses/dias/semanas" com regex ──
        m_rel = re.search(r'[uú]ltimos?\s+(\d+)\s+(m[eê]s(?:es)?|dia[s]?|semana[s]?)', pl)
        if m_rel:
            n = int(m_rel.group(1))
            unidade = m_rel.group(2)
            if 'm' in unidade:  # meses
                # Volta N meses a partir do primeiro dia do mês atual
                primeiro_mes_atual = hoje.replace(day=1)
                mes = primeiro_mes_atual.month - n
                ano = primeiro_mes_atual.year + (mes - 1) // 12
                mes = ((mes - 1) % 12) + 1
                d_ini = pd.Timestamp(ano, mes, 1)
                dff = dff[dff['DATA_MOVTO'] >= d_ini]
            elif 'semana' in unidade:
                dff = dff[dff['DATA_MOVTO'] >= hoje - timedelta(weeks=n)]
            else:  # dias
                dff = dff[dff['DATA_MOVTO'] >= hoje - timedelta(days=n)]

    return _finalize_filter(dff, pl)


def _finalize_filter(dff: pd.DataFrame, pl: str) -> pd.DataFrame:
    """Aplica filtros de filial/cliente/vendedor/produto e limita tamanho."""
    hoje = datetime.now()

    filiais = {'itap':'ITAP','bjesus':'BJESUS','porc':'PORC','trindade':'TRINDADE'}
    for key, val in filiais.items():
        if key in pl:
            dff = dff[dff['NOME_FILIAL'].str.upper() == val]
            break

    m_cliente = re.search(r'cliente[:\s]+([a-záéíóúâêîôûãõç\s]+)', pl)
    if m_cliente and len(m_cliente.group(1).strip()) > 2:
        dff = dff[dff['NOME_CLIENTE'].str.lower().str.contains(m_cliente.group(1).strip(), na=False)]
    elif not any(x in pl for x in ['produto','vendedor','filial','ranking','comparar','top','total','resumo',
                                    'março','marco','janeiro','fevereiro','abril','maio','junho','julho',
                                    'agosto','setembro','outubro','novembro','dezembro','trimestre','semestre']):
        palavras = [p for p in pl.split() if len(p) > 3 and p not in
                    ['últimas','ultimas','vendas','venda','quais','qual','como','foram','mais','este',
                     'essa','esse','para','pela','pelo','mês','mes','ano','2025','2026']]
        if palavras:
            termo = ' '.join(palavras[:3])
            mask = dff['NOME_CLIENTE'].str.lower().str.contains(termo, na=False)
            if mask.sum() > 0:
                dff = dff[mask]

    cols = ['NOME_FILIAL','DATA_MOVTO','NUM_DOCTO','COD_PRODUTO','DESC_PRODUTO','NOME_CLIENTE',
            'NOM_VENDEDOR','COD_VENDEDOR','QTDE_PRI','VALOR_LIQUIDO','DESC_DIVISAO2','DESC_DIVISAO3']

    if any(x in pl for x in ['últimas vendas','ultimas vendas','ultima venda','última venda']):
        dff = dff.sort_values('DATA_MOVTO', ascending=False).head(15)
        return dff[[c for c in cols if c in dff.columns]]

    m = re.search(r'vendedor[:\s]+([a-záéíóúâêîôûãõç\s]+)', pl)
    if m and len(m.group(1).strip()) > 2:
        dff = dff[dff['NOM_VENDEDOR'].str.lower().str.contains(m.group(1).strip(), na=False)]
    else:
        # Detecta código de vendedor: "cod 4063", "código 4063", "cod_vendedor 4063"
        m_cod = re.search(r'cod(?:igo)?[_\s]+(?:vendedor[_\s]+)?(\d{3,6})', pl)
        if m_cod:
            cod = m_cod.group(1)
            if 'COD_VENDEDOR' in dff.columns:
                dff_cod = dff[dff['COD_VENDEDOR'].astype(str).str.strip() == cod]
                if len(dff_cod) > 0:
                    dff = dff_cod

    m = re.search(r'produto[:\s]+([a-záéíóúâêîôûãõç\s]+)', pl)
    if m and len(m.group(1).strip()) > 2:
        dff = dff[dff['DESC_PRODUTO'].str.lower().str.contains(m.group(1).strip(), na=False)]

    if len(dff) == 0:
        dff_orig = pd.DataFrame()  # retorna vazio — não força fallback 30 dias
        return dff_orig[[c for c in cols if c in dff_orig.columns]] if len(dff_orig) else dff[[c for c in cols if c in dff.columns]]

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

    # Todos os vendedores (com código)
    lines.append("## VENDEDORES (por volume)")
    if 'COD_VENDEDOR' in dff.columns:
        por_vend = dff.groupby(['COD_VENDEDOR','NOM_VENDEDOR']).agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum')).sort_values('kg', ascending=False)
        for idx, r in por_vend.iterrows():
            pm = r.fat/r.kg if r.kg > 0 else 0
            lines.append(f"COD {idx[0]} | {idx[1]}: {r.kg:,.2f} kg | R$ {r.fat:,.2f} | R$ {pm:.2f}/kg")
    else:
        por_vend = dff.groupby('NOM_VENDEDOR').agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum')).sort_values('kg', ascending=False)
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
        'como está','como esta','como foi','como ficou','me mostra','me mostre',
        'resumo','análise','analise','comparar','comparativo',
        'ranking','top','total','mês','mes','periodo','período','evolução','evolucao',
        'desempenho','performance','balanço','balanco','visão geral','visao geral',
        'quanto vendeu','quanto foi','quanto faturou','quero os dados','quero ver',
        'trimestre','semestre','semana','janeiro','fevereiro','março','marco','abril',
        'maio','junho','julho','agosto','setembro','outubro','novembro','dezembro',
        'mês passado','mes passado','mês anterior','mes anterior','este mês','esse mês',
        'este mes','esse mes','semana passada','semana anterior','esta semana','essa semana'
    ]
    # Não agregar se for busca específica de cliente/produto/nota
    specific_keywords = ['últimas vendas','ultimas vendas','ultima venda','última venda','nota ','nr ']
    if any(x in pl for x in specific_keywords):
        return False

    # Sempre agrega se tiver "últimos N meses/semanas"
    if re.search(r'[uú]ltimos?\s+\d+\s+(m[eê]s|semana)', pl):
        return True

    # Sempre agrega se tiver intervalo de datas explícito (período longo)
    if re.search(r'\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}\s+a[té]?\s+\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}', pl):
        return True

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

def is_chart_query(pergunta: str) -> bool:
    """Detecta se o usuário quer um gráfico."""
    pl = pergunta.lower()
    return any(x in pl for x in [
        'gráfico','grafico','chart','plotar','plot','visualiz',
        'evolução','evolucao','tendencia','tendência','barra','linha','pizza','pie'
    ])

# Paleta Frinense
COR_PRIMARIA  = '#c0392b'   # vermelho
COR_SECUNDARIA = '#f5c800'  # amarelo
COR_BG        = '#1a1a1a'
COR_GRID      = '#2e2e2e'
COR_TEXTO     = '#e0e0e0'
CORES_SERIES  = ['#c0392b','#f5c800','#e67e22','#27ae60','#2980b9','#8e44ad','#16a085','#d35400']

def _fig_to_base64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', facecolor=COR_BG, dpi=120)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')

def _setup_ax(ax, titulo: str = ""):
    ax.set_facecolor(COR_BG)
    ax.tick_params(colors=COR_TEXTO, labelsize=8)
    ax.xaxis.label.set_color(COR_TEXTO)
    ax.yaxis.label.set_color(COR_TEXTO)
    for spine in ax.spines.values():
        spine.set_edgecolor(COR_GRID)
    ax.grid(axis='y', color=COR_GRID, linewidth=0.5, linestyle='--')
    if titulo:
        ax.set_title(titulo, color=COR_TEXTO, fontsize=10, pad=8)

def gerar_grafico(dff: pd.DataFrame, pergunta: str) -> str:
    """
    Gera o gráfico mais adequado para a pergunta e retorna HTML com img base64.
    Retorna None se não conseguir gerar.
    """
    pl = pergunta.lower()
    if dff.empty:
        return None

    dff = dff.copy()
    dff['DATA_MOVTO'] = pd.to_datetime(dff['DATA_MOVTO'], errors='coerce')

    # ── Detecta dimensões pedidas ──
    por_tipo   = any(x in pl for x in ['tipo','carne','divisao','divisão','categoria'])
    por_filial = any(x in pl for x in ['filial','itap','bjesus','porc','trindade','unidade'])
    por_prod   = any(x in pl for x in ['produto','products'])
    por_vend   = any(x in pl for x in ['vendedor','representante'])
    por_cliente= any(x in pl for x in ['cliente'])
    usa_fat    = any(x in pl for x in ['faturamento','receita','valor','r$','financeiro'])
    metrica    = 'VALOR_LIQUIDO' if usa_fat else 'QTDE_PRI'
    label_met  = 'Faturamento (R$)' if usa_fat else 'Volume (kg)'

    # ── Detecta granularidade temporal ──
    span_dias = (dff['DATA_MOVTO'].max() - dff['DATA_MOVTO'].min()).days if len(dff) > 1 else 0
    agrupar_por_mes = span_dias > 45  # mais de 45 dias → agrupa por mês

    fig_title_extra = ""

    try:
        # === GRÁFICO 1: Evolução temporal por tipo de carne ===
        if por_tipo or (not por_filial and not por_prod and not por_vend and not por_cliente):
            col_dim = 'DESC_DIVISAO2' if 'DESC_DIVISAO2' in dff.columns else None

            if agrupar_por_mes:
                dff['periodo'] = dff['DATA_MOVTO'].dt.to_period('M').astype(str)
                label_x = 'Mês'
            else:
                dff['periodo'] = dff['DATA_MOVTO'].dt.strftime('%d/%m')
                label_x = 'Data'

            if col_dim and por_tipo:
                pivot = dff.groupby(['periodo', col_dim])[metrica].sum().unstack(fill_value=0)
                # Mantém só top 6 tipos por volume total
                top_cols = pivot.sum().nlargest(6).index
                pivot = pivot[top_cols]
                fig, ax = plt.subplots(figsize=(10, 5), facecolor=COR_BG)
                for i, col in enumerate(pivot.columns):
                    ax.plot(pivot.index, pivot[col], marker='o', markersize=4,
                            color=CORES_SERIES[i % len(CORES_SERIES)], linewidth=2, label=col)
                _setup_ax(ax, f"Evolução por Tipo de Carne — {label_met}")
                ax.set_xlabel(label_x, color=COR_TEXTO)
                ax.set_ylabel(label_met, color=COR_TEXTO)
                ax.legend(fontsize=7, facecolor=COR_BG, labelcolor=COR_TEXTO, loc='upper left')
                ax.yaxis.set_major_formatter(mticker.FuncFormatter(
                    lambda v,_: f'R${v/1e6:.1f}M' if usa_fat else f'{v/1e3:.0f}t'))
                plt.xticks(rotation=35, ha='right')
            else:
                # Sem dimensão extra — total por período
                serie = dff.groupby('periodo')[metrica].sum()
                fig, ax = plt.subplots(figsize=(10, 4), facecolor=COR_BG)
                bars = ax.bar(serie.index, serie.values, color=COR_PRIMARIA, width=0.6)
                # Destaca maior
                max_idx = serie.values.argmax()
                bars[max_idx].set_color(COR_SECUNDARIA)
                _setup_ax(ax, f"Evolução de {label_met}")
                ax.set_xlabel(label_x, color=COR_TEXTO)
                ax.set_ylabel(label_met, color=COR_TEXTO)
                ax.yaxis.set_major_formatter(mticker.FuncFormatter(
                    lambda v,_: f'R${v/1e6:.1f}M' if usa_fat else f'{v/1e3:.0f}t'))
                plt.xticks(rotation=35, ha='right')

        # === GRÁFICO 2: Ranking por filial ===
        elif por_filial:
            grupo = dff.groupby('NOME_FILIAL')[metrica].sum().sort_values(ascending=True)
            fig, ax = plt.subplots(figsize=(8, 4), facecolor=COR_BG)
            bars = ax.barh(grupo.index, grupo.values, color=CORES_SERIES[:len(grupo)])
            for bar, val in zip(bars, grupo.values):
                ax.text(val * 1.01, bar.get_y() + bar.get_height()/2,
                        f'R${val/1e6:.2f}M' if usa_fat else f'{val/1e3:.0f}t',
                        va='center', color=COR_TEXTO, fontsize=8)
            _setup_ax(ax, f"{label_met} por Filial")
            ax.set_xlabel(label_met, color=COR_TEXTO)

        # === GRÁFICO 3: Top produtos ===
        elif por_prod:
            top = dff.groupby('DESC_PRODUTO')[metrica].sum().nlargest(10).sort_values(ascending=True)
            fig, ax = plt.subplots(figsize=(9, 6), facecolor=COR_BG)
            ax.barh(top.index, top.values, color=COR_PRIMARIA)
            ax.barh(top.index[-1:], top.values[-1:], color=COR_SECUNDARIA)  # destaca top 1
            _setup_ax(ax, f"Top 10 Produtos — {label_met}")
            ax.set_xlabel(label_met, color=COR_TEXTO)
            ax.tick_params(axis='y', labelsize=7)
            ax.xaxis.set_major_formatter(mticker.FuncFormatter(
                lambda v,_: f'R${v/1e6:.1f}M' if usa_fat else f'{v/1e3:.0f}t'))

        # === GRÁFICO 4: Top vendedores ===
        elif por_vend:
            top = dff.groupby('NOM_VENDEDOR')[metrica].sum().nlargest(10).sort_values(ascending=True)
            fig, ax = plt.subplots(figsize=(9, 6), facecolor=COR_BG)
            ax.barh(top.index, top.values, color=COR_PRIMARIA)
            ax.barh(top.index[-1:], top.values[-1:], color=COR_SECUNDARIA)
            _setup_ax(ax, f"Top 10 Vendedores — {label_met}")
            ax.set_xlabel(label_met, color=COR_TEXTO)
            ax.tick_params(axis='y', labelsize=7)
            ax.xaxis.set_major_formatter(mticker.FuncFormatter(
                lambda v,_: f'R${v/1e6:.1f}M' if usa_fat else f'{v/1e3:.0f}t'))

        # === GRÁFICO 5: Top clientes ===
        elif por_cliente:
            top = dff.groupby('NOME_CLIENTE')[metrica].sum().nlargest(10).sort_values(ascending=True)
            fig, ax = plt.subplots(figsize=(10, 6), facecolor=COR_BG)
            ax.barh(top.index, top.values, color=COR_PRIMARIA)
            ax.barh(top.index[-1:], top.values[-1:], color=COR_SECUNDARIA)
            _setup_ax(ax, f"Top 10 Clientes — {label_met}")
            ax.set_xlabel(label_met, color=COR_TEXTO)
            ax.tick_params(axis='y', labelsize=7)
            ax.xaxis.set_major_formatter(mticker.FuncFormatter(
                lambda v,_: f'R${v/1e6:.1f}M' if usa_fat else f'{v/1e3:.0f}t'))

        else:
            return None

        img_b64 = _fig_to_base64(fig)
        return f'<img src="data:image/png;base64,{img_b64}" style="max-width:100%;border-radius:8px;margin-top:8px;" />'

    except Exception as e:
        return f"⚠️ Erro ao gerar gráfico: {e}"


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

    # Se a última mensagem não tem contexto temporal (ex: "1", "2", "sim", "ok", "resumo executivo"),
    # busca no histórico recente (últimas 6 mensagens) para encontrar o período da conversa
    def tem_contexto_temporal(texto: str) -> bool:
        pl = texto.lower()
        indicadores = ['janeiro','fevereiro','março','marco','abril','maio','junho','julho',
                       'agosto','setembro','outubro','novembro','dezembro','mês passado','mes passado',
                       'mês anterior','mes anterior','este mês','esse mês','este mes','esse mes',
                       'semana passada','semana anterior','esta semana','essa semana',
                       'ontem','hoje','trimestre','semestre','último mês','ultimo mes',
                       r'\d{1,2}/\d{2}/\d{2,4}']
        return any(i in pl for i in indicadores[:-1]) or bool(re.search(indicadores[-1], pl)) or bool(re.search(r'[uú]ltimos?\s+\d+\s+(m[eê]s|dia|semana)', pl))

    pergunta_para_filtro = ultima
    if not tem_contexto_temporal(ultima):
        # Varre histórico do mais recente para o mais antigo
        msgs_usuario = [m.content for m in reversed(req.messages) if m.role == "user"]
        for msg_anterior in msgs_usuario[1:4]:  # pula a atual, olha até 3 anteriores
            if tem_contexto_temporal(msg_anterior):
                pergunta_para_filtro = msg_anterior
                break

    try:
        df = load_df()
        dff = filter_for_chat(df, pergunta_para_filtro)
        n = len(dff)

        # ── GRÁFICO: intercepta antes de chamar Claude — zero tokens ──
        if is_chart_query(ultima):
            html_grafico = gerar_grafico(dff, ultima)
            if html_grafico:
                # Monta resposta simulando formato Claude
                d_min = dff['DATA_MOVTO'].min()
                d_max = dff['DATA_MOVTO'].max()
                periodo_str = ""
                if pd.notna(d_min) and pd.notna(d_max):
                    if d_min.date() == d_max.date():
                        periodo_str = d_min.strftime('%d/%m/%y')
                    else:
                        periodo_str = f"{d_min.strftime('%d/%m/%y')} a {d_max.strftime('%d/%m/%y')}"
                resposta_md = f"📊 **Gráfico gerado** — {periodo_str} ({n:,} registros)\n\n{html_grafico}"
                return JSONResponse({
                    "id": "grafico",
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "text", "text": resposta_md}],
                    "model": "local-matplotlib",
                    "stop_reason": "end_turn"
                })



        # Período real dos dados filtrados
        periodo_label = ""
        if n > 0 and 'DATA_MOVTO' in dff.columns:
            d_min = dff['DATA_MOVTO'].min()
            d_max = dff['DATA_MOVTO'].max()
            if pd.notna(d_min) and pd.notna(d_max):
                if d_min.date() == d_max.date():
                    periodo_label = f"Período: {d_min.strftime('%d/%m/%y')}"
                else:
                    periodo_label = f"Período disponível: {d_min.strftime('%d/%m/%y')} a {d_max.strftime('%d/%m/%y')}"

        # Para resumos com muitos dados, usa agregação em vez de CSV bruto
        if n > 1500 or is_summary_query(pergunta_para_filtro) or is_summary_query(ultima):
            sales_data = aggregate_for_summary(dff)
            data_label = f"DADOS AGREGADOS ({n} registros originais){' | ' + periodo_label if periodo_label else ''}"
        else:
            dff = dff.copy()
            dff['DATA_MOVTO'] = dff['DATA_MOVTO'].dt.strftime('%d/%m/%y')
            sales_data = dff.to_csv(sep=';', index=False)
            data_label = f"{n} registros{' | ' + periodo_label if periodo_label else ''}"
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
- Quando perguntado sobre um período (mês, trimestre, semestre, ano ou intervalo de datas), APRESENTE os dados disponíveis diretamente, SEM mencionar datas ou períodos que não existem no dataset. NUNCA diga frases como "não há dados de X a Y", "os registros estão limitados a", "não tenho dados para esse período" — simplesmente apresente o que existe. Se há dados de 06/03 a 11/03, comece direto: "## Vendas de março/2026" e liste os dados. O usuário já sabe o que pediu.
- Quando identificar que a pergunta envolve um período longo (mensal, trimestral, semestral ou anual) E o usuário não especificou o nível de detalhe, pergunte ao usuário qual o nível desejado, oferecendo opções claras: "1) Resumo executivo (totais por filial e top clientes) 2) Análise detalhada por dia 3) Ranking completo de produtos e vendedores 4) Comparativo entre filiais". Só processe após a confirmação — EXCETO se o usuário já deixou claro o que quer (ex: "quero um resumo", "me dê o ranking", "análise detalhada")
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
