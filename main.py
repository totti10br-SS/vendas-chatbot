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
app.add_middleware(CORSMiddleware,
    allow_origins=["https://web-production-91aff.up.railway.app"],
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"])

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

def filter_for_chat(df: pd.DataFrame, pergunta: str, ctx: dict = None) -> pd.DataFrame:
    """Filtra dados para o chat baseado na pergunta. ctx é um dict opcional onde avisos são escritos."""
    if ctx is None:
        ctx = {}
    pl = pergunta.lower()
    dff = df.copy()

    hoje = datetime.now()

    # ── Filtro por CNPJ raiz — roda PRIMEIRO, antes de qualquer filtro temporal ──
    # Detecta "cnpj 73849952" / "cnpj raiz 73849952" / "cnpj: 73.849.952/0001-34"
    m_cnpj_pre = re.search(r'cnpj\s*(?:raiz\s*)?[:\s]*(\d[\d\.\-\/]{7,17}\d|\d{8,14})', pl)
    if m_cnpj_pre and 'CPF_CGC' in dff.columns:
        digits = re.sub(r'\D', '', m_cnpj_pre.group(1))
        raiz = digits[:8]
        cnpj_col = dff['CPF_CGC'].astype(str).str.replace(r'\D', '', regex=True)
        mask_cnpj = cnpj_col.str.startswith(raiz)
        if mask_cnpj.sum() > 0:
            dff = dff[mask_cnpj]
            ctx['cnpj_filtrado'] = raiz
        else:
            ctx['aviso'] = f"⚠️ Nenhum cliente encontrado com CNPJ raiz {raiz}."

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
        return _finalize_filter(dff, pl, ctx, df)

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
        return _finalize_filter(dff, pl, ctx, df)

    elif len(meses_encontrados) == 1:
        _, mes_encontrado, ano_mes = meses_encontrados[0]
        ano_usar = ano_mes or ano_ref
        dff = dff[(dff['DATA_MOVTO'].dt.month == mes_encontrado) &
                  (dff['DATA_MOVTO'].dt.year == ano_usar)]
        return _finalize_filter(dff, pl, ctx, df)

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
        return _finalize_filter(dff, pl, ctx, df)

    # ── Semestre ──
    if 'semestre' in pl:
        if '1º sem' in pl or 'primeiro semestre' in pl:
            dff = dff[(dff['DATA_MOVTO'].dt.month <= 6) & (dff['DATA_MOVTO'].dt.year == ano_ref)]
        else:
            dff = dff[(dff['DATA_MOVTO'].dt.month >= 7) & (dff['DATA_MOVTO'].dt.year == ano_ref)]
        return _finalize_filter(dff, pl, ctx, df)

    # ── Ano inteiro ──
    if 'ano' in pl and anos:
        dff = dff[dff['DATA_MOVTO'].dt.year == ano_ref]
        return _finalize_filter(dff, pl, ctx, df)

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
        dff_ontem = dff[(dff['DATA_MOVTO'] >= pd.Timestamp(dia.date())) &
                        (dff['DATA_MOVTO'] < pd.Timestamp(hoje.date()))]
        if len(dff_ontem) > 0:
            dff = dff_ontem
        else:
            # Não há dados de ontem — usa o penúltimo dia com dados disponíveis
            dias_disp = sorted(dff['DATA_MOVTO'].dt.date.unique(), reverse=True)
            if len(dias_disp) >= 2:
                dia_ant = dias_disp[1]  # penúltimo dia (o último é o dia de referência)
            elif len(dias_disp) == 1:
                dia_ant = dias_disp[0]
            else:
                dia_ant = dia.date()
            dff = dff[dff['DATA_MOVTO'].dt.date == dia_ant]

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
        elif anos:
            # Fallback: ano explicito sem outro filtro temporal
            # Ex: 'vendedor FABIO POLI em 2025', 'clientes de 2025'
            dff = dff[dff['DATA_MOVTO'].dt.year == int(anos[0])]

    return _finalize_filter(dff, pl, ctx, df)


def _finalize_filter(dff: pd.DataFrame, pl: str, ctx: dict = None, df_orig: pd.DataFrame = None) -> pd.DataFrame:
    if ctx is None:
        ctx = {}
    if df_orig is None:
        df_orig = dff
    """Aplica filtros de filial/cliente/vendedor/produto e limita tamanho."""
    hoje = datetime.now()

    filiais = {'itap':'ITAP','bjesus':'BJESUS','porc':'PORC','trindade':'TRINDADE'}
    for key, val in filiais.items():
        if key in pl:
            dff = dff[dff['NOME_FILIAL'].str.upper() == val]
            break

    # ── Filtro por UF / Estado ──
    estados_map = {
        r'\bes\b': 'ES', r'\brj\b': 'RJ', r'\bsp\b': 'SP', r'\bmg\b': 'MG',
        r'\bba\b': 'BA', r'\bpr\b': 'PR', r'\bsc\b': 'SC', r'\brs\b': 'RS',
        r'\bgo\b': 'GO', r'\bdf\b': 'DF', r'\bms\b': 'MS', r'\bmt\b': 'MT',
        r'\bpa\b': 'PA', r'\bam\b': 'AM', r'\bce\b': 'CE', r'\bpe\b': 'PE',
        r'\bma\b': 'MA', r'\bpi\b': 'PI', r'\brn\b': 'RN', r'\bpb\b': 'PB',
        r'\bal\b': 'AL', r'\bse\b': 'SE', r'\bto\b': 'TO', r'\bro\b': 'RO',
        r'\bac\b': 'AC', r'\brr\b': 'RR', r'\bap\b': 'AP',
        r'esp[ii]rito\s+santo': 'ES',
        r'rio\s+de\s+janeiro': 'RJ',
        r's[ao]o\s+paulo': 'SP',
        r'minas\s+gerais': 'MG',
        r'bahia': 'BA',
        r'paran[a]': 'PR',
        r'santa\s+catarina': 'SC',
        r'rio\s+grande\s+do\s+sul': 'RS',
        r'goi[a]s': 'GO',
        r'distrito\s+federal': 'DF',
        r'mato\s+grosso\s+do\s+sul': 'MS',
        r'mato\s+grosso': 'MT',
        r'par[a]': 'PA',
        r'amazonas': 'AM',
        r'cear[a]': 'CE',
        r'pernambuco': 'PE',
        r'maranh[ao]o': 'MA',
        r'piau[i]': 'PI',
        r'rio\s+grande\s+do\s+norte': 'RN',
        r'para[i]ba': 'PB',
        r'alagoas': 'AL',
        r'sergipe': 'SE',
        r'tocantins': 'TO',
        r'rond[o]nia': 'RO',
        r'acre': 'AC',
        r'roraima': 'RR',
        r'amap[a]': 'AP',
    }
    if 'UF' in dff.columns:
        for padrao, uf in estados_map.items():
            if re.search(padrao, pl):
                dff = dff[dff['UF'].str.upper() == uf]
                break

    # Busca por código numérico de cliente: "cod cliente 18676" / "cliente cod 18676"
    m_cod_cli = re.search(r'(?:cod(?:igo)?[_\s]+cliente[_\s]+|cliente[_\s]+cod(?:igo)?[_\s]+)(\d{3,6})', pl)
    if m_cod_cli and 'COD_CLIENTE' in dff.columns:
        cod = m_cod_cli.group(1)
        mask_cod = dff['COD_CLIENTE'].astype(str).str.strip() == cod
        if mask_cod.sum() > 0:
            dff = dff[mask_cod]

    # ── Filtro por cliente ──
    def _melhor_cliente(dff, termo):
        """Retorna df filtrado pelo cliente mais parecido com o termo buscado."""
        mask = dff['NOME_CLIENTE'].str.lower().str.contains(termo, na=False)
        if mask.sum() == 0:
            return dff, False
        candidatos = dff[mask]['NOME_CLIENTE'].str.lower().unique()
        if len(candidatos) == 1:
            # Mesmo com 1 candidato, usa prefixo para pegar todas as variações do grupo
            prefixo = candidatos[0][:20].strip()
            mask_pref = dff['NOME_CLIENTE'].str.lower().str.contains(re.escape(prefixo), na=False)
            return dff[mask_pref], True

        def score(nome, termo):
            # Prioridade 1: nome começa com o termo (ex: "atakarejo distribuidor" começa com "atakarejo")
            if nome.startswith(termo):
                return 0
            palavras_nome = nome.split()
            # Prioridade 2: alguma palavra do nome é EXATAMENTE o termo
            if termo in palavras_nome:
                return 1
            # Prioridade 3: termo é a primeira palavra significativa do nome
            if palavras_nome and palavras_nome[0] == termo:
                return 2
            # Prioridade 4: distância de edição entre termo e a palavra mais parecida do nome
            def levenshtein(a, b):
                dp = list(range(len(b) + 1))
                for ca in a:
                    ndp = [dp[0] + 1]
                    for j, cb in enumerate(b):
                        ndp.append(min(dp[j] + (0 if ca == cb else 1), dp[j+1] + 1, ndp[j] + 1))
                    dp = ndp
                return dp[len(b)]
            min_dist = min(levenshtein(termo, p) for p in palavras_nome)
            # Penaliza nomes mais longos (termo "atakarejo" em "VALIM ATAKAREJO 1,5" deve perder)
            return 10 + min_dist + len(palavras_nome)

        melhor = min(candidatos, key=lambda c: score(c, termo))
        # Usa os primeiros 20 chars do melhor nome para capturar todas as variações do grupo
        # Ex: "ATAKAREJO DISTRIBUIDOR" pega todos os CNPJs diferentes do mesmo grupo
        prefixo = melhor[:20].strip()
        mask_melhor = dff['NOME_CLIENTE'].str.lower().str.contains(re.escape(prefixo), na=False)
        if mask_melhor.sum() == 0:
            # Fallback: match exato
            mask_melhor = dff['NOME_CLIENTE'].str.lower() == melhor
        return dff[mask_melhor], True

    # Prioridade 1: padrão explícito "cliente: NOME" ou "para o NOME" ou "do cliente NOME" ou "o cliente NOME"
    m_cliente = re.search(r'(?:cliente[:\s]+|para\s+(?:o|a)\s+|do\s+cliente\s+|o\s+cliente\s+)([a-záéíóúâêîôûãõç0-9\s]+)', pl)
    if m_cliente:
        # Corta em palavras de parada: preposições, ano, verbos comuns
        nome_cli = re.split(
            r'\s+(?:em|de|no|na|para|do|da|dos|das|nos|nas|com|\d{4}|'
            r'comprou|compra|compras|fez|teve|tem|faz|realizou|realizou|pediu|pedidos|'
            r'quanto|qual|quais|como|quando|onde|quanto)\b',
            m_cliente.group(1)
        )[0].strip()
        # Limpa artigos soltos no início
        nome_cli = re.sub(r'^(o|a|os|as|um|uma)\s+', '', nome_cli).strip()
        if len(nome_cli) > 2:
            dff_cli, achou = _melhor_cliente(dff, nome_cli)
            if achou:
                dff = dff_cli
            else:
                # Não achou no período — busca em todo o df (sem filtro de período) para sugerir
                mask_global = df_orig['NOME_CLIENTE'].str.lower().str.contains(nome_cli[:5], na=False)
                sugestoes = df_orig[mask_global]['NOME_CLIENTE'].unique()[:3].tolist() if mask_global.sum() > 0 else []
                if len(sugestoes) == 1:
                    # Só 1 candidato — usa automaticamente sem perguntar
                    mask_auto = dff['NOME_CLIENTE'].str.lower().str.contains(sugestoes[0][:10].lower(), na=False)
                    if mask_auto.sum() > 0:
                        dff = dff[mask_auto]
                    else:
                        # Candidato existe mas não no período filtrado
                        dff = dff.iloc[0:0].copy()
                        ctx['cliente_nao_encontrado'] = (nome_cli, sugestoes)
                else:
                    dff = dff.iloc[0:0].copy()
                    ctx['cliente_nao_encontrado'] = (nome_cli, sugestoes)
    else:
        # Prioridade 2: busca livre — palavras que não são stop-words comuns
        stopwords = {'últimas','ultimas','vendas','venda','quais','qual','como','foram','mais','este',
                     'essa','esse','para','pela','pelo','mês','mes','ano','2025','2026','2024','2023',
                     'analise','análise','resumo','faca','fazer','quero','cliente','clientes','filial',
                     'produto','vendedor','ranking','comparar','total','faça','faz','uma','uns','umas',
                     'dados','traz','traga','mostra','mostre','lista','liste','apresenta','analisa',
                     'sobre','com','sem','entre','desde','ate','até','ontem','hoje','semana','março',
                     'marco','janeiro','fevereiro','abril','maio','junho','julho','agosto','setembro',
                     'outubro','novembro','dezembro','trimestre','semestre','periodo','período'}
        nao_tem_filtro_especifico = not any(x in pl for x in [
            'produto:','vendedor:','filial:','ranking','comparar','top ','total geral'])
        if nao_tem_filtro_especifico:
            palavras = [p for p in pl.split() if len(p) > 3 and p not in stopwords]
            if palavras:
                achou = False
                for tam in [3, 2, 1]:
                    for i in range(len(palavras) - tam + 1):
                        termo = ' '.join(palavras[i:i+tam])
                        dff_cli, ok = _melhor_cliente(dff, termo)
                        if ok:
                            dff = dff_cli
                            achou = True
                            break
                    if achou:
                        break

    cols = ['NOME_FILIAL','DATA_MOVTO','NUM_DOCTO','COD_PRODUTO','DESC_PRODUTO','NOME_CLIENTE',
            'NOM_VENDEDOR','COD_VENDEDOR','QTDE_PRI','QTDE_AUX','VALOR_LIQUIDO','DESC_DIVISAO2','DESC_DIVISAO3','UF','CIDADE','CPF_CGC']

    if any(x in pl for x in ['últimas vendas','ultimas vendas','ultima venda','última venda']):
        dff = dff.sort_values('DATA_MOVTO', ascending=False).head(15)
        return dff[[c for c in cols if c in dff.columns]]

    # Filtro de vendedor — busca por palavras separadas (mais tolerante)
    m = re.search(r'vendedor[:\s]+([a-záéíóúâêîôûãõç\s]+)', pl)
    if m:
        nome_vend = re.split(r'\s+(?:em|de|no|na|para|do|da|nos|nas|\d{4})\b', m.group(1))[0].strip()
        if len(nome_vend) > 2:
            # Tenta primeiro match exato da string completa
            mask = dff['NOM_VENDEDOR'].str.lower().str.contains(nome_vend, na=False)
            if mask.sum() == 0:
                # Fallback: busca por cada palavra separada (todas devem estar presentes)
                palavras_vend = [p for p in nome_vend.split() if len(p) > 2]
                if palavras_vend:
                    mask = pd.Series([True] * len(dff), index=dff.index)
                    for palavra in palavras_vend:
                        mask = mask & dff['NOM_VENDEDOR'].str.lower().str.contains(palavra, na=False)
            if mask.sum() > 0:
                dff = dff[mask]
            else:
                # Nenhum resultado — marca para sugerir busca por código
                dff._iaf_vendedor_nao_encontrado = nome_vend
    else:
        m_cod = re.search(r'cod(?:igo)?[_\s]+(?:vendedor[_\s]+)?(\d{3,6})', pl)
        if m_cod:
            cod = m_cod.group(1)
            if 'COD_VENDEDOR' in dff.columns:
                dff_cod = dff[dff['COD_VENDEDOR'].astype(str).str.strip() == cod]
                if len(dff_cod) > 0:
                    dff = dff_cod

    m = re.search(r'produto[:\s]+([a-záéíóúâêîôûãõç\s]+)', pl)
    if m:
        nome_prod = re.split(r'\s+(?:em|de|no|na|para|do|da|\d{4})\b', m.group(1))[0].strip()
        if len(nome_prod) > 2:
            dff = dff[dff['DESC_PRODUTO'].str.lower().str.contains(nome_prod, na=False)]

    # ── Filtro por NR NOTA / NUM_DOCTO específico ──
    m_nota = re.search(r'\bnr?\s*(?:nota|docto|doc)?\s*[:\s#]?\s*(\d{3,8})\b', pl)
    if m_nota and 'NUM_DOCTO' in dff.columns:
        nr = m_nota.group(1)
        mask_nota = dff['NUM_DOCTO'].astype(str).str.strip() == nr
        if mask_nota.sum() > 0:
            dff = dff[mask_nota]

    # ── "última nota" / "ultimo pedido" — retorna os itens da nota mais recente ──
    if any(x in pl for x in ['última nota','ultima nota','último pedido','ultimo pedido','last nota']) and 'NUM_DOCTO' in dff.columns:
        if len(dff) > 0:
            ultima_data = dff['DATA_MOVTO'].max()
            dff_dia = dff[dff['DATA_MOVTO'] == ultima_data]
            # Pega o maior NUM_DOCTO do dia mais recente (ou o único)
            ultimo_nr = dff_dia['NUM_DOCTO'].max()
            dff = dff[dff['NUM_DOCTO'] == ultimo_nr]

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
    total_cx = dff['QTDE_AUX'].sum() if 'QTDE_AUX' in dff.columns else 0
    lines.append(f"Total: {total_kg:,.2f} kg | {total_cx:,.0f} cx | R$ {total_fat:,.2f} | {total_notas} notas | R$ {preco_medio:.2f}/kg")
    lines.append("")

    # Por filial
    lines.append("## POR FILIAL")
    agg_filial = {'kg':('QTDE_PRI','sum'), 'fat':('VALOR_LIQUIDO','sum'), 'notas':('NUM_DOCTO','nunique')}
    if 'QTDE_AUX' in dff.columns: agg_filial['cx'] = ('QTDE_AUX','sum')
    por_filial = dff.groupby('NOME_FILIAL').agg(**agg_filial).sort_values('kg', ascending=False)
    for idx, r in por_filial.iterrows():
        pm = r.fat/r.kg if r.kg > 0 else 0
        cx_str = f" | {r.cx:,.0f} cx" if 'cx' in por_filial.columns else ""
        lines.append(f"{idx}: {r.kg:,.2f} kg{cx_str} | R$ {r.fat:,.2f} | {r.notas} notas | R$ {pm:.2f}/kg")
    lines.append("")

    # Por dia
    lines.append("## POR DIA")
    por_dia = dff.groupby(dff['DATA_MOVTO'].dt.strftime('%d/%m/%y')).agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum'), notas=('NUM_DOCTO','nunique')).sort_index()
    for idx, r in por_dia.iterrows():
        pm = r.fat/r.kg if r.kg > 0 else 0
        lines.append(f"{idx}: {r.kg:,.2f} kg | R$ {r.fat:,.2f} | {r.notas} notas | R$ {pm:.2f}/kg")
    lines.append("")

    # Top 15 clientes
    lines.append("## TOP 10 CLIENTES (por volume)")
    agg_cli = {'kg':('QTDE_PRI','sum'), 'fat':('VALOR_LIQUIDO','sum')}
    if 'QTDE_AUX' in dff.columns: agg_cli['cx'] = ('QTDE_AUX','sum')
    por_cli = dff.groupby('NOME_CLIENTE').agg(**agg_cli).sort_values('kg', ascending=False).head(10)
    for idx, r in por_cli.iterrows():
        pm = r.fat/r.kg if r.kg > 0 else 0
        cx_str = f" | {r.cx:,.0f} cx" if 'cx' in por_cli.columns else ""
        lines.append(f"{idx}: {r.kg:,.2f} kg{cx_str} | R$ {r.fat:,.2f} | R$ {pm:.2f}/kg")
    lines.append("")

    # Top 15 produtos
    lines.append("## TOP 10 PRODUTOS (por volume)")
    agg_prod = {'kg':('QTDE_PRI','sum'), 'fat':('VALOR_LIQUIDO','sum')}
    if 'QTDE_AUX' in dff.columns: agg_prod['cx'] = ('QTDE_AUX','sum')
    por_prod = dff.groupby(['COD_PRODUTO','DESC_PRODUTO']).agg(**agg_prod).sort_values('kg', ascending=False).head(10)
    for idx, r in por_prod.iterrows():
        pm = r.fat/r.kg if r.kg > 0 else 0
        cx_str = f" | {r.cx:,.0f} cx" if 'cx' in por_prod.columns else ""
        lines.append(f"{idx[1]}: {r.kg:,.2f} kg{cx_str} | R$ {r.fat:,.2f} | R$ {pm:.2f}/kg")
    lines.append("")

    # Todos os vendedores (com código)
    lines.append("## VENDEDORES (por volume)")
    if 'COD_VENDEDOR' in dff.columns:
        agg_v = {'kg':('QTDE_PRI','sum'), 'fat':('VALOR_LIQUIDO','sum')}
        if 'QTDE_AUX' in dff.columns: agg_v['cx'] = ('QTDE_AUX','sum')
        por_vend = dff.groupby(['COD_VENDEDOR','NOM_VENDEDOR']).agg(**agg_v).sort_values('kg', ascending=False)
        for idx, r in por_vend.iterrows():
            pm = r.fat/r.kg if r.kg > 0 else 0
            cx_str = f" | {r.cx:,.0f} cx" if 'cx' in por_vend.columns else ""
            lines.append(f"COD {idx[0]} | {idx[1]}: {r.kg:,.2f} kg{cx_str} | R$ {r.fat:,.2f} | R$ {pm:.2f}/kg")
    else:
        agg_v = {'kg':('QTDE_PRI','sum'), 'fat':('VALOR_LIQUIDO','sum')}
        if 'QTDE_AUX' in dff.columns: agg_v['cx'] = ('QTDE_AUX','sum')
        por_vend = dff.groupby('NOM_VENDEDOR').agg(**agg_v).sort_values('kg', ascending=False)
        for idx, r in por_vend.iterrows():
            pm = r.fat/r.kg if r.kg > 0 else 0
            cx_str = f" | {r.cx:,.0f} cx" if 'cx' in por_vend.columns else ""
            lines.append(f"{idx}: {r.kg:,.2f} kg{cx_str} | R$ {r.fat:,.2f} | R$ {pm:.2f}/kg")
    lines.append("")

    # Por UF / Estado
    if 'UF' in dff.columns:
        lines.append("## POR ESTADO (UF)")
        agg_uf = {'kg':('QTDE_PRI','sum'), 'fat':('VALOR_LIQUIDO','sum'), 'notas':('NUM_DOCTO','nunique')}
        if 'QTDE_AUX' in dff.columns: agg_uf['cx'] = ('QTDE_AUX','sum')
        por_uf = dff.groupby('UF').agg(**agg_uf).sort_values('kg', ascending=False)
        for idx, r in por_uf.iterrows():
            pm = r.fat/r.kg if r.kg > 0 else 0
            cx_str = f" | {r.cx:,.0f} cx" if 'cx' in por_uf.columns else ""
            lines.append(f"{idx}: {r.kg:,.2f} kg{cx_str} | R$ {r.fat:,.2f} | {r.notas} notas | R$ {pm:.2f}/kg")
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
    # Não agregar se for busca específica de cliente/produto/nota/última nota
    specific_keywords = ['últimas vendas','ultimas vendas','ultima venda','última venda',
                         'nota ','nr ','última nota','ultima nota','último pedido','ultimo pedido',
                         'nr nota','nr_nota','numero da nota','número da nota']
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

        # Horário de modificação do arquivo no Google Drive
        csv_modificado_str = "—"
        try:
            service = get_drive_service()
            file_meta = service.files().get(
                fileId=FILE_ID,
                fields='modifiedTime'
            ).execute()
            modified_utc = file_meta.get('modifiedTime','')
            if modified_utc:
                from datetime import timezone
                dt_utc = datetime.strptime(modified_utc, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
                dt_local = dt_utc.astimezone()
                csv_modificado_str = dt_local.strftime('%d/%m/%Y %H:%M')
        except:
            pass

        dia_label = dia.strftime('%d/%m/%Y')

        return JSONResponse({
            "total_registros": total,
            "dia_label": dia_label,
            "fat": round(fat, 2),
            "kg":  round(kg, 2),
            "notas": notas,
            "ultima_nota": ultima_str,
            "csv_modificado": csv_modificado_str,
            "top10": top10,
            "tipos": tipos
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/cliente/{nome:path}")
def detalhe_cliente(nome: str):
    """Retorna produtos comprados pelo cliente no dia de referência."""
    try:
        df = load_df()
        dia = get_dia_referencia(df)
        df_dia = df[df['DATA_MOVTO'].dt.date == dia]
        # Busca tolerante: tenta match exato primeiro, depois contains
        mask_exato = df_dia['NOME_CLIENTE'].str.upper() == nome.upper()
        if mask_exato.sum() == 0:
            mask_exato = df_dia['NOME_CLIENTE'].str.upper().str.contains(nome.upper()[:20], na=False)
        df_cli = df_dia[mask_exato]

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

@app.get("/tipo/{tipo}")
def detalhe_tipo(tipo: str):
    """Retorna clientes e produtos do tipo de carne no dia de referência."""
    try:
        df = load_df()
        dia = get_dia_referencia(df)
        df_dia = df[df['DATA_MOVTO'].dt.date == dia]
        df_tipo = df_dia[df_dia['DESC_DIVISAO2'].str.upper() == tipo.upper()]

        fat_total = float(df_tipo['VALOR_LIQUIDO'].sum())
        kg_total  = float(df_tipo['QTDE_PRI'].sum())
        cx_total  = float(df_tipo['QTDE_AUX'].sum()) if 'QTDE_AUX' in df_tipo.columns else 0
        notas     = int(df_tipo['NUM_DOCTO'].nunique())
        pm        = round(fat_total / kg_total, 2) if kg_total > 0 else 0

        # Top clientes do tipo
        clientes = (df_tipo.groupby('NOME_CLIENTE')
                    .agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum'))
                    .sort_values('kg', ascending=False)
                    .head(10).reset_index())
        clientes_list = [{"nome": r.NOME_CLIENTE, "kg": round(r.kg,2), "fat": round(r.fat,2)}
                         for r in clientes.itertuples()]

        # Top produtos do tipo
        produtos = (df_tipo.groupby('DESC_PRODUTO')
                    .agg(kg=('QTDE_PRI','sum'), fat=('VALOR_LIQUIDO','sum'))
                    .sort_values('kg', ascending=False)
                    .head(10).reset_index())
        produtos_list = [{"nome": r.DESC_PRODUTO, "kg": round(r.kg,2), "fat": round(r.fat,2)}
                         for r in produtos.itertuples()]

        return JSONResponse({
            "tipo": tipo,
            "fat_total": round(fat_total,2),
            "kg_total":  round(kg_total,2),
            "cx_total":  round(cx_total,0),
            "notas":     notas,
            "pm":        pm,
            "clientes":  clientes_list,
            "produtos":  produtos_list
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

def is_pdf_query(pergunta: str) -> bool:
    """Detecta se o usuário quer exportar PDF."""
    pl = pergunta.lower()
    return any(x in pl for x in ['pdf','exportar','exporta','gerar relatório','gerar relatorio','imprimir','download'])

def is_pptx_query(pergunta: str) -> bool:
    """Detecta se o usuário quer uma apresentação PowerPoint."""
    pl = pergunta.lower()
    return any(x in pl for x in [
        'apresentação','apresentacao','powerpoint','pptx','ppt','slides','slide deck',
        'deck','apresentar para','apresentar à','apresentar a diretoria',
        'apresentar ao board','pitch','reunião de','reuniao de'
    ])

def gerar_pptx(conteudo_md: str) -> str:
    """Gera apresentação PowerPoint executiva com layout Frinense a partir de Markdown."""
    import subprocess, tempfile, os, re as _re

    # Paleta Frinense
    C_RED      = "C0392B"
    C_RED_DRK  = "7B241C"
    C_YELLOW   = "F5C800"
    C_BLACK    = "1A1A1A"
    C_DARK     = "1C2833"
    C_WHITE    = "FFFFFF"
    C_GRAY_L   = "F4F4F4"
    C_GRAY_M   = "888888"
    C_GRAY_D   = "444444"
    C_GREEN    = "1A6B3A"

    # Parseia Markdown em seções de slides
    slides_raw = []
    current = {"title": "", "subtitle": "", "items": [], "table": [], "kpis": []}
    linhas = conteudo_md.split('\n')

    for linha in linhas:
        l = linha.strip()
        if l.startswith('# ') and not l.startswith('## '):
            if current["title"]:
                slides_raw.append(dict(current))
            current = {"title": l[2:].strip(), "subtitle": "", "items": [], "table": [], "kpis": []}
        elif l.startswith('## '):
            if current["title"]:
                slides_raw.append(dict(current))
            current = {"title": l[3:].strip(), "subtitle": "", "items": [], "table": [], "kpis": []}
        elif l.startswith('### '):
            current["subtitle"] = l[4:].strip()
        elif l.startswith('| ') and '|' in l[1:]:
            if not _re.match(r'^\|[\s\-\|:]+\|$', l):
                cols = [c.strip() for c in l.strip('|').split('|')]
                current["table"].append(cols)
        elif l.startswith('- ') or l.startswith('* '):
            current["items"].append(l[2:].strip())
        elif l.startswith('**') and ':' in l:
            current["kpis"].append(l.strip('*').strip())
        elif l and not l.startswith('---'):
            if current["subtitle"] == "" and current["title"]:
                pass  # ignora texto solto por enquanto

    if current["title"]:
        slides_raw.append(dict(current))

    # Gera JS para pptxgenjs
    def esc(s):
        return s.replace('\\','\\\\').replace('"','\\"').replace('\n','\\n').replace('\r','')

    def strip_md(s):
        s = _re.sub(r'\*\*(.+?)\*\*', r'\1', s)
        s = _re.sub(r'\*(.+?)\*', r'\1', s)
        return s.strip()

    js_slides = []

    for idx, slide in enumerate(slides_raw):
        titulo = esc(strip_md(slide["title"]))
        subtitulo = esc(strip_md(slide["subtitle"]))
        items = [strip_md(x) for x in slide["items"]]
        table = [[strip_md(c) for c in row] for row in slide["table"]]
        kpis  = slide["kpis"]
        is_first = (idx == 0)
        is_last  = (idx == len(slides_raw) - 1)

        lines = [f'  // ── Slide {idx+1}: {titulo[:40]} ──']
        lines.append('  { const slide = pres.addSlide();')

        # ── CAPA ──
        if is_first:
            lines.append(f'  slide.background = {{ color: "{C_RED_DRK}" }};')
            # Faixa preta esquerda decorativa
            lines.append(f'  slide.addShape(pres.shapes.RECTANGLE, {{ x:0, y:0, w:0.18, h:5.625, fill:{{ color:"{C_BLACK}" }}, line:{{ color:"{C_BLACK}" }} }});')
            # Accent amarelo topo
            lines.append(f'  slide.addShape(pres.shapes.RECTANGLE, {{ x:0, y:0, w:10, h:0.07, fill:{{ color:"{C_YELLOW}" }}, line:{{ color:"{C_YELLOW}" }} }});')
            # Accent amarelo rodapé
            lines.append(f'  slide.addShape(pres.shapes.RECTANGLE, {{ x:0, y:5.55, w:10, h:0.075, fill:{{ color:"{C_YELLOW}" }}, line:{{ color:"{C_YELLOW}" }} }});')
            # Bloco título
            lines.append(f'  slide.addText("{titulo}", {{ x:0.5, y:1.5, w:9, h:1.6, fontSize:42, fontFace:"Calibri", bold:true, color:"{C_WHITE}", valign:"middle", margin:0 }});')
            if subtitulo:
                lines.append(f'  slide.addText("{subtitulo}", {{ x:0.5, y:3.2, w:8, h:0.6, fontSize:18, fontFace:"Calibri", color:"FFCCCC", valign:"top", margin:0 }});')
            # Data e empresa
            from datetime import datetime as _dt
            data_hoje = _dt.now().strftime('%d/%m/%Y')
            lines.append(f'  slide.addText("Frinense Alimentos  ·  {data_hoje}  ·  Uso Interno", {{ x:0.5, y:5.0, w:9, h:0.4, fontSize:10, fontFace:"Calibri", color:"FFAAAA", align:"left", margin:0 }});')

        # ── CONCLUSÃO / ÚLTIMO SLIDE ──
        elif is_last:
            lines.append(f'  slide.background = {{ color: "{C_DARK}" }};')
            lines.append(f'  slide.addShape(pres.shapes.RECTANGLE, {{ x:0, y:0, w:0.18, h:5.625, fill:{{ color:"{C_RED}" }}, line:{{ color:"{C_RED}" }} }});')
            lines.append(f'  slide.addShape(pres.shapes.RECTANGLE, {{ x:0, y:5.55, w:10, h:0.075, fill:{{ color:"{C_YELLOW}" }}, line:{{ color:"{C_YELLOW}" }} }});')
            lines.append(f'  slide.addText("{titulo}", {{ x:0.5, y:1.8, w:9, h:1.4, fontSize:36, fontFace:"Calibri", bold:true, color:"{C_WHITE}", valign:"middle", align:"center", margin:0 }});')
            if items:
                bullets = [{{ "text": esc(x), "options": {{ "bullet": True, "breakLine": True, "fontSize": 15, "color": "DDDDDD" }} }} for x in items]
                bullet_js = '[' + ','.join([f'{{ text: "{esc(x)}", options: {{ bullet: true, breakLine: true, fontSize: 15, color: "DDDDDD" }} }}' for x in items[:-1]] +
                            [f'{{ text: "{esc(items[-1])}", options: {{ bullet: true, fontSize: 15, color: "DDDDDD" }} }}']) + ']'
                lines.append(f'  slide.addText({bullet_js}, {{ x:1.5, y:3.2, w:7, h:1.8, fontFace:"Calibri", valign:"top", margin:0 }});')
            lines.append(f'  slide.addText("IAF · Analista Comercial · Frinense Alimentos", {{ x:0.5, y:5.1, w:9, h:0.35, fontSize:9, fontFace:"Calibri", color:"777777", align:"center", margin:0 }});')

        # ── SLIDES COM KPIs ──
        elif len(kpis) >= 2:
            lines.append(f'  slide.background = {{ color: "{C_GRAY_L}" }};')
            # Header vermelho
            lines.append(f'  slide.addShape(pres.shapes.RECTANGLE, {{ x:0, y:0, w:10, h:0.85, fill:{{ color:"{C_RED}" }}, line:{{ color:"{C_RED}" }} }});')
            lines.append(f'  slide.addShape(pres.shapes.RECTANGLE, {{ x:0, y:0.83, w:10, h:0.055, fill:{{ color:"{C_YELLOW}" }}, line:{{ color:"{C_YELLOW}" }} }});')
            lines.append(f'  slide.addText("{titulo}", {{ x:0.35, y:0.1, w:9.3, h:0.65, fontSize:22, fontFace:"Calibri", bold:true, color:"{C_WHITE}", valign:"middle", margin:0 }});')
            # KPI cards
            n_kpi = min(len(kpis), 4)
            kw = 9.0 / n_kpi
            for ki, kpi in enumerate(kpis[:n_kpi]):
                parts = kpi.split(':', 1)
                label = parts[0].strip() if len(parts) > 1 else f"KPI {ki+1}"
                valor = parts[1].strip() if len(parts) > 1 else parts[0].strip()
                kx = 0.4 + ki * kw
                lines.append(f'  slide.addShape(pres.shapes.RECTANGLE, {{ x:{kx:.2f}, y:1.1, w:{kw-0.12:.2f}, h:1.5, fill:{{ color:"{C_WHITE}" }}, line:{{ color:"E0E0E0", width:0.5 }}, shadow:{{ type:"outer", blur:4, offset:2, angle:135, color:"000000", opacity:0.08 }} }});')
                lines.append(f'  slide.addShape(pres.shapes.RECTANGLE, {{ x:{kx:.2f}, y:1.1, w:{kw-0.12:.2f}, h:0.1, fill:{{ color:"{C_RED}" }}, line:{{ color:"{C_RED}" }} }});')
                lines.append(f'  slide.addText("{esc(valor)}", {{ x:{kx:.2f}, y:1.25, w:{kw-0.12:.2f}, h:0.85, fontSize:26, fontFace:"Calibri", bold:true, color:"{C_RED}", align:"center", valign:"middle", margin:0 }});')
                lines.append(f'  slide.addText("{esc(label)}", {{ x:{kx:.2f}, y:2.1, w:{kw-0.12:.2f}, h:0.35, fontSize:10, fontFace:"Calibri", color:"{C_GRAY_M}", align:"center", margin:0 }});')
            # Bullets abaixo dos KPIs
            if items:
                bullet_js = '[' + ','.join([f'{{ text: "{esc(x)}", options: {{ bullet: true, breakLine: true, fontSize: 13, color: "{C_GRAY_D}" }} }}' for x in items[:-1]] +
                            [f'{{ text: "{esc(items[-1])}", options: {{ bullet: true, fontSize: 13, color: "{C_GRAY_D}" }} }}']) + ']'
                lines.append(f'  slide.addText({bullet_js}, {{ x:0.5, y:2.85, w:9, h:2.4, fontFace:"Calibri", valign:"top", margin:0 }});')

        # ── SLIDES COM TABELA ──
        elif table and len(table) >= 2:
            lines.append(f'  slide.background = {{ color: "{C_WHITE}" }};')
            lines.append(f'  slide.addShape(pres.shapes.RECTANGLE, {{ x:0, y:0, w:10, h:0.85, fill:{{ color:"{C_RED}" }}, line:{{ color:"{C_RED}" }} }});')
            lines.append(f'  slide.addShape(pres.shapes.RECTANGLE, {{ x:0, y:0.83, w:10, h:0.055, fill:{{ color:"{C_YELLOW}" }}, line:{{ color:"{C_YELLOW}" }} }});')
            lines.append(f'  slide.addText("{titulo}", {{ x:0.35, y:0.1, w:9.3, h:0.65, fontSize:22, fontFace:"Calibri", bold:true, color:"{C_WHITE}", valign:"middle", margin:0 }});')
            n_cols = max(len(r) for r in table)
            table_data = []
            for ri, row in enumerate(table[:16]):
                row_norm = (row + [''] * n_cols)[:n_cols]
                cells = []
                for ci, cell in enumerate(row_norm):
                    if ri == 0:
                        cells.append(f'{{ text: "{esc(cell)}", options: {{ bold: true, color: "{C_WHITE}", fill: {{ color: "{C_RED}" }}, align: "center", fontSize: 11 }} }}')
                    else:
                        bg = C_WHITE if ri % 2 == 1 else "F9F9F9"
                        align = "right" if ci > 0 else "left"
                        cells.append(f'{{ text: "{esc(cell)}", options: {{ color: "{C_GRAY_D}", fill: {{ color: "{bg}" }}, align: "{align}", fontSize: 10 }} }}')
                table_data.append('[' + ','.join(cells) + ']')
            col_w = round(9.0 / n_cols, 2)
            table_js = '[' + ','.join(table_data) + ']'
            lines.append(f'  slide.addTable({table_js}, {{ x:0.4, y:1.05, w:9.2, colW:[{",".join([str(col_w)]*n_cols)}], border:{{ pt:0.5, color:"E0E0E0" }}, rowH:0.38 }});')
            if subtitulo:
                lines.append(f'  slide.addText("{subtitulo}", {{ x:0.4, y:5.2, w:9.2, h:0.3, fontSize:9, fontFace:"Calibri", color:"{C_GRAY_M}", italic:true, margin:0 }});')

        # ── SLIDES COM BULLETS (padrão) ──
        else:
            lines.append(f'  slide.background = {{ color: "{C_WHITE}" }};')
            lines.append(f'  slide.addShape(pres.shapes.RECTANGLE, {{ x:0, y:0, w:10, h:0.85, fill:{{ color:"{C_RED}" }}, line:{{ color:"{C_RED}" }} }});')
            lines.append(f'  slide.addShape(pres.shapes.RECTANGLE, {{ x:0, y:0.83, w:10, h:0.055, fill:{{ color:"{C_YELLOW}" }}, line:{{ color:"{C_YELLOW}" }} }});')
            lines.append(f'  slide.addText("{titulo}", {{ x:0.35, y:0.1, w:9.3, h:0.65, fontSize:22, fontFace:"Calibri", bold:true, color:"{C_WHITE}", valign:"middle", margin:0 }});')
            if subtitulo:
                lines.append(f'  slide.addShape(pres.shapes.RECTANGLE, {{ x:0.4, y:0.98, w:9.2, h:0.4, fill:{{ color:"F4F4F4" }}, line:{{ color:"E0E0E0", width:0.5 }} }});')
                lines.append(f'  slide.addText("{subtitulo}", {{ x:0.5, y:0.98, w:9, h:0.4, fontSize:13, fontFace:"Calibri", color:"{C_GRAY_D}", italic:true, valign:"middle", margin:0 }});')
            if items:
                y_start = 1.55 if subtitulo else 1.1
                # Dois colunas se muitos items
                if len(items) > 5:
                    mid = (len(items) + 1) // 2
                    left_items  = items[:mid]
                    right_items = items[mid:]
                    for col_items, cx in [(left_items, 0.4), (right_items, 5.1)]:
                        if col_items:
                            bullet_js = '[' + ','.join([f'{{ text: "{esc(x)}", options: {{ bullet: true, breakLine: true, fontSize: 14, color: "{C_GRAY_D}" }} }}' for x in col_items[:-1]] +
                                        [f'{{ text: "{esc(col_items[-1])}", options: {{ bullet: true, fontSize: 14, color: "{C_GRAY_D}" }} }}']) + ']'
                            lines.append(f'  slide.addText({bullet_js}, {{ x:{cx}, y:{y_start:.2f}, w:4.5, h:{5.625-y_start-0.4:.2f}, fontFace:"Calibri", valign:"top", margin:0 }});')
                else:
                    bullet_js = '[' + ','.join([f'{{ text: "{esc(x)}", options: {{ bullet: true, breakLine: true, fontSize: 15, color: "{C_GRAY_D}", paraSpaceAfter: 6 }} }}' for x in items[:-1]] +
                                [f'{{ text: "{esc(items[-1])}", options: {{ bullet: true, fontSize: 15, color: "{C_GRAY_D}", paraSpaceAfter: 6 }} }}']) + ']'
                    lines.append(f'  slide.addText({bullet_js}, {{ x:0.5, y:{y_start:.2f}, w:9, h:{5.625-y_start-0.4:.2f}, fontFace:"Calibri", valign:"top", margin:0 }});')
            elif not table:
                lines.append(f'  slide.addText("Sem conteúdo disponível", {{ x:1, y:2.5, w:8, h:1, fontSize:16, color:"{C_GRAY_M}", align:"center", margin:0 }});')

        # Rodapé em todos os slides exceto capa
        if not is_first:
            lines.append(f'  slide.addShape(pres.shapes.RECTANGLE, {{ x:0, y:5.45, w:10, h:0.175, fill:{{ color:"F0F0F0" }}, line:{{ color:"E0E0E0", width:0.5 }} }});')
            lines.append(f'  slide.addText("IAF · Frinense Alimentos · Confidencial", {{ x:0.35, y:5.47, w:7, h:0.15, fontSize:7.5, fontFace:"Calibri", color:"{C_GRAY_M}", margin:0 }});')
            lines.append(f'  slide.addText("{idx+1}", {{ x:9.5, y:5.47, w:0.4, h:0.15, fontSize:7.5, fontFace:"Calibri", color:"{C_GRAY_M}", align:"right", margin:0 }});')

        lines.append('  }')
        js_slides.append('\n'.join(lines))

    js_code = f"""
const pptxgen = require("pptxgenjs");
const pres = new pptxgen();
pres.layout = 'LAYOUT_16x9';
pres.author = 'IAF - Frinense Alimentos';
pres.title = 'Relatorio de Vendas';

{chr(10).join(js_slides)}

pres.writeFile({{ fileName: "/tmp/iaf_apresentacao.pptx" }}).then(() => {{
  process.exit(0);
}}).catch(e => {{
  console.error(e);
  process.exit(1);
}});
"""

    try:
        # Escreve e executa o script JS
        js_path = '/tmp/gerar_pptx.js'
        with open(js_path, 'w') as f:
            f.write(js_code)

        result = subprocess.run(
            ['node', js_path],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            raise Exception(f"Node error: {result.stderr[:500]}")

        with open('/tmp/iaf_apresentacao.pptx', 'rb') as f:
            pptx_b64 = base64.b64encode(f.read()).decode('utf-8')

        from datetime import datetime as _dt
        nome = f"IAF_Apresentacao_{_dt.now().strftime('%d%m%Y_%H%M')}.pptx"
        html = (
            f'<div style="margin-top:8px;">'
            f'<a href="data:application/vnd.openxmlformats-officedocument.presentationml.presentation;base64,{pptx_b64}" '
            f'download="{nome}" '
            f'style="display:inline-flex;align-items:center;gap:6px;background:#1a5276;'
            f'color:#fff;padding:9px 18px;border-radius:6px;text-decoration:none;'
            f'font-family:Barlow Condensed,sans-serif;font-weight:700;font-size:13px;'
            f'letter-spacing:.5px;border:1px solid rgba(245,200,0,.4);">'
            f'&#8595; BAIXAR APRESENTAÇÃO (.pptx)</a>'
            f'<span style="color:rgba(255,255,255,.4);font-size:10px;margin-left:10px;">{nome}</span>'
            f'</div>'
        )
        return html

    except Exception as e:
        import traceback
        return f"Erro ao gerar PPTX: {e}\n{traceback.format_exc()}"

def gerar_pdf(historico_msgs: list) -> str:
    """Gera PDF executivo profissional com layout Frinense."""
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.lib.units import cm
        from reportlab.lib import colors
        from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                        Table, TableStyle, HRFlowable, KeepTogether)
        from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
        from reportlab.pdfgen import canvas as rl_canvas
        import re as _re

        ultima_resposta = ""
        for msg in reversed(historico_msgs):
            if msg.role == "assistant":
                ultima_resposta = msg.content
                break
        if not ultima_resposta:
            return None

        C_RED      = colors.HexColor('#c0392b')
        C_RED_DRK  = colors.HexColor('#7b241c')
        C_YELLOW   = colors.HexColor('#f5c800')
        C_BLACK    = colors.HexColor('#1a1a1a')
        C_DARK     = colors.HexColor('#2c3e50')
        C_GRAY_D   = colors.HexColor('#555555')
        C_GRAY_M   = colors.HexColor('#888888')
        C_GRAY_L   = colors.HexColor('#f4f4f4')
        C_GRAY_LN  = colors.HexColor('#e0e0e0')
        C_WHITE    = colors.white
        C_GREEN    = colors.HexColor('#1a6b3a')
        C_BLUE_DRK = colors.HexColor('#1a3a5c')

        PAGE_W, PAGE_H = A4
        ML = 1.8*cm
        CONTENT_W = PAGE_W - 2*ML

        def P(name, **kw):
            d = dict(fontName='Helvetica', fontSize=9, leading=13,
                     textColor=C_DARK, spaceAfter=2)
            d.update(kw)
            return ParagraphStyle(name, **d)

        sH1    = P('h1', fontSize=15, fontName='Helvetica-Bold', textColor=C_RED,
                   spaceBefore=10, spaceAfter=6, leading=19)
        sH2    = P('h2', fontSize=11, fontName='Helvetica-Bold', textColor=C_WHITE,
                   leading=14)
        sH3    = P('h3', fontSize=10, fontName='Helvetica-Bold', textColor=C_DARK,
                   spaceBefore=6, spaceAfter=2, leading=13)
        sBody  = P('bd', fontSize=9, leading=14, spaceAfter=3, alignment=TA_JUSTIFY)
        sBul   = P('bl', fontSize=9, leading=13, leftIndent=14, spaceAfter=3)
        sSmall = P('sm', fontSize=7.5, textColor=C_GRAY_M, leading=10)
        sKpiV  = P('kv', fontSize=16, fontName='Helvetica-Bold', textColor=C_RED,
                   leading=20, alignment=TA_CENTER)
        sKpiL  = P('kl', fontSize=7, textColor=C_GRAY_M, leading=9, alignment=TA_CENTER)
        sKpiS  = P('ks', fontSize=8, textColor=C_GRAY_D, leading=10, alignment=TA_CENTER)
        sTblH  = P('th', fontSize=8, fontName='Helvetica-Bold', textColor=C_WHITE,
                   alignment=TA_CENTER, leading=11)
        sTblD  = P('td', fontSize=8, textColor=C_DARK, alignment=TA_LEFT, leading=11)
        sTblDR = P('tr', fontSize=8, textColor=C_DARK, alignment=TA_RIGHT, leading=11)

        class FrinesseCanvas(rl_canvas.Canvas):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._saved_page_states = []
            def showPage(self):
                self._saved_page_states.append(dict(self.__dict__))
                self._startPage()
            def save(self):
                total = len(self._saved_page_states)
                for state in self._saved_page_states:
                    self.__dict__.update(state)
                    self._draw_page(total)
                    super().showPage()
                super().save()
            def _draw_page(self, total):
                self.saveState()
                pg = self._pageNumber
                # Header faixa vermelha
                self.setFillColor(C_RED)
                self.rect(0, PAGE_H - 1.7*cm, PAGE_W, 1.7*cm, fill=1, stroke=0)
                # Accent top amarelo
                self.setFillColor(C_YELLOW)
                self.rect(0, PAGE_H - 0.2*cm, PAGE_W, 0.2*cm, fill=1, stroke=0)
                # Coluna preta
                self.setFillColor(C_BLACK)
                self.rect(0, PAGE_H - 1.7*cm, 0.5*cm, 1.7*cm, fill=1, stroke=0)
                # Textos
                self.setFillColor(C_WHITE)
                self.setFont('Helvetica-Bold', 11)
                self.drawString(ML, PAGE_H - 1.05*cm, 'IAF  ANALISTA COMERCIAL')
                self.setFont('Helvetica', 8)
                self.setFillColor(colors.HexColor('#ffcccc'))
                self.drawString(ML, PAGE_H - 1.42*cm, 'Frinense Alimentos - Relatorio de Vendas')
                self.setFont('Helvetica', 7.5)
                self.setFillColor(colors.HexColor('#ffdddd'))
                data_str = datetime.now().strftime('%d/%m/%Y %H:%M')
                self.drawRightString(PAGE_W - ML, PAGE_H - 1.05*cm, data_str)
                self.setFillColor(C_YELLOW)
                self.setFont('Helvetica-Bold', 7.5)
                self.drawRightString(PAGE_W - ML, PAGE_H - 1.40*cm, f'Pagina {pg} de {total}')
                # Footer
                self.setFillColor(C_GRAY_L)
                self.rect(0, 0, PAGE_W, 0.85*cm, fill=1, stroke=0)
                self.setStrokeColor(C_GRAY_LN)
                self.setLineWidth(0.5)
                self.line(0, 0.85*cm, PAGE_W, 0.85*cm)
                self.setStrokeColor(C_RED)
                self.setLineWidth(2)
                self.line(0, 0.85*cm, 2.5*cm, 0.85*cm)
                self.setFont('Helvetica', 7)
                self.setFillColor(C_GRAY_M)
                self.drawCentredString(PAGE_W/2, 0.3*cm,
                    'IAF  Frinense Alimentos  Documento gerado automaticamente  Uso interno')
                self.restoreState()

        buf = io.BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=A4,
            leftMargin=ML, rightMargin=ML,
            topMargin=2.0*cm, bottomMargin=1.1*cm,
            title="IAF - Relatorio de Vendas Frinense")

        story = []
        kpi_buffer = []

        def flush_kpis():
            if not kpi_buffer: return
            n = len(kpi_buffer)
            col_w = CONTENT_W / n
            rows = [
                [Paragraph(k[0], sKpiL) for k in kpi_buffer],
                [Paragraph(k[1], sKpiV) for k in kpi_buffer],
                [Paragraph(k[2], sKpiS) for k in kpi_buffer],
            ]
            t = Table(rows, colWidths=[col_w]*n)
            t.setStyle(TableStyle([
                ('BACKGROUND',    (0,0), (-1,0), C_GRAY_L),
                ('BACKGROUND',    (0,1), (-1,1), C_WHITE),
                ('BACKGROUND',    (0,2), (-1,2), C_GRAY_L),
                ('BOX',           (0,0), (-1,-1), 0.5, C_GRAY_LN),
                ('LINEBEFORE',    (1,0), (-1,-1), 0.5, C_GRAY_LN),
                ('LINEBELOW',     (0,0), (-1,0), 0.5, C_GRAY_LN),
                ('LINEBELOW',     (0,1), (-1,1), 0.5, C_GRAY_LN),
                ('ALIGN',         (0,0), (-1,-1), 'CENTER'),
                ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
                ('TOPPADDING',    (0,0), (-1,0), 5),
                ('BOTTOMPADDING', (0,0), (-1,0), 4),
                ('TOPPADDING',    (0,1), (-1,1), 8),
                ('BOTTOMPADDING', (0,1), (-1,1), 8),
                ('TOPPADDING',    (0,2), (-1,2), 4),
                ('BOTTOMPADDING', (0,2), (-1,2), 5),
            ]))
            story.append(KeepTogether([t, Spacer(1, 10)]))
            kpi_buffer.clear()

        def section_bar(txt, bg=None):
            bg = bg or C_RED
            bar = Table([[Paragraph(txt, sH2)]], colWidths=[CONTENT_W])
            bar.setStyle(TableStyle([
                ('BACKGROUND',    (0,0), (-1,-1), bg),
                ('LEFTPADDING',   (0,0), (-1,-1), 10),
                ('RIGHTPADDING',  (0,0), (-1,-1), 6),
                ('TOPPADDING',    (0,0), (-1,-1), 7),
                ('BOTTOMPADDING', (0,0), (-1,-1), 7),
            ]))
            story.append(Spacer(1, 9))
            story.append(bar)
            story.append(Spacer(1, 5))

        def sub_bar(txt):
            bar = Table([[Paragraph(txt, sH3)]], colWidths=[CONTENT_W])
            bar.setStyle(TableStyle([
                ('LINEBELOW',     (0,0), (-1,-1), 1, C_RED),
                ('LEFTPADDING',   (0,0), (-1,-1), 2),
                ('TOPPADDING',    (0,0), (-1,-1), 4),
                ('BOTTOMPADDING', (0,0), (-1,-1), 4),
            ]))
            story.append(Spacer(1, 5))
            story.append(bar)
            story.append(Spacer(1, 3))

        EMOJI_MAP = {
            '\U0001f4ca':'[Graf]','\U0001f4c8':'[Alta]','\U0001f4c9':'[Baixa]',
            '\u2705':'[OK]','\u274c':'[X]','\u26a0\ufe0f':'[!]','\u26a0':'[!]',
            '\U0001f4a1':'[Insight]','\U0001f534':'[*]','\U0001f7e1':'[*]',
            '\U0001f7e2':'[*]','\u2b50':'[*]','\U0001f3c6':'[Top]',
            '\U0001f4e6':'[Prod]','\U0001f464':'[Vend]','\U0001f4b0':'[R$]',
            '\U0001f3af':'[Alvo]','\U0001f4c5':'[Data]','\U0001f3ed':'[Fil]',
            '\u25a0':'[+]','\u25a0\u25a0':'[-]',
        }

        def fmt_md(txt):
            for e, s in EMOJI_MAP.items():
                txt = txt.replace(e, s)
            txt = txt.encode('latin-1', 'replace').decode('latin-1')
            txt = _re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', txt)
            txt = _re.sub(r'\*(.+?)\*', r'<i>\1</i>', txt)
            return txt

        # Capa
        linhas = ultima_resposta.split('\n')
        titulo_doc = "Relatorio de Vendas"
        idx_start = 0
        for ii, ll in enumerate(linhas[:6]):
            ll2 = ll.strip()
            if ll2.startswith('# ') and not ll2.startswith('## '):
                titulo_doc = fmt_md(ll2[2:].strip())
                idx_start = ii + 1; break
            elif ll2.startswith('## '):
                titulo_doc = fmt_md(ll2[3:].strip())
                idx_start = ii + 1; break

        capa = Table([[Paragraph(titulo_doc,
                        P('cap', fontSize=17, fontName='Helvetica-Bold', textColor=C_WHITE,
                          leading=21, alignment=TA_LEFT))]],
                     colWidths=[CONTENT_W])
        capa.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), C_RED_DRK),
            ('LEFTPADDING',   (0,0), (-1,-1), 14),
            ('RIGHTPADDING',  (0,0), (-1,-1), 14),
            ('TOPPADDING',    (0,0), (-1,-1), 14),
            ('BOTTOMPADDING', (0,0), (-1,-1), 14),
            ('LINEBELOW',     (0,0), (-1,-1), 3, C_YELLOW),
        ]))
        story.append(capa)
        story.append(Spacer(1, 10))

        i = idx_start
        while i < len(linhas):
            linha = linhas[i].strip()

            if '<img ' in linha or linha.startswith('<div') or linha.startswith('<a '):
                i += 1; continue

            if linha.startswith('# ') and not linha.startswith('## '):
                flush_kpis()
                story.append(Paragraph(fmt_md(linha[2:]), sH1))

            elif linha.startswith('## '):
                flush_kpis()
                txt = linha[3:].strip()
                kw_neg = ['fraco','oportunidade','risco','critico','falha','dependencia','queda','baixo']
                kw_pos = ['forte','destaque','top','melhor','sucesso','alto','crescimento']
                if any(x in txt.lower() for x in kw_neg):
                    section_bar(fmt_md(txt), bg=C_BLUE_DRK)
                elif any(x in txt.lower() for x in kw_pos):
                    section_bar(fmt_md(txt), bg=C_GREEN)
                else:
                    section_bar(fmt_md(txt), bg=C_RED)

            elif linha.startswith('### '):
                flush_kpis()
                sub_bar(fmt_md(linha[4:]))

            elif _re.match(r'^-{3,}$', linha):
                flush_kpis()
                story.append(Spacer(1, 6))
                story.append(HRFlowable(width="100%", thickness=1, color=C_GRAY_LN, spaceAfter=6))

            elif linha.startswith('|') and '|' in linha[1:]:
                flush_kpis()
                tab_linhas = []
                while i < len(linhas) and linhas[i].strip().startswith('|'):
                    l = linhas[i].strip()
                    if not _re.match(r'^\|[\s\-\|:]+\|$', l):
                        cols = [fmt_md(c.strip()) for c in l.strip('|').split('|')]
                        tab_linhas.append(cols)
                    i += 1
                if tab_linhas:
                    nc = max(len(r) for r in tab_linhas)
                    tab_linhas = [r + ['']*(nc - len(r)) for r in tab_linhas]
                    if nc == 2:
                        cws = [CONTENT_W*0.55, CONTENT_W*0.45]
                    elif nc == 3:
                        cws = [CONTENT_W*0.44, CONTENT_W*0.29, CONTENT_W*0.27]
                    elif nc == 4:
                        cws = [CONTENT_W*0.37, CONTENT_W*0.22, CONTENT_W*0.22, CONTENT_W*0.19]
                    else:
                        cws = [CONTENT_W/nc]*nc
                    rows_p = []
                    for ri, row in enumerate(tab_linhas):
                        if ri == 0:
                            rows_p.append([Paragraph(c, sTblH) for c in row])
                        else:
                            fmted = []
                            for ci, c in enumerate(row):
                                is_num = bool(_re.match(r'^[\d\-R\$%\+]', c.strip()))
                                fmted.append(Paragraph(c, sTblDR if (is_num and ci > 0) else sTblD))
                            rows_p.append(fmted)
                    t = Table(rows_p, colWidths=cws, repeatRows=1)
                    t.setStyle(TableStyle([
                        ('BACKGROUND',    (0,0), (-1,0), C_RED),
                        ('ROWBACKGROUNDS',(0,1), (-1,-1), [C_WHITE, C_GRAY_L]),
                        ('GRID',          (0,0), (-1,-1), 0.3, C_GRAY_LN),
                        ('LINEBELOW',     (0,0), (-1,0), 2, C_YELLOW),
                        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
                        ('TOPPADDING',    (0,0), (-1,-1), 5),
                        ('BOTTOMPADDING', (0,0), (-1,-1), 5),
                        ('LEFTPADDING',   (0,0), (-1,-1), 7),
                        ('RIGHTPADDING',  (0,0), (-1,-1), 7),
                    ]))
                    story.append(KeepTogether([t, Spacer(1, 8)]))
                continue

            elif linha.startswith('- ') or linha.startswith('* ') or linha.startswith('- '):
                flush_kpis()
                txt = fmt_md(linha[2:].strip())
                icon = '<font color="#c0392b">&#9658;</font>' if txt.startswith('<b>') else '<font color="#aaaaaa">&#8226;</font>'
                story.append(Paragraph(f'{icon}  {txt}', sBul))

            elif linha == '':
                story.append(Spacer(1, 4))

            elif linha:
                flush_kpis()
                story.append(Paragraph(fmt_md(linha), sBody))

            i += 1

        flush_kpis()

        story.append(Spacer(1, 16))
        story.append(HRFlowable(width="100%", thickness=0.5, color=C_GRAY_LN))
        story.append(Spacer(1, 4))
        story.append(Paragraph(
            f'Documento gerado pelo IAF em {datetime.now().strftime("%d/%m/%Y as %H:%M")}  |  Frinense Alimentos',
            sSmall))

        doc.build(story, canvasmaker=FrinesseCanvas)
        buf.seek(0)
        pdf_b64 = base64.b64encode(buf.read()).decode('utf-8')

        nome = f"IAF_Relatorio_{datetime.now().strftime('%d%m%Y_%H%M')}.pdf"
        html = (
            f'<div style="margin-top:8px;">'
            f'<a href="data:application/pdf;base64,{pdf_b64}" download="{nome}" '
            f'style="display:inline-flex;align-items:center;gap:6px;background:#c0392b;'
            f'color:#fff;padding:9px 18px;border-radius:6px;text-decoration:none;'
            f'font-family:Barlow Condensed,sans-serif;font-weight:700;font-size:13px;'
            f'letter-spacing:.5px;border:1px solid rgba(245,200,0,.4);">'
            f'&#8595; BAIXAR PDF</a>'
            f'<span style="color:rgba(255,255,255,.4);font-size:10px;margin-left:10px;">{nome}</span>'
            f'</div>'
        )
        return html

    except Exception as e:
        import traceback
        return f"Erro ao gerar PDF: {e}\n{traceback.format_exc()}"


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
        return (any(i in pl for i in indicadores[:-1])
                or bool(re.search(indicadores[-1], pl))
                or bool(re.search(r'[uú]ltimos?\s+\d+\s+(m[eê]s|dia|semana)', pl))
                or bool(re.search(r'\b202[0-9]\b', pl)))  # ano explícito: 2025, 2026...

    pergunta_para_filtro = ultima
    if not tem_contexto_temporal(ultima):
        # Varre histórico do mais recente para o mais antigo
        msgs_usuario = [m.content for m in reversed(req.messages) if m.role == "user"]
        for msg_anterior in msgs_usuario[1:4]:  # pula a atual, olha até 3 anteriores
            if tem_contexto_temporal(msg_anterior):
                # Combina: período da mensagem anterior + filtros específicos da atual (CNPJ, cliente, etc.)
                pergunta_para_filtro = msg_anterior + " " + ultima
                break

    # ── Se a pergunta atual menciona "dessa nota" sem número, tenta extrair NR NOTA do histórico ──
    pl_ult = ultima.lower()
    tem_ref_nota = any(x in pl_ult for x in ['dessa nota','desta nota','detalhe da nota','detalhes da nota',
                                               'detalhes dessa','detalhe dessa','itens da nota','itens dessa'])
    nr_nota_historico = None
    if tem_ref_nota and not re.search(r'\d{4,8}', ultima):
        # Procura número de nota em todas as mensagens (usuário e assistente)
        for m in reversed(req.messages[:-1]):
            match = re.search(r'\b(?:nr\.?\s*(?:nota\s*)?|nota\s*(?:fiscal\s*)?|num\.?\s*docto\s*)(\d{4,8})\b', m.content, re.IGNORECASE)
            if not match:
                # Tenta pegar qualquer número de 5-6 dígitos que pareça NR NOTA
                match = re.search(r'\b(1[0-9]{4,5}|[2-9][0-9]{4,5})\b', m.content)
            if match:
                nr_nota_historico = match.group(1)
                break
    if nr_nota_historico:
        pergunta_para_filtro = f"nota {nr_nota_historico} {pergunta_para_filtro}"

    try:
        df = load_df()
        import logging
        anos_no_df = sorted(df['DATA_MOVTO'].dt.year.dropna().unique().tolist()) if 'DATA_MOVTO' in df.columns else []
        logging.warning(f"[IAF DEBUG] ultima={ultima!r} | pergunta_para_filtro={pergunta_para_filtro!r} | df total={len(df)} | anos_no_df={anos_no_df}")
        # Log vendedores disponíveis em 2025
        if '2025' in pergunta_para_filtro:
            df2025 = df[df['DATA_MOVTO'].dt.year == 2025]
            vends = sorted(df2025['NOM_VENDEDOR'].dropna().unique().tolist()) if 'NOM_VENDEDOR' in df2025.columns else []
            logging.warning(f"[IAF DEBUG] vendedores em 2025 ({len(df2025)} regs): {vends[:20]}")
        ctx_filtro = {}
        dff = filter_for_chat(df, pergunta_para_filtro, ctx_filtro)
        import re as _re2
        anos_debug = _re2.findall(r'\b(202[0-9])\b', pergunta_para_filtro)
        logging.warning(f"[IAF DEBUG] dff apos filter_for_chat={len(dff)} | anos_detectados={anos_debug}")
        # Log CPF_CGC para diagnóstico de busca por CNPJ
        if 'cnpj' in pergunta_para_filtro.lower() and 'CPF_CGC' in df.columns:
            amostras = df['CPF_CGC'].dropna().astype(str).unique()[:5].tolist()
            logging.warning(f"[IAF DEBUG] CPF_CGC amostras={amostras}")
            # Busca específica pela raiz mencionada
            m_raiz_log = re.search(r'cnpj\s*(?:raiz\s*)?[:\s]*(\d{8,14})', pergunta_para_filtro.lower())
            if m_raiz_log:
                raiz_log = re.sub(r'\D','',m_raiz_log.group(1))[:8]
                col_limpa = df['CPF_CGC'].astype(str).str.replace(r'\D','',regex=True)
                encontrados = df[col_limpa.str.startswith(raiz_log)]['NOME_CLIENTE'].unique()[:3].tolist()
                logging.warning(f"[IAF DEBUG] raiz={raiz_log} → clientes encontrados no df TOTAL: {encontrados}")
        n = len(dff)

        # ── PDF / PPTX: flags para processar após Claude gerar o conteúdo ──
        gerar_pdf_apos_claude  = is_pdf_query(ultima)
        gerar_pptx_apos_claude = is_pptx_query(ultima) and not gerar_pdf_apos_claude

        # ── CONSULTA DE CNPJ: intercepta antes de chamar Claude — zero tokens ──
        m_cnpj_query = re.search(
            r'(?:qual|me\s+(?:diz|fala|passa|mostra?)|cnpj|cpf.?cgc)\b.{0,30}'
            r'(?:cnpj|cpf.?cgc|raiz).{0,20}(?:cliente[:\s]+|do\s+|da\s+|de\s+)?'
            r'([a-záéíóúâêîôûãõç0-9\s]{3,40})',
            ultima.lower()
        )
        # Também detecta padrão simples: "qual cnpj raiz do atakarejo"
        m_cnpj_simple = re.search(
            r'cnpj\s*(?:raiz\s*)?(?:do?|da)?\s*([a-záéíóúâêîôûãõç][a-záéíóúâêîôûãõç0-9\s]{2,30})',
            ultima.lower()
        )
        nome_para_cnpj = None
        if m_cnpj_simple:
            nome_para_cnpj = m_cnpj_simple.group(1).strip()
        elif m_cnpj_query:
            nome_para_cnpj = m_cnpj_query.group(1).strip()

        if nome_para_cnpj and 'CPF_CGC' in dff.columns and 'NOME_CLIENTE' in dff.columns:
            # Busca o cliente pelo nome
            mask_cli = df['NOME_CLIENTE'].str.lower().str.contains(nome_para_cnpj[:8], na=False)
            if mask_cli.sum() > 0:
                rows = df[mask_cli][['NOME_CLIENTE','CPF_CGC']].dropna()
                rows['raiz'] = rows['CPF_CGC'].astype(str).str.replace(r'\D','',regex=True).str[:8]
                raizes = rows.groupby('raiz')['NOME_CLIENTE'].first().reset_index()
                linhas = []
                for _, r in raizes.iterrows():
                    linhas.append(f"**{r['NOME_CLIENTE']}** — CNPJ raiz: `{r['raiz']}`")
                resposta_md = "\n".join(linhas) if linhas else f"Nenhum cliente encontrado com '{nome_para_cnpj}'."
                return JSONResponse({
                    "id": "cnpj_query",
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "text", "text": resposta_md}],
                    "model": "local-lookup",
                    "stop_reason": "end_turn"
                })

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
        # Verifica se vendedor não foi encontrado
        vendedor_nao_encontrado = getattr(dff, '_iaf_vendedor_nao_encontrado', None)
        aviso_extra = ""
        if vendedor_nao_encontrado or (n == 0 and re.search(r'vendedor', pergunta_para_filtro.lower())):
            nome_buscado = vendedor_nao_encontrado or "informado"
            try:
                df_periodo = filter_for_chat(df, pergunta_para_filtro.lower().replace('vendedor','').strip())
                vends_disp = df_periodo['NOM_VENDEDOR'].dropna().value_counts().head(5).index.tolist() if 'NOM_VENDEDOR' in df_periodo.columns else []
                vends_str = " | ".join(vends_disp) if vends_disp else "nenhum"
            except:
                vends_str = "indisponível"
            aviso_extra = f" | ⚠️ VENDEDOR '{nome_buscado.upper()}' NÃO LOCALIZADO — sugerir busca por código (ex: 'vendedor cod XXXX') | Vendedores disponíveis no período: {vends_str}"

        # Verifica se cliente não foi encontrado (flag escrita no ctx pelo filter_for_chat)
        cli_nao_encontrado = ctx_filtro.get('cliente_nao_encontrado')
        if cli_nao_encontrado:
            nome_cli_buscado, sugestoes_cli = cli_nao_encontrado
            sug_str = " | ".join(sugestoes_cli) if sugestoes_cli else "nenhum encontrado no período"
            aviso_extra += f" | ⚠️ CLIENTE '{nome_cli_buscado.upper()}' NÃO ENCONTRADO NO PERÍODO — informe ao usuário e sugira: {sug_str}"

        # Aviso genérico do ctx (ex: CNPJ sem coluna)
        if ctx_filtro.get('aviso'):
            aviso_extra += f" | {ctx_filtro['aviso']}"

        is_nota_query = any(x in pergunta_para_filtro.lower() for x in [
            'última nota','ultima nota','último pedido','ultimo pedido',
            'nota ','nr nota','nr_nota','numero da nota','número da nota',
            'dessa nota','desta nota','detalhe da nota','detalhes da nota',
            'detalhes dessa','detalhe dessa','itens da nota','itens dessa nota'
        ]) or bool(re.search(r'\bnr?\s*(?:nota|docto|doc)?\s*[:\s#]?\s*\d{3,8}\b', pergunta_para_filtro.lower()))

        if (n > 1500 or is_summary_query(pergunta_para_filtro) or is_summary_query(ultima)) and not is_nota_query:
            sales_data = aggregate_for_summary(dff)
            data_label = f"DADOS AGREGADOS ({n} registros originais){' | ' + periodo_label if periodo_label else ''}{aviso_extra}"
        else:
            dff = dff.copy()
            dff['DATA_MOVTO'] = dff['DATA_MOVTO'].dt.strftime('%d/%m/%y')
            sales_data = dff.to_csv(sep=';', index=False)
            data_label = f"{n} registros{' | ' + periodo_label if periodo_label else ''}{aviso_extra}"
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

## IDENTIDADE E POSTURA
- Especialista em indicadores comerciais, foco em volume de vendas (kg)
- Tom executivo e direto — sem rodeios, sem introduções longas, sem enrolação
- Prioriza volume (kg) antes de valor financeiro
- Nunca inventa dados. Se não tiver dados suficientes, diz claramente
- Raciocínio de gerência comercial: sempre conecta dados com decisão de negócio

## FORMATO PADRÃO DE RESPOSTAS
- Perguntas simples (1 dado, 1 métrica) → resposta CURTA, máximo 3-4 linhas
- Análises (período, produto, cliente) → estrutura: Resumo 2 linhas → Tabela/Ranking → Insight
- Análises completas (solicitadas explicitamente) → estrutura completa com seções ## 
- NUNCA comece com "Olá", "Claro!", "Com prazer" — vá direto ao dado
- Use Markdown: ## títulos, **negrito**, tabelas com | Col |
- Valores: R$ X.XXX,XX | Quantidades: X.XXX,XX kg | Datas: DD/MM/AA

## COMPARATIVO AUTOMÁTICO
- Sempre que apresentar dados de um período, calcule e exiba a variação vs. período anterior equivalente
- Ex: hoje vs ontem, semana atual vs semana anterior, mês atual vs mês anterior
- Formato: "29.324 kg (+12% vs ontem)" ou "R$ 1,06M (-8% vs semana passada)"
- Se não houver dados do período anterior, omita a variação silenciosamente

## ALERTAS PROATIVOS
- Se detectar anomalia nos dados (queda/alta acima de 20%, cliente sem compra há mais de 7 dias, produto zerado, filial abaixo da média), mencione no insight mesmo sem ser perguntado
- Formato: "⚠️ Atenção: [anomalia detectada]"

## MÉTRICAS OBRIGATÓRIAS
- PREÇO MÉDIO (R$/kg): calcule como VALOR_LIQUIDO / QTDE_PRI em toda análise de produto, cliente ou vendedor
- CAIXAS: sempre exiba QTDE_AUX (cx) junto com kg. Ex: '29.324 kg | 2.324 cx'
- Filiais: ITAP (Itaperuna), BJESUS (Bom Jesus), PORC (Porciúncula), TRINDADE (Trindade)

## INSIGHT FINAL
- Finalize SEMPRE com 1 insight executivo precedido de "💡 Insight:" em linha separada
- Seja específico: cite dia (DD/MM/AA), NR NOTA, produto ou cliente quando relevante
- O insight deve sugerir uma AÇÃO ou destacar uma OPORTUNIDADE, não apenas repetir dados

## COMPORTAMENTOS ESPECÍFICOS
- "Últimas vendas de [cliente]": tabela DATA | NR NOTA | COD PRODUTO | DESCRIÇÃO | QTDE kg | CX | R$/kg — últimos 15 registros, data decrescente
- Período sem detalhe especificado (mensal/trimestral/anual): ofereça 4 opções antes de processar: "1) Resumo executivo 2) Análise por dia 3) Ranking produtos/vendedores 4) Comparativo filiais"
- EXCEÇÃO: se o usuário já especificou o que quer ("resumo", "ranking", "análise detalhada"), processe direto
- Filtro UF: reconhece siglas (ES, RJ...) e nomes por extenso. Destaque: volume, faturamento, clientes, produtos, cidades
- Busca por CNPJ: aceita "cnpj 73849952" (raiz 8 dígitos) ou "cnpj 73.849.952/0001-34" (completo). Se o CSV não tiver coluna CNPJ, oriente o usuário a buscar pelo nome do cliente.
- Vendedor não encontrado: sugira busca por código e liste até 5 disponíveis no período
- Apresente dados disponíveis SEM mencionar o que não existe. Nunca diga "não tenho dados de X"
- "Análise de [cliente] em [período]": se os dados já vierem filtrados por cliente (1 único NOME_CLIENTE), exiba tabela completa de todas as compras (DATA | NR NOTA | PRODUTO | KG | CX | R$ | R$/kg), depois totais e comparativo. Não resuma — mostre tudo.
- "Ontem" / períodos sem movimento: se os dados recebidos forem de uma data diferente do dia literal pedido, processe normalmente e informe apenas UMA linha no início: "_(Dados de DD/MM/AA — dia útil anterior disponível)_". Não peça confirmação, não ofereça opções, vá direto à análise.

## DETALHE DE NOTA FISCAL
- Quando o usuário pedir detalhes de uma nota E os dados contiverem apenas 1 NUM_DOCTO (todos os registros são da mesma nota): exiba tabela completa linha a linha:
  | # | PRODUTO | COD | DIVISÃO | QTDE kg | CX | VALOR | R$/kg |
  Depois: totais (kg total, cx total, faturamento total, preço médio), cliente, filial, vendedor, data
- Quando o usuário pedir "detalhes dessa nota" ou "detalhe da nota X" mas os dados contiverem MÚLTIPLOS NUM_DOCTO: pergunte "Qual o número da nota? (ex: nr 184828)" — NÃO diga que não tem acesso aos dados
- NUNCA diga que não tem acesso a detalhes transacionais — você tem acesso completo ao CSV com todos os itens

{personalidade}
DADOS ({data_label}):
{sales_data}"""

    # Instrução extra por tipo de output
    if gerar_pdf_apos_claude:
        system += "\n\nIMPORTANTE: O usuário quer um PDF executivo. Gere análise COMPLETA e DETALHADA em Markdown puro (sem HTML, sem base64). Estrutura: # Título principal → ## Seções → ### Subseções → tabelas com | → bullets com -. Seja extenso, analítico e com linguagem de relatório executivo para diretoria. Inclua: resumo executivo, KPIs principais (**Label**: Valor), análise por filial, top clientes, top produtos, análise temporal, pontos de atenção e recomendações."
        max_tok = 4000
    elif gerar_pptx_apos_claude:
        system += "\n\nIMPORTANTE: O usuário quer uma APRESENTAÇÃO POWERPOINT para diretoria. Estruture o conteúdo em slides usando Markdown:\n- Use # para título de cada slide (será 1 slide)\n- Use ## para slides de seção\n- Use **KPI**: Valor para métricas que virão como cards visuais (máx 4 por slide)\n- Use - bullets para listas (máx 6 itens por slide — seja conciso)\n- Use tabelas | Col | para dados tabulares (máx 15 linhas)\n- Estrutura recomendada: 1) Capa (# Título), 2) Resumo Executivo com KPIs, 3) Análise por Filial, 4) Top Clientes, 5) Top Produtos, 6) Análise Temporal, 7) Pontos de Atenção, 8) Conclusão/Recomendações\n- Linguagem executiva, focada em DECISÃO. Cada slide deve ter 1 mensagem clara.\n- Máximo 10-12 slides. Qualidade acima de quantidade."
        max_tok = 4000
    else:
        max_tok = 1500

    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={"Content-Type":"application/json",
                     "x-api-key":CLAUDE_KEY,
                     "anthropic-version":"2023-06-01"},
            json={"model":"claude-haiku-4-5-20251001",
                  "max_tokens":max_tok,
                  "system":system,
                  "messages":[m.dict() for m in req.messages]}
        )
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)

    resposta_json = r.json()

    # ── PDF: pega conteúdo gerado pelo Claude e converte ──
    if gerar_pdf_apos_claude:
        try:
            texto_claude = ""
            for bloco in resposta_json.get("content", []):
                if bloco.get("type") == "text":
                    texto_claude += bloco["text"]

            if texto_claude:
                class _FakeMsg:
                    def __init__(self, role, content):
                        self.role = role
                        self.content = content

                msgs_com_resposta = list(req.messages) + [_FakeMsg("assistant", texto_claude)]
                html_pdf = gerar_pdf(msgs_com_resposta)

                if html_pdf:
                    resposta_com_pdf = f"📄 **Relatório PDF gerado!**\n\n{html_pdf}\n\n---\n\n{texto_claude}"
                    return JSONResponse({
                        "id": resposta_json.get("id", "pdf"),
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "text", "text": resposta_com_pdf}],
                        "model": resposta_json.get("model", "local-reportlab"),
                        "stop_reason": "end_turn"
                    })
        except Exception as e_pdf:
            import logging
            logging.warning(f"[IAF PDF] Erro ao gerar PDF após Claude: {e_pdf}")

    # ── PPTX: pega conteúdo gerado pelo Claude e converte ──
    if gerar_pptx_apos_claude:
        try:
            texto_claude = ""
            for bloco in resposta_json.get("content", []):
                if bloco.get("type") == "text":
                    texto_claude += bloco["text"]

            if texto_claude:
                html_pptx = gerar_pptx(texto_claude)
                if html_pptx:
                    n_slides = texto_claude.count('\n# ') + texto_claude.count('\n## ') + 1
                    resposta_com_pptx = f"📊 **Apresentação gerada!** ({n_slides} slides)\n\n{html_pptx}\n\n---\n\n{texto_claude}"
                    return JSONResponse({
                        "id": resposta_json.get("id", "pptx"),
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "text", "text": resposta_com_pptx}],
                        "model": resposta_json.get("model", "local-pptxgenjs"),
                        "stop_reason": "end_turn"
                    })
        except Exception as e_pptx:
            import logging
            logging.warning(f"[IAF PPTX] Erro ao gerar PPTX após Claude: {e_pptx}")

    return resposta_json

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
