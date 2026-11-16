#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Pipeline ETL Apoio Técnico: Google Sheets -> Supabase.

Chave de negócio: Categoria + Evento + Município + Data + Gerência +
Modalidade + Tipo de Apoio.

O programa SOMENTE INSERE registros ausentes. Não atualiza nem exclui dados.
Interrompe se qualquer aba falhar. Exige chave Supabase secreta sb_secret_ no .env.

Instalação: pip install gspread google-auth pandas supabase python-dotenv openpyxl
Arquivos na mesma pasta: credenciais.json e .env

Teste inicial: python pipeline_atm.py --dry-run
Execução:       python pipeline_atm.py --ano 2026

.env:
SUPABASE_URL=https://SEU-PROJETO.supabase.co
SUPABASE_SERVICE_ROLE_KEY=sb_secret_...

Códigos: 0 sucesso | 1 abortado | 2 falhas de inserção.
"""
import os, re, sys, csv, argparse, logging, unicodedata
from collections import OrderedDict, defaultdict, Counter
from datetime import datetime, date
from pathlib import Path
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

BASE_DIR = Path(__file__).resolve().parent
PASTA_RELATORIOS = BASE_DIR / 'relatorios'
ARQ_LOG = BASE_DIR / 'pipeline.log'
SPREADSHEET_ID = '1cB32hAiwDplsmC1_8KFpAEkoYfX5yexGa_-GxZoQ9S0'
ARQUIVO_CREDENCIAIS = 'credenciais.json'
ESCOPOS = ['https://www.googleapis.com/auth/spreadsheets.readonly', 'https://www.googleapis.com/auth/drive.readonly']
CSV_HEADERS = ['Categoria','Evento','Município','RD','PORTE POPULACIONAL 2022','Data de Realização','Modalidade do Apoio','Tipo de Apoio','GERÊNCIA','Devolutiva','Avaliação']
CSV_DUPLICADOS_HEADERS = ['grupo','aba','linha','status','categoria','evento','municipio','data','gerencia','modalidade','tipo','rd','porte']
GERENCIAS = ['GESTÃO','GEBEN','GEPSB','GEPMC','GEPAC','GETEP','GEFEAS','VSA','CAD']
ABA_ATMS = 'ATM INDIVIDUAIS PRES E REMT'
ABA_OFICINAS = 'OFICINAS, ENCONTROS, MUPS, SICON'
ABA_MEET = 'GOOGLE MEET - RODAS DE DIÁLOGOS'
ARQ_DUPLICADOS = 'duplicados_planilha.csv'
TABELA = 'apoio_tecnico'
ANO_PADRAO = 2026
BATCH_PADRAO = 100
MAX_INSERCOES_PADRAO = 300
PAGE_SIZE = 1000
log = logging.getLogger('pipeline')

def limpar(valor) -> str:
    if valor is None:
        return ''
    if isinstance(valor, float) and pd.isna(valor):
        return ''
    s = str(valor).strip()
    return '' if s.lower() in ('nan', 'none') else s


def tem_conteudo(valor) -> bool:
    return limpar(valor) != ''


def is_concluido(valor) -> bool:
    return limpar(valor).lower() == 'concluído'


def normalizar_data(valor) -> str:
    if valor is None or valor == '':
        return ''
    if isinstance(valor, (datetime, pd.Timestamp)):
        return valor.strftime('%Y-%m-%d')
    s = str(valor).strip()
    if not s or s.lower() in ('nan', 'none'):
        return ''

    formatos = [
        '%d/%m/%Y', '%d/%m/%Y %H:%M:%S', '%d/%m/%y',
        '%Y-%m-%d', '%Y-%m-%d %H:%M:%S',
        '%d-%m-%Y', '%d-%m-%Y %H:%M:%S',
    ]
    for fmt in formatos:
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except ValueError:
            pass

    for dayfirst in (True, False):
        try:
            return pd.to_datetime(s, dayfirst=dayfirst).strftime('%Y-%m-%d')
        except Exception:
            pass

    log.warning(f'Data não reconhecida: {valor!r}')
    return ''


def preparar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).replace('\n', ' ').strip() for c in df.columns]
    return df


def aviso_campo(rotulo: str, campo: str, valor: str, warnings: list):
    if not valor:
        warnings.append(f'{rotulo}: {campo} vazio')
    elif valor.startswith('#') or 'REF' in valor.upper():
        warnings.append(f'{rotulo}: {campo} inválido ({valor!r})')


def _normalizar_nome(nome: str) -> str:
    texto = unicodedata.normalize('NFC', str(nome))
    resultado = []
    for ch in texto:
        categoria = unicodedata.category(ch)
        if categoria.startswith('P') or categoria.startswith('S'):
            continue
        resultado.append(ch)
    return ' '.join(''.join(resultado).split()).strip().lower()


# ---------------------------------------------------------------------------
# Chave de negócio e deduplicação
# ---------------------------------------------------------------------------

def _norm_texto(valor) -> str:
    if valor is None:
        return ''
    s = unicodedata.normalize('NFKD', str(valor))
    s = ''.join(c for c in s if not unicodedata.combining(c))
    return ' '.join(s.casefold().split())


def _chave_negocio(reg: dict) -> tuple:
    return (
        _norm_texto(reg.get('Categoria', '')),
        _norm_texto(reg.get('Evento', '')),
        _norm_texto(reg.get('Município', '')),
        reg.get('Data de Realização', ''),
        _norm_texto(reg.get('GERÊNCIA', '')),
        _norm_texto(reg.get('Modalidade do Apoio', '')),   # <-- novo
        _norm_texto(reg.get('Tipo de Apoio', '')),         # <-- novo
    )


def deduplicar(registros: list):
    """
    Remove duplicados pela chave de negócio.
    Devolve:
        unicos      -> lista sem duplicatas (mantém a 1ª ocorrência)
        duplicados  -> lista das removidas
        grupos_dup  -> OrderedDict{chave: [todas as ocorrências]} só para chaves repetidas
    """
    vistos = set()
    unicos, duplicados = [], []
    grupos = OrderedDict()
    for r in registros:
        k = _chave_negocio(r)
        grupos.setdefault(k, []).append(r)
        if k in vistos:
            duplicados.append(r)
        else:
            vistos.add(k)
            unicos.append(r)
    grupos_dup = OrderedDict((k, v) for k, v in grupos.items() if len(v) > 1)
    return unicos, duplicados, grupos_dup


# ---------------------------------------------------------------------------
# Leitura do Google Sheets
# ---------------------------------------------------------------------------

def conectar():
    creds = Credentials.from_service_account_file(ARQUIVO_CREDENCIAIS, scopes=ESCOPOS)
    return gspread.authorize(creds)


def encontrar_worksheet(planilha, nome_esperado: str):
    alvo = _normalizar_nome(nome_esperado)
    for ws in planilha.worksheets():
        if _normalizar_nome(ws.title) == alvo:
            log.info(f'Aba {nome_esperado!r} → {ws.title!r} (match exato)')
            return ws
    for ws in planilha.worksheets():
        if alvo in _normalizar_nome(ws.title):
            log.info(f'Aba {nome_esperado!r} → {ws.title!r} (match por substring)')
            return ws
    disponiveis = ', '.join(repr(w.title) for w in planilha.worksheets())
    raise ValueError(
        f'Aba {nome_esperado!r} não encontrada.\nAbas disponíveis: {disponiveis}'
    )


def carregar_aba(planilha, nome_esperado: str) -> pd.DataFrame:
    ws = encontrar_worksheet(planilha, nome_esperado)
    valores = ws.get_all_values()

    if not valores or len(valores) < 2:
        log.warning(f'[{nome_esperado}] aba vazia ou sem dados')
        return pd.DataFrame()

    cabecalho_bruto = valores[0]
    linhas_dados = valores[1:]

    ultima_col = -1
    for i, cab in enumerate(cabecalho_bruto):
        cab = str(cab).strip() if cab is not None else ''
        tem_dado = any(
            i < len(linha) and str(linha[i]).strip()
            for linha in linhas_dados
        )
        if cab or tem_dado:
            ultima_col = i

    if ultima_col < 0:
        log.warning(f'[{nome_esperado}] cabeçalho vazio')
        return pd.DataFrame()

    cabecalho_bruto = cabecalho_bruto[: ultima_col + 1]

    vistos = {}
    cabecalho_final = []
    for i, c in enumerate(cabecalho_bruto):
        c = str(c).strip() if c is not None else ''
        if not c:
            c = f'_col_{i}'
        if c in vistos:
            vistos[c] += 1
            c = f'{c}_{vistos[c]}'
        else:
            vistos[c] = 0
        cabecalho_final.append(c)

    n = len(cabecalho_final)
    linhas_ajustadas = []
    for linha in linhas_dados:
        linha = list(linha)
        if len(linha) < n:
            linha += [''] * (n - len(linha))
        else:
            linha = linha[:n]
        linhas_ajustadas.append(linha)

    df = pd.DataFrame(linhas_ajustadas, columns=cabecalho_final)

    # Preserva o número original de cada linha da planilha ANTES de remover vazias.
    # +2 porque o cabeçalho está na linha 1.
    df['_linha_orig'] = range(2, len(df) + 2)

    df = df.replace('', pd.NA).dropna(how='all').fillna('')

    return preparar_colunas(df)


# ---------------------------------------------------------------------------
# Extratores (agora com _aba e _linha para rastreabilidade)
# ---------------------------------------------------------------------------

def extrair_atms(df: pd.DataFrame, warnings: list) -> list:
    registros = []

    for idx, linha in df.iterrows():
        if not is_concluido(linha.get('Status da Solicitação')):
            continue
        municipio = limpar(linha.get('Município'))
        if not municipio:
            continue

        num_linha = int(linha.get('_linha_orig', idx + 2))

        data = normalizar_data(linha.get('Data Realizada')) \
            or normalizar_data(linha.get('Data da Solicitação'))
        rd    = limpar(linha.get('RD'))
        porte = limpar(linha.get('PORTE POPULACIONAL 2022*')) \
             or limpar(linha.get('PORTE POPULACIONAL 2022'))

        rotulo = f'[{ABA_ATMS}] linha {num_linha} ({municipio})'
        if not data:
            warnings.append(f'{rotulo}: data ausente')
        aviso_campo(rotulo, 'RD', rd, warnings)
        aviso_campo(rotulo, 'Porte', porte, warnings)

        modalidade = limpar(linha.get('Modalidade do Apoio'))
        tipo       = limpar(linha.get('Tipo de Apoio'))

        marcou = False
        for ger in GERENCIAS:
            if tem_conteudo(linha.get(ger)):
                marcou = True
                registros.append({
                    'Categoria':               'Apoio Técnico Individualizado',
                    'Evento':                  'Apoio Técnico',
                    'Município':               municipio,
                    'RD':                      rd,
                    'PORTE POPULACIONAL 2022': porte,
                    'Data de Realização':      data,
                    'Modalidade do Apoio':     modalidade,
                    'Tipo de Apoio':           tipo,
                    'GERÊNCIA':                ger,
                    'Devolutiva':              '',
                    'Avaliação':               '',
                    '_aba':                    ABA_ATMS,
                    '_linha':                  num_linha,
                })
        if not marcou:
            warnings.append(f'{rotulo}: Concluído sem nenhuma gerência marcada')

    return registros


def extrair_oficinas(df: pd.DataFrame, warnings: list) -> list:
    registros = []

    for idx, linha in df.iterrows():
        if not is_concluido(linha.get('Status')):
            continue
        municipio = limpar(linha.get('Município'))
        if not municipio:
            continue

        num_linha = int(linha.get('_linha_orig', idx + 2))

        data  = normalizar_data(linha.get('Data Realizada'))
        rd    = limpar(linha.get('RD'))
        porte = limpar(linha.get('PORTE POPULACIONAL'))

        rotulo = f'[{ABA_OFICINAS}] linha {num_linha} ({municipio})'
        if not data:
            warnings.append(f'{rotulo}: data ausente')
        aviso_campo(rotulo, 'RD', rd, warnings)
        aviso_campo(rotulo, 'Porte', porte, warnings)

        categoria  = limpar(linha.get('Categoria'))
        evento     = limpar(linha.get('Evento'))
        modalidade = limpar(linha.get('Modalidade'))
        tipo       = limpar(linha.get('Tipo de Apoio'))

        marcou = False
        for ger in GERENCIAS:
            if tem_conteudo(linha.get(ger)):
                marcou = True
                registros.append({
                    'Categoria':               categoria,
                    'Evento':                  evento,
                    'Município':               municipio,
                    'RD':                      rd,
                    'PORTE POPULACIONAL 2022': porte,
                    'Data de Realização':      data,
                    'Modalidade do Apoio':     modalidade,
                    'Tipo de Apoio':           tipo,
                    'GERÊNCIA':                ger,
                    'Devolutiva':              '',
                    'Avaliação':               '',
                    '_aba':                    ABA_OFICINAS,
                    '_linha':                  num_linha,
                })
        if not marcou:
            warnings.append(f'{rotulo}: Concluído sem nenhuma gerência marcada')

    return registros


def extrair_meet(df: pd.DataFrame, warnings: list) -> list:
    registros = []

    for idx, linha in df.iterrows():
        if not is_concluido(linha.get('Status')):
            continue
        municipio = limpar(linha.get('Município'))
        if not municipio:
            continue

        num_linha = int(linha.get('_linha_orig', idx + 2))

        data  = normalizar_data(linha.get('Data Realizada'))
        rd    = limpar(linha.get('RD'))
        porte = limpar(linha.get('PORTE POPULACIONAL'))

        rotulo = f'[{ABA_MEET}] linha {num_linha} ({municipio})'
        if not data:
            warnings.append(f'{rotulo}: data ausente')
        aviso_campo(rotulo, 'RD', rd, warnings)
        aviso_campo(rotulo, 'Porte', porte, warnings)

        evento     = limpar(linha.get('Evento'))
        modalidade = limpar(linha.get('Modalidade'))
        tipo       = limpar(linha.get('Tipo de Apoio'))

        marcou = False
        for ger in GERENCIAS:
            if tem_conteudo(linha.get(ger)):
                marcou = True
                registros.append({
                    'Categoria':               'Roda de Diálogo',
                    'Evento':                  evento,
                    'Município':               municipio,
                    'RD':                      rd,
                    'PORTE POPULACIONAL 2022': porte,
                    'Data de Realização':      data,
                    'Modalidade do Apoio':     modalidade,
                    'Tipo de Apoio':           tipo,
                    'GERÊNCIA':                ger,
                    'Devolutiva':              '',
                    'Avaliação':               '',
                    '_aba':                    ABA_MEET,
                    '_linha':                  num_linha,
                })
        if not marcou:
            warnings.append(f'{rotulo}: Concluído sem nenhuma gerência marcada')

    return registros


# ---------------------------------------------------------------------------
# Exportação das duplicatas
# ---------------------------------------------------------------------------

def exportar_duplicados_csv(grupos_dup: dict, caminho: str) -> None:
    linhas = []
    for i, (chave, ocorrencias) in enumerate(grupos_dup.items(), start=1):
        # Ordena por aba e linha: a primeira vira MANTIDO, as outras REMOVIDO
        ocorr_ord = sorted(
            ocorrencias,
            key=lambda r: (r.get('_aba', ''), r.get('_linha', 0))
        )
        for pos, r in enumerate(ocorr_ord):
            linhas.append({
                'grupo':      i,
                'aba':        r.get('_aba', ''),
                'linha':      r.get('_linha', ''),
                'status':     'MANTIDO' if pos == 0 else 'REMOVIDO',
                'categoria':  r.get('Categoria', ''),
                'evento':     r.get('Evento', ''),
                'municipio':  r.get('Município', ''),
                'data':       r.get('Data de Realização', ''),
                'gerencia':   r.get('GERÊNCIA', ''),
                'modalidade': r.get('Modalidade do Apoio', ''),
                'tipo':       r.get('Tipo de Apoio', ''),
                'rd':         r.get('RD', ''),
                'porte':      r.get('PORTE POPULACIONAL 2022', ''),
            })

    with open(caminho, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(
            f, fieldnames=CSV_DUPLICADOS_HEADERS, quoting=csv.QUOTE_MINIMAL
        )
        writer.writeheader()
        writer.writerows(linhas)

    log.info(f'CSV de duplicatas: {Path(caminho).resolve()}')


# ---------------------------------------------------------------------------
# Resumo
# ---------------------------------------------------------------------------

def _atividade_unica(reg: dict) -> tuple:
    return (reg['Categoria'], reg['Evento'], reg['Município'], reg['Data de Realização'])


def imprimir_resumo(por_aba: dict, warnings: list, duplicados: list,
                    total_bruto: int, grupos_dup: dict) -> None:
    LARGURA = 70
    linha_dupla = '═' * LARGURA
    linha_simples = '─' * LARGURA

    print()
    print(linha_dupla)
    print('RESUMO DA CONVERSÃO'.center(LARGURA))
    print(linha_dupla)

    print(f"{'Aba':<40}{'Atividades':>13}{'Registros CSV':>15}")
    print(linha_simples)

    total_atividades = 0
    total_registros = 0

    for aba, regs in por_aba.items():
        n_reg = len(regs)
        n_ativ = len({_atividade_unica(r) for r in regs})
        total_atividades += n_ativ
        total_registros  += n_reg
        nome = aba if len(aba) <= 40 else aba[:37] + '...'
        print(f'{nome:<40}{n_ativ:>13}{n_reg:>15}')

    print(linha_simples)
    print(f"{'TOTAL (CSV final)':<40}{total_atividades:>13}{total_registros:>15}")
    print(linha_dupla)

    print(f"Linhas brutas geradas pelos extratores : {total_bruto}")
    print(f"Duplicadas removidas                   : {len(duplicados)}")
    print(f"Linhas efetivamente no CSV             : {total_registros}")
    assert total_registros == total_bruto - len(duplicados), (
        f'Inconsistência: total no CSV ({total_registros}) '
        f'!= brutos ({total_bruto}) - duplicados ({len(duplicados)})'
    )
    print(f"Grupos de duplicatas                   : {len(grupos_dup)}")
    if grupos_dup:
        print(f"Planilha de duplicatas                 : {ARQ_DUPLICADOS}")
    print(linha_dupla)

    print('Distribuição por gerência:')
    todos = [r for regs in por_aba.values() for r in regs]
    contagem_ger = Counter(r['GERÊNCIA'] for r in todos)
    for ger in GERENCIAS:
        qtd = contagem_ger.get(ger, 0)
        if qtd:
            print(f'  {ger:<10}{qtd:>6}')
    print(linha_simples)

    print('Distribuição por categoria:')
    contagem_cat = Counter(r['Categoria'] for r in todos)
    for cat, qtd in contagem_cat.most_common():
        nome = cat if len(cat) <= 55 else cat[:52] + '...'
        print(f'  {nome:<55}{qtd:>6}')
    print(linha_dupla)

    # Detalhes dos grupos de duplicatas (top 20)
    if grupos_dup:
        total_grupos = len(grupos_dup)
        print(f'Detalhes dos grupos de duplicatas '
              f'(mostrando até 20 de {total_grupos}):')
        for i, (chave, ocorr) in enumerate(list(grupos_dup.items())[:20], start=1):
            ocorr_ord = sorted(
                ocorr, key=lambda r: (r.get('_aba', ''), r.get('_linha', 0))
            )
            local = ', '.join(
                f"{r.get('_aba', '?')[:18]} l.{r.get('_linha', '?')}"
                for r in ocorr_ord
            )
            ref = ocorr_ord[0]
            print(f'  Grupo {i}: {len(ocorr)} ocorrências ({local})')
            print(f'           → {ref.get("Município")} | '
                  f'{ref.get("Data de Realização")} | '
                  f'{ref.get("Evento", "")[:40]} | {ref.get("GERÊNCIA")}')
        if total_grupos > 20:
            print(f'  ... e mais {total_grupos - 20} grupos (veja o CSV)')
        print(linha_dupla)

    if warnings:
        print(f'Avisos: {len(warnings)} (mostrando até 20)')
        for w in warnings[:20]:
            print(f'  - {w}')
        if len(warnings) > 20:
            print(f'  ... e mais {len(warnings) - 20} avisos')
        print(linha_dupla)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def coletar_registros() -> dict:
    """
    Lê as 3 abas da planilha, aplica os extratores e deduplica.
    NÃO grava nenhum arquivo (usado diretamente pelo run_pipeline.py).

    Levanta RuntimeError se não conseguir conectar na planilha.
    Devolve um dict com:
        unicos, duplicados, grupos_dup, por_aba_final, warnings, total_bruto
    """
    log.info(f'Conectando ao Google Sheets (ID: {SPREADSHEET_ID})...')
    try:
        cliente = conectar()
        planilha = cliente.open_by_key(SPREADSHEET_ID)
    except Exception as e:
        raise RuntimeError(f'Falha ao conectar/abrir a planilha: {e}') from e

    log.info(f'Abas disponíveis: {[w.title for w in planilha.worksheets()]}')

    warnings: list = []
    por_aba: "OrderedDict[str, list]" = OrderedDict()
    abas_com_falha: list = []

    extratores = [
        (ABA_ATMS,     extrair_atms),
        (ABA_OFICINAS, extrair_oficinas),
        (ABA_MEET,     extrair_meet),
    ]

    for aba, extrator in extratores:
        try:
            df = carregar_aba(planilha, aba)
            log.info(f'[{aba}] {len(df)} linhas brutas')
            por_aba[aba] = extrator(df, warnings)
        except Exception as e:
            log.error(f'Falha na aba {aba!r}: {e}')
            por_aba[aba] = []
            abas_com_falha.append(aba)

    todos_brutos = [r for regs in por_aba.values() for r in regs]
    total_bruto = len(todos_brutos)

    todos_unicos, todos_duplicados, grupos_dup = deduplicar(todos_brutos)
    log.info(
        f'Deduplicação: {total_bruto} -> {len(todos_unicos)} '
        f'({len(todos_duplicados)} duplicados em {len(grupos_dup)} grupos)'
    )

    # Redistribui os únicos por aba para o resumo refletir o resultado final
    mapa_aba = {}
    for aba, regs in por_aba.items():
        for r in regs:
            mapa_aba[_chave_negocio(r)] = aba

    por_aba_final: "OrderedDict[str, list]" = OrderedDict((aba, []) for aba in por_aba)
    for r in todos_unicos:
        por_aba_final[mapa_aba[_chave_negocio(r)]].append(r)

    return {
        'unicos': todos_unicos,
        'duplicados': todos_duplicados,
        'grupos_dup': grupos_dup,
        'por_aba_final': por_aba_final,
        'warnings': warnings,
        'total_bruto': total_bruto,
        'abas_com_falha': abas_com_falha,
    }




def configurar_log() -> None:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S")
    raiz = logging.getLogger()
    raiz.setLevel(logging.INFO)
    for h in list(raiz.handlers):
        raiz.removeHandler(h)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    arquivo = logging.FileHandler(ARQ_LOG, mode="a", encoding="utf-8")
    arquivo.setFormatter(fmt)
    raiz.addHandler(console)
    raiz.addHandler(arquivo)
    # a lib do supabase/httpx é verbosa demais
    for nome in ("httpx", "httpcore", "hpack"):
        logging.getLogger(nome).setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Normalização e chave de negócio (ÚNICA definição, usada em todas as etapas)
# ---------------------------------------------------------------------------

def _sem_acento(s: str) -> str:
    n = unicodedata.normalize("NFKD", s)
    return "".join(c for c in n if not unicodedata.combining(c))


def norm_texto(valor) -> str:
    if valor is None:
        return ""
    s = str(valor).strip()
    if s.lower() in ("nan", "none", "nat", ""):
        return ""
    return " ".join(_sem_acento(s).casefold().split())


def norm_col(nome: str) -> str:
    """Nome de coluna comparável: sem acento, minúsculo, '_' vira espaço."""
    return " ".join(_sem_acento(str(nome)).casefold().replace("_", " ").split())


def norm_data(valor) -> str:
    if valor is None:
        return ""
    if isinstance(valor, (datetime, date)):
        return valor.strftime("%Y-%m-%d")
    s = str(valor).strip()
    if not s or s.lower() in ("nan", "none", "nat"):
        return ""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    m = re.match(r"^(\d{4}-\d{2}-\d{2})[T ]", s)   # timestamps ISO do Postgres
    if m:
        return m.group(1)
    return s


def data_valida(s: str) -> bool:
    try:
        datetime.strptime(s or "", "%Y-%m-%d")
        return True
    except (TypeError, ValueError):
        return False


# campos da planilha -> nomes possíveis da coluna no Supabase
CAMPOS = [
    ("Categoria", ["categoria"]),
    ("Evento", ["evento"]),
    ("Município", ["municipio"]),
    ("RD", ["rd"]),
    ("PORTE POPULACIONAL 2022",
     ["porte populacional 2022", "porte populacional", "porte 2022", "porte"]),
    ("Data de Realização",
     ["data", "data de realizacao", "data realizacao"]),
    ("Modalidade do Apoio",
     ["modalidade do apoio", "modalidade apoio", "modalidade"]),
    ("Tipo de Apoio", ["tipo de apoio", "tipo apoio", "tipo"]),
    ("GERÊNCIA", ["gerencia"]),
    ("Devolutiva", ["devolutiva"]),
    ("Avaliação", ["avaliacao"]),
]
CAMPOS_CHAVE = ["Categoria", "Evento", "Município", "Data de Realização", "GERÊNCIA", "Modalidade do Apoio", "Tipo de Apoio"]


def chave_planilha(r: dict) -> tuple:
    return (
        norm_texto(r.get("Categoria")),
        norm_texto(r.get("Evento")),
        norm_texto(r.get("Município")),
        norm_data(r.get("Data de Realização")),
        norm_texto(r.get("GERÊNCIA")),
        norm_texto(r.get("Modalidade do Apoio")),
        norm_texto(r.get("Tipo de Apoio")),
    )


def resolver_colunas(amostra: dict) -> dict:
    """Descobre, a partir de uma linha do banco, o nome real de cada coluna."""
    indice = {norm_col(c): c for c in amostra.keys()}
    mapa = {}
    for campo, candidatos in CAMPOS:
        for cand in candidatos:
            if cand in indice:
                mapa[campo] = indice[cand]
                break
        else:
            if campo in CAMPOS_CHAVE:
                raise RuntimeError(
                    f"Coluna da chave '{campo}' não encontrada em {TABELA}. "
                    f"Colunas do banco: {sorted(amostra.keys())}"
                )
            log.warning(f"Coluna '{campo}' sem correspondente no banco — será ignorada.")
    return mapa


def chave_banco(r: dict, mapa: dict) -> tuple:
    return (
        norm_texto(r.get(mapa["Categoria"])),
        norm_texto(r.get(mapa["Evento"])),
        norm_texto(r.get(mapa["Município"])),
        norm_data(r.get(mapa["Data de Realização"])),
        norm_texto(r.get(mapa["GERÊNCIA"])),
        norm_texto(r.get(mapa["Modalidade do Apoio"])),
        norm_texto(r.get(mapa["Tipo de Apoio"])),
    )


# ---------------------------------------------------------------------------
# Supabase
# ---------------------------------------------------------------------------

def criar_cliente():
    from dotenv import load_dotenv
    from supabase import create_client

    load_dotenv(BASE_DIR / ".env", override=True)
    url = os.getenv("SUPABASE_URL", "").strip()
    chave = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not url or not chave:
        raise RuntimeError("Defina SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY no .env.")
    if not chave.startswith("sb_secret_"):
        raise RuntimeError(
            "A SUPABASE_SERVICE_ROLE_KEY deve ser uma chave secreta sb_secret_. "
            "Não use chave sb_publishable_ nem SUPABASE_KEY."
        )
    if not url.startswith("https://"):
        raise RuntimeError("SUPABASE_URL deve começar com https://.")
    return create_client(url, chave)


def carregar_banco(cliente, ano: int) -> list:
    """Todos os registros do ano, paginados (ordem estável por id quando existir)."""
    def pagina(offset, com_ordem):
        q = (cliente.table(TABELA).select("*")
             .gte("data", f"{ano}-01-01").lte("data", f"{ano}-12-31"))
        if com_ordem:
            q = q.order("id")
        return (q.range(offset, offset + PAGE_SIZE - 1).execute().data) or []

    com_ordem = True
    try:
        primeira = pagina(0, True)
    except Exception:
        com_ordem = False
        primeira = pagina(0, False)

    todos = list(primeira)
    offset = PAGE_SIZE
    ultima = primeira
    while len(ultima) == PAGE_SIZE:
        ultima = pagina(offset, com_ordem)
        todos.extend(ultima)
        offset += PAGE_SIZE
    log.info(f"Supabase: {len(todos)} registros de {ano}.")
    return todos


def montar_payload(reg: dict, mapa: dict) -> dict:
    out = {}
    for campo, col in mapa.items():
        v = reg.get(campo)
        if isinstance(v, str):
            v = v.strip() or None
        out[col] = v
    col_data = mapa["Data de Realização"]
    if out.get(col_data):
        out[col_data] = norm_data(out[col_data])
    return out


def gravar_faltantes(cliente, faltantes: list, mapa: dict, batch: int, dry_run: bool):
    """Insere em lotes; após erro, relê o banco antes de qualquer repetição."""
    gravados, falhas = [], []
    total = len(faltantes)
    for i in range(0, total, batch):
        lote = faltantes[i:i + batch]
        numero = i // batch + 1
        if dry_run:
            gravados.extend(lote)
            log.info(f"[dry-run] lote {numero}: {len(lote)} registros (não gravado)")
            continue
        try:
            cliente.table(TABELA).insert([montar_payload(r, mapa) for r in lote]).execute()
            gravados.extend(lote)
            log.info(f"Lote {numero}: {len(lote)} registros inseridos.")
            continue
        except Exception as erro_lote:
            log.error(f"Lote {numero} apresentou erro: {erro_lote}")

        # Resposta de rede pode falhar mesmo depois de o servidor gravar.
        # Nunca repetir sem confirmar primeiro o que já existe.
        try:
            ano_lote = int(norm_data(lote[0].get("Data de Realização"))[:4])
            banco_atual = carregar_banco(cliente, ano_lote)
            chaves = {chave_banco(x, mapa) for x in banco_atual}
        except Exception as erro_leitura:
            motivo = f"Resultado incerto; não houve nova tentativa. Falha ao conferir banco: {erro_leitura}"
            falhas.extend((r, motivo) for r in lote)
            continue

        presentes = [r for r in lote if chave_planilha(r) in chaves]
        ausentes = [r for r in lote if chave_planilha(r) not in chaves]
        gravados.extend(presentes)
        if presentes:
            log.warning(f"Lote {numero}: {len(presentes)} já constam no banco após conferência.")

        for registro in ausentes:
            try:
                cliente.table(TABELA).insert(montar_payload(registro, mapa)).execute()
                gravados.append(registro)
            except Exception as erro_individual:
                # Também não presumir falha: verificar se a inserção foi efetivada.
                try:
                    ano_reg = int(norm_data(registro.get("Data de Realização"))[:4])
                    banco_final = carregar_banco(cliente, ano_reg)
                    chaves_finais = {chave_banco(x, mapa) for x in banco_final}
                    if chave_planilha(registro) in chaves_finais:
                        gravados.append(registro)
                    else:
                        falhas.append((registro, str(erro_individual)))
                except Exception as erro_conf:
                    falhas.append((registro, f"{erro_individual} | Conferência falhou: {erro_conf}"))
    return gravados, falhas


# ---------------------------------------------------------------------------
# Comparação
# ---------------------------------------------------------------------------

def preparar_planilha(registros: list, ano: int):
    """Separa o que pode ser comparado/gravado do que deve ser ignorado (e por quê)."""
    validos, ignorados = [], []
    for r in registros:
        d = norm_data(r.get("Data de Realização"))
        if not data_valida(d):
            ignorados.append((r, "Sem data válida"))
        elif int(d[:4]) != ano:
            ignorados.append((r, f"Data fora de {ano}"))
        else:
            validos.append(r)
    return validos, ignorados


def comparar(planilha: list, banco: list, mapa: dict):
    unicos_planilha = OrderedDict()
    colisoes = []   # repetição exata da chave de sete campos (já filtrada pelo conversor)
    for r in planilha:
        k = chave_planilha(r)
        if k in unicos_planilha:
            colisoes.append((r, unicos_planilha[k]))
        else:
            unicos_planilha[k] = r

    grupos_banco = defaultdict(list)
    for r in banco:
        grupos_banco[chave_banco(r, mapa)].append(r)

    faltantes = [r for k, r in unicos_planilha.items() if k not in grupos_banco]
    extras = [v[0] for k, v in grupos_banco.items() if k not in unicos_planilha]
    dup_banco = {k: v for k, v in grupos_banco.items() if len(v) > 1}
    return unicos_planilha, faltantes, extras, dup_banco, colisoes


def correspondentes_provaveis(extras: list, unicos_planilha: dict, mapa: dict) -> dict:
    """
    Para cada extra, procura na planilha registros com mesmo município+data+gerência
    (mas evento/categoria diferentes): provável correção de digitação.
    """
    indice = defaultdict(list)
    for k, r in unicos_planilha.items():
        indice[(k[2], k[3], k[4])].append(r)
    achados = {}
    for e in extras:
        k = chave_banco(e, mapa)
        cands = indice.get((k[2], k[3], k[4]), [])
        if cands:
            achados[id(e)] = " || ".join(
                f"{c.get('Categoria')} | {c.get('Evento')}" for c in cands)
    return achados


# ---------------------------------------------------------------------------
# Apresentação (terminal) e relatório Excel
# ---------------------------------------------------------------------------

def descr_planilha(r: dict) -> str:
    return (f"{r.get('Data de Realização')} | {r.get('Município')} | "
            f"{r.get('GERÊNCIA')} | {r.get('Categoria')} | {r.get('Evento')}")


def descr_banco(r: dict, mapa: dict) -> str:
    g = lambda c: r.get(mapa[c]) or ""
    return (f"{norm_data(g('Data de Realização'))} | {g('Município')} | "
            f"{g('GERÊNCIA')} | {g('Categoria')} | {g('Evento')}")


def listar(titulo: str, linhas: list, limite: int = 150) -> None:
    print(f"\n{titulo} ({len(linhas)})")
    print("─" * 78)
    if not linhas:
        print("  (nenhum)")
    for i, l in enumerate(linhas[:limite], 1):
        print(f"  {i:>3}. {l}")
    if len(linhas) > limite:
        print(f"  ... e mais {len(linhas) - limite} (veja o Excel)")


def df_planilha(regs: list):
    import pandas as pd
    cols = [c for c, _ in CAMPOS]
    return pd.DataFrame([{c: r.get(c, "") for c in cols} for r in regs], columns=cols)


def salvar_excel(caminho: Path, abas: "OrderedDict") -> None:
    import pandas as pd
    from openpyxl.styles import Font
    from openpyxl.utils import get_column_letter

    with pd.ExcelWriter(caminho, engine="openpyxl") as w:
        for nome, df in abas.items():
            df.to_excel(w, sheet_name=nome[:31], index=False)
            ws = w.sheets[nome[:31]]
            ws.freeze_panes = "A2"
            for c in ws[1]:
                c.font = Font(bold=True)
            for i, col in enumerate(df.columns, 1):
                tam = max([len(str(col))] + [len(str(v)) for v in df[col].head(300)])
                ws.column_dimensions[get_column_letter(i)].width = min(max(tam + 2, 10), 60)
    log.info(f"Relatório Excel: {caminho}")


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def executar(ano: int, batch: int, dry_run: bool, max_ins: int, forcar: bool,
             dados_planilha: dict = None, cliente=None) -> int:
    import pandas as pd

    inicio = datetime.now()

    # 1) Planilha -----------------------------------------------------------
    log.info("ETAPA 1/5 — Lendo a planilha Google...")
    if dados_planilha is None:
        dados_planilha = coletar_registros()
    if dados_planilha.get("abas_com_falha"):
        raise RuntimeError(
            f"Falha ao ler as abas {dados_planilha['abas_com_falha']}. "
            "Abortado para não comparar com dados incompletos.")
    todos = dados_planilha["unicos"]
    if not todos:
        raise RuntimeError("A planilha não retornou nenhum registro. Abortado.")
    planilha, ignorados = preparar_planilha(todos, ano)
    log.info(f"Planilha: {len(todos)} registros ({len(planilha)} comparáveis em {ano}, "
             f"{len(ignorados)} ignorados).")

    # 2) Banco ----------------------------------------------------------------
    log.info("ETAPA 2/5 — Lendo o Supabase...")
    cliente = cliente or criar_cliente()
    banco = carregar_banco(cliente, ano)
    if not banco:
        raise RuntimeError(
            f"O Supabase não retornou registros de {ano}. Provável problema de "
            "permissão (RLS) ou de conexão. Abortado para não duplicar tudo.")
    mapa = resolver_colunas(banco[0])

    # 3) Comparação -------------------------------------------------------------
    log.info("ETAPA 3/5 — Comparando...")
    unicos_planilha, faltantes, extras, dup_banco, colisoes = comparar(planilha, banco, mapa)
    log.info(f"Faltantes no banco: {len(faltantes)} | Extras (só no banco): {len(extras)} | "
             f"Duplicatas no banco: {len(dup_banco)} chaves")

    if len(faltantes) > max_ins and not forcar:
        raise RuntimeError(
            f"{len(faltantes)} registros a gravar excede o limite de segurança "
            f"({max_ins}). Confira a planilha/banco; se estiver certo, rode com --forcar.")

    # 4) Gravação ---------------------------------------------------------------
    log.info(f"ETAPA 4/5 — Gravando {len(faltantes)} registros"
             + (" [DRY-RUN]" if dry_run else "") + "...")
    gravados, falhas = ([], [])
    if faltantes:
        gravados, falhas = gravar_faltantes(cliente, faltantes, mapa, batch, dry_run)

    # 5) Verificação --------------------------------------------------------------
    restantes = None
    if faltantes and not dry_run:
        log.info("ETAPA 5/5 — Verificando o banco após a gravação...")
        banco2 = carregar_banco(cliente, ano)
        restantes = len(comparar(planilha, banco2, mapa)[1])
        log.info(f"Faltantes após a gravação: {restantes}")
    else:
        log.info("ETAPA 5/5 — Verificação dispensada (nada gravado).")

    # Relatório -----------------------------------------------------------------
    hoje = date.today().strftime("%Y-%m-%d")
    futuros = [r for r in gravados if norm_data(r.get("Data de Realização")) > hoje]
    prov = correspondentes_provaveis(extras, unicos_planilha, mapa)

    PASTA_RELATORIOS.mkdir(exist_ok=True)
    caminho_xlsx = PASTA_RELATORIOS / f"relatorio_pipeline_{inicio:%Y-%m-%d_%H%M}.xlsx"

    resumo = [
        ("Execução", f"{inicio:%d/%m/%Y %H:%M:%S}" + (" (DRY-RUN)" if dry_run else "")),
        ("Ano comparado", ano),
        ("Registros lidos da planilha (após dedup)", len(todos)),
        ("  comparáveis (data válida no ano)", len(planilha)),
        ("  ignorados (sem data / fora do ano)", len(ignorados)),
        ("Registros no Supabase (ano)", len(banco)),
        ("Faltantes no banco (planilha → banco)", len(faltantes)),
        ("Gravados com sucesso" + (" (simulado)" if dry_run else ""), len(gravados)),
        ("Falhas de gravação", len(falhas)),
        ("Faltantes após a gravação (esperado 0)", "—" if restantes is None else restantes),
        ("Extras: no banco, NÃO na planilha", len(extras)),
        ("Chaves duplicadas no banco", len(dup_banco)),
        ("Duplicatas reais na planilha (corrigir na origem)", len(dados_planilha["grupos_dup"])),
        ("Repetições exatas da chave de sete campos na comparação", len(colisoes)),
        ("Gravados com data futura", len(futuros)),
    ]
    abas = OrderedDict()
    abas["resumo"] = pd.DataFrame(resumo, columns=["métrica", "valor"])
    abas["gravados"] = df_planilha(gravados)

    linhas_extras = []
    for e in extras:
        linha = {"id": e.get("id", "")}
        for campo, col in mapa.items():
            linha[campo] = e.get(col)
        linha["PROVÁVEL CORRESPONDENTE NA PLANILHA (mesmo município/data/gerência)"] = \
            prov.get(id(e), "")
        linhas_extras.append(linha)
    abas["extras_banco_fora_planilha"] = pd.DataFrame(linhas_extras)

    if falhas:
        d = df_planilha([r for r, _ in falhas])
        d["motivo"] = [m for _, m in falhas]
        abas["falhas_gravacao"] = d
    if dup_banco:
        linhas = []
        for k, regs in dup_banco.items():
            for r in regs:
                linha = {"id": r.get("id", ""), "ocorrências": len(regs)}
                for campo, col in mapa.items():
                    linha[campo] = r.get(col)
                linhas.append(linha)
        abas["duplicatas_banco"] = pd.DataFrame(linhas)
    if dados_planilha["grupos_dup"]:
        linhas = []
        for i, (k, ocs) in enumerate(dados_planilha["grupos_dup"].items(), 1):
            for pos, r in enumerate(sorted(ocs, key=lambda x: (x.get("_aba", ""), x.get("_linha", 0)))):
                linhas.append({"grupo": i, "aba": r.get("_aba", ""), "linha": r.get("_linha", ""),
                               "situação": "mantido" if pos == 0 else "duplicata",
                               **{c: r.get(c, "") for c, _ in CAMPOS}})
        abas["duplicatas_planilha"] = pd.DataFrame(linhas)
    if colisoes:
        linhas = [{"Categoria": r.get("Categoria"), "Evento": r.get("Evento"),
                   "Município": r.get("Município"), "Data": r.get("Data de Realização"),
                   "GERÊNCIA": r.get("GERÊNCIA"),
                   "Modalidade (descartada)": r.get("Modalidade do Apoio"),
                   "Tipo (descartado)": r.get("Tipo de Apoio"),
                   "Modalidade (mantida)": m.get("Modalidade do Apoio"),
                   "Tipo (mantido)": m.get("Tipo de Apoio")} for r, m in colisoes]
        abas["colisoes_planilha"] = pd.DataFrame(linhas)
    if ignorados:
        d = df_planilha([r for r, _ in ignorados])
        d["motivo"] = [m for _, m in ignorados]
        abas["ignorados_planilha"] = d
    if dados_planilha["warnings"]:
        abas["avisos_conversao"] = pd.DataFrame({"aviso": dados_planilha["warnings"]})
    salvar_excel(caminho_xlsx, abas)

    # Resumo no terminal --------------------------------------------------------
    L = 78
    print("\n" + "═" * L)
    print(("RELATÓRIO FINAL" + (" — DRY-RUN" if dry_run else "")).center(L))
    print("═" * L)
    for nome, valor in resumo[1:]:
        print(f"  {nome:<58}{valor!s:>10}")
    print("═" * L)

    listar("GRAVADOS NO SUPABASE" + (" (simulado)" if dry_run else ""),
           [descr_planilha(r) for r in gravados])
    if falhas:
        listar("FALHAS DE GRAVAÇÃO",
               [f"{descr_planilha(r)}  →  {m[:120]}" for r, m in falhas])
    listar("NO SUPABASE, MAS NÃO NA PLANILHA (inclua na planilha, se forem válidos)",
           [descr_banco(e, mapa) + (f"\n         ↳ possível correspondente na planilha: {prov[id(e)]}"
                                    if id(e) in prov else "")
            for e in extras])
    if dup_banco:
        print(f"\nATENÇÃO: {len(dup_banco)} chave(s) duplicada(s) no banco "
              f"(detalhes na aba 'duplicatas_banco').")
    if dados_planilha["grupos_dup"]:
        print(f"ATENÇÃO: {len(dados_planilha['grupos_dup'])} duplicata(s) real(is) na planilha "
              f"(aba 'duplicatas_planilha').")
    if colisoes:
        print(f"ATENÇÃO: {len(colisoes)} linha(s) da planilha diferem só em modalidade/tipo e "
              f"colidem na chave — só a primeira foi usada (aba 'colisoes_planilha').")
    if ignorados:
        print(f"ATENÇÃO: {len(ignorados)} linha(s) ignoradas por data ausente/fora do ano "
              f"(aba 'ignorados_planilha').")
    if futuros:
        print(f"ATENÇÃO: {len(futuros)} registro(s) gravado(s) têm data futura (após {hoje}).")
    print(f"\nRelatório: {caminho_xlsx}\nLog:       {ARQ_LOG}")

    return 2 if falhas else 0


def main() -> None:
    p = argparse.ArgumentParser(description="Pipeline ETL de Apoio Técnico (tudo de uma vez).")
    p.add_argument("--ano", type=int, default=ANO_PADRAO, help=f"Ano a comparar (padrão {ANO_PADRAO})")
    p.add_argument("--batch", type=int, default=BATCH_PADRAO, help="Tamanho do lote de inserção")
    p.add_argument("--dry-run", action="store_true", help="Simula: não grava no banco")
    p.add_argument("--max-insercoes", type=int, default=MAX_INSERCOES_PADRAO,
                   help="Aborta se houver mais faltantes que isso (trava de segurança)")
    p.add_argument("--forcar", action="store_true", help="Ignora a trava --max-insercoes")
    args = p.parse_args()

    if args.ano < 2000 or args.ano > 2100:
        p.error("--ano deve estar entre 2000 e 2100.")
    if args.batch < 1:
        p.error("--batch deve ser maior que zero.")
    if args.max_insercoes < 0:
        p.error("--max-insercoes não pode ser negativo.")

    configurar_log()
    log.info("=" * 60)
    log.info("Início do pipeline")
    try:
        codigo = executar(args.ano, args.batch, args.dry_run, args.max_insercoes, args.forcar)
    except RuntimeError as e:
        log.error(f"PIPELINE ABORTADO: {e}")
        codigo = 1
    except Exception:
        log.exception("Erro inesperado")
        codigo = 1
    log.info(f"Fim do pipeline (código {codigo})")
    sys.exit(codigo)




if __name__ == "__main__":
    main()
