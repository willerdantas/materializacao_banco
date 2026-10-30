# app.py - Aplicação Flask para Controle de Migração
# ============================================================

from flask import Flask, render_template, request, jsonify, flash, redirect, url_for, session, send_file
from werkzeug.security import generate_password_hash, check_password_hash
from cryptography.fernet import Fernet, InvalidToken
import io
import json
import re
import psycopg2
from psycopg2 import InterfaceError, OperationalError
from psycopg2.extras import RealDictCursor
import os
import threading
import time
import math
from datetime import datetime, timedelta

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'importacao-intergash-dev-key')
app.config['DB_SAVED_CONNECTIONS'] = []
app.config['DB_SAVED_CONNECTIONS_LOADED'] = False
app.config['DB_CONFIG_LOADED'] = False

# Configuração do Banco de Dados
DEFAULT_DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASSWORD', ''),
    'port': int(os.getenv('DB_PORT', '5432')),
    'schema_destino': os.getenv('DB_SCHEMA_DESTINO', 'public')
}
DB_CLIENT_ENCODING_CANDIDATES = ('WIN1252', 'LATIN1', 'UTF8', 'LATIN9', 'SQL_ASCII')
# SQL_ASCII só serve de último recurso pontual: com ele o psycopg2 codifica todo
# texto em ASCII e qualquer acento enviado ao banco ("já", "não"...) quebra com
# "'ascii' codec can't encode character". Nunca é memorizado nem vai pra frente.
DB_CLIENT_ENCODING_ULTIMO_RECURSO = 'SQL_ASCII'
DB_CONNECT_KEYS = ('host', 'database', 'user', 'password', 'port')
APP_DATA_DIR = os.path.join(app.root_path, 'data')
SAVED_CONNECTIONS_FILE = os.path.join(APP_DATA_DIR, 'db_saved_connections.json')
ACTIVE_DB_CONFIG_FILE = os.path.join(APP_DATA_DIR, 'db_active_config.json')
FDW_CONFIG_FILE = os.path.join(APP_DATA_DIR, 'fdw_config.json')  # credenciais da origem (FDW)
FDW_TABELAS_FILE = os.path.join(APP_DATA_DIR, 'fdw_tabelas.json')  # última lista de tabelas da origem
USERS_FILE = os.path.join(APP_DATA_DIR, 'usuarios.json')  # usuários e perfis de acesso
app.config['DB_CONFIG'] = DEFAULT_DB_CONFIG.copy()

# Configuração padrão do FDW (origem dos dados)
DEFAULT_FDW_CONFIG = {
    'host': '',
    'port': 5432,
    'database': '',
    'user': '',
    'password': '',
    'schema_remoto': 'public',
    'schema_local': '',   # só em instalações antigas (FDW local) — traduz nomes antigos
}
app.config['FDW_CONFIG'] = DEFAULT_FDW_CONFIG.copy()
app.config['FDW_CONFIG_LOADED'] = False

# NOVO: schema usado como staging antes do upsert em public
TEMP_SCHEMA_NAME = 'temp'

# ============================================================
# WORKER DE PROCESSAMENTO EM SEGUNDO PLANO
# ============================================================

background_jobs = {}
background_jobs_lock = threading.Lock()


def _generate_job_id(job_type, resource_id):
    """Gera um identificador único para um job em segundo plano."""
    return f'{job_type}:{resource_id}'


def get_background_job(job_id):
    """Retorna o estado de um job ou None."""
    with background_jobs_lock:
        return background_jobs.get(job_id)


def set_background_job(job_id, data):
    """Atualiza o estado de um job."""
    with background_jobs_lock:
        background_jobs[job_id] = data


def remove_background_job(job_id):
    """Remove um job da lista."""
    with background_jobs_lock:
        background_jobs.pop(job_id, None)


JOB_STATUS_ATIVOS = ('iniciando', 'processando')

# Thread de cada job disparado por iniciar_background_job — usada pelo
# agendador pra liberar jobs "órfãos" (thread morreu sem fechar o status, o
# que travava a tabela com 409 "já existe um ciclo em andamento" pra sempre).
background_threads = {}


def iniciar_background_job(job_id, data, target, args):
    """Reserva o job e dispara a thread, de forma atômica: devolve False (sem
    disparar nada) se já houver um job ativo com esse id."""
    with background_jobs_lock:
        atual = background_jobs.get(job_id)
        if atual and atual.get('status') in JOB_STATUS_ATIVOS:
            return False
        background_jobs[job_id] = data
        thread = threading.Thread(target=target, args=args, daemon=True)
        background_threads[job_id] = thread
    thread.start()
    return True


def liberar_jobs_orfaos():
    """Fecha como 'erro' os jobs ativos cuja thread já terminou."""
    with background_jobs_lock:
        for job_id, thread in list(background_threads.items()):
            if thread.is_alive():
                continue
            background_threads.pop(job_id, None)
            job = background_jobs.get(job_id)
            if job and job.get('status') in JOB_STATUS_ATIVOS:
                background_jobs[job_id] = {
                    **job, 'status': 'erro',
                    'mensagem': 'Execução encerrada inesperadamente — liberada para o próximo disparo.',
                }


def contar_jobs_importacao_ativos():
    """Ciclos da importação automática + execuções agendadas rodando agora."""
    with background_jobs_lock:
        return sum(
            1 for job in background_jobs.values()
            if job.get('tipo') in ('auto_ciclo', 'agendamento') and job.get('status') in JOB_STATUS_ATIVOS
        )


def contar_migracoes_manuais_em_execucao():
    """Conta jobs de segundo plano ativos (processando/iniciando) das
    migrações manuais — mais confiável que o campo `status` de
    controle_migracao, que o worker de migração não atualiza durante o
    processamento."""
    with background_jobs_lock:
        return sum(
            1 for job in background_jobs.values()
            if job.get('tipo') in ('migracao', 'refresh') and job.get('status') in ('processando', 'iniciando')
        )


# ============================================================
# STAGING EM SCHEMA TEMP + PROCESSAMENTO EM LOTE
# ============================================================
# A faixa de IDs é dividida em lotes (tamanho_lote). Para cada lote:
#   1) copia os IDs do lote da origem para uma tabela em `temp`
#      (com reconexão automática se a origem cair);
#   2) faz UPSERT do lote inteiro em UMA instrução SQL (rápido);
#   3) se o upsert em lote falhar, cai para registro-a-registro
#      só naquele lote (não perde o restante da migração);
#   4) grava o status de todo o lote em UMA instrução (bulk).

DEFAULT_TAMANHO_LOTE = 500   # usado se o front não enviar valor
MAX_TAMANHO_LOTE = 5000      # trava para não gerar staging gigante


def obter_staging_table(migracao_id):
    """Nome único da tabela temp por migração."""
    return f'{TEMP_SCHEMA_NAME}.stg_mig_{migracao_id}'


def ensure_temp_schema(conn):
    """Garante existência do schema temp."""
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS {quote_identifier(TEMP_SCHEMA_NAME)}')
    conn.commit()


def preparar_staging_vazia(conn, migracao_id, tab_dest, cols_ins):
    """Cria a tabela temp vazia (estrutura do destino), reaproveitada por todos os lotes."""
    staging = obter_staging_table(migracao_id)
    insert_columns = _split_sql_columns(cols_ins)
    if not insert_columns:
        raise ValueError('Colunas de inserção não preparadas para a migração.')
    destino = normalize_relation_input(tab_dest, 'Tabela Destino', get_db_config()['schema_destino'])
    criar_staging_como_destino(conn, staging, destino, insert_columns)
    return staging


def copiar_lote_para_staging(conn, staging, tab_orig, campo, cols_ins, ids_lote):
    """Substitui o conteúdo da staging pelos IDs do lote atual, lidos direto
    do servidor de origem."""
    copiar_ids_da_origem(conn, staging, normalizar_origem(tab_orig), campo, _split_sql_columns(cols_ins), ids_lote)


def remover_staging(conn, migracao_id):
    """Remove a tabela temp ao final do processamento."""
    staging = obter_staging_table(migracao_id)
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS {staging}')
        conn.commit()
    except Exception:
        try:
            conn.rollback()  # evita deixar a conexão em estado sujo
        except Exception:
            pass


def obter_staging_table_auto(tabela_destino):
    """Nome da staging da Importação Automática — prefixo 'c_' para nunca colidir
    com o staging das migrações manuais (que usa 'stg_mig_<id>'), já que a
    importação automática roda em loop e pode coexistir com uma migração ativa.
    Ex.: destino 'public.t_internacao' -> 'temp.c_t_internacao'.
    """
    _, nome_tabela = tabela_destino.split('.', 1)
    return f'{TEMP_SCHEMA_NAME}.c_{nome_tabela}'


def obter_staging_table_agendamento(tabela_destino):
    """Nome da staging do Agendar Importação (sincronização completa) —
    prefixo 'a_' para nunca colidir com o staging das migrações manuais
    ('stg_mig_<id>') nem com o da importação automática ('c_<tabela>'), já
    que os três podem rodar ao mesmo tempo contra a mesma tabela destino.
    Ex.: destino 'public.t_internacao' -> 'temp.a_t_internacao'.
    """
    _, nome_tabela = tabela_destino.split('.', 1)
    return f'{TEMP_SCHEMA_NAME}.a_{nome_tabela}'


def executar_com_reconexao(operacao, abrir_conexao_fn, conn, max_tentativas=4, job_id=None, etapa='origem'):
    """Roda `operacao(conn)`; se a conexão com a origem cair, reconecta e tenta de novo.
    Conflito de recovery (standby ocupado com replay do WAL) recebe mais tentativas
    e espera maior, já que costuma levar mais tempo pra se resolver sozinho do que
    uma queda de conexão comum."""
    tentativa = 0
    while True:
        try:
            operacao(conn)
            return conn
        except (OperationalError, InterfaceError) as error:
            tentativa += 1
            eh_recovery = _is_recovery_conflict(error)
            limite = 12 if eh_recovery else max_tentativas
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            if tentativa >= limite:
                raise
            espera = min(90, 15 * tentativa) if eh_recovery else min(30, 5 * tentativa)
            if job_id:
                motivo = (
                    'Conflito de recovery na origem (standby ocupado com replay do WAL)'
                    if eh_recovery else f'Conexão com a {etapa} caiu'
                )
                set_background_job(job_id, {
                    **(get_background_job(job_id) or {}),
                    'mensagem': f'{motivo}. Reconectando em {espera}s (tentativa {tentativa}/{limite})...',
                })
            time.sleep(espera)
            conn = abrir_conexao_fn()  # reabre a conexão (origem e destino usam a mesma)


def montar_sql_upsert_lote(staging, tab_dest, campo, cols_ins):
    """SQL que faz o UPSERT do lote INTEIRO em uma única instrução."""
    quoted_target = quote_qualified_relation(normalize_relation_input(tab_dest, 'Tabela Destino', get_db_config()['schema_destino']))
    quoted_key = quote_identifier(campo)
    insert_columns = _split_sql_columns(cols_ins)
    key_token = (campo or '').strip().lower()
    update_columns = [c for c in insert_columns if _normalize_identifier_token(c) != key_token]

    if update_columns:
        update_assignments = ', '.join([f'{c} = EXCLUDED.{c}' for c in update_columns])
        difference_clause = ' OR '.join(
            [f'{quoted_target}.{c} IS DISTINCT FROM EXCLUDED.{c}' for c in update_columns]
        )
        conflict_sql = f"""
            ON CONFLICT ({quoted_key}) DO UPDATE
            SET {update_assignments}
            WHERE {difference_clause}
            RETURNING {quoted_key} AS chave, (xmax = 0) AS inserido
        """
    else:
        conflict_sql = f"""
            ON CONFLICT ({quoted_key}) DO NOTHING
            RETURNING {quoted_key} AS chave, (xmax = 0) AS inserido
        """

    return f"""
        INSERT INTO {quoted_target} ({cols_ins})
        SELECT {', '.join(insert_columns)} FROM {staging}
        {conflict_sql}
    """


def processar_lote_bulk(conn, staging, tab_dest, campo, cols_ins, ids_lote):
    """Faz o upsert do lote inteiro de uma vez (caminho rápido)."""
    quoted_key = quote_identifier(campo)
    sql_upsert = montar_sql_upsert_lote(staging, tab_dest, campo, cols_ins)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f'SELECT {quoted_key} AS chave FROM {staging}')  # ids que existem na origem
        presentes = {str(r['chave']) for r in cur.fetchall()}

        cur.execute(sql_upsert)
        retornados = {str(r['chave']): r['inserido'] for r in cur.fetchall()}
    conn.commit()

    return {
        'inseridos': [c for c, ins in retornados.items() if ins],
        'atualizados': [c for c, ins in retornados.items() if not ins],
        'ja_existem': [c for c in presentes if c not in retornados],
        'nao_encontrados': [c for c in ids_lote if c not in presentes],
        'com_erro': [],
    }


def processar_lote_fallback(conn, staging, tab_dest, campo, cols_ins, ids_lote):
    """Processa o lote registro a registro (usado só quando o upsert em lote falha)."""
    cols_sel_staging = ', '.join([f's.{quote_identifier(c)}' for c in _split_sql_columns(cols_ins)])
    sql_upsert_one = build_single_record_upsert_sql(staging, tab_dest, campo, cols_ins, cols_sel_staging)

    resultado = {'inseridos': [], 'atualizados': [], 'ja_existem': [], 'nao_encontrados': [], 'com_erro': []}

    for chave in ids_lote:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql_upsert_one, (int(chave),))
                outcome = cur.fetchone() or {}
            conn.commit()

            if not outcome.get('existe_origem'):
                resultado['nao_encontrados'].append(chave)
            elif outcome.get('inserido') is True:
                resultado['inseridos'].append(chave)
            elif outcome.get('inserido') is False:
                resultado['atualizados'].append(chave)
            else:
                resultado['ja_existem'].append(chave)
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            resultado['com_erro'].append({'chave': chave, 'erro': str(e)[:500]})

    return resultado


def gravar_status_lote(conn, migracao_id, resultado):
    """Grava o status de TODO o lote em uma única instrução (bulk, via unnest)."""
    chaves, status_list, erro_list = [], [], []

    for chave in resultado.get('inseridos', []):
        chaves.append(chave); status_list.append('inserido'); erro_list.append(None)
    for chave in resultado.get('atualizados', []):
        chaves.append(chave); status_list.append('atualizado'); erro_list.append(None)
    for chave in resultado.get('ja_existem', []):
        chaves.append(chave); status_list.append('ja_existe'); erro_list.append(None)
    for chave in resultado.get('nao_encontrados', []):
        chaves.append(chave); status_list.append('nao_encontrado'); erro_list.append(None)
    for item in resultado.get('com_erro', []):
        chaves.append(item['chave']); status_list.append('erro'); erro_list.append(item['erro'])

    if not chaves:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO controle_migracao_lotes
                (migracao_id, numero_lote, chave_valor, status, erro_msg, updated_at)
            SELECT %s, 1, x.chave, x.status, x.erro, CURRENT_TIMESTAMP
            FROM unnest(%s::text[], %s::text[], %s::text[]) AS x(chave, status, erro)
            ON CONFLICT (migracao_id, chave_valor) DO UPDATE
            SET status = EXCLUDED.status, erro_msg = EXCLUDED.erro_msg, updated_at = CURRENT_TIMESTAMP
            """,
            (migracao_id, chaves, status_list, erro_list)
        )
    conn.commit()


def _background_migracao_worker(job_id, migracao_id, id_inicio, id_fim, delay_segundos,
                                 apenas_erros=False, tamanho_lote=DEFAULT_TAMANHO_LOTE):
    """Thread worker que processa a faixa de IDs em LOTES (ver seção acima).
    Cada lote é copiado para staging em `temp` e gravado no destino com poucas
    instruções SQL, muito mais rápido que processar registro a registro.
    """
    job = get_background_job(job_id)
    if not job:
        return

    tamanho_lote = max(1, min(int(tamanho_lote or DEFAULT_TAMANHO_LOTE), MAX_TAMANHO_LOTE))
    set_background_job(job_id, {**job, 'status': 'processando', 'inicio': datetime.now().isoformat(),
                                 'tamanho_lote': tamanho_lote})

    registros_inseridos = 0
    registros_ja_existem = 0
    registros_atualizados = 0
    registros_nao_encontrados = 0
    registros_com_erro = []
    total_faixa = id_fim - id_inicio + 1

    def abrir_conexao():
        for enc in get_db_client_encoding_candidates():
            try:
                c = open_db_connection(client_encoding=enc, statement_timeout_ms=120000)
                persist_working_client_encoding(enc)
                return c
            except Exception:
                continue
        return open_db_connection(statement_timeout_ms=120000)

    try:
        with app.app_context():
            conn = abrir_conexao()
            try:
                # Ler config da migração UMA VEZ
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT tabela_origem, tabela_destino, campo_id,
                               sql_colunas_insert, sql_colunas_select
                        FROM controle_migracao WHERE id = %s
                    """, (migracao_id,))
                    cfg = cur.fetchone()
                conn.commit()

                if not cfg:
                    set_background_job(job_id, {
                        **get_background_job(job_id),
                        'status': 'erro',
                        'mensagem': 'Migração não encontrada.',
                    })
                    return

                tab_orig = cfg['tabela_origem']
                tab_dest = cfg['tabela_destino']
                campo = cfg['campo_id']
                cols_ins = cfg['sql_colunas_insert']
                cols_sel = cfg['sql_colunas_select']

                if not cols_ins or not cols_sel:
                    set_background_job(job_id, {
                        **get_background_job(job_id),
                        'status': 'erro',
                        'mensagem': 'Colunas não preparadas. Atualize a origem primeiro.',
                    })
                    return

                # Montar lista de IDs a processar
                if apenas_erros:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("""SELECT chave_valor FROM controle_migracao_lotes
                                       WHERE migracao_id = %s AND status = 'erro'
                                       ORDER BY id""", (migracao_id,))
                        ids_list = [r['chave_valor'] for r in cur.fetchall()]
                    conn.commit()
                    total_faixa = len(ids_list)
                    if total_faixa == 0:
                        set_background_job(job_id, {
                            **get_background_job(job_id),
                            'status': 'concluido',
                            'mensagem': 'Nenhum registro com erro encontrado.',
                            'registros_inseridos': 0,
                            'fim': datetime.now().isoformat(),
                        })
                        return
                else:
                    ids_list = [str(v) for v in range(id_inicio, id_fim + 1)]

                # Staging vazia é criada UMA vez; cada lote só troca o conteúdo
                ensure_temp_schema(conn)                                     # garante schema temp
                staging = preparar_staging_vazia(conn, migracao_id, tab_dest, cols_ins)  # cria estrutura

                total_lotes = math.ceil(len(ids_list) / tamanho_lote)

                for lote_idx in range(total_lotes):
                    current_job = get_background_job(job_id)
                    if not current_job or current_job.get('cancelar'):
                        set_background_job(job_id, {
                            **(get_background_job(job_id) or {}),
                            'status': 'cancelado',
                            'mensagem': f'Interrompido pelo usuário no lote {lote_idx + 1}/{total_lotes}.',
                            'ids_verificados': lote_idx * tamanho_lote,
                            'total_faixa': len(ids_list),
                            'registros_inseridos': registros_inseridos,
                            'registros_ja_existem': registros_ja_existem,
                            'registros_atualizados': registros_atualizados,
                            'registros_nao_encontrados': registros_nao_encontrados,
                            'registros_com_erro': registros_com_erro,
                        })
                        return

                    ids_lote = ids_list[lote_idx * tamanho_lote: (lote_idx + 1) * tamanho_lote]

                    set_background_job(job_id, {
                        **get_background_job(job_id),
                        'mensagem': f'Lote {lote_idx + 1}/{total_lotes}: copiando {len(ids_lote)} registro(s) da origem...',
                    })

                    # 1) Copia o lote para staging — reconecta à origem se a conexão cair
                    conn = executar_com_reconexao(
                        lambda c: copiar_lote_para_staging(c, staging, tab_orig, campo, cols_ins, ids_lote),
                        abrir_conexao, conn, job_id=job_id, etapa='origem'
                    )

                    # 2) Upsert em lote (caminho rápido); se falhar, cai pro fallback linha a linha
                    try:
                        resultado = processar_lote_bulk(conn, staging, tab_dest, campo, cols_ins, ids_lote)
                    except Exception:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        set_background_job(job_id, {
                            **get_background_job(job_id),
                            'mensagem': f'Lote {lote_idx + 1}/{total_lotes}: upsert em lote falhou, reprocessando registro a registro...',
                        })
                        resultado = processar_lote_fallback(conn, staging, tab_dest, campo, cols_ins, ids_lote)

                    # 3) Grava o status do lote inteiro em uma única instrução
                    gravar_status_lote(conn, migracao_id, resultado)

                    registros_inseridos += len(resultado['inseridos'])
                    registros_atualizados += len(resultado['atualizados'])
                    registros_ja_existem += len(resultado['ja_existem'])
                    registros_nao_encontrados += len(resultado['nao_encontrados'])
                    registros_com_erro.extend(resultado['com_erro'])

                    ids_verificados = min((lote_idx + 1) * tamanho_lote, len(ids_list))

                    # Atualizar registros_processados no banco a cada lote (não mais a cada registro)
                    try:
                        with conn.cursor() as cur:
                            cur.execute("""
                                UPDATE controle_migracao
                                SET registros_processados = (
                                        SELECT COUNT(*) FROM controle_migracao_lotes
                                        WHERE migracao_id = %s AND status IN ('inserido', 'ja_existe', 'atualizado')
                                    ),
                                    lote_atual = %s,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE id = %s
                            """, (migracao_id, lote_idx + 1, migracao_id))
                        conn.commit()
                    except Exception:
                        try:
                            conn.rollback()
                        except Exception:
                            pass

                    set_background_job(job_id, {
                        **get_background_job(job_id),
                        'status': 'processando',
                        'ids_verificados': ids_verificados,
                        'total_faixa': len(ids_list),
                        'registros_inseridos': registros_inseridos,
                        'registros_ja_existem': registros_ja_existem,
                        'registros_atualizados': registros_atualizados,
                        'registros_nao_encontrados': registros_nao_encontrados,
                        'registros_com_erro': registros_com_erro,
                        'registro_atual': ids_lote[-1] if ids_lote else None,
                        'registros_processados': registros_inseridos + registros_ja_existem + registros_atualizados,
                    })

                    # Delay entre LOTES (não mais entre cada registro) — bem mais rápido
                    if lote_idx < total_lotes - 1 and delay_segundos > 0:
                        for _ in range(delay_segundos * 10):
                            cj = get_background_job(job_id)
                            if not cj or cj.get('cancelar'):
                                break
                            time.sleep(0.1)

            finally:
                fechar_conexao_origem_da_thread()
                # Staging só existe durante o processamento desta migração
                try:
                    remover_staging(conn, migracao_id)  # limpa a tabela temp
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass

    except Exception as e:
        set_background_job(job_id, {
            **(get_background_job(job_id) or {}),
            'status': 'erro',
            'mensagem': mensagem_de_erro(e, 'Erro fatal no worker'),
            'registros_inseridos': registros_inseridos,
            'registros_atualizados': registros_atualizados,
            'registros_com_erro': registros_com_erro,
        })
        return

    partes = []
    if registros_inseridos > 0:
        partes.append(f'{registros_inseridos} inserido(s)')
    if registros_atualizados > 0:
        partes.append(f'{registros_atualizados} atualizado(s)')
    if registros_ja_existem > 0:
        partes.append(f'{registros_ja_existem} já existente(s)')
    if registros_nao_encontrados > 0:
        partes.append(f'{registros_nao_encontrados} não encontrado(s)')
    if registros_com_erro:
        partes.append(f'{len(registros_com_erro)} com erro')
    final_msg = 'Processamento concluído: ' + (', '.join(partes) if partes else 'nenhum registro processado') + '.'

    set_background_job(job_id, {
        **get_background_job(job_id),
        'status': 'concluido',
        'mensagem': final_msg,
        'ids_verificados': total_faixa,
        'total_faixa': total_faixa,
        'registros_inseridos': registros_inseridos,
        'registros_ja_existem': registros_ja_existem,
        'registros_atualizados': registros_atualizados,
        'registros_nao_encontrados': registros_nao_encontrados,
        'registros_com_erro': registros_com_erro,
        'fim': datetime.now().isoformat(),
    })


# ============================================================
# ENCRIPTAÇÃO DAS SENHAS PERSISTIDAS EM DISCO
# ============================================================
# As senhas do banco de destino e do FDW precisam ser recuperáveis (o app
# conecta com elas), então usamos encriptação simétrica (Fernet/AES), não
# hash. A chave vem de APP_ENCRYPTION_KEY (recomendado em produção) ou,
# na ausência dela, de um arquivo local gerado na primeira execução.
# Em memória (app.config) as senhas continuam em texto puro — só o que vai
# para os arquivos .json em disco é encriptado.

ENCRYPTION_KEY_FILE = os.path.join(APP_DATA_DIR, '.encryption_key')


def _carregar_ou_criar_chave_encriptacao():
    """Retorna a chave Fernet a usar: de env var, de arquivo local, ou gera uma nova."""
    chave_env = os.getenv('APP_ENCRYPTION_KEY')
    if chave_env:
        return chave_env.encode('ascii')

    os.makedirs(APP_DATA_DIR, exist_ok=True)  # ensure_app_data_dir() ainda não foi definida aqui
    if os.path.exists(ENCRYPTION_KEY_FILE):
        with open(ENCRYPTION_KEY_FILE, 'rb') as file_handle:
            return file_handle.read().strip()

    nova_chave = Fernet.generate_key()
    with open(ENCRYPTION_KEY_FILE, 'wb') as file_handle:
        file_handle.write(nova_chave)
    os.chmod(ENCRYPTION_KEY_FILE, 0o600)  # só o dono do processo pode ler
    return nova_chave


_fernet = Fernet(_carregar_ou_criar_chave_encriptacao())


def encrypt_secret(valor_texto_puro):
    """Encripta uma string para persistir em disco. Vazio/None passa direto."""
    if not valor_texto_puro:
        return valor_texto_puro
    return _fernet.encrypt(valor_texto_puro.encode('utf-8')).decode('ascii')


def decrypt_secret_or_plain(valor):
    """Descriptografa um valor salvo em disco. Se não for um token válido,
    assume que é uma senha antiga ainda em texto puro (de antes desta
    migração) e devolve como está, sem quebrar a conexão."""
    if not valor:
        return valor
    try:
        return _fernet.decrypt(valor.encode('ascii')).decode('utf-8')
    except (InvalidToken, ValueError, UnicodeDecodeError):
        return valor


def _config_com_senha_encriptada(config_dict):
    """Cópia do dict de configuração com 'password' encriptado, para salvar em
    disco sem alterar o dict em memória (que precisa continuar em texto puro)."""
    copia = dict(config_dict)
    if copia.get('password'):
        copia['password'] = encrypt_secret(copia['password'])
    return copia


# ============================================================
# FUNÇÕES DE CONEXÃO COM O BANCO
# ============================================================

def get_db_config():
    """Retorna a configuração atual de banco em memória."""
    if not app.config.get('DB_CONFIG_LOADED'):
        stored_config = load_json_file(ACTIVE_DB_CONFIG_FILE, default={})
        if isinstance(stored_config, dict) and stored_config:
            merged_config = DEFAULT_DB_CONFIG.copy()
            merged_config.update(stored_config)
            merged_config['password'] = decrypt_secret_or_plain(merged_config.get('password'))
            app.config['DB_CONFIG'] = merged_config
        app.config['DB_CONFIG_LOADED'] = True

    return app.config.setdefault('DB_CONFIG', DEFAULT_DB_CONFIG.copy())


def ensure_app_data_dir():
    """Garante a existência do diretório de persistência local da aplicação."""
    os.makedirs(APP_DATA_DIR, exist_ok=True)


def load_json_file(file_path, default):
    """Lê um arquivo JSON local retornando um valor padrão em caso de ausência ou erro."""
    if not os.path.exists(file_path):
        return default

    try:
        with open(file_path, 'r', encoding='utf-8') as file_handle:
            return json.load(file_handle)
    except (OSError, json.JSONDecodeError):
        return default


def save_json_file(file_path, data):
    """Salva um arquivo JSON local com indentação estável."""
    ensure_app_data_dir()
    with open(file_path, 'w', encoding='utf-8') as file_handle:
        json.dump(data, file_handle, ensure_ascii=False, indent=2)


def persist_saved_db_connections():
    """Persiste a lista de conexões salvas em disco (senhas encriptadas)."""
    conexoes_para_salvar = [_config_com_senha_encriptada(c) for c in get_saved_db_connections()]
    save_json_file(SAVED_CONNECTIONS_FILE, conexoes_para_salvar)


def persist_active_db_config():
    """Persiste a configuração ativa de banco em disco (senha encriptada)."""
    save_json_file(ACTIVE_DB_CONFIG_FILE, _config_com_senha_encriptada(get_db_config()))


def get_fdw_config():
    """Retorna a configuração atual do FDW (origem) em memória."""
    if not app.config.get('FDW_CONFIG_LOADED'):
        stored_config = load_json_file(FDW_CONFIG_FILE, default={})
        if isinstance(stored_config, dict) and stored_config:
            merged_config = DEFAULT_FDW_CONFIG.copy()
            merged_config.update(stored_config)
            merged_config['password'] = decrypt_secret_or_plain(merged_config.get('password'))
            app.config['FDW_CONFIG'] = merged_config
        app.config['FDW_CONFIG_LOADED'] = True

    return app.config.setdefault('FDW_CONFIG', DEFAULT_FDW_CONFIG.copy())


def persist_fdw_config(fdw_config):
    """Persiste a configuração do FDW em disco (senha encriptada), mantendo a
    versão em texto puro em memória (usada pra conectar)."""
    app.config['FDW_CONFIG'] = fdw_config
    app.config['FDW_CONFIG_LOADED'] = True
    save_json_file(FDW_CONFIG_FILE, _config_com_senha_encriptada(fdw_config))


def get_fdw_tabelas():
    """Última lista de tabelas da origem salva pelo "Testar conexão FDW"
    (reaproveitada nas sugestões da Nova Migração). {} se nunca testou."""
    dados = load_json_file(FDW_TABELAS_FILE, default={})
    return dados if isinstance(dados, dict) else {}


def persist_fdw_tabelas(dados):
    save_json_file(FDW_TABELAS_FILE, dados)


def sugerir_mapeamento_migracao(tabela):
    """Origem/destino/campo ID sugeridos pra uma tabela da origem: destino com
    o prefixo v_ trocado por t_ (v_paciente -> t_paciente) e, como chave, a
    primeira coluna isn_* (ou a primeira coluna, se não houver)."""
    nome = tabela['nome']                     # 'v_x' ou 'outro_schema.v_x'
    colunas = tabela.get('colunas') or []
    base = nome.split('.')[-1]
    destino = 't_' + base[2:] if base.lower().startswith('v_') else base
    campo_id = next((c for c in colunas if c.lower().startswith('isn_')), colunas[0] if colunas else '')
    return {'origem': nome, 'destino': destino, 'campo_id': campo_id, 'tipo': tabela.get('tipo'),
            'sem_permissao': bool(tabela.get('sem_permissao'))}


def relacao_origem_da_lista(item_ou_nome):
    """'schema.tabela' de um item da lista da origem (ou do nome digitado)."""
    nome = item_ou_nome['nome'] if isinstance(item_ou_nome, dict) else item_ou_nome
    if '.' in nome:
        return nome
    return f'{(get_fdw_config().get("schema_remoto") or "public").strip()}.{nome}'


def get_mapeamentos_fdw():
    """Mapeamentos sugeridos pra todas as tabelas da última lista do FDW."""
    return [sugerir_mapeamento_migracao(t) for t in get_fdw_tabelas().get('tabelas') or []]


# ============================================================
# ORIGEM: leitura direta do servidor de origem (sem FDW)
# ============================================================
# O sistema conecta direto no servidor de origem (credenciais da seção
# "Servidor de origem" em Configurar Banco) e copia cada lote com
# COPY (SELECT ... WHERE chave = ANY(ids)) -> staging local. Não existe mais
# tabela estrangeira nem schema local do FDW: a origem é sempre
# "<schema remoto>.<tabela>" no servidor de origem.

def mensagem_de_erro(erro, contexto=''):
    """Texto curto pro usuário: só a mensagem principal do PostgreSQL (e a dica,
    se houver), sem os trechos de SQL (LINE/QUERY/CONTEXT). O erro completo
    vai pro log do sistema (docker logs)."""
    diag = getattr(erro, 'diag', None)
    if isinstance(erro, psycopg2.Error) and diag is not None and diag.message_primary:
        curta = diag.message_primary
        if diag.message_hint:
            curta += f' (dica: {diag.message_hint})'
        app.logger.warning('%s%s', f'{contexto}: ' if contexto else '', str(erro).strip())
    else:
        linhas = str(erro).strip().splitlines()
        curta = linhas[0] if linhas else erro.__class__.__name__
        if len(linhas) > 1:
            app.logger.warning('%s%s', f'{contexto}: ' if contexto else '', str(erro).strip())
    return f'{contexto}: {curta}' if contexto else curta


def normalizar_origem(relacao_raw):
    """'v_x', 'guardiao.v_x' ou o nome antigo 'integrasesa.v_x' (schema local
    do FDW) -> '<schema remoto>.v_x'."""
    fdw = get_fdw_config()
    schema_remoto = (fdw.get('schema_remoto') or 'public').strip()
    relacao = normalize_relation_input(relacao_raw, 'Tabela Origem', schema_remoto)
    schema, tabela = relacao.split('.', 1)
    schema_legado = (fdw.get('schema_local') or '').strip().lower()
    if schema_legado and schema.lower() == schema_legado:
        schema = schema_remoto
    return f'{schema}.{tabela}'


def abrir_conexao_origem(statement_timeout_ms=120000):
    """Conexão direta com o servidor de origem."""
    fdw = get_fdw_config()
    if not (fdw.get('host') and fdw.get('database') and fdw.get('user')):
        raise ValueError('Servidor de origem não configurado: preencha e salve a seção "Servidor de origem" em Configurar Banco.')
    conn = psycopg2.connect(
        host=fdw['host'], port=int(fdw.get('port') or 5432), dbname=fdw['database'],
        user=fdw['user'], password=fdw.get('password') or '',
        connect_timeout=int(os.getenv('DB_TEST_CONNECT_TIMEOUT', '8')),
        keepalives=1, keepalives_idle=15, keepalives_interval=5, keepalives_count=5,
        application_name='importacao_intergash',
        options=f'-c statement_timeout={int(statement_timeout_ms)}',
    )
    conn.autocommit = True   # só leitura: não segura snapshot entre os lotes
    with conn.cursor() as cur:
        cur.execute('SHOW server_encoding')
        codificacao = cur.fetchone()[0]
    # SQL_ASCII não converte nada: lê os bytes como LATIN1 (nunca falha)
    conn.set_client_encoding('LATIN1' if codificacao.upper() == 'SQL_ASCII' else 'UTF8')
    return conn


# Uma conexão com a origem por thread (worker), reaproveitada entre os lotes
_origem_por_thread = threading.local()


def conexao_origem_da_thread():
    conn = getattr(_origem_por_thread, 'conn', None)
    if conn is None or conn.closed:
        conn = abrir_conexao_origem()
        _origem_por_thread.conn = conn
    return conn


def fechar_conexao_origem_da_thread():
    conn = getattr(_origem_por_thread, 'conn', None)
    _origem_por_thread.conn = None
    if conn is not None:
        try:
            conn.close()
        except Exception:  # noqa: BLE001 - já caiu: nada a fazer
            pass


def consultar_origem(sql, params=None):
    """SELECT na origem pela conexão da thread; se a conexão caiu, descarta
    pra próxima chamada reabrir (e propaga o erro pro retry de quem chamou)."""
    conn = conexao_origem_da_thread()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    except (OperationalError, InterfaceError):
        fechar_conexao_origem_da_thread()
        raise


def colunas_origem(relacao):
    """[{'nome', 'tipo'}] da tabela/view na origem, na ordem. ValueError se não existir."""
    schema, tabela = relacao.split('.', 1)
    rows = consultar_origem("""
        SELECT a.attname AS nome, format_type(a.atttypid, a.atttypmod) AS tipo
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
    """, (schema, tabela))
    if not rows:
        raise ValueError(
            f'A tabela "{tabela}" não existe no schema "{schema}" do servidor de origem '
            f'(ou o usuário da origem não tem acesso a ela).'
        )
    return [{'nome': r['nome'], 'tipo': r['tipo']} for r in rows]


def validar_origem(relacao):
    """Confere na origem que a tabela existe e que o usuário da origem pode
    lê-la (ValueError com mensagem se não)."""
    try:
        colunas = colunas_origem(relacao)
        schema, tabela = relacao.split('.', 1)
        permissao = consultar_origem("""
            SELECT has_schema_privilege(%s, 'USAGE')
                   AND has_table_privilege(format('%%I.%%I', %s, %s), 'SELECT') AS pode_ler,
                   current_user AS usuario
        """, (schema, schema, tabela))[0]
    finally:
        fechar_conexao_origem_da_thread()
    if not permissao['pode_ler']:
        raise ValueError(
            f'O usuário "{permissao["usuario"]}" da origem não tem permissão de leitura em {relacao}. '
            f'Peça ao DBA da origem: GRANT SELECT ON {relacao} TO "{permissao["usuario"]}".'
        )
    return colunas


def max_id_origem(relacao, chave):
    row = consultar_origem(f'SELECT MAX({quote_identifier(chave)}) AS ultimo FROM {quote_qualified_relation(relacao)}')
    return int(row[0]['ultimo']) if row and row[0]['ultimo'] is not None else 0


def _desquotar(identificador):
    """'"Nome"' -> 'Nome'; 'nome' -> 'nome' (colunas vêm com ou sem aspas)."""
    ident = (identificador or '').strip()
    if len(ident) >= 2 and ident.startswith('"') and ident.endswith('"'):
        return ident[1:-1].replace('""', '"')
    return ident


def _ident_sql(nome):
    """Como o %I do format(): só põe aspas quando precisa."""
    return nome if re.fullmatch(r'[a-z_][a-z0-9_$]*', nome) else quote_identifier(nome)


def copiar_ids_da_origem(conn_local, staging, relacao, chave, colunas, ids):
    """Troca o conteúdo da staging local pelas linhas da origem com esses IDs
    (COPY origem -> memória -> COPY staging). `colunas` com ou sem aspas.

    Os bytes passam crus, sem decodificar no Python: o COPY local informa o
    encoding da origem (ENCODING ...) e o próprio PostgreSQL converte — assim
    o client_encoding da conexão local (ex.: WIN1252) não limita os
    caracteres que vêm da origem."""
    nomes = [quote_identifier(_desquotar(c)) for c in colunas]
    lista = ', '.join(nomes)
    ids_int = [int(v) for v in ids]
    conn_origem = conexao_origem_da_thread()
    buffer = io.BytesIO()
    try:
        with conn_origem.cursor() as cur:
            consulta = cur.mogrify(
                f'SELECT {", ".join("s." + n for n in nomes)} FROM {quote_qualified_relation(relacao)} s '
                f'WHERE s.{quote_identifier(_desquotar(chave))} = ANY(%s)', (ids_int,)
            ).decode(psycopg2.extensions.encodings.get(conn_origem.encoding, 'utf-8'))
            cur.copy_expert(f'COPY ({consulta}) TO STDOUT', buffer)
    except (OperationalError, InterfaceError):
        fechar_conexao_origem_da_thread()
        raise
    buffer.seek(0)
    with conn_local.cursor() as cur:
        cur.execute(f'TRUNCATE TABLE {staging}')
        cur.copy_expert(f"COPY {staging} ({lista}) FROM STDIN WITH (ENCODING '{conn_origem.encoding}')", buffer)
    conn_local.commit()


def criar_staging_como_destino(conn, staging, destino, colunas):
    """Staging vazia com as colunas (e tipos) do DESTINO — o COPY da origem
    converte os valores pros tipos do destino já na carga."""
    lista = ', '.join(quote_identifier(_desquotar(c)) for c in colunas)
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS {staging}')
        cur.execute(f'CREATE UNLOGGED TABLE {staging} AS SELECT {lista} FROM {quote_qualified_relation(destino)} WHERE FALSE')
    conn.commit()


def preparar_colunas_origem_destino(tab_orig, tab_dest, campo):
    """Substitui a antiga função SQL preparar_colunas_migracao (que só lia
    tabela local): colunas em comum origem x destino (sem geradas/identity/
    serial, exceto a chave), mais COUNT e MAX(chave) lidos na origem."""
    relacao_origem = normalizar_origem(tab_orig)
    destino = normalize_relation_input(tab_dest, 'Tabela Destino', get_db_config()['schema_destino'])
    nomes_origem = {c['nome'] for c in colunas_origem(relacao_origem)}

    colunas_destino = execute_query("""
        SELECT a.attname AS nome, a.attidentity AS identidade, a.attgenerated AS gerada,
               pg_get_expr(ad.adbin, ad.adrelid) AS padrao
        FROM pg_attribute a
        LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
        WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
    """, (destino,))
    if not colunas_destino:
        raise ValueError(f'Tabela de destino "{destino}" não encontrada.')
    if campo not in nomes_origem:
        raise ValueError(f'A coluna chave "{campo}" não existe na tabela de origem {relacao_origem}.')
    if not any(c['nome'] == campo for c in colunas_destino):
        raise ValueError(f'A coluna chave "{campo}" não existe na tabela de destino {destino}.')

    comuns = [
        c['nome'] for c in colunas_destino
        if c['nome'] in nomes_origem and (
            c['nome'] == campo or not (
                c['gerada'] or c['identidade'] in ('a', 'd') or (c['padrao'] or '').startswith('nextval(')
            )
        )
    ]
    sem_chave = [c for c in comuns if c != campo]
    total = consultar_origem(
        f'SELECT COUNT(*) AS total, MAX({quote_identifier(campo)}) AS maximo FROM {quote_qualified_relation(relacao_origem)}'
    )[0]
    return {
        'sql_colunas_insert': ', '.join(_ident_sql(c) for c in comuns),
        'sql_colunas_select': ', '.join(f's.{_ident_sql(c)}' for c in comuns),
        'sql_colunas_update': ', '.join(f'{_ident_sql(c)} = EXCLUDED.{_ident_sql(c)}' for c in sem_chave) or None,
        'sql_colunas_update_where': ' OR '.join(f'd.{_ident_sql(c)} IS DISTINCT FROM EXCLUDED.{_ident_sql(c)}' for c in sem_chave) or None,
        'total_registros': int(total['total'] or 0),
        'id_maximo': int(total['maximo'] or 0),
    }


# ============================================================
# LOGO DO SISTEMA
# ============================================================
# A logo é uma variável do sistema (`logo_url` nos templates): a enviada pela
# tela Configurar Banco fica em data/ (volume persistido do Docker); sem
# nenhuma enviada, vale a logo padrão em static/.

LOGO_PADRAO = os.path.join(app.root_path, 'static', 'logo_hcasg.png')
LOGO_NOME_ARQUIVO = 'logo_sistema'
LOGO_MAX_BYTES = 2 * 1024 * 1024
# Só formatos de imagem raster, conferidos pelo conteúdo (não pela extensão).
# SVG fica de fora: pode carregar script e seria servido pelo mesmo domínio.
LOGO_ASSINATURAS = (
    ('png', lambda b: b.startswith(b'\x89PNG\r\n\x1a\n')),
    ('jpg', lambda b: b.startswith(b'\xff\xd8\xff')),
    ('gif', lambda b: b[:6] in (b'GIF87a', b'GIF89a')),
    ('webp', lambda b: b[:4] == b'RIFF' and b[8:12] == b'WEBP'),
)


def caminho_logo_personalizada():
    """Caminho da logo enviada pelo administrador, ou None se não houver."""
    for extensao, _ in LOGO_ASSINATURAS:
        caminho = os.path.join(APP_DATA_DIR, f'{LOGO_NOME_ARQUIVO}.{extensao}')
        if os.path.isfile(caminho):
            return caminho
    return None


def caminho_logo_atual():
    return caminho_logo_personalizada() or LOGO_PADRAO


def salvar_logo(conteudo):
    """Valida e grava a nova logo. Levanta ValueError com mensagem pro usuário."""
    if not conteudo:
        raise ValueError('Selecione um arquivo de imagem.')
    if len(conteudo) > LOGO_MAX_BYTES:
        raise ValueError(f'A imagem passa do limite de {LOGO_MAX_BYTES // (1024 * 1024)} MB.')
    extensao = next((ext for ext, confere in LOGO_ASSINATURAS if confere(conteudo)), None)
    if extensao is None:
        raise ValueError('Formato não suportado. Envie PNG, JPG, GIF ou WEBP.')

    os.makedirs(APP_DATA_DIR, exist_ok=True)
    destino = os.path.join(APP_DATA_DIR, f'{LOGO_NOME_ARQUIVO}.{extensao}')
    temporario = destino + '.tmp'
    with open(temporario, 'wb') as arquivo:
        arquivo.write(conteudo)
    os.replace(temporario, destino)  # troca atômica: nunca serve meio arquivo

    # Remove a logo anterior se ela era de outro formato
    for ext, _ in LOGO_ASSINATURAS:
        antigo = os.path.join(APP_DATA_DIR, f'{LOGO_NOME_ARQUIVO}.{ext}')
        if ext != extensao and os.path.isfile(antigo):
            os.remove(antigo)


def restaurar_logo_padrao():
    caminho = caminho_logo_personalizada()
    while caminho:
        os.remove(caminho)
        caminho = caminho_logo_personalizada()


# Versão do código em execução: data do importacao.py (o COPY do Docker
# preserva a data) — mostrada no rodapé pra conferir se um servidor está
# com o código atualizado.
try:
    VERSAO_SISTEMA = datetime.fromtimestamp(os.path.getmtime(__file__)).strftime('%d/%m/%Y %H:%M')
except OSError:
    VERSAO_SISTEMA = '?'


@app.context_processor
def inject_versao():
    return {'versao_sistema': VERSAO_SISTEMA}


@app.context_processor
def inject_logo():
    """`logo_url` para todos os templates — com a data do arquivo na URL pra
    o navegador não continuar mostrando a logo antiga do cache."""
    try:
        versao = int(os.path.getmtime(caminho_logo_atual()))
    except OSError:
        versao = 0
    return {
        'logo_url': url_for('logo_sistema', v=versao),
        'logo_personalizada': caminho_logo_personalizada() is not None,
    }


# ============================================================
# USUÁRIOS E PERFIS DE ACESSO
# ============================================================
# Perfis suportados:
#   'usuario'        -> acessa Dashboard, Nova Migração, Migrações, Importação Automática
#   'administrador'  -> acessa tudo, incluindo Configurar Banco e esta tela de Usuários

PERFIS_VALIDOS = ('usuario', 'administrador')


def get_users():
    """Retorna a lista de usuários cadastrados (sem cache — arquivo pequeno)."""
    users = load_json_file(USERS_FILE, default=[])
    return users if isinstance(users, list) else []


def save_users(users):
    """Persiste a lista de usuários em disco."""
    save_json_file(USERS_FILE, users)


def find_user_by_username(username):
    """Busca um usuário pelo nome (case-insensitive)."""
    alvo = (username or '').strip().lower()
    if not alvo:
        return None
    for user in get_users():
        if user.get('username', '').lower() == alvo:
            return user
    return None


def criar_usuario(username, password, perfil):
    """Valida e cria um novo usuário. Levanta ValueError em caso de dado inválido."""
    username = (username or '').strip()
    if not username:
        raise ValueError('Informe o nome de usuário.')
    if not password:
        raise ValueError('Informe a senha.')
    if perfil not in PERFIS_VALIDOS:
        raise ValueError('Perfil inválido.')
    if find_user_by_username(username):
        raise ValueError(f'Já existe um usuário com o nome "{username}".')

    users = get_users()
    next_id = max([u['id'] for u in users], default=0) + 1
    novo_usuario = {
        'id': next_id,
        'username': username,
        'password_hash': generate_password_hash(password),
        'perfil': perfil,
        'created_at': datetime.now().isoformat(),
    }
    users.append(novo_usuario)
    save_users(users)
    return novo_usuario


def verificar_login(username, password):
    """Retorna o usuário se as credenciais forem válidas, senão None."""
    user = find_user_by_username(username)
    if not user or not check_password_hash(user.get('password_hash', ''), password or ''):
        return None
    return user


def excluir_usuario(user_id):
    """Remove um usuário pelo id. Levanta ValueError se não encontrado."""
    users = get_users()
    restantes = [u for u in users if u['id'] != user_id]
    if len(restantes) == len(users):
        raise ValueError('Usuário não encontrado.')
    save_users(restantes)


# Endpoints acessíveis sem login (nome da view Flask, não a URL)
PUBLIC_ENDPOINTS = {'login', 'logout', 'static', 'logo_sistema'}

# Endpoints restritos ao perfil administrador
ADMIN_ONLY_ENDPOINTS = {
    'configurar_banco', 'usar_conexao_salva', 'excluir_conexao_salva',
    'testar_conexao_banco', 'testar_fdw_route', 'alterar_logo_route', 'restaurar_logo_route',
    'criar_banco_local_route', 'salvar_origem_route', 'remover_fdw_local_route', 'criar_schema_temp_route', 'revisar_tabelas_route',
    'usuarios', 'excluir_usuario_route',
}


@app.before_request
def exigir_login():
    """Bloqueia acesso a quem não está logado; restringe telas de admin."""
    if request.endpoint is None or request.endpoint in PUBLIC_ENDPOINTS:
        return None

    # Bootstrap: enquanto não existir NENHUM usuário, libera a tela de Usuários
    # sem exigir login, só para permitir criar o primeiro administrador.
    if request.endpoint in ('usuarios', 'excluir_usuario_route') and not get_users():
        return None

    if not session.get('usuario_id'):
        if request.path.startswith('/api/'):
            return jsonify({'status': 'erro', 'mensagem': 'Faça login para continuar.'}), 401
        return redirect(url_for('login', next=request.path))

    if request.endpoint in ADMIN_ONLY_ENDPOINTS and session.get('perfil') != 'administrador':
        if request.path.startswith('/api/'):
            return jsonify({'status': 'erro', 'mensagem': 'Acesso restrito a administradores.'}), 403
        flash('Acesso restrito a administradores.', 'error')
        return redirect(url_for('dashboard'))

    return None


def get_db_connection_config():
    """Retorna apenas as opções válidas para abertura da conexão PostgreSQL."""
    db_config = get_db_config()
    connection_config = {key: db_config.get(key) for key in DB_CONNECT_KEYS}
    connection_config.update({
        'connect_timeout': int(os.getenv('DB_CONNECT_TIMEOUT', '30')),
        'keepalives': 1,
        'keepalives_idle': int(os.getenv('DB_KEEPALIVES_IDLE', '15')),
        'keepalives_interval': int(os.getenv('DB_KEEPALIVES_INTERVAL', '5')),
        'keepalives_count': int(os.getenv('DB_KEEPALIVES_COUNT', '5')),
        'application_name': 'importacao_intergash'
    })
    return connection_config


def get_db_client_encoding_candidates():
    """Retorna a ordem de encodings a testar na conexão com o banco."""
    configured_encoding = (get_db_config().get('client_encoding') or '').strip().upper()
    candidates = []

    if configured_encoding and configured_encoding != DB_CLIENT_ENCODING_ULTIMO_RECURSO:
        candidates.append(configured_encoding)

    for encoding in DB_CLIENT_ENCODING_CANDIDATES:
        if encoding not in candidates:
            candidates.append(encoding)

    return candidates


def get_saved_db_connections():
    """Retorna a lista de conexões salvas, carregando-a do disco quando necessário."""
    if not app.config.get('DB_SAVED_CONNECTIONS_LOADED'):
        stored_connections = load_json_file(SAVED_CONNECTIONS_FILE, default=[])
        if isinstance(stored_connections, list):
            for connection in stored_connections:
                if isinstance(connection, dict) and connection.get('password'):
                    connection['password'] = decrypt_secret_or_plain(connection['password'])
            app.config['DB_SAVED_CONNECTIONS'] = stored_connections
        else:
            app.config['DB_SAVED_CONNECTIONS'] = []
        app.config['DB_SAVED_CONNECTIONS_LOADED'] = True

    return app.config.setdefault('DB_SAVED_CONNECTIONS', [])


def get_db_connection_by_id(connection_id):
    """Busca um perfil salvo pelo identificador."""
    for connection in get_saved_db_connections():
        if connection['id'] == connection_id:
            return connection
    return None


def sanitize_db_connection(connection):
    """Remove dados sensíveis para exibição em template."""
    sanitized = connection.copy()
    sanitized['password'] = ''
    return sanitized


def get_db_form_data(db_config=None):
    """Monta os dados exibidos no formulário de conexão."""
    current_config = (db_config or get_db_config()).copy()
    current_config['connection_mode'] = (
        'local' if current_config.get('host') in ('localhost', '127.0.0.1', '') else 'remote'
    )
    current_config['password'] = ''
    current_config['profile_name'] = current_config.get('profile_name', '')
    current_config['profile_id'] = current_config.get('profile_id', '')
    return current_config


def get_posted_db_form_data(form_data):
    """Lê os dados enviados pelo formulário preservando valores para reexibição."""
    return {
        'profile_id': (form_data.get('profile_id') or '').strip(),
        'profile_name': (form_data.get('profile_name') or '').strip(),
        'connection_mode': (form_data.get('connection_mode') or 'local').strip().lower(),
        'host': (form_data.get('host') or 'localhost').strip(),
        'database': (form_data.get('database') or 'postgres').strip(),
        'user': (form_data.get('user') or '').strip(),
        'password': '',
        'port': (form_data.get('port') or '5432').strip(),
        'schema_destino': (form_data.get('schema_destino') or 'public').strip()
    }


def build_db_config(form_data):
    """Valida e normaliza os dados da conexão recebidos pelo formulário."""
    connection_mode = (form_data.get('connection_mode') or 'local').strip().lower()
    host = (form_data.get('host') or '').strip()
    if connection_mode == 'local' and not host:
        host = 'localhost'
    database = (form_data.get('database') or 'postgres').strip()
    user = (form_data.get('user') or '').strip()
    password = form_data.get('password') or ''
    port_raw = (form_data.get('port') or '5432').strip()
    schema_destino = (form_data.get('schema_destino') or 'public').strip()

    if not host:
        raise ValueError('Informe o IP ou host do banco remoto.')
    if not database:
        raise ValueError('Informe o nome do banco de dados.')
    if not user:
        raise ValueError('Informe o usuário do banco de dados.')
    if not password:
        raise ValueError('Informe a senha do banco de dados.')

    try:
        port = int(port_raw)
    except ValueError as exc:
        raise ValueError('Informe uma porta válida para o banco de dados.') from exc

    return {
        'profile_name': (form_data.get('profile_name') or '').strip(),
        'host': host,
        'database': database,
        'user': user,
        'password': password,
        'port': port,
        'schema_destino': schema_destino
    }


def save_db_connection_profile(db_config, profile_id=None):
    """Salva ou atualiza um perfil de conexão persistido localmente."""
    profile_name = (db_config.get('profile_name') or '').strip()
    if not profile_name:
        raise ValueError('Informe um nome para salvar a conexão na lista.')

    saved_connections = get_saved_db_connections()
    next_id = max([item['id'] for item in saved_connections], default=0) + 1

    profile_data = {
        'id': profile_id or next_id,
        'profile_name': profile_name,
        'host': db_config['host'],
        'database': db_config['database'],
        'user': db_config['user'],
        'password': db_config['password'],
        'port': db_config['port'],
        'schema_destino': db_config.get('schema_destino', 'public')
    }

    existing = get_db_connection_by_id(profile_data['id'])
    if existing:
        existing.update(profile_data)
        persist_saved_db_connections()
        return existing, False

    saved_connections.append(profile_data)
    persist_saved_db_connections()
    return profile_data, True


def create_migration_record(source_table, target_table, key_column, batch_size):
    """Cria o registro inicial da migração sem disparar leitura ou atualização de lotes."""
    result = execute_query(
        """
        INSERT INTO controle_migracao (
            tabela_origem,
            tabela_destino,
            campo_id,
            lote_atual,
            total_registros,
            registros_processados,
            tamanho_lote,
            id_maximo,
            status,
            data_inicio,
            erro_msg,
            updated_at
        ) VALUES (%s, %s, %s, 0, 0, 0, %s, 0, 'pendente', CURRENT_TIMESTAMP, NULL, CURRENT_TIMESTAMP)
        RETURNING id, total_registros, tamanho_lote
        """,
        (source_table, target_table, key_column, batch_size)
    )

    migration = result[0]
    migration['total_lotes'] = 0
    migration['migracao_id'] = migration['id']
    return migration


def update_migration_record(migracao_id, source_table, target_table, key_column):
    """Atualiza origem/destino/chave de uma migração existente e zera os totais
    e o mapeamento de colunas — quem recalcula é o refresh em segundo plano
    disparado logo em seguida, com base nas tabelas novas."""
    execute_query(
        """
        UPDATE controle_migracao
        SET tabela_origem = %s,
            tabela_destino = %s,
            campo_id = %s,
            sql_colunas_insert = NULL,
            sql_colunas_select = NULL,
            total_registros = 0,
            registros_processados = 0,
            id_maximo = 0,
            lote_atual = 0,
            status = 'pendente',
            erro_msg = NULL,
            data_fim = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """,
        (source_table, target_table, key_column, migracao_id),
        fetch=False
    )


def _background_refresh_batches_worker(job_id, migration_id):
    """Thread worker que prepara colunas e obtém totais da migração.

    Não carrega todos os IDs da origem — apenas obtém COUNT e MAX(id).
    O processamento sequencial é feito pelo worker de migração.
    """
    job = get_background_job(job_id)
    if not job:
        return

    set_background_job(job_id, {**job, 'status': 'processando', 'inicio': datetime.now().isoformat()})

    try:
        with app.app_context():
            set_background_job(job_id, {
                **get_background_job(job_id),
                'mensagem': 'Preparando colunas e obtendo totais da migração...',
            })

            # Limpar lotes anteriores (serão populados on-the-fly pelo processar_registro)
            execute_query(
                "DELETE FROM controle_migracao_lotes WHERE migracao_id = %s",
                (migration_id,),
                fetch=False
            )

            # Preparar colunas e obter COUNT + MAX(id) — lidos direto no
            # servidor de origem, sem carregar todos os IDs
            cfg = execute_query(
                "SELECT tabela_origem, tabela_destino, campo_id FROM controle_migracao WHERE id = %s",
                (migration_id,)
            )
            if not cfg:
                raise ValueError('Migração não encontrada.')
            try:
                info = preparar_colunas_origem_destino(cfg[0]['tabela_origem'], cfg[0]['tabela_destino'], cfg[0]['campo_id'])
            finally:
                fechar_conexao_origem_da_thread()
            execute_query(
                """UPDATE controle_migracao
                   SET sql_colunas_insert = %s, sql_colunas_select = %s,
                       sql_colunas_update = %s, sql_colunas_update_where = %s,
                       total_registros = %s, id_maximo = %s, updated_at = CURRENT_TIMESTAMP
                   WHERE id = %s""",
                (info['sql_colunas_insert'], info['sql_colunas_select'], info['sql_colunas_update'],
                 info['sql_colunas_update_where'], info['total_registros'], info['id_maximo'], migration_id),
                fetch=False
            )

            total_registros = info['total_registros']
            id_maximo = info['id_maximo']

            # Resetar contadores para nova execução
            execute_query(
                """UPDATE controle_migracao
                   SET registros_processados = 0, lote_atual = 0,
                       status = CASE WHEN %s = 0 THEN 'concluido' ELSE 'pendente' END,
                       erro_msg = NULL,
                       data_inicio = CURRENT_TIMESTAMP,
                       data_fim = CASE WHEN %s = 0 THEN CURRENT_TIMESTAMP ELSE NULL END,
                       updated_at = CURRENT_TIMESTAMP
                   WHERE id = %s""",
                (total_registros, total_registros, migration_id),
                fetch=False
            )

            set_background_job(job_id, {
                **get_background_job(job_id),
                'status': 'concluido',
                'mensagem': f'Migração preparada: {total_registros} registros, ID máximo: {id_maximo}.',
                'total_registros': total_registros,
                'total_lotes': total_registros,
                'fim': datetime.now().isoformat(),
            })
    except Exception as e:
        set_background_job(job_id, {
            **get_background_job(job_id),
            'status': 'erro',
            'mensagem': mensagem_de_erro(e, 'Erro ao preparar migração'),
            'fim': datetime.now().isoformat(),
        })


def start_background_refresh_batches(migration_id):
    """Inicia a geração de lotes em segundo plano e retorna imediatamente."""
    job_id = _generate_job_id('refresh', migration_id)
    existing = get_background_job(job_id)
    if existing and existing.get('status') in ('processando', 'iniciando'):
        return job_id

    set_background_job(job_id, {
        'job_id': job_id,
        'tipo': 'refresh',
        'migracao_id': migration_id,
        'status': 'iniciando',
        'mensagem': 'Carregando registros em segundo plano...',
        'registros_processados': 0,
        'registros_com_erro': [],
        'registro_atual': None,
        'total_registros_solicitados': 0,
        'ultimo_progresso': None,
        'cancelar': False,
    })

    thread = threading.Thread(
        target=_background_refresh_batches_worker,
        args=(job_id, migration_id),
        daemon=True
    )
    thread.start()
    return job_id


def is_db_configured():
    """Verifica se há credenciais suficientes para tentar abrir a conexão."""
    db_config = get_db_connection_config()
    return all(db_config.get(field) for field in DB_CONNECT_KEYS)


def handle_db_connection_error(error, api=False):
    """Padroniza a resposta para falhas de conexão com o banco."""
    message = 'Não foi possível conectar ao banco de dados com a configuração atual.'
    details = str(error)

    if api:
        return jsonify({
            'status': 'erro',
            'mensagem': message,
            'detalhes': details,
            'configurar_banco': url_for('configurar_banco')
        }), 503

    flash(f'{message} Detalhes: {details}', 'error')
    return redirect(url_for('configurar_banco'))


@app.context_processor
def inject_db_context():
    """Disponibiliza informações resumidas da conexão para os templates."""
    db_config = get_db_config()
    return {
        'db_configured': is_db_configured(),
        'db_host': db_config.get('host') or 'localhost',
        'db_name': db_config.get('database') or 'postgres'
    }

def get_search_path_option():
    """Monta a opção -c search_path com o schema de destino configurado na frente."""
    schema_destino = (get_db_config().get('schema_destino') or 'public').strip()
    if schema_destino == 'public':
        return '-c search_path=public'
    return f'-c search_path={schema_destino},public'


def get_db_connection():
    """Cria conexão com o banco PostgreSQL"""
    connection_config = get_db_connection_config()
    connection_config['options'] = get_search_path_option()
    return psycopg2.connect(**connection_config)


def open_db_connection(client_encoding=None, statement_timeout_ms=300000):
    """Abre a conexão e aplica client encoding explícito quando informado."""
    connection_config = get_db_connection_config()
    options_parts = [f'-c statement_timeout={statement_timeout_ms}', get_search_path_option()]
    if client_encoding:
        options_parts.append(f'-c client_encoding={client_encoding}')
    connection_config['options'] = ' '.join(options_parts)

    connection = psycopg2.connect(**connection_config)
    if client_encoding:
        connection.set_client_encoding(client_encoding)
    return connection


def persist_working_client_encoding(client_encoding):
    """Memoriza o encoding que funcionou para evitar novas tentativas desnecessárias."""
    if not client_encoding or client_encoding.upper() == DB_CLIENT_ENCODING_ULTIMO_RECURSO:
        return

    db_config = get_db_config()
    if db_config.get('client_encoding') != client_encoding:
        db_config['client_encoding'] = client_encoding
        persist_active_db_config()


def run_query_with_connection(conn, query, params=None, fetch=True):
    """Executa uma consulta usando uma conexão já aberta."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        if fetch:
            result = cur.fetchall()
        else:
            result = None
        return result


def _is_encoding_error(error):
    """Verifica se um erro de banco é relacionado a encoding/codificação (PT e EN)."""
    message = str(error).lower()
    patterns = (
        # Inglês
        'invalid byte sequence',
        'character with byte sequence',
        'untranslatable character',
        "codec can't decode",
        "codec can't encode",
        'unicode decode error',
        # Português (PostgreSQL com locale pt_BR)
        'sequ\u00eancia de bytes',
        'inv\u00e1lida para codifica',
        'caractere intraduz\u00edvel',
        'codifica\u00e7\u00e3o',
    )
    return any(p in message for p in patterns)


def _is_recovery_conflict(error):
    """Verifica se o erro é um conflito de recovery em standby PostgreSQL."""
    message = str(error).lower()
    patterns = (
        'conflict with recovery',
        'conflito com recupera',
        'terminating connection',
        'servidor fechou a conex',
        'server closed the connection',
        'connection to server was lost',
    )
    return any(p in message for p in patterns)


def _fetch_with_recovery_retry(query, params=None, max_retries=5, statement_timeout_ms=1800000):
    """Executa query com retry automático para conflitos de recovery em standby."""
    last_error = None
    for attempt in range(max_retries):
        try:
            return execute_query(query, params, statement_timeout_ms=statement_timeout_ms)
        except Exception as e:
            if _is_recovery_conflict(e) and attempt < max_retries - 1:
                wait_seconds = 10 * (attempt + 1)
                time.sleep(wait_seconds)
                last_error = e
                continue
            raise
    raise last_error


def execute_db_operation(operation, statement_timeout_ms=300000):
    """Executa uma operação reutilizando a mesma conexão durante toda a transação."""
    last_error = None

    for client_encoding in get_db_client_encoding_candidates():
        conn = None
        try:
            conn = open_db_connection(client_encoding=client_encoding, statement_timeout_ms=statement_timeout_ms)
            result = operation(conn)
            conn.commit()
            persist_working_client_encoding(client_encoding)
            return result
        except (UnicodeDecodeError, UnicodeEncodeError) as error:
            last_error = error
            if conn is not None:
                conn.rollback()
        except psycopg2.Error as error:
            if _is_encoding_error(error):
                last_error = error
                if conn is not None:
                    conn.rollback()
                continue
            if conn is not None:
                conn.rollback()
            raise error
        except Exception:
            if conn is not None:
                conn.rollback()
            raise
        finally:
            if conn is not None:
                conn.close()

    if last_error is not None:
        raise RuntimeError(
            'Não foi possível decodificar a resposta do banco com os encodings testados '
            f'({", ".join(get_db_client_encoding_candidates())}). Último erro: {last_error}'
        )

    raise RuntimeError('Falha ao executar a consulta no banco de dados.')

def execute_query(query, params=None, fetch=True, statement_timeout_ms=300000):
    """Executa query e retorna resultados"""
    return execute_db_operation(
        lambda conn: run_query_with_connection(conn, query, params=params, fetch=fetch),
        statement_timeout_ms=statement_timeout_ms
    )


def is_ajax_request():
    """Indica se a requisição atual espera resposta JSON para atualização assíncrona."""
    return request.headers.get('X-Requested-With') == 'XMLHttpRequest'


def resolve_relation_name(relation_name):
    """Resolve schema.nome para relações informadas sem schema."""
    candidates = execute_query(
        """
        SELECT
            n.nspname AS schema_name,
            c.relname AS relation_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'v', 'm', 'f', 'p')
          AND lower(c.relname) = lower(%s)
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY
            CASE WHEN n.nspname = 'public' THEN 0 ELSE 1 END,
            n.nspname,
            c.relname
        """,
        (relation_name,)
    )

    if not candidates:
        raise ValueError(
            f'Relação "{relation_name}" não encontrada. Informe no formato schema.tabela, '
            'por exemplo: fdw_externo.v_paciente.'
        )

    if len(candidates) > 1:
        options = ', '.join([f'{item["schema_name"]}.{item["relation_name"]}' for item in candidates[:5]])
        raise ValueError(
            f'Foram encontradas múltiplas relações com nome "{relation_name}": {options}. '
            'Informe explicitamente o schema no campo.'
        )

    candidate = candidates[0]
    return f'{candidate["schema_name"]}.{candidate["relation_name"]}'


def normalize_relation_input(relation_input, field_label, default_schema=None):
    """Normaliza o nome da relação. Sem schema informado, usa default_schema
    (schema local do FDW para origem, schema de destino configurado para destino)."""
    value = (relation_input or '').strip()
    if not value:
        raise ValueError(f'Informe o campo "{field_label}".')

    if '.' in value:
        parts = value.split('.')
        if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
            raise ValueError(f'O campo "{field_label}" deve estar no formato schema.tabela.')
        return f'{parts[0].strip()}.{parts[1].strip()}'

    if default_schema:
        return f'{default_schema}.{value}'

    return resolve_relation_name(value)


def quote_identifier(identifier):
    """Escapa identificadores SQL de forma segura."""
    return '"' + str(identifier).replace('"', '""') + '"'


def quote_qualified_relation(relation_name):
    """Escapa schema.tabela para uso em SQL dinâmico."""
    schema_name, table_name = relation_name.split('.', 1)
    return f'{quote_identifier(schema_name)}.{quote_identifier(table_name)}'


def criar_tabela_destino_como_origem(tabela_origem_raw, tabela_destino_raw, campo_id, falhar_se_existir=True):
    """Cria a tabela de destino com as colunas/tipos da tabela de origem (lidos
    direto no servidor de origem) e define campo_id como chave primária.
    Tipo que não existe no banco local (ex.: enum da origem) vira text.

    Se `falhar_se_existir` for False e o destino já existir, não faz nada e
    retorna com 'criada_agora': False (usado no fluxo automático de
    iniciar/salvar migração, onde a tabela normalmente já existe)."""
    normalized_source = normalizar_origem(tabela_origem_raw)
    normalized_target = normalize_relation_input(tabela_destino_raw, 'Tabela Destino', get_db_config()['schema_destino'])
    normalized_key = (campo_id or '').strip()

    if not normalized_key:
        raise ValueError('Informe o campo chave (campo_id) para criar a tabela de destino.')

    try:
        colunas = colunas_origem(normalized_source)
    finally:
        fechar_conexao_origem_da_thread()
    if not any(c['nome'] == normalized_key for c in colunas):
        raise ValueError(f'A coluna chave "{normalized_key}" não existe na tabela de origem {normalized_source}.')

    quoted_target = quote_qualified_relation(normalized_target)
    target_schema, target_table = normalized_target.split('.', 1)
    resultado_base = {'tabela_origem': normalized_source, 'tabela_destino': normalized_target, 'campo_id': normalized_key}

    def _operacao(conn):
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute('SELECT to_regclass(%s) AS existe', (normalized_target,))
            if cur.fetchone()['existe'] is not None:
                if not falhar_se_existir:
                    return {**resultado_base, 'criada_agora': False}
                raise ValueError(f'A tabela de destino "{normalized_target}" já existe.')

            definicoes = []
            for coluna in colunas:
                cur.execute('SELECT to_regtype(%s) IS NOT NULL AS existe', (coluna['tipo'],))
                tipo = coluna['tipo'] if cur.fetchone()['existe'] else 'text'
                definicoes.append(f'{quote_identifier(coluna["nome"])} {tipo}')

            cur.execute(f'CREATE SCHEMA IF NOT EXISTS {quote_identifier(target_schema)}')
            cur.execute(f'CREATE TABLE {quoted_target} ({", ".join(definicoes)})')
            cur.execute(f'ALTER TABLE {quoted_target} ADD PRIMARY KEY ({quote_identifier(normalized_key)})')
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (target_schema, target_table)
            )
            return {**resultado_base, 'colunas': cur.fetchall(), 'criada_agora': True}

    return execute_db_operation(_operacao, statement_timeout_ms=120000)


def garantir_tabela_destino(tabela_origem_raw, tabela_destino_raw, campo_id):
    """Cria a tabela de destino igual à origem se ela ainda não existir; não faz
    nada se já existir. Retorna True se criou a tabela agora."""
    resultado = criar_tabela_destino_como_origem(
        tabela_origem_raw, tabela_destino_raw, campo_id, falhar_se_existir=False
    )
    return resultado['criada_agora']


def _split_sql_columns(columns_csv):
    """Divide lista CSV simples de colunas SQL preservando identificadores já quotados."""
    return [column.strip() for column in (columns_csv or '').split(',') if column and column.strip()]


def _normalize_identifier_token(identifier_token):
    """Normaliza token de identificador para comparação (sem schema, sem aspas, minúsculo)."""
    token = (identifier_token or '').strip()
    if '.' in token:
        token = token.split('.')[-1].strip()
    if token.startswith('"') and token.endswith('"') and len(token) >= 2:
        token = token[1:-1].replace('""', '"')
    return token.lower()


def build_single_record_upsert_sql(tab_orig, tab_dest, campo, cols_ins, cols_sel):
    """Monta SQL para inserir ou atualizar 1 registro comparando origem x destino.

    OBS: `tab_orig` é a staging local em `temp` (schema.tabela), já com a
    linha copiada da origem (ver api_processar_registro).
    """
    normalized_source = normalize_relation_input(tab_orig, 'Tabela Origem')
    normalized_target = normalize_relation_input(tab_dest, 'Tabela Destino', get_db_config()['schema_destino'])
    normalized_key = (campo or '').strip()

    if not normalized_key:
        raise ValueError('Campo chave inválido na configuração da migração.')

    quoted_source = quote_qualified_relation(normalized_source)
    quoted_target = quote_qualified_relation(normalized_target)
    quoted_key = quote_identifier(normalized_key)

    insert_columns = _split_sql_columns(cols_ins)
    if not insert_columns:
        raise ValueError('Colunas de inserção não preparadas para a migração.')

    normalized_key_token = normalized_key.lower()
    update_columns = [
        column for column in insert_columns
        if _normalize_identifier_token(column) != normalized_key_token
    ]

    base_sql = f"""
        WITH src AS (
            SELECT {cols_sel}
            FROM {quoted_source} s
            WHERE s.{quoted_key} = %s
            LIMIT 1
        ), upserted AS (
            INSERT INTO {quoted_target} ({cols_ins})
            SELECT {', '.join(insert_columns)} FROM src
    """

    if update_columns:
        update_assignments = ', '.join([f'{column} = EXCLUDED.{column}' for column in update_columns])
        difference_clause = ' OR '.join([f'{quoted_target}.{column} IS DISTINCT FROM EXCLUDED.{column}' for column in update_columns])
        conflict_sql = f"""
            ON CONFLICT ({quoted_key}) DO UPDATE
            SET {update_assignments}
            WHERE {difference_clause}
            RETURNING (xmax = 0) AS inserido
        """
    else:
        conflict_sql = f"""
            ON CONFLICT ({quoted_key}) DO NOTHING
            RETURNING (xmax = 0) AS inserido
        """

    tail_sql = """
        )
        SELECT
            EXISTS(SELECT 1 FROM src) AS existe_origem,
            (SELECT inserido FROM upserted LIMIT 1) AS inserido;
    """

    return base_sql + conflict_sql + tail_sql


# ============================================================
# IMPORTAÇÃO AUTOMÁTICA: TABELAS CONFIGURADAS + HISTÓRICO DE CICLOS
# ============================================================
# Persistido no banco (antes ficava só no localStorage do navegador).
# auto_importacao_tabelas    -> lista de tabelas configuradas (como as migrações)
# auto_importacao_ciclos     -> um registro por execução de ciclo
# auto_importacao_ciclo_registros -> detalhe por chave, igual aos "registros
#                                     processados" de uma migração

def garantir_tabelas_e_executar(operacao):
    """Executa `operacao()`; se alguma tabela de controle ainda não existir
    (UndefinedTable), cria todas as tabelas (idempotente, CREATE TABLE IF NOT
    EXISTS) e tenta novamente uma única vez. Evita depender do usuário clicar
    manualmente em "Criar Tabelas de Controle" antes de usar a tela."""
    try:
        return operacao()
    except (psycopg2.errors.UndefinedTable, psycopg2.errors.UndefinedColumn):
        criar_tabelas_controle()
        return operacao()


def listar_migracoes_para_selecao():
    """Lista as migrações manuais já cadastradas (origem/destino/campo_id),
    usada para preencher o seletor de "tabela já configurada" nas telas de
    Importação Automática e Agendar Importação."""
    return garantir_tabelas_e_executar(lambda: execute_query(
        "SELECT id, tabela_origem, tabela_destino, campo_id FROM controle_migracao ORDER BY id"
    ))


def get_auto_import_tabelas():
    """Lista todas as tabelas configuradas, com o total de ciclos já executados
    e a data/hora do último ciclo (usada para não reiniciar um ciclo só porque
    a página foi recarregada antes do intervalo configurado terminar)."""
    return garantir_tabelas_e_executar(lambda: execute_query(
        """
        SELECT t.*, COALESCE(c.total_ciclos, 0) AS total_ciclos, c.ultimo_ciclo_em
        FROM auto_importacao_tabelas t
        LEFT JOIN (
            SELECT tabela_id, COUNT(*) AS total_ciclos, MAX(executado_em) AS ultimo_ciclo_em
            FROM auto_importacao_ciclos
            GROUP BY tabela_id
        ) c ON c.tabela_id = t.id
        ORDER BY t.created_at ASC
        """
    ))


def get_auto_import_tabela(tabela_id):
    """Busca uma tabela configurada pelo id, ou None."""
    def _buscar():
        rows = execute_query(
            "SELECT * FROM auto_importacao_tabelas WHERE id = %s", (tabela_id,)
        )
        return rows[0] if rows else None
    return garantir_tabelas_e_executar(_buscar)


def criar_auto_import_tabela(tabela_origem, tabela_destino, campo_id, intervalo_min, qtd_registros, tamanho_lote):
    """Cadastra uma nova tabela para a rotina de importação automática."""
    def _inserir():
        result = execute_query(
            """
            INSERT INTO auto_importacao_tabelas
                (tabela_origem, tabela_destino, campo_id, intervalo_min, qtd_registros,
                 tamanho_lote, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, 'parada', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING *
            """,
            (tabela_origem, tabela_destino, campo_id, intervalo_min, qtd_registros, tamanho_lote)
        )
        return result[0]
    return garantir_tabelas_e_executar(_inserir)


def atualizar_status_auto_import_tabela(tabela_id, status):
    """Marca a tabela como 'ativa' ou 'parada' (intenção de rodar — quem executa é o JS)."""
    garantir_tabelas_e_executar(lambda: execute_query(
        "UPDATE auto_importacao_tabelas SET status = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
        (status, tabela_id), fetch=False
    ))


def atualizar_ultimo_id_auto_import(tabela_id, ultimo_id):
    """Atualiza o último ID conhecido no destino, exibido na lista."""
    garantir_tabelas_e_executar(lambda: execute_query(
        "UPDATE auto_importacao_tabelas SET ultimo_id_destino = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
        (ultimo_id, tabela_id), fetch=False
    ))


def excluir_auto_import_tabela(tabela_id):
    """Remove a tabela configurada (cascade apaga ciclos e registros)."""
    garantir_tabelas_e_executar(lambda: execute_query(
        "DELETE FROM auto_importacao_tabelas WHERE id = %s", (tabela_id,), fetch=False
    ))


def atualizar_auto_import_tabela(tabela_id, tabela_origem, tabela_destino, campo_id,
                                  intervalo_min, qtd_registros, tamanho_lote):
    """Atualiza a configuração de uma tabela já cadastrada (status e último ID não mudam)."""
    garantir_tabelas_e_executar(lambda: execute_query(
        """
        UPDATE auto_importacao_tabelas
        SET tabela_origem = %s, tabela_destino = %s, campo_id = %s,
            intervalo_min = %s, qtd_registros = %s, tamanho_lote = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """,
        (tabela_origem, tabela_destino, campo_id, intervalo_min, qtd_registros, tamanho_lote, tabela_id),
        fetch=False
    ))


# ============================================================
# AGENDAR IMPORTAÇÃO: SINCRONIZAÇÃO COMPLETA (INSERT/UPDATE/DELETE)
# ============================================================
# Mesmo padrão de tela/tabelas da importação automática, mas cada tabela
# roda no horário configurado (até 4 por dia) em vez de a cada N minutos,
# e cobre a tabela INTEIRA (com DELETE do que sumiu na origem), não só uma
# janela recente.

def get_agendamento_tabelas():
    """Lista todas as tabelas configuradas, com o total de execuções já rodadas."""
    return garantir_tabelas_e_executar(lambda: execute_query(
        """
        SELECT t.*, COALESCE(e.total_execucoes, 0) AS total_execucoes
        FROM agendamento_importacao_tabelas t
        LEFT JOIN (
            SELECT tabela_id, COUNT(*) AS total_execucoes
            FROM agendamento_importacao_execucoes
            GROUP BY tabela_id
        ) e ON e.tabela_id = t.id
        ORDER BY t.created_at ASC
        """
    ))


def get_agendamento_tabela(tabela_id):
    """Busca uma tabela configurada pelo id, ou None."""
    def _buscar():
        rows = execute_query(
            "SELECT * FROM agendamento_importacao_tabelas WHERE id = %s", (tabela_id,)
        )
        return rows[0] if rows else None
    return garantir_tabelas_e_executar(_buscar)


def criar_agendamento_tabela(tabela_origem, tabela_destino, campo_id, tamanho_lote, horarios, id_inicio=1):
    """Cadastra uma nova tabela. `horarios` é uma lista de até 4 strings 'HH:MM' (ou None)."""
    h = (list(horarios) + [None, None, None, None])[:4]
    def _inserir():
        result = execute_query(
            """
            INSERT INTO agendamento_importacao_tabelas
                (tabela_origem, tabela_destino, campo_id, tamanho_lote, id_inicio,
                 horario_1, horario_2, horario_3, horario_4, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'parada', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING *
            """,
            (tabela_origem, tabela_destino, campo_id, tamanho_lote, id_inicio, h[0], h[1], h[2], h[3])
        )
        return result[0]
    return garantir_tabelas_e_executar(_inserir)


def atualizar_agendamento_tabela(tabela_id, tabela_origem, tabela_destino, campo_id, tamanho_lote, horarios,
                                 id_inicio=1):
    """Atualiza a configuração (status e histórico não mudam)."""
    h = (list(horarios) + [None, None, None, None])[:4]
    garantir_tabelas_e_executar(lambda: execute_query(
        """
        UPDATE agendamento_importacao_tabelas
        SET tabela_origem = %s, tabela_destino = %s, campo_id = %s, tamanho_lote = %s, id_inicio = %s,
            horario_1 = %s, horario_2 = %s, horario_3 = %s, horario_4 = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """,
        (tabela_origem, tabela_destino, campo_id, tamanho_lote, id_inicio, h[0], h[1], h[2], h[3], tabela_id),
        fetch=False
    ))


def atualizar_status_agendamento_tabela(tabela_id, status):
    """Marca a tabela como 'ativa' ou 'parada' (liga/desliga os horários)."""
    garantir_tabelas_e_executar(lambda: execute_query(
        "UPDATE agendamento_importacao_tabelas SET status = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
        (status, tabela_id), fetch=False
    ))


def atualizar_ultima_execucao_agendamento(tabela_id, ultimo_id):
    """Marca quando a última execução rodou e o último ID visto no destino."""
    garantir_tabelas_e_executar(lambda: execute_query(
        """
        UPDATE agendamento_importacao_tabelas
        SET ultima_execucao = CURRENT_TIMESTAMP, ultimo_id_destino = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """,
        (ultimo_id, tabela_id), fetch=False
    ))


def excluir_agendamento_tabela(tabela_id):
    """Remove a tabela configurada (cascade apaga execuções e registros)."""
    garantir_tabelas_e_executar(lambda: execute_query(
        "DELETE FROM agendamento_importacao_tabelas WHERE id = %s", (tabela_id,), fetch=False
    ))


LOG_EXECUCAO_MAX_LINHAS = 300
LOTES_EXECUCAO_MAX_ITENS = 300


def _log_execucao(job_id, nivel, mensagem):
    """Adiciona uma linha ao log da execução (lido pela tela de acompanhamento)."""
    job = get_background_job(job_id) or {}
    log = list(job.get('log') or [])
    log.append({'hora': datetime.now().strftime('%H:%M:%S'), 'nivel': nivel, 'mensagem': mensagem})
    if len(log) > LOG_EXECUCAO_MAX_LINHAS:
        log = log[-LOG_EXECUCAO_MAX_LINHAS:]
    set_background_job(job_id, {**job, 'log': log})


def _registrar_lote_execucao_no_job(job_id, numero, inseridos, atualizados, ja_existem, deletados, com_erro):
    """Adiciona o resultado de um lote à lista de lotes do job."""
    job = get_background_job(job_id) or {}
    lotes = list(job.get('lotes') or [])
    lotes.append({
        'numero': numero, 'inseridos': inseridos, 'atualizados': atualizados,
        'ja_existem': ja_existem, 'deletados': deletados, 'com_erro': com_erro,
    })
    if len(lotes) > LOTES_EXECUCAO_MAX_ITENS:
        lotes = lotes[-LOTES_EXECUCAO_MAX_ITENS:]
    set_background_job(job_id, {**job, 'lotes': lotes})


def registrar_execucao_agendamento(conn, tabela_id, status, mensagem, inseridos, atualizados,
                                    ja_existem, deletados, com_erro, total_lotes, tamanho_lote,
                                    ultimo_id_destino):
    """Grava o resumo de uma execução e retorna o id gerado."""
    sql = """
        INSERT INTO agendamento_importacao_execucoes
            (tabela_id, status, mensagem, registros_inseridos, registros_atualizados,
             registros_ja_existem, registros_deletados, registros_com_erro, total_lotes,
             tamanho_lote, ultimo_id_destino, executado_em)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        RETURNING id
    """
    params = (tabela_id, status, mensagem, inseridos, atualizados, ja_existem, deletados,
              com_erro, total_lotes, tamanho_lote, ultimo_id_destino)
    try:
        result = run_query_with_connection(conn, sql, params)
    except (psycopg2.errors.UndefinedTable, psycopg2.errors.UndefinedColumn):
        conn.rollback()
        criar_tabelas_controle()
        result = run_query_with_connection(conn, sql, params)
    conn.commit()
    _limpar_logs_importacao_sem_falhar(conn, 'agendamento_importacao_execucoes')
    return result[0]['id']


def atualizar_execucao_agendamento(conn, execucao_id, status, mensagem, inseridos, atualizados,
                                    ja_existem, deletados, com_erro, ultimo_id_destino):
    """Fecha o resumo de uma execução já criada (registrar_execucao_agendamento cria o
    registro no início, com contadores em zero; esta função atualiza com o resultado final).
    """
    sql = """
        UPDATE agendamento_importacao_execucoes
        SET status = %s, mensagem = %s, registros_inseridos = %s, registros_atualizados = %s,
            registros_ja_existem = %s, registros_deletados = %s, registros_com_erro = %s,
            ultimo_id_destino = %s
        WHERE id = %s
    """
    params = (status, mensagem, inseridos, atualizados, ja_existem, deletados, com_erro,
              ultimo_id_destino, execucao_id)
    run_query_with_connection(conn, sql, params, fetch=False)
    conn.commit()


# Status que NÃO viram linha de detalhe: "ja_existe" é a imensa maioria das
# chaves de cada ciclo/execução (centenas de milhares por rodada) e gravá-lo
# estourou a sequence INTEGER de auto_importacao_ciclo_registros (2^31). O
# total continua no resumo (registros_ja_existem); só o detalhe por chave some.
STATUS_SEM_DETALHE = frozenset({'ja_existe'})


def _filtrar_registros_detalhe(registros):
    return [r for r in registros if r[1] not in STATUS_SEM_DETALHE]


def gravar_registros_execucao(conn, execucao_id, registros):
    """Grava o detalhe por chave de uma execução em uma única instrução (bulk, via unnest).
    `registros` é uma lista de tuplas (chave_valor, status, erro_msg)."""
    registros = _filtrar_registros_detalhe(registros)
    if not registros:
        return

    chaves = [str(r[0]) for r in registros]
    status_list = [r[1] for r in registros]
    erro_list = [r[2] for r in registros]

    sql = """
        INSERT INTO agendamento_importacao_execucao_registros (execucao_id, chave_valor, status, erro_msg)
        SELECT %s, x.chave, x.status, x.erro
        FROM unnest(%s::text[], %s::text[], %s::text[]) AS x(chave, status, erro)
    """
    params = (execucao_id, chaves, status_list, erro_list)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
    except (psycopg2.errors.UndefinedTable, psycopg2.errors.UndefinedColumn):
        conn.rollback()
        criar_tabelas_controle()
        with conn.cursor() as cur:
            cur.execute(sql, params)
    except psycopg2.errors.SequenceGeneratorLimitExceeded:
        # id ainda INTEGER e a sequence chegou em 2^31 — migra pra BIGINT e repete
        conn.rollback()
        migrar_tabelas_detalhe_para_bigint()
        with conn.cursor() as cur:
            cur.execute(sql, params)
    conn.commit()


def _cancelar_execucao_agendamento(job_id, conn, staging, execucao_id=None, contadores=None):
    """Chamado quando o usuário clica em 'Parar' com uma execução em andamento:
    derruba a staging, fecha o registro da execução como cancelado (se já
    tinha sido criado) e marca o job como cancelado."""
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS {staging}')
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

    if execucao_id is not None:
        try:
            c = contadores or {}
            atualizar_execucao_agendamento(
                conn, execucao_id, 'cancelado', 'Execução interrompida pelo usuário.',
                c.get('registros_inseridos', 0), c.get('registros_atualizados', 0),
                c.get('registros_ja_existem', 0), c.get('registros_deletados', 0),
                c.get('registros_com_erro', 0), c.get('ultimo_id_destino')
            )
        except Exception:
            pass

    set_background_job(job_id, {
        **(get_background_job(job_id) or {}),
        'status': 'cancelado',
        'mensagem': 'Execução interrompida pelo usuário.',
        'fim': datetime.now().isoformat(),
    })
    _log_execucao(job_id, 'aviso', 'Execução interrompida pelo usuário — staging removida.')


def registrar_ciclo_auto_import(conn, tabela_id, status, mensagem, inseridos, atualizados,
                                 ja_existem, nao_encontrados, com_erro, total_lotes, tamanho_lote,
                                 id_inicio_faixa, ultimo_id_destino):
    """Grava o resumo de um ciclo executado e retorna o id gerado."""
    sql = """
        INSERT INTO auto_importacao_ciclos
            (tabela_id, status, mensagem, registros_inseridos, registros_atualizados,
             registros_ja_existem, registros_nao_encontrados, registros_com_erro, total_lotes,
             tamanho_lote, id_inicio_faixa, ultimo_id_destino, executado_em)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        RETURNING id
    """
    params = (tabela_id, status, mensagem, inseridos, atualizados, ja_existem, nao_encontrados,
              com_erro, total_lotes, tamanho_lote, id_inicio_faixa, ultimo_id_destino)
    try:
        result = run_query_with_connection(conn, sql, params)
    except (psycopg2.errors.UndefinedTable, psycopg2.errors.UndefinedColumn):
        # Tabela/coluna ainda não existia nesta instalação — cria/atualiza (idempotente) e repete
        conn.rollback()
        criar_tabelas_controle()
        result = run_query_with_connection(conn, sql, params)
    conn.commit()
    _limpar_logs_importacao_sem_falhar(conn, 'auto_importacao_ciclos')
    return result[0]['id']


def gravar_registros_ciclo(conn, ciclo_id, registros):
    """Grava o detalhe por chave de um ciclo em uma única instrução (bulk, via unnest).
    `registros` é uma lista de tuplas (chave_valor, status, erro_msg)."""
    registros = _filtrar_registros_detalhe(registros)
    if not registros:
        return

    chaves = [str(r[0]) for r in registros]
    status_list = [r[1] for r in registros]
    erro_list = [r[2] for r in registros]

    sql = """
        INSERT INTO auto_importacao_ciclo_registros (ciclo_id, chave_valor, status, erro_msg)
        SELECT %s, x.chave, x.status, x.erro
        FROM unnest(%s::text[], %s::text[], %s::text[]) AS x(chave, status, erro)
    """
    params = (ciclo_id, chaves, status_list, erro_list)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
    except psycopg2.errors.UndefinedTable:
        # Tabela ainda não existia nesta instalação — cria (idempotente) e repete
        conn.rollback()
        criar_tabelas_controle()
        with conn.cursor() as cur:
            cur.execute(sql, params)
    except psycopg2.errors.SequenceGeneratorLimitExceeded:
        # id ainda INTEGER e a sequence chegou em 2^31 — migra pra BIGINT e repete
        conn.rollback()
        migrar_tabelas_detalhe_para_bigint()
        with conn.cursor() as cur:
            cur.execute(sql, params)
    conn.commit()


# ============================================================
# REVISÃO DAS TABELAS DE CONTROLE DE IMPORTAÇÃO + RETENÇÃO DE LOGS
# ============================================================
# Instalações antigas criaram as tabelas de detalhe com id SERIAL (INTEGER) e
# gravavam uma linha por chave "ja_existe" a cada ciclo — a sequence estoura em
# 2^31 ("nextval: reached maximum value of sequence"). CREATE TABLE IF NOT
# EXISTS não corrige tabela já existente, então aqui a tabela é recriada com id
# BIGINT copiando só o detalhe relevante dos logs mantidos.
#
# Retenção: só os últimos LOGS_IMPORTACAO_MANTIDOS ciclos/execuções por tabela
# configurada ficam no banco; os mais antigos (e o detalhe, via ON DELETE
# CASCADE) são apagados a cada novo log, pra não encher o disco.

LOGS_IMPORTACAO_MANTIDOS = int(os.getenv('LOGS_IMPORTACAO_MANTIDOS', '20'))

# Tabelas de log (resumo): uma linha por ciclo/execução, com tabela_id + executado_em
TABELAS_LOG_IMPORTACAO = ('auto_importacao_ciclos', 'agendamento_importacao_execucoes')

# Chave do advisory lock: serializa a revisão entre processos/workers
LOCK_MIGRACAO_DETALHE = 300000000

# (tabela, coluna FK, tabela pai, status mantidos, nome do índice)
TABELAS_DETALHE = (
    ('auto_importacao_ciclo_registros', 'ciclo_id', 'auto_importacao_ciclos',
     ['inserido', 'atualizado', 'nao_encontrado', 'erro'],
     'idx_auto_importacao_ciclo_registros_ciclo_status'),
    ('agendamento_importacao_execucao_registros', 'execucao_id', 'agendamento_importacao_execucoes',
     ['inserido', 'atualizado', 'deletado', 'erro'],
     'idx_agendamento_importacao_execucao_registros_status'),
)


def _sql_ids_logs_mantidos(tabela_log):
    """SELECT dos ids dos últimos LOGS_IMPORTACAO_MANTIDOS logs de cada tabela configurada."""
    return f"""
        SELECT id FROM (
            SELECT id, row_number() OVER (
                PARTITION BY tabela_id ORDER BY executado_em DESC, id DESC
            ) AS posicao
            FROM {tabela_log}
        ) ranking
        WHERE posicao <= {int(LOGS_IMPORTACAO_MANTIDOS)}
    """


def limpar_logs_importacao_antigos(conn, tabela_log):
    """Apaga os logs além dos últimos LOGS_IMPORTACAO_MANTIDOS por tabela (o
    detalhe vai junto pelo ON DELETE CASCADE). Retorna quantos logs apagou."""
    assert tabela_log in TABELAS_LOG_IMPORTACAO
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {tabela_log} WHERE id NOT IN ({_sql_ids_logs_mantidos(tabela_log)})")
        return cur.rowcount


def _limpar_logs_importacao_sem_falhar(conn, tabela_log):
    """Retenção chamada a cada novo log: uma falha aqui não pode derrubar o ciclo."""
    try:
        limpar_logs_importacao_antigos(conn, tabela_log)
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        app.logger.exception('Falha ao limpar logs antigos de %s', tabela_log)


def _tipo_id_e_schema(cur, tabela):
    cur.execute("""
        SELECT n.nspname, format_type(a.atttypid, a.atttypmod)
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE a.attrelid = to_regclass(%s) AND a.attname = 'id'
    """, (tabela,))
    return cur.fetchone()


def _recriar_tabela_detalhe_bigint(cur, schema, tabela, coluna_pai, tabela_pai, status_mantidos, indice):
    """Recria `tabela` com id BIGINT, copiando só o detalhe relevante dos logs mantidos."""
    antiga = f'{schema}.{tabela}'
    novo = f'{tabela}_novo'
    pai = f'{schema}.{tabela_pai}'

    # Força a cópia a usar o índice (fk, status) por log em vez de varrer a
    # tabela inteira (centenas de GB nos casos reais)
    cur.execute("SET LOCAL enable_seqscan = off")
    cur.execute("SET LOCAL enable_hashjoin = off")
    cur.execute("SET LOCAL enable_mergejoin = off")
    cur.execute(f"LOCK TABLE {antiga} IN EXCLUSIVE MODE")
    cur.execute(f"DROP TABLE IF EXISTS {schema}.{novo}")
    cur.execute(f"""
        CREATE TABLE {schema}.{novo} (
            id BIGSERIAL PRIMARY KEY,
            {coluna_pai} INTEGER NOT NULL REFERENCES {pai}(id) ON DELETE CASCADE,
            chave_valor TEXT NOT NULL,
            status TEXT NOT NULL,
            erro_msg TEXT
        )
    """)
    cur.execute(f"""
        INSERT INTO {schema}.{novo} ({coluna_pai}, chave_valor, status, erro_msg)
        SELECT r.{coluna_pai}, r.chave_valor, r.status, r.erro_msg
        FROM ({_sql_ids_logs_mantidos(pai)}) p
        JOIN {antiga} r ON r.{coluna_pai} = p.id AND r.status = ANY(%s)
        ORDER BY r.{coluna_pai}, r.id
    """, (status_mantidos,))
    copiados = cur.rowcount
    cur.execute(f"DROP TABLE {antiga}")
    cur.execute(f"ALTER TABLE {schema}.{novo} RENAME TO {tabela}")
    cur.execute(f"ALTER SEQUENCE {schema}.{novo}_id_seq RENAME TO {tabela}_id_seq")
    cur.execute(f"ALTER INDEX {schema}.{novo}_pkey RENAME TO {tabela}_pkey")
    cur.execute(f"CREATE INDEX {indice} ON {antiga} ({coluna_pai}, status)")
    cur.execute("RESET enable_seqscan")
    cur.execute("RESET enable_hashjoin")
    cur.execute("RESET enable_mergejoin")
    return copiados


def _revisar_tabela_detalhe(conn, tabela, coluna_pai, tabela_pai, status_mantidos, indice):
    """Corrige `tabela` se estiver errada. Retorna a descrição do que fez, ou None."""
    with conn.cursor() as cur:
        # Bloqueante: um segundo processo espera e, ao entrar, já vê tudo certo e sai
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_MIGRACAO_DETALHE,))
        row = _tipo_id_e_schema(cur, tabela)
        if not row:
            return None
        schema = psycopg2.extensions.quote_ident(row[0], conn)

        if row[1] == 'integer':
            copiados = _recriar_tabela_detalhe_bigint(
                cur, schema, tabela, coluna_pai, tabela_pai, status_mantidos, indice
            )
            # Logs antigos ficaram sem detalhe — apaga já (cascata na tabela nova, pequena)
            logs_removidos = limpar_logs_importacao_antigos(conn, tabela_pai)
            return (
                f'{tabela}: recriada com id BIGINT ({copiados} registro(s) de detalhe mantido(s), '
                f'{logs_removidos} log(s) antigo(s) de {tabela_pai} removido(s)).'
            )

        # Coluna já BIGINT mas sequence ainda INTEGER (ex.: ALTER COLUMN feito à mão)
        cur.execute("""
            SELECT s.seqrelid::regclass::text
            FROM pg_sequence s
            WHERE s.seqrelid = to_regclass(pg_get_serial_sequence(%s, 'id'))
              AND s.seqtypid = 'integer'::regtype
        """, (f'{schema}.{tabela}',))
        seq = cur.fetchone()
        if seq:
            cur.execute(f"ALTER SEQUENCE {seq[0]} AS BIGINT")
            return f'{tabela}: sequence {seq[0]} convertida para BIGINT.'
    return None


def migrar_tabelas_detalhe_para_bigint():
    """Revisa as tabelas de detalhe (uma transação por tabela, sem
    statement_timeout). Idempotente: não faz nada se já estiverem certas.
    Retorna a lista do que foi corrigido."""
    acoes = []
    for tabela, coluna_pai, tabela_pai, status_mantidos, indice in TABELAS_DETALHE:
        acao = execute_db_operation(
            lambda conn, t=tabela, c=coluna_pai, p=tabela_pai, st=status_mantidos, i=indice:
                _revisar_tabela_detalhe(conn, t, c, p, st, i),
            statement_timeout_ms=0
        )
        if acao:
            acoes.append(acao)
    return acoes


def revisar_tabelas_controle_importacao():
    """Botão "Revisar e Corrigir" da tela Configurar Banco: cria o que faltar,
    recria tabelas de detalhe erradas e aplica a retenção de logs.
    Retorna a lista de mensagens do que foi feito."""
    criar_tabelas_controle()
    acoes = ['Tabelas, colunas, índices e funções SQL de controle verificados/criados no schema de destino.']
    convertidas = migrar_origens_legadas()
    if convertidas:
        acoes.append(f'{convertidas} configuração(ões) antiga(s) (schema local do FDW) passaram a ler direto da origem.')
    acoes += migrar_tabelas_detalhe_para_bigint()

    removidos = execute_db_operation(
        lambda conn: {t: limpar_logs_importacao_antigos(conn, t) for t in TABELAS_LOG_IMPORTACAO},
        statement_timeout_ms=0
    )
    acoes.append(
        f'Logs antigos removidos: {removidos["auto_importacao_ciclos"]} ciclo(s) da importação automática e '
        f'{removidos["agendamento_importacao_execucoes"]} execução(ões) agendada(s) '
        f'(mantidos os últimos {LOGS_IMPORTACAO_MANTIDOS} por tabela).'
    )
    return acoes


def _migrar_tabelas_detalhe_worker():
    try:
        # Tabelas de controle + funções SQL no schema de destino (idempotente)
        criar_tabelas_controle()
        convertidas = migrar_origens_legadas()
        if convertidas:
            app.logger.warning('%s configuração(ões) antigas passaram a ler direto da origem', convertidas)
    except Exception:
        app.logger.exception('Falha ao criar tabelas/funções de controle na subida')
    try:
        acoes = migrar_tabelas_detalhe_para_bigint()
        if acoes:
            app.logger.warning('Tabelas de detalhe corrigidas: %s', ' '.join(acoes))
    except Exception:
        app.logger.exception('Falha ao revisar as tabelas de detalhe')


def iniciar_migracao_tabelas_detalhe():
    """Dispara a revisão em segundo plano na subida do app (pode levar minutos
    em tabelas grandes; não pode travar o servidor web)."""
    if not is_db_configured():
        return
    threading.Thread(target=_migrar_tabelas_detalhe_worker, daemon=True).start()


# ============================================================
# PROVISIONAMENTO: BANCO LOCAL, FDW, SCHEMA TEMP, TABELAS
# ============================================================
# Ações de configuração inicial usadas pela tela "Configurar Banco".
# São idempotentes: rodar de novo não quebra nada que já existe.

def criar_banco_local():
    """Cria o banco configurado (via conexão à base de manutenção 'postgres'),
    caso ele ainda não exista. Retorna True se criou agora, False se já existia."""
    db_config = get_db_connection_config()
    banco_alvo = db_config['database']

    admin_config = db_config.copy()
    admin_config['database'] = 'postgres'  # base de manutenção, sempre existe

    conn = psycopg2.connect(**admin_config)
    try:
        conn.autocommit = True  # CREATE DATABASE não pode rodar dentro de transação
        with conn.cursor() as cur:
            cur.execute('SELECT 1 FROM pg_database WHERE datname = %s', (banco_alvo,))
            ja_existia = cur.fetchone() is not None
            if not ja_existia:
                cur.execute(f'CREATE DATABASE {quote_identifier(banco_alvo)}')
        return not ja_existia
    finally:
        conn.close()


# Funções SQL da migração em lotes — instaladas no schema de destino
# configurado (não em public), junto com as tabelas de controle.
ARQUIVO_FUNCOES_MIGRACAO = os.path.join(app.root_path, 'sql', 'funcoes_migracao.sql')
FUNCOES_MIGRACAO = ('verificar_progresso', 'finalizar_migracao')


def instalar_funcoes_migracao(conn):
    """Cria/atualiza (CREATE OR REPLACE) as funções de sql/funcoes_migracao.sql
    no schema de destino e fixa o search_path de cada uma nesse schema — assim
    elas sempre acham controle_migracao dali, venha a chamada de onde vier."""
    schema = (get_db_config().get('schema_destino') or 'public').strip()
    schema_sql = quote_identifier(schema)
    with open(ARQUIVO_FUNCOES_MIGRACAO, encoding='utf-8') as arquivo:
        script = arquivo.read()

    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_sql}')
        # Só o schema de destino: os CREATE sem schema caem nele, nunca em public
        cur.execute(f'SET LOCAL search_path = {schema_sql}')
        cur.execute(script)
        cur.execute("""
            SELECT p.oid::regprocedure::text
            FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = %s AND p.proname = ANY(%s)
        """, (schema, list(FUNCOES_MIGRACAO)))
        for (assinatura,) in cur.fetchall():
            cur.execute(f'ALTER FUNCTION {schema_sql}.{assinatura} SET search_path = {schema_sql}, public')
    conn.commit()


def criar_tabelas_controle():
    """Cria (se não existirem) as tabelas de controle de migração no schema de destino configurado."""
    def _operacao(conn):
        schema_destino = (get_db_config().get('schema_destino') or 'public').strip()
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS {quote_identifier(schema_destino)}')
            cur.execute("""
                CREATE TABLE IF NOT EXISTS controle_migracao (
                    id SERIAL PRIMARY KEY,
                    tabela_origem TEXT NOT NULL,
                    tabela_destino TEXT NOT NULL,
                    campo_id TEXT NOT NULL,
                    sql_colunas_insert TEXT,
                    sql_colunas_select TEXT,
                    lote_atual INTEGER NOT NULL DEFAULT 0,
                    total_registros INTEGER NOT NULL DEFAULT 0,
                    registros_processados INTEGER NOT NULL DEFAULT 0,
                    tamanho_lote INTEGER NOT NULL DEFAULT 1,
                    id_maximo BIGINT NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'pendente',
                    data_inicio TIMESTAMP,
                    data_fim TIMESTAMP,
                    erro_msg TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)  # colunas batem com create_migration_record() / rotas de leitura
            cur.execute("""
                CREATE TABLE IF NOT EXISTS controle_migracao_lotes (
                    id SERIAL PRIMARY KEY,
                    migracao_id INTEGER NOT NULL REFERENCES controle_migracao(id) ON DELETE CASCADE,
                    numero_lote INTEGER NOT NULL DEFAULT 1,
                    chave_valor TEXT NOT NULL,
                    status TEXT NOT NULL,
                    erro_msg TEXT,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (migracao_id, chave_valor)
                )
            """)  # UNIQUE necessário para o ON CONFLICT usado em gravar_status_lote
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_controle_migracao_lotes_migracao_status
                ON controle_migracao_lotes (migracao_id, status)
            """)  # acelera os filtros por status usados em /api/registros
            # Colunas usadas pelas funções de sql/funcoes_migracao.sql que o
            # CREATE acima não tinha (instalações novas e antigas)
            cur.execute("""
                ALTER TABLE controle_migracao
                ADD COLUMN IF NOT EXISTS sql_colunas_update TEXT,
                ADD COLUMN IF NOT EXISTS sql_colunas_update_where TEXT
            """)
            cur.execute("""
                ALTER TABLE controle_migracao_lotes
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ALTER COLUMN status SET DEFAULT 'pendente'
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_controle_status ON controle_migracao (status)")

            # ---- Importação Automática: tabelas configuradas + histórico de ciclos ----
            cur.execute("""
                CREATE TABLE IF NOT EXISTS auto_importacao_tabelas (
                    id SERIAL PRIMARY KEY,
                    tabela_origem TEXT NOT NULL,
                    tabela_destino TEXT NOT NULL,
                    campo_id TEXT NOT NULL,
                    intervalo_min INTEGER NOT NULL DEFAULT 5,
                    qtd_registros INTEGER NOT NULL DEFAULT 100,
                    tamanho_lote INTEGER NOT NULL DEFAULT 500,
                    status TEXT NOT NULL DEFAULT 'parada',
                    ultimo_id_destino BIGINT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)  # status: 'parada' | 'ativa' — só controla a intenção; quem executa é o JS
            cur.execute("""
                CREATE TABLE IF NOT EXISTS auto_importacao_ciclos (
                    id SERIAL PRIMARY KEY,
                    tabela_id INTEGER NOT NULL REFERENCES auto_importacao_tabelas(id) ON DELETE CASCADE,
                    executado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    status TEXT NOT NULL,
                    mensagem TEXT,
                    registros_inseridos INTEGER NOT NULL DEFAULT 0,
                    registros_atualizados INTEGER NOT NULL DEFAULT 0,
                    registros_ja_existem INTEGER NOT NULL DEFAULT 0,
                    registros_nao_encontrados INTEGER NOT NULL DEFAULT 0,
                    registros_com_erro INTEGER NOT NULL DEFAULT 0,
                    total_lotes INTEGER NOT NULL DEFAULT 0,
                    tamanho_lote INTEGER,
                    id_inicio_faixa BIGINT,
                    ultimo_id_destino BIGINT
                )
            """)  # status do ciclo: 'concluido' | 'erro' (reaproveita as classes CSS existentes)
            cur.execute("""
                ALTER TABLE auto_importacao_ciclos
                ADD COLUMN IF NOT EXISTS registros_nao_encontrados INTEGER NOT NULL DEFAULT 0
            """)  # instalações antigas não ganham a coluna nova só com CREATE TABLE IF NOT EXISTS
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_auto_importacao_ciclos_tabela
                ON auto_importacao_ciclos (tabela_id, executado_em DESC)
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS auto_importacao_ciclo_registros (
                    id BIGSERIAL PRIMARY KEY,
                    ciclo_id INTEGER NOT NULL REFERENCES auto_importacao_ciclos(id) ON DELETE CASCADE,
                    chave_valor TEXT NOT NULL,
                    status TEXT NOT NULL,
                    erro_msg TEXT
                )
            """)  # status por registro: inserido | atualizado | ja_existe | erro
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_auto_importacao_ciclo_registros_ciclo_status
                ON auto_importacao_ciclo_registros (ciclo_id, status)
            """)

            # ---- Agendar Importação: sincronização completa (insert/update/delete) ----
            cur.execute("""
                CREATE TABLE IF NOT EXISTS agendamento_importacao_tabelas (
                    id SERIAL PRIMARY KEY,
                    tabela_origem TEXT NOT NULL,
                    tabela_destino TEXT NOT NULL,
                    campo_id TEXT NOT NULL,
                    tamanho_lote INTEGER NOT NULL DEFAULT 500,
                    horario_1 TIME,
                    horario_2 TIME,
                    horario_3 TIME,
                    horario_4 TIME,
                    status TEXT NOT NULL DEFAULT 'parada',
                    ultima_execucao TIMESTAMP,
                    ultimo_id_destino BIGINT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)  # status: 'parada' | 'ativa' — controla se os horários estão ligados
            cur.execute("""
                ALTER TABLE agendamento_importacao_tabelas
                ADD COLUMN IF NOT EXISTS id_inicio BIGINT NOT NULL DEFAULT 1
            """)  # primeiro ID da faixa verificada em cada execução
            cur.execute("""
                CREATE TABLE IF NOT EXISTS agendamento_importacao_execucoes (
                    id SERIAL PRIMARY KEY,
                    tabela_id INTEGER NOT NULL REFERENCES agendamento_importacao_tabelas(id) ON DELETE CASCADE,
                    executado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    status TEXT NOT NULL,
                    mensagem TEXT,
                    registros_inseridos INTEGER NOT NULL DEFAULT 0,
                    registros_atualizados INTEGER NOT NULL DEFAULT 0,
                    registros_ja_existem INTEGER NOT NULL DEFAULT 0,
                    registros_deletados INTEGER NOT NULL DEFAULT 0,
                    registros_com_erro INTEGER NOT NULL DEFAULT 0,
                    total_lotes INTEGER NOT NULL DEFAULT 0,
                    tamanho_lote INTEGER,
                    ultimo_id_destino BIGINT
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_agendamento_importacao_execucoes_tabela
                ON agendamento_importacao_execucoes (tabela_id, executado_em DESC)
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS agendamento_importacao_execucao_registros (
                    id BIGSERIAL PRIMARY KEY,
                    execucao_id INTEGER NOT NULL REFERENCES agendamento_importacao_execucoes(id) ON DELETE CASCADE,
                    chave_valor TEXT NOT NULL,
                    status TEXT NOT NULL,
                    erro_msg TEXT
                )
            """)  # status por registro: inserido | atualizado | ja_existe | deletado | erro
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_agendamento_importacao_execucao_registros_status
                ON agendamento_importacao_execucao_registros (execucao_id, status)
            """)
        conn.commit()

    execute_db_operation(_operacao, statement_timeout_ms=60000)
    execute_db_operation(instalar_funcoes_migracao, statement_timeout_ms=60000)


def garantir_tabelas_controle_no_schema_ativo():
    """Cria/verifica as tabelas de controle no schema de destino assim que a
    conexão é salva/ativada, para o schema onde elas moram nunca ficar
    dessincronizado do "Schema de destino" configurado (evita tabelas de
    controle órfãs num schema antigo depois de trocar a configuração)."""
    try:
        criar_tabelas_controle()
    except (OperationalError, InterfaceError):
        pass  # sem conexão ainda (ex.: banco local não criado); usuário resolve no provisionamento
    except Exception as e:
        schema_destino = get_db_config().get('schema_destino') or 'public'
        flash(f'Não foi possível criar as tabelas de controle no schema "{schema_destino}": {str(e)}', 'warning')


@app.route('/configurar-banco', methods=['GET', 'POST'])
def configurar_banco():
    """Tela de configuração da conexão com o banco de dados."""
    edit_connection = None
    edit_id = request.args.get('editar')
    if edit_id and edit_id.isdigit():
        edit_connection = get_db_connection_by_id(int(edit_id))

    db_form = get_db_form_data(edit_connection or get_db_config())
    if request.method == 'POST':
        db_form = get_posted_db_form_data(request.form)

        try:
            new_db_config = build_db_config(request.form)
            profile_id = int(db_form['profile_id']) if db_form['profile_id'].isdigit() else None
            saved_profile, created = save_db_connection_profile(new_db_config, profile_id=profile_id)
            active_config = saved_profile.copy()
            active_config['connection_mode'] = 'local' if active_config['host'] == 'localhost' else 'remote'
            active_config['profile_id'] = saved_profile['id']
            app.config['DB_CONFIG'] = active_config
            app.config['DB_CONFIG_LOADED'] = True
            persist_active_db_config()
            flash(
                'Conexão salva na lista e definida como ativa.' if created else 'Conexão atualizada e definida como ativa.',
                'success'
            )
            garantir_tabelas_controle_no_schema_ativo()
            return redirect(url_for('dashboard'))
        except ValueError as error:
            flash(str(error), 'error')

    fdw_form = get_fdw_config().copy()
    fdw_form['password'] = ''  # nunca reexibe a senha salva, só a reusa internamente

    return render_template(
        'configurar_banco.html',
        db_config=db_form,
        fdw_config=fdw_form,
        fdw_tabelas=get_fdw_tabelas(),
        fdw_local=estado_fdw_local(),
        temp_schema_name=TEMP_SCHEMA_NAME,
        logs_importacao_mantidos=LOGS_IMPORTACAO_MANTIDOS,
        error_details=None,
        saved_connections=[sanitize_db_connection(connection) for connection in get_saved_db_connections()]
    )


@app.route('/configurar-banco/testar', methods=['POST'])
def testar_conexao_banco():
    """Testa, via AJAX, os dados preenchidos no formulário sem salvar nada."""
    form_data = request.form

    connection_mode = (form_data.get('connection_mode') or 'local').strip().lower()
    host = (form_data.get('host') or '').strip()
    if connection_mode == 'local' and not host:
        host = 'localhost'
    database = (form_data.get('database') or 'postgres').strip()
    user = (form_data.get('user') or '').strip()
    password = form_data.get('password') or ''
    port_raw = (form_data.get('port') or '5432').strip()

    profile_id = (form_data.get('profile_id') or '').strip()
    if not password and profile_id.isdigit():
        saved_connection = get_db_connection_by_id(int(profile_id))
        if saved_connection:
            password = saved_connection.get('password') or ''

    if not host:
        return jsonify({'sucesso': False, 'mensagem': 'Informe o IP ou host do banco.'})
    if not database:
        return jsonify({'sucesso': False, 'mensagem': 'Informe o nome do banco de dados.'})
    if not user:
        return jsonify({'sucesso': False, 'mensagem': 'Informe o usuário do banco de dados.'})
    if not password:
        return jsonify({'sucesso': False, 'mensagem': 'Informe a senha do banco de dados.'})

    try:
        port = int(port_raw)
    except ValueError:
        return jsonify({'sucesso': False, 'mensagem': 'Informe uma porta válida.'})

    try:
        conexao_teste = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
            connect_timeout=int(os.getenv('DB_TEST_CONNECT_TIMEOUT', '8'))
        )
        conexao_teste.close()
    except (OperationalError, InterfaceError) as error:
        return jsonify({'sucesso': False, 'mensagem': f'Falha na conexão: {error}'})
    except Exception as error:  # noqa: BLE001 - qualquer outro erro do driver vira mensagem para o usuário
        return jsonify({'sucesso': False, 'mensagem': f'Erro inesperado ao testar a conexão: {error}'})

    return jsonify({'sucesso': True, 'mensagem': f'Conexão bem-sucedida com {host}:{port}/{database}.'})


@app.route('/configurar-banco/usar/<int:connection_id>', methods=['POST'])
def usar_conexao_salva(connection_id):
    """Ativa uma conexão salva como configuração corrente da aplicação."""
    connection = get_db_connection_by_id(connection_id)
    if not connection:
        flash('Conexão salva não encontrada.', 'error')
        return redirect(url_for('configurar_banco'))

    active_config = connection.copy()
    active_config['connection_mode'] = 'local' if active_config['host'] == 'localhost' else 'remote'
    active_config['profile_id'] = connection['id']
    app.config['DB_CONFIG'] = active_config
    app.config['DB_CONFIG_LOADED'] = True
    persist_active_db_config()
    flash(f'Conexão "{connection["profile_name"]}" definida como ativa.', 'success')
    garantir_tabelas_controle_no_schema_ativo()
    return redirect(url_for('configurar_banco'))


@app.route('/configurar-banco/excluir/<int:connection_id>', methods=['POST'])
def excluir_conexao_salva(connection_id):
    """Remove uma conexão salva da lista persistida localmente."""
    saved_connections = get_saved_db_connections()
    connection = get_db_connection_by_id(connection_id)
    if not connection:
        flash('Conexão salva não encontrada.', 'error')
        return redirect(url_for('configurar_banco'))

    saved_connections[:] = [item for item in saved_connections if item['id'] != connection_id]
    persist_saved_db_connections()
    current_profile_id = get_db_config().get('profile_id')
    if current_profile_id == connection_id:
        app.config['DB_CONFIG'] = DEFAULT_DB_CONFIG.copy()
        app.config['DB_CONFIG_LOADED'] = True
        persist_active_db_config()

    flash(f'Conexão "{connection["profile_name"]}" removida da lista.', 'success')
    return redirect(url_for('configurar_banco'))


@app.route('/configurar-banco/criar-banco-local', methods=['POST'])
def criar_banco_local_route():
    """Cria o banco de dados configurado no formulário acima, se ainda não existir."""
    db_config = get_db_connection_config()
    if not (db_config.get('host') and db_config.get('user') and db_config.get('password') and db_config.get('database')):
        flash('Preencha e salve host, banco, usuário e senha antes de criar o banco local.', 'warning')
        return redirect(url_for('configurar_banco'))

    try:
        criado_agora = criar_banco_local()
        if criado_agora:
            flash(f'Banco "{db_config["database"]}" criado com sucesso.', 'success')
        else:
            flash(f'Banco "{db_config["database"]}" já existia — nada a fazer.', 'info')
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error)
    except Exception as e:
        flash(f'Erro ao criar o banco local: {str(e)}', 'error')

    return redirect(url_for('configurar_banco'))


def _origem_do_form():
    """Lê a seção "Servidor de origem" do formulário. Senha em branco = mantém
    a salva. Retorna (origem, porta, mensagem_de_erro)."""
    atual = get_fdw_config()
    origem = {
        'host': (request.form.get('fdw_host') or '').strip(),
        'database': (request.form.get('fdw_database') or '').strip(),
        'user': (request.form.get('fdw_user') or '').strip(),
        'password': request.form.get('fdw_password') or atual.get('password') or '',
        'schema_remoto': (request.form.get('fdw_schema_remoto') or 'public').strip(),
        # schema local do antigo FDW: só guardado pra traduzir nomes antigos
        # (integrasesa.v_x -> <schema remoto>.v_x)
        'schema_local': atual.get('schema_local') or '',
    }
    if not origem['host'] or not origem['database'] or not origem['user'] or not origem['password']:
        return origem, None, 'Informe host, banco, usuário e senha da origem.'
    try:
        porta = int((request.form.get('fdw_port') or '5432').strip())
    except ValueError:
        return origem, None, 'Informe uma porta válida.'
    return origem, porta, None


@app.route('/configurar-banco/salvar-origem', methods=['POST'])
def salvar_origem_route():
    """Salva as credenciais do servidor de origem (depois de conectar com
    sucesso) e atualiza a lista de tabelas usada nos exemplos rápidos."""
    origem, porta, erro = _origem_do_form()
    if erro:
        flash(erro, 'error')
        return redirect(url_for('configurar_banco'))

    conectou, mensagem, tabelas = _testar_origem_direta(origem, porta)
    if not conectou:
        flash(f'Origem não salva — {mensagem}', 'error')
        return redirect(url_for('configurar_banco'))

    origem['port'] = porta
    persist_fdw_config(origem)
    lista = salvar_lista_tabelas_fdw(origem, porta, tabelas)
    flash(f'Servidor de origem salvo: {origem["host"]}:{porta}/{origem["database"]} '
          f'(schema "{origem["schema_remoto"]}", {len(lista["tabelas"])} tabela(s) disponíveis).', 'success')
    if is_db_configured():
        try:
            convertidas = migrar_origens_legadas()
            if convertidas:
                flash(f'{convertidas} configuração(ões) antiga(s) passaram a ler direto da origem.', 'info')
        except psycopg2.Error as error:
            flash(f'Não foi possível atualizar as origens antigas: {error}', 'warning')
    return redirect(url_for('configurar_banco'))


def migrar_origens_legadas():
    """Troca o prefixo do antigo schema local do FDW (ex.: integrasesa.v_x)
    pelo schema remoto (guardiao.v_x) nas configurações gravadas. Idempotente."""
    fdw = get_fdw_config()
    legado = (fdw.get('schema_local') or '').strip()
    remoto = (fdw.get('schema_remoto') or '').strip()
    if not legado or not remoto or legado.lower() == remoto.lower():
        return 0

    def _operacao(conn):
        total = 0
        with conn.cursor() as cur:
            for tabela in ('controle_migracao', 'auto_importacao_tabelas', 'agendamento_importacao_tabelas'):
                cur.execute(f"""
                    UPDATE {tabela}
                    SET tabela_origem = %s || substr(tabela_origem, length(split_part(tabela_origem, '.', 1)) + 1)
                    WHERE lower(split_part(tabela_origem, '.', 1)) = lower(%s)
                """, (remoto, legado))
                total += cur.rowcount
        return total

    return garantir_tabelas_e_executar(lambda: execute_db_operation(_operacao, statement_timeout_ms=60000))


def estado_fdw_local():
    """O que ainda existe do antigo FDW local no banco (pra oferecer a remoção)."""
    schema = (get_fdw_config().get('schema_local') or '').strip()
    if not schema or not is_db_configured():
        return None
    try:
        row = execute_query("""
            SELECT
                EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = %s) AS schema_existe,
                EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = %s) AS servidor_existe,
                (SELECT count(*) FROM pg_foreign_table ft JOIN pg_class c ON c.oid = ft.ftrelid
                  JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = %s) AS tabelas_estrangeiras
        """, (schema, f'fdw_srv_{schema}', schema), statement_timeout_ms=10000)[0]
    except Exception:  # noqa: BLE001 - só não oferece o botão
        return None
    if not row['schema_existe'] and not row['servidor_existe']:
        return None
    return {'schema': schema, **row}


@app.route('/configurar-banco/remover-fdw-local', methods=['POST'])
def remover_fdw_local_route():
    """Remove o antigo FDW local: servidor, user mapping e tabelas estrangeiras
    (DROP SERVER ... CASCADE) e o schema local, se não sobrar nada nele."""
    estado = estado_fdw_local()
    if not estado:
        flash('Não há FDW local para remover.', 'info')
        return redirect(url_for('configurar_banco'))
    schema = estado['schema']

    try:
        migrar_origens_legadas()   # configurações deixam de citar o schema antes de removê-lo
    except psycopg2.Error as error:
        flash(f'Não foi possível atualizar as origens antigas: {error}', 'error')
        return redirect(url_for('configurar_banco'))

    def _operacao(conn):
        with conn.cursor() as cur:
            cur.execute(f'DROP SERVER IF EXISTS {quote_identifier(f"fdw_srv_{schema}")} CASCADE')
            cur.execute("""
                SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relkind IN ('r', 'v', 'm', 'f', 'p') ORDER BY 1
            """, (schema,))
            restantes = [r[0] for r in cur.fetchall()]
            if not restantes:
                cur.execute(f'DROP SCHEMA IF EXISTS {quote_identifier(schema)} CASCADE')
        return restantes

    try:
        restantes = execute_db_operation(_operacao, statement_timeout_ms=60000)
    except psycopg2.Error as error:
        flash(f'Erro ao remover o FDW local: {error}', 'error')
        return redirect(url_for('configurar_banco'))

    if restantes:
        flash(f'Servidor FDW e tabelas estrangeiras removidos. O schema "{schema}" foi mantido porque ainda '
              f'tem objetos que não são do FDW: {", ".join(restantes[:10])}'
              f'{"..." if len(restantes) > 10 else ""}.', 'warning')
    else:
        flash(f'FDW local removido: schema "{schema}", servidor, mapeamento e tabelas estrangeiras.', 'success')
    return redirect(url_for('configurar_banco'))


TIPOS_RELACAO_ORIGEM = {'r': 'tabela', 'p': 'tabela', 'v': 'view', 'm': 'view materializada', 'f': 'tabela estrangeira'}


def _testar_origem_direta(origem, porta):
    """Etapa 1: o app conecta direto na origem com os dados informados."""
    try:
        conexao = psycopg2.connect(
            host=origem['host'], port=porta, dbname=origem['database'],
            user=origem['user'], password=origem['password'],
            connect_timeout=int(os.getenv('DB_TEST_CONNECT_TIMEOUT', '8')),
        )
    except Exception as error:  # noqa: BLE001 - qualquer erro do driver vira mensagem
        return False, f'Origem: falha ao conectar em {origem["host"]}:{porta}/{origem["database"]} — {error}', []
    try:
        with conexao.cursor() as cur:
            # Só as VIEWS (comuns e materializadas) do schema configurado.
            # pg_class em vez de information_schema: o information_schema
            # esconde o que o usuário não pode ler e as views materializadas —
            # aqui elas aparecem, marcadas com ou sem permissão de leitura.
            cur.execute("""
                SELECT c.relname, c.relkind,
                       has_schema_privilege(n.oid, 'USAGE') AND has_table_privilege(c.oid, 'SELECT') AS pode_ler,
                       COALESCE(array_agg(a.attname::text ORDER BY a.attnum) FILTER (WHERE a.attnum > 0), '{}')
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                WHERE n.nspname = %s AND c.relkind IN ('v', 'm')
                GROUP BY c.oid, n.oid, c.relname, c.relkind
                ORDER BY c.relname
            """, (origem['schema_remoto'],))
            tabelas = [
                {'nome': nome, 'schema': origem['schema_remoto'], 'tabela': nome,
                 'tipo': TIPOS_RELACAO_ORIGEM.get(tipo, 'tabela'), 'colunas': list(colunas),
                 'sem_permissao': not pode_ler}
                for nome, tipo, pode_ler, colunas in cur.fetchall()
            ]
            # Onde a conexão caiu de fato — pra conferir se é o mesmo servidor/
            # banco que se está olhando em outra ferramenta
            cur.execute("""
                SELECT current_database(), current_user, COALESCE(host(inet_server_addr()), 'socket local'),
                       inet_server_port(), split_part(version(), ' ', 2), pg_is_in_recovery()
            """)
            banco, usuario, ip, porta_real, versao, replica = cur.fetchone()
    finally:
        conexao.close()
    endereco = f'[{ip}]' if ':' in ip else ip   # IPv6 entre colchetes
    servidor = (f'banco "{banco}" no servidor {endereco}:{porta_real or porta} (PostgreSQL {versao}'
                f'{", réplica somente leitura" if replica else ""}), usuário "{usuario}"')
    if not tabelas:
        return False, (f'Origem: conectou ({servidor}), mas o schema "{origem["schema_remoto"]}" '
                       f'não tem views.'), []
    sem_permissao = [t['nome'] for t in tabelas if t['sem_permissao']]
    mensagem = (f'Origem: conexão OK — {len(tabelas)} view(s) no schema "{origem["schema_remoto"]}": '
                f'{len(tabelas) - len(sem_permissao)} com permissão de leitura e {len(sem_permissao)} sem permissão.'
                f'\nConectado em: {servidor}.')
    if sem_permissao:
        mensagem += (f'\nSem permissão para o usuário "{origem["user"]}": {", ".join(sem_permissao[:15])}'
                     f'{"..." if len(sem_permissao) > 15 else ""}. Peça ao DBA da origem: '
                     f'GRANT SELECT ON {origem["schema_remoto"]}.<view> TO "{origem["user"]}".')
    return True, mensagem, tabelas


def salvar_lista_tabelas_fdw(origem, porta, tabelas):
    """Grava a lista de tabelas da origem — é ela que alimenta os exemplos
    rápidos das telas de cadastro."""
    lista = {
        'atualizado_em': datetime.now().strftime('%d/%m/%Y %H:%M'),
        'origem': f'{origem["host"]}:{porta}/{origem["database"]}',
        'schema_remoto': origem['schema_remoto'],
        'tabelas': tabelas,
    }
    persist_fdw_tabelas(lista)
    return lista


@app.route('/configurar-banco/testar-fdw', methods=['POST'])
def testar_fdw_route():
    """Testa, via AJAX, a conexão direta com o servidor de origem e atualiza a
    lista de tabelas (sem salvar as credenciais)."""
    origem, porta, erro = _origem_do_form()
    if erro:
        return jsonify({'sucesso': False, 'mensagem': erro})

    conectou, mensagem, tabelas = _testar_origem_direta(origem, porta)
    lista = None
    if tabelas:
        try:
            lista = salvar_lista_tabelas_fdw(origem, porta, tabelas)
        except OSError as error:
            mensagem += f'\nAviso: não foi possível salvar a lista de tabelas — {error}'
    return jsonify({'sucesso': conectou, 'mensagem': mensagem, 'lista_tabelas': lista})


@app.route('/logo')
def logo_sistema():
    """Serve a logo atual (personalizada ou a padrão)."""
    return send_file(caminho_logo_atual(), max_age=0)


@app.route('/configurar-banco/logo', methods=['POST'])
def alterar_logo_route():
    arquivo = request.files.get('logo')
    try:
        salvar_logo(arquivo.read(LOGO_MAX_BYTES + 1) if arquivo else b'')
        flash('Logo do sistema atualizada.', 'success')
    except ValueError as error:
        flash(str(error), 'error')
    except OSError as error:
        flash(f'Não foi possível gravar a logo: {error}', 'error')
    return redirect(url_for('configurar_banco') + '#logo-sistema')


@app.route('/configurar-banco/logo/restaurar', methods=['POST'])
def restaurar_logo_route():
    try:
        restaurar_logo_padrao()
        flash('Logo padrão restaurada.', 'success')
    except OSError as error:
        flash(f'Não foi possível remover a logo personalizada: {error}', 'error')
    return redirect(url_for('configurar_banco') + '#logo-sistema')


@app.route('/configurar-banco/criar-schema-temp', methods=['POST'])
def criar_schema_temp_route():
    """Cria o schema de staging (temp) usado antes dos upserts em public."""
    if not is_db_configured():
        flash('Configure e salve a conexão antes de criar o schema temp.', 'warning')
        return redirect(url_for('configurar_banco'))

    try:
        execute_db_operation(lambda conn: ensure_temp_schema(conn), statement_timeout_ms=30000)
        flash(f'Schema "{TEMP_SCHEMA_NAME}" verificado/criado com sucesso.', 'success')
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error)
    except Exception as e:
        flash(f'Erro ao criar o schema temp: {str(e)}', 'error')

    return redirect(url_for('configurar_banco'))


@app.route('/configurar-banco/revisar-tabelas', methods=['POST'])
def revisar_tabelas_route():
    """Revisa as tabelas de controle de importação: cria o que faltar, recria as
    que estiverem erradas e apaga os logs além dos últimos LOGS_IMPORTACAO_MANTIDOS."""
    if not is_db_configured():
        flash('Configure e salve a conexão antes de revisar as tabelas.', 'warning')
        return redirect(url_for('configurar_banco'))

    try:
        for acao in revisar_tabelas_controle_importacao():
            flash(acao, 'success')
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error)
    except Exception as e:
        flash(f'Erro ao revisar as tabelas de controle: {str(e)}', 'error')

    return redirect(url_for('configurar_banco'))


@app.route('/', methods=['GET', 'POST'])
def login():
    """Tela de login — nova página inicial da aplicação (index.html)."""
    if session.get('usuario_id'):
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        user = verificar_login(username, password)
        if user:
            session.clear()
            session['usuario_id'] = user['id']
            session['username'] = user['username']
            session['perfil'] = user['perfil']
            destino = request.form.get('next') or url_for('dashboard')
            return redirect(destino)
        flash('Usuário ou senha inválidos.', 'error')

    return render_template(
        'index.html',
        sem_usuarios=not get_users(),
        next_url=request.args.get('next', '')
    )


@app.route('/logout', methods=['POST'])
def logout():
    """Encerra a sessão do usuário logado."""
    session.clear()
    flash('Sessão encerrada.', 'info')
    return redirect(url_for('login'))


@app.route('/usuarios', methods=['GET', 'POST'])
def usuarios():
    """Tela de cadastro de usuários (perfis: usuario / administrador).
    Acesso restrito a administradores — exceto enquanto não existir nenhum
    usuário no sistema, para permitir criar o primeiro administrador.
    """
    if request.method == 'POST':
        perfil_solicitado = request.form.get('perfil') or 'usuario'
        # O primeiro usuário do sistema é sempre administrador (bootstrap)
        perfil = 'administrador' if not get_users() else perfil_solicitado
        try:
            criar_usuario(request.form.get('username'), request.form.get('password'), perfil)
            flash('Usuário criado com sucesso.', 'success')
            return redirect(url_for('usuarios'))
        except ValueError as error:
            flash(str(error), 'error')

    return render_template('usuarios.html', usuarios=get_users())


@app.route('/usuarios/excluir/<int:user_id>', methods=['POST'])
def excluir_usuario_route(user_id):
    """Remove um usuário cadastrado (não permite excluir o próprio usuário logado)."""
    if session.get('usuario_id') == user_id:
        flash('Você não pode excluir o usuário com o qual está logado.', 'error')
        return redirect(url_for('usuarios'))
    try:
        excluir_usuario(user_id)
        flash('Usuário removido.', 'success')
    except ValueError as error:
        flash(str(error), 'error')
    return redirect(url_for('usuarios'))


@app.route('/dashboard')
def dashboard():
    """Página do Dashboard (antigo index.html, agora dashboard.html)."""
    # Só o resumo (configuradas/ativas): quem dispara os ciclos/execuções é o
    # agendador do servidor — abrir o dashboard não dispara nada.
    try:
        todas_auto_tabelas = get_auto_import_tabelas()
    except Exception:
        todas_auto_tabelas = []
    resumo_auto = {
        'configuradas': len(todas_auto_tabelas),
        'ativas': sum(1 for t in todas_auto_tabelas if t.get('status') == 'ativa'),
    }

    try:
        todas_agendamento_tabelas = get_agendamento_tabelas()
    except Exception:
        todas_agendamento_tabelas = []
    resumo_agendamento = {
        'configuradas': len(todas_agendamento_tabelas),
        'ativas': sum(1 for t in todas_agendamento_tabelas if t.get('status') == 'ativa'),
    }

    manual_em_execucao = contar_migracoes_manuais_em_execucao()

    if not is_db_configured():
        flash('Informe usuário e senha do banco na tela Banco para liberar as operações.', 'warning')
        return render_template(
            'dashboard.html', migracoes=[], totais={},
            resumo_auto=resumo_auto, resumo_agendamento=resumo_agendamento,
            manual_em_execucao=manual_em_execucao
        )

    try:
        def _fetch_index(conn):
            query = run_query_with_connection
            migracoes = query(conn, """
                SELECT * FROM controle_migracao
                ORDER BY created_at DESC
            """)

            for m in migracoes:
                # Último ID no destino
                try:
                    r = query(conn,
                        f"SELECT MAX({m['campo_id']}) as ultimo_id FROM {m['tabela_destino']}")
                    m['ultimo_id_destino'] = r[0].get('ultimo_id') or 0 if r else 0
                except Exception:
                    m['ultimo_id_destino'] = 0

                # Último ID na origem (view)
                try:
                    r = query(conn,
                        f"SELECT MAX({m['campo_id']}) as ultimo_id FROM {m['tabela_origem']}")
                    m['ultimo_id_origem'] = r[0].get('ultimo_id') or 0 if r else 0
                except Exception:
                    m['ultimo_id_origem'] = m.get('id_maximo') or 0

            return migracoes

        migracoes = execute_db_operation(_fetch_index, statement_timeout_ms=120000)

        # Totais agregados para KPIs
        totais = {
            'total_registros': sum(m.get('total_registros', 0) or 0 for m in migracoes),
            'registros_processados': sum(m.get('registros_processados', 0) or 0 for m in migracoes),
            'em_processamento': sum(1 for m in migracoes if m.get('status') == 'processando'),
            'concluidas': sum(1 for m in migracoes if m.get('status') == 'concluido'),
            'com_erro': sum(1 for m in migracoes if m.get('status') == 'erro'),
            'pendentes': sum(1 for m in migracoes if m.get('status') in ('pendente', 'preparando')),
        }
        return render_template(
            'dashboard.html', migracoes=migracoes, totais=totais,
            resumo_auto=resumo_auto, resumo_agendamento=resumo_agendamento,
            manual_em_execucao=manual_em_execucao
        )
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error)
    except Exception as e:
        flash(f'Erro ao carregar migrações: {str(e)}', 'error')
        return render_template(
            'dashboard.html', migracoes=[], totais={},
            resumo_auto=resumo_auto, resumo_agendamento=resumo_agendamento,
            manual_em_execucao=manual_em_execucao
        )

# Serializa "conferir duplicidade + criar" — dois envios ao mesmo tempo não
# podem passar os dois pela verificação
_nova_migracao_lock = threading.Lock()


def buscar_migracao_duplicada(tabela_origem, tabela_destino, ignorar_id=None):
    """Migração já cadastrada com a mesma tabela de origem ou de destino
    (nomes já normalizados, sem diferenciar maiúsculas), ou None."""
    rows = execute_query("""
        SELECT id, tabela_origem, tabela_destino FROM controle_migracao
        WHERE (lower(tabela_origem) = lower(%s) OR lower(tabela_destino) = lower(%s))
          AND (%s IS NULL OR id <> %s)
        ORDER BY id LIMIT 1
    """, (tabela_origem, tabela_destino, ignorar_id, ignorar_id))
    return rows[0] if rows else None


def mensagem_migracao_duplicada(existente, tabela_origem):
    campo = 'origem' if existente['tabela_origem'].lower() == tabela_origem.lower() else 'destino'
    return (f'Já existe a migração #{existente["id"]} com a mesma tabela de {campo} '
            f'({existente["tabela_origem"]} → {existente["tabela_destino"]}). '
            f'Abra ou edite a migração existente em vez de criar outra.')


def mapeamentos_fdw_com_migracoes():
    """Mapeamentos do FDW marcados com a migração já cadastrada pra mesma
    origem (ou destino), se houver — a tela não oferece criar de novo."""
    schema_destino = (get_db_config().get('schema_destino') or '').lower()
    try:
        migracoes = listar_migracoes_para_selecao()
    except Exception:  # noqa: BLE001 - sem o banco, só não marca as já cadastradas
        migracoes = []
    por_origem = {(m['tabela_origem'] or '').lower(): m['id'] for m in migracoes}
    por_destino = {(m['tabela_destino'] or '').lower(): m['id'] for m in migracoes}
    resultado = []
    for mp in get_mapeamentos_fdw():
        origem = relacao_origem_da_lista(mp['origem']).lower()
        destino = f'{schema_destino}.{mp["destino"]}'.lower()
        resultado.append({**mp, 'migracao_id': por_origem.get(origem) or por_destino.get(destino)})
    return resultado


@app.route('/nova-migracao', methods=['GET', 'POST'])
def nova_migracao():
    """Criar ou editar uma migração manual"""
    if not is_db_configured():
        if is_ajax_request():
            return jsonify({
                'status': 'erro',
                'mensagem': 'Configure o banco antes de criar uma nova migração.',
                'configurar_banco': url_for('configurar_banco')
            }), 503
        flash('Configure o banco antes de criar uma nova migração.', 'warning')
        return redirect(url_for('configurar_banco'))

    if request.method == 'POST':
        migracao_id_raw = (request.form.get('migracao_id') or '').strip()
        editando_id = int(migracao_id_raw) if migracao_id_raw.isdigit() else None
        tabela_origem_raw = request.form.get('tabela_origem')
        tabela_destino_raw = request.form.get('tabela_destino')
        campo_id = request.form.get('campo_id')
        tamanho_lote = 1

        try:
            tabela_origem = normalizar_origem(tabela_origem_raw)
            tabela_destino = normalize_relation_input(tabela_destino_raw, 'Tabela Destino', get_db_config()['schema_destino'])
            validar_origem(tabela_origem)
        except (ValueError, psycopg2.Error) as error:
            mensagem = mensagem_de_erro(error, 'Origem')
            if is_ajax_request():
                return jsonify({'status': 'erro', 'mensagem': mensagem}), 400
            flash(mensagem, 'error')
            return render_template('nova_migracao.html', fdw_tabelas=get_fdw_tabelas(), fdw_mapeamentos=mapeamentos_fdw_com_migracoes(), migracao_edicao={'id': editando_id} if editando_id else None)

        try:
            with _nova_migracao_lock:
                existente = buscar_migracao_duplicada(tabela_origem, tabela_destino, ignorar_id=editando_id)
                if existente:
                    mensagem = mensagem_migracao_duplicada(existente, tabela_origem)
                    if is_ajax_request():
                        return jsonify({
                            'status': 'erro', 'mensagem': mensagem,
                            'migracao_existente_url': url_for('controle_migracao', id=existente['id']),
                        }), 409
                    flash(mensagem, 'error')
                    return redirect(url_for('controle_migracao', id=existente['id']))

                tabela_criada_agora = garantir_tabela_destino(tabela_origem, tabela_destino, campo_id)

                if editando_id:
                    update_migration_record(
                        migracao_id=editando_id,
                        source_table=tabela_origem,
                        target_table=tabela_destino,
                        key_column=campo_id
                    )
                    migracao_id_resultado = editando_id
                    mensagem_sucesso = (
                        f'Migração #{editando_id} atualizada! Recalculando colunas e totais em segundo plano.'
                    )
                else:
                    migracao = create_migration_record(
                        source_table=tabela_origem,
                        target_table=tabela_destino,
                        key_column=campo_id,
                        batch_size=tamanho_lote
                    )
                    migracao_id_resultado = migracao['migracao_id']
                    mensagem_sucesso = (
                        f'Migração #{migracao_id_resultado} registrada! '
                        f'Acesse o controle para informar a faixa de IDs e iniciar o processamento.'
                    )

                if tabela_criada_agora:
                    flash(f'Tabela de destino "{tabela_destino}" criada automaticamente (mesma estrutura da origem).', 'info')
                if tabela_origem_raw and tabela_origem_raw.strip() != tabela_origem:
                    flash(f'Tabela origem resolvida automaticamente para: {tabela_origem}', 'info')
                if tabela_destino_raw and tabela_destino_raw.strip() != tabela_destino:
                    flash(f'Tabela destino resolvida automaticamente para: {tabela_destino}', 'info')

                # Preparar colunas e totais em segundo plano (apenas COUNT + MAX, não carrega IDs)
                start_background_refresh_batches(migracao_id_resultado)

                flash(mensagem_sucesso, 'success')

                if is_ajax_request():
                    return jsonify({
                        'status': 'sucesso',
                        'mensagem': mensagem_sucesso,
                        'migracao_id': migracao_id_resultado,
                        'redirect_url': url_for('controle_migracao', id=migracao_id_resultado)
                    })

                return redirect(url_for('controle_migracao', id=migracao_id_resultado))
        except (OperationalError, InterfaceError) as error:
            if is_ajax_request():
                response = handle_db_connection_error(error, api=True)
                return response
            return handle_db_connection_error(error)
        except Exception as e:
            acao = 'atualizar' if editando_id else 'criar'
            mensagem = mensagem_de_erro(e, f'Erro ao {acao} migração')
            if is_ajax_request():
                return jsonify({'status': 'erro', 'mensagem': mensagem}), 500
            flash(mensagem, 'error')

    migracao_edicao = None
    editar_id = request.args.get('editar')
    if editar_id and editar_id.isdigit():
        rows = execute_query(
            "SELECT id, tabela_origem, tabela_destino, campo_id FROM controle_migracao WHERE id = %s",
            (int(editar_id),)
        )
        migracao_edicao = rows[0] if rows else None
        if migracao_edicao is None:
            flash('Migração não encontrada para edição.', 'error')

    return render_template('nova_migracao.html', fdw_tabelas=get_fdw_tabelas(), fdw_mapeamentos=mapeamentos_fdw_com_migracoes(), migracao_edicao=migracao_edicao)


# ============================================================
# RELACIONAMENTOS: esquema visual das tabelas das migrações
# ============================================================
# Cada migração tem uma chave (campo_id), tratada como chave primária da
# tabela. Se essa coluna aparece em outra tabela configurada, as duas são
# ligadas (FK -> PK). Duas tabelas com a MESMA chave primária (ex.:
# t_internacao e t_internacao_old) ganham uma ligação de "mesma chave".

def _colunas_das_relacoes(relacoes):
    """{'schema.tabela' (minúsculo): [{'nome', 'tipo'}]} numa consulta só."""
    if not relacoes:
        return {}
    rows = execute_query("""
        SELECT lower(table_schema || '.' || table_name) AS relacao, column_name, data_type
        FROM information_schema.columns
        WHERE lower(table_schema || '.' || table_name) = ANY(%s)
        ORDER BY table_schema, table_name, ordinal_position
    """, (sorted({r.lower() for r in relacoes}),), statement_timeout_ms=30000)
    colunas = {}
    for r in rows:
        colunas.setdefault(r['relacao'], []).append({'nome': r['column_name'], 'tipo': r['data_type']})
    return colunas


def montar_esquema_relacionamentos():
    """Tabelas das migrações (uma por destino) com colunas e as ligações
    encontradas pelo nome da chave primária."""
    migracoes = execute_query(
        "SELECT id, tabela_origem, tabela_destino, campo_id FROM controle_migracao ORDER BY id"
    )
    tabelas = {}
    for mig in migracoes:
        chave = (mig['tabela_destino'] or '').lower()
        if not chave:
            continue
        if chave in tabelas:
            tabelas[chave]['migracoes'].append(mig['id'])
            continue
        tabelas[chave] = {
            'chave': chave,
            'nome': mig['tabela_destino'],
            'origem': mig['tabela_origem'],
            'pk': (mig['campo_id'] or '').strip(),
            'migracoes': [mig['id']],
        }

    colunas = _colunas_das_relacoes([t['nome'] for t in tabelas.values()])
    colunas_fdw = {
        relacao_origem_da_lista(t).lower(): [{'nome': c, 'tipo': None} for c in t.get('colunas') or []]
        for t in get_fdw_tabelas().get('tabelas') or []
    }
    for t in tabelas.values():
        origem = (t['origem'] or '').lower()
        if t['chave'] in colunas:
            t['colunas'], t['fonte'] = colunas[t['chave']], 'destino'
        elif origem in colunas_fdw:
            t['colunas'], t['fonte'] = colunas_fdw[origem], 'fdw'
        else:
            t['colunas'], t['fonte'] = [], None

    # Ligações: coluna de B com o nome da chave primária de A
    tabelas_por_pk = {}
    for t in tabelas.values():
        if t['pk']:
            tabelas_por_pk.setdefault(t['pk'].lower(), []).append(t['chave'])
    relacoes, pares_mesma_chave = [], set()
    for b in tabelas.values():
        for coluna in b['colunas']:
            for a_chave in tabelas_por_pk.get(coluna['nome'].lower(), []):
                if a_chave == b['chave']:
                    continue
                if coluna['nome'].lower() == b['pk'].lower():
                    par = tuple(sorted((a_chave, b['chave'])))
                    if par not in pares_mesma_chave:
                        pares_mesma_chave.add(par)
                        relacoes.append({'de': par[0], 'para': par[1], 'coluna': coluna['nome'], 'tipo': 'mesma_chave'})
                else:
                    relacoes.append({'de': b['chave'], 'para': a_chave, 'coluna': coluna['nome'], 'tipo': 'fk'})

    return {'tabelas': list(tabelas.values()), 'relacoes': relacoes}


@app.route('/relacionamentos')
def relacionamentos():
    """Esquema visual de relacionamento entre as tabelas das migrações."""
    if not is_db_configured():
        flash('Configure o banco antes de ver os relacionamentos.', 'warning')
        return redirect(url_for('configurar_banco'))
    try:
        esquema = montar_esquema_relacionamentos()
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error)
    except psycopg2.Error as error:
        flash(f'Erro ao montar os relacionamentos: {error}', 'error')
        esquema = {'tabelas': [], 'relacoes': []}
    return render_template('relacionamentos.html', esquema=esquema)


@app.route('/importacao-automatica', methods=['GET', 'POST'])
def importacao_automatica():
    """Lista as tabelas configuradas para importação automática (como as
    migrações) e permite cadastrar ou editar. Iniciar/Parar/Verificar/Excluir
    ficam em rotas próprias, uma por tabela."""
    if not is_db_configured():
        flash('Configure o banco antes de acessar a importação automática.', 'warning')
        return redirect(url_for('configurar_banco'))

    if request.method == 'POST':
        tabela_id_raw = (request.form.get('tabela_id') or '').strip()
        tabela_origem_raw = request.form.get('tabela_origem')
        tabela_destino_raw = request.form.get('tabela_destino')
        campo_id = (request.form.get('campo_id') or '').strip()

        try:
            intervalo_min = int(request.form.get('intervalo_min') or 5)
            qtd_registros = int(request.form.get('qtd_registros') or 100)
            tamanho_lote = int(request.form.get('tamanho_lote') or DEFAULT_TAMANHO_LOTE)
        except (TypeError, ValueError):
            flash('Intervalo, quantidade de registros e tamanho do lote devem ser números inteiros.', 'error')
            return redirect(url_for('importacao_automatica'))

        if intervalo_min <= 0 or qtd_registros <= 0 or tamanho_lote <= 0:
            flash('Intervalo, quantidade de registros e tamanho do lote devem ser maiores que zero.', 'error')
            return redirect(url_for('importacao_automatica'))
        tamanho_lote = min(tamanho_lote, MAX_TAMANHO_LOTE)

        try:
            tabela_origem = normalizar_origem(tabela_origem_raw)
            tabela_destino = normalize_relation_input(tabela_destino_raw, 'Tabela Destino', get_db_config()['schema_destino'])
            if not campo_id:
                raise ValueError('Informe o campo chave (campo_id).')
            validar_origem(tabela_origem)

            tabela_criada_agora = garantir_tabela_destino(tabela_origem, tabela_destino, campo_id)

            if tabela_id_raw.isdigit():
                tabela_id = int(tabela_id_raw)
                if not get_auto_import_tabela(tabela_id):
                    raise ValueError('Tabela não encontrada para edição.')
                atualizar_auto_import_tabela(
                    tabela_id, tabela_origem, tabela_destino, campo_id,
                    intervalo_min, qtd_registros, tamanho_lote
                )
                flash(f'Tabela "{tabela_destino}" atualizada.', 'success')
            else:
                criar_auto_import_tabela(tabela_origem, tabela_destino, campo_id, intervalo_min, qtd_registros, tamanho_lote)
                flash(f'Tabela "{tabela_destino}" adicionada à importação automática.', 'success')

            if tabela_criada_agora:
                flash(f'Tabela de destino "{tabela_destino}" criada automaticamente (mesma estrutura da origem).', 'info')
        except (ValueError, psycopg2.Error) as error:
            flash(mensagem_de_erro(error), 'error')

        return redirect(url_for('importacao_automatica'))

    tabela_edicao = None
    editar_id = request.args.get('editar')
    if editar_id and editar_id.isdigit():
        tabela_edicao = get_auto_import_tabela(int(editar_id))
        if not tabela_edicao:
            flash('Tabela não encontrada para edição.', 'error')

    try:
        tabelas = get_auto_import_tabelas()
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error)
    except Exception as e:
        flash(f'Erro ao carregar tabelas configuradas: {str(e)}', 'error')
        tabelas = []

    try:
        migracoes_disponiveis = listar_migracoes_para_selecao()
    except Exception:
        migracoes_disponiveis = []

    return render_template(
        'importacao_automatica.html',
        examples=get_mapeamentos_fdw(),
        tabelas=tabelas,
        tabela_edicao=tabela_edicao,
        migracoes_disponiveis=migracoes_disponiveis
    )


@app.route('/importacao-automatica/<int:tabela_id>/iniciar', methods=['POST'])
def auto_import_iniciar(tabela_id):
    """Marca a tabela como 'ativa' — o agendador do servidor passa a disparar
    os ciclos no intervalo configurado (sem depender de tela aberta)."""
    try:
        if not get_auto_import_tabela(tabela_id):
            return jsonify({'status': 'erro', 'mensagem': 'Tabela não encontrada.'}), 404
        atualizar_status_auto_import_tabela(tabela_id, 'ativa')
        acordar_agendador()
        return jsonify({'status': 'sucesso'})
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error, api=True)
    except Exception as e:
        return jsonify({'status': 'erro', 'mensagem': str(e)}), 500


@app.route('/importacao-automatica/<int:tabela_id>/parar', methods=['POST'])
def auto_import_parar(tabela_id):
    """Marca a tabela como 'parada' e sinaliza cancelamento de um ciclo em
    andamento (se houver) — o worker limpa a staging e para no meio do lote atual."""
    try:
        if not get_auto_import_tabela(tabela_id):
            return jsonify({'status': 'erro', 'mensagem': 'Tabela não encontrada.'}), 404
        atualizar_status_auto_import_tabela(tabela_id, 'parada')

        job_id = _generate_job_id('auto_ciclo', tabela_id)
        job = get_background_job(job_id)
        if job and job.get('status') in ('iniciando', 'processando'):
            set_background_job(job_id, {**job, 'cancelar': True})

        return jsonify({'status': 'sucesso'})
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error, api=True)
    except Exception as e:
        return jsonify({'status': 'erro', 'mensagem': str(e)}), 500


@app.route('/importacao-automatica/<int:tabela_id>/excluir', methods=['POST'])
def auto_import_excluir(tabela_id):
    """Remove a tabela da importação automática (e todo o histórico de ciclos dela)."""
    try:
        if not get_auto_import_tabela(tabela_id):
            flash('Tabela não encontrada.', 'error')
            return redirect(url_for('importacao_automatica'))
        excluir_auto_import_tabela(tabela_id)
        flash('Tabela removida da importação automática.', 'success')
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error)
    except Exception as e:
        flash(f'Erro ao excluir: {str(e)}', 'error')
    return redirect(url_for('importacao_automatica'))


@app.route('/importacao-automatica/<int:tabela_id>/ciclos')
def auto_import_ciclos(tabela_id):
    """Tela 'Verificar' — histórico de ciclos já executados para uma tabela."""
    tabela = get_auto_import_tabela(tabela_id)
    if not tabela:
        flash('Tabela não encontrada.', 'error')
        return redirect(url_for('importacao_automatica'))

    try:
        ciclos = execute_query(
            "SELECT * FROM auto_importacao_ciclos WHERE tabela_id = %s ORDER BY executado_em DESC",
            (tabela_id,)
        )
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error)

    return render_template('importacao_automatica_ciclos.html', tabela=tabela, ciclos=ciclos)


@app.route('/importacao-automatica/ciclo/<int:ciclo_id>/registros')
def auto_import_ciclo_registros(ciclo_id):
    """Detalhe de um ciclo — grid de registros processados, igual ao de uma migração."""
    ciclo_rows = execute_query(
        "SELECT * FROM auto_importacao_ciclos WHERE id = %s", (ciclo_id,)
    )
    if not ciclo_rows:
        flash('Ciclo não encontrado.', 'error')
        return redirect(url_for('importacao_automatica'))
    ciclo = ciclo_rows[0]

    tabela = get_auto_import_tabela(ciclo['tabela_id'])

    try:
        registros = execute_query(
            "SELECT chave_valor, status, erro_msg FROM auto_importacao_ciclo_registros "
            "WHERE ciclo_id = %s ORDER BY id",
            (ciclo_id,)
        )
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error)

    return render_template(
        'importacao_automatica_ciclo_registros.html',
        ciclo=ciclo, tabela=tabela, registros=registros
    )

@app.route('/migracao/<int:id>')
def controle_migracao(id):
    """Página de controle de uma migração específica"""
    if not is_db_configured():
        flash('Configure o banco antes de acessar o controle da migração.', 'warning')
        return redirect(url_for('configurar_banco'))

    try:
        # Buscar informações da migração
        migracao = execute_query(
            "SELECT * FROM controle_migracao WHERE id = %s",
            (id,)
        )[0]
        
        total_registros = migracao['total_registros']
        
        return render_template(
            'controle_migracao.html',
            migracao=migracao,
            total_registros=total_registros
        )
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error)
    except Exception as e:
        flash(f'Erro ao carregar migração: {str(e)}', 'error')
        return redirect(url_for('dashboard'))


@app.route('/migracao/excluir/<int:id>', methods=['POST'])
def excluir_migracao(id):
    """Exclui um registro de controle de migração no dashboard."""
    if not is_db_configured():
        flash('Configure o banco antes de excluir uma migração.', 'warning')
        return redirect(url_for('configurar_banco'))

    try:
        execute_query(
            "DELETE FROM controle_migracao WHERE id = %s",
            (id,),
            fetch=False
        )
        flash(f'Migração #{id} excluída com sucesso.', 'success')
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error)
    except Exception as error:
        flash(f'Erro ao excluir migração: {str(error)}', 'error')

    return redirect(url_for('dashboard'))


@app.route('/migracao/atualizar-lotes/<int:id>', methods=['POST'])
def atualizar_lotes_migracao(id):
    """Recalcula a consulta de diferenças e regenera os lotes pendentes da migração."""
    if not is_db_configured():
        flash('Configure o banco antes de atualizar os lotes.', 'warning')
        return redirect(url_for('configurar_banco'))

    start_background_refresh_batches(id)
    flash(
        f'Migração #{id}: atualização de lotes iniciada em segundo plano.',
        'success'
    )

    return redirect(url_for('controle_migracao', id=id))


@app.route('/api/processar-registro', methods=['POST'])
def api_processar_registro():
    """API para processar um registro específico por chave.

    NOTA: continua consultando a origem diretamente (sem staging em temp).
    Para um único ID, criar/copiar/derrubar tabela temp não compensa o custo.
    """
    if not is_db_configured():
        return jsonify({
            'status': 'erro',
            'mensagem': 'Configure a conexão com o banco de dados para continuar.',
            'configurar_banco': url_for('configurar_banco')
        }), 503

    data = request.json
    migracao_id = data.get('migracao_id')
    chave_valor = data.get('chave_valor')

    if not chave_valor:
        return jsonify({'status': 'erro', 'mensagem': 'Informe a chave do registro.'}), 400

    try:
        def processar_registro_conexao(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Ler config da migração
                cur.execute("""
                    SELECT tabela_origem, tabela_destino, campo_id,
                           sql_colunas_insert, sql_colunas_select
                    FROM controle_migracao WHERE id = %s
                """, (migracao_id,))
                cfg = cur.fetchone()

                if not cfg:
                    return {'registros_afetados': 0, 'status': 'erro', 'mensagem': 'Migração não encontrada'}

                tab_orig = cfg['tabela_origem']
                tab_dest = cfg['tabela_destino']
                campo = cfg['campo_id']
                cols_ins = cfg['sql_colunas_insert']
                cols_sel = cfg['sql_colunas_select']

                if not cols_ins or not cols_sel:
                    return {'registros_afetados': 0, 'status': 'erro', 'mensagem': 'Colunas não preparadas'}

                # 1) Copia a linha da origem pra uma staging só deste registro
                #    (não colide com a staging de uma migração em andamento) e
                #    insere/atualiza conforme a diferença com o destino
                staging_registro = f'{TEMP_SCHEMA_NAME}.r_{int(migracao_id)}'
                ensure_temp_schema(conn)
                destino = normalize_relation_input(tab_dest, 'Tabela Destino', get_db_config()['schema_destino'])
                criar_staging_como_destino(conn, staging_registro, destino, _split_sql_columns(cols_ins))
                try:
                    copiar_ids_da_origem(conn, staging_registro, normalizar_origem(tab_orig), campo,
                                         _split_sql_columns(cols_ins), [chave_valor])
                finally:
                    fechar_conexao_origem_da_thread()
                sql_upsert_one = build_single_record_upsert_sql(staging_registro, tab_dest, campo, cols_ins, cols_sel)
                chave_int = int(chave_valor)
                cur.execute(sql_upsert_one, (chave_int,))
                outcome = cur.fetchone() or {}
                cur.execute(f'DROP TABLE IF EXISTS {staging_registro}')
                existe_origem = outcome.get('existe_origem')
                inserido = outcome.get('inserido')

                if not existe_origem:
                    acao = 'nao_encontrado'
                    rows = 0
                elif inserido is True:
                    acao = 'inserido'
                    rows = 1
                elif inserido is False:
                    acao = 'atualizado'
                    rows = 1
                else:
                    acao = 'ja_existe'
                    rows = 0

                # 3) Registrar no controle de lotes
                cur.execute("""
                    INSERT INTO controle_migracao_lotes
                    (migracao_id, numero_lote, chave_valor, status, erro_msg, updated_at)
                    VALUES (%s, 1, %s, %s, NULL, CURRENT_TIMESTAMP)
                    ON CONFLICT (migracao_id, chave_valor) DO UPDATE
                    SET status = EXCLUDED.status, erro_msg = NULL, updated_at = CURRENT_TIMESTAMP
                """, (migracao_id, chave_valor, acao))

                # 4) Atualizar contador se inserido/atualizado
                if acao in ('inserido', 'atualizado'):
                    cur.execute("""
                        UPDATE controle_migracao
                        SET registros_processados = registros_processados + 1,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (migracao_id,))

                # 5) Buscar progresso
                cur.execute("SELECT * FROM verificar_progresso(%s)", (migracao_id,))
                progresso = cur.fetchone()

            result = {
                'registros_afetados': rows,
                'status': 'nao_encontrado' if acao == 'nao_encontrado' else 'sucesso',
                'mensagem': f'Registro {chave_valor}: {acao}'
            }
            if progresso:
                result['progresso'] = dict(progresso)
            return result

        result = execute_db_operation(processar_registro_conexao, statement_timeout_ms=120000)
        return jsonify(result)
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error, api=True)
    except Exception as e:
        return jsonify({
            'status': 'erro',
            'mensagem': str(e),
            'registros_inseridos': 0
        }), 500


@app.route('/api/registros/<int:migracao_id>')
def api_registros(migracao_id):
    """Retorna os registros da migração com status para exibição no grid."""
    if not is_db_configured():
        return jsonify({'status': 'erro', 'mensagem': 'Banco não configurado.'}), 503

    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 500, type=int)
    status_filter = request.args.get('status', '').strip()
    per_page = min(per_page, 2000)
    offset = (page - 1) * per_page

    try:
        if status_filter:
            registros = execute_query(
                """SELECT id, chave_valor, status, erro_msg
                   FROM controle_migracao_lotes
                   WHERE migracao_id = %s AND status = %s
                   ORDER BY id DESC LIMIT %s OFFSET %s""",
                (migracao_id, status_filter, per_page, offset)
            )
        else:
            registros = execute_query(
                """SELECT id, chave_valor, status, erro_msg
                   FROM controle_migracao_lotes
                   WHERE migracao_id = %s
                   ORDER BY id DESC LIMIT %s OFFSET %s""",
                (migracao_id, per_page, offset)
            )

        stats = execute_query("""
            SELECT
                COUNT(*)::INTEGER as total,
                COUNT(*) FILTER (WHERE status = 'inserido')::INTEGER as inseridos,
                COUNT(*) FILTER (WHERE status = 'ja_existe')::INTEGER as ja_existem,
                COUNT(*) FILTER (WHERE status = 'atualizado')::INTEGER as atualizados,
                COUNT(*) FILTER (WHERE status = 'nao_encontrado')::INTEGER as nao_encontrados,
                COUNT(*) FILTER (WHERE status = 'erro')::INTEGER as com_erro
            FROM controle_migracao_lotes
            WHERE migracao_id = %s
        """, (migracao_id,))[0]

        # Buscar o último registro existente na tabela destino
        try:
            cfg = execute_query(
                "SELECT tabela_destino, campo_id FROM controle_migracao WHERE id = %s",
                (migracao_id,)
            )[0]
            ultimo = execute_query(
                f"SELECT MAX({cfg['campo_id']}) as ultimo_id FROM {cfg['tabela_destino']}"
            )[0]
            stats['ultimo_inserido'] = ultimo.get('ultimo_id')
        except Exception:
            stats['ultimo_inserido'] = None

        return jsonify({
            'registros': registros,
            'stats': stats,
            'page': page,
            'per_page': per_page
        })
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error, api=True)
    except Exception as e:
        return jsonify({'status': 'erro', 'mensagem': str(e)}), 500


def _cancelar_ciclo_auto_import(job_id, conn, staging):
    """Chamado quando o usuário clica em 'Parar' com um ciclo em andamento:
    derruba a staging (não deixa lixo em `temp`) e marca o job como cancelado."""
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS {staging}')
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

    set_background_job(job_id, {
        **(get_background_job(job_id) or {}),
        'status': 'cancelado',
        'mensagem': 'Ciclo interrompido pelo usuário.',
        'fim': datetime.now().isoformat(),
    })
    _log_ciclo(job_id, 'aviso', 'Ciclo interrompido pelo usuário — staging removida.')


LOG_CICLO_MAX_LINHAS = 300  # trava de tamanho do log em memória por ciclo
LOTES_CICLO_MAX_ITENS = 300  # idem para o histórico de lotes


def _log_ciclo(job_id, nivel, mensagem):
    """Adiciona uma linha ao log do ciclo (lido pela tela de acompanhamento)."""
    job = get_background_job(job_id) or {}
    log = list(job.get('log') or [])
    log.append({
        'hora': datetime.now().strftime('%H:%M:%S'),
        'nivel': nivel,  # 'info' | 'sucesso' | 'aviso' | 'erro'
        'mensagem': mensagem,
    })
    if len(log) > LOG_CICLO_MAX_LINHAS:
        log = log[-LOG_CICLO_MAX_LINHAS:]
    set_background_job(job_id, {**job, 'log': log})


def _registrar_lote_no_job(job_id, numero, inseridos, atualizados, ja_existem, com_erro):
    """Adiciona o resultado de um lote à lista de lotes do job (tela de acompanhamento)."""
    job = get_background_job(job_id) or {}
    lotes = list(job.get('lotes') or [])
    lotes.append({
        'numero': numero,
        'inseridos': inseridos,
        'atualizados': atualizados,
        'ja_existem': ja_existem,
        'com_erro': com_erro,
    })
    if len(lotes) > LOTES_CICLO_MAX_ITENS:
        lotes = lotes[-LOTES_CICLO_MAX_ITENS:]
    set_background_job(job_id, {**job, 'lotes': lotes})


def _background_auto_import_ciclo_worker(job_id, tabela_id):
    """Executa um ciclo de importação automática em segundo plano.

    Usa a MESMA arquitetura do worker de migração manual (só muda o nome da
    staging, com prefixo `c_` em vez de `stg_mig_<id>`, pra nunca colidir):
    monta uma lista explícita de IDs a verificar e processa em lotes; cada
    lote copia só aqueles IDs direto do servidor de origem via
    `WHERE chave = ANY(...)` (usa o índice da origem, sem ORDER BY/LIMIT).
    Reaproveita direto as funções da migração: copiar_lote_para_staging,
    processar_lote_bulk, processar_lote_fallback.
    """
    job = get_background_job(job_id)
    if not job:
        return

    set_background_job(job_id, {**job, 'status': 'processando', 'inicio': datetime.now().isoformat(), 'log': [], 'lotes': []})
    _log_ciclo(job_id, 'info', 'Ciclo iniciado.')

    tabela_cfg = get_auto_import_tabela(tabela_id)
    if not tabela_cfg:
        set_background_job(job_id, {**get_background_job(job_id), 'status': 'erro', 'mensagem': 'Tabela não encontrada.'})
        _log_ciclo(job_id, 'erro', 'Tabela não encontrada.')
        return

    try:
        normalized_source = normalizar_origem(tabela_cfg['tabela_origem'])
        normalized_target = normalize_relation_input(tabela_cfg['tabela_destino'], 'Tabela Destino', get_db_config()['schema_destino'])
    except (ValueError, psycopg2.Error) as error:
        mensagem = mensagem_de_erro(error)
        set_background_job(job_id, {**get_background_job(job_id), 'status': 'erro', 'mensagem': mensagem})
        _log_ciclo(job_id, 'erro', mensagem)
        return

    normalized_key = tabela_cfg['campo_id']
    qtd_registros = tabela_cfg['qtd_registros']
    tamanho_lote = min(tabela_cfg['tamanho_lote'], MAX_TAMANHO_LOTE)
    quoted_target = quote_qualified_relation(normalized_target)
    quoted_key = quote_identifier(normalized_key)

    def abrir_conexao_auto():
        for enc in get_db_client_encoding_candidates():
            try:
                c = open_db_connection(client_encoding=enc, statement_timeout_ms=120000)
                persist_working_client_encoding(enc)
                return c
            except Exception:
                continue
        return open_db_connection(statement_timeout_ms=120000)

    conn = None
    registros_ciclo = []  # (chave, status, erro_msg) — vira o detalhe do ciclo
    try:
        conn = abrir_conexao_auto()
        query = run_query_with_connection

        # Trava por tabela: evita que duas execuções simultâneas colidam na
        # mesma staging (temp.c_<tabela>).
        lock_row = query(conn, "SELECT pg_try_advisory_lock(%s) AS locked", (tabela_id,))
        if not (lock_row and lock_row[0]['locked']):
            set_background_job(job_id, {
                **get_background_job(job_id), 'status': 'erro',
                'mensagem': 'Já existe um ciclo em andamento para esta tabela.',
            })
            return
        # Trava de sessão: libera sozinha quando `conn.close()` roda.

        # 1. Colunas comuns entre origem (lidas no servidor de origem) e destino
        target_schema, target_table = normalized_target.split('.', 1)
        source_columns = {c['nome'] for c in colunas_origem(normalized_source)}

        target_cols_raw = query(conn,
            "SELECT column_name, is_identity, is_generated, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (target_schema, target_table))

        if normalized_key not in source_columns:
            raise ValueError(f'A coluna chave "{normalized_key}" não existe na tabela de origem {normalized_source}.')
        if not any(c['column_name'] == normalized_key for c in target_cols_raw):
            raise ValueError(f'A coluna chave "{normalized_key}" não existe na tabela de destino {normalized_target}.')

        common_columns = [
            c['column_name'] for c in target_cols_raw
            if c['column_name'] != normalized_key
            and c['column_name'] in source_columns
            and c.get('is_generated') != 'ALWAYS'
        ]

        all_columns = [normalized_key] + common_columns
        cols_ins = ', '.join(all_columns)  # CSV de nomes SEM aspas — copiar_lote_para_staging/
                                            # montar_sql_upsert_lote fazem a própria quotação
                                            # internamente (é o mesmo formato que sql_colunas_insert
                                            # usa nas migrações manuais)

        # 2. MAX(campo_id) do destino e da origem. A migração recebe a faixa
        #    (ID Inicial/Final) escolhida pelo usuário; aqui, como ninguém
        #    escolhe na hora, precisamos descobrir sozinhos onde parar. Um
        #    MAX com índice é rápido e sempre é empurrado pro FDW (vira
        #    ORDER BY DESC LIMIT 1 remotamente).
        # int(...) é necessário porque campo_id pode ser NUMERIC no Postgres
        # (comum nesse sistema — ex.: "isn_marcacao > 132115::numeric" nos
        # logs) e o psycopg2 devolve NUMERIC como Decimal, que o range()
        # do Python não aceita ("'decimal.Decimal' object cannot be
        # interpreted as an integer").
        max_destino = query(conn, f"SELECT MAX({quoted_key}) AS ultimo_id FROM {quoted_target}")
        ultimo_id_destino = int(max_destino[0]['ultimo_id']) if max_destino and max_destino[0]['ultimo_id'] is not None else 0

        ultimo_id_origem = max_id_origem(normalized_source, normalized_key)

        # 3. Faixa: id_fim é SEMPRE o máximo real da origem — nunca capado,
        #    senão registros novos além do teto de segurança ficariam de fora
        #    (bug real: com destino quase cheio e qtd_registros pequeno
        #    perto do tamanho da tabela, o teto cortava antes de chegar nos
        #    IDs novos). Quem se ajusta pelo teto é o INÍCIO da faixa: pega o
        #    mais recente entre "dobro pra trás" (gaps) e "só os últimos N
        #    antes do fim" (não deixa a janela estourar num destino vazio).
        id_inicio = max(0, ultimo_id_destino - qtd_registros)
        id_inicio_dobro = max(0, id_inicio - qtd_registros)
        id_fim = ultimo_id_origem
        limite_janela = qtd_registros * 2
        id_inicio_faixa = max(0, id_inicio_dobro, id_fim - limite_janela)

        ids_list = [str(v) for v in range(id_inicio_faixa + 1, id_fim + 1)] if id_fim > id_inicio_faixa else []

        # 4. Staging em `temp` com prefixo c_ — mesma estrutura da migração
        #    (só estrutura, WHERE FALSE), reaproveitada por todos os lotes.
        staging = obter_staging_table_auto(normalized_target)
        ensure_temp_schema(conn)

        conn = executar_com_reconexao(
            lambda c: criar_staging_como_destino(c, staging, normalized_target, all_columns),
            abrir_conexao_auto, conn, job_id=job_id, etapa='banco local'
        )

        total_lotes = math.ceil(len(ids_list) / tamanho_lote) if ids_list else 0

        set_background_job(job_id, {
            **get_background_job(job_id),
            'total_lotes': total_lotes,
            'lote_atual': 0,
            'tamanho_lote': tamanho_lote,
            'mensagem': f'{len(ids_list)} id(s) a verificar em {total_lotes} lote(s)...',
        })
        _log_ciclo(
            job_id, 'info',
            f'{len(ids_list)} id(s) a verificar (faixa {id_inicio_faixa + 1}-{id_fim}) em {total_lotes} lote(s).'
        )

        # 5. Processa cada lote: copia só os IDs daquele lote pra staging
        #    (WHERE chave = ANY(...), empurrado pro FDW via índice — mesma
        #    lógica que já funciona sem problemas na migração manual, em
        #    qualquer tabela) e faz o upsert em lote contra o destino.
        #    Reaproveita as funções da migração sem alterar nenhuma delas.
        registros_inseridos = 0
        registros_atualizados = 0
        registros_ja_existem = 0
        registros_nao_encontrados = 0
        registros_com_erro = 0

        for lote_idx in range(total_lotes):
            if (get_background_job(job_id) or {}).get('cancelar'):
                _cancelar_ciclo_auto_import(job_id, conn, staging)
                return

            ids_lote = ids_list[lote_idx * tamanho_lote: (lote_idx + 1) * tamanho_lote]

            set_background_job(job_id, {
                **get_background_job(job_id),
                'mensagem': f'Lote {lote_idx + 1}/{total_lotes}: copiando {len(ids_lote)} registro(s) da origem...',
            })

            # Copia o lote pra staging — reconecta automaticamente (com mais
            # paciência em conflito de recovery) se a origem cair no meio.
            conn = executar_com_reconexao(
                lambda c: copiar_lote_para_staging(c, staging, normalized_source, normalized_key, cols_ins, ids_lote),
                abrir_conexao_auto, conn, job_id=job_id, etapa='origem'
            )

            # Remove duplicatas de chave dentro do lote — algumas views de
            # origem retornam mais de uma linha pra mesma chave (ex.: JOIN que
            # multiplica linhas, comum em views de agendamento/telefone). Sem
            # isso, o upsert em lote falha sempre com "ON CONFLICT ... cannot
            # affect row a second time" e cai pro fallback lento em TODO
            # lote. Mantém uma linha por chave (mesmo critério "arbitrário"
            # que o fallback linha-a-linha já usa via LIMIT 1). Não mexe em
            # nenhuma função da migração — só uma limpeza extra na staging
            # da importação automática.
            with conn.cursor() as cur:
                cur.execute(
                    f'DELETE FROM {staging} a USING {staging} b '
                    f'WHERE a.ctid < b.ctid AND a.{quoted_key} = b.{quoted_key}'
                )
            conn.commit()

            # Upsert em lote (caminho rápido); se falhar, cai pro fallback
            # registro a registro — mesmas funções que a migração usa.
            try:
                resultado = processar_lote_bulk(conn, staging, normalized_target, normalized_key, cols_ins, ids_lote)
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                _log_ciclo(job_id, 'aviso', f'Lote {lote_idx + 1}/{total_lotes}: upsert em lote falhou, reprocessando registro a registro...')
                resultado = processar_lote_fallback(conn, staging, normalized_target, normalized_key, cols_ins, ids_lote)

            lote_inseridos = len(resultado['inseridos'])
            lote_atualizados = len(resultado['atualizados'])
            lote_ja_existem = len(resultado['ja_existem'])
            lote_nao_encontrados = len(resultado['nao_encontrados'])
            lote_com_erro = len(resultado['com_erro'])

            registros_inseridos += lote_inseridos
            registros_atualizados += lote_atualizados
            registros_ja_existem += lote_ja_existem
            registros_nao_encontrados += lote_nao_encontrados
            registros_com_erro += lote_com_erro

            for chave in resultado['inseridos']:
                registros_ciclo.append((chave, 'inserido', None))
            for chave in resultado['atualizados']:
                registros_ciclo.append((chave, 'atualizado', None))
            for chave in resultado['ja_existem']:
                registros_ciclo.append((chave, 'ja_existe', None))
            for chave in resultado['nao_encontrados']:
                registros_ciclo.append((chave, 'nao_encontrado', None))
            for item in resultado['com_erro']:
                registros_ciclo.append((item['chave'], 'erro', item['erro']))

            _registrar_lote_no_job(job_id, lote_idx + 1, lote_inseridos, lote_atualizados, lote_ja_existem, lote_com_erro)

            partes_lote = []
            if lote_inseridos: partes_lote.append(f'{lote_inseridos} inserido(s)')
            if lote_atualizados: partes_lote.append(f'{lote_atualizados} atualizado(s)')
            if lote_ja_existem: partes_lote.append(f'{lote_ja_existem} já existente(s)')
            if lote_nao_encontrados: partes_lote.append(f'{lote_nao_encontrados} não encontrado(s)')
            if lote_com_erro: partes_lote.append(f'{lote_com_erro} com erro')
            _log_ciclo(
                job_id, 'erro' if lote_com_erro else 'info',
                f'Lote {lote_idx + 1}/{total_lotes}: ' + (', '.join(partes_lote) if partes_lote else 'nada a fazer') + '.'
            )

            set_background_job(job_id, {
                **get_background_job(job_id),
                'lote_atual': lote_idx + 1,
                'total_lotes': total_lotes,
                'registros_inseridos': registros_inseridos,
                'registros_atualizados': registros_atualizados,
                'registros_ja_existem': registros_ja_existem,
                'registros_com_erro': registros_com_erro,
                'mensagem': f'Lote {lote_idx + 1}/{total_lotes}...',
            })

        # 6. Staging é recriada do zero a cada ciclo — remove ao final
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS {staging}')
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass

        # 7. Novo MAX(campo_id) do destino
        max_after = query(conn, f"SELECT MAX({quoted_key}) AS ultimo_id FROM {quoted_target}")
        novo_ultimo_id = int(max_after[0]['ultimo_id']) if max_after and max_after[0]['ultimo_id'] is not None else 0

        # 8. Grava o resumo do ciclo + o detalhe por chave (histórico persistido)
        status_ciclo = 'erro' if (
            registros_com_erro > 0
            and registros_inseridos == 0 and registros_atualizados == 0 and registros_ja_existem == 0
        ) else 'concluido'
        partes_msg = []
        if registros_inseridos: partes_msg.append(f'{registros_inseridos} inserido(s)')
        if registros_atualizados: partes_msg.append(f'{registros_atualizados} atualizado(s)')
        if registros_ja_existem: partes_msg.append(f'{registros_ja_existem} já existente(s)')
        if registros_nao_encontrados: partes_msg.append(f'{registros_nao_encontrados} não encontrado(s)')
        if registros_com_erro: partes_msg.append(f'{registros_com_erro} com erro')
        mensagem_ciclo = ', '.join(partes_msg) if partes_msg else 'nenhum registro na janela'

        ciclo_id = registrar_ciclo_auto_import(
            conn, tabela_id, status_ciclo, mensagem_ciclo,
            registros_inseridos, registros_atualizados, registros_ja_existem, registros_nao_encontrados,
            registros_com_erro, total_lotes, tamanho_lote, id_inicio_faixa, novo_ultimo_id
        )
        gravar_registros_ciclo(conn, ciclo_id, registros_ciclo)

    except ValueError as error:
        set_background_job(job_id, {**get_background_job(job_id), 'status': 'erro', 'mensagem': str(error)})
        _log_ciclo(job_id, 'erro', f'Ciclo interrompido: {error}')
        return
    except Exception as error:
        mensagem = mensagem_de_erro(error, 'Erro fatal no ciclo')
        set_background_job(job_id, {**get_background_job(job_id), 'status': 'erro', 'mensagem': mensagem})
        _log_ciclo(job_id, 'erro', mensagem)
        return
    finally:
        fechar_conexao_origem_da_thread()
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    try:
        atualizar_ultimo_id_auto_import(tabela_id, novo_ultimo_id)
    except Exception:
        pass

    _log_ciclo(job_id, 'sucesso', f'Ciclo concluído: {mensagem_ciclo}.')
    set_background_job(job_id, {
        **get_background_job(job_id),
        'status': 'concluido',
        'mensagem': mensagem_ciclo,
        'ciclo_id': ciclo_id,
        'ultimo_id_destino': novo_ultimo_id,
        'id_inicio_faixa': id_inicio_faixa,
        'tamanho_lote': tamanho_lote,
        'total_lotes': total_lotes,
        'lote_atual': total_lotes,
        'registros_inseridos': registros_inseridos,
        'registros_atualizados': registros_atualizados,
        'registros_ja_existem': registros_ja_existem,
        'registros_com_erro': registros_com_erro,
        'fim': datetime.now().isoformat(),
    })


def _fechar_execucao_com_erro(conn, execucao_id, mensagem_erro, variaveis_locais):
    """Se a execução já tinha criado o registro (status 'processando') quando
    uma exceção interrompeu o worker, fecha esse registro como 'erro' em vez
    de deixá-lo preso em 'processando' pra sempre. Best-effort: se a conexão
    já estiver quebrada pelo mesmo erro, só desiste silenciosamente."""
    if execucao_id is None or conn is None:
        return
    try:
        atualizar_execucao_agendamento(
            conn, execucao_id, 'erro', mensagem_erro[:500],
            variaveis_locais.get('registros_inseridos', 0),
            variaveis_locais.get('registros_atualizados', 0),
            variaveis_locais.get('registros_ja_existem', 0),
            variaveis_locais.get('registros_deletados', 0),
            variaveis_locais.get('registros_com_erro', 0),
            variaveis_locais.get('ultimo_id_destino')
        )
    except Exception:
        pass


def _background_agendamento_worker(job_id, tabela_id):
    """Executa uma importação COMPLETA (insert/update/delete) em segundo plano.

    Mesma arquitetura do worker de migração/importação automática — lista
    explícita de IDs processada em lotes (WHERE chave = ANY(...), empurrado
    pro FDW via índice) — mas cobrindo a faixa INTEIRA (1 até o maior ID
    entre origem e destino), não uma janela recente, e com um passo extra de
    DELETE pra remover do destino o que não existe mais na origem. Staging
    com prefixo a_, pra nunca colidir com o stg_mig_<id> da migração nem com
    o c_<tabela> da importação automática.
    """
    job = get_background_job(job_id)
    if not job:
        return

    set_background_job(job_id, {**job, 'status': 'processando', 'inicio': datetime.now().isoformat(), 'log': [], 'lotes': []})
    _log_execucao(job_id, 'info', 'Execução iniciada.')

    tabela_cfg = get_agendamento_tabela(tabela_id)
    if not tabela_cfg:
        set_background_job(job_id, {**get_background_job(job_id), 'status': 'erro', 'mensagem': 'Tabela não encontrada.'})
        _log_execucao(job_id, 'erro', 'Tabela não encontrada.')
        return

    try:
        normalized_source = normalizar_origem(tabela_cfg['tabela_origem'])
        normalized_target = normalize_relation_input(tabela_cfg['tabela_destino'], 'Tabela Destino', get_db_config()['schema_destino'])
    except (ValueError, psycopg2.Error) as error:
        mensagem = mensagem_de_erro(error)
        set_background_job(job_id, {**get_background_job(job_id), 'status': 'erro', 'mensagem': mensagem})
        _log_execucao(job_id, 'erro', mensagem)
        return

    normalized_key = tabela_cfg['campo_id']
    tamanho_lote = min(tabela_cfg['tamanho_lote'], MAX_TAMANHO_LOTE)
    quoted_target = quote_qualified_relation(normalized_target)
    quoted_key = quote_identifier(normalized_key)

    def abrir_conexao_agendamento():
        for enc in get_db_client_encoding_candidates():
            try:
                c = open_db_connection(client_encoding=enc, statement_timeout_ms=120000)
                persist_working_client_encoding(enc)
                return c
            except Exception:
                continue
        return open_db_connection(statement_timeout_ms=120000)

    conn = None
    execucao_id = None
    try:
        conn = abrir_conexao_agendamento()
        query = run_query_with_connection

        # Trava por tabela — soma um deslocamento grande ao id pra não usar o
        # mesmo número de trava que a importação automática usa pra essa
        # mesma tabela_id (evitaria as duas rodarem juntas por engano).
        lock_row = query(conn, "SELECT pg_try_advisory_lock(%s) AS locked", (200000000 + tabela_id,))
        if not (lock_row and lock_row[0]['locked']):
            set_background_job(job_id, {
                **get_background_job(job_id), 'status': 'erro',
                'mensagem': 'Já existe uma execução em andamento para esta tabela.',
            })
            return
        # Trava de sessão: libera sozinha quando `conn.close()` roda.

        # 1. Colunas comuns entre origem (lidas no servidor de origem) e destino
        target_schema, target_table = normalized_target.split('.', 1)
        source_columns = {c['nome'] for c in colunas_origem(normalized_source)}

        target_cols_raw = query(conn,
            "SELECT column_name, is_identity, is_generated, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (target_schema, target_table))

        if normalized_key not in source_columns:
            raise ValueError(f'A coluna chave "{normalized_key}" não existe na tabela de origem {normalized_source}.')
        if not any(c['column_name'] == normalized_key for c in target_cols_raw):
            raise ValueError(f'A coluna chave "{normalized_key}" não existe na tabela de destino {normalized_target}.')

        common_columns = [
            c['column_name'] for c in target_cols_raw
            if c['column_name'] != normalized_key
            and c['column_name'] in source_columns
            and c.get('is_generated') != 'ALWAYS'
        ]

        all_columns = [normalized_key] + common_columns
        cols_ins = ', '.join(all_columns)  # CSV sem aspas — as funções da migração quotam sozinhas

        # 2. MAX(campo_id) do destino e da origem. int(...) porque campo_id
        #    pode ser NUMERIC no Postgres (Decimal não funciona em range()).
        max_destino = query(conn, f"SELECT MAX({quoted_key}) AS ultimo_id FROM {quoted_target}")
        ultimo_id_destino = int(max_destino[0]['ultimo_id']) if max_destino and max_destino[0]['ultimo_id'] is not None else 0

        ultimo_id_origem = max_id_origem(normalized_source, normalized_key)

        # Libera a transação logo — sem isso a conexão fica "idle in
        # transaction" seguradno o snapshot enquanto o Python monta a lista
        # de lotes abaixo, o que pode travar VACUUM e outras operações no
        # banco em tabelas grandes (psycopg2 não usa autocommit por padrão).
        conn.commit()

        # 3. Faixa: do "A partir do ID" configurado (padrão 1) até o maior ID
        #    entre origem e destino — sem o maior dos dois, um ID que só existe
        #    no destino (e deveria ser deletado) nunca entraria na verificação.
        #    IDs abaixo do início ficam fora: nem atualizados nem deletados.
        #
        # IMPORTANTE: NÃO materializa a lista inteira de IDs em memória (um
        # `range(1, id_fim + 1)` convertido em lista de strings, pra uma
        # tabela com milhões de linhas, consome memória e CPU excessivos —
        # foi isso que travou o banco). Cada lote calcula só os IDs que
        # precisa, na hora, via slice do range.
        id_inicio = max(int(tabela_cfg.get('id_inicio') or 1), 1)
        id_fim = max(ultimo_id_destino, ultimo_id_origem)
        total_ids = max(id_fim - id_inicio + 1, 0)
        total_lotes = math.ceil(total_ids / tamanho_lote)

        def ids_do_lote(lote_idx):
            inicio = id_inicio + lote_idx * tamanho_lote
            fim = min(inicio + tamanho_lote - 1, id_fim)
            return [str(v) for v in range(inicio, fim + 1)]

        # 4. Staging em `temp` com prefixo a_ — mesma estrutura da migração
        #    (só estrutura, WHERE FALSE), reaproveitada por todos os lotes.
        staging = obter_staging_table_agendamento(normalized_target)
        ensure_temp_schema(conn)

        conn = executar_com_reconexao(
            lambda c: criar_staging_como_destino(c, staging, normalized_target, all_columns),
            abrir_conexao_agendamento, conn, job_id=job_id, etapa='banco local'
        )

        set_background_job(job_id, {
            **get_background_job(job_id),
            'total_lotes': total_lotes,
            'lote_atual': 0,
            'tamanho_lote': tamanho_lote,
            'mensagem': f'{total_ids} id(s) a verificar em {total_lotes} lote(s)...',
        })
        _log_execucao(
            job_id, 'info',
            f'{total_ids} id(s) a verificar (faixa {id_inicio}-{id_fim}) em {total_lotes} lote(s).'
        )

        # Cria o registro da execução JÁ AGORA (contadores em zero, status
        # 'processando') em vez de só no final — permite gravar o detalhe
        # por chave lote a lote (evita acumular milhões de linhas em memória
        # pra tabelas grandes, e evita uma única instrução gigante no final).
        execucao_id = registrar_execucao_agendamento(
            conn, tabela_id, 'processando', 'em andamento...',
            0, 0, 0, 0, 0, total_lotes, tamanho_lote, ultimo_id_destino
        )

        # 5. Processa cada lote: copia os IDs daquele lote pra staging (mesma
        #    função da migração), faz o upsert em lote e, pra quem não foi
        #    encontrado na origem, apaga do destino (se existir lá).
        registros_inseridos = 0
        registros_atualizados = 0
        registros_ja_existem = 0
        registros_deletados = 0
        registros_com_erro = 0

        for lote_idx in range(total_lotes):
            if (get_background_job(job_id) or {}).get('cancelar'):
                _cancelar_execucao_agendamento(job_id, conn, staging, execucao_id, {
                    'registros_inseridos': registros_inseridos,
                    'registros_atualizados': registros_atualizados,
                    'registros_ja_existem': registros_ja_existem,
                    'registros_deletados': registros_deletados,
                    'registros_com_erro': registros_com_erro,
                    'ultimo_id_destino': ultimo_id_destino,
                })
                return

            ids_lote = ids_do_lote(lote_idx)

            set_background_job(job_id, {
                **get_background_job(job_id),
                'mensagem': f'Lote {lote_idx + 1}/{total_lotes}: copiando {len(ids_lote)} registro(s) da origem...',
            })

            # Copia o lote pra staging — reconecta automaticamente (com mais
            # paciência em conflito de recovery) se a origem cair no meio.
            conn = executar_com_reconexao(
                lambda c: copiar_lote_para_staging(c, staging, normalized_source, normalized_key, cols_ins, ids_lote),
                abrir_conexao_agendamento, conn, job_id=job_id, etapa='origem'
            )

            # Remove duplicatas de chave dentro do lote — mesma razão da
            # importação automática (view de origem pode ter JOIN que
            # multiplica linhas, o que quebraria o upsert em lote).
            with conn.cursor() as cur:
                cur.execute(
                    f'DELETE FROM {staging} a USING {staging} b '
                    f'WHERE a.ctid < b.ctid AND a.{quoted_key} = b.{quoted_key}'
                )
            conn.commit()

            # Upsert em lote (caminho rápido); se falhar, cai pro fallback
            # registro a registro — mesmas funções que a migração usa.
            try:
                resultado = processar_lote_bulk(conn, staging, normalized_target, normalized_key, cols_ins, ids_lote)
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                _log_execucao(job_id, 'aviso', f'Lote {lote_idx + 1}/{total_lotes}: upsert em lote falhou, reprocessando registro a registro...')
                resultado = processar_lote_fallback(conn, staging, normalized_target, normalized_key, cols_ins, ids_lote)

            # DELETE: chaves deste lote que não existem na origem — se também
            # não existirem no destino, o DELETE só não afeta nenhuma linha
            # (inofensivo). RETURNING confirma quais realmente foram apagadas.
            deletados_lote = []
            if resultado['nao_encontrados']:
                try:
                    del_ids = [int(v) for v in resultado['nao_encontrados']]
                    del_result = query(
                        conn,
                        f'DELETE FROM {quoted_target} WHERE {quoted_key} = ANY(%s) RETURNING {quoted_key} AS chave',
                        (del_ids,)
                    )
                    conn.commit()
                    deletados_lote = [str(r['chave']) for r in del_result]
                except Exception as del_error:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    _log_execucao(job_id, 'erro', f'Lote {lote_idx + 1}/{total_lotes}: erro ao deletar — {str(del_error)[:300]}')

            lote_inseridos = len(resultado['inseridos'])
            lote_atualizados = len(resultado['atualizados'])
            lote_ja_existem = len(resultado['ja_existem'])
            lote_deletados = len(deletados_lote)
            lote_com_erro = len(resultado['com_erro'])

            registros_inseridos += lote_inseridos
            registros_atualizados += lote_atualizados
            registros_ja_existem += lote_ja_existem
            registros_deletados += lote_deletados
            registros_com_erro += lote_com_erro

            registros_deste_lote = []
            for chave in resultado['inseridos']:
                registros_deste_lote.append((chave, 'inserido', None))
            for chave in resultado['atualizados']:
                registros_deste_lote.append((chave, 'atualizado', None))
            for chave in resultado['ja_existem']:
                registros_deste_lote.append((chave, 'ja_existe', None))
            for chave in deletados_lote:
                registros_deste_lote.append((chave, 'deletado', None))
            for item in resultado['com_erro']:
                registros_deste_lote.append((item['chave'], 'erro', item['erro']))

            # Grava o detalhe deste lote já (no máximo `tamanho_lote` linhas
            # de cada vez) em vez de acumular tudo em memória pra escrever
            # numa instrução só no final — essencial pra tabelas grandes.
            gravar_registros_execucao(conn, execucao_id, registros_deste_lote)

            _registrar_lote_execucao_no_job(job_id, lote_idx + 1, lote_inseridos, lote_atualizados, lote_ja_existem, lote_deletados, lote_com_erro)

            partes_lote = []
            if lote_inseridos: partes_lote.append(f'{lote_inseridos} inserido(s)')
            if lote_atualizados: partes_lote.append(f'{lote_atualizados} atualizado(s)')
            if lote_ja_existem: partes_lote.append(f'{lote_ja_existem} já existente(s)')
            if lote_deletados: partes_lote.append(f'{lote_deletados} deletado(s)')
            if lote_com_erro: partes_lote.append(f'{lote_com_erro} com erro')
            _log_execucao(
                job_id, 'erro' if lote_com_erro else 'info',
                f'Lote {lote_idx + 1}/{total_lotes}: ' + (', '.join(partes_lote) if partes_lote else 'nada a fazer') + '.'
            )

            set_background_job(job_id, {
                **get_background_job(job_id),
                'lote_atual': lote_idx + 1,
                'total_lotes': total_lotes,
                'registros_inseridos': registros_inseridos,
                'registros_atualizados': registros_atualizados,
                'registros_ja_existem': registros_ja_existem,
                'registros_deletados': registros_deletados,
                'registros_com_erro': registros_com_erro,
                'mensagem': f'Lote {lote_idx + 1}/{total_lotes}...',
            })

        # 6. Staging é recriada do zero a cada execução — remove ao final
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS {staging}')
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass

        # 7. Novo MAX(campo_id) do destino
        max_after = query(conn, f"SELECT MAX({quoted_key}) AS ultimo_id FROM {quoted_target}")
        novo_ultimo_id = int(max_after[0]['ultimo_id']) if max_after and max_after[0]['ultimo_id'] is not None else 0

        # 8. Fecha o resumo da execução (o registro já existe desde o início,
        #    e o detalhe por chave já foi gravado lote a lote acima).
        status_execucao = 'erro' if (
            registros_com_erro > 0
            and registros_inseridos == 0 and registros_atualizados == 0
            and registros_ja_existem == 0 and registros_deletados == 0
        ) else 'concluido'
        partes_msg = []
        if registros_inseridos: partes_msg.append(f'{registros_inseridos} inserido(s)')
        if registros_atualizados: partes_msg.append(f'{registros_atualizados} atualizado(s)')
        if registros_ja_existem: partes_msg.append(f'{registros_ja_existem} já existente(s)')
        if registros_deletados: partes_msg.append(f'{registros_deletados} deletado(s)')
        if registros_com_erro: partes_msg.append(f'{registros_com_erro} com erro')
        mensagem_execucao = ', '.join(partes_msg) if partes_msg else 'nenhuma alteração'

        atualizar_execucao_agendamento(
            conn, execucao_id, status_execucao, mensagem_execucao,
            registros_inseridos, registros_atualizados, registros_ja_existem, registros_deletados,
            registros_com_erro, novo_ultimo_id
        )

    except ValueError as error:
        set_background_job(job_id, {**get_background_job(job_id), 'status': 'erro', 'mensagem': str(error)})
        _log_execucao(job_id, 'erro', f'Execução interrompida: {error}')
        _fechar_execucao_com_erro(conn, execucao_id, str(error), locals())
        return
    except Exception as error:
        mensagem = mensagem_de_erro(error, 'Erro fatal na execução')
        set_background_job(job_id, {**get_background_job(job_id), 'status': 'erro', 'mensagem': mensagem})
        _log_execucao(job_id, 'erro', mensagem)
        _fechar_execucao_com_erro(conn, execucao_id, mensagem, locals())
        return
    finally:
        fechar_conexao_origem_da_thread()
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    try:
        atualizar_ultima_execucao_agendamento(tabela_id, novo_ultimo_id)
    except Exception:
        pass

    _log_execucao(job_id, 'sucesso', f'Execução concluída: {mensagem_execucao}.')
    set_background_job(job_id, {
        **get_background_job(job_id),
        'status': 'concluido',
        'mensagem': mensagem_execucao,
        'execucao_id': execucao_id,
        'ultimo_id_destino': novo_ultimo_id,
        'tamanho_lote': tamanho_lote,
        'total_lotes': total_lotes,
        'lote_atual': total_lotes,
        'registros_inseridos': registros_inseridos,
        'registros_atualizados': registros_atualizados,
        'registros_ja_existem': registros_ja_existem,
        'registros_deletados': registros_deletados,
        'registros_com_erro': registros_com_erro,
        'fim': datetime.now().isoformat(),
    })


def disparar_ciclo_auto_import(tabela_id):
    """Dispara um ciclo em segundo plano. Usado pelo agendador do servidor e
    pelo disparo manual. Retorna (http_status, mensagem)."""
    tabela_cfg = get_auto_import_tabela(tabela_id)
    if not tabela_cfg:
        return 404, 'Tabela não encontrada.'
    if tabela_cfg['status'] != 'ativa':
        return 409, 'Esta tabela está parada.'

    job_id = _generate_job_id('auto_ciclo', tabela_id)
    disparou = iniciar_background_job(job_id, {
        'job_id': job_id,
        'tipo': 'auto_ciclo',
        'tabela_id': tabela_id,
        'status': 'iniciando',
        'mensagem': 'Preparando ciclo...',
        'total_lotes': 0,
        'lote_atual': 0,
        'registros_inseridos': 0,
        'registros_atualizados': 0,
        'registros_ja_existem': 0,
        'registros_com_erro': 0,
        'log': [],
        'lotes': [],
    }, _background_auto_import_ciclo_worker, (job_id, tabela_id))
    if not disparou:
        return 409, 'Já existe um ciclo em andamento para esta tabela.'
    return 200, job_id


@app.route('/api/importacao-automatica/<int:tabela_id>/ciclo', methods=['POST'])
def api_importacao_automatica_executar_ciclo(tabela_id):
    """Dispara um ciclo manualmente e retorna na hora — o progresso é
    acompanhado via GET .../ciclo/status."""
    if not is_db_configured():
        return jsonify({
            'status': 'erro',
            'mensagem': 'Configure a conexão com o banco de dados para continuar.',
            'configurar_banco': url_for('configurar_banco')
        }), 503

    http_status, resultado = disparar_ciclo_auto_import(tabela_id)
    if http_status != 200:
        return jsonify({'status': 'erro', 'mensagem': resultado}), http_status
    return jsonify({'status': 'sucesso', 'job_id': resultado})


@app.route('/api/importacao-automatica/<int:tabela_id>/ciclo/status')
def api_importacao_automatica_ciclo_status(tabela_id):
    """Status do ciclo em segundo plano desta tabela (polling do progresso)."""
    job_id = _generate_job_id('auto_ciclo', tabela_id)
    job = get_background_job(job_id)
    if not job:
        return jsonify({'status': 'inativo'})
    return jsonify(job)


@app.route('/agendar-importacao', methods=['GET', 'POST'])
def agendar_importacao():
    """Lista as tabelas configuradas para importação completa agendada
    (insert/update/delete, até 4 horários por dia) e permite cadastrar/editar."""
    if not is_db_configured():
        flash('Configure o banco antes de acessar o Agendar Importação.', 'warning')
        return redirect(url_for('configurar_banco'))

    if request.method == 'POST':
        tabela_id_raw = (request.form.get('tabela_id') or '').strip()
        tabela_origem_raw = request.form.get('tabela_origem')
        tabela_destino_raw = request.form.get('tabela_destino')
        campo_id = (request.form.get('campo_id') or '').strip()
        horarios = [
            (request.form.get('horario_1') or '').strip() or None,
            (request.form.get('horario_2') or '').strip() or None,
            (request.form.get('horario_3') or '').strip() or None,
            (request.form.get('horario_4') or '').strip() or None,
        ]

        try:
            tamanho_lote = int(request.form.get('tamanho_lote') or DEFAULT_TAMANHO_LOTE)
        except (TypeError, ValueError):
            flash('Tamanho do lote deve ser um número inteiro.', 'error')
            return redirect(url_for('agendar_importacao'))

        if tamanho_lote <= 0:
            flash('O tamanho do lote deve ser maior que zero.', 'error')
            return redirect(url_for('agendar_importacao'))
        tamanho_lote = min(tamanho_lote, MAX_TAMANHO_LOTE)

        try:
            id_inicio = int((request.form.get('id_inicio') or '').strip() or 1)
        except ValueError:
            flash('"A partir do ID" deve ser um número inteiro.', 'error')
            return redirect(url_for('agendar_importacao'))
        if id_inicio < 1:
            flash('"A partir do ID" deve ser 1 ou maior.', 'error')
            return redirect(url_for('agendar_importacao'))

        if not any(horarios):
            flash('Informe pelo menos um horário de execução.', 'error')
            return redirect(url_for('agendar_importacao'))

        try:
            tabela_origem = normalizar_origem(tabela_origem_raw)
            tabela_destino = normalize_relation_input(tabela_destino_raw, 'Tabela Destino', get_db_config()['schema_destino'])
            if not campo_id:
                raise ValueError('Informe o campo chave (campo_id).')
            validar_origem(tabela_origem)

            tabela_criada_agora = garantir_tabela_destino(tabela_origem, tabela_destino, campo_id)

            if tabela_id_raw.isdigit():
                tabela_id = int(tabela_id_raw)
                if not get_agendamento_tabela(tabela_id):
                    raise ValueError('Tabela não encontrada para edição.')
                atualizar_agendamento_tabela(tabela_id, tabela_origem, tabela_destino, campo_id, tamanho_lote, horarios,
                                             id_inicio)
                flash(f'Tabela "{tabela_destino}" atualizada.', 'success')
            else:
                criar_agendamento_tabela(tabela_origem, tabela_destino, campo_id, tamanho_lote, horarios, id_inicio)
                flash(f'Tabela "{tabela_destino}" adicionada ao Agendar Importação.', 'success')

            if tabela_criada_agora:
                flash(f'Tabela de destino "{tabela_destino}" criada automaticamente (mesma estrutura da origem).', 'info')
        except (ValueError, psycopg2.Error) as error:
            flash(mensagem_de_erro(error), 'error')

        return redirect(url_for('agendar_importacao'))

    tabela_edicao = None
    editar_id = request.args.get('editar')
    if editar_id and editar_id.isdigit():
        tabela_edicao = get_agendamento_tabela(int(editar_id))
        if not tabela_edicao:
            flash('Tabela não encontrada para edição.', 'error')

    try:
        tabelas = get_agendamento_tabelas()
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error)
    except Exception as e:
        flash(f'Erro ao carregar tabelas configuradas: {str(e)}', 'error')
        tabelas = []

    try:
        migracoes_disponiveis = listar_migracoes_para_selecao()
    except Exception:
        migracoes_disponiveis = []

    return render_template(
        'agendar_importacao.html',
        examples=get_mapeamentos_fdw(),
        tabelas=tabelas,
        tabela_edicao=tabela_edicao,
        migracoes_disponiveis=migracoes_disponiveis
    )


@app.route('/agendar-importacao/<int:tabela_id>/iniciar', methods=['POST'])
def agendamento_iniciar(tabela_id):
    """Liga os horários desta tabela (o agendador do servidor dispara nos
    horários configurados, sem depender de tela aberta)."""
    try:
        if not get_agendamento_tabela(tabela_id):
            return jsonify({'status': 'erro', 'mensagem': 'Tabela não encontrada.'}), 404
        atualizar_status_agendamento_tabela(tabela_id, 'ativa')
        acordar_agendador()
        return jsonify({'status': 'sucesso'})
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error, api=True)
    except Exception as e:
        return jsonify({'status': 'erro', 'mensagem': str(e)}), 500


@app.route('/agendar-importacao/<int:tabela_id>/parar', methods=['POST'])
def agendamento_parar(tabela_id):
    """Desliga os horários e sinaliza cancelamento de uma execução em andamento (se houver)."""
    try:
        if not get_agendamento_tabela(tabela_id):
            return jsonify({'status': 'erro', 'mensagem': 'Tabela não encontrada.'}), 404
        atualizar_status_agendamento_tabela(tabela_id, 'parada')

        job_id = _generate_job_id('agendamento', tabela_id)
        job = get_background_job(job_id)
        if job and job.get('status') in ('iniciando', 'processando'):
            set_background_job(job_id, {**job, 'cancelar': True})

        return jsonify({'status': 'sucesso'})
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error, api=True)
    except Exception as e:
        return jsonify({'status': 'erro', 'mensagem': str(e)}), 500


@app.route('/agendar-importacao/<int:tabela_id>/excluir', methods=['POST'])
def agendamento_excluir(tabela_id):
    """Remove a tabela do Agendar Importação (e todo o histórico de execuções dela)."""
    try:
        if not get_agendamento_tabela(tabela_id):
            flash('Tabela não encontrada.', 'error')
            return redirect(url_for('agendar_importacao'))
        excluir_agendamento_tabela(tabela_id)
        flash('Tabela removida do Agendar Importação.', 'success')
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error)
    except Exception as e:
        flash(f'Erro ao excluir: {str(e)}', 'error')
    return redirect(url_for('agendar_importacao'))


@app.route('/agendar-importacao/<int:tabela_id>/execucoes')
def agendamento_execucoes(tabela_id):
    """Tela 'Verificar' — histórico de execuções já rodadas para uma tabela."""
    tabela = get_agendamento_tabela(tabela_id)
    if not tabela:
        flash('Tabela não encontrada.', 'error')
        return redirect(url_for('agendar_importacao'))

    try:
        execucoes = execute_query(
            "SELECT * FROM agendamento_importacao_execucoes WHERE tabela_id = %s ORDER BY executado_em DESC",
            (tabela_id,)
        )
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error)

    return render_template('agendar_importacao_execucoes.html', tabela=tabela, execucoes=execucoes)


@app.route('/agendar-importacao/execucao/<int:execucao_id>/registros')
def agendamento_execucao_registros(execucao_id):
    """Detalhe de uma execução — grid de registros processados, igual à migração."""
    execucao_rows = execute_query(
        "SELECT * FROM agendamento_importacao_execucoes WHERE id = %s", (execucao_id,)
    )
    if not execucao_rows:
        flash('Execução não encontrada.', 'error')
        return redirect(url_for('agendar_importacao'))
    execucao = execucao_rows[0]

    tabela = get_agendamento_tabela(execucao['tabela_id'])

    try:
        registros = execute_query(
            "SELECT chave_valor, status, erro_msg FROM agendamento_importacao_execucao_registros "
            "WHERE execucao_id = %s ORDER BY id",
            (execucao_id,)
        )
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error)

    return render_template(
        'agendar_importacao_execucao_registros.html',
        execucao=execucao, tabela=tabela, registros=registros
    )


def disparar_execucao_agendamento(tabela_id):
    """Dispara uma execução completa em segundo plano. Usado pelo agendador do
    servidor e pelo botão "Executar agora". Retorna (http_status, mensagem)."""
    if not get_agendamento_tabela(tabela_id):
        return 404, 'Tabela não encontrada.'

    job_id = _generate_job_id('agendamento', tabela_id)
    disparou = iniciar_background_job(job_id, {
        'job_id': job_id,
        'tipo': 'agendamento',
        'tabela_id': tabela_id,
        'status': 'iniciando',
        'mensagem': 'Preparando execução...',
        'total_lotes': 0,
        'lote_atual': 0,
        'registros_inseridos': 0,
        'registros_atualizados': 0,
        'registros_ja_existem': 0,
        'registros_deletados': 0,
        'registros_com_erro': 0,
        'log': [],
        'lotes': [],
    }, _background_agendamento_worker, (job_id, tabela_id))
    if not disparou:
        return 409, 'Já existe uma execução em andamento para esta tabela.'
    return 200, job_id


@app.route('/api/agendar-importacao/<int:tabela_id>/executar', methods=['POST'])
def api_agendamento_executar(tabela_id):
    """Dispara uma execução manual e retorna na hora — o progresso é
    acompanhado via GET .../executar/status."""
    if not is_db_configured():
        return jsonify({
            'status': 'erro',
            'mensagem': 'Configure a conexão com o banco de dados para continuar.',
            'configurar_banco': url_for('configurar_banco')
        }), 503

    http_status, resultado = disparar_execucao_agendamento(tabela_id)
    if http_status != 200:
        return jsonify({'status': 'erro', 'mensagem': resultado}), http_status
    return jsonify({'status': 'sucesso', 'job_id': resultado})


@app.route('/api/agendar-importacao/<int:tabela_id>/executar/status')
def api_agendamento_executar_status(tabela_id):
    """Status da execução em segundo plano desta tabela (polling do progresso)."""
    job_id = _generate_job_id('agendamento', tabela_id)
    job = get_background_job(job_id)
    if not job:
        return jsonify({'status': 'inativo'})
    return jsonify(job)


@app.route('/api/progresso/<int:id>')
def api_progresso(id):
    """API para verificar progresso em tempo real"""
    if not is_db_configured():
        return jsonify({
            'status': 'erro',
            'mensagem': 'Configure a conexão com o banco de dados para continuar.',
            'configurar_banco': url_for('configurar_banco')
        }), 503

    try:
        result = execute_query(
            "SELECT * FROM verificar_progresso(%s)",
            (id,)
        )[0]
        return jsonify(result)
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error, api=True)
    except Exception as e:
        return jsonify({'status': 'erro', 'mensagem': str(e)}), 500


@app.route('/api/dashboard/stats')
def api_dashboard_stats():
    """Retorna estatísticas atualizadas das migrações para polling do dashboard."""
    try:
        todas_auto_tabelas = get_auto_import_tabelas()
    except Exception:
        todas_auto_tabelas = []
    resumo_auto = {
        'configuradas': len(todas_auto_tabelas),
        'ativas': sum(1 for t in todas_auto_tabelas if t.get('status') == 'ativa'),
    }

    try:
        todas_agendamento_tabelas = get_agendamento_tabelas()
    except Exception:
        todas_agendamento_tabelas = []
    resumo_agendamento = {
        'configuradas': len(todas_agendamento_tabelas),
        'ativas': sum(1 for t in todas_agendamento_tabelas if t.get('status') == 'ativa'),
    }

    manual_em_execucao = contar_migracoes_manuais_em_execucao()

    if not is_db_configured():
        return jsonify({
            'migracoes': [], 'totais': {},
            'resumo_auto': resumo_auto, 'resumo_agendamento': resumo_agendamento,
            'manual_em_execucao': manual_em_execucao
        })
    try:
        def _fetch_stats(conn):
            query = run_query_with_connection
            migracoes = query(conn, """
                SELECT id, tabela_origem, tabela_destino, campo_id,
                       total_registros, registros_processados, id_maximo, status
                FROM controle_migracao ORDER BY created_at DESC
            """)

            for m in migracoes:
                # Último ID no destino
                try:
                    r = query(conn,
                        f"SELECT MAX({m['campo_id']}) as ultimo_id FROM {m['tabela_destino']}")
                    m['ultimo_id_destino'] = r[0].get('ultimo_id') or 0 if r else 0
                except Exception:
                    m['ultimo_id_destino'] = 0

                # Último ID na origem (view) para progresso real
                try:
                    r = query(conn,
                        f"SELECT MAX({m['campo_id']}) as ultimo_id FROM {m['tabela_origem']}")
                    m['ultimo_id_origem'] = r[0].get('ultimo_id') or 0 if r else 0
                except Exception:
                    m['ultimo_id_origem'] = m.get('id_maximo') or 0

            return migracoes

        migracoes = execute_db_operation(_fetch_stats, statement_timeout_ms=120000)

        totais = {
            'total_registros': sum(m.get('total_registros', 0) or 0 for m in migracoes),
            'registros_processados': sum(m.get('registros_processados', 0) or 0 for m in migracoes),
            'em_processamento': sum(1 for m in migracoes if m.get('status') == 'processando'),
            'concluidas': sum(1 for m in migracoes if m.get('status') == 'concluido'),
            'com_erro': sum(1 for m in migracoes if m.get('status') == 'erro'),
            'pendentes': sum(1 for m in migracoes if m.get('status') in ('pendente', 'preparando')),
        }
        return jsonify({
            'migracoes': migracoes, 'totais': totais,
            'resumo_auto': resumo_auto, 'resumo_agendamento': resumo_agendamento,
            'manual_em_execucao': manual_em_execucao
        })
    except Exception as e:
        return jsonify({
            'migracoes': [], 'totais': {}, 'erro': str(e),
            'resumo_auto': resumo_auto, 'resumo_agendamento': resumo_agendamento,
            'manual_em_execucao': manual_em_execucao
        })

@app.route('/api/background/iniciar', methods=['POST'])
def api_background_iniciar():
    """Inicia o processamento de registros por faixa de IDs em segundo plano.

    A faixa é dividida em lotes (tamanho_lote). Cada lote é copiado para uma
    tabela em `temp` (staging) e só então processado em bloco contra o
    schema public. Ver _background_migracao_worker / processar_lote_bulk.
    """
    if not is_db_configured():
        return jsonify({
            'status': 'erro',
            'mensagem': 'Configure a conexão com o banco de dados para continuar.',
            'configurar_banco': url_for('configurar_banco')
        }), 503

    data = request.json or {}
    migracao_id = data.get('migracao_id')
    id_inicio = data.get('id_inicio')
    id_fim = data.get('id_fim')
    delay_segundos = data.get('delay_segundos', 1)
    apenas_erros = data.get('apenas_erros', False)

    # Tamanho do lote enviado pelo front; valida e trava dentro de um limite seguro
    try:
        tamanho_lote = int(data.get('tamanho_lote') or DEFAULT_TAMANHO_LOTE)
    except (TypeError, ValueError):
        return jsonify({'status': 'erro', 'mensagem': 'Tamanho do lote deve ser um número inteiro.'}), 400

    if tamanho_lote <= 0:
        return jsonify({'status': 'erro', 'mensagem': 'O tamanho do lote deve ser maior que zero.'}), 400
    tamanho_lote = min(tamanho_lote, MAX_TAMANHO_LOTE)  # evita staging gigante

    if not migracao_id:
        return jsonify({'status': 'erro', 'mensagem': 'Informe o ID da migração.'}), 400

    if not apenas_erros:
        if not id_inicio or not id_fim:
            return jsonify({'status': 'erro', 'mensagem': 'Informe o ID inicial e o ID final.'}), 400

        try:
            id_inicio = int(id_inicio)
            id_fim = int(id_fim)
        except (TypeError, ValueError):
            return jsonify({'status': 'erro', 'mensagem': 'IDs devem ser números inteiros.'}), 400

        if id_inicio < 1:
            return jsonify({'status': 'erro', 'mensagem': 'O ID inicial deve ser maior que zero.'}), 400

        if id_fim < id_inicio:
            return jsonify({'status': 'erro', 'mensagem': 'O ID final deve ser maior ou igual ao ID inicial.'}), 400

        total_faixa = id_fim - id_inicio + 1

        # Validar contra id_maximo
        try:
            migracao = execute_query(
                "SELECT id_maximo FROM controle_migracao WHERE id = %s",
                (migracao_id,)
            )[0]
            id_maximo = migracao.get('id_maximo', 0) or 0
            if id_maximo > 0 and id_fim > id_maximo:
                return jsonify({'status': 'erro', 'mensagem': f'O ID final ({id_fim}) excede o ID máximo da origem ({id_maximo}).'}), 400
        except Exception:
            pass
    else:
        id_inicio = 0
        id_fim = 0

    job_id = _generate_job_id('migracao', migracao_id)
    existing = get_background_job(job_id)
    if existing and existing.get('status') == 'processando':
        return jsonify({
            'status': 'erro',
            'mensagem': 'Já existe um processamento em segundo plano para esta migração.',
            'job_id': job_id,
        }), 409

    set_background_job(job_id, {
        'job_id': job_id,
        'tipo': 'migracao',
        'migracao_id': migracao_id,
        'status': 'iniciando',
        'id_inicio': id_inicio,
        'id_fim': id_fim,
        'total_faixa': id_fim - id_inicio + 1 if not apenas_erros else 0,
        'ids_verificados': 0,
        'delay_segundos': delay_segundos,
        'tamanho_lote': tamanho_lote,
        'registros_inseridos': 0,
        'registros_ja_existem': 0,
        'registros_atualizados': 0,
        'registros_nao_encontrados': 0,
        'registros_com_erro': [],
        'registro_atual': None,
        'ultimo_progresso': None,
        'cancelar': False,
        'apenas_erros': apenas_erros,
        # Mensagem inicial já cita o processamento em lote
        'mensagem': f'Preparando processamento em lotes de {tamanho_lote}...',
    })

    thread = threading.Thread(
        target=_background_migracao_worker,
        args=(job_id, migracao_id, id_inicio, id_fim, delay_segundos, apenas_erros, tamanho_lote),
        daemon=True
    )
    thread.start()

    return jsonify({
        'status': 'sucesso',
        'mensagem': 'Processamento em segundo plano iniciado.',
        'job_id': job_id,
    })


@app.route('/api/background/parar', methods=['POST'])
def api_background_parar():
    """Solicita a parada do processamento em segundo plano."""
    data = request.json or {}
    migracao_id = data.get('migracao_id')

    if not migracao_id:
        return jsonify({'status': 'erro', 'mensagem': 'Informe o ID da migração.'}), 400

    job_id = _generate_job_id('migracao', migracao_id)
    job = get_background_job(job_id)

    if not job:
        return jsonify({'status': 'erro', 'mensagem': 'Nenhum processamento em segundo plano encontrado.'}), 404

    if job.get('status') not in ('processando', 'iniciando'):
        return jsonify({
            'status': 'info',
            'mensagem': f'O processamento já está no estado: {job.get("status")}.',
            'job': job,
        })

    set_background_job(job_id, {**job, 'cancelar': True})

    return jsonify({
        'status': 'sucesso',
        'mensagem': 'Solicitação de parada enviada. O lote atual será concluído antes de parar.',
    })


@app.route('/api/background/status')
def api_background_status():
    """Retorna o estado do processamento em segundo plano."""
    migracao_id = request.args.get('migracao_id', type=int)
    job_type = request.args.get('tipo', 'migracao')

    if not migracao_id:
        return jsonify({'status': 'erro', 'mensagem': 'Informe o ID da migração.'}), 400

    job_id = _generate_job_id(job_type, migracao_id)
    job = get_background_job(job_id)

    if not job:
        return jsonify({'status': 'inativo', 'mensagem': 'Nenhum processamento em segundo plano encontrado.'})

    return jsonify(job)


@app.route('/api/background/todos')
def api_background_todos():
    """Retorna os jobs de segundo plano, separados em migrações, importação
    automática e agendar importação — cada um já com o nome da tabela sendo processada."""
    with background_jobs_lock:
        jobs = [dict(job) for job in background_jobs.values()]

    jobs_migracao = [j for j in jobs if j.get('tipo') in ('migracao', 'refresh')]
    jobs_auto = [j for j in jobs if j.get('tipo') == 'auto_ciclo']
    jobs_agendamento = [j for j in jobs if j.get('tipo') == 'agendamento']

    # Nomes das tabelas: uma consulta por tipo, numa conexão só — este
    # endpoint é chamado a cada 2s por dashboard aberto, então não pode abrir
    # uma conexão por job.
    grupos = (
        (jobs_migracao, 'migracao_id', 'controle_migracao'),
        (jobs_auto, 'tabela_id', 'auto_importacao_tabelas'),
        (jobs_agendamento, 'tabela_id', 'agendamento_importacao_tabelas'),
    )

    def _nomes(conn):
        nomes = {}
        for lista, campo_id, tabela in grupos:
            ids = sorted({j.get(campo_id) for j in lista if j.get(campo_id)})
            if not ids:
                continue
            rows = run_query_with_connection(
                conn, f"SELECT id, tabela_origem, tabela_destino FROM {tabela} WHERE id = ANY(%s)", (ids,)
            )
            nomes[tabela] = {r['id']: r for r in rows}
        return nomes

    try:
        nomes = execute_db_operation(_nomes, statement_timeout_ms=10000) if jobs and is_db_configured() else {}
    except Exception:
        nomes = {}

    for lista, campo_id, tabela in grupos:
        for job in lista:
            row = nomes.get(tabela, {}).get(job.get(campo_id))
            if row:
                job['tabela_origem'] = row['tabela_origem']
                job['tabela_destino'] = row['tabela_destino']

    return jsonify({
        'jobs_migracao': jobs_migracao,
        'jobs_auto_importacao': jobs_auto,
        'jobs_agendamento': jobs_agendamento,
    })


@app.route('/api/background/limpar', methods=['POST'])
def api_background_limpar():
    """Remove o registro de um job já finalizado."""
    data = request.json or {}
    migracao_id = data.get('migracao_id')
    job_type = data.get('tipo', 'migracao')

    if not migracao_id:
        return jsonify({'status': 'erro', 'mensagem': 'Informe o ID da migração.'}), 400

    job_id = _generate_job_id(job_type, migracao_id)
    job = get_background_job(job_id)

    if job and job.get('status') == 'processando':
        return jsonify({'status': 'erro', 'mensagem': 'Não é possível limpar um job em andamento.'}), 409

    remove_background_job(job_id)
    return jsonify({'status': 'sucesso', 'mensagem': 'Job removido.'})


@app.route('/api/cancelar-migracao/<int:id>', methods=['POST'])
def api_cancelar_migracao(id):
    """Cancela uma migração em andamento"""
    if not is_db_configured():
        return jsonify({
            'status': 'erro',
            'mensagem': 'Configure a conexão com o banco de dados para continuar.',
            'configurar_banco': url_for('configurar_banco')
        }), 503

    try:
        execute_query(
            "UPDATE controle_migracao SET status = 'cancelado' WHERE id = %s",
            (id,),
            fetch=False
        )
        return jsonify({'status': 'sucesso', 'mensagem': 'Migração cancelada'})
    except (OperationalError, InterfaceError) as error:
        return handle_db_connection_error(error, api=True)
    except Exception as e:
        return jsonify({'status': 'erro', 'mensagem': str(e)}), 500

# ============================================================
# AGENDADOR NO SERVIDOR (importação automática + agendar importação)
# ============================================================
# Antes quem disparava os ciclos/execuções era o JavaScript das telas
# (setInterval no navegador): sem aba aberta e usuário logado nada rodava, e
# cada aba/dashboard aberto disparava de novo. Agora uma única thread no
# servidor confere a cada AGENDADOR_INTERVALO_SEG o que está vencido e
# dispara em segundo plano; as telas só acompanham o progresso.

AGENDADOR_INTERVALO_SEG = 15
# Máximo de ciclos/execuções agendados rodando ao mesmo tempo — o resto
# espera o próximo tick (disparos manuais não entram nessa conta)
IMPORTACAO_MAX_SIMULTANEAS = int(os.getenv('IMPORTACAO_MAX_SIMULTANEAS', '5'))
# Até quanto tempo depois do horário configurado uma execução agendada ainda
# é disparada (ex.: app reiniciado às 03:20 ainda roda a das 03:00)
AGENDAMENTO_TOLERANCIA_MIN = int(os.getenv('AGENDAMENTO_TOLERANCIA_MIN', '60'))

_agendador_acordar = threading.Event()
_agendador_iniciado = False
_agendador_lock = threading.Lock()
# Memória do agendador (só deste processo): evita redisparar enquanto o log
# do ciclo/execução ainda não foi gravado no banco
_ultimo_disparo_auto = {}           # tabela_id -> time.monotonic()
_disparos_agendamento_feitos = set()  # (tabela_id, datetime do horário-alvo)


def acordar_agendador():
    """Faz o agendador conferir agora em vez de esperar o próximo tick."""
    _agendador_acordar.set()


def _auto_import_vencidas():
    """Ids das tabelas ativas cujo intervalo já passou desde o fim do último
    ciclo, das mais atrasadas pras menos. Tempo medido no relógio do banco."""
    rows = execute_query("""
        SELECT t.id, t.intervalo_min,
               EXTRACT(EPOCH FROM LOCALTIMESTAMP - MAX(c.executado_em)) AS seg_desde_ultimo
        FROM auto_importacao_tabelas t
        LEFT JOIN auto_importacao_ciclos c ON c.tabela_id = t.id
        WHERE t.status = 'ativa'
        GROUP BY t.id, t.intervalo_min
    """, statement_timeout_ms=30000)
    agora = time.monotonic()
    vencidas = []
    for r in rows:
        intervalo_seg = max(int(r['intervalo_min'] or 5), 1) * 60
        seg = r['seg_desde_ultimo']
        if seg is not None and float(seg) < intervalo_seg:
            continue
        ultimo_disparo = _ultimo_disparo_auto.get(r['id'])
        if ultimo_disparo is not None and agora - ultimo_disparo < intervalo_seg:
            continue
        vencidas.append((float('inf') if seg is None else float(seg), r['id']))
    return [tabela_id for _, tabela_id in sorted(vencidas, reverse=True)]


def _agendamentos_vencidos():
    """[(tabela_id, horário-alvo)] das tabelas ativas com um horário que já
    passou (dentro da tolerância) e ainda não rodou desde então."""
    rows = execute_query("""
        SELECT t.id, t.horario_1, t.horario_2, t.horario_3, t.horario_4,
               MAX(e.executado_em) AS ultima_execucao_em, LOCALTIMESTAMP AS agora
        FROM agendamento_importacao_tabelas t
        LEFT JOIN agendamento_importacao_execucoes e ON e.tabela_id = t.id
        WHERE t.status = 'ativa'
        GROUP BY t.id
    """, statement_timeout_ms=30000)
    tolerancia = timedelta(minutes=AGENDAMENTO_TOLERANCIA_MIN)
    vencidos = []
    for r in rows:
        agora = r['agora']
        ultima = r['ultima_execucao_em']
        horarios = [r[f'horario_{i}'] for i in range(1, 5) if r[f'horario_{i}'] is not None]
        # Ontem também conta: horário 23:50 ainda vale às 00:10
        alvos = [
            datetime.combine(agora.date() - timedelta(days=dias), h)
            for h in horarios for dias in (0, 1)
        ]
        alvos = [a for a in alvos if a <= agora < a + tolerancia and (ultima is None or ultima < a)]
        alvos = [a for a in alvos if (r['id'], a) not in _disparos_agendamento_feitos]
        if alvos:
            vencidos.append((r['id'], max(alvos)))
    return vencidos


def _agendador_tick():
    liberar_jobs_orfaos()

    for tabela_id in _auto_import_vencidas():
        if contar_jobs_importacao_ativos() >= IMPORTACAO_MAX_SIMULTANEAS:
            return
        http_status, resultado = disparar_ciclo_auto_import(tabela_id)
        if http_status == 200:
            _ultimo_disparo_auto[tabela_id] = time.monotonic()

    for tabela_id, alvo in _agendamentos_vencidos():
        if contar_jobs_importacao_ativos() >= IMPORTACAO_MAX_SIMULTANEAS:
            return
        http_status, resultado = disparar_execucao_agendamento(tabela_id)
        if http_status == 200:
            _disparos_agendamento_feitos.add((tabela_id, alvo))
        elif http_status == 409:
            # Já tem uma execução dessa tabela rodando — ela cobre este horário
            _disparos_agendamento_feitos.add((tabela_id, alvo))
        else:
            app.logger.warning('Agendamento %s não disparado: %s', tabela_id, resultado)

    # Esquece disparos antigos (já fora de qualquer janela)
    limite = datetime.now() - timedelta(days=2)
    _disparos_agendamento_feitos.difference_update(
        {d for d in _disparos_agendamento_feitos if d[1] < limite}
    )


def _agendador_loop():
    while True:
        try:
            if is_db_configured():
                _agendador_tick()
        except Exception:
            app.logger.exception('Erro no agendador de importações')
        _agendador_acordar.wait(AGENDADOR_INTERVALO_SEG)
        _agendador_acordar.clear()


def iniciar_agendador_importacoes():
    """Sobe a thread do agendador (uma vez por processo)."""
    global _agendador_iniciado
    with _agendador_lock:
        if _agendador_iniciado:
            return
        _agendador_iniciado = True
    threading.Thread(target=_agendador_loop, name='agendador-importacoes', daemon=True).start()


# ============================================================
# EXECUÇÃO
# ============================================================

if __name__ == '__main__':
    # Com debug=True o reloader do Werkzeug roda este script em dois
    # processos (vigia + servidor). Só o servidor (WERKZEUG_RUN_MAIN) sobe as
    # rotinas de fundo — senão cada ciclo seria disparado duas vezes. Ficam
    # aqui (e não no import do módulo) pra que importar o app — em testes ou
    # scripts — nunca dispare DDL nem importações no banco configurado.
    debug = True
    if not debug or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        iniciar_migracao_tabelas_detalhe()
        iniciar_agendador_importacoes()
    app.run(debug=debug, host='0.0.0.0', port=5310)