-- ============================================================
-- Corrige "nextval: reached maximum value of sequence
-- auto_importacao_ciclo_registros_id_seq (2147483647)".
--
-- As tabelas de detalhe guardavam uma linha por chave a cada ciclo, inclusive
-- as "ja_existe" (~99,9% do volume: 2,1 bilhões de linhas / 178 GB). O app
-- agora não grava mais "ja_existe" (o total continua no resumo do ciclo).
--
-- Este script recria cada tabela de detalhe com id BIGINT, copiando só as
-- linhas relevantes (inserido / atualizado / nao_encontrado / deletado / erro)
-- e descartando os "ja_existe" antigos. DROP da tabela velha devolve o espaço
-- em disco na hora (sem VACUUM FULL).
--
-- Rodar com o app de importação PARADO (ou ao menos sem ciclos em andamento):
--   psql -h localhost -U postgres -d isivitaedb_hm -f limpeza_registros_detalhe.sql
-- ============================================================

\set ON_ERROR_STOP on
\timing on

-- Tabelas de controle ficam no schema de destino configurado no app
SET search_path = integra_local, public;

-- ------------------------------------------------------------
-- 1. auto_importacao_ciclo_registros
-- ------------------------------------------------------------
BEGIN;
SET LOCAL statement_timeout = 0;
-- Força a cópia a usar o índice (ciclo_id, status) por ciclo em vez de ler os 178 GB
SET LOCAL enable_seqscan = off;
SET LOCAL enable_hashjoin = off;
SET LOCAL enable_mergejoin = off;

LOCK TABLE auto_importacao_ciclo_registros IN EXCLUSIVE MODE;

CREATE TABLE auto_importacao_ciclo_registros_novo (
    id BIGSERIAL PRIMARY KEY,
    ciclo_id INTEGER NOT NULL REFERENCES auto_importacao_ciclos(id) ON DELETE CASCADE,
    chave_valor TEXT NOT NULL,
    status TEXT NOT NULL,
    erro_msg TEXT
);

INSERT INTO auto_importacao_ciclo_registros_novo (ciclo_id, chave_valor, status, erro_msg)
SELECT r.ciclo_id, r.chave_valor, r.status, r.erro_msg
FROM auto_importacao_ciclos c
JOIN auto_importacao_ciclo_registros r
  ON r.ciclo_id = c.id
 AND r.status IN ('inserido', 'atualizado', 'nao_encontrado', 'erro')
ORDER BY r.ciclo_id, r.id;

DROP TABLE auto_importacao_ciclo_registros;
ALTER TABLE auto_importacao_ciclo_registros_novo RENAME TO auto_importacao_ciclo_registros;
ALTER SEQUENCE auto_importacao_ciclo_registros_novo_id_seq RENAME TO auto_importacao_ciclo_registros_id_seq;
ALTER INDEX auto_importacao_ciclo_registros_novo_pkey RENAME TO auto_importacao_ciclo_registros_pkey;
CREATE INDEX idx_auto_importacao_ciclo_registros_ciclo_status
    ON auto_importacao_ciclo_registros (ciclo_id, status);
COMMIT;

-- ------------------------------------------------------------
-- 2. agendamento_importacao_execucao_registros
--    (mesmo problema, em 575 mi de 2,1 bi — 48 GB)
-- ------------------------------------------------------------
BEGIN;
SET LOCAL statement_timeout = 0;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_hashjoin = off;
SET LOCAL enable_mergejoin = off;

LOCK TABLE agendamento_importacao_execucao_registros IN EXCLUSIVE MODE;

CREATE TABLE agendamento_importacao_execucao_registros_novo (
    id BIGSERIAL PRIMARY KEY,
    execucao_id INTEGER NOT NULL REFERENCES agendamento_importacao_execucoes(id) ON DELETE CASCADE,
    chave_valor TEXT NOT NULL,
    status TEXT NOT NULL,
    erro_msg TEXT
);

INSERT INTO agendamento_importacao_execucao_registros_novo (execucao_id, chave_valor, status, erro_msg)
SELECT r.execucao_id, r.chave_valor, r.status, r.erro_msg
FROM agendamento_importacao_execucoes e
JOIN agendamento_importacao_execucao_registros r
  ON r.execucao_id = e.id
 AND r.status IN ('inserido', 'atualizado', 'deletado', 'erro')
ORDER BY r.execucao_id, r.id;

DROP TABLE agendamento_importacao_execucao_registros;
ALTER TABLE agendamento_importacao_execucao_registros_novo RENAME TO agendamento_importacao_execucao_registros;
ALTER SEQUENCE agendamento_importacao_execucao_registros_novo_id_seq RENAME TO agendamento_importacao_execucao_registros_id_seq;
ALTER INDEX agendamento_importacao_execucao_registros_novo_pkey RENAME TO agendamento_importacao_execucao_registros_pkey;
CREATE INDEX idx_agendamento_importacao_execucao_registros_status
    ON agendamento_importacao_execucao_registros (execucao_id, status);
COMMIT;

-- Conferência
SELECT relname, pg_size_pretty(pg_total_relation_size(oid)) AS tamanho
FROM pg_class
WHERE relname IN ('auto_importacao_ciclo_registros', 'agendamento_importacao_execucao_registros');

SELECT sequencename, data_type, last_value, max_value
FROM pg_sequences
WHERE sequencename IN ('auto_importacao_ciclo_registros_id_seq', 'agendamento_importacao_execucao_registros_id_seq');
