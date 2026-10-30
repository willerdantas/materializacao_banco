-- ============================================================
-- FUNÇÕES DE CONTROLE DA MIGRAÇÃO (PostgreSQL)
-- ============================================================
-- Instaladas AUTOMATICAMENTE pelo app (instalar_funcoes_migracao) no schema
-- de destino configurado em "Configurar Banco" (ex.: integra_local) — na
-- subida do app, ao salvar a conexão e no botão "Revisar e Corrigir".
-- Só tratam das tabelas de controle: a leitura da origem é feita pelo app,
-- direto no servidor de origem (não há mais funções que leiam a origem
-- dentro do banco, como as antigas preparar_colunas_migracao/processar_lote).
--
-- Para instalar à mão (substitua integra_local pelo schema configurado):
--   psql ... -c "SET search_path = integra_local" -f sql/funcoes_migracao.sql
-- ============================================================


CREATE OR REPLACE FUNCTION verificar_progresso(p_migracao_id INTEGER)
RETURNS TABLE (
    migracao_id INTEGER,
    tabela_origem VARCHAR,
    tabela_destino VARCHAR,
    campo_id_out VARCHAR,
    lote_atual INTEGER,
    total_registros INTEGER,
    registros_processados INTEGER,
    id_maximo BIGINT,
    percentual_concluido NUMERIC,
    status VARCHAR,
    tempo_estimado_minutos INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        cm.id,
        -- casts: o app cria essas colunas como TEXT; o script antigo, como VARCHAR
        cm.tabela_origem::VARCHAR,
        cm.tabela_destino::VARCHAR,
        cm.campo_id::VARCHAR,
        cm.lote_atual,
        cm.total_registros,
        cm.registros_processados,
        cm.id_maximo,
        ROUND((cm.registros_processados::NUMERIC / NULLIF(cm.total_registros, 0)) * 100, 2),
        cm.status::VARCHAR,
        CASE
            WHEN cm.registros_processados > 0 AND cm.data_inicio IS NOT NULL THEN
                CEIL(
                    (EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - cm.data_inicio)) / 60.0)
                    * (cm.total_registros - cm.registros_processados)
                    / NULLIF(cm.registros_processados, 0)
                )::INTEGER
            ELSE NULL
        END
    FROM controle_migracao cm
    WHERE cm.id = p_migracao_id;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION finalizar_migracao(p_migracao_id INTEGER)
RETURNS VOID AS $$
BEGIN
    UPDATE controle_migracao
    SET status = 'concluido',
        data_fim = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = p_migracao_id
      AND registros_processados >= total_registros;
END;
$$ LANGUAGE plpgsql;
