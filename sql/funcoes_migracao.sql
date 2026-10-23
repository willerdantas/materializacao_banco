-- ============================================================
-- FUNÇÕES DA MIGRAÇÃO EM LOTES (PostgreSQL)
-- ============================================================
-- Instaladas AUTOMATICAMENTE pelo app (instalar_funcoes_migracao) no schema
-- de destino configurado em "Configurar Banco" (ex.: integra_local) — na
-- subida do app, ao salvar a conexão e no botão "Revisar e Corrigir".
-- Por isso nenhum nome aqui leva schema: o instalador roda com
-- search_path = <schema de destino> e depois fixa o search_path de cada
-- função nesse schema. As tabelas de controle (controle_migracao,
-- controle_migracao_lotes) são criadas pelo app (criar_tabelas_controle).
--
-- Para instalar à mão (substitua integra_local pelo schema configurado):
--   psql ... -c "SET search_path = integra_local" -f sql/funcoes_migracao.sql
-- ============================================================


CREATE OR REPLACE FUNCTION atualizar_lotes_migracao(p_migracao_id INTEGER)
RETURNS TABLE (
    total_registros INTEGER,
    total_lotes INTEGER,
    migracao_id INTEGER
) AS $$
DECLARE
    v_tabela_origem TEXT;
    v_tabela_destino TEXT;
    v_campo_id TEXT;
    v_colunas_insert TEXT;
    v_colunas_select TEXT;
    v_colunas_update TEXT;
    v_tamanho_lote INTEGER;
    v_total_registros INTEGER := 0;
    v_total_lotes INTEGER := 0;
    v_tabela_origem_regclass REGCLASS;
    v_tabela_destino_regclass REGCLASS;
    v_coluna_chave_destino TEXT;
    v_coluna_chave_origem_tipo OID;
    v_coluna_chave_destino_tipo OID;
    v_coluna_chave_destino_typmod INTEGER;
    v_key_join_expression TEXT;
    v_source_key_text_expression TEXT;
    v_colunas_update_where TEXT;
    v_sql TEXT;
BEGIN
    SELECT
        cm.tabela_origem,
        cm.tabela_destino,
        cm.campo_id,
        cm.tamanho_lote
    INTO
        v_tabela_origem,
        v_tabela_destino,
        v_campo_id,
        v_tamanho_lote
    FROM controle_migracao cm
    WHERE cm.id = p_migracao_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Migracao nao encontrada: %', p_migracao_id;
    END IF;

    IF v_tamanho_lote IS NULL OR v_tamanho_lote <= 0 THEN
        RAISE EXCEPTION 'O tamanho do lote deve ser maior que zero.';
    END IF;

    v_tabela_origem_regclass := to_regclass(v_tabela_origem);
    v_tabela_destino_regclass := to_regclass(v_tabela_destino);

    IF v_tabela_origem_regclass IS NULL THEN
        RAISE EXCEPTION 'Tabela de origem nao encontrada: %', v_tabela_origem;
    END IF;

    IF v_tabela_destino_regclass IS NULL THEN
        RAISE EXCEPTION 'Tabela de destino nao encontrada: %', v_tabela_destino;
    END IF;

    SELECT
        format('%I', a.attname),
        a.atttypid,
        a.atttypmod
    INTO v_coluna_chave_destino, v_coluna_chave_destino_tipo, v_coluna_chave_destino_typmod
    FROM pg_attribute a
    WHERE a.attrelid = v_tabela_destino_regclass
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND a.attname = v_campo_id;

    IF v_coluna_chave_destino IS NULL THEN
        RAISE EXCEPTION 'A coluna chave % precisa existir na tabela de destino.', v_campo_id;
    END IF;

    SELECT a.atttypid
    INTO v_coluna_chave_origem_tipo
    FROM pg_attribute a
    WHERE a.attrelid = v_tabela_origem_regclass
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND a.attname = v_campo_id;

    IF v_coluna_chave_origem_tipo IS NULL THEN
        RAISE EXCEPTION 'A coluna chave % precisa existir na tabela de origem.', v_campo_id;
    END IF;

    IF v_coluna_chave_origem_tipo = v_coluna_chave_destino_tipo THEN
        v_key_join_expression := format('s.%I', v_campo_id);
    ELSIF EXISTS (
        SELECT 1
        FROM pg_cast c
        WHERE c.castsource = v_coluna_chave_origem_tipo
          AND c.casttarget = v_coluna_chave_destino_tipo
          AND c.castcontext IN ('a', 'i')
    ) THEN
        v_key_join_expression := format(
            'CAST(s.%I AS %s)',
            v_campo_id,
            format_type(v_coluna_chave_destino_tipo, v_coluna_chave_destino_typmod)
        );
    ELSE
        RAISE EXCEPTION 'A coluna chave % possui tipos incompatíveis entre origem e destino.', v_campo_id;
    END IF;

    v_source_key_text_expression := format('CAST(s.%I AS TEXT)', v_campo_id);

    WITH destino AS (
        SELECT
            a.attname,
            a.attnum,
            a.atttypid,
            a.atttypmod,
            a.attidentity,
            a.attgenerated,
            pg_get_expr(ad.adbin, ad.adrelid) AS default_expr
        FROM pg_attribute a
        LEFT JOIN pg_attrdef ad
            ON ad.adrelid = a.attrelid
           AND ad.adnum = a.attnum
        WHERE a.attrelid = v_tabela_destino_regclass
          AND a.attnum > 0
          AND NOT a.attisdropped
    ),
    origem AS (
                SELECT
                        a.attname,
                        a.atttypid
        FROM pg_attribute a
        WHERE a.attrelid = v_tabela_origem_regclass
          AND a.attnum > 0
          AND NOT a.attisdropped
    )
    SELECT
        string_agg(format('%I', d.attname), ', ' ORDER BY d.attnum),
        string_agg(
            CASE
                WHEN o.atttypid = d.atttypid THEN format('s.%I', d.attname)
                ELSE format('CAST(s.%I AS %s)', d.attname, format_type(d.atttypid, d.atttypmod))
            END,
            ', ' ORDER BY d.attnum
        ),
        string_agg(
            CASE
                WHEN d.attname = v_campo_id THEN NULL
                ELSE format('%I = EXCLUDED.%I', d.attname, d.attname)
            END,
            ', ' ORDER BY d.attnum
        ),
        string_agg(
            CASE
                WHEN d.attname = v_campo_id THEN NULL
                ELSE format('d.%I IS DISTINCT FROM EXCLUDED.%I', d.attname, d.attname)
            END,
            ' OR ' ORDER BY d.attnum
        )
    INTO v_colunas_insert, v_colunas_select, v_colunas_update, v_colunas_update_where
    FROM destino d
    JOIN origem o ON o.attname = d.attname
    WHERE (
            o.atttypid = d.atttypid
            OR EXISTS (
                    SELECT 1
                    FROM pg_cast c
                    WHERE c.castsource = o.atttypid
                        AND c.casttarget = d.atttypid
                        AND c.castcontext IN ('a', 'i')
            )
        )
      AND NOT (
            d.attname <> v_campo_id
            AND (
                d.attgenerated <> ''
                OR d.attidentity IN ('a', 'd')
                OR COALESCE(d.default_expr, '') LIKE 'nextval(%'
            )
        );

    IF v_colunas_insert IS NULL OR v_colunas_insert = '' THEN
        RAISE EXCEPTION 'Nenhuma coluna compativel encontrada entre origem e destino.';
    END IF;

    IF position(format('%I', v_campo_id) IN v_colunas_insert) = 0 THEN
        RAISE EXCEPTION 'A coluna chave % precisa existir na origem e no destino para o UPSERT.', v_campo_id;
    END IF;

    DELETE FROM controle_migracao_lotes AS cml
    WHERE cml.migracao_id = p_migracao_id;

    -- Obter contagem e ID maximo da tabela de origem (sem carregar todos os IDs)
    EXECUTE format(
        'SELECT COALESCE(COUNT(*)::INTEGER, 0), COALESCE(MAX(%I)::BIGINT, 0) FROM %s',
        v_campo_id, v_tabela_origem_regclass
    ) INTO v_total_registros, v_total_lotes;

    UPDATE controle_migracao
    SET total_registros = COALESCE(v_total_registros, 0),
        id_maximo = COALESCE(v_total_lotes, 0),
        registros_processados = 0,
        lote_atual = 0,
        sql_colunas_insert = v_colunas_insert,
        sql_colunas_select = v_colunas_select,
        sql_colunas_update = v_colunas_update,
        sql_colunas_update_where = v_colunas_update_where,
        status = CASE WHEN COALESCE(v_total_registros, 0) = 0 THEN 'concluido' ELSE 'pendente' END,
        erro_msg = NULL,
        data_inicio = CURRENT_TIMESTAMP,
        data_fim = CASE WHEN COALESCE(v_total_registros, 0) = 0 THEN CURRENT_TIMESTAMP ELSE NULL END,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = p_migracao_id;

    RETURN QUERY
    SELECT COALESCE(v_total_registros, 0), COALESCE(v_total_lotes, 0), p_migracao_id;
END;
$$ LANGUAGE plpgsql;


-- ============================================================
-- PREPARAR COLUNAS DA MIGRAÇÃO (sem gerar lotes)
-- Retorna os metadados e a query de diferenças pronta para execução.
-- ============================================================
CREATE OR REPLACE FUNCTION preparar_colunas_migracao(p_migracao_id INTEGER)
RETURNS TABLE (
    tabela_origem_resolved TEXT,
    tabela_destino_resolved TEXT,
    campo_id_out TEXT,
    tamanho_lote_out INTEGER,
    colunas_insert TEXT,
    colunas_select TEXT,
    colunas_update TEXT,
    colunas_update_where TEXT,
    total_registros_out INTEGER,
    id_maximo_out BIGINT
) AS $$
DECLARE
    v_tabela_origem TEXT;
    v_tabela_destino TEXT;
    v_campo_id TEXT;
    v_colunas_insert TEXT;
    v_colunas_select TEXT;
    v_colunas_update TEXT;
    v_colunas_update_where TEXT;
    v_tamanho_lote INTEGER;
    v_total_registros INTEGER := 0;
    v_id_maximo BIGINT := 0;
    v_tabela_origem_regclass REGCLASS;
    v_tabela_destino_regclass REGCLASS;
    v_coluna_chave_destino TEXT;
    v_coluna_chave_origem_tipo OID;
    v_coluna_chave_destino_tipo OID;
    v_coluna_chave_destino_typmod INTEGER;
    v_key_join_expression TEXT;
    v_source_key_text_expression TEXT;
BEGIN
    SELECT
        cm.tabela_origem,
        cm.tabela_destino,
        cm.campo_id,
        cm.tamanho_lote
    INTO
        v_tabela_origem,
        v_tabela_destino,
        v_campo_id,
        v_tamanho_lote
    FROM controle_migracao cm
    WHERE cm.id = p_migracao_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Migracao nao encontrada: %', p_migracao_id;
    END IF;

    IF v_tamanho_lote IS NULL OR v_tamanho_lote <= 0 THEN
        RAISE EXCEPTION 'O tamanho do lote deve ser maior que zero.';
    END IF;

    v_tabela_origem_regclass := to_regclass(v_tabela_origem);
    v_tabela_destino_regclass := to_regclass(v_tabela_destino);

    IF v_tabela_origem_regclass IS NULL THEN
        RAISE EXCEPTION 'Tabela de origem nao encontrada: %', v_tabela_origem;
    END IF;

    IF v_tabela_destino_regclass IS NULL THEN
        RAISE EXCEPTION 'Tabela de destino nao encontrada: %', v_tabela_destino;
    END IF;

    SELECT
        format('%I', a.attname),
        a.atttypid,
        a.atttypmod
    INTO v_coluna_chave_destino, v_coluna_chave_destino_tipo, v_coluna_chave_destino_typmod
    FROM pg_attribute a
    WHERE a.attrelid = v_tabela_destino_regclass
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND a.attname = v_campo_id;

    IF v_coluna_chave_destino IS NULL THEN
        RAISE EXCEPTION 'A coluna chave % precisa existir na tabela de destino.', v_campo_id;
    END IF;

    SELECT a.atttypid
    INTO v_coluna_chave_origem_tipo
    FROM pg_attribute a
    WHERE a.attrelid = v_tabela_origem_regclass
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND a.attname = v_campo_id;

    IF v_coluna_chave_origem_tipo IS NULL THEN
        RAISE EXCEPTION 'A coluna chave % precisa existir na tabela de origem.', v_campo_id;
    END IF;

    IF v_coluna_chave_origem_tipo = v_coluna_chave_destino_tipo THEN
        v_key_join_expression := format('s.%I', v_campo_id);
    ELSIF EXISTS (
        SELECT 1
        FROM pg_cast c
        WHERE c.castsource = v_coluna_chave_origem_tipo
          AND c.casttarget = v_coluna_chave_destino_tipo
          AND c.castcontext IN ('a', 'i')
    ) THEN
        v_key_join_expression := format(
            'CAST(s.%I AS %s)',
            v_campo_id,
            format_type(v_coluna_chave_destino_tipo, v_coluna_chave_destino_typmod)
        );
    ELSE
        RAISE EXCEPTION 'A coluna chave % possui tipos incompativeis entre origem e destino.', v_campo_id;
    END IF;

    v_source_key_text_expression := format('CAST(s.%I AS TEXT)', v_campo_id);

    WITH destino AS (
        SELECT
            a.attname,
            a.attnum,
            a.atttypid,
            a.atttypmod,
            a.attidentity,
            a.attgenerated,
            pg_get_expr(ad.adbin, ad.adrelid) AS default_expr
        FROM pg_attribute a
        LEFT JOIN pg_attrdef ad
            ON ad.adrelid = a.attrelid
           AND ad.adnum = a.attnum
        WHERE a.attrelid = v_tabela_destino_regclass
          AND a.attnum > 0
          AND NOT a.attisdropped
    ),
    origem AS (
        SELECT
            a.attname,
            a.atttypid
        FROM pg_attribute a
        WHERE a.attrelid = v_tabela_origem_regclass
          AND a.attnum > 0
          AND NOT a.attisdropped
    )
    SELECT
        string_agg(format('%I', d.attname), ', ' ORDER BY d.attnum),
        string_agg(
            CASE
                WHEN o.atttypid = d.atttypid THEN format('s.%I', d.attname)
                ELSE format('CAST(s.%I AS %s)', d.attname, format_type(d.atttypid, d.atttypmod))
            END,
            ', ' ORDER BY d.attnum
        ),
        string_agg(
            CASE
                WHEN d.attname = v_campo_id THEN NULL
                ELSE format('%I = EXCLUDED.%I', d.attname, d.attname)
            END,
            ', ' ORDER BY d.attnum
        ),
        string_agg(
            CASE
                WHEN d.attname = v_campo_id THEN NULL
                ELSE format('d.%I IS DISTINCT FROM EXCLUDED.%I', d.attname, d.attname)
            END,
            ' OR ' ORDER BY d.attnum
        )
    INTO v_colunas_insert, v_colunas_select, v_colunas_update, v_colunas_update_where
    FROM destino d
    JOIN origem o ON o.attname = d.attname
    WHERE (
            o.atttypid = d.atttypid
            OR EXISTS (
                    SELECT 1
                    FROM pg_cast c
                    WHERE c.castsource = o.atttypid
                        AND c.casttarget = d.atttypid
                        AND c.castcontext IN ('a', 'i')
            )
        )
      AND NOT (
            d.attname <> v_campo_id
            AND (
                d.attgenerated <> ''
                OR d.attidentity IN ('a', 'd')
                OR COALESCE(d.default_expr, '') LIKE 'nextval(%'
            )
        );

    IF v_colunas_insert IS NULL OR v_colunas_insert = '' THEN
        RAISE EXCEPTION 'Nenhuma coluna compativel encontrada entre origem e destino.';
    END IF;

    IF position(format('%I', v_campo_id) IN v_colunas_insert) = 0 THEN
        RAISE EXCEPTION 'A coluna chave % precisa existir na origem e no destino para o UPSERT.', v_campo_id;
    END IF;

    -- Atualizar colunas na migração
    UPDATE controle_migracao
    SET sql_colunas_insert = v_colunas_insert,
        sql_colunas_select = v_colunas_select,
        sql_colunas_update = v_colunas_update,
        sql_colunas_update_where = v_colunas_update_where,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = p_migracao_id;

    -- Obter contagem e ID maximo da tabela de origem (sem carregar todos os IDs)
    EXECUTE format(
        'SELECT COALESCE(COUNT(*)::INTEGER, 0), COALESCE(MAX(%I)::BIGINT, 0) FROM %s',
        v_campo_id, v_tabela_origem_regclass
    ) INTO v_total_registros, v_id_maximo;

    -- Atualizar totais na migração
    UPDATE controle_migracao
    SET total_registros = v_total_registros,
        id_maximo = v_id_maximo,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = p_migracao_id;

    RETURN QUERY
    SELECT
        v_tabela_origem_regclass::TEXT,
        v_tabela_destino_regclass::TEXT,
        v_campo_id,
        v_tamanho_lote,
        v_colunas_insert,
        v_colunas_select,
        v_colunas_update,
        v_colunas_update_where,
        v_total_registros,
        v_id_maximo;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION inicializar_migracao(
    p_tabela_origem TEXT,
    p_tabela_destino TEXT,
    p_campo_id TEXT,
    p_tamanho_lote INTEGER DEFAULT 1
)
RETURNS TABLE (
    total_registros INTEGER,
    total_lotes INTEGER,
    migracao_id INTEGER
) AS $$
DECLARE
    v_migracao_id INTEGER;
    v_tabela_origem REGCLASS;
    v_tabela_destino REGCLASS;
BEGIN
    IF p_tamanho_lote IS NULL OR p_tamanho_lote <= 0 THEN
        RAISE EXCEPTION 'O tamanho do lote deve ser maior que zero.';
    END IF;

    v_tabela_origem := to_regclass(p_tabela_origem);
    v_tabela_destino := to_regclass(p_tabela_destino);

    IF v_tabela_origem IS NULL THEN
        RAISE EXCEPTION 'Tabela de origem nao encontrada: %', p_tabela_origem;
    END IF;

    IF v_tabela_destino IS NULL THEN
        RAISE EXCEPTION 'Tabela de destino nao encontrada: %', p_tabela_destino;
    END IF;

    INSERT INTO controle_migracao (
        tabela_origem,
        tabela_destino,
        campo_id,
        total_registros,
        tamanho_lote,
        status,
        data_inicio
    ) VALUES (
        v_tabela_origem::TEXT,
        v_tabela_destino::TEXT,
        p_campo_id,
        0,
        p_tamanho_lote,
        'pendente',
        CURRENT_TIMESTAMP
    )
    RETURNING id INTO v_migracao_id;

    RETURN QUERY
    SELECT 0, 0, v_migracao_id;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION processar_lote(
    p_migracao_id INTEGER,
    p_numero_lote INTEGER
)
RETURNS TABLE (
    registros_inseridos INTEGER,
    status VARCHAR(20),
    mensagem TEXT
) AS $$
DECLARE
    v_tabela_origem TEXT;
    v_tabela_destino TEXT;
    v_campo_id TEXT;
    v_registros_inseridos INTEGER := 0;
    v_colunas_insert TEXT;
    v_colunas_select TEXT;
    v_colunas_update TEXT;
    v_sql TEXT;
    v_tabela_origem_regclass REGCLASS;
    v_tabela_destino_regclass REGCLASS;
    v_registros_lote INTEGER := 0;
    v_keys_literal TEXT;
BEGIN
    IF p_numero_lote IS NULL OR p_numero_lote <= 0 THEN
        RETURN QUERY SELECT 0, 'erro'::VARCHAR, 'Numero de lote invalido'::TEXT;
        RETURN;
    END IF;

    SELECT
        cm.tabela_origem,
        cm.tabela_destino,
        cm.campo_id,
        cm.sql_colunas_insert,
        cm.sql_colunas_select,
        cm.sql_colunas_update
    INTO
        v_tabela_origem,
        v_tabela_destino,
        v_campo_id,
        v_colunas_insert,
        v_colunas_select,
        v_colunas_update
    FROM controle_migracao cm
    WHERE cm.id = p_migracao_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN QUERY SELECT 0, 'erro'::VARCHAR, 'Migracao nao encontrada'::TEXT;
        RETURN;
    END IF;

    v_tabela_origem_regclass := to_regclass(v_tabela_origem);
    v_tabela_destino_regclass := to_regclass(v_tabela_destino);

    IF v_tabela_origem_regclass IS NULL THEN
        RETURN QUERY SELECT 0, 'erro'::VARCHAR, format('Tabela de origem nao encontrada: %s', v_tabela_origem)::TEXT;
        RETURN;
    END IF;

    IF v_tabela_destino_regclass IS NULL THEN
        RETURN QUERY SELECT 0, 'erro'::VARCHAR, format('Tabela de destino nao encontrada: %s', v_tabela_destino)::TEXT;
        RETURN;
    END IF;

    -- Coletar as chaves do lote como lista literal para pushdown no FDW
    SELECT string_agg(quote_literal(cml.chave_valor), ',')
    INTO v_keys_literal
    FROM controle_migracao_lotes AS cml
    WHERE cml.migracao_id = p_migracao_id
      AND cml.numero_lote = p_numero_lote;

    IF v_keys_literal IS NULL OR v_keys_literal = '' THEN
        RETURN QUERY SELECT 0, 'erro'::VARCHAR, format('Lote %s nao encontrado ou ainda nao gerado', p_numero_lote)::TEXT;
        RETURN;
    END IF;

    SELECT COUNT(*)::INTEGER
    INTO v_registros_lote
    FROM controle_migracao_lotes AS cml
    WHERE cml.migracao_id = p_migracao_id
      AND cml.numero_lote = p_numero_lote;

    UPDATE controle_migracao
    SET status = 'processando',
        lote_atual = p_numero_lote,
        updated_at = CURRENT_TIMESTAMP,
        erro_msg = NULL
    WHERE id = p_migracao_id;

    IF v_colunas_insert IS NULL OR v_colunas_insert = '' OR v_colunas_select IS NULL OR v_colunas_select = '' THEN
        PERFORM atualizar_lotes_migracao(p_migracao_id);

        SELECT
            cm.sql_colunas_insert,
            cm.sql_colunas_select,
            cm.sql_colunas_update
        INTO
            v_colunas_insert,
            v_colunas_select,
            v_colunas_update
        FROM controle_migracao cm
        WHERE cm.id = p_migracao_id;
    END IF;

    IF v_colunas_insert IS NULL OR v_colunas_insert = '' THEN
        RETURN QUERY SELECT 0, 'erro'::VARCHAR, 'Nenhuma coluna compativel encontrada entre origem e destino'::TEXT;
        RETURN;
    END IF;

    IF position(format('%I', v_campo_id) IN v_colunas_insert) = 0 THEN
        RETURN QUERY SELECT 0, 'erro'::VARCHAR, format('A coluna chave %s precisa existir na origem e no destino para o UPSERT', v_campo_id)::TEXT;
        RETURN;
    END IF;

    -- Usa WHERE ... IN (literais) em vez de INNER JOIN para permitir pushdown no FDW
    IF v_colunas_update IS NULL OR v_colunas_update = '' THEN
        v_sql := format(
            'INSERT INTO %1$s (%2$s)
             SELECT %3$s
             FROM %5$s s
             WHERE CAST(s.%4$I AS TEXT) IN (%6$s)
             ON CONFLICT (%4$I) DO NOTHING',
            v_tabela_destino_regclass,
            v_colunas_insert,
            v_colunas_select,
            v_campo_id,
            v_tabela_origem_regclass,
            v_keys_literal
        );
    ELSE
        v_sql := format(
            'INSERT INTO %1$s (%2$s)
             SELECT %3$s
             FROM %5$s s
             WHERE CAST(s.%4$I AS TEXT) IN (%6$s)
             ON CONFLICT (%4$I) DO UPDATE
             SET %7$s',
            v_tabela_destino_regclass,
            v_colunas_insert,
            v_colunas_select,
            v_campo_id,
            v_tabela_origem_regclass,
            v_keys_literal,
            v_colunas_update
        );
    END IF;

    EXECUTE v_sql;

    GET DIAGNOSTICS v_registros_inseridos = ROW_COUNT;

    UPDATE controle_migracao
    SET registros_processados = registros_processados + v_registros_inseridos,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = p_migracao_id;

    RETURN QUERY
    SELECT
        v_registros_inseridos,
        'sucesso'::VARCHAR,
        format('Lote %s processado com sucesso', p_numero_lote)::TEXT;

EXCEPTION WHEN OTHERS THEN
    UPDATE controle_migracao
    SET status = 'erro',
        erro_msg = SQLERRM,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = p_migracao_id;

    RETURN QUERY SELECT 0, 'erro'::VARCHAR, SQLERRM::TEXT;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION processar_registro(
    p_migracao_id INTEGER,
    p_chave_valor TEXT
)
RETURNS TABLE (
    registros_inseridos INTEGER,
    status VARCHAR(20),
    mensagem TEXT
) AS $$
DECLARE
    v_tabela_origem TEXT;
    v_tabela_destino TEXT;
    v_campo_id TEXT;
    v_registros_afetados INTEGER := 0;
    v_colunas_insert TEXT;
    v_colunas_select TEXT;
    v_colunas_update TEXT;
    v_colunas_update_where TEXT;
    v_sql TEXT;
    v_tabela_origem_regclass REGCLASS;
    v_tabela_destino_regclass REGCLASS;
    v_existe_origem BOOLEAN := FALSE;
    v_existe_destino BOOLEAN := FALSE;
    v_acao TEXT;
BEGIN
    SELECT
        cm.tabela_origem,
        cm.tabela_destino,
        cm.campo_id,
        cm.sql_colunas_insert,
        cm.sql_colunas_select,
        cm.sql_colunas_update,
        cm.sql_colunas_update_where
    INTO
        v_tabela_origem,
        v_tabela_destino,
        v_campo_id,
        v_colunas_insert,
        v_colunas_select,
        v_colunas_update,
        v_colunas_update_where
    FROM controle_migracao cm
    WHERE cm.id = p_migracao_id;

    IF NOT FOUND THEN
        RETURN QUERY SELECT 0, 'erro'::VARCHAR, 'Migracao nao encontrada'::TEXT;
        RETURN;
    END IF;

    v_tabela_origem_regclass := to_regclass(v_tabela_origem);
    v_tabela_destino_regclass := to_regclass(v_tabela_destino);

    IF v_tabela_origem_regclass IS NULL THEN
        RETURN QUERY SELECT 0, 'erro'::VARCHAR, format('Tabela de origem nao encontrada: %s', v_tabela_origem)::TEXT;
        RETURN;
    END IF;

    IF v_tabela_destino_regclass IS NULL THEN
        RETURN QUERY SELECT 0, 'erro'::VARCHAR, format('Tabela de destino nao encontrada: %s', v_tabela_destino)::TEXT;
        RETURN;
    END IF;

    -- Preparar colunas se ainda nao foram geradas
    IF v_colunas_insert IS NULL OR v_colunas_insert = '' OR v_colunas_select IS NULL OR v_colunas_select = '' THEN
        PERFORM * FROM preparar_colunas_migracao(p_migracao_id);

        SELECT
            cm.sql_colunas_insert,
            cm.sql_colunas_select,
            cm.sql_colunas_update,
            cm.sql_colunas_update_where
        INTO
            v_colunas_insert,
            v_colunas_select,
            v_colunas_update,
            v_colunas_update_where
        FROM controle_migracao cm
        WHERE cm.id = p_migracao_id;
    END IF;

    IF v_colunas_insert IS NULL OR v_colunas_insert = '' THEN
        RETURN QUERY SELECT 0, 'erro'::VARCHAR, 'Nenhuma coluna compativel encontrada entre origem e destino'::TEXT;
        RETURN;
    END IF;

    -- 1. Verificar se o registro existe na tabela de ORIGEM (foreign table)
    EXECUTE format(
        'SELECT EXISTS(SELECT 1 FROM %s WHERE CAST(%I AS TEXT) = %L)',
        v_tabela_origem_regclass, v_campo_id, p_chave_valor
    ) INTO v_existe_origem;

    IF NOT v_existe_origem THEN
        -- ID nao encontrado na origem (gap na sequencia), registrar e pular
        INSERT INTO controle_migracao_lotes (migracao_id, numero_lote, chave_valor, status, erro_msg, updated_at)
        VALUES (p_migracao_id, 1, p_chave_valor, 'nao_encontrado', NULL, CURRENT_TIMESTAMP)
        ON CONFLICT (migracao_id, chave_valor) DO UPDATE
        SET status = 'nao_encontrado', erro_msg = NULL, updated_at = CURRENT_TIMESTAMP;

        RETURN QUERY SELECT 0, 'nao_encontrado'::VARCHAR,
            format('ID %s nao encontrado na origem', p_chave_valor)::TEXT;
        RETURN;
    END IF;

    -- 2. Verificar se o registro ja existe na tabela DESTINO (local)
    EXECUTE format(
        'SELECT EXISTS(SELECT 1 FROM %s WHERE CAST(%I AS TEXT) = %L)',
        v_tabela_destino_regclass, v_campo_id, p_chave_valor
    ) INTO v_existe_destino;

    IF v_existe_destino THEN
        -- 3a. Registro existe no destino: UPDATE somente se houver diferenca
        IF v_colunas_update IS NOT NULL AND v_colunas_update <> '' AND v_colunas_update_where IS NOT NULL AND v_colunas_update_where <> '' THEN
            v_sql := format(
                'UPDATE %1$s d SET %7$s
                 FROM %5$s s
                 WHERE CAST(s.%4$I AS TEXT) = %6$L
                   AND CAST(d.%4$I AS TEXT) = %6$L
                   AND (%8$s)',
                v_tabela_destino_regclass,
                v_colunas_insert,
                v_colunas_select,
                v_campo_id,
                v_tabela_origem_regclass,
                p_chave_valor,
                replace(v_colunas_update, 'EXCLUDED.', 's.'),
                replace(v_colunas_update_where, 'EXCLUDED.', 's.')
            );

            EXECUTE v_sql;
            GET DIAGNOSTICS v_registros_afetados = ROW_COUNT;

            IF v_registros_afetados > 0 THEN
                v_acao := 'atualizado';
            ELSE
                v_acao := 'ja_existe';
            END IF;
        ELSE
            v_acao := 'ja_existe';
        END IF;
    ELSE
        -- 3b. Registro nao existe no destino: INSERT consultando a foreign table
        v_sql := format(
            'INSERT INTO %1$s (%2$s)
             SELECT %3$s
             FROM %5$s s
             WHERE CAST(s.%4$I AS TEXT) = %6$L',
            v_tabela_destino_regclass,
            v_colunas_insert,
            v_colunas_select,
            v_campo_id,
            v_tabela_origem_regclass,
            p_chave_valor
        );

        EXECUTE v_sql;
        GET DIAGNOSTICS v_registros_afetados = ROW_COUNT;
        v_acao := 'inserido';
    END IF;

    -- Registrar no controle de lotes (INSERT ON CONFLICT para tracking on-the-fly)
    INSERT INTO controle_migracao_lotes (migracao_id, numero_lote, chave_valor, status, erro_msg, updated_at)
    VALUES (p_migracao_id, 1, p_chave_valor, v_acao, NULL, CURRENT_TIMESTAMP)
    ON CONFLICT (migracao_id, chave_valor) DO UPDATE
    SET status = v_acao, erro_msg = NULL, updated_at = CURRENT_TIMESTAMP;

    -- Atualizar contador na migracao
    UPDATE controle_migracao
    SET registros_processados = registros_processados + 1,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = p_migracao_id;

    RETURN QUERY
    SELECT
        v_registros_afetados,
        'sucesso'::VARCHAR,
        format('Registro %s: %s', p_chave_valor, v_acao)::TEXT;

EXCEPTION WHEN OTHERS THEN
    -- Registrar erro no controle de lotes
    BEGIN
        INSERT INTO controle_migracao_lotes (migracao_id, numero_lote, chave_valor, status, erro_msg, updated_at)
        VALUES (p_migracao_id, 1, p_chave_valor, 'erro', SQLERRM, CURRENT_TIMESTAMP)
        ON CONFLICT (migracao_id, chave_valor) DO UPDATE
        SET status = 'erro', erro_msg = SQLERRM, updated_at = CURRENT_TIMESTAMP;
    EXCEPTION WHEN OTHERS THEN
        NULL; -- ignora erro ao registrar
    END;

    RETURN QUERY SELECT 0, 'erro'::VARCHAR, SQLERRM::TEXT;
END;
$$ LANGUAGE plpgsql;


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
