/* ==========================================================================
   SCRIPT DE ATUALIZAÇÃO FINANCEIRA E VIGÊNCIA (RELATÓRIO COMPLETO)
   ========================================================================== */
SET NOCOUNT ON; 
PRINT '>>> INICIANDO PROCESSO DE ATUALIZAÇÃO COMPLETA...'

/**********************************************************************************************************************************************************************************************
📁 BLOCO 1 - CRIAÇÃO DA VIEW vw_rubricas_calc
**********************************************************************************************************************************************************************************************/
PRINT '1. Atualizando View vw_rubricas_calc...'
GO
CREATE OR ALTER VIEW vw_rubricas_calc AS
SELECT
    r.id_termo_rubrica,
    GETDATE() AS data_atual,
    DATEDIFF(DAY, GETDATE(), t.data_fim_vigencia) AS dias_restantes,
    COALESCE(SUM(d.valor), 0) AS saldo_total_gasto,
    COALESCE(SUM(d.valor), 0) / NULLIF(DATEDIFF(MONTH, t.data_inicio_vigencia, MAX(d.data_pagamento)) + 1, 0) AS media_mensal_gastos
FROM rubricas r
INNER JOIN termos t 
    ON t.id_termo = CAST(LEFT(r.id_termo_rubrica, CHARINDEX('-', r.id_termo_rubrica) - 1) AS INT)
LEFT JOIN despesas d 
    ON d.id_termo_rubrica = r.id_termo_rubrica
GROUP BY 
    r.id_termo_rubrica, 
    t.data_inicio_vigencia,
    t.data_fim_vigencia;
GO

/**********************************************************************************************************************************************************************************************
📁 BLOCO 2 - ATUALIZAÇÃO DA TABELA RUBRICAS (COM RELATÓRIO TOTAL)
**********************************************************************************************************************************************************************************************/
PRINT '2. Executando UPDATE na tabela RUBRICAS...'

-- Tabela temporária para guardar o log de TUDO o que mudou
DECLARE @LogRubricas TABLE (Id VARCHAR(50), SaldoAntigo MONEY, SaldoNovo MONEY, DiasRestantes INT);

UPDATE r
SET
    r.data_atual = v.data_atual,
    r.dias_restantes = v.dias_restantes,
    r.saldo_total_gasto = v.saldo_total_gasto,
    r.saldo_atual = r.saldo_inicial_previsto - v.saldo_total_gasto + ISNULL(r.valor_estornado, 0),
    r.executado = 
        CASE 
            WHEN r.saldo_inicial_previsto > 0 THEN 
                FORMAT(v.saldo_total_gasto * 100.0 / r.saldo_inicial_previsto, 'N2', 'pt-BR') + '%'
            ELSE NULL
        END,
    r.media_mensal_gastos = v.media_mensal_gastos,
    r.previsao_mensal_saldo = 
        CASE 
            WHEN v.media_mensal_gastos > 0 THEN 
                (r.saldo_inicial_previsto - v.saldo_total_gasto + ISNULL(r.valor_estornado, 0)) / v.media_mensal_gastos
            ELSE NULL
        END,
    r.previsao_mensal_saldo_texto = 
        CASE 
            WHEN v.media_mensal_gastos > 0 THEN
                CAST(FLOOR((r.saldo_inicial_previsto - v.saldo_total_gasto + ISNULL(r.valor_estornado, 0)) / v.media_mensal_gastos / 12) AS VARCHAR) + ' anos, ' +
                CAST(FLOOR((r.saldo_inicial_previsto - v.saldo_total_gasto + ISNULL(r.valor_estornado, 0)) / v.media_mensal_gastos) % 12 AS VARCHAR) + ' meses e ' +
                CAST(FLOOR((((r.saldo_inicial_previsto - v.saldo_total_gasto + ISNULL(r.valor_estornado, 0)) / v.media_mensal_gastos) * 30.4375) % 30.4375) AS VARCHAR) + ' dias'
            ELSE NULL
        END
OUTPUT inserted.id_termo_rubrica, deleted.saldo_atual, inserted.saldo_atual, inserted.dias_restantes INTO @LogRubricas -- SALVA O LOG
FROM rubricas r
JOIN vw_rubricas_calc v 
    ON r.id_termo_rubrica = v.id_termo_rubrica;

-- EXIBE TUDO (SEM LIMITES)
SELECT 'Relatório Completo - Rubricas' as TIPO, * FROM @LogRubricas ORDER BY Id;
GO

/**********************************************************************************************************************************************************************************************
📁 BLOCO 3 - CRIAÇÃO DA VIEW vw_termos_calc
**********************************************************************************************************************************************************************************************/
PRINT '3. Atualizando View vw_termos_calc...'
GO
CREATE OR ALTER VIEW vw_termos_calc AS
SELECT
    CAST(LEFT(r.id_termo_rubrica, CHARINDEX('-', r.id_termo_rubrica) - 1) AS INT) AS id_termo,
    MAX(r.data_fim_vigencia) AS data_fim_vigencia,
    GETDATE() AS data_atual,
    DATEDIFF(DAY, GETDATE(), MAX(r.data_fim_vigencia)) AS dias_restantes,
    SUM(COALESCE(r.saldo_total_gasto, 0)) AS saldo_total_gasto,
    SUM(COALESCE(r.saldo_atual, 0)) AS saldo_atual
FROM rubricas r
GROUP BY CAST(LEFT(r.id_termo_rubrica, CHARINDEX('-', r.id_termo_rubrica) - 1) AS INT);
GO

/**********************************************************************************************************************************************************************************************
📁 BLOCO 4 - ATUALIZAÇÃO DA TABELA TERMOS (COM RELATÓRIO TOTAL)
**********************************************************************************************************************************************************************************************/
PRINT '4. Executando UPDATE na tabela TERMOS...'

-- Tabela temporária para log de Termos
DECLARE @LogTermos TABLE (Id INT, DiasAntigos INT, DiasNovos INT, SaldoNovo MONEY);

UPDATE t
SET
    t.data_atual = v.data_atual,
    t.dias_restantes = DATEDIFF(DAY, GETDATE(), t.data_fim_vigencia), -- Correção de dias aqui
    t.saldo_total_gasto = v.saldo_total_gasto,
    t.saldo_atual = v.saldo_atual + ISNULL(t.rendimento_financeiro_total, 0)
OUTPUT inserted.id_termo, deleted.dias_restantes, inserted.dias_restantes, inserted.saldo_atual INTO @LogTermos
FROM termos t
JOIN vw_termos_calc v ON v.id_termo = t.id_termo;

-- EXIBE TUDO (SEM LIMITES)
SELECT 'Relatório Completo - Termos' as TIPO, * FROM @LogTermos ORDER BY Id;
GO

/**********************************************************************************************************************************************************************************************
📁 BLOCO 5 - AJUSTE DO RENDIMENTO FINANCEIRO CONSUMIDO
**********************************************************************************************************************************************************************************************/
PRINT '5. Ajustando Rendimentos...'

;WITH cte_ajuste_rendimento AS (
    SELECT
        CAST(LEFT(r.id_termo_rubrica, CHARINDEX('-', r.id_termo_rubrica) - 1) AS INT) AS id_termo,
        SUM(CASE 
                WHEN r.saldo_atual < 0 
                     AND r.saldo_inicial_previsto > 0 
                     AND TRY_CAST(REPLACE(REPLACE(r.executado, '%', ''), ',', '.') AS FLOAT) > 100 
                THEN -r.saldo_atual
                ELSE 0
            END) AS valor_utilizado_rendimento
    FROM rubricas r
    GROUP BY CAST(LEFT(r.id_termo_rubrica, CHARINDEX('-', r.id_termo_rubrica) - 1) AS INT)
)
UPDATE t
SET
    t.rendimento_financeiro_atual = ISNULL(t.rendimento_financeiro_total, 0) - ISNULL(a.valor_utilizado_rendimento, 0)
FROM termos t
LEFT JOIN cte_ajuste_rendimento a ON a.id_termo = t.id_termo;

PRINT '>>> PROCESSO CONCLUÍDO COM SUCESSO.'
GO