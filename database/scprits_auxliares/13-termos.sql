;WITH ano AS (
    SELECT YEAR(GETDATE()) AS ano_atual
),

-- FUNCIONÁRIOS ATIVOS: quem recebeu salário no último mês
ativos AS (
    SELECT
        termo,
        cpf,
        MAX(favorecido) AS favorecido,
        MAX(cargo) AS cargo,
        MAX(data_evento) AS ultimo_salario
    FROM favorecidos
    WHERE tipo_evento = 'PAGAMENTO SALÁRIO'
      AND data_evento >= DATEADD(MONTH, -1, DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1))
    GROUP BY termo, cpf
),

-- EVENTOS DE 13º NO ANO
eventos13 AS (
    SELECT
        termo,
        cpf,
        tipo_evento,
        data_evento,
        valor_evento,
        CASE WHEN tipo_evento LIKE '%1ª PARCELA%' THEN 1 END AS eh_p1,
        CASE WHEN tipo_evento LIKE '%2ª PARCELA%' THEN 1 END AS eh_p2
    FROM favorecidos f
    CROSS JOIN ano
    WHERE tipo_evento LIKE '%13%'
      AND YEAR(data_evento) = ano_atual
),

-- MÉDIA SALARIAL DOS ÚLTIMOS 12 MESES
salarios AS (
    SELECT
        cpf,
        AVG(valor_evento) AS media_salarial
    FROM favorecidos
    WHERE tipo_evento = 'PAGAMENTO SALÁRIO'
      AND data_evento >= DATEADD(MONTH, -12, GETDATE())
    GROUP BY cpf
),

-- JUNÇÃO DOS ATIVOS COM OS EVENTOS DE 13º E AS MÉDIAS
processado AS (
    SELECT
        a.termo,
        a.cpf,
        a.favorecido,

        MAX(CASE WHEN e.eh_p1 = 1 THEN e.valor_evento END) AS parcela1,
        MAX(CASE WHEN e.eh_p1 = 1 THEN e.data_evento END) AS data_p1,

        MAX(CASE WHEN e.eh_p2 = 1 THEN e.valor_evento END) AS parcela2,
        MAX(CASE WHEN e.eh_p2 = 1 THEN e.data_evento END) AS data_p2,

        SUM(e.valor_evento) AS total_13,

        s.media_salarial
    FROM ativos a
    LEFT JOIN eventos13 e ON e.cpf = a.cpf AND e.termo = a.termo
    LEFT JOIN salarios s ON s.cpf = a.cpf
    GROUP BY a.termo, a.cpf, a.favorecido, s.media_salarial
),

-- PREVISÕES (apenas para quem não recebeu)
previsao AS (
    SELECT
        termo,
        cpf,
        favorecido,
        parcela1,
        parcela2,
        data_p1,
        data_p2,
        total_13,
        media_salarial,

        CASE WHEN parcela1 IS NULL THEN media_salarial * 0.5 END AS prev_p1,

        CASE WHEN parcela2 IS NULL
            THEN (media_salarial - ISNULL(parcela1, media_salarial * 0.5)) * 0.90
        END AS prev_p2
    FROM processado
)

-- RESUMO FINAL POR TERMO
SELECT 
    termo,
    COUNT(*) AS funcionarios_ativos,

    COUNT(CASE WHEN parcela1 IS NOT NULL THEN 1 END) AS qtd_recebeu_1parcela,
    COUNT(CASE WHEN parcela2 IS NOT NULL THEN 1 END) AS qtd_recebeu_2parcela,
    COUNT(CASE WHEN parcela1 IS NOT NULL AND parcela2 IS NOT NULL THEN 1 END) AS qtd_recebeu_total,

    FORMAT(SUM(ISNULL(prev_p1,0)), 'C', 'pt-br') AS total_prev_1parcela,
    FORMAT(SUM(ISNULL(prev_p2,0)), 'C', 'pt-br') AS total_prev_2parcela,
    FORMAT(SUM(ISNULL(prev_p1,0) + ISNULL(prev_p2,0)), 'C', 'pt-br') AS total_prev_a_pagar_2025
FROM previsao
GROUP BY termo
ORDER BY termo;




-- PReciso saber quem tem direito de receber feriase 13 / periodo de admissao - datadde admissao
 