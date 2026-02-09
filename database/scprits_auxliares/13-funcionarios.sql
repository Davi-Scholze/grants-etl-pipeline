;WITH ano AS (
    SELECT YEAR(GETDATE()) AS ano_atual
),

-- 1) FUNCIONÁRIOS ATIVOS
ativos AS (
    SELECT
        termo,
        cpf,
        MAX(favorecido) AS funcionario,
        MAX(data_evento) AS ultimo_salario
    FROM favorecidos
    WHERE tipo_evento = 'PAGAMENTO SALÁRIO'
      AND data_evento >= DATEADD(MONTH, -1, DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1))
    GROUP BY termo, cpf
),

-- 2) MÉDIA SALARIAL DOS ÚLTIMOS 12 MESES
salarios AS (
    SELECT
        cpf,
        AVG(valor_evento) AS media_salarial
    FROM favorecidos
    WHERE tipo_evento = 'PAGAMENTO SALÁRIO'
      AND data_evento >= DATEADD(MONTH, -12, GETDATE())
    GROUP BY cpf
),

-- 3) EVENTOS DE 13º DOS ATIVOS
eventos13 AS (
    SELECT
        f.termo,
        f.cpf,
        f.favorecido AS funcionario,
        f.tipo_evento,
        f.data_evento,
        f.valor_evento,
        CASE WHEN f.tipo_evento LIKE '%1ª PARCELA%' THEN 1 END AS eh_p1,
        CASE WHEN f.tipo_evento LIKE '%2ª PARCELA%' THEN 1 END AS eh_p2
    FROM favorecidos f
    CROSS JOIN ano a
    WHERE f.tipo_evento LIKE '%13%'
      AND YEAR(f.data_evento) = a.ano_atual
),

-- 4) JUNÇÃO DOS ATIVOS COM 13º E SALÁRIO
detalhe AS (
    SELECT
        a.termo,
        a.cpf,
        a.funcionario,
        s.media_salarial,

        MAX(CASE WHEN e.eh_p1 = 1 THEN e.valor_evento END) AS primeira_parcela,
        MAX(CASE WHEN e.eh_p1 = 1 THEN e.data_evento END) AS data_1parcela,

        MAX(CASE WHEN e.eh_p2 = 1 THEN e.valor_evento END) AS segunda_parcela,
        MAX(CASE WHEN e.eh_p2 = 1 THEN e.data_evento END) AS data_2parcela,

        -- Total somente se as duas parcelas existirem
        CASE
            WHEN MAX(CASE WHEN e.eh_p1 = 1 THEN e.valor_evento END) IS NOT NULL
             AND MAX(CASE WHEN e.eh_p2 = 1 THEN e.valor_evento END) IS NOT NULL
            THEN COALESCE(MAX(CASE WHEN e.eh_p1 = 1 THEN e.valor_evento END),0)
               + COALESCE(MAX(CASE WHEN e.eh_p2 = 1 THEN e.valor_evento END),0)
        END AS total_13
    FROM ativos a
    LEFT JOIN eventos13 e
           ON e.cpf = a.cpf AND e.termo = a.termo
    LEFT JOIN salarios s
           ON s.cpf = a.cpf
    GROUP BY a.termo, a.cpf, a.funcionario, s.media_salarial
)

SELECT
    termo,
    funcionario,
    FORMAT(media_salarial, 'C', 'pt-br') AS media_salarial,
    FORMAT(primeira_parcela, 'C', 'pt-br') AS primeira_parcela,
    data_1parcela,
    FORMAT(segunda_parcela, 'C', 'pt-br') AS segunda_parcela,
    data_2parcela,
    FORMAT(total_13, 'C', 'pt-br') AS total_13
FROM detalhe
ORDER BY termo, funcionario;
