-- Controle de Funcionários por Termo de Convênio --------------------------------------------------------------------------------------------------------------------------------------------	
/* 
Consulta que identifica funcionários vinculados a termos de convênio, com base nas despesas salariais.
Classifica os cargos a partir da descrição da despesa, informa as datas do primeiro e último pagamento,
e define o status funcional como ATIVO, INATIVO ou INDEFINIDO com base na data do último pagamento.
*/
WITH pagamentos AS (
    SELECT 
        d.favorecido,
        d.termo,
        MIN(d.data_debito_convenio) AS data_primeiro_pagamento,
        MAX(d.data_debito_convenio) AS data_ultimo_pagamento,
        CASE 
            WHEN d.descricao_despesa LIKE '%AUXILIAR SERVIÇOS GERAIS%' THEN 'AUXILIAR SERVIÇOS GERAIS'
            WHEN d.descricao_despesa LIKE '%CUIDADORA AUXILIAR%' THEN 'CUIDADORA AUXILIAR'
            WHEN d.descricao_despesa LIKE '%CUIDADORA%' THEN 'CUIDADORA'
            WHEN d.descricao_despesa LIKE '%NUTRICIONISTA%' THEN 'NUTRICIONISTA'
            WHEN d.descricao_despesa LIKE '%PSICÓLOGA%' THEN 'PSICÓLOGA'
            WHEN d.descricao_despesa LIKE '%ASSISTENTE SOCIAL%' THEN 'ASSISTENTE SOCIAL'
            WHEN d.descricao_despesa LIKE '%COORDENADOR%' THEN 'COORDENADOR'
            WHEN d.descricao_despesa LIKE '%COZINHEIRA%' THEN 'COZINHEIRA'
            WHEN d.descricao_despesa LIKE '%PROFESSORA NÃO REGENTE%' THEN 'PROFESSORA NÃO REGENTE'
			WHEN d.descricao_despesa LIKE '%PROFESSORA REGENTE%' THEN 'PROFESSORA REGENTE'
			WHEN d.descricao_despesa LIKE '%MOTORISTA%' THEN 'MOTORISTA'
            ELSE 'OUTRO'
        END AS cargo
    FROM despesas d
    WHERE d.descricao_despesa LIKE '%SALÁRIO%'
      AND (
            d.descricao_despesa LIKE '%CUIDADORA%' 
         OR d.descricao_despesa LIKE '%CUIDADORA AUXILIAR%' 
         OR d.descricao_despesa LIKE '%AUXILIAR SERVIÇOS GERAIS%'
         OR d.descricao_despesa LIKE '%NUTRICIONISTA%'
         OR d.descricao_despesa LIKE '%PSICÓLOGA%'
         OR d.descricao_despesa LIKE '%ASSISTENTE SOCIAL%'
         OR d.descricao_despesa LIKE '%COORDENADOR%'
         OR d.descricao_despesa LIKE '%COZINHEIRA%'
         OR d.descricao_despesa LIKE '%PROFESSORA NÃO REGENTE%'
	     OR d.descricao_despesa LIKE '%PROFESSORA REGENTE%'
		 OR d.descricao_despesa LIKE '%MOTORISTA%'
      )
    GROUP BY 
        d.favorecido, 
        d.termo,
        CASE 
            WHEN d.descricao_despesa LIKE '%AUXILIAR SERVIÇOS GERAIS%' THEN 'AUXILIAR SERVIÇOS GERAIS'
            WHEN d.descricao_despesa LIKE '%CUIDADORA AUXILIAR%' THEN 'CUIDADORA AUXILIAR'
            WHEN d.descricao_despesa LIKE '%CUIDADORA%' THEN 'CUIDADORA'
            WHEN d.descricao_despesa LIKE '%NUTRICIONISTA%' THEN 'NUTRICIONISTA'
            WHEN d.descricao_despesa LIKE '%PSICÓLOGA%' THEN 'PSICÓLOGA'
            WHEN d.descricao_despesa LIKE '%ASSISTENTE SOCIAL%' THEN 'ASSISTENTE SOCIAL'
            WHEN d.descricao_despesa LIKE '%COORDENADOR%' THEN 'COORDENADOR'
            WHEN d.descricao_despesa LIKE '%COZINHEIRA%' THEN 'COZINHEIRA'
            WHEN d.descricao_despesa LIKE '%PROFESSORA NÃO REGENTE%' THEN 'PROFESSORA NÃO REGENTE'
			WHEN d.descricao_despesa LIKE '%PROFESSORA REGENTE%' THEN 'PROFESSORA REGENTE'
			WHEN d.descricao_despesa LIKE '%MOTORISTA%' THEN 'MOTORISTA'
            ELSE 'OUTRO'
        END
)

SELECT 
    p.favorecido,
    p.termo,
    p.cargo,
    p.data_primeiro_pagamento,
    p.data_ultimo_pagamento,
    CASE 
        WHEN FORMAT(p.data_ultimo_pagamento, 'yyyyMM') >= FORMAT(DATEADD(MONTH, -1, GETDATE()), 'yyyyMM') THEN 'ATIVO'
        WHEN FORMAT(p.data_ultimo_pagamento, 'yyyyMM') <= FORMAT(DATEADD(MONTH, -3, GETDATE()), 'yyyyMM') THEN 'INATIVO'
        ELSE 'INDEFINIDO'
    END AS status_funcional
FROM pagamentos p
ORDER BY 
    p.favorecido ASC,
    p.termo ASC,
    p.cargo ASC;












	select*from despesas where favorecido like '%PATR%';
	









-- DEMONSTRATIVO MENSAL DE REMUNERAÇÃO (MES/ANO) POR TERMO E RUBRICA -------------------------------------------------------------------------------------------------------------------------------------------

	--GASTO REAL POR MES
	-- ESPECIFICO TOTAL GAWTo
SELECT 
    termo,
    FORMAT(SUM(valor), 'N2', 'pt-BR') AS total_gasto_pessoal
FROM despesas
WHERE termo = 6893
  AND (
        rubrica IN ('3.1.90.11.99', '3.1.90.13.99')
        OR descricao_despesa LIKE '%REEMBOLSO RECOLHIMENTO%'  -- encargos: INSS, FGTS, IRRF etc.
        OR descricao_despesa LIKE '%PAGAMENTO SALÁRIO%'        -- salários
        OR descricao_despesa LIKE '%FÉRIAS%'                   -- férias
      )
  AND data_debito_convenio >= '2025-09-01'
  AND data_debito_convenio <  '2025-10-01'
GROUP BY termo;



-- Todas as despesas de setembro de 2025 
SELECT * FROM despesas WHERE termo = 6893 AND rubrica IN ('3.1.90.11.99', '3.1.90.13.99') AND data_debito_convenio >= '2025-09-01' AND data_debito_convenio < '2025-10-01';



-- TOTAL GERAL GASTO COM PESSOAL (salários, férias e encargos) - VÁRIOS TERMOS

-- Soma das Médias de

-- GASTO DETALHADO POR CARGO - VÁRIOS TERMOS (Salários, Férias e Encargos)
SELECT 
    termo,

    -- Tipo de despesa: separa salários, férias e encargos
    CASE 
        WHEN descricao_despesa LIKE '%REEMBOLSO RECOLHIMENTO%' THEN 'ENCARGOS (INSS / FGTS / IRRF)'
        WHEN descricao_despesa LIKE '%PAGAMENTO SALÁRIO%' THEN 'SALÁRIO'
        WHEN descricao_despesa LIKE '%FÉRIAS%' THEN 'FÉRIAS'
    END AS tipo_despesa,

    -- Identificação do cargo a partir da descrição
    CASE 
        WHEN descricao_despesa LIKE '%CUIDADORA AUXILIAR%' THEN 'CUIDADORA AUXILIAR'
        WHEN descricao_despesa LIKE '%CUIDADORA%' THEN 'CUIDADORA'
        WHEN descricao_despesa LIKE '%AUXILIAR SERVIÇOS GERAIS%' THEN 'AUXILIAR SERVIÇOS GERAIS'
        WHEN descricao_despesa LIKE '%NUTRICIONISTA%' THEN 'NUTRICIONISTA'
        WHEN descricao_despesa LIKE '%PSICÓLOGA%' THEN 'PSICÓLOGA'
        WHEN descricao_despesa LIKE '%ASSISTENTE SOCIAL%' THEN 'ASSISTENTE SOCIAL'
        WHEN descricao_despesa LIKE '%COORDENADOR%' THEN 'COORDENADOR(A)'
        WHEN descricao_despesa LIKE '%COZINHEIRA%' THEN 'COZINHEIRA'
        WHEN descricao_despesa LIKE '%PROFESSORA REGENTE%' THEN 'PROFESSORA REGENTE'
        ELSE NULL  -- exclui linhas que não se encaixem nas categorias definidas
    END AS cargo,

    COUNT(DISTINCT cpf_cnpj) AS qtd_funcionarios,
    FORMAT(SUM(valor), 'N2', 'pt-BR') AS total_gasto

FROM despesas
WHERE termo IN (6373, 6893, 6932, 26478, 26672)
  AND rubrica IN ('3.1.90.11.99', '3.1.90.13.99') -- Pessoal e encargos
  AND (
        descricao_despesa LIKE '%REEMBOLSO RECOLHIMENTO%'  -- encargos
        OR descricao_despesa LIKE '%PAGAMENTO SALÁRIO%'    -- salários
        OR descricao_despesa LIKE '%FÉRIAS%'               -- férias
      )
  AND data_debito_convenio >= '2025-09-01'
  AND data_debito_convenio <  '2025-10-01'
GROUP BY 
    termo,
    CASE 
        WHEN descricao_despesa LIKE '%REEMBOLSO RECOLHIMENTO%' THEN 'ENCARGOS (INSS / FGTS / IRRF)'
        WHEN descricao_despesa LIKE '%PAGAMENTO SALÁRIO%' THEN 'SALÁRIO'
        WHEN descricao_despesa LIKE '%FÉRIAS%' THEN 'FÉRIAS'
    END,
    CASE 
        WHEN descricao_despesa LIKE '%CUIDADORA AUXILIAR%' THEN 'CUIDADORA AUXILIAR'
        WHEN descricao_despesa LIKE '%CUIDADORA%' THEN 'CUIDADORA'
        WHEN descricao_despesa LIKE '%AUXILIAR SERVIÇOS GERAIS%' THEN 'AUXILIAR SERVIÇOS GERAIS'
        WHEN descricao_despesa LIKE '%NUTRICIONISTA%' THEN 'NUTRICIONISTA'
        WHEN descricao_despesa LIKE '%PSICÓLOGA%' THEN 'PSICÓLOGA'
        WHEN descricao_despesa LIKE '%ASSISTENTE SOCIAL%' THEN 'ASSISTENTE SOCIAL'
        WHEN descricao_despesa LIKE '%COORDENADOR%' THEN 'COORDENADOR(A)'
        WHEN descricao_despesa LIKE '%COZINHEIRA%' THEN 'COZINHEIRA'
        WHEN descricao_despesa LIKE '%PROFESSORA REGENTE%' THEN 'PROFESSORA REGENTE'
        ELSE NULL
    END
ORDER BY termo, tipo_despesa, total_gasto DESC;









-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- SALÁRIOS - Totais p/termo p/mes ou Individuais p/cargo ou pesso termp/mes (Pessoal: 11.99) ------------------------------------------------------------------------------------

select*
from despesas
where id_termo_rubrica = '6729-3.1.90.11.99'
and descricao_despesa like '%RESCISÃO%';


-- RESCISÕES (A indenização vai aparecer no estorno) (Pessoal: 11.99) ----------------------------------------------------------------------------------------------------------------------
 
select*
from despesas
where id_termo_rubrica = '6729-3.1.90.11.99'
and descricao_despesa like '%PAGAMENTO SALÁRIO%';


-- INSS 13 - Totais p/termo p/mes (Pessoal: 11.99) ----------------------------------------------------------------------------------------------------------------------

select*
from despesas
where id_termo_rubrica = '6373-3.1.90.11.99'
and descricao_despesa like '%RECOLHIMENTO INSS - 13º SALÁRIO%';



-- FGTS 13 - Totais p/termo p/mes (Encargos: 13.99) ----------------------------------------------------------------------------------------------------------------------

select*
from despesas
where termo = '6373'
and descricao_despesa like '%13º SALÁRIO%';



-- INSS Salários - Totais p/termo p/mes (Pessoal: 11.99) ----------------------------------------------------------------------------------------------------------------------

select*
from despesas
where id_termo_rubrica = '6729-3.1.90.11.99'
and descricao_despesa like '%RECOLHIMENTO INSS%'
and descricao_despesa not like '%RECOLHIMENTO INSS - 13º SALÁRIO%';


-- IRRF Salários - Totais p/termo p/mes (Pessoal: 11.99) ----------------------------------------------------------------------------------------------------------------------

select*
from despesas
where id_termo_rubrica = '6729-3.1.90.11.99'
and descricao_despesa like '%RECOLHIMENTO IRRF%';

-- FGTS Saários - Totais p/termo p/mes (Encargos: 13.99) ----------------------------------------------------------------------------------------------------------------------

select*
from despesas
where termo = '6729'
and descricao_despesa like '%REEMBOLSO RECOLHIMENTO FGTS%'
and descricao_despesa not like '%13º SALÁRIO%'; 




-- Histórico de Férias de Funcionários -------------------------------------------------------------------------------------------------------------------------------------------------------
/* 
Consulta que extrai informações sobre férias de funcionários a partir da descrição das despesas.
Identifica o cargo, número de dias de férias, período de gozo (início e fim) e o termo de convênio correspondente.
As informações são obtidas por meio de expressões de texto aplicadas à coluna 'descricao_despesa'.
*/

SELECT 
    favorecido,

    -- Cargo extraído com CASE
    CASE 
        WHEN descricao_despesa LIKE '%AUXILIAR SERVIÇOS GERAIS%' THEN 'AUXILIAR SERVIÇOS GERAIS'
        WHEN descricao_despesa LIKE '%CUIDADORA AUXILIAR%' THEN 'CUIDADORA AUXILIAR'
        WHEN descricao_despesa LIKE '%CUIDADORA%' THEN 'CUIDADORA'
        WHEN descricao_despesa LIKE '%NUTRICIONISTA%' THEN 'NUTRICIONISTA'
        WHEN descricao_despesa LIKE '%PSICÓLOGA%' THEN 'PSICÓLOGA'
        WHEN descricao_despesa LIKE '%ASSISTENTE SOCIAL%' THEN 'ASSISTENTE SOCIAL'
        WHEN descricao_despesa LIKE '%COORDENADOR%' THEN 'COORDENADOR'
        WHEN descricao_despesa LIKE '%COZINHEIRA%' THEN 'COZINHEIRA'
		WHEN descricao_despesa LIKE '%PROFESSORA%' THEN 'PROFESSORA'
        ELSE 'OUTRO'
    END AS cargo,

    -- Dias de férias
    LTRIM(RTRIM(
        SUBSTRING(descricao_despesa, 
            CHARINDEX('-', descricao_despesa) + 1, 
            CHARINDEX('DIAS', descricao_despesa) - CHARINDEX('-', descricao_despesa) + 4
        )
    )) AS dias_ferias,

    -- Início do gozo
    TRY_CAST(
        '20' + RIGHT(SUBSTRING(descricao_despesa, CHARINDEX('GOZO DE ', descricao_despesa) + 8, 8), 2) + '-' +
        SUBSTRING(descricao_despesa, CHARINDEX('GOZO DE ', descricao_despesa) + 11, 2) + '-' +
        LEFT(SUBSTRING(descricao_despesa, CHARINDEX('GOZO DE ', descricao_despesa) + 8, 8), 2)
        AS DATE
    ) AS inicio_gozo,

    -- Fim do gozo
    TRY_CAST(
        '20' + RIGHT(SUBSTRING(descricao_despesa, CHARINDEX(' A ', descricao_despesa) + 3, 8), 2) + '-' +
        SUBSTRING(descricao_despesa, CHARINDEX(' A ', descricao_despesa) + 6, 2) + '-' +
        LEFT(SUBSTRING(descricao_despesa, CHARINDEX(' A ', descricao_despesa) + 3, 2), 2)
        AS DATE
    ) AS fim_gozo,

    termo

FROM despesas
WHERE descricao_despesa LIKE '%FÉRIAS%'
  AND (
        descricao_despesa LIKE '%CUIDADORA%' 
     OR descricao_despesa LIKE '%CUIDADORA AUXILIAR%'
     OR descricao_despesa LIKE '%AUXILIAR SERVIÇOS GERAIS%'
     OR descricao_despesa LIKE '%NUTRICIONISTA%'
     OR descricao_despesa LIKE '%PSICÓLOGA%'
     OR descricao_despesa LIKE '%ASSISTENTE SOCIAL%'
     OR descricao_despesa LIKE '%COORDENADOR%'
     OR descricao_despesa LIKE '%COZINHEIRA%'
	 OR descricao_despesa LIKE '%PROFESSORA%'
  ) -- n esta aparecendo algumas
ORDER BY favorecido, inicio_gozo;



-- Histórico de Pagamento de 13º Salário -------------------------------------------------------------------------------------------------------------------------------------------------------
/*
Consulta que extrai informações sobre o pagamento do décimo terceiro salário (1ª e 2ª parcelas) a partir da descrição das despesas.
Identifica o favorecido, cargo, parcela paga, valor, termo de convênio e a data do pagamento.
As informações são obtidas por meio de expressões aplicadas à coluna 'descricao_despesa', permitindo contemplar todos os termos.
*/

SELECT 
    favorecido,

    -- Cargo identificado por palavras-chave na descrição da despesa
    CASE 
        WHEN descricao_despesa LIKE '%AUXILIAR SERVIÇOS GERAIS%' THEN 'AUXILIAR SERVIÇOS GERAIS'
        WHEN descricao_despesa LIKE '%CUIDADORA AUXILIAR%' THEN 'CUIDADORA AUXILIAR'
        WHEN descricao_despesa LIKE '%CUIDADORA%' THEN 'CUIDADORA'
        WHEN descricao_despesa LIKE '%NUTRICIONISTA%' THEN 'NUTRICIONISTA'
        WHEN descricao_despesa LIKE '%PSICÓLOGA%' THEN 'PSICÓLOGA'
        WHEN descricao_despesa LIKE '%ASSISTENTE SOCIAL%' THEN 'ASSISTENTE SOCIAL'
        WHEN descricao_despesa LIKE '%COORDENADOR%' THEN 'COORDENADOR'
        WHEN descricao_despesa LIKE '%COZINHEIRA%' THEN 'COZINHEIRA'
        ELSE 'OUTRO'
    END AS cargo,

    -- Parcela do décimo terceiro (1ª ou 2ª)
    CASE 
        WHEN descricao_despesa LIKE '%1ª PARCELA%' THEN '1ª PARCELA'
        WHEN descricao_despesa LIKE '%2ª PARCELA%' THEN '2ª PARCELA'
        ELSE 'NÃO IDENTIFICADA'
    END AS parcela,

    -- Valor pago
    valor,

    -- Data de pagamento
    data_pagamento,

    -- Termo de convênio
    termo

FROM despesas
WHERE 
    descricao_despesa LIKE '%13%' AND
    (
        descricao_despesa LIKE '%1ª PARCELA%' OR
        descricao_despesa LIKE '%2ª PARCELA%'
    ) AND (
        descricao_despesa LIKE '%CUIDADORA%' 
        OR descricao_despesa LIKE '%CUIDADORA AUXILIAR%'
        OR descricao_despesa LIKE '%AUXILIAR SERVIÇOS GERAIS%'
        OR descricao_despesa LIKE '%NUTRICIONISTA%'
        OR descricao_despesa LIKE '%PSICÓLOGA%'
        OR descricao_despesa LIKE '%ASSISTENTE SOCIAL%'
        OR descricao_despesa LIKE '%COORDENADOR%'
        OR descricao_despesa LIKE '%COZINHEIRA%'
    )
ORDER BY termo, favorecido, parcela;




