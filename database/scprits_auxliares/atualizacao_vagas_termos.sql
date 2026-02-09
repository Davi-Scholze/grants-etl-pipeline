	-- =============================================
	-- Atualização diária de vagas_termos
	-- Sobrescreve todas as vagas
	-- =============================================

	-- 1️⃣ Primeiro, limpa as vagas para reatribuição
	UPDATE vagas_termos
	SET 
		favorecido_atual = 'DISPONÍVEL',
		data_inicio = NULL,           -- deixa NULL ou use uma data fictícia
		status_vaga = 'DISPONÍVEL';

	GO

	-- 2️⃣ Seleciona favorecidos ativos e prepara numeração
	WITH ult_salario AS (
		SELECT
			termo,
			favorecido,
			cpf,
			cargo,
			data_evento,
			ROW_NUMBER() OVER (PARTITION BY cpf, termo ORDER BY data_evento ASC) AS rn_inicio,
			ROW_NUMBER() OVER (PARTITION BY cpf, termo ORDER BY data_evento DESC) AS rn_ultimo
		FROM favorecidos
		WHERE tipo_evento = 'PAGAMENTO SALÁRIO'
	),

	primeiro_pagamento_termo AS (
		SELECT
			termo,
			cpf,
			favorecido,
			cargo,
			data_evento AS data_inicio
		FROM ult_salario
		WHERE rn_inicio = 1
	),

	ultimo_pagamento AS (
		SELECT
			termo,
			cpf,
			favorecido,
			cargo,
			data_evento AS ultimo_pagamento
		FROM ult_salario
		WHERE rn_ultimo = 1
	),

	favorecidos_ativos AS (
		SELECT
			u.termo,
			u.cpf,
			u.favorecido,
			u.cargo,
			p.data_inicio
		FROM ultimo_pagamento u
		INNER JOIN primeiro_pagamento_termo p
			ON u.cpf = p.cpf AND u.termo = p.termo
		WHERE u.ultimo_pagamento >= DATEADD(MONTH, -1, DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1))
	),

	-- Numerando vagas disponíveis por termo+cargo
	vagas_disponiveis AS (
		SELECT
			id_vaga,
			termo,
			cargo,
			ROW_NUMBER() OVER (PARTITION BY termo, cargo ORDER BY id_vaga) AS rn_vaga
		FROM vagas_termos
	),

	-- Numerando favorecidos ativos por termo+cargo
	favorecidos_numerados AS (
		SELECT
			cpf,
			favorecido,
			termo,
			cargo,
			data_inicio,
			ROW_NUMBER() OVER (PARTITION BY termo, cargo ORDER BY cpf) AS rn_fav
		FROM favorecidos_ativos
	)

	-- 3️⃣ Atualiza vagas_termos associando 1:1 favorecido <> vaga
	UPDATE v
	SET 
		v.favorecido_atual = f.favorecido,
		v.data_inicio = f.data_inicio,
		v.status_vaga = 'OCUPADA'
	FROM vagas_termos v
	INNER JOIN vagas_disponiveis vd
		ON v.id_vaga = vd.id_vaga
	INNER JOIN favorecidos_numerados f
		ON vd.termo = f.termo
	   AND vd.cargo = f.cargo
	   AND vd.rn_vaga = f.rn_fav;

	GO



	select*from vagas_termos