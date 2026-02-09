-- Exclui todos os registros da tabela
DELETE FROM despesas;

-- Reseta o valor do IDENTITY (por exemplo, para uma coluna ID)
DBCC CHECKIDENT ('despesas', RESEED, 0);



