-- ============================================================================
-- INICIALIZAÇÃO DO SQL SERVER - DOCKER
-- ============================================================================
-- Este script é executado automaticamente quando o container SQL Server inicia
-- Cria a database ETL_Convenios e suas tabelas
-- ============================================================================

-- Aguarda alguns segundos para SQL Server estar pronto
WAITFOR DELAY '00:00:05'
GO

-- ============================================================================
-- 1. CRIAR DATABASE
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'ETL_Convenios')
BEGIN
    CREATE DATABASE ETL_Convenios
    COLLATE Latin1_General_CI_AS
END
GO

-- Usar a database
USE ETL_Convenios
GO

-- ============================================================================
-- 2. EXECUTAR SCRIPTS DDL (criação de tabelas)
-- ============================================================================

-- Tabela: dbo.despesas
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'despesas')
BEGIN
    CREATE TABLE dbo.despesas (
        id_despesa INT IDENTITY(1,1) PRIMARY KEY,
        nro_sit VARCHAR(50) NOT NULL,
        nro_favorecido VARCHAR(50),
        tipo_pessoa VARCHAR(10),
        cpf_cnpj VARCHAR(20),
        nome_favorecido VARCHAR(255),
        tipo_despesa VARCHAR(100),
        descricao_despesa VARCHAR(500),
        valor_despesa DECIMAL(18, 2),
        data_despesa DATE,
        rubrica_codigo VARCHAR(50),
        rubrica_descricao VARCHAR(255),
        status VARCHAR(20),
        data_criacao DATETIME DEFAULT GETDATE(),
        data_atualizacao DATETIME,
        CONSTRAINT UK_despesa_sit_favorecido UNIQUE (nro_sit, nro_favorecido, data_despesa)
    );
    CREATE INDEX IX_despesa_sit ON dbo.despesas(nro_sit);
    CREATE INDEX IX_despesa_data ON dbo.despesas(data_despesa);
END
GO

-- Tabela: dbo.termos
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'termos')
BEGIN
    CREATE TABLE dbo.termos (
        id_termo INT PRIMARY KEY,
        nro_sit VARCHAR(50) NOT NULL UNIQUE,
        rendimento_financeiro_total DECIMAL(18, 2),
        status VARCHAR(20),
        data_atualizacao DATETIME DEFAULT GETDATE()
    );
    CREATE INDEX IX_termo_sit ON dbo.termos(nro_sit);
END
GO

-- Tabela: dbo.rubricas
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'rubricas')
BEGIN
    CREATE TABLE dbo.rubricas (
        id_rubrica INT IDENTITY(1,1) PRIMARY KEY,
        id_termo INT NOT NULL,
        nro_sit VARCHAR(50) NOT NULL,
        rubrica_codigo VARCHAR(50),
        valor_estornado DECIMAL(18, 2),
        id_termo_rubrica INT,
        data_atualizacao DATETIME DEFAULT GETDATE(),
        FOREIGN KEY (id_termo) REFERENCES dbo.termos(id_termo),
        CONSTRAINT UK_rubrica_termo_sit UNIQUE (id_termo, nro_sit, rubrica_codigo)
    );
    CREATE INDEX IX_rubrica_sit ON dbo.rubricas(nro_sit);
    CREATE INDEX IX_rubrica_termo ON dbo.rubricas(id_termo);
END
GO

-- Tabela: dbo.favorecidos
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'favorecidos')
BEGIN
    CREATE TABLE dbo.favorecidos (
        id_favorecido INT IDENTITY(1,1) PRIMARY KEY,
        nro_sit VARCHAR(50) NOT NULL,
        nro_favorecido VARCHAR(50),
        tipo_pessoa VARCHAR(10),
        cpf_cnpj VARCHAR(20) UNIQUE,
        nome_favorecido VARCHAR(255),
        email VARCHAR(100),
        telefone VARCHAR(20),
        endereco VARCHAR(500),
        data_atualizacao DATETIME DEFAULT GETDATE(),
        CONSTRAINT UK_favorecido_sit_numero UNIQUE (nro_sit, nro_favorecido)
    );
    CREATE INDEX IX_favorecido_cpf ON dbo.favorecidos(cpf_cnpj);
    CREATE INDEX IX_favorecido_sit ON dbo.favorecidos(nro_sit);
END
GO

-- Tabela: dbo.vagas_termos
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'vagas_termos')
BEGIN
    CREATE TABLE dbo.vagas_termos (
        id_vaga INT IDENTITY(1,1) PRIMARY KEY,
        id_termo INT NOT NULL,
        nro_sit VARCHAR(50),
        vaga_numero INT,
        status_vaga VARCHAR(20),
        data_atualizacao DATETIME DEFAULT GETDATE(),
        FOREIGN KEY (id_termo) REFERENCES dbo.termos(id_termo)
    );
    CREATE INDEX IX_vaga_termo ON dbo.vagas_termos(id_termo);
    CREATE INDEX IX_vaga_sit ON dbo.vagas_termos(nro_sit);
END
GO

-- ============================================================================
-- 3. CRIAR USUÁRIO PARA APLICAÇÃO (Opcional)
-- ============================================================================

-- Criar login se não existir
IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = 'etl_user')
BEGIN
    CREATE LOGIN etl_user WITH PASSWORD = 'ETL@Convenios2024!'
END
GO

-- Criar usuário no database
USE ETL_Convenios
GO

IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'etl_user')
BEGIN
    CREATE USER etl_user FOR LOGIN etl_user
    ALTER ROLE db_datareader ADD MEMBER etl_user
    ALTER ROLE db_datawriter ADD MEMBER etl_user
END
GO

-- ============================================================================
-- 4. MENSAGEM DE SUCESSO
-- ============================================================================

PRINT '========================================='
PRINT 'Database ETL_Convenios criada com sucesso!'
PRINT 'Tabelas: despesas, termos, rubricas, favorecidos, vagas_termos'
PRINT '========================================='
GO
