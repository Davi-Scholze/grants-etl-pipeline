USE [ETL_Convenios]
GO

/****** Object:  Table [dbo].[despesas]    Script Date: 18/12/2025 13:52:19 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[despesas](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[termo] [varchar](50) NULL,
	[rubrica] [varchar](20) NULL,
	[tipo_despesa] [varchar](50) NULL,
	[cpf_cnpj] [varchar](14) NULL,
	[favorecido] [varchar](255) NULL,
	[tipo_doc_despesa] [varchar](100) NULL,
	[descricao_despesa] [varchar](255) NULL,
	[tipo_doc_pagamento] [varchar](100) NULL,
	[data_pagamento] [date] NULL,
	[data_debito_convenio] [date] NULL,
	[valor] [decimal](18, 2) NULL,
	[id_termo_rubrica] [varchar](70) NULL,
) ON [PRIMARY]
GO


