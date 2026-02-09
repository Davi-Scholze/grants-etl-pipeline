USE [ETL_Convenios]
GO

/****** Object:  Table [dbo].[favorecidos]    Script Date: 18/12/2025 13:53:53 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[favorecidos](
	[termo] [int] NOT NULL,
	[favorecido] [nvarchar](250) NOT NULL,
	[cpf] [nvarchar](20) NULL,
	[cargo] [nvarchar](150) NULL,
	[tipo_evento] [varchar](200) NULL,
	[data_evento] [date] NULL,
	[valor_evento] [decimal](18, 2) NULL
) ON [PRIMARY]
GO


