USE [ETL_Convenios]
GO

/****** Object:  Table [dbo].[rubricas]    Script Date: 18/12/2025 13:54:03 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[rubricas](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[id_termo_rubrica] [varchar](70) NULL,
	[data_inicio_vigencia] [date] NULL,
	[data_fim_vigencia] [date] NULL,
	[saldo_inicial_previsto] [decimal](18, 2) NULL,
	[data_atual] [date] NULL,
	[dias_restantes] [int] NULL,
	[saldo_total_gasto] [decimal](18, 2) NULL,
	[saldo_atual] [decimal](18, 2) NULL,
	[media_mensal_gastos] [decimal](18, 2) NULL,
	[previsao_mensal_saldo] [int] NULL,
	[previsao_mensal_saldo_texto] [varchar](255) NULL,
	[termo] [varchar](50) NULL,
	[descricao_rubrica] [varchar](255) NULL,
	[valor_estornado] [decimal](10, 2) NULL,
	[executado] [nvarchar](20) NULL,
	[nro_sit] [int] NULL
) ON [PRIMARY]
GO


