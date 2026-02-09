USE [ETL_Convenios]
GO

/****** Object:  Table [dbo].[termos]    Script Date: 18/12/2025 13:54:11 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[termos](
	[id_termo] [int] NOT NULL,
	[situacao_atual] [varchar](50) NULL,
	[data_inicio_vigencia] [date] NULL,
	[data_fim_vigencia] [date] NULL,
	[valor_total_transferido] [decimal](14, 2) NULL,
	[data_atual] [date] NULL,
	[dias_restantes] [int] NULL,
	[saldo_total_gasto] [decimal](14, 2) NULL,
	[saldo_atual] [decimal](14, 2) NULL,
	[rendimento_financeiro_total] [decimal](14, 2) NULL,
	[rendimento_financeiro_atual] [decimal](18, 2) NULL,
	[nro_sit] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[id_termo] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


