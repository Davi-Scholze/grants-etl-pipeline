USE [ETL_Convenios]
GO

/****** Object:  Table [dbo].[vagas_termos]    Script Date: 18/12/2025 13:54:34 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[vagas_termos](
	[termo] [int] NOT NULL,
	[cargo] [nvarchar](150) NOT NULL,
	[status_vaga] [nvarchar](20) NOT NULL,
	[favorecido_atual] [nvarchar](250) NULL,
	[data_inicio] [date] NULL,
	[id_vaga] [int] IDENTITY(1,1) NOT NULL,
 CONSTRAINT [PK_vagas_termos] PRIMARY KEY CLUSTERED 
(
	[id_vaga] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[vagas_termos]  WITH CHECK ADD CHECK  (([status_vaga]='RESERVADA' OR [status_vaga]='DISPONÍVEL' OR [status_vaga]='OCUPADA'))
GO


