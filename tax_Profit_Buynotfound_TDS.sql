USE [GCC]
GO

/****** Object:  Table [dbo].[tax_Profit_Buynotfound]    Script Date: 8/1/2023 5:49:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
--drop table tax_Profit_Buynotfound_TDS
CREATE TABLE [dbo].[tax_Profit_Buynotfound_TDS](
	[TransId] [int] IDENTITY(1,1) NOT NULL,
	[Clientid] [int] NULL,
	[Trandate] [datetime] NULL,
	[Security] [varchar](32) NULL,
	[ISIN] [varchar](32) NULL,
	[Qty] [int] NULL,
	[SellValue] [int] NULL,
	[Profit] [int] null,
	[OtherCharges] [Decimal](18,3) NULL,

	[Euser] [varchar](10) NULL,
	[Lastupdatedon] [datetime] NULL
	
PRIMARY KEY CLUSTERED 
(
	[TransId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[tax_Profit_Buynotfound_TDS] ADD  DEFAULT (getdate()) FOR [Lastupdatedon]
GO


