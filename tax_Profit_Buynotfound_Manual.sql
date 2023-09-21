--drop table tax_Profit_Buynotfound_Manual
CREATE TABLE [dbo].[tax_Profit_Buynotfound_Manual](
	[TransId] [int] IDENTITY(1,1) NOT NULL,
	[ClientId] [varchar](max) NULL,
	[Trandate] [datetime] NULL,
	[Purchasedate] [datetime] NULL,
	[Security] [varchar](32) NULL,
	[ISIN] [varchar](32) NULL,
	[Qty] [int] NULL,
	[Value] [int] NULL,
	[BROKERAGE] [numeric](15, 2) NULL,
	[SERVICETAX] [numeric](15, 2) NULL,
	[TOLEVY] [numeric](15, 2) NULL,
	[StampDuty] [numeric](15, 2)  NULL,
	[Euser] [varchar](10) NULL,
	[Lastupdatedon] [datetime] NULL,
	[BuyExpense][numeric](15,2) NULL
	)
GO
