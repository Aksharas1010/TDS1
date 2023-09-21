USE [GCC]
GO

/****** Object:  Table [dbo].[Tax_Profit_Details_Cash]    Script Date: 8/1/2023 5:43:20 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Tax_Profit_Details_Cash_TDS](
	[TransID] [int] IDENTITY(1,1) NOT NULL,
	[Clientid] [int] NULL,
	[Type] [varchar](32) NULL,
	[TranDateBuy] [datetime] NULL,
	[Security] [varchar](32) NULL,
	[ISIN] [varchar](32) NULL,
	[BuyQty] [int] NULL,
	[BuyValue] [numeric](15, 2) NULL,
	[TranDateSale] [datetime] NULL,
	[SaleQty] [int] NULL,
	[SaleValue] [numeric](15, 2) NULL,
	[DayToSell] [int] NULL,
	[Profit] [numeric](15, 2) NULL,
	[Euser] [varchar](10) NULL,
	[LastuPdatedon] [datetime] NULL,
	[BuyExpense] [numeric](15, 2) NULL,
	[PurchaseBrokerage] [numeric](15, 2) NULL,
	[PurchaseServiceTax] [numeric](15, 2) NULL,
	[PurchaseExchangeLevy] [numeric](15, 2) NULL,
	[PurchaseStampDuty] [numeric](15, 2) NULL,
	[SaleBrokerage] [numeric](15, 2) NULL,
	[SaleServiceTax] [numeric](15, 2) NULL,
	[SaleExchangeLevy] [numeric](15, 2) NULL,
	[SaleStampDuty] [numeric](15, 2) NULL,
	[SellExpense] [numeric](15, 2) NULL,
	[CreatedDate][datetime]null,
	[Active][int]null,
	[Description] [varchar](128) NULL,
PRIMARY KEY CLUSTERED 
(
	[TransID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


