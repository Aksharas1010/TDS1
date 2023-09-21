--drop table TDS_JournalEntry
CREATE TABLE TDS_JournalEntry (
	[TransId] [int] IDENTITY(1,1) NOT NULL,
    Client VARCHAR(50),
	ClientCode VARCHAR(50),
	TransSaleDate [datetime] NULL,      
	TotalTax DECIMAL(10, 2),
	Description varchar(1000),
	Lock int null
);