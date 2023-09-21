alter PROCEDURE SpInsertRecomputationData  
    @ClientId NVARCHAR(50),  
    @Security NVARCHAR(50),
	@ISIN NVARCHAR(50),
    @PurchaseAmt DECIMAL(18,2),  
    @Date DATETIME,  
    @Qty INT,
	@BuyExpense Decimal(18,2)
AS  
BEGIN  
    INSERT INTO tax_Profit_Buynotfound_Manual (ClientId, Security, Value, Purchasedate, Qty,Trandate,ISIN,BuyExpense)  
    VALUES (@ClientId, @Security, @PurchaseAmt, @Date, @Qty,getdate(),@ISIN,@BuyExpense)  
END  