--exec SpGetDailyTransactionDetails '2023-09-06','2023-09-06',1291229150,'2023-2024'      
alter Procedure SpGetDailyTransactionDetails     
@frmdate date null,      
@todate date null,      
@clientid int null,      
@FiscYear varchar(12)         
As                                                             
begin                                                                                                                                              
  Set NoCount On ;
  CREATE TABLE #temp (  
   ClientId INT,  
   salesid INT,  
   Selldate DATE,  
   sales_amount DECIMAL(10, 2),  
   sale_qty INT,  
   Security VARCHAR(255),  
   purchase_date DATE,  
   purchase_amount DECIMAL(10, 2),  
   purchase_qty INT,  
   profit decimal(18,3),  
   POH varchar(max),  
   SaleExpense decimal(18,3),  
   BuyExpense decimal(18,3),  
   ISIN varchar(max)  
 );  
  if @clientid !=0  
  begin  
 DECLARE @inputDate DATE;  
 DECLARE @startDate DATE;  
 DECLARE @endDate DATE ;   
 DECLARE @CurrentDate DATE = @frmdate;  
 DECLARE @year INT;
 DECLARE @month INT;
 DECLARE dateCursor CURSOR FOR  
 SELECT DATEADD(DAY, number, @frmdate)FROM master..spt_values WHERE type = 'P' AND DATEADD(DAY, number, @frmdate) <= @ToDate;  
 DECLARE @TransSaleDate DATE;  
 OPEN dateCursor;  
 FETCH NEXT FROM dateCursor INTO @TransSaleDate;  
 WHILE @@FETCH_STATUS = 0  
 BEGIN  
 set @inputDate = @TransSaleDate;  
 set @startDate = DATEFROMPARTS(YEAR(@inputDate), MONTH(@inputDate), 1);  
 set @endDate = EOMONTH(@inputDate);  
 set @year=YEAR(@inputDate);
 set @month=MONTH(@inputDate);
 IF EXISTS (SELECT 1 FROM tblTransactionsLock WHERE Year = @year AND Month = @month AND Active = 1)
BEGIN  
      
    SET @startDate = DATEFROMPARTS(YEAR(@inputDate), MONTH(@inputDate), 1);
    SET @endDate=EOMONTH(@inputDate); 
END
ELSE
BEGIN
    DECLARE @quarterStartDate DATE;
    DECLARE @quarterEndDate DATE;	
    SET @quarterStartDate = DATEFROMPARTS(@year, ((@month - 1) / 3) * 3 + 1, 1);
	SET @quarterEndDate = DATEADD(DAY, -1, DATEADD(MONTH, 3, @quarterStartDate));	SET @startDate =@quarterStartDate;
    SET @endDate =@quarterEndDate;
	print @startDate
print @endDate
end ;
 WITH RankedPurchases AS (  
   SELECT  
  tpbm.TransId,  
  tpbm.Purchasedate,  
  tpbm.Value AS purchase_amount,  
  tpbm.Security,  
  tpbm.Qty,  
  tpbm.ClientId,  
  tpbm.BuyExpense,  
  ROW_NUMBER() OVER (PARTITION BY tpbm.Security ORDER BY tpbm.Purchasedate) AS PurchaseRank  
   FROM  
  tax_Profit_Buynotfound_Manual tpbm where ClientId=@clientid and  tpbm.Trandate between @startDate and @endDate  
 ),  
  
 SalesWithPurchase AS (  
   SELECT  
  tpbt.TransId AS salesid,  
  tpbt.Trandate,  
  tpbt.SellValue AS sales_amount,  
  CAST(CASE WHEN tpbt.Qty <= rp.Qty THEN tpbt.SellValue ELSE CAST(rp.Qty AS DECIMAL(18, 3)) * (CAST(tpbt.SellValue AS DECIMAL(18, 3)) / CAST(tpbt.Qty AS DECIMAL(18, 3)))END AS DECIMAL(18, 3)) AS corresponding_sales_amount,  
  tpbt.Security,  
  tpbt.Qty AS saleqty,  
  rp.TransId AS purid,  
  rp.Purchasedate AS corresponding_purchase_date,  
  CASE WHEN tpbt.Qty <= rp.Qty THEN tpbt.Qty * (rp.purchase_amount / rp.Qty) ELSE rp.Qty * (rp.purchase_amount / rp.Qty)END AS corresponding_purchase_amount,  
  rp.PurchaseRank AS purchase_rank,  
  rp.Qty AS purqty,  
  CASE WHEN tpbt.Qty <= rp.Qty THEN tpbt.Qty ELSE rp.Qty END AS corresponding_sale_qty,  
  CASE WHEN tpbt.Qty <= rp.Qty THEN tpbt.Qty ELSE rp.Qty END AS corresponding_purchase_qty  
  ,tpbt.ClientId  
  ,CAST(CASE WHEN tpbt.Qty <= rp.Qty THEN tpbt.OtherCharges ELSE CAST(rp.Qty AS DECIMAL(18, 3)) * (CAST(tpbt.OtherCharges AS DECIMAL(18, 3)) / CAST(tpbt.Qty AS DECIMAL(18, 3)))END AS DECIMAL(18, 3)) AS SaleExpense,  
  CASE WHEN tpbt.Qty <= rp.Qty THEN tpbt.Qty * (rp.BuyExpense / rp.Qty) ELSE rp.Qty * (rp.BuyExpense/ rp.Qty)END AS BuyExpense ,  
  tpbt.ISIN  
  FROM  
  tax_Profit_Buynotfound_TDS tpbt  
  LEFT JOIN RankedPurchases rp ON tpbt.Security = rp.Security WHERE tpbt.SellValue > 0 and tpbt.Clientid=@clientid and tpbt.Trandate=@TransSaleDate  
 )  
  
 INSERT INTO #temp (salesid, Selldate, sales_amount,sale_qty, Security, purchase_date, purchase_amount, purchase_qty,profit,POH,ClientId,SaleExpense,BuyExpense,ISIN)  
 SELECT salesid,  
   Trandate,  
   corresponding_sales_amount,  
   corresponding_sale_qty,  
   Security,  
   corresponding_purchase_date,  
   corresponding_purchase_amount,  
   corresponding_purchase_qty,  
   (corresponding_sales_amount-corresponding_purchase_amount)-(SaleExpense+BuyExpense),  
   case when  DATEDIFF(day,corresponding_purchase_date, Trandate)>0 then DATEDIFF(day,corresponding_purchase_date, Trandate)else 1 end  AS days_diff,  
   ClientId,  
   SaleExpense,  
   BuyExpense,  
   ISIN  
 FROM  
   SalesWithPurchase  
 WHERE  
   purchase_rank = 1;  
  
   --select * from #temp  
  
 ----bal qty as buynotfound  
    if exists(SELECT t.Security,sum(t.purchase_qty),sum(b.Qty)FROM #temp t INNER JOIN tax_Profit_Buynotfound_TDS b ON b.Security = t.Security AND b.TransId = t.salesid and b.Trandate=@TransSaleDate GROUP BY t.Security HAVING SUM(b.Qty) <> SUM(t.purchase_qty))  
 begin   
    INSERT INTO #temp (salesid, Selldate, sales_amount, sale_qty, Security, purchase_date, purchase_amount, purchase_qty,profit,POH,ClientId,SaleExpense,BuyExpense,ISIN)  
    SELECT t.salesid, t.Selldate, (SUM(b.Qty) - SUM(t.purchase_qty))*(b.SellValue/b.Qty),  (SUM(b.Qty) - SUM(t.purchase_qty)), t.Security,NULL AS purchase_date, 0 AS purchase_amount,0 AS purchase_qty,  
    (SUM(b.Qty) - SUM(t.purchase_qty))*(b.SellValue/b.Qty)-(SUM(b.Qty) - SUM(t.purchase_qty))*(b.OtherCharges/b.Qty),1,b.Clientid,(SUM(b.Qty) - SUM(t.purchase_qty))*(b.OtherCharges/b.Qty),0,b.ISIN FROM #temp t  
    INNER JOIN tax_Profit_Buynotfound_TDS b ON b.Security = t.Security AND b.TransId = t.salesid and b.Trandate=@TransSaleDate  
    GROUP BY t.salesid, t.Selldate, t.sales_amount, t.sale_qty, t.Security,b.SellValue,b.Qty,1  
    HAVING SUM(t.purchase_qty) <> SUM(b.Qty);  
 end  
  
 --- without purchase  
   INSERT INTO #temp (salesid, Selldate, sales_amount, sale_qty, Security, purchase_date, purchase_amount, purchase_qty,profit,POH,ClientId,SaleExpense,BuyExpense,ISIN)  
   SELECT tds.TransId, tds.Trandate,tds.SellValue,tds.Qty,tds.Security,NULL AS purchase_date,   
   0 AS purchase_amount,0 AS purchase_qty,tds.Profit,1,tds.Clientid,tds.OtherCharges,0,tds.ISIN FROM tax_profit_buynotfound_tds tds  
   LEFT JOIN #temp temp ON tds.TransId = temp.salesid  
   WHERE temp.salesid IS NULL and tds.Trandate=@TransSaleDate;  
  
    FETCH NEXT FROM dateCursor INTO @TransSaleDate;  
END;  
 CLOSE dateCursor;  
 DEALLOCATE dateCursor;      
 end  
  else  
  begin  
	 set @CurrentDate  = @frmdate;  
	 DECLARE dateCursor1 CURSOR FOR  
	 SELECT DATEADD(DAY, number, @frmdate)FROM master..spt_values WHERE type = 'P' AND DATEADD(DAY, number, @frmdate) <= @ToDate;  
	 OPEN dateCursor1;  
	 FETCH NEXT FROM dateCursor1 INTO @TransSaleDate;  
	 WHILE @@FETCH_STATUS = 0  
	 BEGIN  
	 set @inputDate = @TransSaleDate;  
	 set @startDate = DATEFROMPARTS(YEAR(@inputDate), MONTH(@inputDate), 1);  
	 set @endDate = EOMONTH(@inputDate);  
	 set @year=YEAR(@inputDate);
	 set @month=MONTH(@inputDate);
	 IF EXISTS (SELECT 1 FROM tblTransactionsLock WHERE Year = @year AND Month = @month AND Active = 1)
		BEGIN        
			SET @startDate = DATEFROMPARTS(YEAR(@inputDate), MONTH(@inputDate), 1);
			SET @endDate=EOMONTH(@inputDate); 
		END
        ELSE
        BEGIN		
			SET @quarterStartDate = DATEFROMPARTS(@year, ((@month - 1) / 3) * 3 + 1, 1);
			SET @quarterEndDate = DATEADD(DAY, -1, DATEADD(MONTH, 3, @quarterStartDate));	SET @startDate =@quarterStartDate;
			SET @endDate =@quarterEndDate;
			print @startDate
			print @endDate
		end ;
	 WITH RankedPurchases AS (  
	   SELECT  
	  tpbm.TransId,  
	  tpbm.Purchasedate,  
	  tpbm.Value AS purchase_amount,  
	  tpbm.Security,  
	  tpbm.Qty,  
	  tpbm.ClientId,  
	  tpbm.BuyExpense,  
	  ROW_NUMBER() OVER (PARTITION BY tpbm.Security ORDER BY tpbm.Purchasedate) AS PurchaseRank  
	   FROM  
	  tax_Profit_Buynotfound_Manual tpbm where  tpbm.Trandate between @startDate and @endDate  
	 ),  
  
	 SalesWithPurchase AS (  
	   SELECT  
	  tpbt.TransId AS salesid,  
	  tpbt.Trandate,  
	  tpbt.SellValue AS sales_amount,  
	  CAST(CASE WHEN tpbt.Qty <= rp.Qty THEN tpbt.SellValue ELSE CAST(rp.Qty AS DECIMAL(18, 3)) * (CAST(tpbt.SellValue AS DECIMAL(18, 3)) / CAST(tpbt.Qty AS DECIMAL(18, 3)))END AS DECIMAL(18, 3)) AS corresponding_sales_amount,  
	  tpbt.Security,  
	  tpbt.Qty AS saleqty,  
	  rp.TransId AS purid,  
	  rp.Purchasedate AS corresponding_purchase_date,  
	  CASE WHEN tpbt.Qty <= rp.Qty THEN tpbt.Qty * (rp.purchase_amount / rp.Qty) ELSE rp.Qty * (rp.purchase_amount / rp.Qty)END AS corresponding_purchase_amount,  
	  rp.PurchaseRank AS purchase_rank,  
	  rp.Qty AS purqty,  
	  CASE WHEN tpbt.Qty <= rp.Qty THEN tpbt.Qty ELSE rp.Qty END AS corresponding_sale_qty,  
	  CASE WHEN tpbt.Qty <= rp.Qty THEN tpbt.Qty ELSE rp.Qty END AS corresponding_purchase_qty  
	  ,tpbt.ClientId  
	  ,CAST(CASE WHEN tpbt.Qty <= rp.Qty THEN tpbt.OtherCharges ELSE CAST(rp.Qty AS DECIMAL(18, 3)) * (CAST(tpbt.OtherCharges AS DECIMAL(18, 3)) / CAST(tpbt.Qty AS DECIMAL(18, 3)))END AS DECIMAL(18, 3)) AS SaleExpense,  
	  CASE WHEN tpbt.Qty <= rp.Qty THEN tpbt.Qty * (rp.BuyExpense / rp.Qty) ELSE rp.Qty * (rp.BuyExpense/ rp.Qty)END AS BuyExpense ,  
	  tpbt.ISIN  
	  FROM  
	  tax_Profit_Buynotfound_TDS tpbt  
	  LEFT JOIN RankedPurchases rp ON tpbt.Security = rp.Security and tpbt.Clientid=rp.ClientId and tpbt.SellValue > 0  and tpbt.Trandate=@TransSaleDate  
	 )  
  
	 INSERT INTO #temp (salesid, Selldate, sales_amount,sale_qty, Security, purchase_date, purchase_amount, purchase_qty,profit,POH,ClientId,SaleExpense,BuyExpense,ISIN)  
	 SELECT salesid,  
	   Trandate,  
	   corresponding_sales_amount,  
	   corresponding_sale_qty,  
	   Security,  
	   corresponding_purchase_date,  
	   corresponding_purchase_amount,  
	   corresponding_purchase_qty,  
	   (corresponding_sales_amount-corresponding_purchase_amount)-(SaleExpense-BuyExpense),  
	   case when  DATEDIFF(day,corresponding_purchase_date, Trandate)>0 then DATEDIFF(day,corresponding_purchase_date, Trandate)else 1 end  AS days_diff,  
	   ClientId,  
	   SaleExpense,  
	   BuyExpense,  
	   ISIN  
	 FROM  
	   SalesWithPurchase  
	 WHERE  
	   purchase_rank = 1;  
	  -- select * from #temp
	 if exists(SELECT t.Security,sum(t.purchase_qty),sum(b.Qty)FROM #temp t INNER JOIN tax_Profit_Buynotfound_TDS b ON b.Security = t.Security AND b.TransId = t.salesid and b.Trandate=@TransSaleDate GROUP BY t.Security HAVING SUM(b.Qty) <> SUM(t.purchase_qty))  
	 begin   
		INSERT INTO #temp (salesid, Selldate, sales_amount, sale_qty, Security, purchase_date, purchase_amount, purchase_qty,profit,POH,ClientId,SaleExpense,BuyExpense,ISIN)  
		SELECT t.salesid, t.Selldate, (SUM(b.Qty) - SUM(t.purchase_qty))*(b.SellValue/b.Qty),  (SUM(b.Qty) - SUM(t.purchase_qty)), t.Security,NULL AS purchase_date, 0 AS purchase_amount,0 AS purchase_qty,  
		(SUM(b.Qty) - SUM(t.purchase_qty))*(b.SellValue/b.Qty)-(SUM(b.Qty) - SUM(t.purchase_qty))*(b.OtherCharges/b.Qty),1,b.Clientid,(SUM(b.Qty) - SUM(t.purchase_qty))*(b.OtherCharges/b.Qty),0,b.ISIN FROM #temp t  
		INNER JOIN tax_Profit_Buynotfound_TDS b ON b.Security = t.Security AND b.TransId = t.salesid and b.Trandate=@TransSaleDate  
		GROUP BY t.salesid, t.Selldate, t.sales_amount, t.sale_qty, t.Security,b.SellValue,b.Qty,b.Clientid,t.ClientId,b.OtherCharges,b.ISIN
		HAVING SUM(t.purchase_qty) <> SUM(b.Qty);  
	 end  
  
	 --- without purchase  
	 INSERT INTO #temp (salesid, Selldate, sales_amount, sale_qty, Security, purchase_date, purchase_amount, purchase_qty,profit,POH,ClientId,SaleExpense,BuyExpense,ISIN)  
	   SELECT tds.TransId, tds.Trandate,tds.SellValue,tds.Qty,tds.Security,NULL AS purchase_date,   
	   0 AS purchase_amount,0 AS purchase_qty,tds.Profit,1,tds.Clientid,tds.OtherCharges,0,tds.ISIN FROM tax_profit_buynotfound_tds tds  
	   LEFT JOIN #temp temp ON tds.TransId = temp.salesid  
	   WHERE temp.salesid IS NULL and tds.Trandate=@TransSaleDate;  
  
	 FETCH NEXT FROM dateCursor1 INTO @TransSaleDate;  
	END;  
	 CLOSE dateCursor1;  
	 DEALLOCATE dateCursor1;      
 end 
  
--select * from #temp  
  
  if @clientid != 0    
  begin    
 WITH ProfitData AS (      
    SELECT      
        c.NAME as ClientName,      
        c.TRADECODE as TradeCode,      
        RTRIM(curlocation) + RTRIM(tradecode) as ClientCode,      
        c.CLIENTID,      
        c.PAN_GIR as Pan,      
        t.BuyQty,      
        t.BuyValue,      
        t.TranDateBuy,      
        t.TranDateSale,      
        t.TransID,      
		t.Type,      
        T.SaleQty,      
        T.SaleValue,      
        T.ISIN,      
        t.Security,      
        t.DayToSell,      
        t.Profit,      
        t.BuyExpense,      
        t.SellExpense,      
        max(s.SeriesCode) as SecurityType,      
        MAX(s.PRODUCT) as PRODUCT -- Using MAX to pick one PRODUCT name      
  from Tax_Profit_Details_Cash_TDS t  
  INNER JOIN Client c ON c.CLIENTID = t.Clientid      
  LEFT JOIN Sauda s ON s.CLIENTID = t.Clientid AND t.TranDateSale = s.TRANDATE AND t.Security = s.SECURITY      
  WHERE TranDateSale between @frmdate and @todate  AND t.clientid = @clientid    
  GROUP BY      
        c.NAME,      
        c.TRADECODE,      
        RTRIM(curlocation) + RTRIM(tradecode),      
        c.CLIENTID,      
        t.BuyQty,      
        t.BuyValue,      
        t.TranDateBuy,      
        t.TranDateSale,      
        t.TransID,      
        T.SaleQty,      
        T.SaleValue,      
        T.ISIN,      
        t.Security,      
        t.DayToSell,      
        t.Profit,      
        t.BuyExpense,      
        t.SellExpense,      
        c.PAN_GIR,  
        t.Type      
     
  union     
    
   SELECT    
    c.NAME as ClientName,      
    c.TRADECODE as TradeCode,       
    RTRIM(curlocation) + RTRIM(tradecode) as ClientCode,    
    c.CLIENTID,    
    c.PAN_GIR as Pan,        
    tb.purchase_qty as BuyQty,    
    tb.purchase_amount as BuyValue,    
    case when tb.purchase_date is null then tb.Selldate else tb.purchase_date end  as purchase_date,    
    tb.Selldate,    
    0 as TransID,    
    case when POH<=1 then 'Short Term' else 'Long Term'  end as Type,    
    tb.sale_qty as SaleQty,    
    tb.sales_amount as SaleValue,    
    tb.ISIN as ISIN,    
    tb.Security as Security,    
    tb.POH as DayToSell,      
    tb.Profit as Profit,      
    tb.BuyExpense as BuyExpense,      
    tb.SaleExpense as SellExpense,      
    max(s.SeriesCode) as SecurityType,      
    MAX(s.PRODUCT) as PRODUCT    
    from #temp tb  
    INNER JOIN Client c ON c.CLIENTID = tb.Clientid    
    LEFT JOIN Sauda s ON s.CLIENTID = tb.Clientid AND tb.Selldate= s.TRANDATE AND tb.Security = s.SECURITY    
    WHERE tb.Selldate  between @frmdate and @todate AND tb.ClientId = @clientid   
    GROUP BY    
  c.NAME,    
  c.TRADECODE,    
  RTRIM(curlocation) + RTRIM(tradecode),    
  c.CLIENTID,    
  C.PAN_GIR,    
  tb.Selldate,    
  -- tb.TransID,    
  tb.sale_qty,    
  tb.sales_amount,    
  tb.ISIN,    
  tb.Security,    
  tb.SaleExpense,    
  tb.Profit  ,  
  tb.POH,  
  tb.purchase_amount,  
  tb.purchase_qty,  
  tb.purchase_date    
  ,tb.BuyExpense
),      
     
TaxRates AS (      
    SELECT      
        Gain_type,      
        YTD_gain_range,      
        finyear,      
        Tax_rate,      
        Surcharge,      
        Cess,      
        Total_tax_rate      
    FROM TaxratesMaster      
    WHERE finyear = @FiscYear      
),      
      
TaxCalculation AS (      
    SELECT      
        pd.*,      
      CASE      
            WHEN pd.Type = 'Short Term' THEN      
    CASE      
     WHEN pd.Profit <= 5000000 THEN      
                        (SELECT Total_tax_rate  FROM TaxratesMaster        
      WHERE Gain_type = 'Short Term' AND YTD_gain_range = '<= 50,00,000' AND finyear = @FiscYear)       
                    ELSE      
       (SELECT Total_tax_rate  FROM TaxratesMaster        
      WHERE Gain_type = 'Short Term' AND YTD_gain_range = '> 50,00,000' AND finyear = @FiscYear)      
    END      
            WHEN pd.Type = 'Long Term' THEN      
                CASE      
                    WHEN pd.Profit <= 5000000 THEN      
                        (SELECT Total_tax_rate  FROM TaxratesMaster        
      WHERE Gain_type = 'Long Term' AND YTD_gain_range = '<= 50,00,000' AND finyear = @FiscYear)      
                    ELSE      
                        (SELECT Total_tax_rate  FROM TaxratesMaster        
      WHERE Gain_type = 'Long Term' AND YTD_gain_range = '> 50,00,000' AND finyear = @FiscYear)      
                END      
        END AS TaxPercentage,      
        CASE      
            WHEN pd.Type = 'Short Term' THEN      
    CASE      
     WHEN pd.Profit <= 5000000 THEN      
                        pd.Profit *  (SELECT Total_tax_rate / 100 FROM TaxratesMaster        
      WHERE Gain_type = 'Short Term' AND YTD_gain_range = '<= 50,00,000' AND finyear =@FiscYear)       
                    ELSE      
      pd.Profit * (SELECT Total_tax_rate / 100 FROM TaxratesMaster        
      WHERE Gain_type = 'Short Term' AND YTD_gain_range = '> 50,00,000' AND finyear = @FiscYear)      
    END      
            WHEN pd.Type = 'Long Term' THEN      
                CASE      
                    WHEN pd.Profit <= 5000000 THEN      
                        pd.Profit *(SELECT Total_tax_rate / 100 FROM TaxratesMaster        
      WHERE Gain_type = 'Long Term' AND YTD_gain_range = '<= 50,00,000' AND finyear = @FiscYear)      
                    ELSE      
                        pd.Profit *(SELECT Total_tax_rate / 100 FROM TaxratesMaster        
      WHERE Gain_type = 'Long Term' AND YTD_gain_range = '> 50,00,000' AND finyear =@FiscYear)      
                END      
        END AS TaxAmount      
    FROM ProfitData pd      
)      
      
-- Finally, you can select the columns you need from the TaxCalculation CTE      
SELECT      
    ClientName,      
    TradeCode,      
    ClientCode,      
    CLIENTID,      
    Pan,      
    BuyQty,      
    BuyValue,      
    TranDateBuy,      
    TranDateSale,      
    TransID,      
    CASE WHEN Type='Long Term' THEN 'LG' ELSE 'SG' END as Type,      
    SaleQty,      
    SaleValue,      
    ISIN,      
    Security,      
    DayToSell,      
    Profit,      
    BuyExpense,      
    SellExpense,      
    SecurityType,      
    PRODUCT,      
 TaxPercentage,      
    TaxAmount      
FROM TaxCalculation order by TranDateSale;      
  end    
  else    
  begin    
 WITH ProfitData AS (      
    SELECT      
        c.NAME as ClientName,      
        c.TRADECODE as TradeCode,      
        RTRIM(curlocation) + RTRIM(tradecode) as ClientCode,      
        c.CLIENTID,      
        c.PAN_GIR as Pan,      
        t.BuyQty,      
        t.BuyValue,      
        t.TranDateBuy,      
        t.TranDateSale,      
        t.TransID,      
		t.Type,      
       -- CASE WHEN t.Type='Long Term' THEN 'LG' ELSE 'SG' END as Type,      
        T.SaleQty,      
        T.SaleValue,      
        T.ISIN,      
        t.Security,      
        t.DayToSell,      
        t.Profit,      
        t.BuyExpense,      
        t.SellExpense,      
        max(s.SeriesCode) as SecurityType,      
        MAX(s.PRODUCT) as PRODUCT -- Using MAX to pick one PRODUCT name      
    FROM Tax_Profit_Details_Cash_TDS t      
    INNER JOIN Client c ON c.CLIENTID = t.Clientid      
    LEFT JOIN Sauda s ON s.CLIENTID = t.Clientid AND t.TranDateSale = s.TRANDATE AND t.Security = s.SECURITY      
    WHERE TranDateSale between @frmdate and @todate      
    GROUP BY      
        c.NAME,      
        c.TRADECODE,      
        RTRIM(curlocation) + RTRIM(tradecode),      
        c.CLIENTID,      
        t.BuyQty,      
        t.BuyValue,      
        t.TranDateBuy,      
        t.TranDateSale,      
        t.TransID,      
       -- CASE WHEN t.Type='Long Term' THEN 'LG' ELSE 'SG' END,      
        T.SaleQty,      
        T.SaleValue,      
        T.ISIN,      
        t.Security,      
        t.DayToSell,      
        t.Profit,      
        t.BuyExpense,      
        t.SellExpense,      
        c.PAN_GIR,      
  t.Type      
    
    
    
    
  union     
    
    SELECT    
    c.NAME as ClientName,      
    c.TRADECODE as TradeCode,       
    RTRIM(curlocation) + RTRIM(tradecode) as ClientCode,    
    c.CLIENTID,    
    c.PAN_GIR as Pan,        
    tb.purchase_qty as BuyQty,    
    tb.purchase_amount as BuyValue,    
    case when tb.purchase_date is null then tb.Selldate else tb.purchase_date end  as purchase_date,    
    tb.Selldate,    
    0 as TransID,    
    case when POH<=1 then 'Short Term' else 'Long Term'  end as Type,    
    tb.sale_qty as SaleQty,    
    tb.sales_amount as SaleValue,    
    tb.ISIN as ISIN,    
    tb.Security as Security,    
    tb.POH as DayToSell,      
    tb.Profit as Profit,      
    tb.BuyExpense as BuyExpense,      
    tb.SaleExpense as SellExpense,      
    max(s.SeriesCode) as SecurityType,      
    MAX(s.PRODUCT) as PRODUCT    
    from #temp tb  
    INNER JOIN Client c ON c.CLIENTID = tb.Clientid    
    LEFT JOIN Sauda s ON s.CLIENTID = tb.Clientid AND tb.Selldate= s.TRANDATE AND tb.Security = s.SECURITY    
    WHERE tb.Selldate  between @frmdate and @todate 
    GROUP BY    
  c.NAME,    
  c.TRADECODE,    
  RTRIM(curlocation) + RTRIM(tradecode),    
  c.CLIENTID,    
  C.PAN_GIR,    
  tb.Selldate,    
  -- tb.TransID,    
  tb.sale_qty,    
  tb.sales_amount,    
  tb.ISIN,    
  tb.Security,    
  tb.SaleExpense,    
  tb.Profit  ,  
  tb.POH,  
  tb.purchase_amount,  
  tb.purchase_qty,  
  tb.purchase_date    
  ,tb.BuyExpense
),      
      
TaxRates AS (      
    -- Select the appropriate tax rates based on the gain type and gain range      
    SELECT      
        Gain_type,      
        YTD_gain_range,      
        finyear,      
        Tax_rate,      
        Surcharge,      
        Cess,      
        Total_tax_rate      
    FROM TaxratesMaster      
    WHERE finyear = @FiscYear      
),      
      
TaxCalculation AS (      
    -- Calculate tax on profit      
    SELECT      
        pd.*,      
      CASE      
            WHEN pd.Type = 'Short Term' THEN      
    CASE      
     WHEN pd.Profit <= 5000000 THEN      
                        (SELECT Total_tax_rate  FROM TaxratesMaster        
      WHERE Gain_type = 'Short Term' AND YTD_gain_range = '<= 50,00,000' AND finyear = @FiscYear)       
                    ELSE      
       (SELECT Total_tax_rate  FROM TaxratesMaster        
      WHERE Gain_type = 'Short Term' AND YTD_gain_range = '> 50,00,000' AND finyear = @FiscYear)      
    END      
            WHEN pd.Type = 'Long Term' THEN      
                CASE      
                    WHEN pd.Profit <= 5000000 THEN      
                        (SELECT Total_tax_rate  FROM TaxratesMaster        
      WHERE Gain_type = 'Long Term' AND YTD_gain_range = '<= 50,00,000' AND finyear = @FiscYear)      
                    ELSE      
                        (SELECT Total_tax_rate  FROM TaxratesMaster        
      WHERE Gain_type = 'Long Term' AND YTD_gain_range = '> 50,00,000' AND finyear = @FiscYear)      
                END      
        END AS TaxPercentage,      
        CASE      
            WHEN pd.Type = 'Short Term' THEN      
    CASE      
     WHEN pd.Profit <= 5000000 THEN      
                        pd.Profit *  (SELECT Total_tax_rate / 100 FROM TaxratesMaster        
      WHERE Gain_type = 'Short Term' AND YTD_gain_range = '<= 50,00,000' AND finyear =@FiscYear)       
                    ELSE      
      pd.Profit * (SELECT Total_tax_rate / 100 FROM TaxratesMaster        
      WHERE Gain_type = 'Short Term' AND YTD_gain_range = '> 50,00,000' AND finyear = @FiscYear)      
    END      
            WHEN pd.Type = 'Long Term' THEN      
                CASE      
                    WHEN pd.Profit <= 5000000 THEN      
                        pd.Profit *(SELECT Total_tax_rate / 100 FROM TaxratesMaster        
      WHERE Gain_type = 'Long Term' AND YTD_gain_range = '<= 50,00,000' AND finyear = @FiscYear)      
                    ELSE      
                        pd.Profit *(SELECT Total_tax_rate / 100 FROM TaxratesMaster        
      WHERE Gain_type = 'Long Term' AND YTD_gain_range = '> 50,00,000' AND finyear =@FiscYear)      
                END      
        END AS TaxAmount      
    FROM ProfitData pd      
)      
      
-- Finally, you can select the columns you need from the TaxCalculation CTE      
SELECT      
    ClientName,      
    TradeCode,      
    ClientCode,      
    CLIENTID,      
    Pan,      
    BuyQty,      
    BuyValue,      
    TranDateBuy,      
    TranDateSale,      
    TransID,      
    CASE WHEN Type='Long Term' THEN 'LG' ELSE 'SG' END as Type,      
    SaleQty,      
    SaleValue,      
    ISIN,      
    Security,      
    DayToSell,      
    Profit,      
    BuyExpense,      
    SellExpense,      
    SecurityType,      
    PRODUCT,      
 TaxPercentage,      
    TaxAmount      
FROM TaxCalculation order by TranDateSale;      
  end    
      
  
  
  drop table #temp  
End 