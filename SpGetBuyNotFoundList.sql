--exec SpGetBuyNotFoundList '2023-08-08'
create Procedure SpGetBuyNotFoundList
@date date
As                                                       
begin                                                                                                                                        
  Set NoCount On   
    
	select t.*,l.EMAIL as LocationEmail,l.LOCATION,c.TRADECODE as TradeCode ,Rtrim(c.curlocation)+Rtrim(c.tradecode) as ClientCode from tax_Profit_Buynotfound_TDS t
	Inner join CLient c on c.CLIENTID=t.Clientid
	inner join Location l on l.LOCATION=c.CURLOCATION where t.Trandate=@date
End 
