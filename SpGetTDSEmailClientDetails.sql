
create Procedure SpGetTDSEmailClientDetails  
@clientid int=null
As                                                       
begin                                                                                                                                        
  Set NoCount On   
    select ClientEmail = Isnull(C.RESEMAIL,''),Location = C.CurLocation,Type=C.Type ,LocEmail=L.Email,ClientName=C.Name ,RTRIM(C.curlocation) + RTRIM(C.tradecode) as TradeCode,
	 PDFPassword=(Case When C.Type in ('CL','NRO','NROCM','NRE') Then Substring(C.PAN_GIR,1,4) + Substring(Replace(Convert(varchar, isnull(C.DOB,''), 4),'.',''),1,4) else C.PAN_GIR End )        
	 from Client C
	 inner join Location L on L.Location=C.CurLocation
	where Clientid=@clientid
End 
