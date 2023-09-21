 -- exec SpGetTDSClientDetails '2023-07-11'
 --select CAST(GetDate()-1 as Date)
create Procedure SpGetTDSClientDetails  
-- @fromdate VARCHAR(20),
@todate varchar(20),
@clientid int= null
As                                                       
begin                                                                                                                                        
  Set NoCount On   
   --select s.* from Sauda s
	 --inner join Client c on c.clientid=s.clientid
	 --where   c.Type='NRO' and CAST(s.TRANDATE as Date)=CAST(GetDate()-1 as Date) 
	 if @clientid is  null
	 begin	
		  select distinct c.CLIENTID from Sauda s with(nolock)
		 inner join Client c with(nolock) on c.clientid=s.clientid
		 where   c.Type='NROCM' and CAST(s.TRANDATE as Date) =@todate and s.BuySell='S'	
	end
	else
	begin
	print @clientid
		select distinct c.CLIENTID,s.SECURITY from Sauda s with(nolock)
		 inner join Client c with(nolock) on c.clientid=s.clientid
		 where   c.Type='NROCM' and CAST(s.TRANDATE as Date)= @todate and s.BuySell='S' and s.clientid=@clientid
	end
End 
