
--EXEC SpGetTDSReturnsReportData '2023-09-01','2023-09-30','2023-2024'
create Procedure SpGetTDSReturnsReportData  
 @frm_date VARCHAR(MAX),
 @to_date VARCHAR(MAX) , 
 @FiscYear varchar(12)  
As                                                           
begin
SELECT
    c.PAN_GIR,
    c.NAME AS ClientName,
    t1.Adjusted_Short_Term,
    t1.Adjusted_Long_Term,
    CASE WHEN t1.Adjusted_Short_Term > 0 THEN t1.Adjusted_Short_Term ELSE 0 END AS ShortTermGain,
    CASE WHEN t1.Adjusted_Long_Term > 0 THEN t1.Adjusted_Long_Term ELSE 0 END AS LongTermGain,
    t1.*
FROM
    Tax_Daily_Profit_Summary t1
INNER JOIN (
    SELECT
        Client,
        DATEFROMPARTS(YEAR(TransSaleDate), MONTH(TransSaleDate), 1) AS MonthYear,
        MAX(TransSaleDate) AS last_date_entry
    FROM
        Tax_Daily_Profit_Summary
    WHERE
        TransSaleDate >= @frm_date
        AND TransSaleDate <= @to_date
    GROUP BY
        Client, DATEFROMPARTS(YEAR(TransSaleDate), MONTH(TransSaleDate), 1)
) t2 ON t1.Client = t2.Client AND t1.TransSaleDate = t2.last_date_entry
INNER JOIN client c ON t1.Client = c.CLIENTID
WHERE
    t1.Adjusted_Short_Term > 0 OR t1.Adjusted_Long_Term > 0
end

