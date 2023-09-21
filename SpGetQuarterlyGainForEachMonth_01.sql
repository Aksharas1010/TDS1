--exec SpGetQuarterlyGainForEachMonth '2023-08-08'
create PROCEDURE SpGetQuarterlyGainForEachMonth
    @todate VARCHAR(20)
AS
BEGIN                                                                                                                                        
    SET NOCOUNT ON;

 DECLARE @input_date DATE = CAST(@todate AS DATE);
DECLARE @quarter INT = (DATEPART(QUARTER, @input_date));
DECLARE @quarter_start_date DATE = DATEFROMPARTS(YEAR(@input_date), (3 * @quarter - 2), 1);
DECLARE @quarter_end_date DATE = DATEADD(DAY, -1, DATEADD(MONTH, 3, @quarter_start_date));

    -- Generate a table of months within the quarter
    DECLARE @QuarterMonths TABLE (MonthStart DATE);
    INSERT INTO @QuarterMonths (MonthStart)
    VALUES
        (@quarter_start_date),
        (DATEADD(MONTH, 1, @quarter_start_date)),
        (DATEADD(MONTH, 2, @quarter_start_date));

    WITH FirstItemOfMonth AS (
        SELECT
        DATEADD(MONTH, DATEDIFF(MONTH, 0, TransSaleDate), 0) AS MonthStart,
        MIN(TransSaleDate) AS FirstItemDate
    FROM
        Tax_Daily_Profit_Summary
    WHERE
        TransSaleDate >= @quarter_start_date AND TransSaleDate <= @quarter_end_date
    GROUP BY
        DATEADD(MONTH, DATEDIFF(MONTH, 0, TransSaleDate), 0)
    ),
    LastItemOfMonth AS (
       SELECT
        DATEADD(MONTH, DATEDIFF(MONTH, 0, TransSaleDate), 0) AS MonthStart,
        MAX(TransSaleDate) AS LastItemDate
    FROM
        Tax_Daily_Profit_Summary
    WHERE
        TransSaleDate >= @quarter_start_date AND TransSaleDate <= @quarter_end_date
    GROUP BY
        DATEADD(MONTH, DATEDIFF(MONTH, 0, TransSaleDate), 0)
    ),
    MonthSum AS (
     SELECT
        DATEADD(MONTH, DATEDIFF(MONTH, 0, TransSaleDate), 0) AS MonthStart,
        SUM(DailySetOffST) AS MonthProfitSumST,
		 SUM(DailySetOffLT) AS MonthProfitSumLT
    FROM
        Tax_Daily_Profit_Summary
    WHERE
        TransSaleDate >= @quarter_start_date AND TransSaleDate <= @quarter_end_date
    GROUP BY
        DATEADD(MONTH, DATEDIFF(MONTH, 0, TransSaleDate), 0)
    ),
    TaxSum AS (
       SELECT
        DATEADD(MONTH, DATEDIFF(MONTH, 0, TransSaleDate), 0) AS MonthStart,
        SUM(ST_Tax) AS MonthTaxSumST,
		SUM(LT_Tax) AS MonthTaxSumLT
    FROM
        Tax_Daily_Profit_Summary
    WHERE
        TransSaleDate >= @quarter_start_date AND TransSaleDate <= @quarter_end_date
    GROUP BY
        DATEADD(MONTH, DATEDIFF(MONTH, 0, TransSaleDate), 0)
    )
 SELECT
    DATEPART(QUARTER, QM.MonthStart) AS Quarter,
    DATEPART(MONTH, QM.MonthStart) AS MonthNumber,
    DATENAME(MONTH, QM.MonthStart) AS MonthName,
    ISNULL((SELECT OpeningBalST FROM Tax_Daily_Profit_Summary WHERE TransSaleDate = F.FirstItemDate), 0.00) AS OpeningBalanceST,
    ISNULL((SELECT ClosingBalST FROM Tax_Daily_Profit_Summary WHERE TransSaleDate = L.LastItemDate), 0.00) AS ClosingBalanceST,
    COALESCE(M.MonthProfitSumST, 0.00) AS STGain,
    COALESCE(T.MonthTaxSumST, 0.00) AS STTaxSum,
    ISNULL((SELECT OpeningBalLT FROM Tax_Daily_Profit_Summary WHERE TransSaleDate = F.FirstItemDate), 0.00) AS OpeningBalanceLT,
    ISNULL((SELECT ClosingBalLT FROM Tax_Daily_Profit_Summary WHERE TransSaleDate = L.LastItemDate), 0.00) AS ClosingBalanceLT,
    COALESCE((SELECT MAX(ST_TaxPercentage) FROM Tax_Daily_Profit_Summary WHERE TransSaleDate = L.LastItemDate), 0.00) AS STTaxPercentage,
    COALESCE((SELECT MAX(LT_TaxPercentage) FROM Tax_Daily_Profit_Summary WHERE TransSaleDate = L.LastItemDate), 0.00) AS LTTaxPercentage,
    COALESCE(M.MonthProfitSumLT, 0.00) AS LTGain,
    COALESCE(T.MonthTaxSumLT, 0.00) AS LTTaxSum
FROM
    @QuarterMonths QM
LEFT JOIN
    FirstItemOfMonth F ON QM.MonthStart = F.MonthStart
LEFT JOIN
    LastItemOfMonth L ON QM.MonthStart = L.MonthStart
LEFT JOIN
    MonthSum M ON QM.MonthStart = M.MonthStart
LEFT JOIN
    TaxSum T ON QM.MonthStart = T.MonthStart
ORDER BY
    Quarter, MonthNumber;

END;
