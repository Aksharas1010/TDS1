alter PROCEDURE DeleteTaxRecordsByDateRange
    @fromDate DATE,
    @toDate DATE,
    @clientId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Success INT = 1; -- Default to success

    BEGIN TRY
        BEGIN TRANSACTION;
        DELETE FROM Tax_Daily_Profit_Summary
        WHERE Client = @clientId
          AND TransSaleDate = @toDate;
        DELETE FROM TDS_JournalEntry
        WHERE Client = @clientId
          AND TransSaleDate = @toDate;
		  
        DELETE FROM tax_Profit_Buynotfound_TDS
        WHERE Trandate BETWEEN @fromDate AND @toDate;

        COMMIT;
    END TRY
    BEGIN CATCH
        -- If an error occurs, set @Success to 0 and roll back the transaction
        SET @Success = 0;
        ROLLBACK;
    END CATCH;

    SELECT @Success AS Success;
END;
