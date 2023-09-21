create PROCEDURE SpUpdateProcessedRequest
    @todate VARCHAR(20),
    @clientid INT,
    @result INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @rowCount INT;

    SELECT @rowCount = COUNT(*) 
    FROM TDS_JournalEntry 
    WHERE TransSaleDate = @todate AND Client = @clientid;

    IF @rowCount > 0
    BEGIN
        UPDATE TDS_JournalEntry 
        SET Lock = 1 
        WHERE TransSaleDate = @todate AND Client = @clientid;

        SET @result = 1; -- Success
    END
    ELSE
    BEGIN
        SET @result = 0; -- No update occurred
    END

    
END;
