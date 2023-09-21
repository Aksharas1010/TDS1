CREATE PROCEDURE InsertAuthorizedUser
@EmpCode NVARCHAR(MAX),
@Role NVARCHAR(MAX),
@Active INT
AS
BEGIN
    INSERT INTO tbl_AuthorizedUser (EmpCode, Role, Active)
    VALUES (@EmpCode, @Role, @Active)
END
