
--select * from tbl_AuthorizedUser
CREATE TABLE [dbo].[tbl_AuthorizedUser](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[EmpCode][varchar](30) NULL,
	[Role][varchar](30) NULL,
	[Active][int]null
)
update tbl_AuthorizedUser set Active=1
insert into tbl_AuthorizedUser values('10023','Accounts',1)
insert into tbl_AuthorizedUser values('10024','Operations',1)
insert into tbl_AuthorizedUser values('10025','Accounts',1)
insert into tbl_AuthorizedUser values('10026','Accounts',1)



