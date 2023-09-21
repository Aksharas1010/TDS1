CREATE TABLE tblTransactionsLock (
    TransID INT IDENTITY(1,1) PRIMARY KEY,
    Year INT,
    Month INT,
    Active int
);
--drop table tblTransactionsLock
update tblTransactionsLock set Active=0
insert into tblTransactionsLock values('2023',08,1)