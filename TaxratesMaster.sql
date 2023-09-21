
CREATE TABLE TaxratesMaster (
    Gain_type VARCHAR(20),
    YTD_gain_range VARCHAR(50),
	  finyear VARCHAR(10),
    Tax_rate DECIMAL(5, 2),
    Surcharge DECIMAL(5, 2),
    Cess DECIMAL(5, 2),
    Total_tax_rate DECIMAL(5, 2)
);

INSERT INTO TaxratesMaster (Gain_type,YTD_gain_range,finyear, Tax_rate, Surcharge, Cess, Total_tax_rate)
VALUES
    ('Short Term', '> 50,00,000','2023-2024', 15.00, 15.00, 4.00, 17.94),
    ('Short Term', '<= 50,00,000','2023-2024', 15.00, 0.00, 4.00, 15.60);

INSERT INTO TaxratesMaster (Gain_type,YTD_gain_range,finyear, Tax_rate, Surcharge, Cess, Total_tax_rate)
VALUES
    ('Long Term', '> 50,00,000','2023-2024', 10.00, 15.00, 4.00, 11.96),
    ('Long Term', '<= 50,00,000','2023-2024', 10.00, 0.00, 4.00, 10.40);

SELECT * FROM TaxratesMaster;
--drop table TaxratesMaster