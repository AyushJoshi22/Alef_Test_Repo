CREATE TABLE [dbo].[Retail_Sales_Data] (

	[Sale_ID] varchar(8000) NULL, 
	[Product_Name] varchar(8000) NULL, 
	[Category] varchar(8000) NULL, 
	[Quantity_Sold] bigint NULL, 
	[Price_per_Unit] float NULL, 
	[Sale_Date] date NULL, 
	[Payment_Method] varchar(8000) NULL, 
	[Store_Location] varchar(8000) NULL
);