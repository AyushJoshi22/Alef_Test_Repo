CREATE TABLE [dbo].[Customer_Feedback_Data] (

	[Feedback_ID] bigint NULL, 
	[Sale_ID] varchar(8000) NULL, 
	[Customer_Name] varchar(8000) NULL, 
	[Feedback_Rating] bigint NULL, 
	[Feedback_Comments] varchar(8000) NULL, 
	[Feedback_Date] date NULL
);