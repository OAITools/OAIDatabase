-- *******************************************************************
-- Initialize empty OAI database, 
-- http://www.fda.gov/Drugs/InformationOnDrugs/ucm079750.htm
-- @retrieved 2015-28-8
-- @author Jason Alan Fries
-- @email jfries [at] stanford.edu
-- *******************************************************************

--
-- Variable Categories and Subcategories (Categories): 
--
CREATE TABLE Categories (
		VarName VARCHAR(20) NOT NULL,
		CatType INTEGER NOT NULL,
		CatName TEXT NOT NULL,
		PRIMARY KEY(VarName) );
--	
-- DataSet origin and description (DataSets): 
--
CREATE TABLE DataSets (
		VarName VARCHAR(20) NOT NULL,
		DataSet VARCHAR(128) NOT NULL,
		CollectForm TEXT,
		Comment TEXT,
		PRIMARY KEY(VarName) );
		
		