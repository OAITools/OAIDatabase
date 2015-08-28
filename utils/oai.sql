-- *******************************************************************
-- Initialize empty OAI database, 
-- http://www.fda.gov/Drugs/InformationOnDrugs/ucm079750.htm
-- @retrieved 2015-28-8
-- @author Jason Alan Fries
-- @email jfries [at] stanford.edu
-- *******************************************************************

-- Application Documents (AppDoc): 
-- Document addresses or URLs to letters, labels, reviews, 
-- Consumer Information Sheets, FDA Talk Papers, and other types.

CREATE TABLE AppDoc (
		AppDocID INTEGER NOT NULL,
		ApplNo VARCHAR(6) NOT NULL,
		SeqNo VARCHAR(4) NOT NULL,
		DocType VARCHAR(50) NOT NULL,
		DocTitle VARCHAR(100),
		DocURL VARCHAR(200),
		DocDate DATE,
		ActionType VARCHAR(10) NOT NULL,
		DuplicateCounter INTEGER,
		PRIMARY KEY(AppDocID) );
		
-- Application Document Type Lookup (AppDocType_Lookup): 
-- Type of document that is linked, which relates to the AppDoc table.