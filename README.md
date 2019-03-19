Engine database: Parquet
Essential libraries:
	pyArrow 	
	dask		
	sqlAlchemy	
	panda		

configuration:
	1- define organizations
	2- define organization business entities
	3- define organization attributes
	4- define organization source connections
	5- define business entity attributs
	6- define business entity sources
	7- do mapping between source query columns and business entity attributes
	8- define execution plan
workflow:
	1- extract data from source system
	2- load it to staging area
	3- melt data, to be in long formate (variable and value) instead of wide formate
	4- compare the the melted data with the existing
	5- extract difference 
	6- move old data to archive table
	7- push new/changed data to current active table
	8- apply rules on each data value based on the pre-configred execution plane
	9- generate result in new table with data quality issues for each business entities
	
Most of the operations above works in parallel using Dask.
