# Farmers-market-dataset

Farmers Market Dataset


1. Introduction and Overview
 	The Farmers Market dataset is an extensive dataset containing valuable data regarding the location, season, days and time of operation, social media handles, products on sale, mode of accepted payment to name a few. The main aim of this project is to help users find the famers market in their locality which are either nut free, vegan, organic, gluten free or markets selling prepared food. This would save the shoppers a lot of time as they can choose a market easily depending on the choice of produce/items they are particularly looking for. 
	
	The shoppers with specific food restrictions can now easily enjoy a great variety of fresh, local produce in seasons when its flavor and nutrition are at its peak. The prices of organic fruits and veggies are usually a lot cheaper and would help the buyer.

                    
2. Initial assessment of the dataset

	This dataset like many other datasets requires a fair bit of cleaning to be used for further analysis. 

	The address of each farmers market comprises of a street, city, county, state and zip. There are many records that are missing either a part or a few parts which makes the address incomplete. 
	
 	The operating seasons are divided into Season1Date, Season2Date, Season3Date and Season4Date. These columns have a from and to date which needs to be extracted and formatted into separate columns making it easy for further analysis.

	The season time column per season has cell values with day and time. This column needs to be split into operating days columns with operating times as cell values for further analysis.


3. Data Cleaning methods and process
	OpenRefine has been extensively used for this project for the data cleaning phase. All the white spaces have been collapsed and trimmed. Incorrect data types have been inspected and converted into their correct data type. 

Common Transformations:
The following columns have been transformed to number.
Zip
x co-ordinates
Y co-ordinates

The following columns have been transformed to date format 
1. Season1Date
2. Season2Date
3. Season3Date
4. Season4Date
5. UpdateTime


Facets and Clustering:
Text facets were created for the following columns
1. MarketName
2. City
3. County
4. State

Clustering for performed on the following columns
1. MarketName
2. City
3. County

Column splitting
Major part of the data cleaning for this project for the data to be fit for further analysis involves column splitting. The available seasons and time are extremely vital data so splitting and formatting was required. The following columns were split into multiple columns 
1. Season1Date
2. Season1Time
3. Season2Date
4. Season2Time
5. Season3Date
6. Season3Time
7. Season4Date
8. Season4Time
