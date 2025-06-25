

-- =============================== DATABASE SETUP FOR ZILLOW DATASETS ===============================
-- _______________________________________________________________________________________




-- use this code to access MySQL prompt in Terminal/Command Prompt
-- mysql -u ENTER_USER_NAME -p 



-- enter this code to make the database
DROP DATABASE IF EXISTS zillow_housing_db;
CREATE DATABASE zillow_housing_db;
USE zillow_housing_db;



-- use this code to create the table


CREATE TABLE `listing_mortgage_info` (
  `zpid` int,
  `bucketType` text,
  `rate` double,
  `rateSource` text,
  `lastUpdatedTimestamp` text,
  `lastUpdatedDate` text
);


CREATE TABLE `listing_nearby_homes` (
  `zpid` int,
  `zpidComp` int,
  `addressComp` text,
  `cityComp` text,
  `stateComp` text,
  `zipComp` int,
  `priceComp` int,
  `homeTypeComp` text,
  `homeStatusComp` text,
  `livingAreaValueComp` int,
  `livingAreaUnitsComp` text,
  `lotAreaValueComp` int,
  `lotAreaUnitsComp` text,
  `lastUpdated` text
);


CREATE TABLE `listing_price_history` (
  `zpid` int,
  `event` text,
  `price` text,
  `pricePerSquareFoot` text,
  `priceChangeRate` double,
  `dateOfEvent` text,
  `source` text,
  `postingIsRental` text,
  `lastUpdated` text
);


CREATE TABLE `listing_schools_info` (
  `zpid` int,
  `schoolName` text,
  `schoolRating` text,
  `type` text,
  `gradeLevel` text,
  `grades` text,
  `distanceFromListing` double,
  `link` text,
  `lastUpdated` text
);


CREATE TABLE `listing_subtype` (
  `zpid` int,
  `is_FSBA` text,
  `is_comingSoon` text,
  `is_newHome` text,
  `is_pending` text,
  `is_forAuction` text,
  `is_foreclosure` text,
  `is_bankOwned` text,
  `is_openHouse` text,
  `is_FSBO` text,
  `lastUpdated` text
);


CREATE TABLE `listing_tax_info` (
  `zpid` int,
  `lastUpdatedTimestamp` text,
  `lastUpdatedDate` text,
  `valueIncreaseRate` int,
  `taxIncreaseRate` int,
  `taxPaid` double,
  `propertyValue` text
);


CREATE TABLE `property_listings` (
  `zpid` int,
  `price` int,
  `homeStatus` text,
  `homeType` text,
  `datePosted` text,
  `streetAddress` text,
  `city` text,
  `state` text,
  `zipcode` int,
  `county` text,
  `yearBuilt` text,
  `livingArea` text,
  `livingAreaUnits` text,
  `rentZestimate` int,
  `bathrooms` int,
  `bedrooms` text,
  `pageViewCount` int,
  `favoriteCount` int,
  `propertyTaxRate` double,
  `timeOnZillow` text,
  `dateSold` text,
  `url` text,
  `lastUpdated` text
);



-- exit from mysql prompt and then enter this code into Terminal/Command prompt to access db (uncommented):
-- mysql --local-infile=1 -u ENTER_USER_NAME -p zillow_housing_db



-- enter this code to directly input .csv file


LOAD DATA LOCAL INFILE 'ENTER_PATHWAY_TO.csv'
INTO TABLE listing_mortgage_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
  `zpid`,
  `bucketType`,
  `rate`,
  `rateSource`,
  `lastUpdatedTimestamp`,
  `lastUpdatedDate`
);


LOAD DATA LOCAL INFILE 'ENTER_PATHWAY_TO.csv'
INTO TABLE listing_nearby_homes
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
  `zpid`,
  `zpidComp`,
  `addressComp`,
  `cityComp`,
  `stateComp`,
  `zipComp`,
  `priceComp`,
  `homeTypeComp`,
  `homeStatusComp`,
  `livingAreaValueComp`,
  `livingAreaUnitsComp`,
  `lotAreaValueComp`,
  `lotAreaUnitsComp`,
  `lastUpdated` 
);


LOAD DATA LOCAL INFILE 'ENTER_PATHWAY_TO.csv'
INTO TABLE listing_price_history
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
  `zpid`,
  `event`,
  `price`,
  `pricePerSquareFoot`,
  `priceChangeRate`,
  `dateOfEvent`,
  `source`,
  `postingIsRental`,
  `lastUpdated`
);


LOAD DATA LOCAL INFILE 'ENTER_PATHWAY_TO.csv'
INTO TABLE listing_schools_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
  `zpid`,
  `schoolName`,
  `schoolRating`,
  `type`,
  `gradeLevel`,
  `grades`,
  `distanceFromListing`,
  `link`,
  `lastUpdated`
);


LOAD DATA LOCAL INFILE 'ENTER_PATHWAY_TO.csv'
INTO TABLE listing_subtype
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
  `zpid`,
  `is_FSBA`,
  `is_comingSoon`,
  `is_newHome`,
  `is_pending`,
  `is_forAuction`,
  `is_foreclosure`,
  `is_bankOwned`,
  `is_openHouse`,
  `is_FSBO`,
  `lastUpdated`
);


LOAD DATA LOCAL INFILE 'ENTER_PATHWAY_TO.csv'
INTO TABLE listing_tax_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
  `zpid`,
  `lastUpdatedTimestamp`,
  `lastUpdatedDate`,
  `valueIncreaseRate`,
  `taxIncreaseRate`,
  `taxPaid`,
  `propertyValue` 
);


LOAD DATA LOCAL INFILE 'ENTER_PATHWAY_TO.csv'
INTO TABLE property_listings
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
  `zpid`,
  `price`,
  `homeStatus`,
  `homeType`,
  `datePosted`,
  `streetAddress`,
  `city`,
  `state`,
  `zipcode`,
  `county`,
  `yearBuilt`,
  `livingArea`,
  `livingAreaUnits`,
  `rentZestimate`,
  `bathrooms`,
  `bedrooms`,
  `pageViewCount`,
  `favoriteCount`,
  `propertyTaxRate`,
  `timeOnZillow`,
  `dateSold`,
  `url`,
  `lastUpdated` 
);
