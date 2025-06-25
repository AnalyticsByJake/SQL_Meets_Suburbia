-- DATASET USED: https://www.kaggle.com/datasets/tonygordonjr/zillow-real-estate-data
-- ------------
-- Import OG data tables before running code below
-- Cleaned data tables can be used with analysis queries directly

-- =========================== ZILLOW DATABASE CREATION AND CLEANING ===========================
-- _____________________________________________________________________________________________



-- SECTIONS:
	-- A.) Delete Duplicate Rows
	-- B.) Create Primary Keys
	-- C.) Create Foreign Keys
	-- D.) Change Data Types
	-- E.) Create Indexes


-- TABLES:
	-- 1.) listing_mortgage_info
	-- 2.) listing_nearby_homes
	-- 3.) listing_price_history
	-- 4.) listing_schools_info
	-- 5.) listing_subtype
	-- 6.) listing_tax_info
	-- 7.) property_listings


-- ALIASES
	-- lmi = listing_mortgage_info
	-- lnh = listing_nearby_homes
	-- lph = listing_price_history
	-- lsi = listing_schools_info
	-- ls = listing_subtype
	-- lti = listing_tax_info
	-- pl = property_listings



-- ========================================= SECTION A =========================================
-- =================================== DELETE DUPLICATE ROWS ===================================
-- _____________________________________________________________________________________________
-- The following code check for duplicates and, if necessary, drop the duplicates. 
-- Comments for first query can be applied to all following.


-- LISTING_MORTGAGE_INFO - DROP DUPLICATES
-- _______________________________________

WITH ranked_duplicates AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY zpid, bucketType, rate, rateSource, lastUpdatedTimestamp, lastUpdatedDate
               ORDER BY zpid
           ) AS rn -- attributes 1 to first occurence of each group
    FROM listing_mortgage_info
)
DELETE 
FROM listing_mortgage_info
WHERE (zpid, bucketType, rate, rateSource, lastUpdatedTimestamp, lastUpdatedDate) 
	IN (
	-- selects duplicate occurences
    SELECT zpid, bucketType, rate, rateSource, lastUpdatedTimestamp, lastUpdatedDate
    FROM ranked_duplicates
    WHERE rn > 1 -- keeps only first occurence
);


-- LISTING_NEARBY_HOMES - DROP DUPLICATES
-- ______________________________________

WITH ranked_duplicates AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY zpid, zpidComp, addressComp, cityComp, stateComp, zipComp, priceComp,
                            homeTypeComp, homeStatusComp, livingAreaValueComp, livingAreaUnitsComp,
                            lotAreaValueComp, lotAreaUnitsComp, lastUpdated
               ORDER BY zpid
           ) AS rn
    FROM listing_nearby_homes
)
DELETE 
FROM listing_nearby_homes
WHERE (zpid, zpidComp, addressComp, cityComp, stateComp, zipComp, priceComp,
       homeTypeComp, homeStatusComp, livingAreaValueComp, livingAreaUnitsComp,
       lotAreaValueComp, lotAreaUnitsComp, lastUpdated) 
	IN (
	SELECT zpid, zpidComp, addressComp, cityComp, stateComp, zipComp, priceComp,
			homeTypeComp, homeStatusComp, livingAreaValueComp, livingAreaUnitsComp,
			lotAreaValueComp, lotAreaUnitsComp, lastUpdated
    FROM ranked_duplicates
    WHERE rn > 1
);


-- LISTING_PRICE_HISTORY - DROP DUPLICATES
-- _______________________________________

WITH ranked_duplicates AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY zpid, `event`, price, pricePerSquareFoot, priceChangeRate,
                            dateOfEvent, `source`, postingIsRental, lastUpdated
               ORDER BY zpid
           ) AS rn
    FROM listing_price_history
)
DELETE 
FROM listing_price_history
WHERE (zpid, `event`, price, pricePerSquareFoot, priceChangeRate,
       dateOfEvent, `source`, postingIsRental, lastUpdated) 
	IN (
    SELECT zpid, `event`, price, pricePerSquareFoot, priceChangeRate,
			dateOfEvent, `source`, postingIsRental, lastUpdated
    FROM ranked_duplicates
    WHERE rn > 1
);


-- LISTING_SCHOOLS_INFO - DROP DUPLICATES
-- ______________________________________

WITH ranked_duplicates AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY zpid, schoolName, schoolRating, `type`, gradeLevel,
                            grades, distanceFromListing, link, lastUpdated
               ORDER BY zpid
           ) AS rn
    FROM listing_schools_info
)
DELETE 
FROM listing_schools_info
WHERE (zpid, schoolName, schoolRating, `type`, gradeLevel,
       grades, distanceFromListing, link, lastUpdated) 
	IN (
    SELECT zpid, schoolName, schoolRating, `type`, gradeLevel,
           grades, distanceFromListing, link, lastUpdated
    FROM ranked_duplicates
    WHERE rn > 1
);


-- LISTING_SUBTYPE - DROP DUPLICATES
-- _________________________________

WITH ranked_duplicates AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY zpid, is_FSBA, is_comingSoon, is_newHome, is_pending,
                            is_forAuction, is_foreclosure, is_bankOwned, is_openHouse,
                            is_FSBO, lastUpdated
               ORDER BY zpid
           ) AS rn
    FROM listing_subtype
)
DELETE 
FROM listing_subtype
WHERE (zpid, is_FSBA, is_comingSoon, is_newHome, is_pending,
       is_forAuction, is_foreclosure, is_bankOwned, is_openHouse,
       is_FSBO, lastUpdated) 
	IN (
    SELECT zpid, is_FSBA, is_comingSoon, is_newHome, is_pending,
           is_forAuction, is_foreclosure, is_bankOwned, is_openHouse,
           is_FSBO, lastUpdated
    FROM ranked_duplicates
    WHERE rn > 1
);


-- LISTING_TAX_INFO - DROP DUPLICATES
-- __________________________________

WITH ranked_duplicates AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY zpid, lastUpdatedTimestamp, lastUpdatedDate, valueIncreaseRate,
                            taxIncreaseRate, taxPaid, propertyValue
               ORDER BY zpid
           ) AS rn
    FROM listing_tax_info
)
DELETE 
FROM listing_tax_info
WHERE (zpid, lastUpdatedTimestamp, lastUpdatedDate, valueIncreaseRate,
       taxIncreaseRate, taxPaid, propertyValue) 
	IN (
    SELECT zpid, lastUpdatedTimestamp, lastUpdatedDate, valueIncreaseRate,
           taxIncreaseRate, taxPaid, propertyValue
    FROM ranked_duplicates
    WHERE rn > 1
);


-- PROPERTY_LISTINGS - DROP DUPLICATES
-- ___________________________________

WITH ranked_duplicates AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY zpid, price, homeStatus, homeType, datePosted, streetAddress,
                            city, state, zipcode, county, yearBuilt, livingArea, livingAreaUnits,
                            rentZestimate, bathrooms, bedrooms, pageViewCount, favoriteCount, propertyTaxRate,
                            timeOnZillow, dateSold, url, lastUpdated
               ORDER BY zpid
           ) AS rn
    FROM property_listings
)
DELETE 
FROM property_listings
WHERE (zpid, price, homeStatus, homeType, datePosted, streetAddress,
       city, state, zipcode, county, yearBuilt, livingArea, livingAreaUnits,
       rentZestimate, bathrooms, bedrooms, pageViewCount, favoriteCount, propertyTaxRate,
       timeOnZillow, dateSold, url, lastUpdated) 
	IN (
    SELECT zpid, price, homeStatus, homeType, datePosted, streetAddress,
           city, state, zipcode, county, yearBuilt, livingArea, livingAreaUnits,
           rentZestimate, bathrooms, bedrooms, pageViewCount, favoriteCount, propertyTaxRate,
           timeOnZillow, dateSold, url, lastUpdated
    FROM ranked_duplicates
    WHERE rn > 1
);



-- ========================================== SECTION B ==========================================
-- ===================================== CREATE PRIMARY KEYS =====================================
-- _______________________________________________________________________________________________
-- The following code sets up unique primary keys for each table


-- LISTING_MORTGAGE_INFO - PRIMARY KEY (lmi_id)
-- ____________________________________________

ALTER TABLE listing_mortgage_info
ADD COLUMN lmi_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY;


-- LISTING_NEARBY_HOMES - PRIMARY KEY (lnh_id)
-- ___________________________________________

ALTER TABLE listing_nearby_homes
ADD COLUMN lnh_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY;


-- LISTING_PRICE_HISTORY - PRIMARY KEY (lph_id)
-- ____________________________________________

ALTER TABLE listing_price_history
ADD COLUMN lph_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY;


-- LISTING_SCHOOLS_INFO - PRIMARY KEY (lsi_id)
-- ___________________________________________

ALTER TABLE listing_schools_info
ADD COLUMN lsi_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY;


-- LISTING_SUBTYPE - PRIMARY KEY (ls_id)
-- _____________________________________

ALTER TABLE listing_subtype
ADD COLUMN ls_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY;


-- LISTING_TAX_INFO - PRIMARY KEY (lti_id)
-- _______________________________________

ALTER TABLE listing_tax_info
ADD COLUMN lti_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY;


-- LISTING_PROPERTY_LISTINGS - PRIMARY KEY (pl_id)
-- _______________________________________________

ALTER TABLE property_listings
ADD COLUMN pl_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY;





-- ========================================== SECTION C ==========================================
-- ===================================== CREATE FOREIGN KEYS =====================================
-- _______________________________________________________________________________________________
-- The following code creates foreign keys between various tables for easier navigation


-- LISTING_MORTGAGE_INFO - FOREIGN KEYS (x2)
-- _________________________________________


-- Adds listing_subtype(ls_id) to listing_mortgage_info

ALTER TABLE listing_mortgage_info
ADD COLUMN ls_id INT;

UPDATE listing_mortgage_info lmi
JOIN listing_subtype ls
	ON lmi.zpid = ls.zpid
SET lmi.ls_id = ls.ls_id
-- uncomment below line to update in batches
-- WHERE lmi.lmi_id BETWEEN 1 AND 5000 
;

ALTER TABLE listing_mortgage_info
ADD CONSTRAINT fk_ls_id
	FOREIGN KEY (ls_id) REFERENCES listing_subtype(ls_id)
    ON DELETE CASCADE ON UPDATE CASCADE;


-- Adds listing_tax_info(lti_id) to listing_mortgage_info

ALTER TABLE listing_mortgage_info
ADD COLUMN lti_id INT;

UPDATE listing_mortgage_info lmi
JOIN listing_tax_info lti
	ON lmi.zpid = lti.zpid
SET lmi.lti_id = lti.lti_id
-- uncomment below line to update in batches
-- WHERE lmi.lmi_id BETWEEN 1 AND 5000 
;

ALTER TABLE listing_mortgage_info
ADD CONSTRAINT fk_lti_id
	FOREIGN KEY (lti_id) REFERENCES listing_tax_info(lti_id)
	ON DELETE CASCADE ON UPDATE CASCADE;



-- LISTING_NEARBY_HOMES - FOREIGN KEYS (x1)
-- ________________________________________


-- Adds listing_school_info(lsi_id) to listing_nearby_homes

ALTER TABLE listing_nearby_homes
ADD COLUMN lsi_id INT;

UPDATE listing_nearby_homes lnh
JOIN listing_schools_info lsi
	ON lnh.zpid = lsi.zpid
SET lnh.lsi_id = lsi.lsi_id
-- uncomment below line to update in batches
-- WHERE lnh.lnh_id BETWEEN 1 AND 5000 
;

ALTER TABLE listing_nearby_homes
ADD CONSTRAINT fk_lsi_id
	FOREIGN KEY (lsi_id) REFERENCES listing_schools_info(lsi_id)
	ON DELETE CASCADE ON UPDATE CASCADE;



-- LISTING_PRICE_HISTORY - FOREIGN KEYS (x2)
-- _________________________________________


-- Adds property_listings(pl_id) to listing_price_history

ALTER TABLE listing_price_history
ADD COLUMN pl_id INT;

UPDATE listing_price_history lph
JOIN property_listings pl
	ON lph.zpid = pl.zpid
SET lph.pl_id = pl.pl_id
-- uncomment below line to update in batches
-- WHERE lph.lph_id BETWEEN 1 AND 5000
;

ALTER TABLE listing_price_history 
ADD CONSTRAINT fk_pl_id
	FOREIGN KEY (pl_id) REFERENCES property_listings(pl_id)
	ON DELETE CASCADE ON UPDATE CASCADE;


-- Adds property_tax_info(lti_id) to listing_price_history

ALTER TABLE listing_price_history
ADD COLUMN lti_id INT;

UPDATE listing_price_history lph
JOIN listing_tax_info lti
	ON lph.zpid = lti.zpid
SET lph.lti_id = lti.lti_id
-- uncomment below line to update in batches
-- WHERE lph.lph_id BETWEEN 1 AND 5000
;

ALTER TABLE listing_price_history
ADD CONSTRAINT fk_lti_id_lph
	FOREIGN KEY (lti_id) REFERENCES listing_tax_info(lti_id)
	ON DELETE CASCADE ON UPDATE CASCADE;



-- LISTING_SCHOOLS_INFO - FOREIGN KEYS (x1)
-- ________________________________________


-- Adds listing_nearby_homes(lnh_id) to listing_schools_info

ALTER TABLE listing_schools_info
ADD COLUMN lnh_id INT;

UPDATE listing_schools_info lsi
JOIN listing_nearby_homes lnh
	ON lsi.zpid = lnh.zpid
SET lsi.lnh_id = lnh.lnh_id
-- uncomment below line to update in batches
-- WHERE lsi.lsi_id BETWEEN 1 AND 5000
;

ALTER TABLE listing_schools_info
ADD CONSTRAINT fk_lnh_id
	FOREIGN KEY (lnh_id) REFERENCES listing_nearby_homes(lnh_id)
    ON DELETE CASCADE ON UPDATE CASCADE;



-- LISTING_SUBTYPE- FOREIGN KEYS (x2)
-- __________________________________


-- Adds listing_mortgage_info(lmi_id) to listing_subtype

ALTER TABLE listing_subtype
ADD COLUMN lmi_id INT;

UPDATE listing_subtype ls
JOIN listing_mortgage_info lmi
	ON ls.zpid = lmi.zpid
SET ls.lmi_id = lmi.lmi_id
-- uncomment below line to update in batches
-- WHERE ls.ls_id BETWEEN 1 AND 5000
;

ALTER TABLE listing_subtype
ADD CONSTRAINT fk_lmi_id
	FOREIGN KEY (lmi_id) REFERENCES listing_mortgage_info(lmi_id)
	ON DELETE CASCADE ON UPDATE CASCADE;


-- Adds property_listings(pl_id) to listing_subtype

ALTER TABLE listing_subtype
ADD COLUMN pl_id INT;

UPDATE listing_subtype ls
JOIN property_listings pl
	ON ls.zpid = pl.zpid
SET ls.pl_id = pl.pl_id
-- uncomment below line to update in batches
-- WHERE ls.ls_id BETWEEN 1 AND 5000
;

ALTER TABLE listing_subtype
ADD CONSTRAINT fk_pl_id_ls
	FOREIGN KEY (pl_id) REFERENCES property_listings(pl_id)
	ON DELETE CASCADE ON UPDATE CASCADE;



-- LISTING_TAX_INFO - FOREIGN KEYS (x2)
-- ____________________________________


-- Adds listing_mortgage_info(lmi_id) to listing_tax_info

ALTER TABLE listing_tax_info
ADD COLUMN lmi_id INT;

UPDATE listing_tax_info lti
JOIN listing_mortgage_info lmi
	ON lti.zpid = lmi.zpid
SET lti.lmi_id = lmi.lmi_id
-- uncomment below line to update in batches
-- WHERE lti.lti_id BETWEEN 1 and 5000
;

ALTER TABLE listing_tax_info
ADD CONSTRAINT fk_lmi_id_lti
	FOREIGN KEY (lmi_id) REFERENCES listing_mortgage_info(lmi_id)
	ON DELETE CASCADE ON UPDATE CASCADE;


-- Adds listing_price-history(lph_id) to listing_tax_info

ALTER TABLE listing_tax_info
ADD COLUMN lph_id INT;

UPDATE listing_tax_info lti
JOIN listing_price_history lph
	ON lti.zpid = lph.zpid
SET lti.lph_id = lph.lph_id
-- uncomment below line to update in batches
-- WHERE lti.lti_id BETWEEN 1 and 5000;
;

ALTER TABLE listing_tax_info
ADD CONSTRAINT fk_lph_id
	FOREIGN KEY (lph_id) REFERENCES listing_price_history(lph_id)
	ON DELETE CASCADE ON UPDATE CASCADE;



-- PROPERTY_LISTINGS - FOREIGN KEYS (x3)
-- _____________________________________



-- Adds listing_schools_info(lsi_id) to propety_listings

ALTER TABLE property_listings 
ADD COLUMN lsi_id INT;

UPDATE property_listings pl
JOIN listing_schools_info lsi
	ON pl.zpid = lsi.zpid
SET pl.lsi_id = lsi.lsi_id
-- uncomment below line to update in batches
-- WHERE pl.pl_id BETWEEN 1 and 5000
;

ALTER TABLE property_listings
ADD CONSTRAINT fk_lsi_id_pl
	FOREIGN KEY (lsi_id) REFERENCES listing_schools_info(lsi_id)
	ON DELETE CASCADE ON UPDATE CASCADE;
    
    
-- Adds listing_price_history(lph_id) to property_listings

ALTER TABLE property_listings
ADD COLUMN lph_id INT;

UPDATE property_listings pl
JOIN listing_price_history lph
	ON pl.zpid = lph.zpid
SET pl.lph_id = lph.lph_id
-- uncomment below line to update in batches
-- WHERE pl.pl_id BETWEEN 1 and 5000
;

ALTER TABLE property_listings
ADD CONSTRAINT fk_lph_id
	FOREIGN KEY (lph_id) REFERENCES listing_price_history(lph_id)
	ON DELETE CASCADE ON UPDATE CASCADE;


-- Adds listing_subtype(ls_id) to property_listings

ALTER TABLE property_listings
ADD COLUMN ls_id INT;

UPDATE property_listings pl
JOIN listing_subtype ls
	ON pl.zpid = ls.zpid
SET pl.ls_id = ls.ls_id
-- uncomment below line to update in batches
-- WHERE pl.pl_id BETWEEN 1 and 5000
;

ALTER TABLE property_listings
ADD CONSTRAINT fk_ls_id_pl
	FOREIGN KEY (ls_id) REFERENCES listing_subtype(ls_id)
	ON DELETE CASCADE ON UPDATE CASCADE;





-- ========================================= SECTION D =========================================
-- ===================================== CHANGE DATA TYPES =====================================
-- _____________________________________________________________________________________________
-- Corrects data types for each table (the original data type largely categorizes them as TEXT)
-- To improve efficiency, all "MODIFY" statements can be combined at the end of each table.
-- The repetitive form was chosen for visibility only



-- CHANGE DATA TYPES FOR LISTING_MORTGAGE_INFO 
-- ___________________________________________


-- Makes lastUpdatedTimestamp a DATETIME data type by converting to timestamp format,then converting to DATETIME data type

UPDATE listing_mortgage_info
SET lastUpdatedTimestamp = NULL
WHERE lastUpdatedTimestamp IN ('0', '', 'NULL')
	OR lastUpdatedTimestamp IS NULL;

UPDATE listing_mortgage_info
SET lastUpdatedTimestamp = STR_TO_DATE(LEFT(lastUpdatedTimestamp, 19), '%Y-%m-%d %H:%i:%s')
WHERE lastUpdatedTimestamp IS NOT NULL;

ALTER TABLE listing_mortgage_info
MODIFY lastUpdatedTimestamp DATETIME;


-- Makes lastUpdatedDate a DATE data type

UPDATE listing_mortgage_info
SET lastUpdatedDate = NULL
WHERE lastUpdatedDate IN ('0', '', 'NULL')
	OR lastUpdatedDate IS NULL;

UPDATE listing_mortgage_info
SET lastUpdatedDate = STR_TO_DATE(lastUpdatedDate, '%Y-%m-%d')
WHERE STR_TO_DATE(lastUpdatedDate, '%Y-%m-%d') IS NOT NULL;

ALTER TABLE listing_mortgage_info
MODIFY lastUpdatedDate DATE;



-- CHANGE DATA TYPES FOR LISTING_NEARBY_HOMES 
-- __________________________________________


-- Makes lastUpdated a DATETIME data type by converting to timestamp format,then converting to DATETIME data type

UPDATE listing_nearby_homes
SET lastUpdated = NULL
WHERE lastUpdated IN ('0', '', 'NULL')
	OR lastUpdated IS NULL;

UPDATE listing_nearby_homes
SET lastUpdated = STR_TO_DATE(LEFT(lastUpdated, 19), '%Y-%m-%d %H:%i:%s')
WHERE lastUpdated IS NOT NULL;

ALTER TABLE listing_nearby_homes
MODIFY lastUpdated DATETIME;



-- CHANGE DATA TYPES FOR LISTING_PRICE_HISTORY
-- ___________________________________________


-- Makes price an INT data type

UPDATE listing_price_history
SET price = NULL
WHERE price IN ('0', '', 'NULL')
	OR price IS NULL;

ALTER TABLE listing_price_history
MODIFY price INT;


-- Makes pricePerSquareFoot an INT data type
 
UPDATE listing_price_history
SET pricePerSquareFoot = NULL
WHERE pricePerSquareFoot IN ('0', '', 'NULL')
	OR pricePerSquareFoot IS NULL;

ALTER TABLE listing_price_history
MODIFY COLUMN pricePerSquareFoot INT; 


-- Makes dateOfEvent a DATE data type

UPDATE listing_price_history
SET dateOfEvent = NULL
WHERE dateOfEvent IN ('0', '', 'NULL')
	OR dateOfEvent IS NULL;
    
UPDATE listing_price_history
SET dateOfEvent = STR_TO_DATE(dateOfEvent, '%Y-%m-%d')
WHERE STR_TO_DATE(dateOfEvent, '%Y-%m-%d') IS NOT NULL;

ALTER TABLE listing_price_history
MODIFY dateOfEvent DATE;


-- Makes postingIsRental a BOOLEAN data type (first checks for null or abnormal values)

SELECT 
	COUNT(postingIsRental) AS error_values
FROM listing_price_history
WHERE TRIM(postingIsRental) NOT IN ('true', 'false', '', 'NULL')
	AND postingIsRental IS NOT NULL;
    
UPDATE listing_price_history -- changes 'true' values to 1 
SET postingIsRental = 1
WHERE postingIsRental = 'true';

UPDATE listing_price_history -- changes 'false' values to 0 
SET postingIsRental = 0
WHERE postingIsRental = 'false';

ALTER TABLE listing_price_history
MODIFY postingIsRental BOOLEAN;


-- Makes lastUpdated a DATETIME data type by converting to timestamp format,then converting to DATETIME data type

UPDATE listing_price_history
SET lastUpdated = NULL
WHERE lastUpdated IN ('0', '', 'NULL')
	OR lastUpdated IS NULL;

UPDATE listing_price_history
SET lastUpdated = STR_TO_DATE(LEFT(lastUpdated, 19), '%Y-%m-%d %H:%i:%s');

ALTER TABLE listing_price_history
MODIFY lastUpdated DATETIME;



-- CHANGE DATA TYPES FOR LISTING_SCHOOLS_INFO
-- __________________________________________


-- Makes schoolRating an INT data type

UPDATE listing_schools_info
SET schoolRating = NULL
WHERE schoolRating IN ('0', '', 'NULL')
	OR schoolRating IS NULL;

ALTER TABLE listing_schools_info
MODIFY schoolRating INT;


-- Makes empty values in grades NULL

UPDATE listing_schools_info
SET grades = NULL
WHERE grades IN ('0', '', 'NULL')
	OR grades IS NULL;


-- Makes lastUpdated a DATETIME data type by converting to timestamp format,then converting to DATETIME data type

UPDATE listing_schools_info
SET lastUpdated = NULL
WHERE lastUpdated IN ('0', '', 'NULL')
	OR lastUpdated IS NULL;

UPDATE listing_schools_info
SET lastUpdated = STR_TO_DATE(LEFT(lastUpdated, 19), '%Y-%m-%d %H:%i:%s');

ALTER TABLE listing_schools_info
MODIFY lastUpdated DATETIME;



-- CHANGE DATA TYPES FOR LISTING_SUBTYPE 
-- _____________________________________


-- change is_FSBA to BOOLEAN data type

UPDATE listing_subtype
SET is_FSBA = NULL
WHERE is_FSBA IN ('0', '', 'NULL')
	OR is_FSBA IS NULL;
    
SELECT DISTINCT is_FSBA
FROM listing_subtype;

UPDATE listing_subtype
SET is_FSBA = 
	CASE
		WHEN is_FSBA = 'true' THEN 1
        WHEN is_FSBA = 'false' THEN 0
        ELSE NULL
	END;

ALTER TABLE listing_subtype
MODIFY is_FSBA BOOLEAN;


-- change is_comingSoon to BOOLEAN data type

UPDATE listing_subtype
SET is_comingSoon = NULL
WHERE is_comingSoon IN ('0', '', 'NULL')
	OR is_comingSoon IS NULL;

UPDATE listing_subtype
SET is_comingSoon = 
	CASE
		WHEN is_comingSoon = 'true' THEN 1
        WHEN is_comingSoon = 'false' THEN 0
        ELSE NULL
	END;

ALTER TABLE listing_subtype
MODIFY is_comingSoon BOOLEAN;


-- change is_newHome to BOOLEAN data type

UPDATE listing_subtype
SET is_newHome = NULL
WHERE is_newHome IN ('0', '', 'NULL')
	OR is_newHome IS NULL;

UPDATE listing_subtype
SET is_newHome = 
	CASE
		WHEN is_newHome = 'true' THEN 1
        WHEN is_newHome = 'false' THEN 0
        ELSE NULL
	END;

ALTER TABLE listing_subtype
MODIFY is_newHome BOOLEAN;


-- change is_pending to BOOLEAN data type

UPDATE listing_subtype
SET is_pending = NULL
WHERE is_pending IN ('0', '', 'NULL')
	OR is_pending IS NULL;

UPDATE listing_subtype
SET is_pending = 
	CASE
		WHEN is_pending = 'true' THEN 1
        WHEN is_pending = 'false' THEN 0
        ELSE NULL
	END;

ALTER TABLE listing_subtype
MODIFY is_pending BOOLEAN;


-- change is_forAuction to BOOLEAN data type

UPDATE listing_subtype
SET is_forAuction = NULL
WHERE is_forAuction IN ('0', '', 'NULL')
	OR is_forAuction IS NULL;

UPDATE listing_subtype
SET is_forAuction = 
	CASE
		WHEN is_forAuction = 'true' THEN 1
        WHEN is_forAuction = 'false' THEN 0
        ELSE NULL
	END;

ALTER TABLE listing_subtype
MODIFY is_forAuction BOOLEAN;


-- change is_foreclosure to BOOLEAN data type

UPDATE listing_subtype
SET is_foreclosure = NULL
WHERE is_foreclosure IN ('0', '', 'NULL')
	OR is_foreclosure IS NULL;

UPDATE listing_subtype
SET is_foreclosure = 
	CASE
		WHEN is_foreclosure = 'true' THEN 1
        WHEN is_foreclosure = 'false' THEN 0
        ELSE NULL
	END;

ALTER TABLE listing_subtype
MODIFY is_foreclosure BOOLEAN;


-- change is_bankOwned to BOOLEAN data type

UPDATE listing_subtype
SET is_bankOwned = NULL
WHERE is_bankOwned IN ('0', '', 'NULL')
	OR is_bankOwned IS NULL;

UPDATE listing_subtype
SET is_bankOwned = 
	CASE
		WHEN is_bankOwned = 'true' THEN 1
        WHEN is_bankOwned = 'false' THEN 0
        ELSE NULL
	END;

ALTER TABLE listing_subtype
MODIFY is_bankOwned BOOLEAN;


-- change is_openHouse to BOOLEAN data type

UPDATE listing_subtype
SET is_openHouse = NULL
WHERE is_openHouse IN ('0', '', 'NULL')
	OR is_openHouse IS NULL;

UPDATE listing_subtype
SET is_openHouse = 
	CASE
		WHEN is_openHouse = 'true' THEN 1
        WHEN is_openHouse = 'false' THEN 0
        ELSE NULL
	END;

ALTER TABLE listing_subtype
MODIFY is_openHouse BOOLEAN;


-- change is_FSBO to BOOLEAN data type

UPDATE listing_subtype
SET is_FSBO = NULL
WHERE is_FSBO IN ('0', '', 'NULL')
	OR is_FSBO IS NULL;

UPDATE listing_subtype
SET is_FSBO = 
	CASE
		WHEN is_FSBO = 'true' THEN 1
        WHEN is_FSBO = 'false' THEN 0
        ELSE NULL
	END;

ALTER TABLE listing_subtype
MODIFY is_FSBO BOOLEAN;


-- Makes lastUpdated a DATETIME data type by converting to timestamp format,then converting to DATETIME data type

UPDATE listing_subtype
SET lastUpdated = NULL
WHERE lastUpdated IN ('0', '', 'NULL')
	OR lastUpdated IS NULL;

UPDATE listing_subtype
SET lastUpdated = STR_TO_DATE(LEFT(lastUpdated, 19), '%Y-%m-%d %H:%i:%s');

ALTER TABLE listing_subtype
MODIFY lastUpdated DATETIME;



-- CHANGE DATA TYPES FOR LISTING_TAX_INFO
-- ______________________________________


-- Makes lastUpdatedTimestamp a DATETIME data type by converting to timestamp format,then converting to DATETIME data type

UPDATE listing_tax_info
SET lastUpdatedTimestamp = NULL
WHERE lastUpdatedTimestamp IN ('0', '', 'NULL')
	OR lastUpdatedTimestamp IS NULL;

UPDATE listing_tax_info
SET lastUpdatedTimestamp = STR_TO_DATE(LEFT(lastUpdatedTimestamp, 19), '%Y-%m-%d %H:%i:%s');

ALTER TABLE listing_tax_info
MODIFY lastUpdatedTimestamp DATETIME;


-- Makes lastUpdatedDate a DATE data type

UPDATE listing_tax_info
SET lastUpdatedDate = NULL
WHERE lastUpdatedDate IN ('0', '', 'NULL')
	OR lastUpdatedDate IS NULL;

UPDATE listing_tax_info
SET lastUpdatedDate = STR_TO_DATE(lastUpdatedDate, '%Y-%m-%d')
WHERE lastUpdatedDate IS NOT NULL;

ALTER TABLE listing_tax_info
MODIFY lastUpdatedDate DATE;


-- Makes propertyValue an INT data type

UPDATE listing_tax_info
SET propertyValue = NULL
WHERE propertyValue IN ('0', '', 'NULL')
	OR propertyValue IS NULL;

ALTER TABLE listing_tax_info
MODIFY propertyValue INT;



-- CHANGE DATA TYPES FOR PROPERTY_LISTINGS
-- _______________________________________


-- Makes price an INT data type
 
UPDATE property_listings
SET price = 0
WHERE price = '';

ALTER TABLE property_listings
MODIFY COLUMN price INT; 


-- Makes state a VARCHAR(2) data type

ALTER TABLE property_listings
MODIFY COLUMN state VARCHAR(2); 


-- Makes city a VARCHAR(20) data type

ALTER TABLE property_listings
MODIFY COLUMN city VARCHAR(20); 


-- Makes homeStatus a VARCHAR(40) data type

ALTER TABLE property_listings
MODIFY COLUMN homeStatus VARCHAR(40); 


-- Makes datePosted a DATE data type

UPDATE property_listings
SET datePosted = NULL
WHERE datePosted IN ('0', '', 'NULL')
   OR datePosted IS NULL;
    
UPDATE property_listings
SET datePosted = STR_TO_DATE(datePosted, '%Y-%m-%d')
WHERE STR_TO_DATE(datePosted, '%Y-%m-%d') IS NOT NULL;

ALTER TABLE property_listings
MODIFY datePosted DATE;


-- Makes yearBuilt an INT data type

UPDATE property_listings
SET yearBuilt = NULL
WHERE yearBuilt IN ('0', '', 'NULL')
	OR yearBuilt IS NULL;

ALTER TABLE property_listings
MODIFY yearBuilt INT;


-- Makes livingArea an INT data type

UPDATE property_listings
SET livingArea = NULL
WHERE livingArea IN ('0', '', 'NULL')
	OR livingArea IS NULL;
    
ALTER TABLE property_listings
MODIFY livingArea INT;


-- Makes livingAreaUnits an INT data type

UPDATE property_listings
SET livingAreaUnits = NULL
WHERE livingAreaUnits IN ('0', '', 'NULL')
	OR livingAreaUnits IS NULL;
    
ALTER TABLE property_listings
MODIFY livingAreaUnits INT;


-- Makes bedrooms an INT data type

UPDATE property_listings
SET bedrooms = NULL
WHERE bedrooms IN ('0', '', 'NULL')
	OR bedrooms IS NULL;
    
ALTER TABLE property_listings
MODIFY bedrooms INT;


-- Converts timeOnZillow to hours (thus converting 'days') and an INT data type

UPDATE property_listings
SET timeOnZillow = NULL
WHERE timeOnZillow IN ('0', '', 'NULL')
	OR timeOnZillow IS NULL;

UPDATE property_listings
SET timeOnZillow = CASE
    WHEN timeOnZillow LIKE '%hours%' THEN CAST(SUBSTRING_INDEX(timeOnZillow, ' ', 1) AS UNSIGNED)
    WHEN timeOnZillow LIKE '%days%' THEN CAST(SUBSTRING_INDEX(timeOnZillow, ' ', 1) AS UNSIGNED) * 24
    ELSE NULL
END
WHERE timeOnZillow IS NOT NULL;

ALTER TABLE property_listings
MODIFY timeOnZillow INT;


-- Makes datePosted a DATE data type

UPDATE property_listings
SET dateSold = NULL
WHERE dateSold IN ('0', '', 'NULL')
	OR dateSold IS NULL;

UPDATE property_listings
SET dateSold = STR_TO_DATE(dateSold, '%Y-%m-%d');

ALTER TABLE property_listings
MODIFY dateSold DATE;


-- Makes lastUpdated a DATETIME data type by converting to timestamp format,then converting to DATETIME data type

UPDATE property_listings
SET lastUpdated = NULL
WHERE lastUpdated IN ('0', '', 'NULL')
	OR lastUpdated IS NULL;

UPDATE property_listings
SET lastUpdated = STR_TO_DATE(LEFT(lastUpdated, 19), '%Y-%m-%d %H:%i:%s');

ALTER TABLE property_listings
MODIFY lastUpdated DATETIME;





-- ========================================= SECTION E ========================================
-- ====================================== CREATE INDEXES ======================================
-- ____________________________________________________________________________________________


-- To check existing indexes

SHOW INDEX
FROM
	-- uncomment the table you would like to check
	-- listing_mortgage_info
    -- listing_nearby_homes
    -- listing_price_history
    -- listing_schools_info
    -- listing_subtype
    -- listing_tax_info
    property_listings
;



-- LISTING_NEARBY_HOMES - CREATE INDEXES
-- _____________________________________


-- indexes lnh.zpid

CREATE INDEX idx_lnh_zpid 
	ON listing_nearby_homes(zpid);
    
-- indexes lnh.zipComp
    
CREATE INDEX idx_lnh_zipComp 
	ON listing_nearby_homes(zipComp);




-- LISTING_PRICE_HISTORY - CREATE INDEXES
-- ______________________________________


-- indexes lph.zpid

CREATE INDEX idx_lph_zpid 
	ON listing_price_history(zpid);
    
-- indexes lph.pricePerSquareFoot

CREATE INDEX idx_lph_pricePerSquareFoot 
	ON listing_price_history(pricePerSquareFoot);
    
-- indexes lph.dateOfEvent

CREATE INDEX idx_lph_dateOfEvent 
	ON listing_price_history(dateOfEvent);




-- LISTING_SCHOOLS_INFO - CREATE INDEXES
-- _____________________________________


-- indexes lsi.zpid

CREATE INDEX idx_lsi_zpid 
	ON listing_schools_info(zpid);
    
-- indexes lsi.schoolRating

CREATE INDEX idx_lsi_schoolRating 
	ON listing_schools_info(schoolRating);
    
-- indexes lsi.distanceFromListing

CREATE INDEX idx_lsi_distanceFromListing 
	ON listing_schools_info(distanceFromListing);




-- LISTING_SUBTYPE - CREATE INDEXES
-- ________________________________


-- indexes ls.zpid

CREATE INDEX idx_ls_zpid 
	ON listing_subtype(zpid);

-- indexes ls.is_foreclosure

CREATE INDEX idx_ls_foreclosure 
	ON listing_subtype(is_foreclosure);




-- LISTING_TAX_INFO - CREATE INDEXES
-- _________________________________


-- indexes lti.zpid

CREATE INDEX idx_lti_zpid 
	ON listing_tax_info(zpid);
    
-- indexes lti.propertyValue

CREATE INDEX idx_lti_propertyValue 
	ON listing_tax_info(propertyValue);





-- PROPERTY_LISTINGS - CREATE INDEXES
-- __________________________________


-- indexes pl.zpid

CREATE INDEX idx_pl_zpid 
	ON property_listings(zpid);
    
-- indexes pl.state and pl.city

CREATE INDEX idx_pl_state_city 
	ON property_listings(state, city);

-- indexes pl.zipcode

CREATE INDEX idx_pl_zipcode 
	ON property_listings(zipcode);
    
-- indexes pl.price

CREATE INDEX idx_pl_price 
	ON property_listings(price);

-- indexes pl.dateSold

CREATE INDEX idx_pl_dateSold 
	ON property_listings(dateSold);

-- indexes pl.homeStatus

CREATE INDEX idx_pl_homeStatus 
	ON property_listings(homeStatus);
