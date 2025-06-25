-- DATASET USED: https://www.kaggle.com/datasets/tonygordonjr/zillow-real-estate-data
-- ------------


-- =============================== ANALYSIS OF ZILLOW DATA ===============================
-- _______________________________________________________________________________________


-- SECTIONS:
-- A.) Property Score Calculator
-- B.) Price and Commonality by State/City Queries
-- C.) Factors of Property Price Queries
-- D.) Property Popularity Queries
-- E.) Properties Over Time Queries
-- F.) Property Status Queries


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



-- ===================================== SECTION A =====================================
-- ============================= PROPERTY SCORE CALCULATOR =============================
-- _____________________________________________________________________________________
-- This section creates a composite score for each property using 5 key metrics:
-- (1) Listing price, (2) Zipcode vs State pricing gap, (3) Engagement rate (favorites/views),
-- (4) Distance to nearby schools, and (5) Average school rating.
-- Useful for visualizing "best value" listings on a map or ranking homes for buyers.
		-- can be unblocked to seach by state, city, zipcode, price, school rating and school distance 
		-- adjust SELECT, WHERE, GROUP BY and HAVING clauses (instructions in comments below)
		-- Query results also include listing ID, listing price, max count of favorites 
				-- and page visits, school rating and distance
		-- results can also be adjusted to only select homes still on the market



WITH avg_zipcode_price AS (
	SELECT
        state,
        zipcode,
        AVG(price) AS zipPrice
	FROM property_listings
    WHERE price > 0
    GROUP by state, zipcode
), -- selects avg price by zipcode
avg_state_price AS (
	SELECT 
		state,
        AVG(price) AS statePrice
	FROM property_listings
    WHERE price > 0
    GROUP BY state
),  -- selects avg price by state
zip_price AS (
	SELECT
		pl.zpid,
        (azp.zipPrice - asp.statePrice) AS compZipPrice
	FROM property_listings pl
    JOIN avg_zipcode_price azp
		ON pl.state = azp.state
			AND pl.zipcode = azp.zipcode
	JOIN avg_state_price asp
		ON azp.state = asp.state
), -- selects avg zip price minus avg state price by listing (for Score)
avg_school_agg AS (
	SELECT
        zpid,
		AVG(distanceFromListing) AS avgSchoolDist,
        AVG(schoolRating) AS avgSchoolRating
	FROM listing_schools_info
    WHERE distanceFromListing IS NOT NULL
		AND schoolRating IS NOT NULL
        AND schoolRating > 0
	GROUP BY zpid
), -- selects avg distance and avg school rating from school by listing
max_fav_view AS (
	SELECT
		zpid,
        MAX(favoriteCount) AS maxFav,
        MAX(pageViewCount) AS maxView
	FROM property_listings
    WHERE favoriteCount IS NOT NULL
		AND pageViewCount IS NOT NULL
    GROUP BY zpid
), -- selects max fav count and max page visits by listing
fav_to_view AS (
	SELECT
		mfv.zpid,
        AVG(maxFav / NULLIF(maxView, 0)) AS favToView,
        AVG(price) AS avgPrice
	FROM max_fav_view mfv
	JOIN property_listings pl
		ON mfv.zpid = pl.zpid
	WHERE price IS NOT NULL
		AND price > 0
    GROUP BY zpid
), -- selects max fav count divided by max page visits by listing and avg price
agg_values AS (
	SELECT
		pl.zpid,
        asa.avgSchoolRating,
        asa.avgSchoolDist,
        ftv.favToView,
        ftv.avgPrice,
        zp.compZipPrice  -- aggregates all agg variables into one table
    FROM property_listings pl
    JOIN avg_school_agg asa
		ON pl.zpid = asa.zpid
	JOIN fav_to_view ftv
		ON asa.zpid = ftv.zpid
	JOIN zip_price zp
		ON ftv.zpid = zp.zpid -- connects all cts with property_listings
), -- selects all relevant agg variables
ntile_values AS (
	SELECT
		zpid,
        avgSchoolRating,
        avgSchoolDist,
        favToView,
        avgPrice,
        compZipPrice,
		NTILE(5) OVER(ORDER BY avgPrice) AS tilePrice,
        NTILE(5) OVER(ORDER BY compZipPrice) AS tileZip,
        NTILE(5) OVER(ORDER BY favToView) AS tileView,
        NTILE(5) OVER(ORDER BY avgSchoolDist DESC) AS tileSchoolDist,
        NTILE(5) OVER(ORDER BY avgSchoolRating) AS tileSchoolRate
	FROM agg_values
), -- selects the percentile (in 5) for all agg values
custom_score AS (
	SELECT
		zpid,
		avgSchoolRating,
        avgSchoolDist,
        favToView,
        avgPrice,
        compZipPrice,
        ROUND((tilePrice + tileZip + tileView + tileSchoolDist + tileSchoolRate) / 5, 1) AS customScore 
	FROM ntile_values
) -- finds the avg score of all metrics
SELECT 
	ROW_NUMBER() OVER (
		ORDER BY AVG(cs.customScore) DESC) AS `Rank`, -- calculates rank from Score
    ROUND(AVG(cs.customScore), 1) AS Score, -- calculates avg score to one decimal
    pl.zpid AS listingId,
    -- uncomment code to specify state, city and zipcode
    -- pl.state,
    -- pl.city, 
    -- pl.zipcode,
    ROUND(AVG(pl.price), 2) AS listingPrice,
    MAX(pl.favoriteCount) AS favCount,
    MAX(pl.pageViewCount) AS pageVisits,
    ROUND(AVG(cs.avgSchoolRating), 1) AS schoolRating,
	ROUND(AVG(cs.avgSchoolDist), 1) AS schoolDistance
FROM custom_score cs
JOIN property_listings pl
	ON cs.zpid = pl.zpid
WHERE pl.price > 0
	-- uncomment following lines to specify state, city and zipcode, school rating and school distance
	-- AND pl.state = 'AZ'
    -- AND pl.city = 'Phoenix'
    -- AND pl.zipcode = 85001
    -- AND cs.avgSchoolRating >= 8.0
    -- AND cs.avgSchoolDist <= 4.0
	-- uncomment this final line of blocked out code to only select houses still on the market
	AND dateSold IS NULL
GROUP BY pl.zpid
	-- uncomment following lines if using state, city or zipcode
	-- , pl.state
    -- , pl.city
    -- , pl.zipcode
-- uncomment following lines to specify price range, state and city
-- HAVING AVG(pl.price) BETWEEN 150000 AND 700000 
ORDER BY `Rank`
-- uncomment to limit results
-- LIMIT 100
;





-- ===================================== SECTION A =====================================
-- ======================== PRICE AND COMMONALITY BY STATE/CITY ========================
-- _____________________________________________________________________________________



-- ------->> 1.) WHAT ARE THE MOST COMMON HOME TYPES IN EVERY STATE?


-- selects perc of home types in each state
	-- useful for determining oversaturated and underdeveloped housing types by state
    -- comparative bar graphs would be useful on state-by-state basis
    -- could be fruitful for dashboards, stacked bar chart
 
 SELECT 
	state, 
    homeType, 
    COUNT(homeType) as numOfHomes, 
	ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY state), 2) AS inStatePerc -- calculates perc of homeType by state
 FROM property_listings
 GROUP BY state, homeType 
 ORDER BY state, inStatePerc DESC; -- orders by state and perc of in state sold
 
 
 
 
 
 -- ------->> 2.) WHAT STATES ARE THE MOST EXPENSIVE?


-- selects states by average listing price
	-- useful for identifying high and low cost states
    -- helpful when compared with cost of living or investment risk
    -- could be useful turned into horizontal bar chart (limited states, e.g. top 10)
 
WITH price_by_state AS (
	SELECT 
		state,
		ROUND(AVG(price), 0) AS avgPriceUSD
	FROM property_listings
	WHERE price > 0
	GROUP BY state -- groups by state
	ORDER BY avgPriceUSD DESC -- orders by avg price DESC
) -- selects the average price of listing by state
SELECT 
	state,
    RANK() OVER(ORDER BY avgPriceUSD DESC) AS stateRanking, -- ranks avg price by state 
    avgPriceUSD
FROM price_by_state;





-- ------->> 3.) WHICH CITIES AND STATES HAVE ABOVE AND BELOW AVERAGE SALES?


-- selects cities with above avg sales by state
	-- useful for determining active and high-demand markets
    -- good for understanding market momentum and inventory turnover
    -- great for comparative graphs by state or cities in same state

WITH city_sold AS (
	SELECT
		city,
        state,
        COUNT(homeStatus) AS numSold 
	FROM property_listings
	WHERE homeStatus = 'Recently Sold'
		AND dateSold IS NOT NULL -- ensures home sold recently and that home was sold
    GROUP BY city, state 
), -- selects the num of 'Recently Sold' listings in each state/city
avg_sold AS (
	SELECT
		AVG(numSold) AS avgSold
	FROM city_sold
) -- selects the avg num of sold homes 
SELECT 
	cs.city,
    cs.state,
    cs.numSold,
    ROUND(avgs.avgSold, 0) AS avgSales 
FROM city_sold cs
CROSS JOIN avg_sold avgs
WHERE cs.numSold > avgs.avgSold -- ensures cities with above avg sales 
ORDER BY cs.state, cs.numSold DESC;


-- selects states with above avg sales

WITH state_sold AS (  
	SELECT
        state,
        COUNT(homeStatus) AS numSold 
	FROM property_listings
	WHERE homeStatus = 'Recently Sold'
		AND dateSold IS NOT NULL -- ensures home sold recently and that home was sold
    GROUP BY state
), -- selects the num of 'Recently Sold' listings in each state
avg_sold AS ( 
	SELECT
		AVG(numSold) AS avgSold
	FROM state_sold
) -- selects the avg num of sold homes 
SELECT 
	ss.state,
    ss.numSold,
    ROUND(avgs.avgSold, 0) as avgSales
FROM state_sold ss
CROSS JOIN avg_sold avgs
WHERE ss.numSold > avgs.avgSold -- specifies states with above avg sales 
ORDER BY ss.state, ss.numSold DESC;


-- selects cities with above below avg sales by state
	-- useful for determining stagnant and low-demand markets
	-- good for understanding market momentum and inventory turnover
    -- great for comparative graphs by state or cities in same state
    
WITH city_sold AS ( 
	SELECT
		city,
        state,
        COUNT(homeStatus) AS numSold
	FROM property_listings
	WHERE homeStatus = 'Recently Sold'
		AND dateSold IS NOT NULL
    GROUP BY city, state
), -- selects the num of 'Recently Sold' listings in each state
avg_sold AS ( 
	SELECT
		AVG(numSold) AS avgSold
	FROM city_sold
) -- selects the avg num of sold homes 
SELECT 
	cs.city,
	cs.state,
	cs.numSold,
    ROUND(avgs.avgSold, 0) AS avgSales
FROM city_sold cs
CROSS JOIN avg_sold avgs
WHERE cs.numSold < avgs.avgSold -- ensures cities with below avg sales 
ORDER BY cs.state, cs.numSold ASC;


-- selects states with below avg sales

WITH state_sold AS (
	SELECT
        state,
        COUNT(homeStatus) AS numSold
	FROM property_listings
    WHERE homeStatus = 'Recently Sold'
		AND dateSold IS NOT NULL
    GROUP BY state
), -- selects the num of 'Recently Sold' listings in each state
avg_sold AS (
	SELECT
		AVG(numSold) AS avgSold
	FROM state_sold
) -- selects the avg num of sold homes 
SELECT 
	ss.state,
    numSold,
    ROUND(avgs.avgSold, 0) as avgSales
FROM state_sold ss
CROSS JOIN avg_sold avgs
WHERE ss.numSold < avgs.avgSold -- ensures states with below avg sales 
ORDER BY ss.state, ss.numSold DESC; -- orders by state and num sold DESC





-- ------->> 4.) WHAT IS THE AVERAGE PRICE PER SQUARE FOOT ACROSS CITIES OR STATES?


-- selects avg price per sqft, from lease to most expensive by state
	-- great for comparing relative value of location
    -- can determine overpriced cities compared to national average
    -- if combined with local salaries, could make a fantastic dashboard; also boxplots

SELECT 
    pl.city,
    pl.state,
    ROUND(AVG(lph.pricePerSquareFoot), 2) AS avgPriceSqft -- calculates avg price per sqft
FROM property_listings pl
JOIN listing_price_history lph 
	ON pl.zpid = lph.zpid
GROUP BY pl.state , pl.city
ORDER BY pl.state, avgPriceSqft; 





-- ------->> 5.) WHAT ARE THE LEAST AND MOST EXPENSIVE CITIES IN EACH STATE BY THE AVERAGE PRICE PER SQUARE FOOT?


-- selects the most and least expensive cities in each state by average price per sqft
	-- helps determine good and "not so great" places to purchase property
    -- useful metric for investors and sub-markets (e.g., vacation homes, retirees, first-time buyers)
    -- could be useful as a side-by-side bar graph for each state

WITH city_avg_price_sqft AS (
	SELECT
		pl.city, 
        pl.state,
        ROUND(AVG(lph.pricePerSquareFoot), 2) AS avgPricePerSqft -- calculates avg price per sqft
	FROM property_listings pl
    JOIN listing_price_history lph
		ON pl.zpid = lph.zpid
	WHERE lph.pricePerSquareFoot IS NOT NULL 
		AND lph.pricePerSquareFoot > 0 -- ensures price per sqft is not NULL and not zero, to remove outliers
	GROUP BY pl.state, pl.city
    ORDER BY pl.state, pl.city
), -- selects avg price per sqft by city 
ranked_cities AS (
	SELECT 
		city,
        state,
        avgPricePerSqft,
        RANK() OVER (PARTITION BY state ORDER BY avgPricePerSqft DESC) AS mostExpensiveRank, -- ranks price per sqft by state from most to least expensive
        RANK() OVER (PARTITION BY state ORDER BY avgPricePerSqft ASC) AS leastExpensiveRank -- ranks price per sqft by state from least to most expensive
	FROM city_avg_price_sqft
) -- selects most and least expensive price per sqft in each city 
SELECT 
	state, 
    city, 
    avgPricePerSqft, 
    'Most Expensive' AS category
FROM ranked_cities
WHERE mostExpensiveRank = 1
	AND avgPricePerSqft > 0 -- selects most expensive listing by city 
UNION -- peforms union on "least" and "most" expensivce categories 
SELECT 
	state,
    city, 
    avgPricePerSqft, 
    'Least Expensive' AS category
FROM ranked_cities
WHERE leastExpensiveRank = 1
	AND avgPricePerSqft > 0 -- selects most expensive listing by city  
ORDER BY state, category; -- orders union by state and least/most category 





-- ===================================== SECTION B =====================================
-- ============================= FACTORS OF PROPERTY PRICE =============================
-- _____________________________________________________________________________________



-- ------->> 6.) WHAT ARE THE 5 MOST RECENTLY SOLD PROPERTIES IN EACH CITY?


-- selects the types of homeStatus, for reference 

SELECT DISTINCT homeStatus
FROM property_listings;


-- selects 5 most recently sold properties in each city
	-- includes zpid, home type, price, hoem status, living area, year built, time on Zillow, sell date and ranking
    -- a larger sample could indicate highly active markets
    -- a useful table for dashboard; for longer timeline, could do heatmap

WITH ranked_listings AS (
	SELECT
		zpid,
        homeType,
        streetAddress,
        city, 
        price,
        homeStatus, 
		livingArea,
        yearBuilt,
        timeOnZillow,
        dateSold,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY dateSold DESC) AS rn -- ranks most recently sold listings by city
	FROM property_listings
    WHERE dateSold IS NOT NULL
		AND price > 0 -- ensures price does not equal zero, removing outliers 
), -- selects row_num for listings ordered by date sold 
top_five AS (
	SELECT 
		zpid,
        homeType,
        streetAddress,
        city, 
        price,
        homeStatus, 
		livingArea,
        yearBuilt,
        timeOnZillow,
        dateSold,
        rn
    FROM ranked_listings
    WHERE rn < 6
) -- selects top 5 most recently sold by city
SELECT 
		zpid,
        homeType,
        streetAddress,
        city, 
        price,
		livingArea,
        yearBuilt,
        CONCAT(timeOnZillow, ' days') AS timeOnZillow, -- adds unit of measurement for ease in reading 
        dateSold
FROM top_five
ORDER BY city, dateSold DESC; -- orders the results by city, then by date sold 





-- ------->> 7.) WHAT IMPACT DOES THE DISTANCE FROM A SCHOOL HAVE ON THE PRICE


-- selects impact of school distance on listing price
	-- could help determine overall importance of metric on price
    -- good to make a scatter plot (d v. p)

SELECT
	lsi.distanceFromListing,
    ROUND(AVG(pl.price), 2) AS avgPrice -- calculates avg price from pl
FROM property_listings pl
JOIN listing_schools_info lsi
	ON pl.zpid = lsi.zpid
WHERE pl.price IS NOT NULL
	AND pl.price NOT IN (0, '') -- ensures price is not null
GROUP BY lsi.distanceFromListing -- groups distances with avg price
ORDER BY lsi.distanceFromListing;


-- selects the avg price by distance from listing to a school 
	-- uses Arizona, but can substitute any state 
    -- very useful information for families or prospective families
    -- could be useful for a scatterplot or linear regression

SELECT
	pl.state,
	lsi.distanceFromListing,
    ROUND(AVG(pl.price), 2) AS avgPrice -- calculates avg price from pl
FROM property_listings pl
JOIN listing_schools_info lsi
	ON pl.zpid = lsi.zpid
WHERE pl.price IS NOT NULL
	AND pl.price NOT IN (0, '') -- ensures price is not null
	-- enter state code below to have a specific state
    -- AND pl.state = 'AZ'
GROUP BY pl.state, lsi.distanceFromListing -- groups distances with avg price
ORDER BY pl.state, lsi.distanceFromListing;


-- selects list of state codes for previous query

SELECT DISTINCT state
FROM property_listings
ORDER BY 1;





-- ------->> 8.) WHAT IS THE AVERAGE LISTING PRICE BY SQUARE FOOTAGE?


-- selects avg listing price per sqft
	-- unlike query 4, this is at the national level
    -- could give better indication of national markets
    -- if combined with home type, could be helpful for luxury or budget purchases
    -- with a longer time frame, this would be helpful for a time-series line graph or density plot

SELECT
	lph.pricePerSquareFoot,
    ROUND(AVG(pl.price), 2) AS avgPrice -- calculates avg price from pl
FROM listing_price_history lph
JOIN property_listings pl
	ON lph.zpid = pl.zpid
WHERE lph.pricePerSquareFoot > 0 -- ensures price is not null
GROUP BY lph.pricePerSquareFoot -- groups size with avg price
ORDER BY lph.pricePerSquareFoot;


-- selects avg price per sqft in brackets of 100 sqft
	-- more helpful for determining size relation to price
    -- great data for a bar graph

SELECT
	pl.state,
	lph.pricePerSquareFoot,
    ROUND(AVG(pl.price), 2) AS avgPrice -- calculates avg price from pl
FROM listing_price_history lph
JOIN property_listings pl
	ON lph.zpid = pl.zpid
WHERE lph.pricePerSquareFoot IN (100, 200, 300, 400, 500, 600, 700, 800, 900, 1000) -- ensures partitioned bins of 100 (price)
GROUP BY pl.state, lph.pricePerSquareFoot -- groups by state and sqft
ORDER BY pl.state, avgPrice;





-- ------->> 9.)  WHAT IMPACT DOES THE YEAR BUILT AND LIVING AREA HAVE WHEN IT COMES TO THE LISTING PRICE?


-- selects avg price by year built
	-- useful in determining the role year built plays in price
    -- could split along key time influences (e.g., WWII, Internet bubble, COVID)
    -- great data for a line plot (y v. p)
 
SELECT
	yearBuilt,
    ROUND(AVG(price), 2) as avgPrice -- calculates avg price
FROM property_listings
WHERE yearBuilt IS NOT NULL
	AND price > 0 -- ensures price is not null and not 0
GROUP BY yearBuilt -- groups by year built
ORDER BY yearBuilt;


-- selects avg price by decade built 
	-- helps to aggregate previous query results
    -- good candidate for a bar graph or histogram
 
SELECT
	CASE
		WHEN yearBuilt BETWEEN 1800 AND 1809 THEN '1800s'
        WHEN yearBuilt BETWEEN 1810 AND 1819 THEN '1810s'
        WHEN yearBuilt BETWEEN 1820 AND 1829 THEN '1820s'
        WHEN yearBuilt BETWEEN 1830 AND 1839 THEN '1830s'
        WHEN yearBuilt BETWEEN 1840 AND 1849 THEN '1840s'
        WHEN yearBuilt BETWEEN 1850 AND 1859 THEN '1850s'
        WHEN yearBuilt BETWEEN 1860 AND 1869 THEN '1860s'
        WHEN yearBuilt BETWEEN 1870 AND 1879 THEN '1870s'
        WHEN yearBuilt BETWEEN 1880 AND 1889 THEN '1880s'
        WHEN yearBuilt BETWEEN 1890 AND 1899 THEN '1890s'
		WHEN yearBuilt BETWEEN 1900 AND 1909 THEN '1900s'
        WHEN yearBuilt BETWEEN 1910 AND 1919 THEN '1910s'
        WHEN yearBuilt BETWEEN 1920 AND 1929 THEN '1920s'
        WHEN yearBuilt BETWEEN 1930 AND 1939 THEN '1930s'
        WHEN yearBuilt BETWEEN 1940 AND 1949 THEN '1940s'
        WHEN yearBuilt BETWEEN 1950 AND 1959 THEN '1950s'
        WHEN yearBuilt BETWEEN 1960 AND 1969 THEN '1960s'
        WHEN yearBuilt BETWEEN 1970 AND 1979 THEN '1970s'
        WHEN yearBuilt BETWEEN 1980 AND 1989 THEN '1980s'
        WHEN yearBuilt BETWEEN 1990 AND 1999 THEN '1990s'
		WHEN yearBuilt BETWEEN 2000 AND 2009 THEN '2000s'
        WHEN yearBuilt BETWEEN 2010 AND 2019 THEN '2010s'
        WHEN yearBuilt BETWEEN 2020 AND 2029 THEN '2020s'
	END AS yearBracket, -- calculates decade bins
    ROUND(AVG(price), 2) as avgPrice -- calculates avg price
FROM property_listings
WHERE yearBuilt IS NOT NULL
	AND price > 0 -- ensures price is not null and not 0
GROUP BY yearBracket -- groups by decade bins
ORDER BY yearBracket;
    
    
-- selects avg price by livingArea
	-- beyond sqft, this accounts for living space compared to overall space
	-- could be combined with data from sqft price to indicate the impact
 
SELECT
	livingArea,
    ROUND(AVG(price), 2) as avgPrice -- calculates avg price
FROM property_listings
WHERE livingArea IS NOT NULL
	AND price > 0 -- ensures price is not null and not 0
GROUP BY livingArea
ORDER BY livingArea;





-- ------->> 10.) DO NEARBY LISTINGS GENERALLY RATE HIGHER OR LOWER THAN THE LISTINGS AROUND THEM IN EACH STATE?


-- selects num of listings whose price is higher and lower than nearby avgs
	-- helps determine Zillow value compared to overall real estate market
    -- can tell if homes are locally under or over priced
    -- could be combined with other metrics for more exhaustive conclusions

WITH avg_zip_price AS (
	SELECT
		zpid,
		zipComp AS compZip,
		AVG(priceComp) OVER(PARTITION BY zipComp) AS avgLocalPrice
	FROM listing_nearby_homes
), -- selects the avg price per zipcode of nearby listings
avg_listing_price AS (
	SELECT
		zpid,
        zipcode,
        AVG(price) AS avgPrice
	FROM property_listings
    GROUP BY zpid, zipcode
), -- selects the avg price per zipcode of listings
lower_than_avg AS (
	SELECT
		COUNT(alp.zpid) AS lowerThanAvg -- calculates the num of listings lower than avg
	FROM avg_listing_price alp
	JOIN avg_zip_price azp
		ON alp.zpid = azp.zpid
	WHERE alp.avgPrice < azp.avgLocalPrice
		AND alp.zipcode = azp.compZip -- ensures listing below avg price of nearby and zipcodes of two are identical
), -- selects the listings below avg of nearby 
higher_than_avg AS (
	SELECT
		COUNT(alp.zpid) AS higherThanAvg -- calculates the num of listings higher than avg
	FROM avg_listing_price alp
	JOIN avg_zip_price azp
		ON alp.zpid = azp.zpid
	WHERE alp.avgPrice > azp.avgLocalPrice 
		AND alp.zipcode = azp.compZip -- ensures listing below avg price of nearby and zipcodes of two are identical
) -- selects the listings above avg of nearby 
SELECT
	'Above Avgerage' AS category,
    higherThanAvg
FROM higher_than_avg hta
UNION -- connects two nums into one table
SELECT
	'Below Average' AS category,
    lowerThanAvg
FROM lower_than_avg lta; 


-- selects num of listings whose price is higher and lower than nearby avgs by state
	-- helpful in seeing viability of using Zillow in different markets
    -- this is true for both buyers and sellers
    -- great option for side-by-side bar graph, esp. if limited num of states

WITH avg_state_price AS (
	SELECT
        pl.state,
		AVG(lnh.priceComp) AS avgLocalPrice -- calculates the avg price of nearby
	FROM listing_nearby_homes lnh
    JOIN property_listings pl
		ON pl.zpid = lnh.zpid
	GROUP BY pl.state
), -- selects avg price of nearby
avg_listing_price AS (
	SELECT
		pl.state,
        CASE
			WHEN pl.price < asp.avgLocalPrice THEN 'Below Average'
			WHEN pl.price > asp.avgLocalPrice THEN 'Above Average'
			ELSE 'At Average'
		END AS priceComp -- calculates categories for above and below avgs
	FROM property_listings pl
    JOIN avg_state_price asp
		ON pl.state = asp.state
) -- selects listings of above and below avg groups
SELECT
	state,
	CASE
		WHEN (SUM(CASE WHEN priceComp = 'Above Average' THEN 1 ELSE 0 END) -
			  SUM(CASE WHEN priceComp = 'Below Average' THEN 1 ELSE 0 END)) > 0 THEN 'Above Average'
		WHEN (SUM(CASE WHEN priceComp = 'Below Average' THEN 1 ELSE 0 END) -
			  SUM(CASE WHEN priceComp = 'Above Average' THEN 1 ELSE 0 END)) > 0 THEN 'Below Average'
		ELSE 'At Average'
    END AS labelByState, -- calculates above and below avg listings by state for labels
    SUM(CASE WHEN priceComp = 'Below Average' THEN 1 ELSE 0 END) AS lowerThanAverage, -- calculates count of lower than avg listings
    SUM(CASE WHEN priceComp = 'Above Average' THEN 1 ELSE 0 END) AS higherThanAverage -- calculates count of above than avg listings
FROM avg_listing_price
GROUP BY state -- groups by state
ORDER BY labelBYState, state; -- orders by label and state


-- selects num of listings whose price is higher and lower than avg zipcode price
	-- great for more minute and detailed investigations
    -- when arranged by state or region, could be used for heat map or sorted bar graph

WITH avg_zip_price AS (
	SELECT
		zpid,
		zipComp AS compZip,
		ROUND(AVG(priceComp), 2) AS avgLocalPrice -- calculates avg price per nearby zip
	FROM listing_nearby_homes
    GROUP BY compZip, zpid
), -- selects avg zipcode price of nearby
avg_price_listing AS (
	SELECT 
		zpid AS listingID,
        state,
        zipcode AS listingZip,
        ROUND(AVG(price), 2) AS avgListingPrice -- calculates avg price of listing zip
	FROM property_listings
    GROUP BY listingID, listingZip, state
) -- selects avg price of listing
SELECT 
	listingId,
    state,
	listingZip,
	CASE
		WHEN ROUND(avgListingPrice - avgLocalPrice, 2) > 0 THEN 'Above Average'
        WHEN ROUND(avgListingPrice - avgLocalPrice, 2) < 0 THEN 'Below Average'
    END AS priceComp, -- calculates categories for above and below avgs
	avgListingPrice,
	avgLocalPrice,
	ROUND(avgListingPrice - avgLocalPrice, 2) AS priceDifference -- calculates whether avg listing price or avg nearvy price higher
FROM avg_zip_price azp
JOIN avg_price_listing apl
	ON azp.zpid = apl.listingID
ORDER BY state, priceComp, priceDifference DESC; -- orders by state, label and price difference


-- selects same calc (num of listings whose price is higher and lower than avg) by state

SELECT 
	pl.state,
    CASE
		WHEN ROUND((AVG(pl.price) - AVG(lnh.priceComp)), 2) > 0 THEN 'Above Average'
        WHEN ROUND((AVG(pl.price) - AVG(lnh.priceComp)), 2) < 0 THEN 'Below Average'
    END AS priceComp, -- calculates categories for above and below avgs
    ROUND(AVG(pl.price), 2) AS avgListingPrice, -- calculates avg listing price
    ROUND(AVG(lnh.priceComp), 2) AS avgLocalPrice, -- calcuclates avg price of nearby
    ROUND((AVG(pl.price) - AVG(lnh.priceComp)), 2) AS priceDifference -- calculates price difference between avg listing and nearby price
FROM property_listings pl
JOIN listing_nearby_homes lnh
	ON pl.zpid = lnh.zpid
GROUP BY pl.state -- gropus by state
ORDER BY priceDifference DESC; -- orders by price difference DESC


-- selects 5 states with highest above and lowest below avg price
	-- for investors, helpful for determining cheap and expensive markets to enter
    -- can be turned into bar graph (either one or two separate ones)

WITH price_comp AS (
	SELECT
		pl.state AS state,
        pl.zpid,
        pl.price,
        AVG(lnh.priceComp) AS avgCompPrice, -- calcuclates avg price of nearby
		CASE
			WHEN pl.price > AVG(lnh.priceComp) THEN 'Above Average'
			WHEN pl.price < AVG(lnh.priceComp) THEN 'Below Average'
		END AS priceComp -- calculates categories for above and below avgs
	FROM property_listings pl
    JOIN listing_nearby_homes lnh
		ON pl.zpid = lnh.zpid
	GROUP BY pl.state, pl.zpid, pl.price -- groups by state, zpid and price
), -- selects avg nearby listing price by state, zpid and price
avg_table AS (
	SELECT 
		pc.state,
		pc.priceComp,
        RANK() OVER(PARTITION BY pc.priceComp ORDER BY AVG(pl.price) - AVG(lnh.priceComp) DESC) AS stateRanking, -- selects ranking above and below avg by price difference (avg listing price - avg nearby price)
		ROUND(AVG(pl.price), 2) AS avgListingPrice, -- calcualtes avg price of listing
		ROUND(AVG(lnh.priceComp), 2) AS avgLocalPrice, -- calculates avg price of nearby
		ROUND((AVG(pl.price) - AVG(lnh.priceComp)), 2) AS priceDifference -- calcualtes price difference between listing and nearby price
	FROM property_listings pl
	JOIN listing_nearby_homes lnh
		ON pl.zpid = lnh.zpid
	JOIN price_comp pc -- connects cte to two tables
		ON lnh.zpid = pc.zpid
	GROUP BY pc.state, pc.priceComp -- groups by state and above/below avg
),
five_highest AS (
	SELECT
		state,
        priceComp,
        stateRanking,
        avgListingPrice,
        avgLocalPrice,
        priceDifference
	FROM avg_table
    WHERE priceComp = 'Above Average'
    ORDER BY priceDifference DESC
    LIMIT 5
), -- selects 5 highest above avg
five_lowest AS (
	SELECT
		state,
        priceComp,
        stateRanking,
        avgListingPrice,
        avgLocalPrice,
        priceDifference
	FROM avg_table
    WHERE priceComp = 'Below Average'
    ORDER BY priceDifference
    LIMIT 5
) -- selects 5 lowest below avg
SELECT
	state,
	priceComp,
	stateRanking,
	avgListingPrice,
	avgLocalPrice,
	priceDifference
FROM five_highest
UNION ALL -- connects five highest and 5 lowest into one table
SELECT 
	state,
	priceComp,
	stateRanking,
	avgListingPrice,
	avgLocalPrice,
	priceDifference
FROM five_lowest
ORDER BY priceComp, priceDifference DESC; -- orders by above/below avg and price difference





-- ------->> 11.) WHICH CITY HAS THE HIGHEST AVERAGE PROPERTY TAX RATE BY STATE, RANKED BY MOST EXPENSIVE?


-- selects cheapest and most expensive tax rates by state
	-- helps determined indirect costs to buyers
    -- can be used to create bar graphs if scope is limited 

WITH ranked_property_tax_rate AS (
	SELECT
		state, 
        city,
        ROUND(AVG(propertyTaxRate), 2) AS avgPropTax -- calculates avg property tax rate
	FROM property_listings
    GROUP BY state, city -- groups by state and city
), -- selects ranked tax rate by state
ranked_cities AS (
	SELECT
		state,
        city,
        avgPropTax,
        ROW_NUMBER() OVER (PARTITION BY state ORDER BY avgPropTax DESC) AS rn 
	FROM ranked_property_tax_rate
) -- selects ranked tax rate by lowest by state
SELECT 
	state,
    city,
    avgPropTax
FROM ranked_cities
WHERE rn = 1 -- ensures lowest tax rate city for each state
ORDER BY avgPropTax; -- orders by tax rate




-- ===================================== SECTION C =====================================
-- ================================ PROPERTY POPULARITY ================================
-- _____________________________________________________________________________________



-- ------->> 12.) WHICH PROPERTY TYPES HAVE THE HIGHEST AVERAGE PAGE VIEWS OR FAVORITE CLICKS?


-- selects avg page views by home type
	-- very helpful in determining demand and interest
    -- useful as KPI
    -- a great candidate for bar graph, esp. if compared with fav count

WITH homeType_avg AS (
	SELECT
		homeType,
		ROUND(AVG(pageViewCount), 0) AS avgViews -- calculates avg page view count 
	FROM property_listings
	GROUP BY homeType -- groups avg by homeType
) -- selects avg view count for home type
SELECT
	homeType,
    avgViews,
    ROW_NUMBER() OVER (ORDER BY avgViews DESC) as rankedViews -- calculates rank of avg views count
FROM homeType_avg;


-- selects avg fav count by home type
	-- very helpful in determining in-demand preferences
    -- a great candidate for bar graph, esp. if compared with page views
WITH homeType_avg AS (
	SELECT
		homeType,
		ROUND(AVG(favoriteCount), 0) AS avg_favorites -- calculates avg page favorite count 
	FROM property_listings
	GROUP BY homeType -- groups avg by homeType
) -- selects avg favorite count for home type
SELECT
	homeType,
    avg_favorites,
    ROW_NUMBER() OVER (ORDER BY avg_favorites DESC) as ranked_favorites -- calculates rank of avg favorite count
FROM homeType_avg;





-- ------->> 13.) WHAT ARE THE MOST EXPENSIVE ZIPCODES IN EACH STATE?


-- selects avg price by zipcode in each state
	-- helpful for potential buyers determining location or comparing local zipcodes
    -- could be very important for luxury or budget purchases
	-- if a more refined search, could contribute to dashboard

SELECT
	state,
    zipcode,
    ROUND(AVG(price), 0) AS avgPriceByZip -- calculates avg price 
FROM property_listings
GROUP BY state, zipcode
ORDER BY state, avgPriceByZip DESC; -- orders by state and avg zipcode price DESC





-- ------->> 14.) WHAT ARE THE MOST COMMON EVENT TYPES?


-- selects types of 'events' (e.g., 'Listed for sale' or 'Price change')

SELECT DISTINCT `event`
FROM listing_price_history;


-- selects ranked counts of event types in descending order 
	-- very helpful in determining overall health of real estate market
    -- if limited to a specific location, could be useful for comparison
    -- great option for bar graph; time series line graph (by day or week)

WITH event_count_cte AS (
	SELECT 
		`event`,
		COUNT(`event`) AS eventCount -- calculates event type count
	FROM listing_price_history
	GROUP BY event -- groups by event type
) -- selects event count by event
SELECT
	ROW_NUMBER() OVER(ORDER BY eventCount DESC) AS rankedCount, -- calculates rank for count of event types DESC
    `event`,
    eventCount AS numOfEvents
FROM event_count_cte;





-- ===================================== SECTION D =====================================
-- ================================ PROPERTIES OVER TIME ===============================
-- _____________________________________________________________________________________



-- ------->> 15.) HOW DID THE AVERAGE PRICE OF POSTING CHANGE OVER THE MONTH OF NOVEMBER?


-- calculates the average price of listing day-by-day in November
	-- longer scope could indicate best and worst times to buy
    -- longer time scope could also indicate seasonal pricing
    -- this query is useful for time series line graphs

SELECT
	DAY(datePosted) AS dayOfMonth, -- calculates numerical day of month
    ROUND(AVG(price), 2) AS avgPrice 
FROM property_listings
WHERE MONTH(datePosted) = '11' 
	AND price > 0
    AND price IS NOT NULL -- ensures month is "November", price does not equal zero and not NULL, removing outliers 
GROUP BY dayOfMonth -- groups by day of month
ORDER BY dayOfMonth;





-- ------->> 16.) WHAT PERIOD OF THE MONTH OF NOVEMBER WERE PROPERTIES REPORTED AS SOLD?


-- calculates the average price of listing by day ranges (increments of 5) in November
	-- longer scope could indicate best and worst times to buy
    -- this query is useful for time series graphs
    
WITH month_list AS (
	SELECT
		dateSold,
		CASE
            WHEN MONTH(dateSold) = '11' THEN 'November'
            ELSE 'Nothing'
		END AS monthLabel, -- calculates special label for month of "November"
        CASE
			WHEN DAY(dateSold) BETWEEN 1 AND 5 THEN '1-5'
            WHEN DAY(dateSold) BETWEEN 6 AND 10 THEN '6-10'
            WHEN DAY(dateSold) BETWEEN 11 AND 15 THEN '11-15'
            WHEN DAY(dateSold) BETWEEN 15 AND 20 THEN '15-20'
            WHEN DAY(dateSold) BETWEEN 21 AND 25 THEN '21-25'
            WHEN DAY(dateSold) BETWEEN 25 AND 31 THEN '25-31'
            ELSE 'Nothing'
		END AS dayRange -- calculates day ranges in increments of 5 days (6 days for 25-31)
	FROM property_listings
), -- selects partitioned num of sold houses into 5/6 day bins
total_count AS (
	SELECT
        COUNT(*) AS novCount -- calculates total num sold in "November"
	FROM month_list
    WHERE monthLabel = 'November'
		AND dayRange != 'Nothing'
) -- selects total num listings sold in "November"
SELECT 
    ml.monthLabel,
    ml.dayRange,
    COUNT(ml.dayRange) AS numSoldProperties,
    ROUND(COUNT(*) * 100 / tc.novCount, 2) AS percSold -- calculates perc sold in day range compared to overall sold
FROM month_list ml
JOIN total_count tc
	ON 1 =1 -- connects two ctes
WHERE ml.monthLabel = 'November'
	AND ml.dayRange != 'Nothing'
GROUP BY  ml.monthLabel, ml.dayRange, tc.novCount -- groups by month (November by default), day range and total count
ORDER BY ml.dayRange;





-- ------->> 17.) HOW LONG DO PROPERTIES TYPICALLY STAY ON THE MARKET BEFORE BEING SOLD?


-- selects avg num of days on Zillow by property type
	-- helps see what kind of listings are difficult to offload
    -- could indicate other factors, such as job market
    -- another great candidate for bar graph
    
SELECT 
    homeType, 
    -- uncomment following line to include state grouping
    -- state,
    CONCAT(ROUND(AVG(timeOnZillow), 1), ' days') as avgTimeOnZillow -- calculates avg time on zillow, adding "days" for reabability
FROM property_listings
-- uncomment following line to specify state
-- WHERE state = 'AZ'
GROUP BY 
	-- uncomment following line if including state
    -- state,    
    homeType -- groups by home type
HAVING AVG(timeOnZillow) IS NOT NULL  -- ensures listing has relevant data
ORDER BY avgTimeOnZillow DESC; -- orders by timeOnZillow DESC


-- selects min and max num of days on Zillow by state
	-- helpful info if combined with above query to understand range
    -- could further calc percentiles to get an image 
    -- great option for contributing to histogram

SELECT
	state,
    MIN(timeOnZillow) AS minTimeOnZillow, -- calculates max time on Zillow
    MAX(timeOnZillow) AS maxTimeOnZillow -- calculates min time on Zillow
FROM property_listings
-- uncomment following line to specify state
-- WHERE state = 'AZ'
GROUP BY state -- groups by state
ORDER BY state;





-- ===================================== SECTION E =====================================
-- ================================== PROPERTY STATUS ==================================
-- _____________________________________________________________________________________



-- ------->> 18.) FORE FORECLOSED HOMES, WHAT TYPES OF HOMES, HOW MANY BATHROOMS AND BEDROOMS AND WHICH STATES ARE MOST COMMON?


-- selects num and perc of foreclosed homes by type
	-- great for determining problematic properties
    -- could be combined with larger economic data
    -- great for bar graph (could be broken down by states or zipcodes)
 
SELECT 
	pl.homeType,
	SUM(CASE WHEN ls.is_foreclosure = 1 THEN 1 ELSE 0 END) AS forecloseCount, -- calculates total num of foreclosed listings
	ROUND(SUM(CASE WHEN ls.is_foreclosure = 1 THEN 1 ELSE 0 END) * 100 / COUNT(*), 2) AS percForeclosure -- calculates perc of overall foreclosed listings
FROM listing_subtype ls
JOIN property_listings pl
	ON ls.zpid = pl.zpid
GROUP BY pl.homeType -- groups by hoem type
ORDER BY forecloseCount DESC; -- order by total foreclosed listings DESC


-- selects num and perc of foreclosed homes by num of bathrooms
	-- great for determining problematic properties
    -- could be combined with larger economic data
    -- great for bar graph (could be broken down by states or zipcodes)

WITH foreclose_bath AS (
	SELECT 
		SUM(CASE WHEN is_foreclosure = 1 THEN 1 ELSE 0 END) AS forecloseCount -- calculates total num of foreclosed listings
	FROM listing_subtype ls
    JOIN property_listings pl
		ON ls.zpid = pl.zpid
    WHERE ls.is_foreclosure IS NOT NULL
		AND pl.bathrooms IS NOT NULL -- ensures foreclosure and bathrooms is not null
) -- selects total foreclosed baths 
SELECT 
	pl.bathrooms,
    COUNT(pl.bathrooms) AS bathCount, -- calculates num bathrooms
    fb.forecloseCount AS totalForeBath, -- calculates num foreclosed baths
    ROUND(COUNT(pl.bathrooms) * 100 / fb.forecloseCount, 2) as percBath -- calculates perc of foreclosed baths compared to overall
FROM property_listings pl
JOIN listing_subtype ls
	ON pl.zpid = ls.zpid
JOIN foreclose_bath fb 
	ON 1=1 -- connects tables to cte
WHERE ls.is_foreclosure > 0
	AND pl.bathrooms IS NOT NULL -- ensures foreclosure is true and bathrooms is not null
GROUP BY pl.bathrooms, totalForeBath -- groups by num baths and total foreclosed baths
ORDER BY pl.bathrooms;


-- selects num and perc of foreclosed homes by num of bedrooms
	-- great for determining problematic properties
    -- could be combined with larger economic data
    -- great for bar graph (could be broken down by states or zipcodes)
    
WITH foreclose_bed AS (
	SELECT 
		SUM(CASE WHEN ls.is_foreclosure = 1 THEN 1 ELSE 0 END) AS forecloseCount -- calculates total num of foreclosed listings
	FROM listing_subtype ls
    JOIN property_listings pl
		ON ls.zpid = pl.zpid
    WHERE ls.is_foreclosure IS NOT NULL
		AND pl.bedrooms IS NOT NULL -- ensures foreclosure and bedrooms is not null
) -- selects total foreclosed beds 
SELECT 
	pl.bedrooms,
    COUNT(pl.bedrooms) AS bedCount,  -- calculates num bedrooms
    fb.forecloseCount AS totalForeBed, -- calculates num foreclosed beds
    ROUND(COUNT(pl.bedrooms) * 100 / fb.forecloseCount, 2) as percBed -- calculates perc of foreclosed beds compared to overall
FROM property_listings pl
JOIN listing_subtype ls
	ON pl.zpid = ls.zpid
JOIN foreclose_bed fb 
	ON 1=1 -- connects tables to cte
WHERE ls.is_foreclosure > 0
	AND pl.bedrooms IS NOT NULL  -- ensures foreclosure is true and bedrooms is not null
GROUP BY pl.bedrooms, totalForeBed -- groups by num baths and total foreclosed beds
ORDER BY pl.bedrooms;
    

-- selects num and perc of foreclosed homes by state
	-- great for determining problematic regions or states
    -- could be combined with larger economic data
    -- great for bar graph (could be broken down by states or zipcodes)
    
WITH foreclose_state AS (
	SELECT 
		SUM(CASE WHEN ls.is_foreclosure = 1 THEN 1 ELSE 0 END) AS forecloseCount -- calculates total num of foreclosed listings
	FROM listing_subtype ls
    JOIN property_listings pl
		ON ls.zpid = pl.zpid
    WHERE ls.is_foreclosure IS NOT NULL
		AND pl.state IS NOT NULL -- ensures foreclosure and bathrooms is not null
) -- selects total foreclosed where state not null 
SELECT 
	RANK() OVER(ORDER BY COUNT(pl.state) DESC) AS stateRanking,
    pl.state,
    COUNT(pl.state) AS stateCount, -- calculates num states
    fb.forecloseCount AS totalForeState, -- calculates num foreclosed in state
    ROUND(COUNT(pl.state) * 100 / fb.forecloseCount, 2) as percState -- calculates perc of foreclosed in state compared to overall
FROM property_listings pl
JOIN listing_subtype ls
	ON pl.zpid = ls.zpid
JOIN foreclose_state fb 
	ON 1=1 -- connects tables to cte
WHERE ls.is_foreclosure > 0
	AND pl.state IS NOT NULL  -- ensures foreclosure is true and state is not null
GROUP BY pl.state, totalForeState  -- groups by num baths and total foreclosed in state
ORDER BY stateCount DESC;




    
-- ------->> 19.) WHAT PERCENTAGE OF HOMES DOES EACH SUBTYPE ACCOUNT FOR REGARDING EACH HOME TYPE?


-- selects perc of home types by subtypes
	-- gives comprehensive picture of relevant variables 
    -- also helps determine overall markets (can be combined with high or low demand data)
    -- could remove home type for bar graph
    -- if limiting variables and home types, could make nice side-by-side bar graph

SELECT 
	pl.homeType AS homeType,
	ROUND(SUM(ls.is_FSBA = '1') / COUNT(*) * 100, 2) AS percFSBA, -- calculates perc of field compared to overall homes
	ROUND(SUM(ls.is_comingSoon = '1') / COUNT(*) * 100, 2) AS percComingSoon,
	ROUND(SUM(ls.is_newHome = '1') / COUNT(*) * 100, 2) AS percNewHome,
	ROUND(SUM(ls.is_pending = '1') / COUNT(*) * 100, 2) AS percPending,
	ROUND(SUM(ls.is_forAuction = '1') / COUNT(*) * 100, 2) AS percForAuction,
	ROUND(SUM(ls.is_foreclosure = '1') / COUNT(*) * 100, 2) AS percForeclosure,
	ROUND(SUM(ls.is_bankOwned = '1') / COUNT(*) * 100, 2) AS percBankOwned,
	ROUND(SUM(ls.is_openHouse = '1') / COUNT(*) * 100, 2) AS percOpenHouse,
	ROUND(SUM(ls.is_FSBO = '1') / COUNT(*) * 100, 2) AS percFSBO
FROM property_listings pl
JOIN listing_subtype ls
	ON pl.zpid = ls.zpid
GROUP BY pl.homeType -- groups by home type
ORDER BY pl.homeType;





-- ------->> 20.) HOW DO MORTGAGE RATES VARY ACROSS DIFFERENT STATES?


-- selects avg mortgage rate by mortgage type
	-- helps determine indirect costs
    -- if combined with tax rate, could be very informative
    -- could also combine with foreclosed data
    -- perfect candidate for bar graph

SELECT 
	pl.state,
    lmi.bucketType,
    ROUND(AVG(lmi.rate), 2) as mortgageRate -- calculates avg mortgage rate
FROM listing_mortgage_info lmi
JOIN property_listings pl
	ON lmi.zpid = pl.zpid
GROUP BY pl.state, lmi.bucketType -- groups by state and mortgage type
ORDER BY pl.state, lmi.bucketType; -- orders by state and mortgage type

