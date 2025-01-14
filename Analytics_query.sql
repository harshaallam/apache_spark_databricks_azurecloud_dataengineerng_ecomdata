select * from comprehensive_obt_table limit 5;

select count(*) from comprehensive_obt_table;

describe formatted comprehensive_obt_table;

select * from comprehensive_obt_table limit 1;

describe comprehensive_obt_table;

SELECT Users_account_age_group, COUNT(*) AS Total_Users
FROM comprehensive_obt_table
GROUP BY Users_account_age_group
ORDER BY Total_Users DESC;

SELECT Country, AVG(Users_productsSold) AS Avg_Products_Sold
FROM comprehensive_obt_table
GROUP BY Country
ORDER BY Avg_Products_Sold DESC;

SELECT Country, Countries_FemaleSellers
FROM comprehensive_obt_table
ORDER BY Countries_FemaleSellers DESC
LIMIT 10;

SELECT Users_hasanyapp, COUNT(*) AS Total_Users
FROM comprehensive_obt_table
GROUP BY Users_hasanyapp;

SELECT Sellers_Sex, COUNT(*) AS Total_Sellers
FROM comprehensive_obt_table
GROUP BY Sellers_Sex
ORDER BY Total_Sellers DESC;

SELECT Country, Countries_TopSellers
FROM comprehensive_obt_table
ORDER BY Countries_TopSellers DESC
LIMIT 10;

SELECT Country, AVG(Users_productsSold) AS Avg_Products_Listed
FROM comprehensive_obt_table
GROUP BY Country
ORDER BY Avg_Products_Listed DESC;

SELECT 'Male' AS Gender, SUM(Buyers_Male) AS Total_Buyers
FROM comprehensive_obt_table
UNION ALL
SELECT 'Female' AS Gender, SUM(Buyers_Female) AS Total_Buyers
FROM comprehensive_obt_table;