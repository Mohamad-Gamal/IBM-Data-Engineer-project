'''GROUPING SETS Query
Goal: Group by different combinations of country and category to show total sales.'''
SELECT 
    c.country,
    cat.category,
    SUM(f.price) AS totalsales
FROM staging."softcartFactSales" f
JOIN staging."softcartDimCountry" c ON f.countryid = c.countryid
JOIN staging."softcartDimCategory" cat ON f.categoryid = cat.categoryid
GROUP BY GROUPING SETS (
    (c.country, cat.category),
    (c.country),
    (cat.category),
    ()
)
ORDER BY c.country, cat.category;
'''########################'''
'''ROLLUP Query
Goal: Generate subtotals by year, country, and grand total.'''
SELECT 
    d.year,
    c.country,
    SUM(f.price) AS totalsales
FROM staging."softcartFactSales" f
JOIN staging."softcartDimDate" d ON f.dateid = d.dateid
JOIN staging."softcartDimCountry" c ON f.countryid = c.countryid
GROUP BY ROLLUP (d.year, c.country)
ORDER BY d.year, c.country;
'''########################'''
'''CUBE Query
Goal: Produce all combinations of year and country for average sales.'''
SELECT 
    d.year,
    c.country,
    AVG(f.price::numeric) AS average_sales
FROM staging."softcartFactSales" f
JOIN staging."softcartDimDate" d ON f.dateid = d.dateid
JOIN staging."softcartDimCountry" c ON f.countryid = c.countryid
GROUP BY CUBE (d.year, c.country)
ORDER BY d.year, c.country;
'''######################'''
'''Create a Materialized Query Table (MQT)
Goal: Store total sales per country for faster access.'''
CREATE MATERIALIZED VIEW total_sales_per_country AS
SELECT 
    c.country,
    SUM(f.price) AS total_sales
FROM staging."softcartFactSales" f
JOIN staging."softcartDimCountry" c ON f.countryid = c.countryid
GROUP BY c.country;

SELECT * FROM total_sales_per_country;