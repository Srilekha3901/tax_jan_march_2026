SELECT
    StateName,
    COUNT(Customer_ID) AS TotalCustomers
FROM silver.Customer_Current
GROUP BY StateName