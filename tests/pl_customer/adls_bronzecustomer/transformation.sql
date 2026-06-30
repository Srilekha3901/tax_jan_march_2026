SELECT *
FROM Bronze.Customer_Bronze
WHERE SourceFileName IN (
    SELECT SourceFileName
    FROM (
        SELECT TOP (1)
            SourceFileName
        FROM Bronze.Customer_Bronze
        GROUP BY SourceFileName
        ORDER BY MAX(InsertDate) DESC
    ) AS LatestFile
)