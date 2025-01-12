-- 1. Celková tržba podľa krajiny (Na základe faktúr)

CHIPMUNK_DB.STAGING.CHIPMUNK_DB_STAGE;

SELECT 
    BillingCountry AS Country,
    SUM(Total) AS TotalRevenue
FROM Invoice
GROUP BY BillingCountry
ORDER BY TotalRevenue DESC;


-- 2.Počet predaných skladieb podla žáneru

CHIPMUNK_DB.STAGING.CHIPMUNK_DB_STAGE;

SELECT 
    Genre.Name AS Genre,
    SUM(InvoiceLine.Quantity) AS TotalTracksSold
FROM InvoiceLine
JOIN Track ON InvoiceLine.TrackId = Track.TrackId
JOIN Genre ON Track.GenreId = Genre.GenreId
GROUP BY Genre.Name
ORDER BY TotalTracksSold DESC;


-- 3.Počet zákazníkov na daného Support-reprezentatíva (podľa ID)

CHIPMUNK_DB.STAGING.CHIPMUNK_DB_STAGE;

SELECT 
    E.EmployeeId AS SupportRepId,
    CONCAT(E.FirstName, ' ', E.LastName) AS SupportRepName,
    COUNT(C.CustomerId) AS TotalCustomersServed
FROM Employee E
JOIN Customer C ON E.EmployeeId = C.SupportRepId
GROUP BY E.EmployeeId, E.FirstName, E.LastName
ORDER BY TotalCustomersServed DESC;


-- 4.Najpredávanejší Album od top 10 Artistov

CHIPMUNK_DB.STAGING.CHIPMUNK_DB_STAGE;

SELECT 
    Artist.Name AS Artist,
    Album.Title AS Album,
    SUM(InvoiceLine.Quantity) AS TotalTracksSold
FROM InvoiceLine
JOIN Track ON InvoiceLine.TrackId = Track.TrackId
JOIN Album ON Track.AlbumId = Album.AlbumId
JOIN Artist ON Album.ArtistId = Artist.ArtistId
GROUP BY Artist.Name, Album.Title
ORDER BY TotalTracksSold DESC
LIMIT 10;


-- **5.Tržba na Quaterly úrovni **

CHIPMUNK_DB.STAGING.CHIPMUNK_DB_STAGE;

SELECT 
    TO_CHAR(InvoiceDate, 'YYYY-Q') AS YearQ,
    SUM(Total) AS TotalRevenue
FROM Invoice
GROUP BY TO_CHAR(InvoiceDate, 'YYYY-Q')
ORDER BY YearQ ASC;
