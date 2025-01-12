USE database CHIPMUNK_DB;

CREATE SCHEMA IF NOT EXISTS CHIPMUNK_DB.STAGING;

USE SCHEMA CHIPMUNK_DB.STAGING;

CREATE OR REPLACE STAGE CHIPMUNK_DB_STAGECHIPMUNK_DB.STAGING.CHIPMUNK_DB_STAGE;

CREATE or replace warehouse CHIPMUNK_WH;

-- vytvorenie tabuliek

CREATE OR REPLACE TABLE Album (
    AlbumId INT,
    Title VARCHAR(200),
    ArtistId INT
);

CREATE OR REPLACE TABLE Artist (
    ArtistId INT,
    Name VARCHAR(200)
);

CREATE OR REPLACE TABLE Customer (
    CustomerId INT,
    FirstName VARCHAR(200),
    LastName VARCHAR(200),
    Company VARCHAR(200),
    Address VARCHAR(200),
    City VARCHAR(200),
    State VARCHAR(200),
    Country VARCHAR(200),
    PostalCode VARCHAR(200),
    Phone VARCHAR(200),
    Fax VARCHAR(200),
    Email VARCHAR(200),
    SupportRepId INT

);

CREATE OR REPLACE TABLE Employee (
    EmployeeId INT,
    LastName VARCHAR(200),
    FirstName VARCHAR(200),
    Title VARCHAR(200),
    ReportsTo INT,
    BirthDate TIMESTAMP_NTZ,
    HireDate TIMESTAMP_NTZ,
    Address VARCHAR(200),
    City VARCHAR(200),
    State VARCHAR(200),
    Country VARCHAR(200),
    PostalCode VARCHAR(200),
    Phone VARCHAR(200),
    Fax VARCHAR(200),
    Email VARCHAR(200)

);

CREATE OR REPLACE TABLE Genre (
    GenreId INT,
    Name Varchar(200)
);

CREATE OR REPLACE TABLE Invoice (
    InvoiceId INT,
    CustomerId INT,
    InvoiceDate TIMESTAMP_NTZ,
    BillingAddress Varchar(200),
    BillingCity Varchar(200),
    BillingState Varchar(200),
    BillingCountry Varchar(200),
    BillingPostalCode Varchar(200),
    Total DOUBLE
);

Create OR REPLACE TABLE InvoiceLine (
    InvoiceLineId INT,
    InvoiceId INT,
    TrackId INT,
    UnitPrice DOUBLE,
    Quantity INT
);


CREATE OR REPLACE TABLE MediaType(
    MediaTypeId INT,
    Name VARCHAR(100)
);

CREATE OR REPLACE TABLE PlayList(
    PlayListId INT,
    Name VARCHAR(100)
);

CREATE OR REPLACE TABLE PlaylistTrack(
    PlayListId INT,
    TrackId INT
);

CREATE OR REPLACE TABLE Track(
    TrackId INT,
    Name VARCHAR(200),
    AlbumId INT,
    MediaTypeId INT,
    GenreId INT,
    Composer varchar(200),
    Milliseconds bigint,
    Bytes BIGINT,
    UnitPrice DOUBLE
);


-- Nacitanie dat

COPY INTO ARTIST
FROM @CHIPMUNK_DB_STAGE/Artist.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ESCAPE_UNENCLOSED_FIELD = '\\' 
  FIELD_DELIMITER = ','
)
ON_ERROR = 'CONTINUE';



COPY INTO ALBUM
FROM @CHIPMUNK_DB_STAGE/Album.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ESCAPE_UNENCLOSED_FIELD = '\\' 
  FIELD_DELIMITER = ','
)
ON_ERROR = 'CONTINUE';


COPY INTO CUSTOMER
FROM @CHIPMUNK_DB_STAGE/Customer.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ESCAPE_UNENCLOSED_FIELD = '\\' 
  FIELD_DELIMITER = ','
)
ON_ERROR = 'CONTINUE';


COPY INTO EMPLOYEE
FROM @CHIPMUNK_DB_STAGE/Employee.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ESCAPE_UNENCLOSED_FIELD = '\\' 
  FIELD_DELIMITER = ','
)
ON_ERROR = 'CONTINUE';

COPY INTO GENRE
FROM @CHIPMUNK_DB_STAGE/Genre.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ESCAPE_UNENCLOSED_FIELD = '\\' 
  FIELD_DELIMITER = ','
)
ON_ERROR = 'CONTINUE';

COPY INTO INVOICE
FROM @CHIPMUNK_DB_STAGE/Invoice.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ESCAPE_UNENCLOSED_FIELD = '\\' 
  FIELD_DELIMITER = ','
)
ON_ERROR = 'CONTINUE';

COPY INTO INVOICELINE
FROM @CHIPMUNK_DB_STAGE/InvoiceLine.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ESCAPE_UNENCLOSED_FIELD = '\\' 
  FIELD_DELIMITER = ','
)
ON_ERROR = 'CONTINUE';


COPY INTO MEDIATYPE
FROM @CHIPMUNK_DB_STAGE/MediaType.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ESCAPE_UNENCLOSED_FIELD = '\\' 
  FIELD_DELIMITER = ','
)
ON_ERROR = 'CONTINUE';

COPY INTO PLAYLIST
FROM @CHIPMUNK_DB_STAGE/Playlist.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ESCAPE_UNENCLOSED_FIELD = '\\' 
  FIELD_DELIMITER = ','
)
ON_ERROR = 'CONTINUE';

COPY INTO PLAYLISTTRACK
FROM @CHIPMUNK_DB_STAGE/PlaylistTrack.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ESCAPE_UNENCLOSED_FIELD = '\\' 
  FIELD_DELIMITER = ','
)
ON_ERROR = 'CONTINUE';

COPY INTO TRACK
FROM @CHIPMUNK_DB_STAGE/Track.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ESCAPE_UNENCLOSED_FIELD = '\\' 
  FIELD_DELIMITER = ','
)
ON_ERROR = 'CONTINUE';




-- Tranformacia




CREATE OR REPLACE TABLE dim_employee AS 
SELECT
  DISTINCT 
  EmployeeId,
  LastName,
  FirstName,
  HireDate,
  ADDRESS,
  City,
  Country,
  PostalCode 
FROM Employee;


CREATE OR REPLACE TABLE dim_customer AS 
SELECT
  DISTINCT 
  CustomerId,
  FIRSTNAME,
  lastname,
  ADDRESS,
  City,
  Country,
  PostalCode 
FROM customer;


CREATE OR REPLACE TABLE dim_track AS 
SELECT 
  DISTINCT 
  TrackId,
  Track.Name AS Name,
  Album.Title AS AlbumTitle,
  Artist.Name AS ArtistName,
  MediaType.Name AS MediaTypeName,
  Genre.Name AS GenreName,
  Composer,
  Milliseconds,
  Bytes
FROM Track
JOIN Album ON Track.AlbumId = Album.AlbumId
JOIN Artist ON Album.ArtistId = Artist.ArtistId
JOIN MediaType ON Track.MediaTypeId = MediaType.MediaTypeId
JOIN Genre ON Track.GenreId = Genre.GenreId;


CREATE OR REPLACE TABLE dim_date AS 
SELECT DISTINCT 
  InvoiceDate AS DateKey,
  YEAR(InvoiceDate) AS Year,
  MONTH(InvoiceDate) AS Month,
  DAY(InvoiceDate) AS Day,
  DAYOFWEEK(INVOICEDATE) AS DayOfWeek,
  QUARTER(InvoiceDate) AS Quarter,
FROM Invoice;


CREATE OR REPLACE TABLE fact_invoiceline AS 
SELECT
  InvoiceLineId,
  InvoiceId,
  TrackId,
  UnitPrice,
  Quantity,
  (UnitPrice * Quantity) AS Total
FROM InvoiceLine;

-- Dropnutie

DROP TABLE IF EXISTS Artist;
DROP TABLE IF EXISTS Album;
DROP TABLE IF EXISTS Customer;
DROP TABLE IF EXISTS Employee;
DROP TABLE IF EXISTS Genre;
DROP TABLE IF EXISTS InvoiceLine;
DROP TABLE IF EXISTS Invoice;
DROP TABLE IF EXISTS MediaType;
DROP TABLE IF EXISTS PlayList
DROP TABLE IF EXISTS PlaylistTrack
DROP TABLE IF EXISTS Track;