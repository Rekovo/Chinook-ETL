# Chinook-ETL

Repozitár obsahuje ETL implementaciu v Snowflake Chinook dataset.

---
# **1. Úvod a popis zdrojových dát**
Tento dataset možno použiť na analýzu údajov z obchodu Chinook. Analýzu na základe nákupov zákazníkov, počet predaných skladieb podla žáneru, identifikovať trendy v žánroch alebo interpretoch.

Zdrojové dáta pochádzajú z [Kaggle datasetu](https://www.kaggle.com/datasets/anurag629/chinook-csv-dataset?resource=download). Dataset obsahuje jedenásť tabuliek:
- `albums`          - Obsahuje Názov Albumu 
- `artists`         - Obsahuje názov interpreta
- `customers`       - Obsahuje informácie o použivateloch(Meno, adresa, kontakt)
- `employees`       - Obsahuje informácie o zamestnancoch(Meno, adresa, dátum narodenia, dátum nástupu, kontakt)
- `genres`          - Obsahuje žánre jednotlivých trackov
- `invoiceLine`     - Spája Invoice a Track tabulky. Obsahuje Unit price a Quantity.
- `invoices`        - Obsahuje billing informacie a spoločnú cenu invoicu ().
- `media_types` 
- `playlist_track`  - Spája Playlist a Track tabulky
- `playlists`
- `tracks`          - Obsahuje informácie o skladbách (Názov, Composera, dĺžku, veľkosť, cenu)

---

### **1.1 Dátová architektúra**

### **ERD diagram**
<p align="center">
  <img src="https://github.com/Rekovo/Chinook-ETL/blob/main/ERD_Schema.png" alt="ERD_Schema.png">
</p>

---
## **2 Dimenzionálny model**
Navrhnutý bol **hviezdicový model (star schema)**, pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **`fact_InvoiceLine`**, ktorá je prepojená s nasledujúcimi dimenziami:

- **`Dim_Date`**: Informácie o dátume a času invoicu (rok, mesiac, deň, deň v týždni, štvrťrok, čas). 
- **`Dim_Customer`**: Informácie o zákazníkoch (Krstné Meno, Priezvisko, adresa, mesto, poštové smerovacie číslo).
- **`Dim_Track`**: Informácie o skladbe (Názov, skladatel, dĺzka, žáner, interpert, typ medií).
- **`Dim_Emplozee`**: Základné info o zamestnancoch(Meno, dátum najatia, adresa, PSČ)

<p align="center">
  <img src="https://github.com/Rekovo/Chinook-ETL/blob/main/Star_Schema.png" alt="Star_Schema.png">
</p>

---
## **3. ETL proces v Snowflake**
ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**
Dáta zo zdrojového datasetu (formát `.csv`) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom `CHIPMUNK_DB_STAGE`. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom:

#### Príklad kódu:
```sql
CREATE OR REPLACE STAGE CHIPMUNK_DB_STAGECHIPMUNK_DB.STAGING.CHIPMUNK_DB_STAGE;
```
Do stage boli následne nahraté súbory. Dáta boli importované do staging tabuliek pomocou príkazu `COPY INTO`. Pre každú tabuľku sa použil podobný príkaz:

```sql
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
```

Parameter ON_ERROR = 'CONTINUE' zabezpečil pokračovanie procesu bez prerušenia ak by vyskočila nejaka nekonzistencia v záznamoch.

### **3.2 Transfor (Transformácia dát)**
Transformácie zahŕňali vytvorenie dimenzionálných tabuliek.

Príklad vytvorenia **dimenzionalnej tabulky (Dim_Employee)**:

```sql

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

```
Príklad vytvorenia **faktovej tabulky (Fact_InvoiceLine)**:
```sql
CREATE OR REPLACE TABLE fact_invoiceline AS 
SELECT
  InvoiceLineId,
  InvoiceId,
  TrackId,
  UnitPrice,
  Quantity,
  (UnitPrice * Quantity) AS Total
FROM InvoiceLine;
```


### **3.3 Load (Načítanie dát)**
Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:
```sql
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
```

## **4 Vizualizácia dát**
Dashboard obsahuje 5 vizualizácií, ktoré poskytujú prehľad o dôležitých metrikách:

<p align="center">
  <img src="https://github.com/Rekovo/Chinook-ETL/blob/main/Chinook_Dashboard.png" alt="ERD Schema">
</p>

---
### **1. Celková tržba podľa krajiny (Na základe faktúr)**
Tento query vypočíta celkové tržby pre každú krajinu na základe faktúr.

```sql
CHIPMUNK_DB.STAGING.CHIPMUNK_DB_STAGE;

SELECT 
    BillingCountry AS Country,
    SUM(Total) AS TotalRevenue
FROM Invoice
GROUP BY BillingCountry
ORDER BY TotalRevenue DESC;
```

### **2.Počet predaných skladieb podla žáneru**
Query sumarizuje predajnosť skladieb podľa žánrov.

```sql
CHIPMUNK_DB.STAGING.CHIPMUNK_DB_STAGE;

SELECT 
    Genre.Name AS Genre,
    SUM(InvoiceLine.Quantity) AS TotalTracksSold
FROM InvoiceLine
JOIN Track ON InvoiceLine.TrackId = Track.TrackId
JOIN Genre ON Track.GenreId = Genre.GenreId
GROUP BY Genre.Name
ORDER BY TotalTracksSold DESC;

```

### **3.Počet zákazníkov na daného Support-reprezentatíva (podľa ID)**
Query analyzuje počet zákazníkov, ktorých obslúžil každý zamestnanec podpory.

```sql
SELECT 
    E.EmployeeId AS SupportRepId,
    CONCAT(E.FirstName, ' ', E.LastName) AS SupportRepName,
    COUNT(C.CustomerId) AS TotalCustomersServed
FROM Employee E
JOIN Customer C ON E.EmployeeId = C.SupportRepId
GROUP BY E.EmployeeId, E.FirstName, E.LastName
ORDER BY TotalCustomersServed DESC;
```

### **4.Najpredávanejší Album od top 10 Artistov**
Query vráti 10 najpredávanejších interpretov a ich albumy,

```sql
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
```

### **5.Tržba na Quaterly úrovni**
query zobrazuje tržby na štrvťrokovej úrovni

```sql
CHIPMUNK_DB.STAGING.CHIPMUNK_DB_STAGE;

SELECT 
    TO_CHAR(InvoiceDate, 'YYYY-Q') AS YearQ,
    SUM(Total) AS TotalRevenue
FROM Invoice
GROUP BY TO_CHAR(InvoiceDate, 'YYYY-Q')
ORDER BY YearQ ASC;
```

---

Autor: Štefan Krajczár
