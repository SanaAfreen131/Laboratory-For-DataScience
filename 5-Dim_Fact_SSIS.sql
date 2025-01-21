-- Create the top 10% of Dim_Date
SELECT TOP 10 PERCENT *
INTO Dim_Date_SSIS
FROM Dim_Date
ORDER BY DateKey; 

-- Create the top 10% of Dim_Beat
SELECT TOP 10 PERCENT *
INTO Dim_Beat_SSIS
FROM Dim_Beat
ORDER BY Beatkey; 

-- Create the top 10% of Dim_Cause
SELECT TOP 10 PERCENT *
INTO Dim_Cause_SSIS
FROM Dim_Cause
ORDER BY Causekey; 

-- Create the top 10% of Dim_Client
SELECT TOP 10 PERCENT *
INTO Dim_Client_SSIS
FROM Dim_Client
ORDER BY clientkey; 

-- Create the top 10% of Dim_Vehicle
SELECT TOP 10 PERCENT *
INTO Dim_Vehicle_SSIS
FROM Dim_Vehicle
ORDER BY Vehiclekey; 

-- Create the top 10% of Dim_Crash
SELECT TOP 10 PERCENT *
INTO Dim_Crash_SSIS
FROM Dim_Crash
ORDER BY Crashkey; 

-- Create the top 10% of Fact_Crash by joining with the top 10% from all dimension tables
SELECT fc.*
INTO Fact_Crash_SSIS
FROM Fact_Crash fc
INNER JOIN Dim_Date_SSIS dd ON fc.DateKey = dd.DateKey
INNER JOIN Dim_Beat_SSIS db ON fc.Beatkey = db.Beatkey
INNER JOIN Dim_Cause_SSIS dc ON fc.Causekey = dc.Causekey
INNER JOIN Dim_Client_SSIS dcl ON fc.clientkey = dcl.clientkey
INNER JOIN Dim_Vehicle_SSIS dv ON fc.Vehiclekey = dv.Vehiclekey
INNER JOIN Dim_Crash_SSIS dcst ON fc.Crashkey = dcst.Crashkey;



-- Check for mismatched DateKey
SELECT fc.DateKey
FROM Fact_Crash_SSIS fc
LEFT JOIN Dim_Date_SSIS dd ON fc.DateKey = dd.DateKey
WHERE dd.DateKey IS NULL;

-- Check for mismatched BeatKey
SELECT fc.BeatKey
FROM Fact_Crash_SSIS fc
LEFT JOIN Dim_Beat_SSIS db ON fc.BeatKey = db.BeatKey
WHERE db.BeatKey IS NULL;

-- Check for mismatched CauseKey
SELECT fc.CauseKey
FROM Fact_Crash_SSIS fc
LEFT JOIN Dim_Cause_SSIS dc ON fc.CauseKey = dc.CauseKey
WHERE dc.CauseKey IS NULL;

-- Check for mismatched PersonID
SELECT fc.clientkey
FROM Fact_Crash_SSIS fc
LEFT JOIN Dim_Client_SSIS dcl ON fc.clientkey = dcl.clientkey
WHERE dcl.clientkey IS NULL;

-- Check for mismatched VehicleID
SELECT fc.Vehiclekey
FROM Fact_Crash_SSIS fc
LEFT JOIN Dim_Vehicle_SSIS dv ON fc.Vehiclekey = dv.Vehiclekey
WHERE dv.Vehiclekey IS NULL;

-- Check for mismatched CrashID
SELECT fc.Crashkey
FROM Fact_Crash_SSIS fc
LEFT JOIN Dim_Crash_SSIS dcst ON fc.Crashkey = dcst.Crashkey
WHERE dcst.Crashkey IS NULL;










