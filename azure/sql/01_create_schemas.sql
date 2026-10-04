-- Azure SQL Database (serving Power BI) : schemas alignes sur le projet local (dbt).
-- Idempotent : peut etre rejoue sans effet. A executer avec sqlcmd ou l'editeur de requetes
-- du portail, une fois connecte a la base ${SQL_DATABASE_NAME} (jamais a `master`).

-- staging : zone d'atterrissage ecrite par Databricks (03_gold_to_sql.py), videe a chaque run.
IF SCHEMA_ID(N'staging') IS NULL EXEC (N'CREATE SCHEMA staging');
GO

-- intermediate : reserve, reflete le schema dbt du meme nom. Non utilise par le serving
-- (les calculs intermediaires sont faits dans Databricks, cf. 02_silver_to_gold.py).
IF SCHEMA_ID(N'intermediate') IS NULL EXEC (N'CREATE SCHEMA intermediate');
GO

-- marts : schema en etoile consomme par Power BI (dimensions, faits, vue).
IF SCHEMA_ID(N'marts') IS NULL EXEC (N'CREATE SCHEMA marts');
GO
