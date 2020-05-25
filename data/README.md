# How to import data to test SQL and Cypher

1. Import the `data.sql` into My SQL
2. Copy `data.csv` under `/var/lib/neo4j/import` folder.
3. Execute the following code in neo4j:

```cypher
LOAD CSV FROM 'file:///data.csv' AS row
WITH row[0] AS Code, row[1] AS Name, row[2] AS Address, row[3] AS Zip, row[4] as Country
MERGE (company:Company {Code: Code})
  SET company.Name = Name, company.Address = Address, company.Zip = Zip, company.Country = Country
```