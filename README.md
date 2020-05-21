# Convert SQL to Cypher

There are some examples which come from (neo4j)[https://neo4j.com/developer/guide-sql-to-cypher/]

```sql
SELECT p.*
FROM products as p;
```
->
```cypher
MATCH (p:Product)
RETURN p;
```

Some more complex examples:
```sql
SELECT p.ProductName, p.UnitPrice
FROM products as p
ORDER BY p.UnitPrice DESC
LIMIT 10;
```
->
```cypher
MATCH (p:Product)
RETURN p.productName, p.unitPrice
ORDER BY p.unitPrice DESC
LIMIT 10;
```

# Plan
