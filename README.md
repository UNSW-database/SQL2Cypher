# Convert SQL to Cypher

There are some examples which come from [neo4j](https://neo4j.com/developer/guide-sql-to-cypher/)

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

# Useful packages

1. [sqlparse](https://sqlparse.readthedocs.io/en/latest/intro/)

   Sample example:

   ```python
   import sqlparse
   sql = 'select * from mytable where id = 1'
   res = sqlparse.parse(sql)
   print(res[0].tokens)
   
   """
   [<DML 'select' at 0x7FA0A944CE20>, <Whitespace ' ' at 0x7FA0A944CE80>, <Wildcard '*' at 0x7FA0A944CEE0>, <Whitespace ' ' at 0x7FA0A944CF40>, <Keyword 'from' at 0x7FA0A944CFA0>, <Whitespace ' ' at 0x7FA0A9454040>, <Identifier 'somesc...' at 0x7FA0A9445CF0>, <Whitespace ' ' at 0x7FA0A94541C0>, <Where 'where ...' at 0x7FA0A9445C80>]
   """
   ```

   Another [blog](https://blog.csdn.net/qq_39607437/article/details/79620383)

# Plan

To be determined