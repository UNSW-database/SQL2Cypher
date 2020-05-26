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

In cypher, when we create node in cypher we need node name and label name like `CREATE (p:Product)`. 
So when execute the sql query, it should like this format: `SELECT * FROM Product as p` (we need to known the label name).

# How to use it

```shell script
python3 sql2cypher < sql.txt
# or you can type sql by yourself
python3 sql2cypher
```
In `sql.txt` you can find more examples.
For test whether the syntax correct, you can import the data to MySQL and NEO4J.
You can read how to [import](/data/README.md) 

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

2. [cypher](https://www.w3cschool.cn/neo4j/neo4j_cql_match_command.html)
    This is the cypher tutorial.

3. [moz-sql-parser](https://github.com/mozilla/moz-sql-parser)
    It can parser the SQL into a JSON format
    
   ```json
   '{"select": [{"value": "p.ProductName"}, {"value": "p.UnitPrice"}], "from": {"value": "products", "name": "p"}, "orderby": {"value": "p.UnitPrice", "sort": "desc"}, "limit": 10}'
   ```

# TODO

- [ ] Convert MySQL database into Neo4J with relation.
- [ ] Make join SQL query works for neo4j.