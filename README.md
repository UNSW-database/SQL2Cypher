[![License](https://img.shields.io/badge/License-Flask-blue.svg)](https://flask.palletsprojects.com/en/1.1.x/) [![License](https://img.shields.io/badge/License-Python3-blue.svg)](https://www.python.org/) ![passing](https://img.shields.io/badge/build-passing-green)

# Reference
The sql2cypher project is based on 'SQL2Cypher: Automated Data and Query Migration from RDBMS to GDBMS' paper.

Autho list: Shunyang Li, Zhengyi Yang, Xianhang Zhang, Xuemin Lin and Wenjie Zhang.

Corresponding author contact: sli@cse.unsw.edu.au

# Attention
Before convert sql to cypher make sure the programming has permission to write files. 
You can execute the following code to get enough permission for python3:
```shell script
sudo chmod 777 /var/lib/neo4j/import
```

# Configuration and usage
Usage:
```shell script
python3 sql2cypher.py --help
```

## Config
Before execute the code, please make sure done the config:
```shell script
cd conf
vim db.ini
```
Please complete the config then it works.

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
python3 sql2cypher.py -s < sql.txt
# or you can type sql by yourself
python3 sql2cypher.py
```
In `sql.txt` you can find more examples.
For test whether the syntax correct, you can import the data to MySQL and NEO4J.
You can read how to [import](/data/README.md) 

# Required packages

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

2. [cypher](https://www.w3cschool.cn/neo4j/neo4j_cql_match_command.html)
    This is the cypher tutorial.

3. [moz-sql-parser](https://github.com/mozilla/moz-sql-parser)
    It can parser the SQL into a JSON format
    
   ```json
   '{"select": [{"value": "p.ProductName"}, {"value": "p.UnitPrice"}], "from": {"value": "products", "name": "p"}, "orderby": {"value": "p.UnitPrice", "sort": "desc"}, "limit": 10}'
   ```

4. [py2neo](https://py2neo.org/v4/index.html)
   similar with neomodel

# How to add relation for tables
Firstly, you need to choose `2. Convert the whole database to cypher` and then it will display all the tables which in your database. 
Then you need to add relation like this format: Table->Table: Relation | Table<->Table: Relation (eg: A->B: Working).
`->` means one to one relation from a to b, `<->` a has relation to b and b has relation to a.

# Get relation between tables
```mysql
SELECT `TABLE_NAME`,  `REFERENCED_TABLE_NAME`, `REFERENCED_COLUMN_NAME`
FROM `INFORMATION_SCHEMA`.`KEY_COLUMN_USAGE` 
WHERE `TABLE_SCHEMA` = SCHEMA() 
    AND `REFERENCED_TABLE_NAME` IS NOT NULL;
```

Show table schema:
```mysql
SELECT COLUMN_NAME FROM   INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'table_name';
```

It was stored as dict format and the name of table is key, `to` is the relation table, `on` is the foreign key, 
`relation` is the relationship between two tables. If the table does not have any relation the value will be None. 
And it looks like:
```json
[
   {
      "departments":{
         "to":"dept_manager",
         "on":"dept_no",
         "relation":"departments_dept_manager"
      
      }
  },
  {
      "employee": None
  }
]
```
