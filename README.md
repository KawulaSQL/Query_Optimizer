# MEGA Project - mini Database Management System Development (Query Optimizer Component)

## Table of Contents
* [General Info](#general-information)
* [Overview](#overview)
* [Class Descriptions](#class-descriptions)
* [Technologies Used](#technologies-used)
* [Setup and Usage](#setup-and-usage)
* [Team Members](#team-members)

## **General Info**

This component accepts a query as input, parses and validates it, and returns an optimized query plan in the form of a parsed query object. 

## **Overview**

The Query Optimizer is designed to optimize queries that have already been defined in the DBMS. It returns both the optimized query and the corresponding query plan that will be executed in the DBMS.

## **Class Descriptions**

1. **Query Optimizer**
The `QueryOptimizer` class manages the transformation of a query into a `ParsedQuery`. It allows for query optimization upon initialization. The main functions of this class include:
    - parse():
        - Converts the input query into a `ParsedQuery`.
        - Stores the original query and constructs a query tree by separating clauses for sequential execution. The top to bottom sequence is: LIMIT, ORDER, GROUP, PROJECT, SIGMA (selection), JOIN/NATURAL JOIN, and TABLE.

    - optimize(): <br>
        - Generates and evaluates alternative query execution plans using relational algebra rules.
        - Analyzes nodes of the query tree recursively and applies transformation rules based on node types (e.g., selection, projection, joins).
        - Evaluates each generated query plan for cost using `get_cost()`, selecting the plan with the minimum cost as the optimal execution plan.

    - get_cost():
        - Estimates the execution cost of a query tree.
        - Evaluates costs starting from the leaf nodes and utilizes statistics from the `get_stats` function provided by the storage manager during initialization.
        - Validates that columns are from the selected tables.

2. **ParsedQuery**
The `ParsedQuery` class represents a parsed query with two attributes:
    - **query**: The original query provided to the query optimizer.
    - **query_tree**: A `QueryTree` that details the internal representation of the query.

3. **QueryTree**
The QueryTree class represents the structure of the query tree that dictates how a query is executed. Key attributes include:
    - **type**: Indicates the node type (e.g., table, join operation, selection, projection).
    - **val**: Contains the value of the node (e.g., table name for type = 'table').
    - **condition**: Specifies conditions for selection type nodes (e.g., age > 50).
    - **child**: Contains child nodes of the current node.
    - **parent**: Refers to the parent node.
    - **total_block**: Represents the total blocks after preceding operations.
    - **total_row**: Represents the total rows after preceding operations.
    - **columns**: Lists available columns from the children of the node.

## **Technologies Used**

- Python 3

## **Setup and Usage**

1. Clone this project:
```
https://github.com/KawulaSQL/Query_Optimizer.git
```

2. Navigate to the root directory and run the command:
```
python driver.py
```

3. To run unit tests, execute the following command from the root directory:
```
python -m unittest tests/test_query_optimizer.py
```


## **Team Members**

| Nama            | NIM      | Workload |
| --------------- | -------- | -------- |
| Fahrian Afdholi | 13521031 | setup repo and file query optimizer, validation functions, models, parse(), checkpoint document 1,2,3, class diagram, final document |
| Ferindya Aulia Berlianty  | 13521161 | optimize(), final document |
| Eduardus Alvito Kristiadi   | 13522004 | optimize(), final document, checkpoint document 1,2,3 |
| Francesco Michael Kusuma   | 13522038 | get_cost(), checkpoint document 1,2,3, final document, unit test |
| Muhammad Neo Cicero Koda   | 13522108 | get_cost(), checkpoint document 1,2,3, final document, validation functions |