---
title: "Top K by group"
description: "Find the top- or bottom-K elements in a group."
weight:
aliases:
  - /sql/idioms
  - /guides/top-k
  - /sql/patterns/top-k/
menu:
  main:
    parent: 'sql-patterns'
disable_toc: true
---

## Top K using a `LATERAL` subquery and `LIMIT`

Suppose you want to group rows in a table by some key, then filter out all but
the first _K_ elements within each group according to some ordering. In other
databases, you might use window functions. In Materialize, we recommend using a
[`LATERAL` subquery](/transform-data/join/#lateral-subqueries). The general form of the
query looks like this:

```mzsql
SELECT * FROM
    (SELECT DISTINCT key_col FROM tbl) grp,
    LATERAL (
        SELECT col1, col2... FROM tbl
        WHERE key_col = grp.key_col
        ORDER BY order_col LIMIT k
    )
```

For example, suppose you have a relation containing the population of various
U.S. cities.

```mzsql
CREATE TABLE cities (
    name text NOT NULL,
    state text NOT NULL,
    pop int NOT NULL
);

INSERT INTO cities VALUES
    ('Los_Angeles', 'CA', 3979576),
    ('Phoenix', 'AZ', 1680992),
    ('Houston', 'TX', 2320268),
    ('San_Diego', 'CA', 1423851),
    ('San_Francisco', 'CA', 881549),
    ('New_York', 'NY', 8336817),
    ('Dallas', 'TX', 1343573),
    ('San_Antonio', 'TX', 1547253),
    ('San_Jose', 'CA', 1021795),
    ('Chicago', 'IL', 2695598),
    ('Austin', 'TX', 978908);
```

To fetch the three most populous cities in each state:

```mzsql
SELECT state, name FROM
    (SELECT DISTINCT state FROM cities) grp,
    LATERAL (
        SELECT name FROM cities
        WHERE state = grp.state
        ORDER BY pop DESC LIMIT 3
    );
```
```nofmt
AZ  Phoenix
CA  Los_Angeles
CA  San_Diego
CA  San_Jose
IL  Chicago
NY  New_York
TX  Houston
TX  San_Antonio
TX  Dallas
```

Despite the verbosity of the above query, Materialize produces a straightforward
plan:

```mzsql
EXPLAIN SELECT state, name FROM ...
```
```nofmt
Explained Query:
  Project (#1, #0)
    TopK group_by=[#1] order_by=[#2 desc nulls_first] limit=3
      ReadStorage materialize.public.cities
```

## Top 1 using `DISTINCT ON`

If _K_ = 1, i.e., you would like to see only the most populous city in each state, another approach is to use `DISTINCT ON`:

```mzsql
SELECT DISTINCT ON(state) state, name
FROM cities
ORDER BY state, pop DESC;
```
Note that the `ORDER BY` clause should start with the expressions that are in the `DISTINCT ON`.

## Group size hints

When using either the above `LATERAL` subquery pattern or `DISTINCT ON`, we recommend
specifying [query hints](/sql/select/#query-hints) to improve memory usage. For example:

```mzsql
SELECT state, name FROM
    (SELECT DISTINCT state FROM cities) grp,
    LATERAL (
        SELECT name FROM cities
        WHERE state = grp.state
        OPTIONS (LIMIT INPUT GROUP SIZE = 1000)
        ORDER BY pop DESC LIMIT 3
    );
```

or

```mzsql
SELECT DISTINCT ON(state) state, name
FROM cities
OPTIONS (DISTINCT ON INPUT GROUP SIZE = 1000)
ORDER BY state, pop DESC;
```

To learn more about how to set the values for the query hints above, see the [Optimization](/transform-data/optimization/#query-hints) page.
