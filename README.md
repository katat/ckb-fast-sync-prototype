# ckb-full-index-prototype

## Introduction
This is a prototype aiming to demonstrate full indexing for a CKB node with `sqlite` database on `nodejs`.

Several table schemas have been drafted to capture the relationships between `cells`, `transactions` and `blocks`. 
There are also indexes provided for these schemas to facilitate navigation between the relationship. 

The table `transactions_cells` encapsulates the relationships between the input/output cells and the transactions. Meanwhile, this table will allow to lookup live cells under a lock script, so as to facilitate in deriving other operations such as balance calculations.

## Setup
```
npm i
```

## Run
```
npm start
```

## Sqlite Data
The `./db` folder will hold the sqlite data. Deleting this folder will reset the indexing data.
