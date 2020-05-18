# ckb-full-index-prototype

## Introduction
This is a prototype aiming to demonstrate for full indexing for a CKB node, using pure javascript and a sqlite database.

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
