'use strict';
const sqlite3 = require('sqlite3').verbose();
const async = require('async');

const connectDB = (filePath) => {
    return new Promise((resolve, reject) => {
        const db = new sqlite3.Database(
            filePath,
            (err) => {
                if (err) {
                    console.error(err.message);
                    return reject(err);
                }

                console.log('Connected to the database.');
                resolve(db);
            });
    });
};

const sqlitePragmas = (db) => {
    const pragmas = [
        // 'PRAGMA TEMP_STORE=2',
        'PRAGMA JOURNAL_MODE=WAL',
        'PRAGMA SYNCHRONOUS=0',
        'PRAGMA LOCKING_MODE=EXCLUSIVE'
    ];

    return new Promise((resolve) => {
        db.serialize(() => {
            for (const pragma of pragmas) {
                db.run(pragma, (err) => {
                    if (err) 
                        console.error(pragma, err);
                });
            }
            resolve();
        });
    });
};

const createTables = (db) => {
    const createTableSqls = [
        [
            `CREATE TABLE IF NOT EXISTS blocks (
                number integer NOT NULL,
                hash text NOT NULL,
                parent_hash text NOT NULL,
                timestamp text NOT NULL
            );`
        ],
        [
            `CREATE TABLE IF NOT EXISTS transactions (
                hash text NOT NULL,
                block_number integer NOT NULL
            );`
        ],
        [
            `CREATE TABLE IF NOT EXISTS cells (
                transaction_hash text NOT NULL,
                cell_index integer NOT NULL,
                capacity text NOT NULL,
                lock_code_hash text,
                lock_hash_type text,
                lock_args text,
                type_code_hash text,
                type_hash_type text,
                type_args text
            );`
        ],
        [
            `CREATE TABLE IF NOT EXISTS transactions_cells (
                transaction_hash text NOT NULL,
                cell_index integer NOT NULL,
                is_input boolean NOT NULL
            );`
        ]
    ];
    return new Promise((resolve, reject) => {
        db.serialize(() => {
            db.run('BEGIN');
            for (const tableSqls of createTableSqls) {
                for (const sql of tableSqls) {
                    db.run(sql, (err) => {
                        if (err) 
                            console.error(sql, err);
                        
                    });
                }
            }
            db.run('COMMIT', (err) => {
                if (err) {
                    console.error(err);
                    return reject(err);
                }
                resolve();
            });
        });
    });
};

const createIndexes = (db) => {
    const createIndexeSqls = [
        [
            'CREATE UNIQUE INDEX IF NOT EXISTS blocks_hash_key ON blocks(hash);',
            'CREATE UNIQUE INDEX IF NOT EXISTS blocks_number ON blocks(number);'
        ],
        [
            'CREATE UNIQUE INDEX IF NOT EXISTS transactions_key ON transactions(hash);',
            'CREATE INDEX IF NOT EXISTS transactions_block_number ON transactions(block_number);'
        ],
        [
            'CREATE UNIQUE INDEX IF NOT EXISTS cells_key ON cells(transaction_hash,cell_index);',
            'CREATE INDEX IF NOT EXISTS cells_lock ON cells(lock_code_hash,lock_hash_type,lock_args);'
        ],
        [
            'CREATE INDEX IF NOT EXISTS transactions_cells_key ON transactions_cells(transaction_hash, cell_index, is_input);'
        ]
    ];
    return new Promise((resolve, reject) => {
        db.serialize(() => {
            db.run('BEGIN');
            for (const tableSqls of createIndexeSqls) {
                for (const sql of tableSqls) {
                    db.run(sql, (err) => {
                        if (err) 
                            console.error(sql, err);
                        
                    });
                }
            }
            db.run('COMMIT', (err) => {
                if (err) {
                    console.error(err);
                    return reject(err);
                }
                resolve();
            });
        });
    });
};

const insertBlocks = (db, blocks) => {
    return new Promise((resolve) => {
        db.serialize(() => {
            const statement = db.prepare(`
                INSERT INTO blocks (number, hash, parent_hash, timestamp) VALUES (?,?,?,?)
            `);
            for (const block of blocks) {
                const {
                    header: {
                        number,
                        hash,
                        parentHash,
                        timestamp
                    },
                    transactions
                } = block;

                statement.run([
                    number,
                    hash,
                    parentHash,
                    timestamp
                ]);
                runInsertTransactionsStatements(db, number, transactions);
            }

            db.run('COMMIT', () => {
                resolve();
            });
        });
    });
};

const runInsertTransactionsStatements = (db, blockNumber, txs) => {
    const statement = db.prepare(`
        INSERT INTO transactions (hash, block_number) VALUES (?,?)
    `);
    for (const tx of txs) {
        const {hash} = tx;

        statement.run([
            hash,
            blockNumber
        ]);

        runInsertCellsStatements(db, tx);
    }
};

const runInsertCellsStatements = (db, tx) => {
    const insertCellStatement = db.prepare(`
        INSERT INTO cells (
            transaction_hash, 
            cell_index, 
            capacity, 
            lock_code_hash, 
            lock_hash_type, 
            lock_args, 
            type_code_hash, 
            type_hash_type, 
            type_args
        ) VALUES (?,?,?,?,?,?,?,?,?)
    `);
    const insertTransactionsCellsStatement = db.prepare(`
        INSERT INTO transactions_cells (
            transaction_hash,
            cell_index,
            is_input
        ) VALUES (?,?,?)
    `);
    const {hash, inputs, outputs} = tx;

    for (let index = 0; index < outputs.length; index++) {
        const output = outputs[index];
        const {lock, type, capacity} = output;

        const hexIndex = `0x${index.toString(16)}`;

        let parameters = [
            hash,
            hexIndex,
            capacity,
            lock.codeHash,
            lock.hashType,
            lock.args
        ];

        if (type) 
            parameters = parameters.concat([type.codeHash, type.hashType, type.args]);
        else 
            parameters = parameters.concat([null, null, null]);
        
        insertCellStatement.run(parameters);
        insertTransactionsCellsStatement.run([hash, hexIndex, false]);
    }

    for (const input of inputs) {
        const {previousOutput: {txHash, index}} = input;
        const parameters = [txHash, index, true];
        insertTransactionsCellsStatement.run(parameters);
    }
};

const startIndexingBlocks = async (BLOCK_INSERT_SIZE) => {
    const db = await connectDB('./db/ckb.sqlite');
    await createTables(db);
    await sqlitePragmas(db);
    // await createIndexes(db);
    
    const dbCargo = async.cargo(async (blocks) => {
        const lastBlockNumber = blocks[blocks.length - 1].header.number;
        console.time(`inserting ${blocks.length} blocks to #${BigInt(lastBlockNumber)}`);
        await insertBlocks(db, blocks);
        console.timeEnd(`inserting ${blocks.length} blocks to #${BigInt(lastBlockNumber)}`);
    }, BLOCK_INSERT_SIZE);
    
    process.on('message', async msg => {
        const {blocks} = msg;
        const lastBlock = blocks[blocks.length - 1];
        if (lastBlock === null) 
            blocks.pop();
        
        dbCargo.push(blocks);

        if (lastBlock === null) {
            await dbCargo.drain();
            console.time('create table indexes');
            await createIndexes(db);
            console.timeEnd('create table indexes');
        }
    });

};

module.exports = {
    startIndexingBlocks,
    createIndexes
};