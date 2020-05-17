'use strict';
const sqlite3 = require('sqlite3').verbose();
const async = require('async');

const BLOCK_INSERT_SIZE = 100000;

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

const createTables = (db) => {
    const createTableSqls = [
        [
            `CREATE TABLE IF NOT EXISTS blocks (
                number integer NOT NULL,
                hash text NOT NULL,
                parent_hash text NOT NULL,
                timestamp text NOT NULL
            );`,
            'CREATE UNIQUE INDEX IF NOT EXISTS blocks_hash_key ON blocks(hash);',
            'CREATE UNIQUE INDEX IF NOT EXISTS blocks_number ON blocks(number);'
        ],
        [
            `CREATE TABLE IF NOT EXISTS transactions (
                hash text NOT NULL,
                block_number integer NOT NULL
            );`,
            'CREATE UNIQUE INDEX IF NOT EXISTS transactions_key ON transactions(hash);',
            'CREATE INDEX IF NOT EXISTS transactions_block_number ON transactions(block_number);'
        ],
        [
            `CREATE TABLE IF NOT EXISTS cells (
                transaction_hash text NOT NULL,
                "index" integer NOT NULL,
                capacity text NOT NULL,
                lock_code_hash text,
                lock_hash_type text,
                lock_args text,
                type_code_hash text,
                type_hash_type text,
                type_args text
            );`,
            'CREATE UNIQUE INDEX IF NOT EXISTS cells_key ON cells(transaction_hash,"index");',
            'CREATE INDEX IF NOT EXISTS cells_lock ON cells(lock_code_hash,lock_hash_type,lock_args);'
        ],
        [
            `CREATE TABLE IF NOT EXISTS transactions_cells (
                transaction_hash text NOT NULL,
                cell_index integer NOT NULL,
                is_input boolean NOT NULL
            );`,
            'CREATE UNIQUE INDEX IF NOT EXISTS transactions_cells_key ON transactions_cells(transaction_hash, cell_index);'
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

const insertBlocks = (db, blocks) => {
    return new Promise((resolve) => {
        db.serialize(() => {
            db.run(`
                BEGIN
            `);

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
                    }
                } = block;

                statement.run([
                    number,
                    hash,
                    parentHash,
                    timestamp
                ]);
            }

            db.run('COMMIT', () => {
                resolve();
            });

        });
    });
};

const blocksProcessor = async () => {
    const db = await connectDB('./db/ckb.sqlite');
    await createTables(db);
    
    const dbCargo = async.cargo(async (blocks) => {
        console.time(`inserting ${blocks.length} blocks to #${BigInt(blocks[blocks.length - 1].header.number)}`);
        await insertBlocks(db, blocks);
        console.timeEnd(`inserting ${blocks.length} blocks to #${BigInt(blocks[blocks.length - 1].header.number)}`);
    }, BLOCK_INSERT_SIZE);
    
    process.on('message', msg => {
        const {
            blocks
        } = msg;
        dbCargo.push(blocks);
    });
};

module.exports = blocksProcessor;