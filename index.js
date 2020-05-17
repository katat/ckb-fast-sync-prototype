'use strict';

const https = require('https');
const http = require('http');
const async = require('async');
const sqlite3 = require('sqlite3').verbose();
const CKB = require('@nervosnetwork/ckb-sdk-core').default;

const CKB_RPC_URL = process.env.CKB_RPC_URL || 'http://localhost:8114';
const BLOCK_FETCH_NUM = process.env.BLOCK_FETCH_NUM || 10;
const BLOCK_INSERT_SIZE = 10000;
const FORCE_DRAIN_INTERVAL = 100000;

const initSDK = (url) => {
    const ckb = new CKB(url);
    if (url.startsWith('https')) {
        const httpsAgent = new https.Agent({
            keepAlive: true
        });
        ckb.rpc.setNode({
            url,
            httpsAgent
        });
    }
    else {
        const httpAgent = new http.Agent({
            keepAlive: true
        });
        ckb.rpc.setNode({
            url,
            httpAgent
        });
    }
    return ckb;
};

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
    return new Promise((resolve, reject) => {
        db.run(
            `
                CREATE TABLE IF NOT EXISTS blocks (
                    number integer NOT NULL,
                    hash text NOT NULL,
                    parent_hash text NOT NULL,
                    timestamp text NOT NULL
                );
            `, 
            (err) => {
                if (err) {
                    console.error(err);
                    return reject(err);
                }
                resolve();
            });
    });
};

const insertBlocks = (db, blocks) => {
    return new Promise((resolve, reject) => {
        console.log(`processing blocks to ${BigInt(blocks[blocks.length - 1].header.number)}`);
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

(
    async () => {
        const ckb = initSDK(CKB_RPC_URL);
        const db = await connectDB('./db/ckb.sqlite');

        await createTables(db);

        const dbCargo = async.cargo(async (blocks) => {
            console.time(`inserting ${blocks.length} blocks to #: ${BigInt(blocks[blocks.length - 1].header.number)}`);
            await insertBlocks(db, blocks);
            console.timeEnd(`inserting ${blocks.length} blocks to #: ${BigInt(blocks[blocks.length - 1].header.number)}`);
        }, BLOCK_INSERT_SIZE);

        console.time(`fetch blocks`);
        const fetcherCargo = async.cargoQueue(async (tasks) => {
            const firstTask = tasks[0];
            const lastTask = tasks[tasks.length - 1];
            const startNumber = firstTask[0];
            const endNumber = lastTask[lastTask.length - 1];

            const results = await Promise.all(
                tasks.map(
                    blockNumbers => Promise.all(
                        blockNumbers.map(
                            number => ckb.rpc.getBlockByNumber(number)
                        )
                    )
                )
            );
            console.log('proceeded block number:', endNumber);
            console.timeLog(`fetch blocks`);

            const blocks = results.flat(1);
            dbCargo.push(blocks);

            if (startNumber % BigInt(FORCE_DRAIN_INTERVAL) === 0n) {
                await dbCargo.drain();
            }

            return results;
        }, 2, 10);

        const tipBlockNumber = await ckb.rpc.getTipBlockNumber();
        for (let i = 0;; i++) {
            const startNumber = BLOCK_FETCH_NUM * i;
            let endNumber = BLOCK_FETCH_NUM * (i + 1);

            if (endNumber > tipBlockNumber) {
                endNumber = tipBlockNumber;
                break;
            }

            const blockNumbers = Array.from(
                new Array(endNumber - startNumber),
                (x, i) => BigInt(startNumber + i)
            );

            // console.time(`fetch blocks ${startNumber} - ${endNumber}`);
            fetcherCargo.push([blockNumbers], (err, completedTasks) => {
                // console.timeEnd(`fetch blocks ${startNumber} - ${endNumber}`);
            });
        }

        await fetcherCargo.drain();
        await dbCargo.drain();

    }
)();