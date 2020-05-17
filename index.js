'use strict';
const fs = require('fs');
const cluster = require('cluster');

const blocksFetcher = require('./blocks_fetcher');
const blocksProcessor = require('./blocks_processor');

const BLOCK_INSERT_SIZE = 10000;

const sqliteDIR = './db';

if (!fs.existsSync(sqliteDIR))
    fs.mkdirSync(sqliteDIR);


if (cluster.isMaster) {
    const blocksProcessor = cluster.fork();
    blocksFetcher(blocksProcessor, BLOCK_INSERT_SIZE);
}
else {
    blocksProcessor(BLOCK_INSERT_SIZE);
}
