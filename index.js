'use strict';
const cluster = require('cluster');

const blocksFetcher = require('./blocks_fetcher');
const blocksProcessor = require('./blocks_processor');

if (cluster.isMaster) {
    const blocksProcessor = cluster.fork();
    blocksFetcher(blocksProcessor);
}
else {
    blocksProcessor();
}
