'use strict';

const https = require('https');
const http = require('http');
const async = require('async');
const CKB = require('@nervosnetwork/ckb-sdk-core').default;

const CKB_RPC_URL = process.env.CKB_RPC_URL || 'http://localhost:8114';
const BLOCK_FETCH_NUM = process.env.BLOCK_FETCH_NUM || 5;
const TARGET_BLOCK_NUMBER = process.env.TARGET_BLOCK_NUMBER;
const FORCE_DRAIN_INTERVAL = process.env.FORCE_DRAIN_INTERVAL || 100000;

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

const initFetcher = async (blocksProcessor, BLOCK_INSERT_SIZE) => {
    const ckb = initSDK(CKB_RPC_URL);

    const processorMessgerCargo = async.cargo((blocks, callback) => {
        blocksProcessor.send({
            blocks
        }, () => {
            callback();
        });
    }, BLOCK_INSERT_SIZE);

    console.time('total lasted time');
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
        if (startNumber % 1000n === 0n) 
            console.timeLog('total lasted time', ' ~ proceeded block number:', endNumber);
                

        const blocks = results.flat(1);
        processorMessgerCargo.push(blocks);

        if (startNumber % BigInt(FORCE_DRAIN_INTERVAL) === 0n)
            await processorMessgerCargo.drain();

        return results;
    }, 1, 5);

    const tipBlockNumber = TARGET_BLOCK_NUMBER || await ckb.rpc.getTipBlockNumber();
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

        fetcherCargo.push([blockNumbers]);
    }

    await fetcherCargo.drain();
};

module.exports = initFetcher;