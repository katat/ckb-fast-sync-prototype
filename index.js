'use strict';

const https = require('https');
const http = require('http');
const async = require('async');
const CKB = require('@nervosnetwork/ckb-sdk-core').default;

const CKB_RPC_URL = process.env.CKB_RPC_URL || 'http://localhost:8114';
const BLOCK_FETCH_NUM = process.env.BLOCK_FETCH_NUM || 10;

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

(
    async () => {
        const ckb = initSDK(CKB_RPC_URL);

        const fetcherCargo = async.cargoQueue(async (tasks) => {
            const results = await Promise.all(
                tasks.map(
                    blockNumbers => Promise.all(
                        blockNumbers.map(
                            number => ckb.rpc.getBlockByNumber(number)
                        )
                    )
                )
            );
            return results;
        }, 2, 10);

        const tipBlockNumber = await ckb.rpc.getTipBlockNumber();
        for (let i = 0; ; i++) {
            const startNumber = BLOCK_FETCH_NUM * i;
            let endNumber = BLOCK_FETCH_NUM * (i + 1) - 1;
            if (endNumber > tipBlockNumber) {
                endNumber = tipBlockNumber;
                break;
            }
            
            const blockNumbers = Array.from(
                new Array(endNumber - startNumber), 
                (x, i) => BigInt(startNumber + i)
            );

            console.time(`fetch blocks ${startNumber} - ${endNumber}`);
            fetcherCargo.push([blockNumbers], function (err, results) {
                console.timeEnd(`fetch blocks ${startNumber} - ${endNumber}`);
            });
        }

        await fetcherCargo.drain();

    }
)();