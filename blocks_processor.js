'use strict';

const {parentPort, workerData} = require('worker_threads');

parentPort.postMessage(workerData);