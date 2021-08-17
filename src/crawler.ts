import * as getRawBody from 'raw-body';
import * as Koa from 'koa';
import * as Router from 'koa-router';
import Ipfs from '@decentralized-identity/sidetree/dist/lib/ipfs/Ipfs';
import LogColor from '../bin/LogColor';
import {
    SidetreeConfig,
} from '@decentralized-identity/sidetree';
import MongoDbTransactionStore from '@decentralized-identity/sidetree/dist/lib/common/MongoDbTransactionStore';

const zlib = require('zlib');

/** Configuration used by this server. */
interface ServerConfig extends SidetreeConfig {
    /** IPFS HTTP API endpoint URI. */
    ipfsHttpApiEndpointUri: string;

    /** Port to be used by the server. */
    port: number;
}

// Selecting core config file, environment variable overrides default config file.
let configFilePath = '../json/testnet-bitcoin-config.json';
if (process.env.ION_CORE_CONFIG_FILE_PATH === undefined) {
    console.log(LogColor.yellow(`Environment variable ION_BITCOIN_CONFIG_FILE_PATH undefined, using default core config path ${configFilePath} instead.`));
} else {
    configFilePath = process.env.ION_CORE_CONFIG_FILE_PATH;
    console.log(LogColor.lightBlue(`Loading core config from ${LogColor.green(configFilePath)}...`));
}
const config: ServerConfig = require(configFilePath);

// Selecting versioning file, environment variable overrides default config file.
let versioningConfigFilePath = '../json/testnet-bitcoin-versioning.json';
if (process.env.ION_CORE_VERSIONING_CONFIG_FILE_PATH === undefined) {
    console.log(LogColor.yellow(`Environment variable ION_BITCOIN_VERSIONING_CONFIG_FILE_PATH undefined, using default core versioning config path ${versioningConfigFilePath} instead.`));
} else {
    versioningConfigFilePath = process.env.ION_CORE_VERSIONING_CONFIG_FILE_PATH;
    console.log(LogColor.lightBlue(`Loading core versioning config from ${LogColor.green(versioningConfigFilePath)}...`));
}

const ipfsFetchTimeoutInSeconds = 10;
// @ts-ignore
const cas = new Ipfs(config.ipfsHttpApiEndpointUri, ipfsFetchTimeoutInSeconds);

const app = new Koa();

// Raw body parser.
app.use(async (ctx, next) => {
    ctx.body = await getRawBody(ctx.req);
    await next();
});

const router = new Router();
router.get('/operations', async (ctx, _next) => {
    const types = ctx.request.query["type"];
    const since = ctx.request.query["since"];
    console.log(`querying for type ${types} since DID ${since}`);

    ctx.response.status = 200;
});

app.use(router.routes())
    .use(router.allowedMethods());

// Handler to return bad request for all unhandled paths.
app.use((ctx, _next) => {
    ctx.response.status = 400;
});

function sleep (ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

(async () => {
    try {
        // await sidetreeCore.initialize();

        let transactionStore = new MongoDbTransactionStore();
        await transactionStore.initialize(config.mongoDbConnectionString, config.databaseName);
        console.log(`Parsing ${await transactionStore.getTransactionsCount()} transactions`);

        const port = config.port;
        app.listen(port, () => {
            console.log(`Crawler node running on port: ${port}`);
        });

        let allTransactions = await transactionStore.getTransactions();

        let ipfsLookupCoreIndexFileHashes = allTransactions.map(trans => trans.anchorString);

        let ipfsHashes = ipfsLookupCoreIndexFileHashes.map(hash => hash.split(".")[1]);

        let docs = new Array<any>();
        for (let hash of ipfsHashes) {
            let buffer = (await cas.read(hash, 100000)).content;
            zlib.gunzip(buffer, (error: Error | null, result: Buffer) => {
                if (error) {
                    // console.error(error);
                } else {
                    let str = result.toString();
                    docs.push(str);
                }
            });
            await sleep(100);
        }
        // @ts-ignore
        let a = docs.length;
    } catch (error) {
        console.log(`Crawler node initialization failed with error ${error}`);
        process.exit(1);
    }
})();
