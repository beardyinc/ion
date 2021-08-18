import * as getRawBody from 'raw-body';
import * as Koa from 'koa';
import * as Router from 'koa-router';
import Ipfs from '@decentralized-identity/sidetree/dist/lib/ipfs/Ipfs';
import LogColor from '../bin/LogColor';
import {SidetreeConfig,} from '@decentralized-identity/sidetree';
import MongoDbTransactionStore from '@decentralized-identity/sidetree/dist/lib/common/MongoDbTransactionStore';
import JsonCanonicalizer from '@decentralized-identity/sidetree/dist/lib/core/versions/latest/util/JsonCanonicalizer';
import Multihash from '@decentralized-identity/sidetree/dist/lib/core/versions/latest/Multihash';
import Encoder from '@decentralized-identity/sidetree/dist/lib/core/versions/latest/Encoder';

const zlib = require('zlib');

/** Configuration used by this server. */
interface ServerConfig extends SidetreeConfig {
    /** IPFS HTTP API endpoint URI. */
    ipfsHttpApiEndpointUri: string;

    /** Port to be used by the server. */
    port: number;
    /** a 4byte string in the base64 alphabet to mark a gaia-x did */
    didType: string;
}

// Selecting core config file, environment variable overrides default config file.
let configFilePath = '/opt/ion/mainnet-crawler-config.json';
if (process.env.ION_CRAWLER_CONFIG_FILE_PATH === undefined) {
    console.log(LogColor.yellow(`Environment variable ION_CRAWLER_CONFIG_FILE_PATH undefined, using default core config path ${configFilePath} instead.`));
} else {
    configFilePath = process.env.ION_CRAWLER_CONFIG_FILE_PATH;
    console.log(LogColor.lightBlue(`Loading crawler config from ${LogColor.green(configFilePath)}...`));
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
router.get('/dids', async (ctx, _next) => {
    const type = ctx.request.query["type"];
    if (!type) {
        ctx.response.status = 400;
    } else {
        const maxFiles = ctx.request.query["limit"] | 20;

        let suffixes = new Array<any>();
        await getDidsWithType(type, didSuffixes => {
            suffixes.push(...didSuffixes);
        }, maxFiles);

        ctx.response.body = suffixes;
        ctx.response.status = 200;
    }
});

app.use(router.routes())
    .use(router.allowedMethods());

// Handler to return bad request for all unhandled paths.
app.use((ctx, _next) => {
    ctx.response.status = 400;
});

function createDidSuffix (op: any): string {
    // canonicalize json
    let buffer: Buffer = JsonCanonicalizer.canonicalizeAsBuffer(op);
    // multihash
    let multihash = Multihash.hash(buffer, 18);
    // encode to base64url

    let encodedMultihash = Encoder.encode(multihash);

    let shortFormDid = `did:ion:${encodedMultihash}`;

    return shortFormDid;
    // const initialState = {
    //     suffixData: op.suffixData,
    //     delta: op.delta
    // };
    // // Initial state must be canonicalized as per spec.
    // const canonicalizedInitialStateBuffer = JsonCanonicalizer.canonicalizeAsBuffer(initialState);
    // const encodedCanonicalizedInitialStateString = Encoder.encode(canonicalizedInitialStateBuffer);
    // const longFormDid = `${shortFormDid}:${encodedCanonicalizedInitialStateString}`;
    // return longFormDid;
}

async function getDidsWithType (didType: string, callback: (didSuffixes: string[]) => any, maxFiles: number = 20) {
    let transactionStore = new MongoDbTransactionStore();
    await transactionStore.initialize(config.mongoDbConnectionString, config.databaseName);
    console.log(`Parsing ${await transactionStore.getTransactionsCount()} transactions`);

    let allTransactions = await transactionStore.getTransactions();

    //travel back in time by reversing it
    let ipfsLookupCoreIndexFileHashes = allTransactions.reverse().map(trans => trans.anchorString);

    let count = 0;
    for (let encodedHash of ipfsLookupCoreIndexFileHashes) {
        let hash = encodedHash.split(".")[1];
        let buffer = (await cas.read(hash, 100000)).content;
        zlib.gunzip(buffer, (error: Error | null, result: Buffer) => {
            if (error) {
                // console.error(error);
            } else {
                let str = result.toString();
                let cif = JSON.parse(str);
                if (cif.operations && cif.operations.create) {
                    // @ts-ignore
                    let operationsWithGaiaxType = cif.operations.create.filter(co => co.suffixData.type === didType);
                    if (operationsWithGaiaxType.length > 0) {
                        let dids = operationsWithGaiaxType.map((op: any) => createDidSuffix(op));
                        callback(dids);
                    }
                }
            }
        });

        count++;
        if (count >= maxFiles) break;
    }
}

(async () => {
    try {
        // await sidetreeCore.initialize();

        const port = config.port;
        app.listen(port, () => {
            console.log(`Crawler node running on port: ${port}`);
        });

        // @ts-ignore
        // await getDidsWithType(config.didType, (didSuffixes: string[]) => {
        //     Logger.info(LogColor.lightBlue(`Found GAIA-X : ${didSuffixes.join(", ")}`));
        //
        // });
    } catch (error) {
        console.log(`Crawler node initialization failed with error ${error}`);
        process.exit(1);
    }
})();
