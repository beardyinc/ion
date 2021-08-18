import MongoDbTransactionStore from '@decentralized-identity/sidetree/dist/lib/common/MongoDbTransactionStore';
import JsonCanonicalizer from '@decentralized-identity/sidetree/dist/lib/core/versions/latest/util/JsonCanonicalizer';
import Multihash from '@decentralized-identity/sidetree/dist/lib/core/versions/latest/Multihash';
import Encoder from '@decentralized-identity/sidetree/dist/lib/core/versions/latest/Encoder';
import ICas from '@decentralized-identity/sidetree/dist/lib/core/interfaces/ICas';

const zlib = require('zlib');

export default class Crawler {
    private readonly mongoConnectionString: string;
    private readonly databaseName: string;
    private cas: ICas;

    constructor (mongoConnectionString: string, databaseName: string, cas: ICas) {
        this.mongoConnectionString = mongoConnectionString;
        this.databaseName = databaseName;
        this.cas = cas;
    }

    public createDidSuffix (op: any): string {
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

    public async getDidsWithType (didType: string, callback: (didSuffixes: string[]) => any, maxFiles: number = 20) {
        let transactionStore = new MongoDbTransactionStore();
        await transactionStore.initialize(this.mongoConnectionString, this.databaseName);
        console.log(`Parsing ${await transactionStore.getTransactionsCount()} transactions`);

        let allTransactions = await transactionStore.getTransactions();

        //travel back in time by reversing it
        let ipfsLookupCoreIndexFileHashes = allTransactions.reverse().map(trans => trans.anchorString);

        let count = 0;
        for (let encodedHash of ipfsLookupCoreIndexFileHashes) {
            let hash = encodedHash.split(".")[1];
            let buffer = (await this.cas.read(hash, 100000)).content;
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
                            let dids = operationsWithGaiaxType.map((op: any) => this.createDidSuffix(op));
                            callback(dids);
                        }
                    }
                }
            });

            count++;
            if (count >= maxFiles) break;
        }
    }
}
