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
        let multihash = Multihash.hash(buffer, 18); //18 is SHA256

        // encode to base64url
        let encodedMultihash = Encoder.encode(multihash);

        return `did:ion:${encodedMultihash}`;
    }

    public async getDidsWithType (didType: string, maxFiles: number = 20, callback: (didSuffixes: string[]) => any) {
        let transactionStore = new MongoDbTransactionStore();
        await transactionStore.initialize(this.mongoConnectionString, this.databaseName);
        console.log(`Parsing top ${maxFiles} of ${await transactionStore.getTransactionsCount()} transactions`);

        let allTransactions = (await transactionStore.getTransactions()).reverse();

        //travel back in time by reversing it.
        // we start at the youngest block and go back as many as "maxFiles" specifies
        let ipfsLookupCoreIndexFileHashes = allTransactions.map(trans => trans.anchorString);

        let count = 0;
        for (let encodedHash of ipfsLookupCoreIndexFileHashes) {
            // they come in the form <NumOps>.<Hash>
            let hash = encodedHash.split(".")[1];
            let coreIndexFile = await this.cas.read(hash, 100000);
            if(coreIndexFile.code !== 'success'){
                console.error(`Received error from CAS: ${coreIndexFile.code}`);
                continue;
            }
            let buffer = coreIndexFile.content;
            zlib.gunzip(buffer, (error: Error | null, result: Buffer) => {
                if (error) {
                    console.error(error);
                } else {
                    let str = result.toString();
                    let cif = JSON.parse(str);
                    if (cif.operations && cif.operations.create) {
                        // @ts-ignore
                        let operationsWithGaiaxType = cif.operations.create.filter(co => co.suffixData.type === didType);
                        if (operationsWithGaiaxType.length > 0) {
                            let dids = operationsWithGaiaxType.map((op: any) => this.createDidSuffix(op.suffixData));
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
