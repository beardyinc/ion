import {Binary, Collection, Db, MongoClient} from 'mongodb';

// tslint:disable-next-line:completed-docs
interface IMongoSubmittedDid {
    didSuffix: string;
    type: string[];
    document: Binary;
    timestamp: Date;
}

export class SubmittedDidModel {
    didSuffix: string;
    type: string[];
    timestamp: Date;

    constructor (didSuffix: string, type: string[], timestamp: Date) {
        this.didSuffix = didSuffix;
        this.type = type;
        this.timestamp = timestamp;
    }
}

/*
 * Used to track all DIDs that were submitted to this node
 */
// tslint:disable-next-line:completed-docs
export default class MongoDbSubmittedDid {
    public static readonly collectionName: string = 'submitted-dids';

    private collection: Collection<IMongoSubmittedDid> | undefined;

    private db: Db | undefined;

    public async initialize (serverUrl: string, databaseName: string) {
        const client = await MongoClient.connect(serverUrl);
        this.db = client.db(databaseName);
        this.collection = await MongoDbSubmittedDid.createCollectionIfNotExist(this.db);
    }

    async enqueue (didId: string, type: string[], document: Buffer) {
        try {
            const queuedOperation: IMongoSubmittedDid = {
                didSuffix: didId,
                type: type,
                document: new Binary(document),
                timestamp: new Date()
            };

            await this.collection!.insertOne(queuedOperation);
        } catch (error) {
            // Duplicate insert errors (error code 11000).
            if (error.code === 11000) {
                throw "Cannot insert DID document";
            }

            throw error;
        }
    }

    async findByType (since: string | undefined, ...types: string[]): Promise<SubmittedDidModel[]> {
        try {
            let cursor;
            if (since) {
                let partition = await this.collection!.find({didSuffix: since}).limit(1).toArray();
                if (partition && partition.length > 0) {
                    let entity = partition[0];
                    let timestamp = entity.timestamp;

                    cursor = await this.collection!.find({type: {$all: types}, timestamp: {$gte: timestamp}}).toArray()
                }
            } else {
                cursor = await this.collection!.find({type: {$all: types}}).toArray();
            }
            return !cursor ? new Array<SubmittedDidModel>() : cursor.map(entity => new SubmittedDidModel(entity.didSuffix, entity.type, entity.timestamp));
        } catch (error) {
            console.error(error);
            throw error;
        }
    }

    private static async createCollectionIfNotExist (db: Db): Promise<Collection<IMongoSubmittedDid>> {
        // Get the names of existing collections.
        const collections = await db.collections();
        const collectionNames = collections.map((collection: Collection<IMongoSubmittedDid>) => collection.collectionName);

        // If the queued operation collection exists, use it; else create it then use it.
        let collection;
        if (collectionNames.includes(this.collectionName)) {
            collection = db.collection(this.collectionName);
        } else {
            collection = await db.createCollection(this.collectionName);
            // Create an index on didUniqueSuffix make `contains()` operations more efficient.
            // This is an unique index, so duplicate inserts are rejected.
            await collection.createIndex({didUniqueSuffix: 1}, {unique: true});
        }

        return collection;
    }
}
