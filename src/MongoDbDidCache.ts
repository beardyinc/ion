import {Collection, Db, MongoClient} from 'mongodb';

export interface DidCacheEntryModel {
    didSuffix: string;
    didType: string;
}

export default class MongoDbDidCache {

    public static readonly defaultDatabaseName: string = "did_cache";
    public static readonly collectionName: string = "did_cache";
    private db: Db | undefined;
    private cacheEntryCollection: Collection<any> | undefined;
    private readonly serverUrl: string;
    private readonly databaseName: string;

    constructor (serverUrl: string, databaseName: string) {
        this.serverUrl = serverUrl;
        this.databaseName = databaseName;
    }

    private static async createTransactionCollectionIfNotExist (db: Db): Promise<Collection<DidCacheEntryModel>> {
        const collections = await db.collections();
        const collectionNames = collections.map(collection => collection.collectionName);

        // If 'transactions' collection exists, use it; else create it.
        let cacheEntryCollection;
        if (collectionNames.includes(this.collectionName)) {
            console.log('Cache Entry collection already exists.');
            cacheEntryCollection = db.collection(this.collectionName);
        } else {
            console.log('Cache entry collection does not exists, creating...');
            cacheEntryCollection = await db.createCollection(this.collectionName);
            // Note the unique index, so duplicate inserts are rejected.
            await cacheEntryCollection.createIndex({didSuffix: 1}, {unique: true});
            console.log('Cache entry collection created.');
        }

        // @ts-ignore
        return cacheEntryCollection;
    }

    public async getDidSuffixesForType (type: string): Promise<DidCacheEntryModel[]> {

        let cacheEntries = await this.cacheEntryCollection!.find({didType: type}).toArray();

        if (!cacheEntries) {
            cacheEntries = new Array(0);
        }

        return cacheEntries;
    }

    public async addCacheEntry (didSuffix: string, didType: string): Promise<void> {

        try {
            const existing = await this.cacheEntryCollection!.find({didSuffix: didSuffix, didType: didType}).toArray();

            if (existing && existing.length > 0) {
                console.log(`a DID cache entry with suffix ${didSuffix}/${didType} already exists, skipping...`);
                return;
            }
            const newEntry: DidCacheEntryModel = {
                didSuffix: didSuffix,
                didType: didType
            };
            await this.cacheEntryCollection!.insertOne(newEntry);

        } catch (error) {
            // Swallow duplicate insert errors (error code 11000) as no-op; rethrow others
            if (error.code !== 11000) {
                throw error;
            }
        }

    }

    public async initialize (): Promise<void> {
        if (!this.db || !this.cacheEntryCollection) {
            // @ts-ignore
            const client = await MongoClient.connect(this.serverUrl, {useNewUrlParser: true}); // `useNewUrlParser` addresses nodejs's URL parser deprecation warning.
            this.db = client.db(this.databaseName);
            this.cacheEntryCollection = await MongoDbDidCache.createTransactionCollectionIfNotExist(this.db);
        }
    }
}
