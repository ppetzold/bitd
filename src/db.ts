import { Db, MongoClient } from 'mongodb'

export interface DbConfig {
    name: string
    url: string
    index: {
        [collection: string]: {
            keys: string[]
            fulltext: string[]
        }
    }
}

let db: Db
let mongo: MongoClient
let config: DbConfig

export function init(_config: DbConfig): Promise<void> {
    config = _config
    return new Promise(resolve => {
        MongoClient.connect(_config.url, { useNewUrlParser: true }, (err, client) => {
            if (err) {
                console.log(err)
            }
            db = client.db(_config.name)
            mongo = client
            resolve()
        })
    })
}

export function exit(): Promise<void> {
    return new Promise(resolve => {
        mongo.close()
        resolve()
    })
}

export const mempool = {
    async insert(item: any): Promise<void> {
        await db.collection('unconfirmed').insertMany([item])
    },
    async reset(): Promise<void> {
        await db
            .collection('unconfirmed')
            .deleteMany({})
            .catch(err => {
                console.log('## ERR ', err)
                process.exit()
            })
    },
    async sync(items: any): Promise<void> {
        await db
            .collection('unconfirmed')
            .deleteMany({})
            .catch(err => {
                console.log('## ERR ', err)
            })
        let index = 0
        while (true) {
            const chunk = items.splice(0, 1000)
            if (chunk.length > 0) {
                await db
                    .collection('unconfirmed')
                    .insertMany(chunk, { ordered: false })
                    .catch(err => {
                        // duplicates are ok because they will be ignored
                        if (err.code !== 11000) {
                            console.log('## ERR ', err, items)
                            process.exit()
                        }
                    })
                console.log('..chunk ' + index + ' processed ...', new Date().toString())
                index++
            } else {
                break
            }
        }
        console.log('Mempool synchronized with ' + items.length + ' items')
    },
}

export const block = {
    async reset(): Promise<void> {
        await db
            .collection('confirmed')
            .deleteMany({})
            .catch(err => {
                console.log('## ERR ', err)
                process.exit()
            })
    },
    async replace(items: any, block_index: number): Promise<void> {
        console.log('Deleting all blocks greater than or equal to', block_index)
        await db
            .collection('confirmed')
            .deleteMany({
                'blk.i': {
                    $gte: block_index,
                },
            })
            .catch(err => {
                console.log('## ERR ', err)
                process.exit()
            })
        console.log('Updating block', block_index, 'with', items.length, 'items')
        let index = 0
        while (true) {
            const chunk = items.slice(index, index + 1000)
            if (chunk.length > 0) {
                await db
                    .collection('confirmed')
                    .insertMany(chunk, { ordered: false })
                    .catch(err => {
                        // duplicates are ok because they will be ignored
                        if (err.code !== 11000) {
                            console.log('## ERR ', err, items)
                            process.exit()
                        }
                    })
                console.log('\tchunk ' + index + ' processed ...')
                index += 1000
            } else {
                break
            }
        }
    },
    async insert(items: any, block_index: number): Promise<void> {
        let index = 0
        while (true) {
            const chunk = items.slice(index, index + 1000)
            if (chunk.length > 0) {
                try {
                    await db.collection('confirmed').insertMany(chunk, { ordered: false })
                    console.log('..chunk ' + index + ' processed ...')
                } catch (e) {
                    // duplicates are ok because they will be ignored
                    if (e.code !== 11000) {
                        console.log('## ERR ', e, items, block_index)
                        process.exit()
                    }
                }
                index += 1000
            } else {
                break
            }
        }
        console.log('Block ' + block_index + ' inserted ')
    },
    async index(): Promise<void> {
        console.log('* Indexing MongoDB...')
        console.time('TotalIndex')

        if (config.index) {
            const collectionNames = Object.keys(config.index)
            for (const collectionName of collectionNames) {
                const keys: string[] = config.index[collectionName].keys
                const fulltext: string[] = config.index[collectionName].fulltext
                if (keys) {
                    console.log('Indexing keys...')
                    for (const key of keys) {
                        const o: { [key: string]: number } = {}
                        o[key] = 1
                        console.time('Index:' + key)
                        try {
                            if (key === 'tx.h') {
                                await db.collection(collectionName).createIndex(o, { unique: true })
                                console.log('* Created unique index for ', key)
                            } else {
                                await db.collection(collectionName).createIndex(o)
                                console.log('* Created index for ', key)
                            }
                        } catch (e) {
                            console.log(e)
                            process.exit()
                        }
                        console.timeEnd('Index:' + key)
                    }
                }
                if (fulltext) {
                    console.log('Creating full text index...')
                    const o: { [key: string]: string }  = {}
                    fulltext.forEach(key => {
                        o[key] = 'text'
                    })
                    console.time('Fulltext search for ' + collectionName)
                    try {
                        await db.collection(collectionName).createIndex(o, { name: 'fulltext' })
                    } catch (e) {
                        console.log(e)
                        process.exit()
                    }
                    console.timeEnd('Fulltext search for ' + collectionName)
                }
            }
        }

        console.log('* Finished indexing MongoDB...')
        console.timeEnd('TotalIndex')

        try {
            let result = await db.collection('confirmed').indexInformation({ full: true } as any)
            console.log('* Confirmed Index = ', result)
            result = await db.collection('unconfirmed').indexInformation({ full: true } as any)
            console.log('* Unonfirmed Index = ', result)
        } catch (e) {
            console.log('* Error fetching index info ', e)
            process.exit()
        }
    },
}
