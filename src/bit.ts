import * as zmq from 'zeromq'
const RpcClient = require('bitcoind-rpc')
const TNA = require('tna')
import pLimit from 'p-limit'
import pQueue from 'p-queue'
import Config from './config'
const queue = new pQueue({ concurrency: Config.rpc.limit })
import mingo from 'mingo'
const jq = require('bigjq')
const bcode = require('bcode')

const Filter = require('../bitdb.json')

let Db: any
let Info: any
let rpc: any
let filter: mingo.Query | null
let processor: any

export function init(db: any, info: any): Promise<void> {
    return new Promise(resolve => {
        Db = db
        Info = info

        if (Filter.filter && Filter.filter.q && Filter.filter.q.find) {
            const query = bcode.encode(Filter.filter.q.find)
            filter = new mingo.Query(query)
        } else {
            filter = null
        }

        if (Filter.filter && Filter.filter.r && Filter.filter.r.f) {
            processor = Filter.filter.r.f
        }

        rpc = new RpcClient(Config.rpc)
        resolve()
    })
}
const request = {
    block(block_index: number): Promise<any> {
        return new Promise(resolve => {
            rpc.getBlockHash(block_index, (err: any, res: any) => {
                if (err) {
                    console.log('Err = ', err)
                    throw new Error(err)
                } else {
                    rpc.getBlock(res.result, (_: any, block: any) => {
                        resolve(block)
                    })
                }
            })
        })
    },
    /**
     * Return the current blockchain height
     */
    height(): Promise<number> {
        return new Promise(resolve => {
            rpc.getBlockCount((err: any, res: any) => {
                if (err) {
                    console.log('Err = ', err)
                    throw new Error(err)
                } else {
                    resolve(res.result)
                }
            })
        })
    },
    async tx(hash: string): Promise<any> {
        const content = await TNA.fromHash(hash, Config.rpc)
        return content
    },
    mempool(): Promise<any> {
        return new Promise(resolve => {
            rpc.getRawMemPool(async (err: any, ret: any) => {
                if (err) {
                    console.log('Err', err)
                } else {
                    const tasks = []
                    const limit = pLimit(Config.rpc.limit)
                    const txs = ret.result
                    console.log('txs = ', txs.length)
                    for (const tx of txs) {
                        tasks.push(
                            limit(async () => {
                                const content = await request.tx(tx).catch(e => {
                                    console.log('Error = ', e)
                                })
                                return content
                            })
                        )
                    }
                    const btxs = await Promise.all(tasks)
                    resolve(btxs)
                }
            })
        })
    },
}
export async function crawl(block_index: number): Promise<any> {
    const block_content = await request.block(block_index)
    const block_hash = block_content.result.hash
    const block_time = block_content.result.time

    if (block_content && block_content.result) {
        const txs = block_content.result.tx
        console.log('crawling txs = ', txs.length)
        const tasks = []
        const limit = pLimit(Config.rpc.limit)
        for (const tx of txs) {
            tasks.push(
                limit(async () => {
                    const t = await request.tx(tx).catch(e => {
                        console.log('Error = ', e)
                    })
                    t.blk = {
                        i: block_index,
                        h: block_hash,
                        t: block_time,
                    }
                    return t
                })
            )
        }
        let btxs = await Promise.all(tasks)

        if (filter) {
            btxs = btxs.filter(row => {
                return filter!.test(row)
            })

            if (processor) {
                btxs = bcode.decode(btxs)
                btxs = await jq.run(processor, btxs)
            }
            console.log('Filtered Xputs = ', btxs.length)
        }

        console.log('Block ' + block_index + ' : ' + txs.length + 'txs | ' + btxs.length + ' filtered txs')
        return btxs
    } else {
        return []
    }
}
const outsock = zmq.socket('pub')
export function listen(): void {
    const sock = zmq.socket('sub')
    sock.connect('tcp://' + Config.zmq.incoming.host + ':' + Config.zmq.incoming.port)
    sock.subscribe('hashtx')
    sock.subscribe('hashblock')
    console.log('Subscriber connected to port ' + Config.zmq.incoming.port)

    outsock.bindSync('tcp://' + Config.zmq.outgoing.host + ':' + Config.zmq.outgoing.port)
    console.log('Started publishing to ' + Config.zmq.outgoing.host + ':' + Config.zmq.outgoing.port)

    // Listen to ZMQ
    sock.on('message', async (topic, message) => {
        if (topic.toString() === 'hashtx') {
            const hash = message.toString('hex')
            console.log('New mempool hash from ZMQ = ', hash)
            await sync('mempool', hash)
        } else if (topic.toString() === 'hashblock') {
            const hash = message.toString('hex')
            console.log('New block hash from ZMQ = ', hash)
            await sync('block')
        }
    })

    // Don't trust ZMQ. Try synchronizing every 1 minute in case ZMQ didn't fire
    setInterval(async () => {
        await sync('block')
    }, 60000)
}

export async function sync(type: 'block' | 'mempool', hash?: string): Promise<any> {
    if (type === 'block') {
        try {
            const lastSynchronized = await Info.checkpoint()
            const currentHeight = await request.height()
            console.log('Last Synchronized = ', lastSynchronized)
            console.log('Current Height = ', currentHeight)

            for (let index = lastSynchronized + 1; index <= currentHeight; index++) {
                console.log('RPC BEGIN ' + index, new Date().toString())
                console.time('RPC END ' + index)
                const content = await crawl(index)
                console.timeEnd('RPC END ' + index)
                console.log(new Date().toString())
                console.log('DB BEGIN ' + index, new Date().toString())
                console.time('DB Insert ' + index)

                await Db.block.insert(content, index)

                await Info.updateTip(index)
                console.timeEnd('DB Insert ' + index)
                console.log('------------------------------------------')
                console.log('\n')

                // zmq broadcast
                const b = { i: index, txs: content }
                console.log('Zmq block = ', JSON.stringify(b, null, 2))
                outsock.send(['block', JSON.stringify(b)])
            }

            // clear mempool and synchronize
            if (lastSynchronized < currentHeight) {
                console.log('Clear mempool and repopulate')
                const items = await request.mempool()
                await Db.mempool.sync(items)
            }

            if (lastSynchronized === currentHeight) {
                console.log('no update')
                return null
            } else {
                console.log('[finished]')
                return currentHeight
            }
        } catch (e) {
            console.log('Error', e)
            console.log('Shutting down Bitdb...', new Date().toString())
            await Db.exit()
            process.exit()
        }
    } else if (type === 'mempool') {
        queue.add(async () => {
            const content = await request.tx(hash!)
            try {
                await Db.mempool.insert(content)
                console.log('# Q inserted [size: ' + queue.size + ']', hash)
                console.log(content)
                outsock.send(['mempool', JSON.stringify(content)])
            } catch (e) {
                // duplicates are ok because they will be ignored
                if (e.code === 11000) {
                    console.log('Duplicate mempool item: ', content)
                } else {
                    console.log('## ERR ', e, content)
                    process.exit()
                }
            }
        })
        return hash
    }
}
export async function run(): Promise<void> {
    // initial block sync
    await sync('block')

    // initial mempool sync
    console.log('Clear mempool and repopulate')
    const items = await request.mempool()
    await Db.mempool.sync(items)
}
