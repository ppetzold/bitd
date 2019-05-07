require('dotenv').config()
import Config from './config'
const Filter = require('./bitdb.json')
import * as ip from 'ip'
import * as Bit from './bit'
import * as Db from './db'
import * as Info from './info'
console.log(ip.address())

const daemon = {
    async run(): Promise<void> {
        // 1. Initialize
        await Db.init(Config.db)
        await Bit.init(Db, Info)

        // 2. Bootstrap actions depending on first time
        const lastSynchronized = await Info.checkpoint()

        console.time('Indexing Keys')
        if (lastSynchronized === Filter.from) {
            // First time. Try indexing
            console.log('Indexing...', new Date())
            await Db.block.index()
        }
        console.timeEnd('Indexing Keys')

        if (lastSynchronized !== Filter.from) {
            // Resume
            // Rewind one step and start
            // so that it can recover even in cases
            // where the last run crashed during index
            // and the block was not indexed completely.
            console.log('Resuming...')
            await util.fix(lastSynchronized - 1)
        }

        // 3. Start synchronizing
        console.log('Synchronizing...', new Date())
        console.time('Initial Sync')
        await Bit.run()
        console.timeEnd('Initial Sync')

        // 4. Start listening
        Bit.listen()
    },
}

const util = {
    async run(): Promise<void> {
        await Db.init(Config.db)
        const cmd = process.argv[2]
        if (cmd === 'fix') {
            let from
            if (process.argv.length > 3) {
                from = +process.argv[3]
            } else {
                from = await Info.checkpoint()
            }
            await util.fix(from)
            process.exit()
        } else if (cmd === 'reset') {
            await Db.block.reset()
            await Db.mempool.reset()
            await Info.deleteTip()
            process.exit()
        } else if (cmd === 'index') {
            await Db.block.index()
            process.exit()
        }
    },
    async fix(from: number): Promise<void> {
        console.log('Restarting from index ', from)
        console.time('replace')
        await Bit.init(Db, Info)
        const content = await Bit.crawl(from)
        await Db.block.replace(content, from)
        console.log('Block', from, 'fixed.')
        await Info.updateTip(from)
        console.log('[finished]')
        console.timeEnd('replace')
    },
}

async function start(): Promise<void> {
    if (process.argv.length > 2) {
        util.run()
    } else {
        daemon.run()
    }
}

start()
