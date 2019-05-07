const level = require('level')
const kv = level('./.state')
const Filter = require('./bitdb.json')
/**
 * Return the last synchronized checkpoint
 */
export function checkpoint(): Promise<number> {
    return new Promise(async (resolve, reject) => {
        kv.get('tip', (err: any, value: any) => {
            if (err) {
                if (err.notFound) {
                    console.log('Checkpoint not found, starting from GENESIS')
                    resolve(Filter.from)
                } else {
                    console.log('err', err)
                    reject(err)
                }
            } else {
                const cp = +value
                console.log('Checkpoint found,', cp)
                resolve(cp)
            }
        })
    })
}
export function updateTip(index: number): Promise<void> {
    return new Promise((resolve, reject) => {
        kv.put('tip', index, (err: any) => {
            if (err) {
                console.log(err)
                reject()
            } else {
                console.log('Tip updated to', index)
                resolve()
            }
        })
    })
}
export function deleteTip(): Promise<void> {
    return new Promise((resolve, reject) => {
        kv.del('tip', (err: any) => {
            if (err) {
                console.log(err)
                reject()
            } else {
                console.log('Tip deleted')
                resolve()
            }
        })
    })
}
