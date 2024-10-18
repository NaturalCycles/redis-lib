import {
  CommonDBCreateOptions,
  CommonKeyValueDB,
  commonKeyValueDBFullSupport,
  CommonKeyValueDBSaveBatchOptions,
  IncrementTuple,
  KeyValueDBTuple,
} from '@naturalcycles/db-lib'
import { _chunk } from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import { RedisClient } from './redisClient'

export interface RedisKeyValueDBCfg {
  client: RedisClient
}

export class RedisKeyValueDB2 implements CommonKeyValueDB, AsyncDisposable {
  constructor(cfg: RedisKeyValueDBCfg) {
    this.client = cfg.client
  }

  client: RedisClient

  support = {
    ...commonKeyValueDBFullSupport,
  }

  async ping(): Promise<void> {
    await this.client.ping()
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.client.disconnect()
  }

  async getByIds(table: string, ids: string[]): Promise<KeyValueDBTuple[]> {
    if (!ids.length) return []
    // we assume that the order of returned values is the same as order of input ids
    const bufs = await this.client.hmgetBuffer(table, ids)
    return bufs.map((buf, i) => [ids[i], buf] as KeyValueDBTuple).filter(([_k, v]) => v !== null)
  }

  async deleteByIds(table: string, ids: string[]): Promise<void> {
    if (!ids.length) return
    await this.client.hdel(table, ids)
  }

  async saveBatch(
    table: string,
    entries: KeyValueDBTuple[],
    opt?: CommonKeyValueDBSaveBatchOptions,
  ): Promise<void> {
    if (!entries.length) return

    const record = Object.fromEntries(entries)

    if (opt?.expireAt) {
      await this.client.hsetWithTTL(table, record, opt.expireAt)
    } else {
      await this.client.hset(table, record)
    }
  }

  streamIds(table: string, limit?: number): ReadableTyped<string> {
    const stream = this.client
      .hscanStream(table)
      .flatMap(keyValueList => {
        const keys: string[] = []
        keyValueList.forEach((keyOrValue, index) => {
          if (index % 2 !== 0) return
          keys.push(keyOrValue)
        })
        return keys
      })
      .take(limit || Infinity)

    return stream
  }

  streamValues(table: string, limit?: number): ReadableTyped<Buffer> {
    return this.client
      .hscanStream(table)
      .flatMap(keyValueList => {
        const values: string[] = []
        keyValueList.forEach((keyOrValue, index) => {
          if (index % 2 !== 1) return
          values.push(keyOrValue)
        })
        return values.map(v => Buffer.from(v))
      })
      .take(limit || Infinity)
  }

  streamEntries(table: string, limit?: number): ReadableTyped<KeyValueDBTuple> {
    return this.client
      .hscanStream(table)
      .flatMap(keyValueList => {
        const entries = _chunk(keyValueList, 2) as [string, string][]
        return entries.map(([k, v]) => {
          return [k, Buffer.from(String(v))] satisfies KeyValueDBTuple
        })
      })
      .take(limit || Infinity)
  }

  async count(table: string): Promise<number> {
    return await this.client.hscanCount(table)
  }

  async incrementBatch(table: string, increments: IncrementTuple[]): Promise<IncrementTuple[]> {
    return await this.client.hincrBatch(table, increments)
  }

  async createTable(table: string, opt?: CommonDBCreateOptions): Promise<void> {
    if (!opt?.dropIfExists) return

    await this.client.del([table])
  }
}
