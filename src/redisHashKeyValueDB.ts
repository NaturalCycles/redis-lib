import {
  CommonKeyValueDBSaveBatchOptions,
  CommonDBCreateOptions,
  CommonKeyValueDB,
  KeyValueDBTuple,
} from '@naturalcycles/db-lib'
import { _chunk, _mapObject, StringMap } from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import { RedisClient } from './redisClient'
import { RedisKeyValueDBCfg } from './redisKeyValueDB'

export interface RedisHashKeyValueDBCfg extends RedisKeyValueDBCfg {
  hashKey: string
}

export class RedisHashKeyValueDB implements CommonKeyValueDB, AsyncDisposable {
  client: RedisClient
  keyOfHashField: string

  constructor(cfg: RedisHashKeyValueDBCfg) {
    this.client = cfg.client
    this.keyOfHashField = cfg.hashKey
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
    const bufs = await this.client.hmgetBuffer(this.keyOfHashField, this.idsToKeys(table, ids))
    return bufs.map((buf, i) => [ids[i], buf] as KeyValueDBTuple).filter(([_k, v]) => v !== null)
  }

  async deleteByIds(table: string, ids: string[]): Promise<void> {
    if (!ids.length) return
    await this.client.hdel(this.keyOfHashField, this.idsToKeys(table, ids))
  }

  async saveBatch(
    table: string,
    entries: KeyValueDBTuple[],
    opt?: CommonKeyValueDBSaveBatchOptions,
  ): Promise<void> {
    if (!entries.length) return

    const map: StringMap<any> = _mapObject(Object.fromEntries(entries), (k, v) => [
      this.idToKey(table, String(k)),
      v,
    ])

    if (opt?.expireAt) {
      await this.client.hsetWithTTL(this.keyOfHashField, map, opt.expireAt)
    } else {
      await this.client.hset(this.keyOfHashField, map)
    }
  }

  streamIds(table: string, limit?: number): ReadableTyped<string> {
    let stream = this.client
      .hscanStream(this.keyOfHashField, {
        match: `${table}:*`,
      })
      .flatMap(keyValueList => {
        const keys: string[] = []
        keyValueList.forEach((keyOrValue, index) => {
          if (index % 2 !== 0) return
          keys.push(keyOrValue)
        })
        return this.keysToIds(table, keys)
      })

    if (limit) {
      stream = stream.take(limit)
    }

    return stream
  }

  streamValues(table: string, limit?: number): ReadableTyped<Buffer> {
    return this.client
      .hscanStream(this.keyOfHashField, {
        match: `${table}:*`,
      })
      .flatMap(
        async keyValueList => {
          const values: string[] = []
          keyValueList.forEach((keyOrValue, index) => {
            if (index % 2 !== 1) return
            values.push(keyOrValue)
          })
          return values.map(v => Buffer.from(v))
        },
        {
          concurrency: 16,
        },
      )
      .take(limit || Infinity)
  }

  streamEntries(table: string, limit?: number | undefined): ReadableTyped<KeyValueDBTuple> {
    return this.client
      .hscanStream(this.keyOfHashField, {
        match: `${table}:*`,
      })
      .flatMap(
        async keyValueList => {
          const entries = _chunk(keyValueList, 2)
          return entries.map(([k, v]) => {
            return [
              this.keyToId(table, String(k)),
              Buffer.from(String(v)),
            ] satisfies KeyValueDBTuple
          })
        },
        {
          concurrency: 16,
        },
      )
      .take(limit || Infinity)
  }

  async count(table: string): Promise<number> {
    return await this.client.hscanCount(this.keyOfHashField, {
      match: `${table}:*`,
    })
  }

  async increment(table: string, id: string, by: number = 1): Promise<number> {
    return await this.client.hincr(this.keyOfHashField, this.idToKey(table, id), by)
  }

  async createTable(table: string, opt?: CommonDBCreateOptions): Promise<void> {
    if (!opt?.dropIfExists) return

    await this.client.dropTable(table)
  }

  private idsToKeys(table: string, ids: string[]): string[] {
    return ids.map(id => this.idToKey(table, id))
  }

  private idToKey(table: string, id: string): string {
    return `${table}:${id}`
  }

  private keysToIds(table: string, keys: string[]): string[] {
    return keys.map(key => this.keyToId(table, key))
  }

  private keyToId(table: string, key: string): string {
    return key.slice(table.length + 1)
  }
}
