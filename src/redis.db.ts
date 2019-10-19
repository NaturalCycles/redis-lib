import {
  CommonDB,
  CommonDBOptions,
  CommonDBSaveOptions,
  DBQuery,
  RunQueryResult,
  SavedDBEntity,
} from '@naturalcycles/db-lib'
import { queryInMemory } from '@naturalcycles/db-lib/dist/inMemory.db'
import { _flatten } from '@naturalcycles/js-lib'
import { Debug, readableFrom, streamToArray } from '@naturalcycles/nodejs-lib'
import c from 'chalk'
import { RedisOptions } from 'ioredis'
import * as Redis from 'ioredis'
import { Readable, Transform } from 'stream'

const log = Debug('nc:redis-lib')

export interface RedisDBCfg {
  redisOptions?: RedisOptions

  /**
   * If true - it will "emulate" queries by using SCAN $table_
   * which will load ALL table keys into memory and filter in-memory.
   * @default false
   */
  runQueries?: boolean

  /**
   * If set - all keys will be prefixed by it.
   * So the key will look like:
   * ${namespacePrefix}${table}_${id}
   */
  namespacePrefix?: string
}

/**
 * streamQuery doesn't support limit and order - it always returns unlimited unsorted results.
 */
export class RedisDB implements CommonDB {
  constructor(cfg: RedisDBCfg = {}) {
    this.cfg = {
      runQueries: false,
      namespacePrefix: '',
      ...cfg,
      redisOptions: {
        showFriendlyErrorStack: true,
        lazyConnect: true,
        ...cfg.redisOptions,
      },
    }

    this.redis = this.create()
  }

  public cfg!: Required<RedisDBCfg>

  redis!: Redis.Redis

  protected create(): Redis.Redis {
    const redis = new Redis(this.cfg.redisOptions)

    const redisEvents = ['connect', 'close', 'reconnecting', 'end']
    redisEvents.forEach(e => redis.on(e, () => log(`event:`, c.bold(e))))

    const closeEvents: NodeJS.Signals[] = ['SIGINT', 'SIGTERM']
    closeEvents.forEach(e => process.once(e, () => redis.quit()))

    redis.on('error', err => log.error(err))

    // log('connected')
    return redis
  }

  async quit(): Promise<void> {
    log('disconnecting...')
    log(`quit:`, await this.redis.quit())
  }

  async resetCache(table?: string): Promise<void> {
    const pattern = `${this.cfg.namespacePrefix}${table || ''}*`
    const keys = await this.redis.keys(pattern)
    if (keys.length) {
      await this.redis.del(...keys)
    }
    log(`resetCache deleted ${keys.length} keys under ${pattern}`)
  }

  key(table: string, id: string): string {
    return this.cfg.namespacePrefix + [table, id].join('_')
  }

  parseKey(table: string, key: string): { table: string; id: string } {
    return {
      table,
      id: key.substr(this.cfg.namespacePrefix.length + table.length + 1),
    }
  }

  serialize<T extends object>(obj: T): string {
    return JSON.stringify(obj)
  }

  deserialize<T = any>(s?: string | null): T {
    try {
      return s && JSON.parse(s)
    } catch (err) {
      log.error(s, typeof s, err)
      return undefined as any
    }
  }

  async saveBatch<DBM extends SavedDBEntity>(
    table: string,
    dbms: DBM[],
    opts?: CommonDBSaveOptions,
  ): Promise<void> {
    if (!dbms.length) return
    await this.redis.mset(_flatten(dbms.map(dbm => [this.key(table, dbm.id), this.serialize(dbm)])))
  }

  async getByIds<DBM extends SavedDBEntity>(
    table: string,
    ids: string[],
    opts?: CommonDBOptions,
  ): Promise<DBM[]> {
    if (!ids.length) return []
    const dbms = (await this.redis.mget(...ids.map(id => this.key(table, id)))) as string[]
    return dbms.filter(Boolean).map(dbm => this.deserialize<DBM>(dbm))
  }

  async deleteByIds(table: string, ids: string[], opts?: CommonDBOptions): Promise<number> {
    if (!ids.length) return 0
    return await this.redis.del(...ids.map(id => this.key(table, id)))
  }

  streamQuery<DBM extends SavedDBEntity>(q: DBQuery<DBM>, opts?: CommonDBOptions): Readable {
    if (!this.cfg.runQueries) return readableFrom([])
    const _this = this

    return this.redis
      .scanStream({
        match: `${this.cfg.namespacePrefix}${q.table}_*`,
      })
      .pipe(
        new Transform({
          objectMode: true,
          async transform(keys: string[], _encoding, cb) {
            const ids = keys.map(k => _this.parseKey(q.table, k).id)
            const dbms = await _this.getByIds<DBM>(q.table, ids)
            const items = queryInMemory(q, dbms)
            // tslint:disable-next-line:no-invalid-this
            items.forEach(item => this.push(item)) // push multiple items!
            cb()
          },
        }),
      )
  }

  async runQuery<DBM extends SavedDBEntity>(
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<RunQueryResult<DBM>> {
    const dbms = await streamToArray<DBM>(this.streamQuery(q, opts))
    return { records: queryInMemory(q, dbms) }
  }

  async runQueryCount<DBM extends SavedDBEntity>(
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<number> {
    const { records } = await this.runQuery(q, opts)
    return records.length
  }

  async deleteByQuery<DBM extends SavedDBEntity>(
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<number> {
    const { records } = await this.runQuery(q, opts)
    return await this.deleteByIds(q.table, records.map(dbm => dbm.id), opts)
  }
}
