import {
  BaseDBEntity,
  CommonDB,
  CommonDBOptions,
  CommonDBSaveOptions,
  DBQuery,
} from '@naturalcycles/db-lib'
import { queryInMemory } from '@naturalcycles/db-lib/dist/inMemory.db'
import { _flatten, by, pMap } from '@naturalcycles/js-lib'
import { Debug, streamToObservable } from '@naturalcycles/nodejs-lib'
import c from 'chalk'
import { RedisOptions } from 'ioredis'
import * as Redis from 'ioredis'
import { EMPTY, Observable } from 'rxjs'
import { mergeMap, toArray } from 'rxjs/operators'

const log = Debug('nc:redis-lib')

export interface RedisDBCfg {
  redisOptions?: RedisOptions

  /**
   * If true - it will "emulate" queries by using SCAN $table_
   * which will load ALL table keys into memory and filter in-memory.
   * @default false
   */
  runQueries?: boolean
}

/**
 * streamQuery doesn't support limit and order - it always returns unlimited unsorted results.
 */
export class RedisDB implements CommonDB {
  constructor (public cfg: RedisDBCfg = {}) {
    this.redis = this.create()
  }

  redis!: Redis.Redis

  protected create (): Redis.Redis {
    const redis = new Redis({
      showFriendlyErrorStack: true,
      lazyConnect: true,
      ...this.cfg.redisOptions,
    })

    const redisEvents = ['connect', 'close', 'reconnecting', 'end']
    redisEvents.forEach(e => redis.on(e, () => log(`event:`, c.bold(e))))

    const closeEvents: NodeJS.Signals[] = ['SIGINT', 'SIGTERM']
    closeEvents.forEach(e => process.once(e, () => redis.quit()))

    redis.on('error', err => log.error(err))

    // log('connected')
    return redis
  }

  async quit (): Promise<void> {
    log('disconnecting...')
    log(`quit:`, await this.redis.quit())
  }

  async resetCache (): Promise<void> {
    log('flushall:', await this.redis.flushall())
  }

  key (table: string, id: string): string {
    return [table, id].join('_')
  }

  parseKey (table: string, key: string): { table: string; id: string } {
    return {
      table,
      id: key.substr(table.length + 1),
    }
  }

  serialize<T extends object> (obj: T): string {
    return JSON.stringify(obj)
  }

  deserialize<T = any> (s?: string | null): T {
    try {
      return s && JSON.parse(s)
    } catch (err) {
      log.error(s, typeof s, err)
      return undefined as any
    }
  }

  async saveBatch<DBM extends BaseDBEntity> (
    table: string,
    dbms: DBM[],
    opts?: CommonDBSaveOptions,
  ): Promise<void> {
    if (!dbms.length) return
    await this.redis.mset(_flatten(dbms.map(dbm => [this.key(table, dbm.id), this.serialize(dbm)])))
  }

  async getByIds<DBM extends BaseDBEntity> (
    table: string,
    ids: string[],
    opts?: CommonDBOptions,
  ): Promise<DBM[]> {
    if (!ids.length) return []
    const dbms = (await this.redis.mget(...ids.map(id => this.key(table, id)))) as string[]
    return dbms.filter(Boolean).map(dbm => this.deserialize<DBM>(dbm))
  }

  async deleteByIds (table: string, ids: string[], opts?: CommonDBOptions): Promise<string[]> {
    if (!ids.length) return []
    const deletedIds = await pMap(ids, async id => {
      const deleted = await this.redis.del(this.key(table, id))
      return deleted === 1 ? id : undefined
    })

    return deletedIds.filter(Boolean) as string[]
  }

  streamQuery<DBM extends BaseDBEntity> (q: DBQuery<DBM>, opts?: CommonDBOptions): Observable<DBM> {
    if (!this.cfg.runQueries) return EMPTY

    return streamToObservable<string[]>(
      this.redis.scanStream({
        match: `${q.table}_*`,
      }),
    ).pipe(
      mergeMap(async keys => {
        const ids = keys.map(k => this.parseKey(q.table, k).id)
        const dbms = await this.getByIds<DBM>(q.table, ids)
        return queryInMemory(q, by(dbms, dbm => dbm.id))
      }),
      mergeMap(dbms => dbms),
    )
  }

  async runQuery<DBM extends BaseDBEntity> (
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<DBM[]> {
    const dbms = await this.streamQuery(q, opts)
      .pipe(toArray())
      .toPromise()
    return queryInMemory(q, by(dbms, dbm => dbm.id))
  }

  async runQueryCount<DBM extends BaseDBEntity> (
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<number> {
    const dbms = await this.runQuery(q, opts)
    return dbms.length
  }

  async deleteByQuery<DBM extends BaseDBEntity> (
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<string[]> {
    const dbms = await this.runQuery(q, opts)
    return this.deleteByIds(q.table, dbms.map(dbm => dbm.id), opts)
  }
}
