import {
  AnyObject,
  CommonLogger,
  NullableBuffer,
  NullableString,
  Promisable,
  UnixTimestampNumber,
} from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
// eslint-disable-next-line import/no-duplicates
import type { Redis, RedisOptions } from 'ioredis'
// eslint-disable-next-line import/no-duplicates
import type * as RedisLib from 'ioredis'
import type { ScanStreamOptions } from 'ioredis/built/types'
import type { ChainableCommander } from 'ioredis/built/utils/RedisCommander'

export interface CommonClient extends AsyncDisposable {
  connected: boolean
  connect: () => Promise<void>
  disconnect: () => Promise<void>
  ping: () => Promise<void>
}

export interface RedisClientCfg {
  redisOptions?: RedisOptions

  /**
   * Defaults to console.
   */
  logger?: CommonLogger
}

/**
 Wraps the redis sdk with unified interface.
 Features:
 
 - Lazy loading & initialization
 - Reasonable defaults
 
 */
export class RedisClient implements CommonClient {
  constructor(cfg: RedisClientCfg = {}) {
    this.cfg = {
      logger: console,
      ...cfg,
      redisOptions: {
        showFriendlyErrorStack: true,
        lazyConnect: true,
        ...cfg.redisOptions,
      },
    }
  }

  cfg!: Required<RedisClientCfg>

  connected = false

  private _redis?: Redis

  redis(): Redis {
    if (this._redis) return this._redis

    // lazy-load the library
    const redisLib = require('ioredis') as typeof RedisLib
    const redis = new redisLib.Redis(this.cfg.redisOptions)

    const { logger } = this.cfg

    const redisEvents = ['connect', 'close', 'reconnecting', 'end']
    redisEvents.forEach(e => redis.on(e, () => logger.log(`redis: ${e}`)))

    const closeEvents: NodeJS.Signals[] = ['SIGINT', 'SIGTERM']
    closeEvents.forEach(e => process.once(e, () => redis.quit()))

    redis.on('error', err => logger.error(err))

    this.connected = true
    this._redis = redis
    this.log(`redis: created`)
    return redis
  }

  async connect(): Promise<void> {
    if (!this.connected) {
      await this.redis().connect()
      this.connected = true
    }
  }

  async disconnect(): Promise<void> {
    this.log('redis: quit...')
    this.log(`redis: quit`, await this.redis().quit())
    this.connected = false
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.disconnect()
  }

  async ping(): Promise<void> {
    await this.redis().ping()
  }

  async del(keys: string[]): Promise<number> {
    return await this.redis().del(keys)
  }

  async get(key: string): Promise<NullableString> {
    return await this.redis().get(key)
  }

  async getBuffer(key: string): Promise<NullableBuffer> {
    return await this.redis().getBuffer(key)
  }

  async mget(keys: string[]): Promise<NullableString[]> {
    return await this.redis().mget(keys)
  }

  async mgetBuffer(keys: string[]): Promise<NullableBuffer[]> {
    return await this.redis().mgetBuffer(keys)
  }

  async set(key: string, value: string | number | Buffer): Promise<void> {
    await this.redis().set(key, value)
  }

  async hgetall<T extends Record<string, string> = Record<string, string>>(
    key: string,
  ): Promise<T | null> {
    const result = await this.redis().hgetall(key)
    if (Object.keys(result).length === 0) return null
    return result as T
  }

  async hget(key: string, field: string): Promise<NullableString> {
    return await this.redis().hget(key, field)
  }

  async hset(key: string, value: AnyObject): Promise<void> {
    await this.redis().hset(key, value)
  }

  async hdel(key: string, fields: string[]): Promise<void> {
    await this.redis().hdel(key, ...fields)
  }

  async hmget(key: string, fields: string[]): Promise<NullableString[]> {
    return await this.redis().hmget(key, ...fields)
  }

  async hmgetBuffer(key: string, fields: string[]): Promise<NullableBuffer[]> {
    return await this.redis().hmgetBuffer(key, ...fields)
  }

  async hincr(key: string, field: string, increment: number = 1): Promise<number> {
    return await this.redis().hincrby(key, field, increment)
  }

  async setWithTTL(
    key: string,
    value: string | number | Buffer,
    expireAt: UnixTimestampNumber,
  ): Promise<void> {
    await this.redis().set(key, value, 'EXAT', expireAt)
  }

  async hsetWithTTL(
    key: string,
    value: AnyObject,
    expireAt: UnixTimestampNumber
  ): Promise<void> {
    const valueKeys = Object.keys(value)
    const numberOfKeys = valueKeys.length
    const keyList = valueKeys.join(' ')
    const commandString = `HEXPIREAT ${key} ${expireAt} FIELDS ${numberOfKeys} ${keyList}`
    const [command, ...args] = commandString.split(' ')
    await this.redis().hset(key, value)
    await this.redis().call(command!, args)
  }
  
  async mset(obj: Record<string, string | number>): Promise<void> {
    await this.redis().mset(obj)
  }

  async msetBuffer(obj: Record<string, Buffer>): Promise<void> {
    await this.redis().mset(obj)
  }

  async incr(key: string, by: number = 1): Promise<number> {
    return await this.redis().incrby(key, by)
  }

  async ttl(key: string): Promise<number> {
    return await this.redis().ttl(key)
  }

  async dropTable(table: string): Promise<void> {
    let count = 0

    await this.withPipeline(async pipeline => {
      await this.scanStream({
        match: `${table}:*`,
      }).forEach(keys => {
        pipeline.del(keys)
        count += keys.length
      })
    })

    this.log(`redis: dropped table ${table} (${count} keys)`)
  }

  async clearAll(): Promise<void> {
    this.log(`redis: clearAll...`)
    let count = 0

    await this.withPipeline(async pipeline => {
      await this.scanStream({
        match: `*`,
      }).forEach(keys => {
        pipeline.del(keys)
        count += keys.length
      })
    })

    this.log(`redis: clearAll removed ${count} keys`)
  }

  /**
   Convenient type-safe wrapper.
   Returns BATCHES of keys in each iteration (as-is).
   */
  scanStream(opt: ScanStreamOptions): ReadableTyped<string[]> {
    return this.redis().scanStream(opt)
  }

  /**
   * Like scanStream, but flattens the stream of keys.
   */
  scanStreamFlat(opt: ScanStreamOptions): ReadableTyped<string> {
    return (this.redis().scanStream(opt) as ReadableTyped<string[]>).flatMap(keys => keys)
  }

  async scanCount(opt: ScanStreamOptions): Promise<number> {
    // todo: implement more efficiently, e.g via LUA?
    let count = 0

    await (this.redis().scanStream(opt) as ReadableTyped<string[]>).forEach(keys => {
      count += keys.length
    })

    return count
  }

  hscanStream(key: string, opt: ScanStreamOptions): ReadableTyped<string[]> {
    return this.redis().hscanStream(key, opt)
  }

  async hScanCount(key: string, opt: ScanStreamOptions): Promise<number> {
    let count = 0

    const stream = this.redis().hscanStream(key, opt)

    await stream.forEach((keyValueList: string[]) => {
      count += keyValueList.length / 2
    })

    return count
  }

  async withPipeline(fn: (pipeline: ChainableCommander) => Promisable<void>): Promise<void> {
    const pipeline = this.redis().pipeline()
    await fn(pipeline)
    await pipeline.exec()
  }

  private log(...args: any[]): void {
    this.cfg.logger.log(...args)
  }
}
