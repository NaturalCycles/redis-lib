import { localTime } from '@naturalcycles/js-lib'
import { RedisClient } from './redisClient'

let client: RedisClient

beforeAll(() => {
  client = new RedisClient()
})

beforeEach(async () => {
  await client.dropTable('test')
})

afterAll(async () => {
  await client.dropTable('test')
  await client.disconnect()
})

describe('hashmap functions', () => {
  test('hset should save a map', async () => {
    await client.hset('test:key', { foo: 'bar' })

    const result = await client.hgetall('test:key')

    expect(result).toEqual({ foo: 'bar' })
  })

  test('should store/fetch numbers as strings', async () => {
    await client.hset('test:key', { one: 1 })

    const result = await client.hgetall('test:key')

    expect(result).toEqual({ one: '1' })
  })

  test('hgetall should not fetch nested objects', async () => {
    await client.hset('test:key', { nested: { one: 1 } })

    const result = await client.hgetall('test:key')

    expect(result).toEqual({ nested: '[object Object]' })
  })

  test('hget should fetch map property', async () => {
    await client.hset('test:key', { foo: 'bar' })

    const result = await client.hget('test:key', 'foo')

    expect(result).toBe('bar')
  })

  test('hget should fetch value as string', async () => {
    await client.hset('test:key', { one: 1 })

    const result = await client.hget('test:key', 'one')

    expect(result).toBe('1')
  })

  test('hmgetBuffer should get the values of the fields as strings', async () => {
    await client.hset('test:key', { one: 1, two: 2, three: 3 })

    const result = await client.hmget('test:key', ['one', 'three'])

    expect(result).toEqual(['1', '3'])
  })

  test('hmgetBuffer should get the values of the fields as buffers', async () => {
    await client.hset('test:key', { one: 1, two: 2, three: 3 })

    const result = await client.hmgetBuffer('test:key', ['one', 'three'])

    expect(result).toEqual([Buffer.from('1'), Buffer.from('3')])
  })

  test('hincr should change the value and return with a numeric result', async () => {
    await client.hset('test:key', { one: 1 })

    const result = await client.hincr('test:key', 'one', -2)

    expect(result).toBe(-1)
  })

  test('hincr should increase the value by 1 by default', async () => {
    await client.hset('test:key', { one: 1 })

    const result = await client.hincr('test:key', 'one')

    expect(result).toBe(2)
  })

  test('hincr should set the value to 1 for a non-existing field', async () => {
    const result = await client.hincr('test:key', 'one')

    expect(result).toBe(1)
  })

  test('hScanCount should return the number of keys in the hash', async () => {
    await client.hset('test:key', { one: 1, two: 2, three: 3 })

    const result = await client.hScanCount('test:key', {})

    expect(result).toBe(3)
  })

  test('hScanCount with a match pattern should return the number of matching keys in the hash', async () => {
    await client.hset('test:key', { one: 1, two: 2, three: 3 })

    const result = await client.hScanCount('test:key', { match: 't*' })

    expect(result).toBe(2)
  })

  test('hdel should delete a fields from the hash', async () => {
    await client.hset('test:key', { one: 1, two: 2, three: 3 })

    await client.hdel('test:key', ['two', 'three'])

    const result = await client.hgetall('test:key')
    expect(result).toEqual({ one: '1' })
  })

  test('hsetWithTTL should set the fields with expiry', async () => {
    const now = localTime.now().unix

    await client.hsetWithTTL('test:key', { foo1: 'bar' }, now + 1000)
    await client.hsetWithTTL('test:key', { foo2: 'bar' }, now - 1)

    const result = await client.hgetall('test:key')
    expect(result).toEqual({ foo1: 'bar' })
  })
})
