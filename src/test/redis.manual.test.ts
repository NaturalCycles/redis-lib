import { CommonKeyValueDao, CommonKeyValueDaoMemoCache } from '@naturalcycles/db-lib'
import { runCommonKeyValueDBTest, TEST_TABLE } from '@naturalcycles/db-lib/dist/testing'
import { runCommonKeyValueDaoTest } from '@naturalcycles/db-lib/dist/testing/keyValueDaoTest'
import { KeyValueDBTuple } from '@naturalcycles/db-lib/src/kv/commonKeyValueDB'
import { _AsyncMemo, _range, localTime, pDelay } from '@naturalcycles/js-lib'
import { RedisClient } from '../redisClient'
import { RedisKeyValueDB } from '../redisKeyValueDB'

const client = new RedisClient()
const db = new RedisKeyValueDB({ client })

const dao = new CommonKeyValueDao<Buffer>({
  db,
  table: TEST_TABLE,
})

afterAll(async () => {
  await client.disconnect()
})

test('connect', async () => {
  await db.ping()
})

describe('runCommonKeyValueDBTest', () => runCommonKeyValueDBTest(db))

describe('runCommonKeyValueDaoTest', () => runCommonKeyValueDaoTest(dao))

test('saveBatch with EXAT', async () => {
  const testIds = _range(1, 4).map(n => `id${n}`)
  const testEntries: KeyValueDBTuple[] = testIds.map(id => [id, Buffer.from(`${id}value`)])

  await db.saveBatch(TEST_TABLE, testEntries, {
    expireAt: localTime.now().plus(1, 'second').unix(),
  })
  let loaded = await db.getByIds(TEST_TABLE, testIds)
  expect(loaded.length).toBe(3)
  await pDelay(2000)
  loaded = await db.getByIds(TEST_TABLE, testIds)
  expect(loaded.length).toBe(0)
})

class C {
  @_AsyncMemo({
    cacheFactory: () =>
      new CommonKeyValueDaoMemoCache({
        dao,
        ttl: 1,
      }),
  })
  async get(k: string): Promise<Buffer | null> {
    console.log(`get ${k}`)
    return Buffer.from(k)
  }
}

const c = new C()

test('CommonKeyValueDaoMemoCache serial', async () => {
  for (const _ of _range(10)) {
    console.log(await c.get('key'))
    await pDelay(100)
  }
})

test('CommonKeyValueDaoMemoCache async swarm', async () => {
  await Promise.all(
    _range(30).map(async () => {
      console.log(await c.get('key'))
    }),
  )
})
