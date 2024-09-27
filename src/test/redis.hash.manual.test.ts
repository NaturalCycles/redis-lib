import { CommonKeyValueDao } from '@naturalcycles/db-lib'
import {
  runCommonHashKeyValueDBTest,
  runCommonHashKeyValueDaoTest,
  TEST_TABLE,
} from '@naturalcycles/db-lib/dist/testing'
import { KeyValueDBTuple } from '@naturalcycles/db-lib/src/kv/commonKeyValueDB'
import { _range, localTime, pDelay } from '@naturalcycles/js-lib'
import { RedisClient } from '../redisClient'
import { RedisHashKeyValueDB } from '../redisHashKeyValueDB'

const client = new RedisClient()
const hashKey = 'hashField'
const db = new RedisHashKeyValueDB({ client, hashKey })
const dao = new CommonKeyValueDao<Buffer>({ db, table: TEST_TABLE })

afterAll(async () => {
  await client.disconnect()
})

test('connect', async () => {
  await db.ping()
})

describe('runCommonHashKeyValueDBTest', () => runCommonHashKeyValueDBTest(db))
describe('runCommonKeyValueDaoTest', () => runCommonHashKeyValueDaoTest(dao))

test('saveBatch with EXAT', async () => {
  const testIds = _range(1, 4).map(n => `id${n}`)
  const testEntries: KeyValueDBTuple[] = testIds.map(id => [id, Buffer.from(`${id}value`)])

  await db.saveBatch(TEST_TABLE, testEntries, {
    expireAt: localTime.now().plus(1, 'second').unix,
  })
  let loaded = await db.getByIds(TEST_TABLE, testIds)
  expect(loaded.length).toBe(3)
  await pDelay(2000)
  loaded = await db.getByIds(TEST_TABLE, testIds)
  expect(loaded.length).toBe(0)
})
