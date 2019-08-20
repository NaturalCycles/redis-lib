import {
  TEST_TABLE,
  testDao,
  testDB,
  testItems,
  testItemUnsavedSchema,
} from '@naturalcycles/db-dev-lib'
import { CommonDao, DBQuery } from '@naturalcycles/db-lib'
import { _sortBy } from '@naturalcycles/js-lib'
import { Debug } from '@naturalcycles/nodejs-lib'
import { toArray } from 'rxjs/operators'
import { RedisDB } from '../../redis.db'

// Start server before running these tests:
// redis-server /usr/local/etc/redis.conf

// Debug.enable('*')
Debug.enable('nc:*')
jest.setTimeout(60000)

const items = testItems(5)

let redis: RedisDB

beforeEach(async () => {
  redis = new RedisDB({
    runQueries: true,
  })
  await redis.resetCache()
})

afterEach(async () => {
  await redis.quit()
})

test('test1', async () => {
  // console.log(items)

  // const redis = new RedisDB()
  // await redis.resetCache()

  let loadedItems = await redis.getByIds(TEST_TABLE, items.map(i => i.id))
  expect(loadedItems).toEqual([])

  await redis.saveBatch(TEST_TABLE, items)
  // await redis.deleteByIds(TEST_TABLE, ['asdasd', items[0].id])
  loadedItems = await redis.getByIds(TEST_TABLE, items.map(i => i.id))
  // console.log(loadedItems)
  expect(loadedItems).toEqual(items)

  loadedItems = await redis
    .streamQuery(new DBQuery(TEST_TABLE))
    .pipe(toArray())
    .toPromise()
  expect(_sortBy(loadedItems, 'id')).toEqual(items)
  // console.log(loadedItems)
  // await pDelay(5000)

  // await redis.quit()
})

test('testDB', async () => {
  // await redis.resetCache()
  await testDB(redis, DBQuery)
})

test('testDao', async () => {
  const testItemDao = new CommonDao({
    table: TEST_TABLE,
    db: redis,
    bmUnsavedSchema: testItemUnsavedSchema,
    dbmUnsavedSchema: testItemUnsavedSchema,
  })
  await testDao(testItemDao, DBQuery)
})
