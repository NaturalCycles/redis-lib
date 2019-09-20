import { runCommonDaoTest, runCommonDBTest } from '@naturalcycles/db-lib'
import { Debug } from '@naturalcycles/nodejs-lib'
import { RedisDB } from '../../redis.db'

// Start server before running these tests:
// redis-server /usr/local/etc/redis.conf

// Debug.enable('*')
Debug.enable('nc:*')
jest.setTimeout(60000)

let redisDB: RedisDB

beforeEach(async () => {
  redisDB = new RedisDB({
    runQueries: true,
    namespacePrefix: 'test_',
  })
  await redisDB.resetCache()
})

afterEach(async () => {
  await redisDB.quit()
})

test('runCommonDBTest', async () => {
  await runCommonDBTest(redisDB)
})

test('runCommonDaoTest', async () => {
  await runCommonDaoTest(redisDB)
})
