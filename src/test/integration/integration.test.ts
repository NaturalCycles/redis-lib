import { runCommonDaoTest, runCommonDBTest } from '@naturalcycles/db-lib'
import { Debug } from '@naturalcycles/nodejs-lib'
import { RedisDB } from '../../redis.db'

// Start server before running these tests:
// redis-server /usr/local/etc/redis.conf

// Debug.enable('*')
Debug.enable('nc:*')
jest.setTimeout(60000)

const redisDB = new RedisDB({
  runQueries: true,
  namespacePrefix: 'test_',
})

beforeAll(async () => {
  await redisDB.resetCache()
})

afterAll(async () => {
  await redisDB.quit()
})

const opt = {
  allowQueryUnsorted: true,
  allowStreamQueryToBeUnsorted: true,
}

describe('runCommonDBTest', () => runCommonDBTest(redisDB, opt))

describe('runCommonDaoTest', () => runCommonDaoTest(redisDB, opt))
