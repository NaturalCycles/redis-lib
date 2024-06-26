import { TEST_TABLE } from '@naturalcycles/db-lib/dist/testing'
import { pDelay } from '@naturalcycles/js-lib'
import { RedisClient } from './redisClient'
import { RedisKeyValueDB } from './redisKeyValueDB'

test('redis lazy initialization should not throw', async () => {
  await using _redis = new RedisClient({
    redisOptions: {
      maxRetriesPerRequest: 1,
    },
  })
  await pDelay(1000)
})

test('redis connection failure should throw', async () => {
  await using client = new RedisClient({
    redisOptions: {
      port: 15464, // non-existing
      maxRetriesPerRequest: 1,
    },
  })
  const db = new RedisKeyValueDB({ client })
  await expect(db.getByIds(TEST_TABLE, ['a'])).rejects.toThrow()
})
