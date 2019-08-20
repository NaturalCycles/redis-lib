import { TEST_TABLE } from '@naturalcycles/db-dev-lib'
import { pDelay } from '@naturalcycles/js-lib'
import { RedisDB } from './redis.db'

test('redis lazy initialization should not throw', async () => {
  const redis = new RedisDB({
    redisOptions: {
      maxRetriesPerRequest: 1,
    },
  })
  await pDelay(1000)
  await redis.quit()
})

test('redis connection failure should throw', async () => {
  const redis = new RedisDB({
    redisOptions: {
      maxRetriesPerRequest: 1,
    },
  })
  await expect(redis.getByIds(TEST_TABLE, ['a'])).rejects.toThrow()
  // await redis.quit()
  await redis.quit()
})
