## @naturalcycles/redis-lib

> Redis implementation of CommonDB interface

[![npm](https://img.shields.io/npm/v/@naturalcycles/redis-lib/latest.svg)](https://www.npmjs.com/package/@naturalcycles/redis-lib)
[![code style: prettier](https://img.shields.io/badge/code_style-prettier-ff69b4.svg?style=flat-square)](https://github.com/prettier/prettier)

# Features

- ...

# DEBUG namespaces

- `nc:redis-lib`
- `ioredis`, `ioredis:redis`, `ioredis:connection`

# Packaging

- `engines.node >= 10.13`: Latest Node.js LTS
- `main: dist/index.js`: commonjs, es2018
- `types: dist/index.d.ts`: typescript types
- `/src` folder with source `*.ts` files included

# Starting / debugging Redis server on OSX

    brew install redis
    brew services start redis
    brew services stop redis

    redis-server /usr/local/etc/redis.conf

    redis-cli ping
    redis-cli flushall

Location and size of local DB:

    ls -l /usr/local/var/db/redis/dump.rdb
