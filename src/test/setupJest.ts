import { jestLog, jestLogger } from '@naturalcycles/dev-lib/dist/testing'
import { polyfillDispose } from '@naturalcycles/js-lib'

polyfillDispose()

// Patch console functions so jest doesn't log it so verbose
console.log = console.warn = jestLog
console.error = jestLogger.error.bind(jestLogger)
