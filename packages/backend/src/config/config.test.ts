import { LogLevel, UnixTime } from '@l2beat/common'
import { config as dotenv } from 'dotenv'

import { Config } from './Config'
import { getEnv } from './getEnv'

export function getTestConfig(): Config {
  dotenv()
  return {
    name: 'Backend/Test',
    logger: {
      logLevel: LogLevel.NONE,
      format: 'json',
    },
    port: 1337,
    alchemyApiKey: 'xXTestAlchemyKeyXx',
    etherscanApiKey: 'xXTestEtherscanKeyXx',
    databaseUrl: getEnv('TEST_DB_URL', 'xXTestDatabaseUrlXx'),
    core: {
      minBlockTimestamp: new UnixTime(0),
      safeBlockRefreshIntervalMs: 0,
      safeBlockBlockOffset: 0n,
    },
    tokens: [],
  }
}
