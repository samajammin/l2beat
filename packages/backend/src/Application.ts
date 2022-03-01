import { CoingeckoClient, Logger } from '@l2beat/common'

import { ApiServer } from './api/ApiServer'
import { BlocksController } from './api/controllers/BlocksController'
import { createBlocksRouter } from './api/routers/BlocksRouter'
import { createStatusRouter } from './api/routers/StatusRouter'
import { Config } from './config'
import { BlockNumberUpdater } from './core/BlockNumberUpdater'
import { PriceUpdater } from './core/PriceUpdater'
import { SafeBlockService } from './core/SafeBlockService'
import { StatusService } from './core/StatusService'
import { CoingeckoQueryService } from './peripherals/coingecko/CoingeckoQueryService'
import { BlockNumberRepository } from './peripherals/database/BlockNumberRepository'
import { DatabaseService } from './peripherals/database/DatabaseService'
import { PriceRepository } from './peripherals/database/PriceRepository'
import { AlchemyHttpClient } from './peripherals/ethereum/AlchemyHttpClient'
import { EthereumClient } from './peripherals/ethereum/EthereumClient'
import { EtherscanClient } from './peripherals/etherscan'
import { HttpClient } from './peripherals/HttpClient'

export class Application {
  start: () => Promise<void>

  constructor(config: Config) {
    /* - - - - - TOOLS - - - - - */

    const logger = new Logger(config.logger)

    /* - - - - - PERIPHERALS - - - - - */

    const knex = DatabaseService.createKnexInstance(config.databaseUrl)
    const databaseService = new DatabaseService(knex, logger)
    const blockNumberRepository = new BlockNumberRepository(knex, logger)
    const priceRepository = new PriceRepository(knex, logger)

    const httpClient = new HttpClient()
    const alchemyHttpClient = new AlchemyHttpClient(
      config.alchemyApiKey,
      httpClient,
      logger
    )
    const ethereumClient = new EthereumClient(alchemyHttpClient)
    const etherscanClient = new EtherscanClient(
      config.etherscanApiKey,
      httpClient,
      logger
    )
    const coingeckoClient = new CoingeckoClient(httpClient)
    const coingeckoQueryService = new CoingeckoQueryService(coingeckoClient)

    /* - - - - - CORE - - - - - */

    const safeBlockService = new SafeBlockService(
      config.core.safeBlockRefreshIntervalMs,
      config.core.safeBlockBlockOffset,
      ethereumClient,
      logger
    )
    const blockNumberUpdater = new BlockNumberUpdater(
      config.core.minBlockTimestamp,
      safeBlockService,
      etherscanClient,
      blockNumberRepository,
      logger
    )

    const priceUpdater = new PriceUpdater(
      coingeckoQueryService,
      priceRepository,
      config.tokens,
      config.core.minBlockTimestamp,
      logger
    )

    const statusService = new StatusService({
      alchemyHttpClient,
      blockNumberUpdater,
      databaseService,
      etherscanClient,
      safeBlockService,
    })

    /* - - - - - API - - - - - */

    const blocksController = new BlocksController(blockNumberRepository)

    const apiServer = new ApiServer(config.port, logger, [
      createBlocksRouter(blocksController),
      createStatusRouter(statusService),
    ])

    /* - - - - - START - - - - - */

    this.start = async () => {
      logger.for(this).info('Starting')

      await databaseService.migrateToLatest()

      await apiServer.listen()

      await safeBlockService.start()
      await blockNumberUpdater.start()
      await priceUpdater.start()

      logger.for(this).info('Started')
    }
  }
}
