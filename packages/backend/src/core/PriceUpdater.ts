import {
  createEventEmitter,
  JobQueue,
  Logger,
  UnixTime,
} from '@l2beat/common'

import { Token } from '../model/Token'
import { CoingeckoQueryService } from '../peripherals/coingecko/CoingeckoQueryService'
import {
  PriceRecord,
  PriceRepository,
} from '../peripherals/database/PriceRepository'

interface PriceUpdaterEvents {
  newPrices: PriceRecord[]
}

export class PriceUpdater {
  private events = createEventEmitter<PriceUpdaterEvents>()
  private jobQueue: JobQueue

  constructor(
    private coingeckoQueryService: CoingeckoQueryService,
    private priceRepository: PriceRepository,
    private tokens: Token[],
    private minTimestamp: UnixTime,
    private logger: Logger
  ) {
    this.logger = this.logger.for(this)
    this.jobQueue = new JobQueue({ maxConcurrentJobs: 20 }, this.logger)
  }

  async start() {
    console.log('STARTED')
  }

  async syncTokenPriceFromDB(token: Token) {
    const prices = await this.priceRepository.getAllByToken(token.coingeckoId)
    if(prices.length > 0) {
      this.events.emit('newPrices',prices)
    }
    //for testing until i figure out how to test events
    return prices
  }

  async updateTokenPrice(token: Token) {
    const latestKnownDate =
      await this.priceRepository.getLatestKnownDateByToken(token.coingeckoId)
    const from = latestKnownDate ? latestKnownDate : this.minTimestamp
    const to = UnixTime.fromDate(new Date())

    const newPrices = await this.coingeckoQueryService.getUsdPriceHistory(
      token.coingeckoId,
      from,
      to,
      'hourly'
    )
    const priceRecords: PriceRecord[] = newPrices.map(price => ({
      coingeckoId: token.coingeckoId,
      timestamp: price.timestamp,
      priceUsd: price.value
    }))

    if(newPrices.length > 0) {
      this.events.emit('newPrices',priceRecords)
      this.priceRepository.addOrUpdate(priceRecords)
    }
  }
}
