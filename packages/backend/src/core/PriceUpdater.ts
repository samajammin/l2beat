import { createEventEmitter, JobQueue, Logger, UnixTime } from '@l2beat/common'

import { Token } from '../model/Token'
import { CoingeckoQueryService } from '../peripherals/coingecko/CoingeckoQueryService'
import {
  PriceRecord,
  PriceRepository,
} from '../peripherals/database/PriceRepository'

interface PriceUpdaterEvents {
  newPrices: {
    coingeckoId: CoingeckoId,
    timestamp: UnixTime,
  }
}

export class PriceUpdater {
  private events = createEventEmitter<PriceUpdaterEvents>()
  private jobQueue: JobQueue

  constructor(
    private coingeckoQueryService: CoingeckoQueryService,
    private priceRepository: PriceRepository,
    private tokens: Token[],
    private minTimestamp: UnixTime,
    private logger: Logger,
    private refreshIntervalMs = 60 * 60 * 1000
  ) {
    this.logger = this.logger.for(this)
    this.jobQueue = new JobQueue({ maxConcurrentJobs: 20 }, this.logger)
  }

  async start() {
    for (const token of this.tokens) {
      this.jobQueue.add({
        name: `${token.coingeckoId}-sync`,
        execute: () => this.syncTokenPriceFromDB(token),
      })
      this.jobQueue.add({
        name: `${token.coingeckoId}-update-price`,
        execute: () => this.updateTokenPrice(token),
      })
      setInterval(() => {
        this.jobQueue.add({
          name: `${token.coingeckoId}-update-price`,
          execute: () => this.updateTokenPrice(token),
        })
      }, this.refreshIntervalMs)
    }
  }

  async syncTokenPriceFromDB(token: Token) {
    const prices = await this.priceRepository.getAllByToken(token.coingeckoId)
    if (prices.length > 0) {
      this.events.emit('syncedPrices', prices)
    }
  }

  async updateTokenPrice(token: Token) {
    const latestKnownDate =
      await this.priceRepository.getLatestKnownDateByToken(token.coingeckoId)
    const from = (latestKnownDate ?? this.minTimestamp).toStartOf('hour')
    const to = UnixTime.now().toStartOf('hour')

    if (from.equals(to)) {
      return
    }

    const newPrices = await this.coingeckoQueryService.getUsdPriceHistory(
      token.coingeckoId,
      from,
      to,
      'hourly'
    )
    const priceRecords: PriceRecord[] = newPrices.map((price) => ({
      coingeckoId: token.coingeckoId,
      timestamp: price.timestamp,
      priceUsd: price.value,
    }))

    if (newPrices.length > 0) {
      this.events.emit('newPrices', priceRecords)
      this.priceRepository.addOrUpdate(priceRecords)
    }
  }

  onNewPrices(fn: (prices: PriceRecord[]) => void) {
    this.events.on('newPrices', fn)
    return () => {
      this.events.off('newPrices', fn)
    }
  }

  onSyncedPrices(fn: (prices: PriceRecord[]) => void) {
    this.events.on('syncedPrices', fn)
    return () => {
      this.events.off('syncedPrices', fn)
    }
  }
}
