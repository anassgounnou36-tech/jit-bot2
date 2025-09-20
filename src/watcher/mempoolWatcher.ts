import WebSocket from 'ws';
import { ethers } from 'ethers';
import { EventEmitter } from 'events';
import { getLogger } from '../logging/logger';
import { BloxrouteWatcher } from './bloxrouteWatcher';

export interface PendingSwapDetected {
  candidateId: string;
  txHash: string;
  rawTxHex: string;
  poolAddress: string;
  tokenIn: string;
  tokenOut: string;
  amountIn: string;
  amountInHuman: string;
  amountOutMin?: string; // Optional field from exactInputSingle/exactInput
  feeTier: number;
  direction: 'token0->token1' | 'token1->token0';
  estimatedUsd: string;
  blockNumberSeen: number;
  timestamp: number;
  provider: 'local-node' | 'alchemy' | 'blocknative' | 'bloxroute';
  decodedCall: {
    method: string;
    params: any;
  };
  // Legacy compatibility fields
  id?: string;
  poolId?: string;
  amountUSD?: string;
  poolFeeTier?: number;
  calldata?: string;
  amountOutEstimated?: string;
  from?: string;
  to?: string;
}

// Legacy interface for backward compatibility
export interface PendingSwap {
  hash: string;
  from: string;
  to: string;
  value: string;
  data: string;
  gasPrice: string;
  gasLimit: string;
  nonce: number;
  pool: string;
  tokenIn: string;
  tokenOut: string;
  amountIn: string;
  amountOutMinimum: string;
  expectedPrice: string;
  estimatedProfit: string;
  rawTransaction?: string;
}

export class MempoolWatcher extends EventEmitter {
  private ws: WebSocket | null = null;
  private provider: ethers.providers.WebSocketProvider;
  private fallbackProvider?: ethers.providers.JsonRpcProvider;
  private bloxrouteWatcher?: BloxrouteWatcher;
  private logger: any;
  private config: any;
  private metrics: any;
  
  private readonly UNISWAP_V3_ROUTER = '0xE592427A0AEce92De3Edee1F18E0157C05861564';
  private readonly UNISWAP_V3_ROUTER_V2 = '0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45';
  private readonly UNISWAP_UNIVERSAL_ROUTER = '0xEf1c6E67703c7BD7107eed8303Fbe6EC2554BF6B';
  
  private readonly UNISWAP_ROUTER_ADDRESSES = [
    this.UNISWAP_V3_ROUTER,
    this.UNISWAP_V3_ROUTER_V2,
    this.UNISWAP_UNIVERSAL_ROUTER
  ];
  
  private readonly UNISWAP_V3_SIGNATURES = {
    exactInputSingle: '0x414bf389',
    exactInput: '0xc04b8d59',
    exactOutputSingle: '0xdb3e2198',
    exactOutput: '0xf28c0498',
    multicall: '0xac9650d8',
    multicallWithDeadline: '0x5ae401dc',
    swap: '0x128acb08'
  };

  private targetPools: Map<string, any> = new Map();
  
  // Deduplication cache for transaction hashes (TTL: 5 minutes)
  private seenTxHashes: Map<string, number> = new Map();
  private readonly CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

  // Pending transaction volume tracking
  private pendingTxCounts: Map<string, number> = new Map(); // source -> count
  private lastPendingCount60s = 0;
  private lowFeedConsecutiveMinutes = 0;
  private pendingCountInterval10s?: NodeJS.Timeout;
  private pendingCountInterval60s?: NodeJS.Timeout;

  constructor(config: any, provider: ethers.providers.WebSocketProvider, fallbackProvider?: ethers.providers.JsonRpcProvider, metrics?: any) {
    super();
    this.config = config;
    this.logger = getLogger().child({ component: 'mempool-watcher' });
    this.provider = provider;
    this.fallbackProvider = fallbackProvider;
    this.metrics = metrics;
    
    this.initializeTargetPools();
    
    // Initialize bloXroute watcher if enabled
    if (this.config.useBloxroute) {
      this.bloxrouteWatcher = new BloxrouteWatcher(this.config, this.parseSwapTransaction.bind(this));
      this.setupBloxrouteEventHandlers();
    }
    
    this.logger.info({
      msg: 'MempoolWatcher initialized',
      targetPoolsCount: this.targetPools.size,
      minSwapEth: config.minSwapEth,
      minSwapUsd: config.minSwapUsd,
      allowReconstruct: config.allowReconstructRawTx,
      bloxrouteEnabled: config.useBloxroute
    });
  }

  private initializeTargetPools(): void {
    for (const pool of this.config.pools) {
      const key = `${pool.token0}-${pool.token1}-${pool.fee}`;
      this.targetPools.set(key, pool);
      
      const reverseKey = `${pool.token1}-${pool.token0}-${pool.fee}`;
      this.targetPools.set(reverseKey, { ...pool, direction: 'reverse' });
    }
    
    this.logger.debug({
      msg: 'Target pools initialized',
      pools: Array.from(this.targetPools.keys())
    });
  }

  private setupBloxrouteEventHandlers(): void {
    if (!this.bloxrouteWatcher) return;

    // Handle transaction seen metrics
    this.bloxrouteWatcher.on('txSeen', (source: string) => {
      this.incrementPendingCount(source);
      if (this.metrics) {
        this.metrics.incrementMempoolTxsSeen(source);
      }
    });

    // Handle swap decoded metrics
    this.bloxrouteWatcher.on('swapDecoded', (source: string) => {
      if (this.metrics) {
        this.metrics.incrementMempoolSwapsDecoded(source);
      }
    });

    // Handle pending swap detected events
    this.bloxrouteWatcher.on('PendingSwapDetected', (swapData: PendingSwapDetected) => {
      // Check for deduplication
      if (this.isTransactionSeen(swapData.txHash, 'bloxroute')) {
        this.logger.debug({
          msg: 'Duplicate transaction from bloXroute',
          txHash: swapData.txHash
        });
        return;
      }

      // Re-emit the event for downstream processing
      this.emit('PendingSwapDetected', swapData);
    });
  }

  /**
   * Check if a transaction has already been processed and mark it as seen
   * @param txHash Transaction hash to check
   * @param source Source that processed the transaction
   * @returns true if already seen, false if new
   */
  private isTransactionSeen(txHash: string, source: string): boolean {
    const now = Date.now();
    
    // Clean expired entries
    this.cleanupExpiredCache();
    
    if (this.seenTxHashes.has(txHash)) {
      this.logger.debug({
        txHash,
        source,
        msg: 'Transaction already processed, skipping duplicate'
      });
      return true;
    }
    
    // Mark as seen
    this.seenTxHashes.set(txHash, now);
    return false;
  }

  /**
   * Clean up expired entries from the transaction cache
   */
  private cleanupExpiredCache(): void {
    const now = Date.now();
    const keysToDelete: string[] = [];
    
    this.seenTxHashes.forEach((timestamp, txHash) => {
      if (now - timestamp > this.CACHE_TTL_MS) {
        keysToDelete.push(txHash);
      }
    });
    
    keysToDelete.forEach(txHash => {
      this.seenTxHashes.delete(txHash);
    });
  }

  /**
   * Increment pending transaction count for a specific source
   */
  private incrementPendingCount(source: string): void {
    const currentCount = this.pendingTxCounts.get(source) || 0;
    this.pendingTxCounts.set(source, currentCount + 1);
  }

  /**
   * Start pending transaction volume tracking with periodic logging
   */
  private startPendingVolumeTracking(): void {
    this.logger.info({
      msg: 'Starting pending transaction volume tracking',
      logInterval10s: true,
      logInterval60s: true,
      warnThresholdPerMin: this.config.pendingFeedWarnThresholdPerMin
    });

    // Log counts every 10 seconds
    this.pendingCountInterval10s = setInterval(() => {
      this.logPendingCounts('10s');
    }, 10 * 1000);

    // Log counts every 60 seconds and check for low feed warning
    this.pendingCountInterval60s = setInterval(() => {
      this.logPendingCounts('60s');
      this.checkPendingFeedHealth();
    }, 60 * 1000);
  }

  /**
   * Log pending transaction counts and reset counters
   */
  private logPendingCounts(interval: '10s' | '60s'): void {
    let totalCount = 0;
    this.pendingTxCounts.forEach((count) => {
      totalCount += count;
    });
    
    if (this.pendingTxCounts.size === 0) {
      this.logger.info({
        msg: `Pending TX count (${interval})`,
        total: 0,
        sources: {}
      });
    } else {
      const sources: Record<string, number> = {};
      this.pendingTxCounts.forEach((count, source) => {
        sources[source] = count;
      });

      this.logger.info({
        msg: `Pending TX count (${interval})`,
        total: totalCount,
        sources
      });
    }

    // Store 60s count for warning detection
    if (interval === '60s') {
      this.lastPendingCount60s = totalCount;
    }

    // Reset counters
    this.pendingTxCounts.clear();
  }

  /**
   * Check if pending feed is too low and emit warning
   */
  private checkPendingFeedHealth(): void {
    const threshold = this.config.pendingFeedWarnThresholdPerMin;
    
    if (this.lastPendingCount60s < threshold) {
      this.lowFeedConsecutiveMinutes++;
      
      if (this.lowFeedConsecutiveMinutes >= 2) {
        this.logger.warn({
          msg: '⚠️ Provider feed is too low, consider using Erigon/Nethermind or upgraded RPC plan',
          currentCount: this.lastPendingCount60s,
          thresholdPerMin: threshold,
          consecutiveMinutes: this.lowFeedConsecutiveMinutes
        });
      }
    } else {
      // Reset consecutive counter if feed is healthy
      this.lowFeedConsecutiveMinutes = 0;
    }
  }

  /**
   * Stop pending transaction volume tracking
   */
  private stopPendingVolumeTracking(): void {
    if (this.pendingCountInterval10s) {
      clearInterval(this.pendingCountInterval10s);
      this.pendingCountInterval10s = undefined;
    }
    if (this.pendingCountInterval60s) {
      clearInterval(this.pendingCountInterval60s);
      this.pendingCountInterval60s = undefined;
    }
  }

  async start(): Promise<void> {
    this.logger.info('Starting mempool watcher - always on for real-time monitoring');
    
    try {
      const promises: Promise<void>[] = [];
      const activeSubscriptions: string[] = [];

      // Start Alchemy subscription if configured
      if (this.config.useAlchemyPendingTx) {
        promises.push(this.subscribeAlchemyPendingTransactions());
        activeSubscriptions.push('alchemy-pending-tx');
      }

      // Start ABI fallback subscription if configured
      if (this.config.useAbiPendingFallback) {
        promises.push(this.subscribeAbiFallbackPending());
        activeSubscriptions.push('abi-fallback');
      }

      // Start bloXroute subscription if configured
      if (this.config.useBloxroute && this.bloxrouteWatcher) {
        promises.push(this.bloxrouteWatcher.start());
        activeSubscriptions.push('bloxroute');
      }

      // If none are configured, fall back to standard subscription
      if (!this.config.useAlchemyPendingTx && !this.config.useAbiPendingFallback && !this.config.useBloxroute) {
        promises.push(this.subscribeStandardPendingTransactions());
        activeSubscriptions.push('standard-pending');
      }

      // Wait for all subscriptions to establish
      await Promise.all(promises);

      this.logger.info({
        msg: 'Mempool watcher started successfully',
        subscriptions: activeSubscriptions,
        deduplication: (this.config.useAlchemyPendingTx + this.config.useAbiPendingFallback + this.config.useBloxroute > 1) ? 'enabled' : 'disabled',
        features: ['raw-tx-capture', 'uniswap-decoding', 'pool-matching'],
        logAllPendingTx: this.config.logAllPendingTx,
        pendingFeedWarnThreshold: this.config.pendingFeedWarnThresholdPerMin
      });

      // Start pending transaction volume tracking if enabled
      if (this.config.logAllPendingTx) {
        this.startPendingVolumeTracking();
      }
    } catch (error: any) {
      this.logger.error({
        err: error,
        msg: 'Failed to start mempool watcher'
      });
      throw error;
    }
  }

  async stop(): Promise<void> {
    try {
      // Stop pending volume tracking
      this.stopPendingVolumeTracking();
      
      // Stop bloXroute watcher if initialized
      if (this.bloxrouteWatcher) {
        await this.bloxrouteWatcher.stop();
      }
      
      if (this.provider) {
        await this.provider.destroy();
      }
      if (this.ws) {
        this.ws.close();
      }
      this.logger.info('Mempool watcher stopped');
    } catch (error: any) {
      this.logger.error({
        err: error,
        msg: 'Error stopping mempool watcher'
      });
    }
  }

  private async subscribeStandardPendingTransactions(): Promise<void> {
    await this.provider.send('eth_subscribe', ['newPendingTransactions']);
    
    this.provider.on('pending', (txHash: string) => {
      this.processPendingTransactionEnhanced(txHash).catch(error => {
        this.logger.debug({
          err: error,
          txHash,
          msg: 'Error processing pending transaction'
        });
      });
    });
  }

  /**
   * Subscribe to generic pending transaction hashes and decode Uniswap router swaps via ABI
   * This provides broader mempool coverage with any WebSocket-enabled Ethereum node
   */
  private async subscribeAbiFallbackPending(): Promise<void> {
    // Subscribe to generic pending transaction hashes
    await this.provider.send('eth_subscribe', ['newPendingTransactions']);
    
    this.logger.info({
      msg: 'ABI fallback pending transactions subscription established',
      concurrencyLimit: 10,
      routers: this.UNISWAP_ROUTER_ADDRESSES
    });

    // Set up handler for transaction hashes
    this.provider.on('pending', (txHash: string) => {
      // Count pending transactions if debug mode is enabled
      if (this.config.logAllPendingTx) {
        this.incrementPendingCount('abi-fallback');
      }

      // Skip if already seen (deduplication)
      if (this.isTransactionSeen(txHash, 'abi-fallback')) {
        return;
      }

      this.processAbiFallbackTransaction(txHash).catch(error => {
        this.logger.debug({
          err: error,
          txHash,
          source: 'abi-fallback',
          msg: 'Error processing ABI fallback transaction'
        });
      });
    });
  }

  private async subscribeAlchemyPendingTransactions(): Promise<void> {
    try {
      // Subscribe to Alchemy's enhanced pending transactions with filters
      const subscriptionPayload = {
        toAddress: this.UNISWAP_ROUTER_ADDRESSES,
        includeRemoved: false,
        hashesOnly: false
      };

      const result = await this.provider.send('alchemy_pendingTransactions', [subscriptionPayload]);
      
      this.logger.info({
        msg: 'Alchemy pending transactions subscription established',
        subscriptionId: result,
        filteredAddresses: this.UNISWAP_ROUTER_ADDRESSES
      });

      // Set up message handler for full transaction objects
      this.provider._websocket.on('message', (data: string) => {
        try {
          const message = JSON.parse(data);
          
          if (message.method === 'alchemy_pendingTransactions' && message.params?.result) {
            const txData = message.params.result;
            
            // Count pending transactions if debug mode is enabled
            if (this.config.logAllPendingTx) {
              this.incrementPendingCount('alchemy');
            }
            
            this.processAlchemyTransactionObject(txData).catch(error => {
              this.logger.debug({
                err: error,
                txHash: txData.hash,
                msg: 'Error processing Alchemy transaction object'
              });
            });
          }
        } catch (parseError: any) {
          this.logger.debug({
            err: parseError,
            msg: 'Failed to parse Alchemy WebSocket message'
          });
        }
      });

    } catch (error: any) {
      this.logger.warn({
        err: error,
        msg: 'Failed to subscribe to Alchemy pending transactions, falling back to standard subscription'
      });
      // Fallback to standard subscription
      await this.subscribeStandardPendingTransactions();
    }
  }

  /**
   * Process transaction via ABI fallback path: fetch full tx, filter by router, and decode via ABI
   */
  private async processAbiFallbackTransaction(txHash: string): Promise<void> {
    if (this.metrics) {
      this.metrics.incrementMempoolTxsSeen('abi_fallback');
    }

    try {
      // Step 1: Fetch full transaction object using getTransaction
      const tx = await this.provider.getTransaction(txHash);
      if (!tx || !tx.to) {
        return;
      }

      // Step 2: Filter by router addresses - only process Uniswap router transactions
      if (!this.isUniswapV3Transaction(tx.to)) {
        return;
      }

      // Step 3: Check if transaction is already included in a block
      if (tx.blockNumber) {
        this.logger.debug({
          txHash,
          blockNumber: tx.blockNumber,
          source: 'abi-fallback',
          msg: 'CandidateRejected',
          reason: 'already_included'
        });
        return;
      }

      // Step 4: Attempt to get raw transaction hex (with backoff/retry)
      const rawTxHex = await this.getRawSignedTransactionWithRetry(txHash);
      
      // Allow proceeding without raw tx if reconstruction is enabled
      if (!rawTxHex && !this.config.allowReconstructRawTx) {
        if (this.metrics) {
          this.metrics.incrementMempoolTxsRawMissing('abi-fallback');
        }
        this.logger.debug({
          txHash,
          source: 'abi-fallback',
          allowReconstruct: this.config.allowReconstructRawTx,
          msg: 'CandidateRejected',
          reason: 'raw_tx_unavailable'
        });
        return;
      }

      if (rawTxHex) {
        if (this.metrics) {
          this.metrics.incrementMempoolTxsRawFetched('abi-fallback');
        }
      }

      // Step 5: Parse transaction via ABI decoding
      const swapData = await this.parseSwapTransaction(tx, rawTxHex || '');
      if (!swapData) {
        return;
      }

      // Log SwapObserved for ABI fallback source
      if (this.config.logTargetPoolSwaps) {
        this.logger.info({
          msg: 'SwapObserved',
          source: 'abi-fallback',
          candidateId: swapData.candidateId,
          txHash: swapData.txHash,
          poolAddress: swapData.poolAddress,
          tokenIn: swapData.tokenIn,
          tokenOut: swapData.tokenOut,
          feeTier: swapData.feeTier,
          direction: swapData.direction,
          amountIn: swapData.amountIn,
          amountInHuman: swapData.amountInHuman,
          decodedMethod: swapData.decodedCall.method
        });
      }

      // Increment decoded counter with source label
      if (this.metrics) {
        this.metrics.incrementMempoolSwapsDecoded('abi_fallback');
      }

      // Step 6: Emit PendingSwapDetected with source tracking
      this.logger.info({
        msg: 'PendingSwapDetected',
        source: 'abi-fallback',
        candidateId: swapData.candidateId,
        txHash: swapData.txHash,
        poolAddress: swapData.poolAddress,
        tokenIn: swapData.tokenIn,
        tokenOut: swapData.tokenOut,
        amountIn: swapData.amountIn,
        amountInHuman: swapData.amountInHuman,
        feeTier: swapData.feeTier,
        direction: swapData.direction,
        estimatedUsd: swapData.estimatedUsd,
        decodedMethod: swapData.decodedCall.method,
        provider: 'abi-fallback'
      });

      // Emit the event 
      this.emit('swapDetected', swapData);

    } catch (error: any) {
      this.logger.debug({
        err: error,
        txHash,
        source: 'abi-fallback',
        msg: 'Error in ABI fallback transaction processing'
      });
    }
  }

  /**
   * Get raw signed transaction with retry and backoff for better reliability
   */
  private async getRawSignedTransactionWithRetry(txHash: string, maxRetries: number = 3): Promise<string> {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const rawTx = await this.getRawSignedTransaction(txHash);
        if (rawTx) {
          return rawTx;
        }
      } catch (error: any) {
        this.logger.debug({
          err: error,
          txHash,
          attempt,
          maxRetries,
          source: 'abi-fallback',
          msg: 'Error fetching raw transaction, retrying'
        });
      }

      // Exponential backoff with jitter
      if (attempt < maxRetries) {
        const baseDelay = 100 * Math.pow(2, attempt - 1); // 100ms, 200ms, 400ms
        const jitter = Math.random() * 50; // 0-50ms jitter
        await new Promise(resolve => setTimeout(resolve, baseDelay + jitter));
      }
    }

    return ''; // Return empty string if all retries failed
  }

  private async processAlchemyTransactionObject(txData: any): Promise<void> {
    // Check for deduplication first
    if (this.isTransactionSeen(txData.hash, 'alchemy')) {
      return;
    }

    if (this.metrics) {
      this.metrics.incrementMempoolTxsSeen('alchemy');
    }

    try {
      // Convert Alchemy tx object to ethers format
      const tx: ethers.providers.TransactionResponse = {
        hash: txData.hash,
        to: txData.to,
        from: txData.from,
        data: txData.input || txData.data || '0x',
        value: ethers.BigNumber.from(txData.value || '0x0'),
        gasLimit: ethers.BigNumber.from(txData.gas || txData.gasLimit || '0x0'),
        gasPrice: txData.gasPrice ? ethers.BigNumber.from(txData.gasPrice) : undefined,
        maxFeePerGas: txData.maxFeePerGas ? ethers.BigNumber.from(txData.maxFeePerGas) : undefined,
        maxPriorityFeePerGas: txData.maxPriorityFeePerGas ? ethers.BigNumber.from(txData.maxPriorityFeePerGas) : undefined,
        nonce: parseInt(txData.nonce || '0x0', 16),
        type: txData.type ? parseInt(txData.type, 16) : undefined,
        blockNumber: txData.blockNumber ? parseInt(txData.blockNumber, 16) : undefined,
        blockHash: txData.blockHash || undefined,
        confirmations: 0,
        wait: () => Promise.resolve({} as any),
        chainId: txData.chainId ? parseInt(txData.chainId, 16) : 1,
        r: txData.r,
        s: txData.s,
        v: txData.v ? parseInt(txData.v, 16) : undefined
      };

      if (!tx.to) {
        return;
      }

      // Check if it's a Uniswap V3 transaction
      if (!this.isUniswapV3Transaction(tx.to)) {
        return;
      }

      // Check if already included
      if (tx.blockNumber) {
        this.logger.debug({
          txHash: tx.hash,
          blockNumber: tx.blockNumber,
          msg: 'CandidateRejected',
          reason: 'already_included'
        });
        return;
      }

      // For Alchemy events, we have the transaction input directly, so check if we need raw tx
      let rawTxHex = '';
      let rawTxSource = 'from_ws_object';

      // If the event includes all the data we need, proceed without raw tx fetch
      if (tx.data && tx.data !== '0x') {
        this.logger.debug({
          txHash: tx.hash,
          msg: 'Using transaction data directly from Alchemy event'
        });
      } else {
        // Fallback to fetching raw tx if needed
        rawTxHex = await this.getRawSignedTransaction(tx.hash);
        rawTxSource = rawTxHex ? 'http_fallback' : 'unavailable';
        
        if (!rawTxHex && !this.config.allowReconstructRawTx) {
          if (this.metrics) {
            this.metrics.incrementMempoolTxsRawMissing('alchemy-fallback');
          }
          this.logger.debug({
            txHash: tx.hash,
            allowReconstruct: this.config.allowReconstructRawTx,
            msg: 'CandidateRejected',
            reason: 'raw_tx_unavailable'
          });
          return;
        }
      }

      if (rawTxHex) {
        if (this.metrics) {
          this.metrics.incrementMempoolTxsRawFetched('alchemy-enhanced');
        }
      }

      // Log the source of raw transaction data
      this.logger.debug({
        txHash: tx.hash,
        raw_source: rawTxSource,
        msg: 'Processing Alchemy transaction object'
      });

      // Parse transaction data
      const swapData = await this.parseSwapTransaction(tx, rawTxHex || '');
      if (!swapData) {
        return;
      }

      // Update the provider field to indicate this came from Alchemy
      swapData.provider = 'alchemy';

      // Log SwapObserved IMMEDIATELY after decode and BEFORE amount threshold checks
      if (this.config.logTargetPoolSwaps) {
        this.logger.info({
          msg: 'SwapObserved',
          txHash: swapData.txHash,
          poolAddress: swapData.poolAddress,
          tokenIn: swapData.tokenIn,
          tokenOut: swapData.tokenOut,
          feeTier: swapData.feeTier,
          direction: swapData.direction,
          amountIn: swapData.amountIn,
          amountInHuman: swapData.amountInHuman,
          decodedMethod: swapData.decodedCall.method,
          provider: 'alchemy'
        });
      }

      // Increment decoded counter
      if (this.metrics) {
        this.metrics.incrementMempoolSwapsDecoded('alchemy');
      }

      // Emit PendingSwapDetected BEFORE threshold validation for visibility
      this.logger.info({
        msg: 'PendingSwapDetected',
        candidateId: swapData.candidateId,
        txHash: swapData.txHash,
        poolAddress: swapData.poolAddress,
        tokenIn: swapData.tokenIn,
        tokenOut: swapData.tokenOut,
        amountIn: swapData.amountIn,
        amountInHuman: swapData.amountInHuman,
        feeTier: swapData.feeTier,
        direction: swapData.direction,
        estimatedUsd: swapData.estimatedUsd,
        blockNumberSeen: swapData.blockNumberSeen,
        timestamp: swapData.timestamp,
        provider: swapData.provider,
        decodedCall: swapData.decodedCall,
        rawTxHex: swapData.rawTxHex ? 'available' : 'missing'
      });

      this.emit('PendingSwapDetected', swapData);

      // Validate thresholds (for downstream filtering, but emission already happened)
      const amountEth = parseFloat(ethers.utils.formatEther(swapData.amountIn));
      const estimatedUsdValue = parseFloat(swapData.estimatedUsd);

      if (amountEth < this.config.minSwapEth && estimatedUsdValue < this.config.minSwapUsd) {
        if (this.metrics) {
          this.metrics.incrementMempoolSwapsRejected('amount_too_small');
        }
        this.logger.debug({
          txHash: tx.hash,
          amountEth,
          estimatedUsdValue,
          msg: 'CandidateRejected',
          reason: 'amount_below_threshold'
        });
        return;
      }

      if (this.metrics) {
        this.metrics.incrementMempoolSwapsMatched();
      }

    } catch (error: any) {
      this.logger.debug({
        err: error,
        txHash: txData.hash,
        msg: 'Failed to process Alchemy transaction object'
      });
    }
  }

  private async processPendingTransactionEnhanced(txHash: string): Promise<void> {
    // Check for deduplication first
    if (this.isTransactionSeen(txHash, 'local-node')) {
      return;
    }

    if (this.metrics) {
      this.metrics.incrementMempoolTxsSeen('local-node');
    }

    try {
      // Step 1: Get transaction from mempool
      const tx = await this.provider.getTransaction(txHash);
      if (!tx || !tx.to) {
        return;
      }

      // Step 2: Check if it's a Uniswap V3 transaction
      if (!this.isUniswapV3Transaction(tx.to)) {
        return;
      }

      // Step 3: Check for victim replacement
      const currentTx = await this.provider.getTransaction(txHash);
      if (!currentTx || currentTx.hash !== txHash) {
        if (this.metrics) {
          this.metrics.incrementVictimReplacements();
        }
        this.logger.warn({
          txHash,
          msg: 'VictimReplaced',
          reason: 'transaction_replaced_before_processing'
        });
        return;
      }

      // Step 4: Check if already included
      if (currentTx.blockNumber) {
        this.logger.debug({
          txHash,
          blockNumber: currentTx.blockNumber,
          msg: 'CandidateRejected',
          reason: 'already_included'
        });
        return;
      }

      // Step 5: Attempt to get raw transaction hex
      const rawTxHex = await this.getRawSignedTransaction(txHash);
      
      // Don't reject if raw tx unavailable when reconstruction is allowed
      if (!rawTxHex && !this.config.allowReconstructRawTx) {
        if (this.metrics) {
          this.metrics.incrementMempoolTxsRawMissing('local-node');
        }
        this.logger.debug({
          txHash,
          allowReconstruct: this.config.allowReconstructRawTx,
          msg: 'CandidateRejected',
          reason: 'raw_tx_unavailable'
        });
        return;
      }

      if (rawTxHex) {
        if (this.metrics) {
          this.metrics.incrementMempoolTxsRawFetched('local-node');
        }
      } else {
        this.logger.warn({
          txHash,
          allowReconstruct: this.config.allowReconstructRawTx,
          msg: 'Proceeding without raw transaction - rawTxHex will be empty'
        });
      }

      // Step 6: Parse transaction data
      const swapData = await this.parseSwapTransaction(tx, rawTxHex || '');
      if (!swapData) {
        return;
      }

      // Log SwapObserved IMMEDIATELY after decode and BEFORE amount threshold checks
      if (this.config.logTargetPoolSwaps) {
        this.logger.info({
          msg: 'SwapObserved',
          txHash: swapData.txHash,
          poolAddress: swapData.poolAddress,
          tokenIn: swapData.tokenIn,
          tokenOut: swapData.tokenOut,
          feeTier: swapData.feeTier,
          direction: swapData.direction,
          amountIn: swapData.amountIn,
          amountInHuman: swapData.amountInHuman,
          decodedMethod: swapData.decodedCall.method
        });
      }

      // Increment decoded counter
      if (this.metrics) {
        this.metrics.incrementMempoolSwapsDecoded('local-node');
      }

      // Step 7: Emit PendingSwapDetected BEFORE threshold validation for visibility
      this.logger.info({
        msg: 'PendingSwapDetected',
        candidateId: swapData.candidateId,
        txHash: swapData.txHash,
        poolAddress: swapData.poolAddress,
        tokenIn: swapData.tokenIn,
        tokenOut: swapData.tokenOut,
        amountIn: swapData.amountIn,
        amountInHuman: swapData.amountInHuman,
        feeTier: swapData.feeTier,
        direction: swapData.direction,
        estimatedUsd: swapData.estimatedUsd,
        blockNumberSeen: swapData.blockNumberSeen,
        timestamp: swapData.timestamp,
        provider: swapData.provider,
        decodedCall: swapData.decodedCall,
        rawTxHex: swapData.rawTxHex ? 'available' : 'missing'
      });

      this.emit('PendingSwapDetected', swapData);

      // Step 8: Validate thresholds (for downstream filtering, but emission already happened)
      const amountEth = parseFloat(ethers.utils.formatEther(swapData.amountIn));
      const estimatedUsdValue = parseFloat(swapData.estimatedUsd);

      if (amountEth < this.config.minSwapEth && estimatedUsdValue < this.config.minSwapUsd) {
        if (this.metrics) {
          this.metrics.incrementMempoolSwapsRejected('amount_too_small');
        }
        this.logger.debug({
          txHash,
          amountEth,
          estimatedUsdValue,
          msg: 'CandidateRejected',
          reason: 'amount_below_threshold'
        });
        return;
      }

      if (this.metrics) {
        this.metrics.incrementMempoolSwapsMatched();
      }

    } catch (error: any) {
      this.logger.debug({
        err: error,
        txHash,
        msg: 'Failed to process pending transaction'
      });
    }
  }

  private async getRawSignedTransaction(txHash: string): Promise<string> {
    let rawTx = '';
    let source = 'unknown';

    try {
      // Try local node first
      rawTx = await this.provider.send('eth_getRawTransactionByHash', [txHash]);
      if (rawTx && rawTx !== '0x') {
        source = 'raw_from_ws';
        this.logger.debug({
          txHash,
          source,
          msg: 'Raw transaction retrieved from primary provider'
        });
        return rawTx;
      }
    } catch (error: any) {
      this.logger.debug({
        err: error,
        txHash,
        msg: 'Failed to get raw transaction from local node'
      });
    }

    // Try fallback provider if available
    if (this.fallbackProvider) {
      try {
        rawTx = await this.fallbackProvider.send('eth_getRawTransactionByHash', [txHash]);
        if (rawTx && rawTx !== '0x') {
          source = 'raw_from_http_fallback';
          this.logger.debug({
            txHash,
            source,
            msg: 'Raw transaction retrieved from fallback provider'
          });
          return rawTx;
        }
      } catch (error: any) {
        this.logger.debug({
          err: error,
          txHash,
          msg: 'Failed to get raw transaction from fallback provider'
        });
      }
    }

    // If raw tx unavailable and reconstruction is allowed, try to reconstruct
    if (this.config.allowReconstructRawTx) {
      try {
        const tx = await this.provider.getTransaction(txHash);
        if (tx && tx.v && tx.r && tx.s) {
          const reconstructed = this.reconstructRawTransaction(tx);
          if (reconstructed) {
            source = 'raw_reconstructed';
            this.logger.debug({
              txHash,
              source,
              msg: 'Raw transaction reconstructed from transaction data'
            });
            return reconstructed;
          }
        } else {
          this.logger.debug({
            txHash,
            msg: 'Cannot reconstruct: missing signature components (v/r/s)'
          });
        }
      } catch (error: any) {
        this.logger.debug({
          err: error,
          txHash,
          msg: 'Failed to reconstruct raw transaction'
        });
      }
    }

    source = 'raw_unavailable';
    this.logger.debug({
      txHash,
      source,
      allowReconstruct: this.config.allowReconstructRawTx,
      msg: 'Raw transaction unavailable'
    });

    return '';
  }

  private reconstructRawTransaction(tx: ethers.providers.TransactionResponse): string | null {
    try {
      // Prepare transaction data for serialization
      const txData: any = {
        to: tx.to,
        value: tx.value,
        data: tx.data,
        gasLimit: tx.gasLimit,
        nonce: tx.nonce,
        chainId: tx.chainId
      };

      // Handle different transaction types
      if (tx.type === 2) {
        // EIP-1559 transaction
        txData.type = 2;
        if (tx.maxFeePerGas) {
          txData.maxFeePerGas = tx.maxFeePerGas;
        }
        if (tx.maxPriorityFeePerGas) {
          txData.maxPriorityFeePerGas = tx.maxPriorityFeePerGas;
        }
      } else {
        // Legacy transaction
        txData.type = tx.type || 0;
        if (tx.gasPrice) {
          txData.gasPrice = tx.gasPrice;
        }
      }

      // Create signature object
      const signature = {
        v: tx.v!,
        r: tx.r!,
        s: tx.s!
      };

      // Serialize the transaction with signature
      return ethers.utils.serializeTransaction(txData, signature);
    } catch (error: any) {
      this.logger.debug({
        err: error,
        txHash: tx.hash,
        msg: 'Error reconstructing raw transaction'
      });
      return null;
    }
  }

  private isUniswapV3Transaction(to: string): boolean {
    const normalizedTo = to.toLowerCase();
    
    // Check if it's a router transaction
    if (this.UNISWAP_ROUTER_ADDRESSES.some(addr => addr.toLowerCase() === normalizedTo)) {
      return true;
    }
    
    // Check if it's a direct pool transaction
    for (const pool of this.config.pools) {
      if (pool.address.toLowerCase() === normalizedTo) {
        return true;
      }
    }
    
    return false;
  }

  private async parseSwapTransaction(tx: ethers.providers.TransactionResponse, rawTxHex: string): Promise<PendingSwapDetected | null> {
    try {
      // Determine if this is a router or direct pool transaction
      const normalizedTo = tx.to!.toLowerCase();
      const isRouter = this.UNISWAP_ROUTER_ADDRESSES.some(addr => addr.toLowerCase() === normalizedTo);
      
      if (isRouter) {
        return await this.parseRouterTransaction(tx, rawTxHex);
      } else {
        return await this.parseDirectPoolTransaction(tx, rawTxHex);
      }
    } catch (error: any) {
      this.logger.debug({
        err: error,
        txHash: tx.hash,
        msg: 'Error parsing swap transaction'
      });
      return null;
    }
  }

  private async parseRouterTransaction(tx: ethers.providers.TransactionResponse, rawTxHex: string): Promise<PendingSwapDetected | null> {
    try {
      const routerIface = new ethers.utils.Interface([
        'function exactInputSingle((address tokenIn, address tokenOut, uint24 fee, address recipient, uint256 deadline, uint256 amountIn, uint256 amountOutMinimum, uint160 sqrtPriceLimitX96))',
        'function exactInput((bytes path, address recipient, uint256 deadline, uint256 amountIn, uint256 amountOutMinimum))',
        'function exactOutputSingle((address tokenIn, address tokenOut, uint24 fee, address recipient, uint256 deadline, uint256 amountOut, uint256 amountInMaximum, uint160 sqrtPriceLimitX96))',
        'function exactOutput((bytes path, address recipient, uint256 deadline, uint256 amountOut, uint256 amountInMaximum))',
        'function multicall(bytes[] data) external payable',
        'function multicall(uint256 deadline, bytes[] data) external payable'
      ]);

      // Try to decode the transaction
      const methodId = tx.data.slice(0, 10);
      let parsed: any;

      try {
        parsed = routerIface.parseTransaction({ data: tx.data, value: tx.value });
      } catch (error: any) {
        this.logger.debug({
          txHash: tx.hash,
          methodId,
          msg: 'Failed to parse router transaction',
          err: error.message
        });
        return null;
      }

      // Handle different swap functions
      switch (parsed.name) {
        case 'exactInputSingle':
          return await this.parseExactInputSingle(tx, rawTxHex, parsed.args);
        
        case 'exactInput':
          return await this.parseExactInput(tx, rawTxHex, parsed.args);
        
        case 'exactOutputSingle':
          return await this.parseExactOutputSingle(tx, rawTxHex, parsed.args);
        
        case 'exactOutput':
          return await this.parseExactOutput(tx, rawTxHex, parsed.args);
        
        case 'multicall':
          return await this.parseMulticall(tx, rawTxHex, parsed.args);
        
        default:
          this.logger.debug({
            txHash: tx.hash,
            method: parsed.name,
            msg: 'Unsupported router method'
          });
          return null;
      }
    } catch (error: any) {
      this.logger.debug({
        err: error,
        txHash: tx.hash,
        msg: 'Error parsing router transaction'
      });
      return null;
    }
  }

  private async parseDirectPoolTransaction(tx: ethers.providers.TransactionResponse, rawTxHex: string): Promise<PendingSwapDetected | null> {
    try {
      const poolIface = new ethers.utils.Interface([
        'function swap(address recipient, bool zeroForOne, int256 amountSpecified, uint160 sqrtPriceLimitX96, bytes data)'
      ]);

      let parsed: any;
      try {
        parsed = poolIface.parseTransaction({ data: tx.data, value: tx.value });
      } catch (error: any) {
        this.logger.debug({
          txHash: tx.hash,
          msg: 'Failed to parse direct pool transaction',
          err: error.message
        });
        return null;
      }

      if (parsed.name === 'swap') {
        return await this.parseDirectPoolSwap(tx, rawTxHex, parsed.args);
      }

      return null;
    } catch (error: any) {
      this.logger.debug({
        err: error,
        txHash: tx.hash,
        msg: 'Error parsing direct pool transaction'
      });
      return null;
    }
  }

  private async parseExactInputSingle(
    tx: ethers.providers.TransactionResponse, 
    rawTxHex: string, 
    args: any
  ): Promise<PendingSwapDetected | null> {
    try {
      const params = args[0]; // exactInputSingle takes a struct as first param
      const { tokenIn, tokenOut, fee, amountIn, amountOutMinimum } = params;

      // Find target pool
      const poolKey = `${tokenIn.toLowerCase()}-${tokenOut.toLowerCase()}-${fee}`;
      const reversePoolKey = `${tokenOut.toLowerCase()}-${tokenIn.toLowerCase()}-${fee}`;
      
      const targetPool = this.targetPools.get(poolKey) || this.targetPools.get(reversePoolKey);
      if (!targetPool) {
        return null; // Not a pool we're monitoring
      }

      // Determine direction
      const direction = tokenIn.toLowerCase() === targetPool.token0.toLowerCase() ? 'token0->token1' : 'token1->token0';

      // Estimate USD value (simplified)
      const estimatedUsdValue = await this.estimateUSDValue(tokenIn, amountIn);

      const currentBlock = await this.provider.getBlockNumber();

      const swapData: PendingSwapDetected = {
        candidateId: `${tx.hash}_${Math.floor(Date.now() / 1000)}`,
        txHash: tx.hash!,
        rawTxHex,
        poolAddress: targetPool.address,
        tokenIn,
        tokenOut,
        amountIn: amountIn.toString(),
        amountInHuman: this.formatTokenAmount(amountIn, tokenIn === targetPool.token0 ? targetPool.decimals0 : targetPool.decimals1),
        amountOutMin: amountOutMinimum ? amountOutMinimum.toString() : undefined,
        feeTier: fee,
        direction,
        estimatedUsd: estimatedUsdValue.toString(),
        blockNumberSeen: currentBlock,
        timestamp: Math.floor(Date.now() / 1000),
        provider: 'local-node',
        decodedCall: {
          method: 'exactInputSingle',
          params: {
            tokenIn,
            tokenOut,
            fee,
            amountIn: amountIn.toString(),
            amountOutMinimum: amountOutMinimum ? amountOutMinimum.toString() : undefined,
            recipient: params.recipient,
            deadline: params.deadline.toString()
          }
        },
        // Legacy compatibility
        id: tx.hash,
        poolId: targetPool.pool,
        amountUSD: estimatedUsdValue.toString(),
        poolFeeTier: fee,
        calldata: tx.data
      };

      return swapData;
    } catch (error: any) {
      this.logger.debug({
        err: error,
        txHash: tx.hash,
        msg: 'Error parsing exactInputSingle'
      });
      return null;
    }
  }

  private async parseExactInput(
    tx: ethers.providers.TransactionResponse, 
    rawTxHex: string, 
    args: any
  ): Promise<PendingSwapDetected | null> {
    try {
      const { path, amountIn, amountOutMinimum } = args;
      
      // Decode path to get tokens and fees
      const pathInfo = this.decodePath(path);
      if (!pathInfo || pathInfo.tokens.length < 2) {
        return null;
      }

      // For multi-hop swaps, we focus on the first hop
      const tokenIn = pathInfo.tokens[0];
      const tokenOut = pathInfo.tokens[1];
      const fee = pathInfo.fees[0];

      // Find target pool for first hop
      const poolKey = `${tokenIn.toLowerCase()}-${tokenOut.toLowerCase()}-${fee}`;
      const reversePoolKey = `${tokenOut.toLowerCase()}-${tokenIn.toLowerCase()}-${fee}`;
      
      const targetPool = this.targetPools.get(poolKey) || this.targetPools.get(reversePoolKey);
      if (!targetPool) {
        return null; // Not a pool we're monitoring
      }

      // Determine direction
      const direction = tokenIn.toLowerCase() === targetPool.token0.toLowerCase() ? 'token0->token1' : 'token1->token0';

      // Estimate USD value
      const estimatedUsdValue = await this.estimateUSDValue(tokenIn, amountIn);

      const currentBlock = await this.provider.getBlockNumber();

      const swapData: PendingSwapDetected = {
        candidateId: `${tx.hash}_${Math.floor(Date.now() / 1000)}`,
        txHash: tx.hash!,
        rawTxHex,
        poolAddress: targetPool.address,
        tokenIn,
        tokenOut,
        amountIn: amountIn.toString(),
        amountInHuman: this.formatTokenAmount(amountIn, tokenIn === targetPool.token0 ? targetPool.decimals0 : targetPool.decimals1),
        amountOutMin: amountOutMinimum ? amountOutMinimum.toString() : undefined,
        feeTier: fee,
        direction,
        estimatedUsd: estimatedUsdValue.toString(),
        blockNumberSeen: currentBlock,
        timestamp: Math.floor(Date.now() / 1000),
        provider: 'local-node',
        decodedCall: {
          method: 'exactInput',
          params: {
            path: pathInfo,
            amountIn: amountIn.toString(),
            amountOutMinimum: amountOutMinimum ? amountOutMinimum.toString() : undefined,
            recipient: args.recipient,
            deadline: args.deadline.toString()
          }
        },
        // Legacy compatibility
        id: tx.hash,
        poolId: targetPool.pool,
        amountUSD: estimatedUsdValue.toString(),
        poolFeeTier: fee,
        calldata: tx.data
      };

      return swapData;
    } catch (error: any) {
      this.logger.debug({
        err: error,
        txHash: tx.hash,
        msg: 'Error parsing exactInput'
      });
      return null;
    }
  }

  private async parseExactOutputSingle(
    tx: ethers.providers.TransactionResponse, 
    rawTxHex: string, 
    args: any
  ): Promise<PendingSwapDetected | null> {
    try {
      const params = args[0]; // exactOutputSingle takes a struct as first param
      const { tokenIn, tokenOut, fee, amountOut, amountInMaximum } = params;

      // Find target pool
      const poolKey = `${tokenIn.toLowerCase()}-${tokenOut.toLowerCase()}-${fee}`;
      const reversePoolKey = `${tokenOut.toLowerCase()}-${tokenIn.toLowerCase()}-${fee}`;
      
      const targetPool = this.targetPools.get(poolKey) || this.targetPools.get(reversePoolKey);
      if (!targetPool) {
        return null; // Not a pool we're monitoring
      }

      // Determine direction
      const direction = tokenIn.toLowerCase() === targetPool.token0.toLowerCase() ? 'token0->token1' : 'token1->token0';

      // For exactOutputSingle, we use amountInMaximum as the input estimate for USD calculation
      const estimatedUsdValue = await this.estimateUSDValue(tokenIn, amountInMaximum);

      const currentBlock = await this.provider.getBlockNumber();

      const swapData: PendingSwapDetected = {
        candidateId: `${tx.hash}_${Math.floor(Date.now() / 1000)}`,
        txHash: tx.hash!,
        rawTxHex,
        poolAddress: targetPool.address,
        tokenIn,
        tokenOut,
        amountIn: amountInMaximum.toString(), // Maximum amount in for exactOutput
        amountInHuman: this.formatTokenAmount(amountInMaximum, tokenIn === targetPool.token0 ? targetPool.decimals0 : targetPool.decimals1),
        amountOutMin: amountOut.toString(), // This is the exact output amount
        feeTier: fee,
        direction,
        estimatedUsd: estimatedUsdValue.toString(),
        blockNumberSeen: currentBlock,
        timestamp: Math.floor(Date.now() / 1000),
        provider: 'local-node',
        decodedCall: {
          method: 'exactOutputSingle',
          params: {
            tokenIn,
            tokenOut,
            fee,
            amountOut: amountOut.toString(),
            amountInMaximum: amountInMaximum.toString(),
            recipient: params.recipient,
            deadline: params.deadline.toString(),
            sqrtPriceLimitX96: params.sqrtPriceLimitX96 ? params.sqrtPriceLimitX96.toString() : undefined
          }
        },
        // Legacy compatibility
        id: tx.hash,
        poolId: targetPool.pool,
        amountUSD: estimatedUsdValue.toString(),
        poolFeeTier: fee,
        calldata: tx.data
      };

      return swapData;
    } catch (error: any) {
      this.logger.debug({
        err: error,
        txHash: tx.hash,
        msg: 'Error parsing exactOutputSingle'
      });
      return null;
    }
  }

  private async parseExactOutput(
    tx: ethers.providers.TransactionResponse, 
    rawTxHex: string, 
    args: any
  ): Promise<PendingSwapDetected | null> {
    try {
      const { path, amountOut, amountInMaximum } = args;
      
      // Decode path to get tokens and fees
      const pathInfo = this.decodePath(path);
      if (!pathInfo || pathInfo.tokens.length < 2) {
        return null;
      }

      // For multi-hop swaps, we focus on the first hop
      const tokenIn = pathInfo.tokens[0];
      const tokenOut = pathInfo.tokens[1];
      const fee = pathInfo.fees[0];

      // Find target pool
      const poolKey = `${tokenIn.toLowerCase()}-${tokenOut.toLowerCase()}-${fee}`;
      const reversePoolKey = `${tokenOut.toLowerCase()}-${tokenIn.toLowerCase()}-${fee}`;
      
      const targetPool = this.targetPools.get(poolKey) || this.targetPools.get(reversePoolKey);
      if (!targetPool) {
        return null; // Not a pool we're monitoring
      }

      // Determine direction
      const direction = tokenIn.toLowerCase() === targetPool.token0.toLowerCase() ? 'token0->token1' : 'token1->token0';

      // For exactOutput, we use amountInMaximum as the input estimate for USD calculation
      const estimatedUsdValue = await this.estimateUSDValue(tokenIn, amountInMaximum);

      const currentBlock = await this.provider.getBlockNumber();

      const swapData: PendingSwapDetected = {
        candidateId: `${tx.hash}_${Math.floor(Date.now() / 1000)}`,
        txHash: tx.hash!,
        rawTxHex,
        poolAddress: targetPool.address,
        tokenIn,
        tokenOut,
        amountIn: amountInMaximum.toString(), // Maximum amount in for exactOutput
        amountInHuman: this.formatTokenAmount(amountInMaximum, tokenIn === targetPool.token0 ? targetPool.decimals0 : targetPool.decimals1),
        amountOutMin: amountOut.toString(), // This is the exact output amount
        feeTier: fee,
        direction,
        estimatedUsd: estimatedUsdValue.toString(),
        blockNumberSeen: currentBlock,
        timestamp: Math.floor(Date.now() / 1000),
        provider: 'local-node',
        decodedCall: {
          method: 'exactOutput',
          params: {
            path: pathInfo,
            amountOut: amountOut.toString(),
            amountInMaximum: amountInMaximum.toString(),
            recipient: args.recipient,
            deadline: args.deadline.toString()
          }
        },
        // Legacy compatibility
        id: tx.hash,
        poolId: targetPool.pool,
        amountUSD: estimatedUsdValue.toString(),
        poolFeeTier: fee,
        calldata: tx.data
      };

      return swapData;
    } catch (error: any) {
      this.logger.debug({
        err: error,
        txHash: tx.hash,
        msg: 'Error parsing exactOutput'
      });
      return null;
    }
  }

  private async parseMulticall(
    tx: ethers.providers.TransactionResponse, 
    rawTxHex: string, 
    args: any
  ): Promise<PendingSwapDetected | null> {
    try {
      // Handle both multicall signatures: multicall(bytes[]) and multicall(uint256, bytes[])
      let data: string[];
      
      if (args.length === 1) {
        // multicall(bytes[] data)
        data = args[0];
      } else if (args.length === 2) {
        // multicall(uint256 deadline, bytes[] data)
        data = args[1];
      } else {
        this.logger.debug({
          txHash: tx.hash,
          msg: 'Unexpected multicall args length',
          argsLength: args.length
        });
        return null;
      }
      
      // Parse each call in the multicall
      for (const callData of data) {
        const methodId = callData.slice(0, 10);
        
        if (methodId === this.UNISWAP_V3_SIGNATURES.exactInputSingle || 
            methodId === this.UNISWAP_V3_SIGNATURES.exactInput ||
            methodId === this.UNISWAP_V3_SIGNATURES.exactOutputSingle ||
            methodId === this.UNISWAP_V3_SIGNATURES.exactOutput) {
          
          // Recursively parse the nested call
          const nestedTx = { ...tx, data: callData };
          const result = await this.parseRouterTransaction(nestedTx, rawTxHex);
          if (result) {
            result.decodedCall.method = 'multicall->' + result.decodedCall.method;
            return result;
          }
        }
      }
      
      return null;
    } catch (error: any) {
      this.logger.debug({
        err: error,
        txHash: tx.hash,
        msg: 'Error parsing multicall'
      });
      return null;
    }
  }

  private async parseDirectPoolSwap(
    tx: ethers.providers.TransactionResponse, 
    rawTxHex: string, 
    args: any
  ): Promise<PendingSwapDetected | null> {
    try {
      const { recipient, zeroForOne, amountSpecified } = args;
      
      // Find the target pool by address
      const poolAddress = tx.to!.toLowerCase();
      let targetPool: any = null;
      
      for (const pool of this.config.pools) {
        if (pool.address.toLowerCase() === poolAddress) {
          targetPool = pool;
          break;
        }
      }
      
      if (!targetPool) {
        return null; // Pool not in our target list
      }
      
      // Determine tokenIn/tokenOut from zeroForOne
      const tokenIn = zeroForOne ? targetPool.token0 : targetPool.token1;
      const tokenOut = zeroForOne ? targetPool.token1 : targetPool.token0;
      
      // Convert amountSpecified (which can be negative) to amountIn
      const amountIn = amountSpecified.lt(0) ? amountSpecified.abs() : amountSpecified;
      
      // Determine direction
      const direction = zeroForOne ? 'token0->token1' : 'token1->token0';
      
      // Estimate USD value
      const estimatedUsdValue = await this.estimateUSDValue(tokenIn, amountIn);
      
      const currentBlock = await this.provider.getBlockNumber();
      
      const swapData: PendingSwapDetected = {
        candidateId: `${tx.hash}_${Math.floor(Date.now() / 1000)}`,
        txHash: tx.hash!,
        rawTxHex,
        poolAddress: targetPool.address,
        tokenIn,
        tokenOut,
        amountIn: amountIn.toString(),
        amountInHuman: this.formatTokenAmount(amountIn, zeroForOne ? targetPool.decimals0 : targetPool.decimals1),
        feeTier: targetPool.fee,
        direction,
        estimatedUsd: estimatedUsdValue.toString(),
        blockNumberSeen: currentBlock,
        timestamp: Math.floor(Date.now() / 1000),
        provider: 'local-node',
        decodedCall: {
          method: 'swap',
          params: {
            recipient,
            zeroForOne,
            amountSpecified: amountSpecified.toString(),
            poolAddress: targetPool.address
          }
        },
        // Legacy compatibility
        id: tx.hash,
        poolId: targetPool.pool,
        amountUSD: estimatedUsdValue.toString(),
        poolFeeTier: targetPool.fee,
        calldata: tx.data
      };
      
      return swapData;
    } catch (error: any) {
      this.logger.debug({
        err: error,
        txHash: tx.hash,
        msg: 'Error parsing direct pool swap'
      });
      return null;
    }
  }

  private decodePath(path: string): { tokens: string[], fees: number[] } | null {
    try {
      // Remove 0x prefix
      const cleanPath = path.startsWith('0x') ? path.slice(2) : path;
      
      const tokens: string[] = [];
      const fees: number[] = [];
      
      let offset = 0;
      
      // First token (20 bytes)
      tokens.push('0x' + cleanPath.slice(offset, offset + 40));
      offset += 40;
      
      // Parse subsequent (fee, token) pairs
      while (offset < cleanPath.length) {
        // Fee (3 bytes)
        const feeHex = cleanPath.slice(offset, offset + 6);
        const fee = parseInt(feeHex, 16);
        fees.push(fee);
        offset += 6;
        
        // Token (20 bytes)
        if (offset + 40 <= cleanPath.length) {
          tokens.push('0x' + cleanPath.slice(offset, offset + 40));
          offset += 40;
        }
      }
      
      return { tokens, fees };
    } catch (error: any) {
      this.logger.debug({
        err: error,
        path,
        msg: 'Error decoding path'
      });
      return null;
    }
  }

  private async estimateUSDValue(tokenAddress: string, amount: ethers.BigNumber): Promise<number> {
    try {
      // Simplified USD estimation - in production this would use price oracles
      // For now, use rough estimates based on known tokens
      const tokenLower = tokenAddress.toLowerCase();
      
      if (tokenLower === '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') { // WETH
        const ethValue = parseFloat(ethers.utils.formatEther(amount));
        return ethValue * 3500; // Rough ETH price
      } else if (tokenLower === '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48') { // USDC
        return parseFloat(ethers.utils.formatUnits(amount, 6));
      } else if (tokenLower === '0xdac17f958d2ee523a2206206994597c13d831ec7') { // USDT
        return parseFloat(ethers.utils.formatUnits(amount, 6));
      } else if (tokenLower === '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599') { // WBTC
        const btcValue = parseFloat(ethers.utils.formatUnits(amount, 8));
        return btcValue * 65000; // Rough BTC price
      }
      
      // Default fallback
      return 0;
    } catch (error: any) {
      this.logger.debug({
        err: error,
        tokenAddress,
        amount: amount.toString(),
        msg: 'Error estimating USD value'
      });
      return 0;
    }
  }

  private formatTokenAmount(amount: ethers.BigNumber, decimals: number): string {
    try {
      return parseFloat(ethers.utils.formatUnits(amount, decimals)).toFixed(6);
    } catch (error: any) {
      return '0.0';
    }
  }
}
