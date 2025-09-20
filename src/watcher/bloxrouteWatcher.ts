import WebSocket from 'ws';
import { EventEmitter } from 'events';
import { ethers } from 'ethers';
import { getLogger } from '../logging/logger';
import { JitBotConfig } from '../config';
import { PendingSwapDetected } from './mempoolWatcher';

export interface BloxroutePendingTx {
  tx_hash: string;
  tx_contents: {
    to: string;
    input: string;
    from: string;
    value: string;
    gas_price?: string;
    gas?: string;
    nonce?: string;
  };
}

export class BloxrouteWatcher extends EventEmitter {
  private ws: WebSocket | null = null;
  private logger: any;
  private config: JitBotConfig;
  private reconnectInterval: ReturnType<typeof setTimeout> | null = null;
  private isConnected = false;
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 10;
  private reconnectDelay = 5000; // 5 seconds
  
  // Uniswap router addresses to filter
  private readonly UNISWAP_ROUTER_ADDRESSES = [
    '0xE592427A0AEce92De3Edee1F18E0157C05861564', // SwapRouter
    '0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45', // SwapRouter02
    '0xEf1c6E67703c7BD7107eed8303Fbe6EC2554BF6B'  // Universal Router
  ];

  constructor(config: JitBotConfig, parseSwapTransaction: (tx: ethers.providers.TransactionResponse, rawTxHex: string) => Promise<PendingSwapDetected | null>) {
    super();
    this.config = config;
    this.logger = getLogger().child({ component: 'bloxroute-watcher' });
    this.parseSwapTransaction = parseSwapTransaction;
  }

  private parseSwapTransaction: (tx: ethers.providers.TransactionResponse, rawTxHex: string) => Promise<PendingSwapDetected | null>;

  async start(): Promise<void> {
    if (!this.config.useBloxroute) {
      this.logger.info('bloXroute integration disabled');
      return;
    }

    if (!this.config.bloxrouteWsUrl || !this.config.bloxrouteAuthHeader) {
      this.logger.error('bloXroute configuration missing');
      return;
    }

    this.logger.info({
      msg: 'Starting bloXroute watcher',
      wsUrl: this.config.bloxrouteWsUrl
    });

    await this.connect();
  }

  async stop(): Promise<void> {
    this.logger.info('Stopping bloXroute watcher');
    
    if (this.reconnectInterval) {
      clearInterval(this.reconnectInterval);
      this.reconnectInterval = null;
    }

    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    
    this.isConnected = false;
  }

  private async connect(): Promise<void> {
    try {
      this.ws = new WebSocket(this.config.bloxrouteWsUrl!, {
        headers: {
          Authorization: this.config.bloxrouteAuthHeader!
        }
      });

      this.ws.on('open', () => {
        this.logger.info('bloXroute WebSocket connected');
        this.isConnected = true;
        this.reconnectAttempts = 0;
        this.subscribe();
      });

      this.ws.on('message', (data: string) => {
        this.handleMessage(data).catch(error => {
          this.logger.debug({
            err: error,
            msg: 'Error handling bloXroute message'
          });
        });
      });

      this.ws.on('close', (code: number, reason: string) => {
        this.logger.warn({
          msg: 'bloXroute WebSocket closed',
          code,
          reason: reason.toString()
        });
        this.isConnected = false;
        this.scheduleReconnect();
      });

      this.ws.on('error', (error: Error) => {
        this.logger.error({
          err: error,
          msg: 'bloXroute WebSocket error'
        });
        this.isConnected = false;
      });

    } catch (error: any) {
      this.logger.error({
        err: error,
        msg: 'Failed to connect to bloXroute'
      });
      this.scheduleReconnect();
    }
  }

  private subscribe(): void {
    const subscription = {
      method: 'subscribe',
      id: 1,
      params: {
        subscription: 'pendingTxs',
        include: ['tx_hash', 'tx_contents.to', 'tx_contents.input', 'tx_contents.from', 'tx_contents.value']
      }
    };

    this.logger.info({
      msg: 'Subscribing to bloXroute pending transactions',
      subscription
    });

    this.ws?.send(JSON.stringify(subscription));
  }

  private async handleMessage(data: string): Promise<void> {
    try {
      const message = JSON.parse(data);
      
      // Handle subscription confirmation
      if (message.id === 1 && message.result) {
        this.logger.info({
          msg: 'bloXroute subscription confirmed',
          result: message.result
        });
        return;
      }

      // Handle incoming pending transactions
      if (message.method === 'subscription' && message.params) {
        await this.handlePendingTransaction(message.params);
      }

    } catch (error: any) {
      this.logger.debug({
        err: error,
        msg: 'Error parsing bloXroute message',
        data: data.substring(0, 200) // Log first 200 chars for debugging
      });
    }
  }

  private async handlePendingTransaction(params: any): Promise<void> {
    const pendingTx: BloxroutePendingTx = params.result;
    
    if (!pendingTx || !pendingTx.tx_hash || !pendingTx.tx_contents) {
      return;
    }

    // Emit metrics for transaction seen
    this.emit('txSeen', 'bloxroute');

    // Filter by Uniswap router addresses
    const toAddress = pendingTx.tx_contents.to?.toLowerCase();
    if (!toAddress || !this.UNISWAP_ROUTER_ADDRESSES.some(addr => addr.toLowerCase() === toAddress)) {
      return;
    }

    this.logger.debug({
      component: 'mempool-watcher',
      msg: 'bloXroute pending TX received',
      txHash: pendingTx.tx_hash,
      router: this.getRouterName(toAddress)
    });

    try {
      // Convert bloXroute transaction format to ethers format
      const ethersTransaction: ethers.providers.TransactionResponse = {
        hash: pendingTx.tx_hash,
        to: pendingTx.tx_contents.to,
        from: pendingTx.tx_contents.from,
        value: ethers.BigNumber.from(pendingTx.tx_contents.value || '0'),
        data: pendingTx.tx_contents.input || '0x',
        gasPrice: ethers.BigNumber.from(pendingTx.tx_contents.gas_price || '0'),
        gasLimit: ethers.BigNumber.from(pendingTx.tx_contents.gas || '0'),
        nonce: parseInt(pendingTx.tx_contents.nonce || '0'),
        blockNumber: 0, // Not available in pending tx
        blockHash: undefined,
        confirmations: 0,
        timestamp: Math.floor(Date.now() / 1000),
        wait: async () => ({} as any),
        raw: pendingTx.tx_contents.input || '0x'
      };

      // Parse the swap transaction using existing parser
      const swapData = await this.parseSwapTransaction(ethersTransaction, pendingTx.tx_contents.input || '0x');
      
      if (swapData) {
        // Update the provider field to indicate bloXroute source
        swapData.provider = 'bloxroute' as any;
        
        this.logger.info({
          component: 'mempool-watcher',
          msg: 'bloXroute pending TX received',
          txHash: pendingTx.tx_hash,
          router: this.getRouterName(toAddress)
        });

        // Emit metrics for successful decode
        this.emit('swapDecoded', 'bloxroute');

        // Emit the swap detection event
        this.emit('PendingSwapDetected', swapData);
      }

    } catch (error: any) {
      this.logger.debug({
        err: error,
        txHash: pendingTx.tx_hash,
        msg: 'Error processing bloXroute transaction'
      });
    }
  }

  private getRouterName(address: string): string {
    const lowerAddress = address.toLowerCase();
    switch (lowerAddress) {
      case '0xe592427a0aece92de3edee1f18e0157c05861564':
        return 'SwapRouter';
      case '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45':
        return 'SwapRouter02';
      case '0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b':
        return 'Universal Router';
      default:
        return 'Unknown Router';
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      this.logger.error({
        msg: 'Max reconnection attempts reached for bloXroute',
        attempts: this.reconnectAttempts
      });
      return;
    }

    this.reconnectAttempts++;
    const delay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1); // Exponential backoff
    
    this.logger.info({
      msg: 'Scheduling bloXroute reconnection',
      attempt: this.reconnectAttempts,
      delayMs: delay
    });

    this.reconnectInterval = setTimeout(() => {
      this.connect();
    }, delay);
  }

  public isConnectedToBloXroute(): boolean {
    return this.isConnected;
  }
}