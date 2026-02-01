/**
 * LilySwap Pool Server
 * Professional, optimized, and reliable
 */

require('dotenv').config();
const { ethers } = require('ethers');
const express = require('express');
const fs = require('fs');
const path = require('path');

// State persistence file
const STATE_FILE = path.join(__dirname, 'state.json');

// ============================================
// CONFIGURATION
// ============================================

const POOL_ADDRESS = '0x5b184251f2269bab1216f3360d1ff89499cc9891';
const USDC_ADDRESS = '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48';
const POLL_INTERVAL = 2500; // Fast 2.5s polling
const BLOCKSCOUT_API = 'https://eth.blockscout.com/api';
const WEB_PORT = 3001;
const GAS_LIMIT = 100000;
const MAX_GAS_GWEI = 100;
const FEE_CACHE_MS = 10000; // 10s fee cache
const BALANCE_CACHE_MS = 2000; // 2s balance cache
const TX_CACHE_MS = 2000; // 2s tx list cache
const USDC_TX_CACHE_MS = 1500; // 1.5s USDC tx cache for faster updates

const RPC_LIST = [
    'https://rpc.ankr.com/eth',
    'https://ethereum.publicnode.com',
    'https://1rpc.io/eth'
];

// Optimized State with caching
const state = {
    balance: '0',
    balanceWei: null,
    balanceCacheTime: 0,
    usdcBalance: '0',
    usdcBalanceCacheTime: 0,
    ethPrice: 0,
    ltcPrice: 0,
    btcPrice: 0,
    usdtPrice: 1,
    ethPriceCacheTime: 0,
    processed: new Set(),
    queued: new Map(),
    swaps: [],
    logs: [],
    status: 'Starting...',
    rpcIndex: 0,
    feeCache: null,
    feeCacheTime: 0,
    nextNonce: null,
    pendingTxs: new Map(),
    startTime: Date.now(),
    txCache: null,
    txCacheTime: 0,
    pollCount: 0,
    lastPollTime: 0,
    lilyHolders: new Map(), // wallet -> LILY balance
    lilyBuys: [], // All LILY buy transactions (ETH -> LILY)
    lilySells: [], // All LILY sell transactions (LILY -> ETH)
    usdcTransactions: [], // USDC transactions
    onrampTransactions: [], // Credit card onramp transactions
    usdcTxCacheTime: 0, // USDC tx cache time
    userBalances: new Map(), // wallet -> {eth_balance, usdc_balance, lily_balance}
    txBalances: new Map(), // transaction hash -> balance after transaction
    // Optimization flags
    balancesDirty: true, // Only recalculate when transactions change
    lastTxCount: 0, // Track transaction count for change detection
    lastBalanceCalcTime: 0, // Debounce balance calculations
    cachedSortedTxs: null, // Cache sorted transactions
    cachedSortedTxsTime: 0, // Time of cached sorted txs
    // API response cache
    allTransactionsSorted: [], // Pre-sorted combined transactions for API
    allTransactionsSortedTime: 0 // Time of cached sorted API transactions
};

// ============================================
// STATE PERSISTENCE
// ============================================

const BACKUP_FILE = path.join(__dirname, 'state-backup.json');

function saveState() {
    try {
        const saveData = {
            processed: Array.from(state.processed),
            swaps: state.swaps,
            lilyHolders: Array.from(state.lilyHolders.entries()),
            lilyBuys: state.lilyBuys,
            lilySells: state.lilySells,
            usdcTransactions: state.usdcTransactions,
            onrampTransactions: state.onrampTransactions,
            userBalances: Array.from(state.userBalances.entries()),
            txBalances: Array.from(state.txBalances.entries()),
            savedAt: new Date().toISOString()
        };
        // Write to main file
        fs.writeFileSync(STATE_FILE, JSON.stringify(saveData, null, 2));
        // Also write to backup file
        fs.writeFileSync(BACKUP_FILE, JSON.stringify(saveData, null, 2));
    } catch (e) {
        console.error('Failed to save state:', e.message);
    }
}

function loadState() {
    // Try main file first, then backup
    const filesToTry = [STATE_FILE, BACKUP_FILE];

    for (const file of filesToTry) {
        try {
            if (fs.existsSync(file)) {
                const data = JSON.parse(fs.readFileSync(file, 'utf8'));
                if (data.processed) state.processed = new Set(data.processed);
                if (data.swaps) state.swaps = data.swaps;
                if (data.lilyHolders) state.lilyHolders = new Map(data.lilyHolders);
                if (data.lilyBuys) state.lilyBuys = data.lilyBuys;
                if (data.lilySells) state.lilySells = data.lilySells;
                if (data.usdcTransactions) state.usdcTransactions = data.usdcTransactions;
                if (data.onrampTransactions) state.onrampTransactions = data.onrampTransactions;
                if (data.userBalances) state.userBalances = new Map(data.userBalances);
                if (data.txBalances) state.txBalances = new Map(data.txBalances);
                console.log(`Loaded state from ${file} (saved: ${data.savedAt})`);
                return true;
            }
        } catch (e) {
            console.error(`Failed to load ${file}:`, e.message);
        }
    }
    return false;
}

// Auto-save state every 10 seconds (more frequent for safety)
setInterval(saveState, 10000);

// Save state on process exit
process.on('SIGINT', () => {
    console.log('\nSaving state before exit...');
    saveState();
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('\nSaving state before exit...');
    saveState();
    process.exit(0);
});

process.on('uncaughtException', (err) => {
    console.error('Uncaught exception, saving state:', err);
    saveState();
    process.exit(1);
});

// ============================================
// LOGGING (silent in production)
// ============================================

const SILENT_MODE = process.env.SILENT_MODE === 'true' || process.env.NODE_ENV === 'production';

function log(msg, type = 'info') {
    const time = new Date().toLocaleTimeString();
    state.logs.unshift({ time, msg, type });
    state.logs = state.logs.slice(0, 100);
    if (SILENT_MODE) return; // No console output in production
    const icons = { info: '\x1b[36m●\x1b[0m', success: '\x1b[32m✓\x1b[0m', warn: '\x1b[33m!\x1b[0m', error: '\x1b[31m✗\x1b[0m' };
    console.log(`${icons[type] || icons.info} [${time}] ${msg}`);
}

// ============================================
// RPC PROVIDER
// ============================================

let provider = null;
let wallet = null;

function createProvider() {
    const url = RPC_LIST[state.rpcIndex];
    return new ethers.JsonRpcProvider(url, 1, { staticNetwork: true });
}

function rotateRpc() {
    state.rpcIndex = (state.rpcIndex + 1) % RPC_LIST.length;
    provider = createProvider();
    wallet = new ethers.Wallet(process.env.POOL_PRIVATE_KEY, provider);
    state.nextNonce = null;
    state.feeCache = null;
    log(`RPC switched to ${RPC_LIST[state.rpcIndex].split('/')[2]}`, 'warn');
}

function initWallet() {
    if (!process.env.POOL_PRIVATE_KEY) {
        if (!SILENT_MODE) console.error('\x1b[31m✗ ERROR: POOL_PRIVATE_KEY not set in .env\x1b[0m');
        process.exit(1);
    }
    provider = createProvider();
    wallet = new ethers.Wallet(process.env.POOL_PRIVATE_KEY, provider);
    if (wallet.address.toLowerCase() !== POOL_ADDRESS) {
        if (!SILENT_MODE) console.error('\x1b[31m✗ ERROR: Private key does not match pool address\x1b[0m');
        process.exit(1);
    }
    return wallet;
}

// ============================================
// UTILITIES
// ============================================

// Fast fetch with timeout
async function fetchWithRetry(url, retries = 2) {
    for (let i = 0; i <= retries; i++) {
        try {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), 8000);
            const res = await fetch(url, { signal: controller.signal });
            clearTimeout(timeout);
            if (res.ok) return await res.json();
            if (res.status === 429) return null; // Rate limited, don't retry
        } catch (e) {
            if (i === retries) return null;
            await new Promise(r => setTimeout(r, 500));
        }
    }
    return null;
}

// Fetch coin prices
async function updateEthPrice() {
    const now = Date.now();
    if (state.ethPrice && (now - state.ethPriceCacheTime) < 30000) return; // 30s cache
    try {
        let got = false;
        const data = await fetchWithRetry('https://api.coingecko.com/api/v3/simple/price?ids=ethereum,litecoin,bitcoin,tether&vs_currencies=usd');
        if (data?.ethereum?.usd) { state.ethPrice = data.ethereum.usd; got = true; }
        if (data?.litecoin?.usd) { state.ltcPrice = data.litecoin.usd; got = true; }
        if (data?.bitcoin?.usd) { state.btcPrice = data.bitcoin.usd; got = true; }
        if (data?.tether?.usd) { state.usdtPrice = data.tether.usd; got = true; }
        // Fallback to Coinbase if CoinGecko failed
        if (!got) {
            const pairs = [['ETH-USD','ethPrice'],['LTC-USD','ltcPrice'],['BTC-USD','btcPrice'],['USDT-USD','usdtPrice']];
            await Promise.all(pairs.map(async ([pair,key]) => {
                try {
                    const r = await fetchWithRetry('https://api.coinbase.com/v2/prices/'+pair+'/spot');
                    if (r?.data?.amount) state[key] = parseFloat(r.data.amount);
                } catch(e){}
            }));
        }
        state.ethPriceCacheTime = now;
    } catch (e) {
        // Keep existing prices on error
    }
}

// Cached balance update
async function updateBalance(force = false) {
    const now = Date.now();
    if (!force && state.balanceWei && (now - state.balanceCacheTime) < BALANCE_CACHE_MS) {
        return true; // Use cached
    }
    try {
        const bal = await provider.getBalance(wallet.address);
        state.balanceWei = bal;
        state.balance = parseFloat(ethers.formatEther(bal)).toFixed(6);
        state.balanceCacheTime = now;
        return true;
    } catch (e) {
        rotateRpc();
        return false;
    }
}

// Cached USDC balance update
async function updateUsdcBalance(force = false) {
    const now = Date.now();
    if (!force && state.usdcBalance && (now - state.usdcBalanceCacheTime) < BALANCE_CACHE_MS) {
        return true; // Use cached
    }
    try {
        // ERC20 balanceOf ABI
        const usdcContract = new ethers.Contract(
            USDC_ADDRESS,
            ['function balanceOf(address) view returns (uint256)'],
            provider
        );
        const balance = await usdcContract.balanceOf(POOL_ADDRESS);
        // USDC has 6 decimals
        state.usdcBalance = parseFloat(ethers.formatUnits(balance, 6)).toFixed(2);
        state.usdcBalanceCacheTime = now;
        return true;
    } catch (e) {
        return false;
    }
}

// Calculate running balances for each transaction (optimized with caching)
function calculateRunningBalances(force = false) {
    const now = Date.now();
    const currentTxCount = state.lilyBuys.length + state.lilySells.length + state.usdcTransactions.length + state.onrampTransactions.length;

    // Fast path: if nothing changed and not forced, return cached results
    if (!force && !state.balancesDirty && currentTxCount === state.lastTxCount &&
        state.cachedSortedTxs && (now - state.cachedSortedTxsTime) < 1000) {
        return { userBalances: state.userBalances, txBalances: state.txBalances, cached: true };
    }

    // Build transaction array only when needed
    const allTxs = [];

    // Use for loop instead of forEach for better performance
    for (let i = 0; i < state.lilyBuys.length; i++) {
        const tx = state.lilyBuys[i];
        const isLilyEth = tx.type === 'LILYETH_TO_LILY';
        const isLilyLtc = tx.type === 'LILYLTC_TO_LILY';
        const isLilyBtc = tx.type === 'LILYBTC_TO_LILY';
        const isLilyUsdt = tx.type === 'LILYUSDT_TO_LILY';
        const isEthToLtc = tx.type === 'LILYETH_TO_LILYLTC';
        const isLtcToEth = tx.type === 'LILYLTC_TO_LILYETH';
        const isEthToBtc = tx.type === 'LILYETH_TO_LILYBTC';
        const isBtcToEth = tx.type === 'LILYBTC_TO_LILYETH';
        const isLtcToBtc = tx.type === 'LILYLTC_TO_LILYBTC';
        const isBtcToLtc = tx.type === 'LILYBTC_TO_LILYLTC';
        const isEthToUsdt = tx.type === 'LILYETH_TO_LILYUSDT';
        const isUsdtToEth = tx.type === 'LILYUSDT_TO_LILYETH';
        const isLtcToUsdt = tx.type === 'LILYLTC_TO_LILYUSDT';
        const isUsdtToLtc = tx.type === 'LILYUSDT_TO_LILYLTC';
        const isBtcToUsdt = tx.type === 'LILYBTC_TO_LILYUSDT';
        const isUsdtToBtc = tx.type === 'LILYUSDT_TO_LILYBTC';
        let txType = 'BUY_ETH';
        if (isLilyUsdt) txType = 'BUY_LILYUSDT';
        else if (isLilyBtc) txType = 'BUY_LILYBTC';
        else if (isLilyLtc) txType = 'BUY_LILYLTC';
        else if (isLilyEth) txType = 'BUY_LILYETH';
        else if (isEthToLtc) txType = 'SWAP_ETH_TO_LTC';
        else if (isLtcToEth) txType = 'SWAP_LTC_TO_ETH';
        else if (isEthToBtc) txType = 'SWAP_ETH_TO_BTC';
        else if (isBtcToEth) txType = 'SWAP_BTC_TO_ETH';
        else if (isLtcToBtc) txType = 'SWAP_LTC_TO_BTC';
        else if (isBtcToLtc) txType = 'SWAP_BTC_TO_LTC';
        else if (isEthToUsdt) txType = 'SWAP_ETH_TO_USDT';
        else if (isUsdtToEth) txType = 'SWAP_USDT_TO_ETH';
        else if (isLtcToUsdt) txType = 'SWAP_LTC_TO_USDT';
        else if (isUsdtToLtc) txType = 'SWAP_USDT_TO_LTC';
        else if (isBtcToUsdt) txType = 'SWAP_BTC_TO_USDT';
        else if (isUsdtToBtc) txType = 'SWAP_USDT_TO_BTC';
        allTxs.push({
            hash: tx.hash,
            txType,
            user: tx.user.toLowerCase(),
            time: new Date(tx.time).getTime(),
            lilyAmount: tx.lilyAmount || 0,
            ethAmount: tx.ethAmount || 0,
            ltcAmount: tx.ltcAmount || 0,
            btcAmount: tx.btcAmount || 0,
            usdtAmount: tx.usdtAmount || 0
        });
    }

    for (let i = 0; i < state.lilySells.length; i++) {
        const tx = state.lilySells[i];
        const isLilyEth = tx.type === 'LILY_TO_LILYETH';
        const isLilyLtc = tx.type === 'LILY_TO_LILYLTC';
        const isLilyBtc = tx.type === 'LILY_TO_LILYBTC';
        const isLilyUsdt = tx.type === 'LILY_TO_LILYUSDT';
        const isEthToLtc = tx.type === 'LILYETH_TO_LILYLTC';
        const isLtcToEth = tx.type === 'LILYLTC_TO_LILYETH';
        const isEthToBtc = tx.type === 'LILYETH_TO_LILYBTC';
        const isBtcToEth = tx.type === 'LILYBTC_TO_LILYETH';
        const isLtcToBtc = tx.type === 'LILYLTC_TO_LILYBTC';
        const isBtcToLtc = tx.type === 'LILYBTC_TO_LILYLTC';
        const isEthToUsdt = tx.type === 'LILYETH_TO_LILYUSDT';
        const isUsdtToEth = tx.type === 'LILYUSDT_TO_LILYETH';
        const isLtcToUsdt = tx.type === 'LILYLTC_TO_LILYUSDT';
        const isUsdtToLtc = tx.type === 'LILYUSDT_TO_LILYLTC';
        const isBtcToUsdt = tx.type === 'LILYBTC_TO_LILYUSDT';
        const isUsdtToBtc = tx.type === 'LILYUSDT_TO_LILYBTC';
        let txType = 'SELL';
        if (isLilyUsdt) txType = 'SELL_LILYUSDT';
        else if (isLilyBtc) txType = 'SELL_LILYBTC';
        else if (isLilyLtc) txType = 'SELL_LILYLTC';
        else if (isLilyEth) txType = 'SELL_LILYETH';
        else if (isEthToLtc) txType = 'SWAP_ETH_TO_LTC';
        else if (isLtcToEth) txType = 'SWAP_LTC_TO_ETH';
        else if (isEthToBtc) txType = 'SWAP_ETH_TO_BTC';
        else if (isBtcToEth) txType = 'SWAP_BTC_TO_ETH';
        else if (isLtcToBtc) txType = 'SWAP_LTC_TO_BTC';
        else if (isBtcToLtc) txType = 'SWAP_BTC_TO_LTC';
        else if (isEthToUsdt) txType = 'SWAP_ETH_TO_USDT';
        else if (isUsdtToEth) txType = 'SWAP_USDT_TO_ETH';
        else if (isLtcToUsdt) txType = 'SWAP_LTC_TO_USDT';
        else if (isUsdtToLtc) txType = 'SWAP_USDT_TO_LTC';
        else if (isBtcToUsdt) txType = 'SWAP_BTC_TO_USDT';
        else if (isUsdtToBtc) txType = 'SWAP_USDT_TO_BTC';
        allTxs.push({
            hash: tx.hash,
            txType,
            user: tx.user.toLowerCase(),
            time: new Date(tx.time).getTime(),
            lilyAmount: tx.lilyAmount || 0,
            ethAmount: tx.ethAmount || 0,
            ltcAmount: tx.ltcAmount || 0,
            btcAmount: tx.btcAmount || 0,
            usdtAmount: tx.usdtAmount || 0
        });
    }

    for (let i = 0; i < state.usdcTransactions.length; i++) {
        const tx = state.usdcTransactions[i];
        if (tx.isIncoming && tx.from) {
            allTxs.push({
                hash: tx.hash,
                txType: 'BUY_USDC',
                user: tx.from.toLowerCase(),
                time: new Date(tx.time).getTime(),
                lilyAmount: tx.lilyAmount || tx.value * 0.94
            });
        } else if (!tx.isIncoming && tx.to) {
            allTxs.push({
                hash: tx.hash,
                txType: 'SELL_USDC',
                user: tx.to.toLowerCase(),
                time: new Date(tx.time).getTime(),
                lilyAmount: tx.lilyAmount || tx.value
            });
        }
    }

    for (let i = 0; i < state.onrampTransactions.length; i++) {
        const tx = state.onrampTransactions[i];
        allTxs.push({
            hash: tx.hash,
            txType: 'ONRAMP',
            user: (tx.wallet || tx.user || '').toLowerCase(),
            time: new Date(tx.time).getTime(),
            lilyAmount: tx.lilyAmount || 0
        });
    }

    // Sort by time (oldest first)
    allTxs.sort((a, b) => a.time - b.time);

    // Calculate running balance for each user (LILY + Lily ETH + Lily LTC + Lily BTC + Lily USDT)
    const userBalances = new Map(); // user -> { lily, lilyEth, lilyLtc, lilyBtc, lilyUsdt }
    const txBalances = new Map();

    for (let i = 0; i < allTxs.length; i++) {
        const tx = allTxs[i];
        const current = userBalances.get(tx.user) || { lily: 0, lilyEth: 0, lilyLtc: 0, lilyBtc: 0, lilyUsdt: 0 };
        const newBal = { lily: current.lily, lilyEth: current.lilyEth, lilyLtc: current.lilyLtc, lilyBtc: current.lilyBtc, lilyUsdt: current.lilyUsdt };

        if (tx.txType === 'BUY_ETH' || tx.txType === 'BUY_USDC') {
            newBal.lily = current.lily + tx.lilyAmount;
        } else if (tx.txType === 'SELL' || tx.txType === 'SELL_USDC') {
            newBal.lily = Math.max(0, current.lily - tx.lilyAmount);
        } else if (tx.txType === 'BUY_LILYETH') {
            // LILYETH_TO_LILY: spent Lily ETH, gained LILY
            newBal.lily = current.lily + tx.lilyAmount;
            newBal.lilyEth = Math.max(0, current.lilyEth - tx.ethAmount);
        } else if (tx.txType === 'SELL_LILYETH') {
            // LILY_TO_LILYETH: spent LILY, gained Lily ETH
            newBal.lily = Math.max(0, current.lily - tx.lilyAmount);
            newBal.lilyEth = current.lilyEth + tx.ethAmount;
        } else if (tx.txType === 'BUY_LILYLTC') {
            // LILYLTC_TO_LILY: spent Lily LTC, gained LILY
            newBal.lily = current.lily + tx.lilyAmount;
            newBal.lilyLtc = Math.max(0, current.lilyLtc - (tx.ltcAmount || 0));
        } else if (tx.txType === 'SELL_LILYLTC') {
            // LILY_TO_LILYLTC: spent LILY, gained Lily LTC
            newBal.lily = Math.max(0, current.lily - tx.lilyAmount);
            newBal.lilyLtc = current.lilyLtc + (tx.ltcAmount || 0);
        } else if (tx.txType === 'SWAP_ETH_TO_LTC') {
            // LILYETH_TO_LILYLTC: spent Lily ETH, gained Lily LTC
            newBal.lilyEth = Math.max(0, current.lilyEth - (tx.ethAmount || 0));
            newBal.lilyLtc = current.lilyLtc + (tx.ltcAmount || 0);
        } else if (tx.txType === 'SWAP_LTC_TO_ETH') {
            // LILYLTC_TO_LILYETH: spent Lily LTC, gained Lily ETH
            newBal.lilyLtc = Math.max(0, current.lilyLtc - (tx.ltcAmount || 0));
            newBal.lilyEth = current.lilyEth + (tx.ethAmount || 0);
        } else if (tx.txType === 'BUY_LILYBTC') {
            // LILYBTC_TO_LILY: spent Lily BTC, gained LILY
            newBal.lily = current.lily + tx.lilyAmount;
            newBal.lilyBtc = Math.max(0, current.lilyBtc - (tx.btcAmount || 0));
        } else if (tx.txType === 'SELL_LILYBTC') {
            // LILY_TO_LILYBTC: spent LILY, gained Lily BTC
            newBal.lily = Math.max(0, current.lily - tx.lilyAmount);
            newBal.lilyBtc = current.lilyBtc + (tx.btcAmount || 0);
        } else if (tx.txType === 'SWAP_ETH_TO_BTC') {
            newBal.lilyEth = Math.max(0, current.lilyEth - (tx.ethAmount || 0));
            newBal.lilyBtc = current.lilyBtc + (tx.btcAmount || 0);
        } else if (tx.txType === 'SWAP_BTC_TO_ETH') {
            newBal.lilyBtc = Math.max(0, current.lilyBtc - (tx.btcAmount || 0));
            newBal.lilyEth = current.lilyEth + (tx.ethAmount || 0);
        } else if (tx.txType === 'SWAP_LTC_TO_BTC') {
            newBal.lilyLtc = Math.max(0, current.lilyLtc - (tx.ltcAmount || 0));
            newBal.lilyBtc = current.lilyBtc + (tx.btcAmount || 0);
        } else if (tx.txType === 'SWAP_BTC_TO_LTC') {
            newBal.lilyBtc = Math.max(0, current.lilyBtc - (tx.btcAmount || 0));
            newBal.lilyLtc = current.lilyLtc + (tx.ltcAmount || 0);
        } else if (tx.txType === 'BUY_LILYUSDT') {
            // LILYUSDT_TO_LILY: spent Lily USDT, gained LILY
            newBal.lily = current.lily + tx.lilyAmount;
            newBal.lilyUsdt = Math.max(0, current.lilyUsdt - (tx.usdtAmount || 0));
        } else if (tx.txType === 'SELL_LILYUSDT') {
            // LILY_TO_LILYUSDT: spent LILY, gained Lily USDT
            newBal.lily = Math.max(0, current.lily - tx.lilyAmount);
            newBal.lilyUsdt = current.lilyUsdt + (tx.usdtAmount || 0);
        } else if (tx.txType === 'SWAP_ETH_TO_USDT') {
            newBal.lilyEth = Math.max(0, current.lilyEth - (tx.ethAmount || 0));
            newBal.lilyUsdt = current.lilyUsdt + (tx.usdtAmount || 0);
        } else if (tx.txType === 'SWAP_USDT_TO_ETH') {
            newBal.lilyUsdt = Math.max(0, current.lilyUsdt - (tx.usdtAmount || 0));
            newBal.lilyEth = current.lilyEth + (tx.ethAmount || 0);
        } else if (tx.txType === 'SWAP_LTC_TO_USDT') {
            newBal.lilyLtc = Math.max(0, current.lilyLtc - (tx.ltcAmount || 0));
            newBal.lilyUsdt = current.lilyUsdt + (tx.usdtAmount || 0);
        } else if (tx.txType === 'SWAP_USDT_TO_LTC') {
            newBal.lilyUsdt = Math.max(0, current.lilyUsdt - (tx.usdtAmount || 0));
            newBal.lilyLtc = current.lilyLtc + (tx.ltcAmount || 0);
        } else if (tx.txType === 'SWAP_BTC_TO_USDT') {
            newBal.lilyBtc = Math.max(0, current.lilyBtc - (tx.btcAmount || 0));
            newBal.lilyUsdt = current.lilyUsdt + (tx.usdtAmount || 0);
        } else if (tx.txType === 'SWAP_USDT_TO_BTC') {
            newBal.lilyUsdt = Math.max(0, current.lilyUsdt - (tx.usdtAmount || 0));
            newBal.lilyBtc = current.lilyBtc + (tx.btcAmount || 0);
        } else if (tx.txType === 'ONRAMP') {
            newBal.lily = current.lily + tx.lilyAmount;
        }

        userBalances.set(tx.user, newBal);
        txBalances.set(tx.hash, {
            user: tx.user,
            balance: newBal.lily,
            lilyEthBalance: newBal.lilyEth,
            lilyLtcBalance: newBal.lilyLtc,
            lilyBtcBalance: newBal.lilyBtc,
            lilyUsdtBalance: newBal.lilyUsdt,
            change: newBal.lily - current.lily
        });
    }

    // Cache results
    state.cachedSortedTxs = allTxs;
    state.cachedSortedTxsTime = now;
    state.lastTxCount = currentTxCount;
    state.balancesDirty = false;

    return { userBalances, txBalances, cached: false };
}

// Update all user balances from transactions (optimized)
let lastUserBalanceCount = 0;
function updateAllUserBalances(force = false) {
    const now = Date.now();

    // Debounce: don't recalculate more than once per 500ms unless forced
    if (!force && (now - state.lastBalanceCalcTime) < 500) {
        return;
    }

    const { userBalances, txBalances, cached } = calculateRunningBalances(force);

    // If we used cached results, no need to update state
    if (cached) {
        return;
    }

    // Store transaction-specific balances
    state.txBalances = txBalances;

    // Store current user balances (reuse Map to avoid garbage collection)
    state.userBalances.clear();
    userBalances.forEach((bal, user) => {
        const lilyBalanceStr = bal.lily > 0 ? `${bal.lily.toFixed(4)} LILY` : 'EMPTY';
        const lilyEthStr = bal.lilyEth > 0 ? `${bal.lilyEth.toFixed(6)} ETH` : '0 ETH';
        const lilyLtcStr = bal.lilyLtc > 0 ? `${bal.lilyLtc.toFixed(6)} LTC` : '0 LTC';
        const lilyBtcStr = bal.lilyBtc > 0 ? `${bal.lilyBtc.toFixed(6)} BTC` : '0 BTC';
        const lilyUsdtStr = bal.lilyUsdt > 0 ? `${bal.lilyUsdt.toFixed(6)} USDT` : '0 USDT';
        state.userBalances.set(user, {
            eth_balance: '-- ETH',
            usdc_balance: '-- USDC',
            lily_balance: lilyBalanceStr,
            lily_eth_balance: lilyEthStr,
            lily_ltc_balance: lilyLtcStr,
            lily_btc_balance: lilyBtcStr,
            lily_usdt_balance: lilyUsdtStr
        });
    });

    state.lastBalanceCalcTime = now;
    state._userBalancesObjDirty = true;

    // Only log when user count changed (indicates new transactions)
    if (userBalances.size !== lastUserBalanceCount) {
        log(`Updated balances for ${userBalances.size} users`);
        lastUserBalanceCount = userBalances.size;
    }
}

// Get cached sorted transactions for API responses
function getCachedSortedTransactions() {
    const now = Date.now();
    const currentTxCount = state.lilyBuys.length + state.lilySells.length + state.usdcTransactions.length + state.onrampTransactions.length;

    // Use cache if less than 1 second old and tx count hasn't changed
    if (state.allTransactionsSorted.length > 0 &&
        (now - state.allTransactionsSortedTime) < 1000 &&
        currentTxCount === state.lastTxCount) {
        return state.allTransactionsSorted;
    }

    // Rebuild sorted transactions - deduplicate properly
    const lilyBuys = state.lilyBuys || [];
    const lilySells = state.lilySells || [];
    const usdcTxs = state.usdcTransactions || [];

    const txMap = new Map();

    // Add buys (ETH -> LILY)
    lilyBuys.forEach(tx => {
        if (tx.hash) txMap.set(tx.hash, tx);
    });

    // Add sells (LILY -> ETH or LILY -> USDC) - these are the user's swap requests
    lilySells.forEach(tx => {
        if (tx.hash) txMap.set(tx.hash, tx);
    });

    // Only add INCOMING USDC transactions (user buying LILY with USDC)
    // Skip outgoing USDC (pool returning USDC) - those are duplicates of lilySells LILY_TO_USDC
    usdcTxs.forEach(tx => {
        if (tx.hash && tx.isIncoming && !txMap.has(tx.hash)) {
            txMap.set(tx.hash, tx);
        }
    });

    // Add onramp (credit card) transactions
    (state.onrampTransactions || []).forEach(tx => {
        if (tx.hash && !txMap.has(tx.hash)) {
            txMap.set(tx.hash, tx);
        }
    });

    const allTransactions = Array.from(txMap.values()).sort((a, b) =>
        new Date(b.time) - new Date(a.time)
    );

    state.allTransactionsSorted = allTransactions;
    state.allTransactionsSortedTime = now;
    return allTransactions;
}

// Get balance for a specific transaction
function getBalanceForTransaction(txHash, userAddress) {
    const txBalance = state.txBalances?.get(txHash);
    if (txBalance && txBalance.user === userAddress.toLowerCase()) {
        return {
            eth_balance: '-- ETH',
            usdc_balance: '-- USDC',
            lily_balance: txBalance.balance > 0 ? `${txBalance.balance.toFixed(4)} LILY` : 'EMPTY',
            lily_eth_balance: txBalance.lilyEthBalance > 0 ? `${txBalance.lilyEthBalance.toFixed(6)} ETH` : '0 ETH',
            lily_ltc_balance: txBalance.lilyLtcBalance > 0 ? `${txBalance.lilyLtcBalance.toFixed(6)} LTC` : '0 LTC',
            lily_btc_balance: txBalance.lilyBtcBalance > 0 ? `${txBalance.lilyBtcBalance.toFixed(6)} BTC` : '0 BTC',
            lily_usdt_balance: txBalance.lilyUsdtBalance > 0 ? `${txBalance.lilyUsdtBalance.toFixed(6)} USDT` : '0 USDT',
            change: txBalance.change
        };
    }

    // Fallback to current balance
    return state.userBalances.get(userAddress.toLowerCase()) || {
        eth_balance: '-- ETH',
        usdc_balance: '-- USDC',
        lily_balance: 'EMPTY',
        lily_eth_balance: '0 ETH',
        lily_ltc_balance: '0 LTC',
        lily_btc_balance: '0 BTC',
        lily_usdt_balance: '0 USDT'
    };
}

// Cached transaction list fetch
async function fetchTransactions() {
    const now = Date.now();
    if (state.txCache && (now - state.txCacheTime) < TX_CACHE_MS) {
        return state.txCache;
    }
    const data = await fetchWithRetry(`${BLOCKSCOUT_API}?module=account&action=txlist&address=${POOL_ADDRESS}&sort=desc&limit=50`);
    if (data?.result) {
        state.txCache = data.result;
        state.txCacheTime = now;
    }
    return data?.result || state.txCache || [];
}

// Fetch USDC token transfers and decode messages (with caching for speed)
async function fetchUsdcTransactions(force = false) {
    const now = Date.now();
    // Use cache unless forced or cache expired
    if (!force && state.usdcTransactions.length > 0 && (now - state.usdcTxCacheTime) < USDC_TX_CACHE_MS) {
        return state.usdcTransactions;
    }

    try {
        const data = await fetchWithRetry(`${BLOCKSCOUT_API}?module=account&action=tokentx&address=${POOL_ADDRESS}&contractaddress=${USDC_ADDRESS}&sort=desc&limit=50`);
        if (data?.result) {
            // Process transactions quickly without fetching individual tx details (faster)
            const usdcTxs = data.result.map(tx => {
                const isIncoming = tx.to.toLowerCase() === POOL_ADDRESS.toLowerCase();
                const usdcAmount = parseFloat(tx.value) / 1e6;
                // Calculate LILY amount: 1 USDC = 1 LILY, minus 6% fee for buys
                const lilyAmount = isIncoming ? usdcAmount * 0.94 : usdcAmount;
                // User is the sender for incoming (buy), or recipient for outgoing (withdrawal)
                const user = isIncoming ? tx.from : tx.to;

                return {
                    hash: tx.hash,
                    from: tx.from,
                    to: tx.to,
                    user: user,
                    value: usdcAmount,
                    usdcAmount: usdcAmount,
                    lilyAmount: lilyAmount,
                    time: new Date(parseInt(tx.timeStamp) * 1000).toISOString(),
                    type: 'USDC',
                    action: isIncoming ? 'buy' : 'sell',
                    isIncoming: isIncoming
                };
            });

            // Check if transaction count changed (new transactions)
            if (usdcTxs.length !== state.usdcTransactions.length) {
                state.balancesDirty = true;
            }
            state.usdcTransactions = usdcTxs;
            state.usdcTxCacheTime = now;
            return usdcTxs;
        }
    } catch (e) {
        log('Failed to fetch USDC transactions', 'warn');
    }
    return state.usdcTransactions || [];
}

async function getCachedFeeData() {
    const now = Date.now();
    if (state.feeCache && (now - state.feeCacheTime) < FEE_CACHE_MS) {
        return state.feeCache;
    }
    try {
        const feeData = await provider.getFeeData();
        let maxFeePerGas = feeData.maxFeePerGas || ethers.parseUnits('30', 'gwei');
        let maxPriorityFeePerGas = feeData.maxPriorityFeePerGas || ethers.parseUnits('2', 'gwei');
        maxFeePerGas = maxFeePerGas * 110n / 100n;
        const maxGas = ethers.parseUnits(MAX_GAS_GWEI.toString(), 'gwei');
        if (maxFeePerGas > maxGas) maxFeePerGas = maxGas;
        state.feeCache = { maxFeePerGas, maxPriorityFeePerGas };
        state.feeCacheTime = now;
        return state.feeCache;
    } catch {
        return {
            maxFeePerGas: ethers.parseUnits('30', 'gwei'),
            maxPriorityFeePerGas: ethers.parseUnits('2', 'gwei')
        };
    }
}

function trackConfirmation(txSend, record) {
    state.pendingTxs.set(txSend.hash, { tx: txSend, record, startTime: Date.now() });
    txSend.wait(1).then(receipt => {
        state.pendingTxs.delete(txSend.hash);
        if (receipt.status === 1) {
            record.status = 'confirmed';
            log(`Confirmed: ${txSend.hash.slice(0,10)}... (block ${receipt.blockNumber})`, 'success');
        } else {
            record.status = 'failed';
            log(`TX failed: ${txSend.hash.slice(0,10)}...`, 'error');
        }
        updateBalance();
    }).catch(() => {
        state.pendingTxs.delete(txSend.hash);
        if (!record.status.includes('confirmed')) record.status = 'sent';
    });
}

// Calculate LILY holders from blockchain transactions (EXACT match to frontend logic)
// Cache for holder calculation
let holderCalcCacheTime = 0;
let holderCalcHasRun = false;
const HOLDER_CALC_CACHE_MS = 5000; // 5 second cache for holder calculations

async function calculateLilyHolders() {
    const now = Date.now();

    // Use cache to avoid recalculating too frequently (cache even if 0 holders)
    if (holderCalcHasRun && (now - holderCalcCacheTime) < HOLDER_CALC_CACHE_MS) {
        return; // Use cached holders
    }

    try {
        // Match frontend: sort=desc, limit=200
        const data = await fetchWithRetry(`${BLOCKSCOUT_API}?module=account&action=txlist&address=${POOL_ADDRESS}&sort=desc&limit=200`);
        if (!data?.result) {
            log('Failed to fetch transactions for holders', 'warn');
            return;
        }

        holderCalcCacheTime = now;
        holderCalcHasRun = true;
        // Silent in production - only log on initial fetch or errors

        state.lilyHolders.clear();
        const txns = data.result;

        for (const tx of txns) {
            // Skip outgoing transactions from pool
            if (tx.from.toLowerCase() === POOL_ADDRESS) continue;

            const userAddr = tx.from.toLowerCase();
            const inputData = tx.input || '';
            if (!inputData || inputData.length <= 2 || inputData === '0x') continue;

            try {
                const hexStr = inputData.startsWith('0x') ? inputData.slice(2) : inputData;
                const bytes = new Uint8Array(hexStr.match(/.{1,2}/g).map(byte => parseInt(byte, 16)));
                const message = new TextDecoder().decode(bytes);

                if (!message.startsWith('LILYSWAP:')) continue;

                const msgContent = message.replace('LILYSWAP:', '').trim();
                const current = state.lilyHolders.get(userAddr) || 0;

                // LILYETH_TO_LILY: user gains LILY
                const lilyEthToLilyMatch = msgContent.match(/^LILYETH_TO_LILY\s+([\d.]+)\s+ETH\s*->\s*([\d.]+)\s+LILY/);
                if (lilyEthToLilyMatch) {
                    const lilyAmount = parseFloat(lilyEthToLilyMatch[2]) || 0;
                    state.lilyHolders.set(userAddr, current + lilyAmount);
                    continue;
                }

                // LILY_TO_LILYETH: user loses LILY
                const lilyToLilyEthMatch = msgContent.match(/^LILY_TO_LILYETH\s+([\d.]+)\s+LILY\s*->\s*([\d.]+)\s+ETH/);
                if (lilyToLilyEthMatch) {
                    const lilyAmount = parseFloat(lilyToLilyEthMatch[1]) || 0;
                    state.lilyHolders.set(userAddr, current - lilyAmount);
                    continue;
                }

                // Standard swap: "X ETH -> Y LILY" or "X LILY -> Y ETH" or "X LILY -> Y USDC"
                const swapMatch = msgContent.match(/^([\d.]+)\s+(\w+)\s*->\s*([\d.]+)\s+(\w+)/);
                if (swapMatch) {
                    const [, fromAmt, fromSym, toAmt, toSym] = swapMatch;
                    // ETH -> LILY: user gains LILY
                    if (toSym.toUpperCase() === 'LILY') {
                        const lilyAmount = parseFloat(toAmt) || 0;
                        state.lilyHolders.set(userAddr, current + lilyAmount);
                    }
                    // LILY -> ETH or LILY -> USDC: user loses LILY
                    if (fromSym.toUpperCase() === 'LILY' && (toSym.toUpperCase() === 'ETH' || toSym.toUpperCase() === 'USDC')) {
                        const lilyAmount = parseFloat(fromAmt) || 0;
                        state.lilyHolders.set(userAddr, current - lilyAmount);
                    }
                }
            } catch (e) {
                // Could not decode - skip
            }
        }

        // Intermediate results (silent)

        // Add USDC transactions to holdings (buys add LILY, sells subtract LILY)
        if (state.usdcTransactions && state.usdcTransactions.length > 0) {
            for (const usdcTx of state.usdcTransactions) {
                if (usdcTx.isIncoming && usdcTx.from) {
                    // Incoming USDC = user buying LILY (add to balance)
                    const userAddr = usdcTx.from.toLowerCase();
                    const current = state.lilyHolders.get(userAddr) || 0;
                    const lilyAmount = usdcTx.lilyAmount || usdcTx.value * 0.94;
                    state.lilyHolders.set(userAddr, current + lilyAmount);
                } else if (!usdcTx.isIncoming && usdcTx.to) {
                    // Outgoing USDC = user withdrew/sold LILY (subtract from balance)
                    const userAddr = usdcTx.to.toLowerCase();
                    const current = state.lilyHolders.get(userAddr) || 0;
                    const lilyAmount = usdcTx.lilyAmount || usdcTx.value;
                    state.lilyHolders.set(userAddr, current - lilyAmount);
                }
            }
            // USDC transactions added (silent)
        }

        // Add onramp (credit card) transactions to holdings
        if (state.onrampTransactions && state.onrampTransactions.length > 0) {
            for (const tx of state.onrampTransactions) {
                const userAddr = (tx.wallet || tx.user || '').toLowerCase();
                if (!userAddr) continue;
                const current = state.lilyHolders.get(userAddr) || 0;
                state.lilyHolders.set(userAddr, current + (tx.lilyAmount || 0));
            }
        }

        // Apply Math.max(0, balance) at the end like frontend does
        for (const [addr, balance] of state.lilyHolders) {
            if (balance <= 0) {
                state.lilyHolders.delete(addr);
            } else {
                state.lilyHolders.set(addr, Math.max(0, balance));
            }
        }

        // Final holders calculated (state.lilyHolders.size available via API)
    } catch (e) {
        log(`Error calculating holders: ${e.message}`, 'error');
    }
}

// Get sorted list of LILY holders (use userBalances for accuracy)
function getLilyHoldersList() {
    const holders = [];

    state.userBalances.forEach((balanceObj, address) => {
        let lilyBal = 0;
        let lilyEthBal = 0;
        let lilyLtcBal = 0;

        if (balanceObj.lily_balance && balanceObj.lily_balance !== 'EMPTY') {
            const match = balanceObj.lily_balance.match(/([\d.]+)\s*LILY/);
            if (match) lilyBal = parseFloat(match[1]);
        }
        if (balanceObj.lily_eth_balance) {
            const match = balanceObj.lily_eth_balance.match(/([\d.]+)\s*ETH/);
            if (match) lilyEthBal = parseFloat(match[1]);
        }
        if (balanceObj.lily_ltc_balance) {
            const match = balanceObj.lily_ltc_balance.match(/([\d.]+)\s*LTC/);
            if (match) lilyLtcBal = parseFloat(match[1]);
        }
        let lilyBtcBal = 0;
        if (balanceObj.lily_btc_balance) {
            const match = balanceObj.lily_btc_balance.match(/([\d.]+)\s*BTC/);
            if (match) lilyBtcBal = parseFloat(match[1]);
        }
        let lilyUsdtBal = 0;
        if (balanceObj.lily_usdt_balance) {
            const match = balanceObj.lily_usdt_balance.match(/([\d.]+)\s*USDT/);
            if (match) lilyUsdtBal = parseFloat(match[1]);
        }

        if (lilyBal > 0 || lilyEthBal > 0 || lilyLtcBal > 0 || lilyBtcBal > 0 || lilyUsdtBal > 0) {
            holders.push({ address, balance: lilyBal, lilyEthBalance: lilyEthBal, lilyLtcBalance: lilyLtcBal, lilyBtcBalance: lilyBtcBal, lilyUsdtBalance: lilyUsdtBal });
        }
    });

    return holders.sort((a, b) => b.balance - a.balance);
}

// Get all LILY buy and sell transactions from blockchain
async function getLilyTransactions() {
    try {
        const data = await fetchWithRetry(`${BLOCKSCOUT_API}?module=account&action=txlist&address=${POOL_ADDRESS}&sort=desc&limit=200`);
        if (!data?.result) return { buys: [], sells: [] };

        const buys = [];
        const sells = [];
        const processedHashes = new Set(); // Track processed transaction hashes to avoid duplicates

        for (const tx of data.result) {
            if (tx.from.toLowerCase() === POOL_ADDRESS.toLowerCase()) continue;
            if (processedHashes.has(tx.hash)) continue; // Skip if already processed

            const inputData = tx.input || '';
            if (!inputData || inputData.length <= 2 || inputData === '0x') continue;

            try {
                const hexStr = inputData.startsWith('0x') ? inputData.slice(2) : inputData;
                const bytes = new Uint8Array(hexStr.match(/.{1,2}/g).map(byte => parseInt(byte, 16)));
                const message = new TextDecoder().decode(bytes);

                if (!message.startsWith('LILYSWAP:')) continue;
                const msgContent = message.replace('LILYSWAP:', '').trim();

                // LILYETH_TO_LILY: user buys LILY with Lilychain ETH
                const lilyEthToLilyMatch = msgContent.match(/^LILYETH_TO_LILY\s+([\d.]+)\s+ETH\s*->\s*([\d.]+)\s+LILY/);
                if (lilyEthToLilyMatch) {
                    buys.push({
                        hash: tx.hash,
                        user: tx.from,
                        ethAmount: parseFloat(lilyEthToLilyMatch[1]),
                        lilyAmount: parseFloat(lilyEthToLilyMatch[2]),
                        type: 'LILYETH_TO_LILY',
                        action: 'buy',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // LILY_TO_LILYETH: user sells LILY for Lilychain ETH
                const lilyToLilyEthMatch = msgContent.match(/^LILY_TO_LILYETH\s+([\d.]+)\s+LILY\s*->\s*([\d.]+)\s+ETH/);
                if (lilyToLilyEthMatch) {
                    sells.push({
                        hash: tx.hash,
                        user: tx.from,
                        lilyAmount: parseFloat(lilyToLilyEthMatch[1]),
                        ethAmount: parseFloat(lilyToLilyEthMatch[2]),
                        type: 'LILY_TO_LILYETH',
                        action: 'sell',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // LILYLTC_TO_LILY: user buys LILY with Lilychain LTC
                const lilyLtcToLilyMatch = msgContent.match(/^LILYLTC_TO_LILY\s+([\d.]+)\s+LTC\s*->\s*([\d.]+)\s+LILY/);
                if (lilyLtcToLilyMatch) {
                    buys.push({
                        hash: tx.hash,
                        user: tx.from,
                        ltcAmount: parseFloat(lilyLtcToLilyMatch[1]),
                        lilyAmount: parseFloat(lilyLtcToLilyMatch[2]),
                        type: 'LILYLTC_TO_LILY',
                        action: 'buy',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // LILY_TO_LILYLTC: user sells LILY for Lilychain LTC
                const lilyToLilyLtcMatch = msgContent.match(/^LILY_TO_LILYLTC\s+([\d.]+)\s+LILY\s*->\s*([\d.]+)\s+LTC/);
                if (lilyToLilyLtcMatch) {
                    sells.push({
                        hash: tx.hash,
                        user: tx.from,
                        lilyAmount: parseFloat(lilyToLilyLtcMatch[1]),
                        ltcAmount: parseFloat(lilyToLilyLtcMatch[2]),
                        type: 'LILY_TO_LILYLTC',
                        action: 'sell',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // LILYETH_TO_LILYLTC: ETH(LC) -> LTC(LC)
                const ethToLtcMatch = msgContent.match(/^LILYETH_TO_LILYLTC\s+([\d.]+)\s+ETH\s*->\s*([\d.]+)\s+LTC/);
                if (ethToLtcMatch) {
                    buys.push({
                        hash: tx.hash,
                        user: tx.from,
                        ethAmount: parseFloat(ethToLtcMatch[1]),
                        ltcAmount: parseFloat(ethToLtcMatch[2]),
                        type: 'LILYETH_TO_LILYLTC',
                        action: 'buy',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // LILYLTC_TO_LILYETH: LTC(LC) -> ETH(LC)
                const ltcToEthMatch = msgContent.match(/^LILYLTC_TO_LILYETH\s+([\d.]+)\s+LTC\s*->\s*([\d.]+)\s+ETH/);
                if (ltcToEthMatch) {
                    buys.push({
                        hash: tx.hash,
                        user: tx.from,
                        ltcAmount: parseFloat(ltcToEthMatch[1]),
                        ethAmount: parseFloat(ltcToEthMatch[2]),
                        type: 'LILYLTC_TO_LILYETH',
                        action: 'buy',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // LILYBTC_TO_LILY: user buys LILY with Lilychain BTC
                const lilyBtcToLilyMatch = msgContent.match(/^LILYBTC_TO_LILY\s+([\d.]+)\s+BTC\s*->\s*([\d.]+)\s+LILY/);
                if (lilyBtcToLilyMatch) {
                    buys.push({
                        hash: tx.hash,
                        user: tx.from,
                        btcAmount: parseFloat(lilyBtcToLilyMatch[1]),
                        lilyAmount: parseFloat(lilyBtcToLilyMatch[2]),
                        type: 'LILYBTC_TO_LILY',
                        action: 'buy',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // LILY_TO_LILYBTC: user sells LILY for Lilychain BTC
                const lilyToLilyBtcMatch = msgContent.match(/^LILY_TO_LILYBTC\s+([\d.]+)\s+LILY\s*->\s*([\d.]+)\s+BTC/);
                if (lilyToLilyBtcMatch) {
                    sells.push({
                        hash: tx.hash,
                        user: tx.from,
                        lilyAmount: parseFloat(lilyToLilyBtcMatch[1]),
                        btcAmount: parseFloat(lilyToLilyBtcMatch[2]),
                        type: 'LILY_TO_LILYBTC',
                        action: 'sell',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // LILYETH_TO_LILYBTC: ETH(LC) -> BTC(LC)
                const ethToBtcMatch = msgContent.match(/^LILYETH_TO_LILYBTC\s+([\d.]+)\s+ETH\s*->\s*([\d.]+)\s+BTC/);
                if (ethToBtcMatch) {
                    buys.push({
                        hash: tx.hash,
                        user: tx.from,
                        ethAmount: parseFloat(ethToBtcMatch[1]),
                        btcAmount: parseFloat(ethToBtcMatch[2]),
                        type: 'LILYETH_TO_LILYBTC',
                        action: 'buy',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // LILYBTC_TO_LILYETH: BTC(LC) -> ETH(LC)
                const btcToEthMatch = msgContent.match(/^LILYBTC_TO_LILYETH\s+([\d.]+)\s+BTC\s*->\s*([\d.]+)\s+ETH/);
                if (btcToEthMatch) {
                    buys.push({
                        hash: tx.hash,
                        user: tx.from,
                        btcAmount: parseFloat(btcToEthMatch[1]),
                        ethAmount: parseFloat(btcToEthMatch[2]),
                        type: 'LILYBTC_TO_LILYETH',
                        action: 'buy',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // LILYLTC_TO_LILYBTC: LTC(LC) -> BTC(LC)
                const ltcToBtcMatch = msgContent.match(/^LILYLTC_TO_LILYBTC\s+([\d.]+)\s+LTC\s*->\s*([\d.]+)\s+BTC/);
                if (ltcToBtcMatch) {
                    buys.push({
                        hash: tx.hash,
                        user: tx.from,
                        ltcAmount: parseFloat(ltcToBtcMatch[1]),
                        btcAmount: parseFloat(ltcToBtcMatch[2]),
                        type: 'LILYLTC_TO_LILYBTC',
                        action: 'buy',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // LILYBTC_TO_LILYLTC: BTC(LC) -> LTC(LC)
                const btcToLtcMatch = msgContent.match(/^LILYBTC_TO_LILYLTC\s+([\d.]+)\s+BTC\s*->\s*([\d.]+)\s+LTC/);
                if (btcToLtcMatch) {
                    buys.push({
                        hash: tx.hash,
                        user: tx.from,
                        btcAmount: parseFloat(btcToLtcMatch[1]),
                        ltcAmount: parseFloat(btcToLtcMatch[2]),
                        type: 'LILYBTC_TO_LILYLTC',
                        action: 'buy',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // LILYUSDT_TO_LILY: user buys LILY with Lilychain USDT
                const lilyUsdtToLilyMatch = msgContent.match(/^LILYUSDT_TO_LILY\s+([\d.]+)\s+USDT\s*->\s*([\d.]+)\s+LILY/);
                if (lilyUsdtToLilyMatch) {
                    buys.push({
                        hash: tx.hash, user: tx.from,
                        usdtAmount: parseFloat(lilyUsdtToLilyMatch[1]),
                        lilyAmount: parseFloat(lilyUsdtToLilyMatch[2]),
                        type: 'LILYUSDT_TO_LILY', action: 'buy',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // LILY_TO_LILYUSDT: user sells LILY for Lilychain USDT
                const lilyToLilyUsdtMatch = msgContent.match(/^LILY_TO_LILYUSDT\s+([\d.]+)\s+LILY\s*->\s*([\d.]+)\s+USDT/);
                if (lilyToLilyUsdtMatch) {
                    sells.push({
                        hash: tx.hash, user: tx.from,
                        lilyAmount: parseFloat(lilyToLilyUsdtMatch[1]),
                        usdtAmount: parseFloat(lilyToLilyUsdtMatch[2]),
                        type: 'LILY_TO_LILYUSDT', action: 'sell',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // LILYETH_TO_LILYUSDT: ETH(LC) -> USDT(LC)
                const ethToUsdtMatch = msgContent.match(/^LILYETH_TO_LILYUSDT\s+([\d.]+)\s+ETH\s*->\s*([\d.]+)\s+USDT/);
                if (ethToUsdtMatch) {
                    buys.push({
                        hash: tx.hash, user: tx.from,
                        ethAmount: parseFloat(ethToUsdtMatch[1]),
                        usdtAmount: parseFloat(ethToUsdtMatch[2]),
                        type: 'LILYETH_TO_LILYUSDT', action: 'buy',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // LILYUSDT_TO_LILYETH: USDT(LC) -> ETH(LC)
                const usdtToEthMatch = msgContent.match(/^LILYUSDT_TO_LILYETH\s+([\d.]+)\s+USDT\s*->\s*([\d.]+)\s+ETH/);
                if (usdtToEthMatch) {
                    buys.push({
                        hash: tx.hash, user: tx.from,
                        usdtAmount: parseFloat(usdtToEthMatch[1]),
                        ethAmount: parseFloat(usdtToEthMatch[2]),
                        type: 'LILYUSDT_TO_LILYETH', action: 'buy',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // LILYLTC_TO_LILYUSDT: LTC(LC) -> USDT(LC)
                const ltcToUsdtMatch = msgContent.match(/^LILYLTC_TO_LILYUSDT\s+([\d.]+)\s+LTC\s*->\s*([\d.]+)\s+USDT/);
                if (ltcToUsdtMatch) {
                    buys.push({
                        hash: tx.hash, user: tx.from,
                        ltcAmount: parseFloat(ltcToUsdtMatch[1]),
                        usdtAmount: parseFloat(ltcToUsdtMatch[2]),
                        type: 'LILYLTC_TO_LILYUSDT', action: 'buy',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // LILYUSDT_TO_LILYLTC: USDT(LC) -> LTC(LC)
                const usdtToLtcMatch = msgContent.match(/^LILYUSDT_TO_LILYLTC\s+([\d.]+)\s+USDT\s*->\s*([\d.]+)\s+LTC/);
                if (usdtToLtcMatch) {
                    buys.push({
                        hash: tx.hash, user: tx.from,
                        usdtAmount: parseFloat(usdtToLtcMatch[1]),
                        ltcAmount: parseFloat(usdtToLtcMatch[2]),
                        type: 'LILYUSDT_TO_LILYLTC', action: 'buy',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // LILYBTC_TO_LILYUSDT: BTC(LC) -> USDT(LC)
                const btcToUsdtMatch = msgContent.match(/^LILYBTC_TO_LILYUSDT\s+([\d.]+)\s+BTC\s*->\s*([\d.]+)\s+USDT/);
                if (btcToUsdtMatch) {
                    buys.push({
                        hash: tx.hash, user: tx.from,
                        btcAmount: parseFloat(btcToUsdtMatch[1]),
                        usdtAmount: parseFloat(btcToUsdtMatch[2]),
                        type: 'LILYBTC_TO_LILYUSDT', action: 'buy',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // LILYUSDT_TO_LILYBTC: USDT(LC) -> BTC(LC)
                const usdtToBtcMatch = msgContent.match(/^LILYUSDT_TO_LILYBTC\s+([\d.]+)\s+USDT\s*->\s*([\d.]+)\s+BTC/);
                if (usdtToBtcMatch) {
                    buys.push({
                        hash: tx.hash, user: tx.from,
                        usdtAmount: parseFloat(usdtToBtcMatch[1]),
                        btcAmount: parseFloat(usdtToBtcMatch[2]),
                        type: 'LILYUSDT_TO_LILYBTC', action: 'buy',
                        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                    });
                    processedHashes.add(tx.hash);
                    continue;
                }

                // Standard swap: ETH -> LILY (buy) or LILY -> ETH (sell)
                const swapMatch = msgContent.match(/^([\d.]+)\s+(\w+)\s*->\s*([\d.]+)\s+(\w+)/);
                if (swapMatch) {
                    const [, fromAmt, fromSym, toAmt, toSym] = swapMatch;

                    // ETH -> LILY = BUY
                    if (toSym.toUpperCase() === 'LILY' && fromSym.toUpperCase() === 'ETH') {
                        buys.push({
                            hash: tx.hash,
                            user: tx.from,
                            ethAmount: parseFloat(fromAmt),
                            lilyAmount: parseFloat(toAmt),
                            type: 'ETH_TO_LILY',
                            action: 'buy',
                            time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                        });
                        processedHashes.add(tx.hash);
                    }

                    // LILY -> ETH = SELL
                    else if (fromSym.toUpperCase() === 'LILY' && toSym.toUpperCase() === 'ETH') {
                        sells.push({
                            hash: tx.hash,
                            user: tx.from,
                            lilyAmount: parseFloat(fromAmt),
                            ethAmount: parseFloat(toAmt),
                            type: 'LILY_TO_ETH',
                            action: 'sell',
                            time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                        });
                        processedHashes.add(tx.hash);
                    }

                    // LILY -> USDC = SELL
                    else if (fromSym.toUpperCase() === 'LILY' && toSym.toUpperCase() === 'USDC') {
                        sells.push({
                            hash: tx.hash,
                            user: tx.from,
                            lilyAmount: parseFloat(fromAmt),
                            usdcAmount: parseFloat(toAmt),
                            ethAmount: 0,
                            type: 'LILY_TO_USDC',
                            action: 'sell',
                            time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                        });
                        processedHashes.add(tx.hash);
                    }
                }
            } catch (e) {
                // Could not decode - skip
            }
        }

        // Check if transaction count changed (mark dirty for recalculation)
        if (buys.length !== state.lilyBuys.length || sells.length !== state.lilySells.length) {
            state.balancesDirty = true;
        }
        state.lilyBuys = buys;
        state.lilySells = sells;
        return { buys, sells };
    } catch (e) {
        return { buys: state.lilyBuys || [], sells: state.lilySells || [] };
    }
}

// ============================================
// DECODE SWAP MESSAGE
// ============================================

function decodeSwap(input) {
    if (!input || input.length <= 2) return null;
    try {
        const hex = input.startsWith('0x') ? input.slice(2) : input;
        const bytes = new Uint8Array(hex.match(/.{1,2}/g).map(b => parseInt(b, 16)));
        const msg = new TextDecoder().decode(bytes);
        if (!msg.startsWith('LILYSWAP:')) return null;

        const withdrawMatch = msg.match(/^LILYSWAP:\s*ETH_WITHDRAW\s+([\d.]+)\s+ETH\s*->\s*([\d.]+)\s+ETH/);
        if (withdrawMatch) {
            return { fromAmt: parseFloat(withdrawMatch[1]), fromSym: 'ETH', toAmt: parseFloat(withdrawMatch[2]), toSym: 'ETH', type: 'ETH_WITHDRAW' };
        }

        const depositMatch = msg.match(/^LILYSWAP:\s*ETH_DEPOSIT\s+([\d.]+)\s+ETH/);
        if (depositMatch) {
            return { fromAmt: parseFloat(depositMatch[1]), fromSym: 'ETH', toAmt: parseFloat(depositMatch[1]), toSym: 'ETH', type: 'ETH_DEPOSIT' };
        }

        const lilyEthToLilyMatch = msg.match(/^LILYSWAP:\s*LILYETH_TO_LILY\s+([\d.]+)\s+ETH\s*->\s*([\d.]+)\s+LILY/);
        if (lilyEthToLilyMatch) {
            return { fromAmt: parseFloat(lilyEthToLilyMatch[1]), fromSym: 'ETH', toAmt: parseFloat(lilyEthToLilyMatch[2]), toSym: 'LILY', type: 'LILYETH_TO_LILY' };
        }

        const lilyToLilyEthMatch = msg.match(/^LILYSWAP:\s*LILY_TO_LILYETH\s+([\d.]+)\s+LILY\s*->\s*([\d.]+)\s+ETH/);
        if (lilyToLilyEthMatch) {
            return { fromAmt: parseFloat(lilyToLilyEthMatch[1]), fromSym: 'LILY', toAmt: parseFloat(lilyToLilyEthMatch[2]), toSym: 'ETH', type: 'LILY_TO_LILYETH' };
        }

        const lilyLtcToLilyMatch = msg.match(/^LILYSWAP:\s*LILYLTC_TO_LILY\s+([\d.]+)\s+LTC\s*->\s*([\d.]+)\s+LILY/);
        if (lilyLtcToLilyMatch) {
            return { fromAmt: parseFloat(lilyLtcToLilyMatch[1]), fromSym: 'LTC', toAmt: parseFloat(lilyLtcToLilyMatch[2]), toSym: 'LILY', type: 'LILYLTC_TO_LILY' };
        }

        const lilyToLilyLtcMatch = msg.match(/^LILYSWAP:\s*LILY_TO_LILYLTC\s+([\d.]+)\s+LILY\s*->\s*([\d.]+)\s+LTC/);
        if (lilyToLilyLtcMatch) {
            return { fromAmt: parseFloat(lilyToLilyLtcMatch[1]), fromSym: 'LILY', toAmt: parseFloat(lilyToLilyLtcMatch[2]), toSym: 'LTC', type: 'LILY_TO_LILYLTC' };
        }

        const ethToLtcMatch = msg.match(/^LILYSWAP:\s*LILYETH_TO_LILYLTC\s+([\d.]+)\s+ETH\s*->\s*([\d.]+)\s+LTC/);
        if (ethToLtcMatch) {
            return { fromAmt: parseFloat(ethToLtcMatch[1]), fromSym: 'ETH', toAmt: parseFloat(ethToLtcMatch[2]), toSym: 'LTC', type: 'LILYETH_TO_LILYLTC' };
        }

        const ltcToEthMatch = msg.match(/^LILYSWAP:\s*LILYLTC_TO_LILYETH\s+([\d.]+)\s+LTC\s*->\s*([\d.]+)\s+ETH/);
        if (ltcToEthMatch) {
            return { fromAmt: parseFloat(ltcToEthMatch[1]), fromSym: 'LTC', toAmt: parseFloat(ltcToEthMatch[2]), toSym: 'ETH', type: 'LILYLTC_TO_LILYETH' };
        }

        const btcToLilyMatch = msg.match(/^LILYSWAP:\s*LILYBTC_TO_LILY\s+([\d.]+)\s+BTC\s*->\s*([\d.]+)\s+LILY/);
        if (btcToLilyMatch) {
            return { fromAmt: parseFloat(btcToLilyMatch[1]), fromSym: 'BTC', toAmt: parseFloat(btcToLilyMatch[2]), toSym: 'LILY', type: 'LILYBTC_TO_LILY' };
        }

        const lilyToBtcMatch = msg.match(/^LILYSWAP:\s*LILY_TO_LILYBTC\s+([\d.]+)\s+LILY\s*->\s*([\d.]+)\s+BTC/);
        if (lilyToBtcMatch) {
            return { fromAmt: parseFloat(lilyToBtcMatch[1]), fromSym: 'LILY', toAmt: parseFloat(lilyToBtcMatch[2]), toSym: 'BTC', type: 'LILY_TO_LILYBTC' };
        }

        const ethToBtcMatch = msg.match(/^LILYSWAP:\s*LILYETH_TO_LILYBTC\s+([\d.]+)\s+ETH\s*->\s*([\d.]+)\s+BTC/);
        if (ethToBtcMatch) {
            return { fromAmt: parseFloat(ethToBtcMatch[1]), fromSym: 'ETH', toAmt: parseFloat(ethToBtcMatch[2]), toSym: 'BTC', type: 'LILYETH_TO_LILYBTC' };
        }

        const btcToEthMatch = msg.match(/^LILYSWAP:\s*LILYBTC_TO_LILYETH\s+([\d.]+)\s+BTC\s*->\s*([\d.]+)\s+ETH/);
        if (btcToEthMatch) {
            return { fromAmt: parseFloat(btcToEthMatch[1]), fromSym: 'BTC', toAmt: parseFloat(btcToEthMatch[2]), toSym: 'ETH', type: 'LILYBTC_TO_LILYETH' };
        }

        const ltcToBtcMatch = msg.match(/^LILYSWAP:\s*LILYLTC_TO_LILYBTC\s+([\d.]+)\s+LTC\s*->\s*([\d.]+)\s+BTC/);
        if (ltcToBtcMatch) {
            return { fromAmt: parseFloat(ltcToBtcMatch[1]), fromSym: 'LTC', toAmt: parseFloat(ltcToBtcMatch[2]), toSym: 'BTC', type: 'LILYLTC_TO_LILYBTC' };
        }

        const btcToLtcMatch = msg.match(/^LILYSWAP:\s*LILYBTC_TO_LILYLTC\s+([\d.]+)\s+BTC\s*->\s*([\d.]+)\s+LTC/);
        if (btcToLtcMatch) {
            return { fromAmt: parseFloat(btcToLtcMatch[1]), fromSym: 'BTC', toAmt: parseFloat(btcToLtcMatch[2]), toSym: 'LTC', type: 'LILYBTC_TO_LILYLTC' };
        }

        const m = msg.match(/^LILYSWAP:\s*([\d.]+)\s+(\w+)\s*->\s*([\d.]+)\s+(\w+)/);
        if (!m) return null;
        return { fromAmt: parseFloat(m[1]), fromSym: m[2].toUpperCase(), toAmt: parseFloat(m[3]), toSym: m[4].toUpperCase(), type: 'SWAP' };
    } catch { return null; }
}

// ============================================
// LOAD FULFILLED SWAPS
// ============================================

async function loadFulfilled() {
    const data = await fetchWithRetry(`${BLOCKSCOUT_API}?module=account&action=txlist&address=${POOL_ADDRESS}&sort=desc&limit=200`);
    if (!data?.result) return;

    const sent = new Map(); // ETH sent
    const usdcSent = new Map(); // USDC sent
    const outgoingTxs = new Map();

    // Track outgoing ETH transactions
    for (const tx of data.result) {
        if (tx.from.toLowerCase() === POOL_ADDRESS && tx.txreceipt_status === '1') {
            const to = tx.to.toLowerCase();
            const amt = parseInt(tx.value) / 1e18;
            outgoingTxs.set(tx.hash, { to, amt });
            if (!sent.has(to)) sent.set(to, []);
            sent.get(to).push({ amt: amt.toFixed(6), txHash: tx.hash, type: 'ETH' });
        }
    }

    // Track outgoing USDC token transfers
    try {
        const usdcData = await fetchWithRetry(`${BLOCKSCOUT_API}?module=account&action=tokentx&address=${POOL_ADDRESS}&contractaddress=${USDC_ADDRESS}&sort=desc&limit=100`);
        if (usdcData?.result) {
            for (const tx of usdcData.result) {
                if (tx.from.toLowerCase() === POOL_ADDRESS && tx.to.toLowerCase() !== POOL_ADDRESS) {
                    const to = tx.to.toLowerCase();
                    const amt = parseFloat(tx.value) / 1e6; // USDC has 6 decimals
                    if (!usdcSent.has(to)) usdcSent.set(to, []);
                    usdcSent.get(to).push({ amt: amt.toFixed(2), txHash: tx.hash, type: 'USDC' });
                }
            }
        }
    } catch (e) {
        log('Failed to fetch USDC outgoing transactions', 'warn');
    }

    for (const tx of data.result) {
        if (tx.from.toLowerCase() === POOL_ADDRESS) continue;
        const swap = decodeSwap(tx.input);
        if (!swap) continue;

        const isLilychainSwap = swap.type === 'LILYETH_TO_LILY' || swap.type === 'LILY_TO_LILYETH';
        const isLilychainLtcSwap = swap.type === 'LILYLTC_TO_LILY' || swap.type === 'LILY_TO_LILYLTC';
        if (isLilychainSwap && tx.txreceipt_status === '1') {
            state.processed.add(tx.hash);
            state.swaps.push({
                hash: tx.hash, user: tx.from,
                lily: swap.type === 'LILYETH_TO_LILY' ? swap.toAmt : swap.fromAmt,
                eth: swap.type === 'LILYETH_TO_LILY' ? swap.fromAmt : swap.toAmt,
                type: swap.type, time: new Date(parseInt(tx.timeStamp) * 1000).toISOString(), status: 'confirmed'
            });
            continue;
        }
        if (isLilychainLtcSwap && tx.txreceipt_status === '1') {
            state.processed.add(tx.hash);
            state.swaps.push({
                hash: tx.hash, user: tx.from,
                lily: swap.type === 'LILYLTC_TO_LILY' ? swap.toAmt : swap.fromAmt,
                ltc: swap.type === 'LILYLTC_TO_LILY' ? swap.fromAmt : swap.toAmt,
                type: swap.type, time: new Date(parseInt(tx.timeStamp) * 1000).toISOString(), status: 'confirmed'
            });
            continue;
        }
        const isEthLtcSwap = swap.type === 'LILYETH_TO_LILYLTC' || swap.type === 'LILYLTC_TO_LILYETH';
        if (isEthLtcSwap && tx.txreceipt_status === '1') {
            state.processed.add(tx.hash);
            state.swaps.push({
                hash: tx.hash, user: tx.from,
                eth: swap.type === 'LILYETH_TO_LILYLTC' ? swap.fromAmt : swap.toAmt,
                ltc: swap.type === 'LILYETH_TO_LILYLTC' ? swap.toAmt : swap.fromAmt,
                type: swap.type, time: new Date(parseInt(tx.timeStamp) * 1000).toISOString(), status: 'confirmed'
            });
            continue;
        }

        // Track ETH -> LILY buys (user sends ETH, gets LILY)
        const isEthToLily = swap.fromSym === 'ETH' && swap.toSym === 'LILY' && swap.type === 'SWAP';
        if (isEthToLily && tx.txreceipt_status === '1') {
            state.processed.add(tx.hash);
            state.swaps.push({
                hash: tx.hash, user: tx.from,
                lily: swap.toAmt, eth: swap.fromAmt,
                type: 'ETH_TO_LILY', time: new Date(parseInt(tx.timeStamp) * 1000).toISOString(), status: 'confirmed'
            });
            continue;
        }

        const isLilyToEth = swap.fromSym === 'LILY' && swap.toSym === 'ETH' && swap.type === 'SWAP';
        const isLilyToUsdc = swap.fromSym === 'LILY' && swap.toSym === 'USDC' && swap.type === 'SWAP';
        const isEthWithdraw = swap.type === 'ETH_WITHDRAW';

        // Skip if not a LILY → ETH/USDC swap or ETH withdrawal
        if (!isLilyToEth && !isEthWithdraw && !isLilyToUsdc) continue;

        const user = tx.from.toLowerCase();
        const gasFee = parseInt(tx.value) / 1e18;

        // Check for LILY → USDC fulfilled swaps
        if (isLilyToUsdc && usdcSent.has(user)) {
            const amt = swap.toAmt.toFixed(2);
            const sentList = usdcSent.get(user);
            const idx = sentList.findIndex(s => Math.abs(parseFloat(s.amt) - parseFloat(amt)) < 0.01);
            if (idx >= 0) {
                state.processed.add(tx.hash);
                state.swaps.push({
                    hash: tx.hash, user: tx.from, lily: swap.fromAmt,
                    usdc: swap.toAmt, type: 'LILY_TO_USDC', gasFee, returnTx: sentList[idx].txHash,
                    status: 'confirmed', time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                });
                sentList.splice(idx, 1);
                continue;
            }
        }

        // Check for LILY → ETH or ETH withdrawal fulfilled swaps
        if ((isLilyToEth || isEthWithdraw) && sent.has(user)) {
            const amt = swap.toAmt.toFixed(6);
            const sentList = sent.get(user);
            const idx = sentList.findIndex(s => Math.abs(parseFloat(s.amt) - parseFloat(amt)) < 0.00001);
            if (idx >= 0) {
                state.processed.add(tx.hash);
                state.swaps.push({
                    hash: tx.hash, user: tx.from, lily: isEthWithdraw ? 0 : swap.fromAmt,
                    eth: swap.toAmt, type: swap.type || 'SWAP', gasFee, returnTx: sentList[idx].txHash,
                    status: 'confirmed', time: new Date(parseInt(tx.timeStamp) * 1000).toISOString()
                });
                sentList.splice(idx, 1);
            }
        }
    }

    state.swaps.sort((a, b) => new Date(b.time) - new Date(a.time));
    state.swaps = state.swaps.slice(0, 50);
    await calculateLilyHolders();
    log(`Loaded ${state.processed.size} completed swaps`, 'success');
    log(`Tracking ${state.lilyHolders.size} LILY holders`, 'info');
}

// ============================================
// PROCESS SWAP
// ============================================

async function processSwap(tx, swap) {
    const hash = tx.hash;
    const user = tx.from;
    const ethAmt = swap.toAmt;
    const gasFee = parseFloat(tx.value) / 1e18;
    const isEthWithdraw = swap.type === 'ETH_WITHDRAW';

    const record = {
        hash, user, lily: isEthWithdraw ? 0 : swap.fromAmt, eth: ethAmt,
        gasFee, type: swap.type || 'SWAP', time: new Date().toISOString(), status: 'processing'
    };

    try {
        const valueWei = ethers.parseEther(ethAmt.toString());
        const [balanceWei, feeData, gasEstimate, networkNonce] = await Promise.all([
            provider.getBalance(wallet.address),
            getCachedFeeData(),
            provider.estimateGas({ from: wallet.address, to: user, value: valueWei }).catch(() => null),
            state.nextNonce !== null ? Promise.resolve(state.nextNonce) : provider.getTransactionCount(wallet.address, 'pending')
        ]);

        const balance = parseFloat(ethers.formatEther(balanceWei));
        state.balance = balance.toFixed(6);
        const { maxFeePerGas, maxPriorityFeePerGas } = feeData;
        let gasLimit = gasEstimate ? gasEstimate * 120n / 100n : BigInt(GAS_LIMIT);
        const gasCostWei = maxFeePerGas * gasLimit;
        const gasCost = parseFloat(ethers.formatEther(gasCostWei));
        const totalNeeded = ethAmt + gasCost;

        if (balance < totalNeeded) {
            if (!state.queued.has(hash)) {
                log(`Queued: ${hash.slice(0,10)}... (need ${totalNeeded.toFixed(4)} ETH)`, 'warn');
                record.status = 'queued';
                state.swaps.unshift(record);
                state.swaps = state.swaps.slice(0, 50);
            }
            state.queued.set(hash, totalNeeded);
            return false;
        }

        log(`Sending ${ethAmt} ETH to ${user.slice(0,10)}...`);

        const txSend = await wallet.sendTransaction({
            to: user, value: valueWei, gasLimit, maxFeePerGas, maxPriorityFeePerGas, nonce: networkNonce, type: 2
        });

        state.nextNonce = networkNonce + 1;
        log(`TX sent: ${txSend.hash.slice(0,10)}...`, 'success');
        record.returnTx = txSend.hash;
        record.status = 'sent';
        state.processed.add(hash);
        state.queued.delete(hash);
        state.swaps = state.swaps.filter(s => s.hash !== hash);
        state.swaps.unshift(record);
        state.swaps = state.swaps.slice(0, 50);
        trackConfirmation(txSend, record);
        return true;

    } catch (e) {
        const msg = e.message?.slice(0, 50) || 'Failed';
        log(`Error: ${msg}`, 'error');
        record.status = 'error';
        state.swaps.unshift(record);
        state.swaps = state.swaps.slice(0, 50);
        state.nextNonce = null;
        if (msg.includes('network') || msg.includes('timeout') || msg.includes('connect') || msg.includes('nonce')) {
            rotateRpc();
        }
        return false;
    }
}

// Helper to add a swap record
function addSwap(record) {
    state.swaps.unshift(record);
    state.swaps = state.swaps.slice(0, 50);
    state.balancesDirty = true;
    saveState();
}

// ============================================
// PROCESS LILYCHAIN BRIDGE (LILY <-> Lilychain ETH)
// ============================================
async function processLilychainBridge(tx, swap) {
    // LILY_TO_LILYETH: User sells LILY for Lilychain ETH
    // This is an internal Lilychain swap - no real ETH needs to be sent
    const record = {
        hash: tx.hash,
        user: tx.from,
        lily: swap.fromAmt,
        eth: swap.toAmt,
        type: swap.type,
        time: new Date(parseInt(tx.timeStamp) * 1000).toISOString(),
        status: 'confirmed'
    };
    addSwap(record);
    log(`Lilychain bridge: ${swap.fromAmt} LILY -> ${swap.toAmt} ETH for ${tx.from.slice(0,10)}...`, 'success');
}

// ============================================
// PROCESS USDC SWAP
// ============================================

async function processUsdcSwap(tx, swap) {
    const hash = tx.hash;
    const user = tx.from;
    const usdcAmtRequested = swap.toAmt;
    // Apply 6% withdrawal fee - user gets 94%, truncate to 6 decimals (USDC precision)
    const usdcAmt = Math.floor(usdcAmtRequested * 0.94 * 1e6) / 1e6;
    const gasFee = parseFloat(tx.value) / 1e18;

    const record = {
        hash, user, lily: swap.fromAmt, usdc: usdcAmt, usdcRequested: usdcAmtRequested,
        gasFee, type: 'LILY_TO_USDC', time: new Date().toISOString(), status: 'processing'
    };

    try {
        // Check USDC balance
        const usdcContract = new ethers.Contract(
            USDC_ADDRESS,
            ['function balanceOf(address) view returns (uint256)', 'function transfer(address,uint256) returns (bool)'],
            wallet
        );

        const usdcBalance = await usdcContract.balanceOf(POOL_ADDRESS);
        const usdcBalanceFormatted = parseFloat(ethers.formatUnits(usdcBalance, 6));

        log(`USDC Balance: ${usdcBalanceFormatted.toFixed(2)}, Need: ${usdcAmt.toFixed(2)}`);

        if (usdcBalanceFormatted < usdcAmt) {
            log(`Insufficient USDC: need ${usdcAmt}, have ${usdcBalanceFormatted}`, 'warn');
            record.status = 'queued';
            state.swaps.unshift(record);
            state.swaps = state.swaps.slice(0, 50);
            state.queued.set(hash, usdcAmt);
            return false;
        }

        log(`Sending ${usdcAmt.toFixed(2)} USDC (94% of ${usdcAmtRequested.toFixed(2)}) to ${user.slice(0,10)}...`);

        // Send USDC (6 decimals)
        const usdcAmountWei = ethers.parseUnits(usdcAmt.toString(), 6);
        const feeData = await getCachedFeeData();
        const networkNonce = state.nextNonce !== null ? state.nextNonce : await provider.getTransactionCount(wallet.address, 'pending');

        const txSend = await usdcContract.transfer(user, usdcAmountWei, {
            gasLimit: 100000,
            maxFeePerGas: feeData.maxFeePerGas,
            maxPriorityFeePerGas: feeData.maxPriorityFeePerGas,
            nonce: networkNonce,
            type: 2
        });

        state.nextNonce = networkNonce + 1;
        log(`USDC TX sent: ${txSend.hash.slice(0,10)}...`, 'success');
        record.returnTx = txSend.hash;
        record.status = 'sent';
        state.processed.add(hash);
        state.queued.delete(hash);
        state.swaps = state.swaps.filter(s => s.hash !== hash);
        state.swaps.unshift(record);
        state.swaps = state.swaps.slice(0, 50);

        // Update USDC balance
        await updateUsdcBalance(true);

        return true;

    } catch (e) {
        const msg = e.message?.slice(0, 50) || 'Failed';
        log(`USDC Swap Error: ${msg}`, 'error');
        record.status = 'error';
        state.swaps.unshift(record);
        state.swaps = state.swaps.slice(0, 50);
        state.nextNonce = null;
        if (msg.includes('network') || msg.includes('timeout') || msg.includes('connect') || msg.includes('nonce')) {
            rotateRpc();
        }
        return false;
    }
}

// ============================================
// MAIN POLL - Optimized for speed
// ============================================

async function poll() {
    const pollStart = Date.now();
    state.pollCount++;
    state.status = 'Checking...';

    // Parallel fetch: balance + USDC balance + transactions + USDC transactions + price
    const [, , txList] = await Promise.all([
        updateBalance(),
        updateUsdcBalance(),
        fetchTransactions(),
        fetchUsdcTransactions(),
        updateEthPrice()
    ]);

    if (!txList || txList.length === 0) {
        state.status = 'Watching...';
        state.lastPollTime = Date.now() - pollStart;
        return;
    }

    // Process transactions - skip already processed using Set (O(1) lookup)
    let newSwaps = 0;
    for (const tx of txList) {
        // Fast skip with Set lookup
        if (state.processed.has(tx.hash)) continue;

        // Skip outgoing transactions
        if (tx.from.toLowerCase() === POOL_ADDRESS) {
            state.processed.add(tx.hash);
            continue;
        }

        const swap = decodeSwap(tx.input);
        if (!swap) {
            state.processed.add(tx.hash);
            continue;
        }

        const isLilyToEth = swap.fromSym === 'LILY' && swap.toSym === 'ETH' && swap.type === 'SWAP';
        const isLilyToUsdc = swap.fromSym === 'LILY' && swap.toSym === 'USDC' && swap.type === 'SWAP';
        const isEthWithdraw = swap.type === 'ETH_WITHDRAW';
        const isLilychainSwap = swap.type === 'LILYETH_TO_LILY' || swap.type === 'LILY_TO_LILYETH';
        const isLilychainLtcSwap = swap.type === 'LILYLTC_TO_LILY' || swap.type === 'LILY_TO_LILYLTC';

        if (isLilychainLtcSwap) {
            const record = {
                hash: tx.hash, user: tx.from,
                lily: swap.type === 'LILYLTC_TO_LILY' ? swap.toAmt : swap.fromAmt,
                ltc: swap.type === 'LILYLTC_TO_LILY' ? swap.fromAmt : swap.toAmt,
                type: swap.type, time: new Date(parseInt(tx.timeStamp) * 1000).toISOString(), status: 'confirmed'
            };
            addSwap(record);
            state.processed.add(tx.hash);
            log(`Lilychain LTC: ${swap.type} from ${tx.from.slice(0,10)}...`, 'success');
            newSwaps++;
            continue;
        }

        const isEthLtcSwap = swap.type === 'LILYETH_TO_LILYLTC' || swap.type === 'LILYLTC_TO_LILYETH';
        if (isEthLtcSwap) {
            const record = {
                hash: tx.hash, user: tx.from,
                eth: swap.type === 'LILYETH_TO_LILYLTC' ? swap.fromAmt : swap.toAmt,
                ltc: swap.type === 'LILYETH_TO_LILYLTC' ? swap.toAmt : swap.fromAmt,
                type: swap.type, time: new Date(parseInt(tx.timeStamp) * 1000).toISOString(), status: 'confirmed'
            };
            addSwap(record);
            state.processed.add(tx.hash);
            log(`Lilychain ETH<>LTC: ${swap.type} from ${tx.from.slice(0,10)}...`, 'success');
            newSwaps++;
            continue;
        }

        if (isLilychainSwap) {
            // LILY_TO_LILYETH: User is bridging LILY from Lilychain to Ethereum, send ETH for gas
            if (swap.type === 'LILY_TO_LILYETH') {
                await processLilychainBridge(tx, swap);
                state.processed.add(tx.hash);
                continue;
            }

            // LILYETH_TO_LILY: User is bridging ETH from Ethereum to Lilychain, just record
            const record = {
                hash: tx.hash, user: tx.from,
                lily: swap.type === 'LILYETH_TO_LILY' ? swap.toAmt : swap.fromAmt,
                eth: swap.type === 'LILYETH_TO_LILY' ? swap.fromAmt : swap.toAmt,
                type: swap.type, time: new Date(parseInt(tx.timeStamp) * 1000).toISOString(), status: 'confirmed'
            };
            addSwap(record);
            state.processed.add(tx.hash);
            log(`Lilychain: ${swap.type} from ${tx.from.slice(0,10)}...`, 'success');
            newSwaps++;
            continue;
        }

        // Handle LILY → USDC swaps
        if (isLilyToUsdc) {
            state.processed.add(tx.hash);
            await processUsdcSwap(tx, swap);
            continue;
        }

        if (!swap || (!isLilyToEth && !isEthWithdraw)) { state.processed.add(tx.hash); continue; }

        if (state.queued.has(tx.hash)) {
            const needed = state.queued.get(tx.hash);
            if (parseFloat(state.balance) < needed) continue;
            state.queued.delete(tx.hash);
            log(`Retrying ${tx.hash.slice(0,10)}...`);
        }

        state.processed.add(tx.hash);
        await processSwap(tx, swap);
    }

    const pending = state.queued.size;
    state.status = pending > 0 ? `${pending} pending` : 'Watching...';

    // Always recalculate holders and transactions to catch external trades
    await Promise.all([
        calculateLilyHolders(),
        getLilyTransactions()
    ]);

    // Update user balances after fetching transactions
    updateAllUserBalances();
}

// ============================================
// WEB UI
// ============================================

function startWeb() {
    const app = express();

    // Serve logos from the main lilyswap directory
    app.use('/logos', express.static('/Users/maddoxlukegrayson/Desktop/lilyswap/logos'));

    app.use((req, res, next) => {
        res.header('Access-Control-Allow-Origin', '*');
        res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
        res.header('Access-Control-Allow-Headers', 'Content-Type');
        if (req.method === 'OPTIONS') return res.sendStatus(200);
        next();
    });

    app.get('/', async (req, res) => {
        const pending = state.swaps.filter(s => s.status === 'queued').length;
        const completed = state.swaps.filter(s => s.status === 'confirmed' || s.status === 'sent').length;
        const lowBalance = parseFloat(state.balance) < 0.002;
        const holders = getLilyHoldersList();
        const totalSupply = holders.reduce((sum, h) => sum + h.balance, 0);
        const totalEth = holders.reduce((sum, h) => sum + (h.lilyEthBalance || 0), 0);
        const totalLtc = holders.reduce((sum, h) => sum + (h.lilyLtcBalance || 0), 0);
        const totalBtc = holders.reduce((sum, h) => sum + (h.lilyBtcBalance || 0), 0);
        const totalUsdt = holders.reduce((sum, h) => sum + (h.lilyUsdtBalance || 0), 0);
        const usdValue = (parseFloat(state.balance) * state.ethPrice).toFixed(2);

        // Get all LILY transactions (buys and sells)
        if (state.lilyBuys.length === 0 && state.lilySells.length === 0) {
            await getLilyTransactions();
        }
        const lilyBuys = state.lilyBuys;
        const lilySells = state.lilySells;

        // Use cached sorted transactions for performance
        const allTransactions = getCachedSortedTransactions();
        const totalLilyBought = lilyBuys.reduce((sum, b) => sum + b.lilyAmount, 0);
        const totalLilySold = lilySells.reduce((sum, s) => sum + s.lilyAmount, 0);

        // Use cached balances (poll() keeps these updated, debounce prevents excessive recalculation)
        updateAllUserBalances();

        res.send(`<!DOCTYPE html>
<html><head>
<title>LilyScan</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#f8f9fb;--surface:#fff;--border:#e8eaef;--text:#111318;--text2:#7c8291;--pink:#e64980;--green:#0ea472;--yellow:#e8a308;--red:#e5484d;--mono:'JetBrains Mono',monospace;--sans:'Inter',system-ui,-apple-system,sans-serif;--r:8px}
body{font-family:var(--sans);background:var(--bg);color:var(--text);font-size:13px;line-height:1.5;-webkit-font-smoothing:antialiased;padding-bottom:48px}

/* Footer prices */
.price-bar{display:flex;align-items:center;gap:6px;padding:8px 20px;background:var(--surface);border-top:1px solid var(--border);position:fixed;bottom:0;left:0;right:0;z-index:100}
.price-item{display:flex;align-items:center;gap:6px;padding:6px 12px;background:var(--bg);border:1px solid var(--border);border-radius:var(--r);font-size:12px}
.price-item:hover{border-color:#cdd0d8}
.price-dot{width:6px;height:6px;border-radius:50%}
.price-dot.lily{background:#e64980}.price-dot.eth{background:#627eea}.price-dot.ltc{background:#a0a0a0}.price-dot.btc{background:#f7931a}.price-dot.usdt{background:#26a17b}
.price-sym{font-size:11px;font-weight:600;color:var(--text2)}
.price-val{color:var(--text);font-weight:600;font-size:12px;font-family:var(--mono);transition:color 0.3s}
.price-val.flash-up{color:var(--green)}.price-val.flash-down{color:var(--red)}

/* Header */
.header{display:flex;align-items:center;justify-content:space-between;padding:12px 20px;background:var(--surface);border-bottom:1px solid var(--border)}
.header-left{display:flex;align-items:center;gap:14px}
.logo{font-size:16px;font-weight:700;color:var(--pink);letter-spacing:-0.5px}
.dot{width:6px;height:6px;border-radius:50%;background:var(--green);box-shadow:0 0 6px rgba(14,164,114,0.4)}
.dot.warn{background:var(--yellow);box-shadow:0 0 6px rgba(232,163,8,0.4)}
.header-stats{display:flex;gap:8px}
.stat{display:flex;align-items:center;gap:5px;padding:5px 10px;background:var(--bg);border-radius:var(--r);border:1px solid var(--border)}
.stat-val{font-family:var(--mono);font-weight:600;font-size:12px}
.stat-val.pink{color:var(--pink)}.stat-val.green{color:var(--green)}
.stat-lbl{color:var(--text2);font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px}
.stat-usd{color:var(--text2);font-family:var(--mono);font-size:9px;font-weight:500;opacity:0.5}
.header-right{display:flex;align-items:center;gap:10px}
.addr{font-family:var(--mono);font-size:11px;color:var(--text2);padding:5px 10px;background:var(--bg);border-radius:var(--r);border:1px solid var(--border)}
.addr a{color:var(--pink);text-decoration:none;margin-left:6px;font-weight:600}
.addr a:hover{text-decoration:underline}

/* Layout */
.main{display:grid;grid-template-columns:1fr 1fr;min-height:calc(100vh - 100px)}
.panel{padding:16px 20px;border-right:1px solid var(--border);display:flex;flex-direction:column;overflow:hidden;background:var(--surface)}
.panel:last-child{border-right:none}
.panel-title{font-size:10px;color:var(--text2);text-transform:uppercase;letter-spacing:1.2px;font-weight:700;margin-bottom:12px;display:flex;justify-content:space-between;align-items:center}
.list{display:flex;flex-direction:column;gap:3px;overflow-y:auto;flex:1;max-height:calc(100vh - 160px)}
.list::-webkit-scrollbar{width:3px}
.list::-webkit-scrollbar-thumb{background:#d1d5db;border-radius:3px}

/* Transaction rows */
.item{display:flex;flex-direction:column;gap:4px;padding:8px 12px;background:var(--bg);border-radius:var(--r);font-family:var(--sans);transition:background 0.15s;border:1px solid transparent}
.item:hover{background:#eceef2;border-color:var(--border)}
.tag{font-family:var(--sans);font-size:9px;padding:2px 8px;border-radius:4px;font-weight:700;text-transform:uppercase;letter-spacing:0.6px;flex-shrink:0}
.tag-green{background:rgba(14,164,114,.08);color:var(--green)}
.tag-red{background:rgba(229,72,77,.08);color:var(--red)}
.item-top{display:flex;align-items:center;width:100%;gap:6px}
.item-swap{display:flex;align-items:center;flex:1;min-width:0}
.swap-side{display:flex;align-items:center;width:110px;flex-shrink:0;font-size:12.5px;font-weight:600;font-family:var(--mono);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.item-swap .arrow{margin:0 2px;color:#ccc;font-size:9px;flex-shrink:0}
.item-right{display:flex;align-items:center;gap:6px;margin-left:auto;flex-shrink:0}
.item-right a{color:var(--pink);text-decoration:none;font-weight:600;font-size:10px}
.item-right a:hover{text-decoration:underline}
.item-sub{display:flex;align-items:center;justify-content:space-between;width:100%;font-size:9.5px;color:var(--text2);font-family:var(--mono);opacity:0.5}
.item-sub .item-addr{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1}
.item-sub .item-time{flex-shrink:0}

/* Coin icons */
.coin-wrap{position:relative;display:inline-flex;align-items:center;justify-content:center;width:20px;height:20px;flex-shrink:0;margin-right:3px}
.coin-wrap img.coin-logo{width:18px;height:18px;border-radius:50%;object-fit:cover}
.coin-wrap.usdc-bg img.coin-logo{background:#000;border-radius:50%}
.coin-wrap .chain-badge{position:absolute;bottom:-2px;right:-3px;width:9px;height:9px;border-radius:2px;display:flex;align-items:center;justify-content:center;border:1.5px solid var(--surface)}
.coin-wrap .chain-badge img{width:6px;height:6px;border-radius:0}

/* Holder rows */
.holder-group{display:flex;flex-direction:column;gap:0;margin-bottom:4px}
.holder-row{display:flex;align-items:center;justify-content:space-between;padding:8px 12px;background:var(--bg);font-family:var(--sans);font-size:12px;transition:background 0.15s;border:1px solid transparent}
.holder-row:hover{background:#eceef2;border-color:var(--border)}
.holder-group .holder-row:first-child{border-radius:var(--r) var(--r) 0 0}
.holder-group .holder-row:last-child{border-radius:0 0 var(--r) var(--r)}
.holder-group .holder-row:only-child{border-radius:var(--r)}
.holder-left{display:flex;align-items:center;gap:8px}
.holder-addr{font-size:10px;color:var(--text2);font-family:var(--mono);opacity:0.7}
.holder-ticker{font-family:var(--sans);font-size:9px;padding:2px 7px;border-radius:4px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px}
.holder-ticker.lily{background:rgba(230,73,128,.08);color:var(--pink)}
.holder-ticker.eth{background:rgba(98,126,234,.08);color:#627eea}
.holder-ticker.ltc{background:rgba(160,160,160,.08);color:#888}
.holder-ticker.btc{background:rgba(247,147,26,.08);color:#f7931a}
.holder-ticker.usdt{background:rgba(38,161,123,.08);color:#26a17b}
.holder-right{display:flex;align-items:center;gap:12px}
.holder-amt{font-weight:600;color:var(--text);font-size:12px;font-family:var(--mono)}
.holder-usd{font-size:10px;color:var(--text2);font-family:var(--mono);font-weight:500;min-width:60px;text-align:right;opacity:0.6}
.ht-total-inline{color:var(--pink);font-weight:700;font-size:14px;font-family:var(--mono);letter-spacing:-0.3px}

.empty{padding:40px;text-align:center;color:var(--text2);font-size:12px}
@keyframes slideIn{from{opacity:0;transform:translateY(-6px)}to{opacity:1;transform:translateY(0)}}
.item.new,.holder.new{animation:slideIn 0.25s ease-out}
.updating{opacity:0.6;transition:opacity 0.15s}
@media(max-width:900px){.main{grid-template-columns:1fr}.panel{border-right:none;border-bottom:1px solid var(--border)}.header{flex-wrap:wrap;gap:12px}}
</style>
</head><body>
<div class="header">
<div class="header-left">
<span class="logo">LilyScan</span>
<div class="dot ${lowBalance ? 'warn' : ''}"></div>
<div class="header-stats">
<div class="stat"><span class="stat-val pink" id="ethBalance">${state.balance}</span><span class="stat-lbl">ETH</span><span class="stat-usd" id="ethUsdVal">$${(parseFloat(state.balance) * state.ethPrice).toFixed(2)}</span></div>
<div class="stat"><span class="stat-val green" id="usdcBalance">${state.usdcBalance}</span><span class="stat-lbl">USDC</span><span class="stat-usd" id="usdcUsdVal">$${parseFloat(state.usdcBalance || 0).toFixed(2)}</span></div>
<div class="stat"><span class="stat-val" id="holderCount">${holders.length}</span><span class="stat-lbl">Holders</span></div>
</div>
</div>
<div class="header-right">
<span class="addr">${POOL_ADDRESS.slice(0,6)}...${POOL_ADDRESS.slice(-4)}<a href="https://etherscan.io/address/${POOL_ADDRESS}" target="_blank">View</a></span>
</div>
</div>
<div class="main">
<div class="panel">
<div class="panel-title"><span>Transactions</span></div>
<div class="list" id="transactionList">
${allTransactions.length === 0 ? '<div class="empty">No transactions yet</div>' : allTransactions.slice(0,50).map(t => {
    const isUsdcTx = t.type === 'USDC' || t.type === 'LILY_TO_USDC';
    const isBuy = t.action === 'buy';
    const isLilychain = t.type === 'LILYETH_TO_LILY' || t.type === 'LILY_TO_LILYETH';
    const isLilychainLtc = t.type === 'LILYLTC_TO_LILY' || t.type === 'LILY_TO_LILYLTC';
    const ethLogo = 'https://cryptologos.cc/logos/ethereum-eth-logo.png';
    const ltcLogo = 'https://cryptologos.cc/logos/litecoin-ltc-logo.png';
    const lilyLogo = 'http://127.0.0.1:8000/flower-logo.png';
    const usdcLogo = '/logos/usdc.png';
    const lcBadge = `<div class="chain-badge" style="background:#fff"><img src="${lilyLogo}" alt="LC"></div>`;
    const ethBadge = `<div class="chain-badge" style="background:#627eea"><img src="${ethLogo}" style="filter:brightness(0) invert(1)" alt="ETH"></div>`;
    function cw(logo, alt, badge, cls) { return `<span class="coin-wrap${cls?' '+cls:''}"><img src="${logo}" class="coin-logo" alt="${alt}" onerror="this.style.display='none'">${badge||''}</span>`; }

    let from, to, fromAddr, tag, tagClass;

    if (isUsdcTx) {
        // USDC transaction (both type='USDC' and type='LILY_TO_USDC')
        fromAddr = t.user || t.from;
        const lilyAmount = (t.lily || t.lilyAmount || (t.value * 0.94) || 0).toFixed ? (t.lily || t.lilyAmount || (t.value * 0.94) || 0).toFixed(2) : parseFloat(t.lily || t.lilyAmount || t.value * 0.94 || 0).toFixed(2);
        const usdcAmount = (t.usdc || t.usdcAmount || t.value || 0).toFixed ? (t.usdc || t.usdcAmount || t.value || 0).toFixed(2) : parseFloat(t.usdc || t.usdcAmount || t.value || 0).toFixed(2);

        if (t.isIncoming || t.action === 'buy') {
            from = `${cw(usdcLogo,'USDC',null,'usdc-bg')}${usdcAmount}`;
            to = `${cw(lilyLogo,'LILY')}${lilyAmount}`;
            tag = 'BUY';
            tagClass = 'tag-green';
        } else {
            from = `${cw(lilyLogo,'LILY')}${lilyAmount}`;
            to = `${cw(usdcLogo,'USDC',null,'usdc-bg')}${usdcAmount}`;
            tag = 'SELL';
            tagClass = 'tag-red';
        }
    } else if (isLilychainLtc) {
        // LTC Lilychain transaction
        fromAddr = t.user;
        const ltcAmt = (t.ltc || t.ltcAmount || 0);
        const lilyAmt = (t.lily || t.lilyAmount || 0);
        from = isBuy
            ? `${cw(ltcLogo,'LTC',lcBadge)}${ltcAmt.toFixed(4)}`
            : `${cw(lilyLogo,'LILY')}${lilyAmt.toFixed(2)}`;
        to = isBuy
            ? `${cw(lilyLogo,'LILY')}${lilyAmt.toFixed(2)}`
            : `${cw(ltcLogo,'LTC',lcBadge)}${ltcAmt.toFixed(4)}`;
        tag = isBuy ? 'Buy' : 'Sell';
        tagClass = isBuy ? 'tag-green' : 'tag-red';
    } else if (t.type === 'LILYETH_TO_LILYLTC' || t.type === 'LILYLTC_TO_LILYETH') {
        // ETH <-> LTC on Lilychain
        fromAddr = t.user;
        const ethAmt = (t.eth || t.ethAmount || 0);
        const ltcAmt = (t.ltc || t.ltcAmount || 0);
        if (t.type === 'LILYETH_TO_LILYLTC') {
            from = `${cw(ethLogo,'ETH',lcBadge)}${ethAmt.toFixed(4)}`;
            to = `${cw(ltcLogo,'LTC',lcBadge)}${ltcAmt.toFixed(4)}`;
        } else {
            from = `${cw(ltcLogo,'LTC',lcBadge)}${ltcAmt.toFixed(4)}`;
            to = `${cw(ethLogo,'ETH',lcBadge)}${ethAmt.toFixed(4)}`;
        }
        tag = 'Swap';
        tagClass = 'tag-green';
    } else if (t.type === 'LILYBTC_TO_LILY' || t.type === 'LILY_TO_LILYBTC') {
        // BTC Lilychain <-> LILY transaction
        fromAddr = t.user;
        const btcLogo = 'https://cryptologos.cc/logos/bitcoin-btc-logo.png';
        const btcAmt = (t.btc || t.btcAmount || 0);
        const lilyAmt = (t.lily || t.lilyAmount || 0);
        from = isBuy
            ? `${cw(btcLogo,'BTC',lcBadge)}${btcAmt.toFixed(4)}`
            : `${cw(lilyLogo,'LILY')}${lilyAmt.toFixed(2)}`;
        to = isBuy
            ? `${cw(lilyLogo,'LILY')}${lilyAmt.toFixed(2)}`
            : `${cw(btcLogo,'BTC',lcBadge)}${btcAmt.toFixed(4)}`;
        tag = isBuy ? 'Buy' : 'Sell';
        tagClass = isBuy ? 'tag-green' : 'tag-red';
    } else if (t.type === 'LILYETH_TO_LILYBTC' || t.type === 'LILYBTC_TO_LILYETH') {
        // ETH <-> BTC on Lilychain
        fromAddr = t.user;
        const btcLogo = 'https://cryptologos.cc/logos/bitcoin-btc-logo.png';
        const ethAmt = (t.eth || t.ethAmount || 0);
        const btcAmt = (t.btc || t.btcAmount || 0);
        if (t.type === 'LILYETH_TO_LILYBTC') {
            from = `${cw(ethLogo,'ETH',lcBadge)}${ethAmt.toFixed(4)}`;
            to = `${cw(btcLogo,'BTC',lcBadge)}${btcAmt.toFixed(4)}`;
        } else {
            from = `${cw(btcLogo,'BTC',lcBadge)}${btcAmt.toFixed(4)}`;
            to = `${cw(ethLogo,'ETH',lcBadge)}${ethAmt.toFixed(4)}`;
        }
        tag = 'Swap';
        tagClass = 'tag-green';
    } else if (t.type === 'LILYLTC_TO_LILYBTC' || t.type === 'LILYBTC_TO_LILYLTC') {
        // LTC <-> BTC on Lilychain
        fromAddr = t.user;
        const btcLogo = 'https://cryptologos.cc/logos/bitcoin-btc-logo.png';
        const ltcAmt = (t.ltc || t.ltcAmount || 0);
        const btcAmt = (t.btc || t.btcAmount || 0);
        if (t.type === 'LILYLTC_TO_LILYBTC') {
            from = `${cw(ltcLogo,'LTC',lcBadge)}${ltcAmt.toFixed(4)}`;
            to = `${cw(btcLogo,'BTC',lcBadge)}${btcAmt.toFixed(4)}`;
        } else {
            from = `${cw(btcLogo,'BTC',lcBadge)}${btcAmt.toFixed(4)}`;
            to = `${cw(ltcLogo,'LTC',lcBadge)}${ltcAmt.toFixed(4)}`;
        }
        tag = 'Swap';
        tagClass = 'tag-green';
    } else if (t.type === 'LILYUSDT_TO_LILY' || t.type === 'LILY_TO_LILYUSDT') {
        // USDT Lilychain <-> LILY transaction
        fromAddr = t.user;
        const usdtLogo2 = 'https://cryptologos.cc/logos/tether-usdt-logo.png';
        const usdtAmt = (t.usdt || t.usdtAmount || 0);
        const lilyAmt = (t.lily || t.lilyAmount || 0);
        from = isBuy
            ? `${cw(usdtLogo2,'USDT',lcBadge)}${usdtAmt.toFixed(4)}`
            : `${cw(lilyLogo,'LILY')}${lilyAmt.toFixed(2)}`;
        to = isBuy
            ? `${cw(lilyLogo,'LILY')}${lilyAmt.toFixed(2)}`
            : `${cw(usdtLogo2,'USDT',lcBadge)}${usdtAmt.toFixed(4)}`;
        tag = isBuy ? 'Buy' : 'Sell';
        tagClass = isBuy ? 'tag-green' : 'tag-red';
    } else if (t.type === 'LILYETH_TO_LILYUSDT' || t.type === 'LILYUSDT_TO_LILYETH') {
        // ETH <-> USDT on Lilychain
        fromAddr = t.user;
        const usdtLogo2 = 'https://cryptologos.cc/logos/tether-usdt-logo.png';
        const ethAmt = (t.eth || t.ethAmount || 0);
        const usdtAmt = (t.usdt || t.usdtAmount || 0);
        if (t.type === 'LILYETH_TO_LILYUSDT') {
            from = `${cw(ethLogo,'ETH',lcBadge)}${ethAmt.toFixed(4)}`;
            to = `${cw(usdtLogo2,'USDT',lcBadge)}${usdtAmt.toFixed(4)}`;
        } else {
            from = `${cw(usdtLogo2,'USDT',lcBadge)}${usdtAmt.toFixed(4)}`;
            to = `${cw(ethLogo,'ETH',lcBadge)}${ethAmt.toFixed(4)}`;
        }
        tag = 'Swap';
        tagClass = 'tag-green';
    } else if (t.type === 'LILYLTC_TO_LILYUSDT' || t.type === 'LILYUSDT_TO_LILYLTC') {
        // LTC <-> USDT on Lilychain
        fromAddr = t.user;
        const usdtLogo2 = 'https://cryptologos.cc/logos/tether-usdt-logo.png';
        const ltcAmt = (t.ltc || t.ltcAmount || 0);
        const usdtAmt = (t.usdt || t.usdtAmount || 0);
        if (t.type === 'LILYLTC_TO_LILYUSDT') {
            from = `${cw(ltcLogo,'LTC',lcBadge)}${ltcAmt.toFixed(4)}`;
            to = `${cw(usdtLogo2,'USDT',lcBadge)}${usdtAmt.toFixed(4)}`;
        } else {
            from = `${cw(usdtLogo2,'USDT',lcBadge)}${usdtAmt.toFixed(4)}`;
            to = `${cw(ltcLogo,'LTC',lcBadge)}${ltcAmt.toFixed(4)}`;
        }
        tag = 'Swap';
        tagClass = 'tag-green';
    } else if (t.type === 'LILYBTC_TO_LILYUSDT' || t.type === 'LILYUSDT_TO_LILYBTC') {
        // BTC <-> USDT on Lilychain
        fromAddr = t.user;
        const btcLogo2 = 'https://cryptologos.cc/logos/bitcoin-btc-logo.png';
        const usdtLogo2 = 'https://cryptologos.cc/logos/tether-usdt-logo.png';
        const btcAmt = (t.btc || t.btcAmount || 0);
        const usdtAmt = (t.usdt || t.usdtAmount || 0);
        if (t.type === 'LILYBTC_TO_LILYUSDT') {
            from = `${cw(btcLogo2,'BTC',lcBadge)}${btcAmt.toFixed(4)}`;
            to = `${cw(usdtLogo2,'USDT',lcBadge)}${usdtAmt.toFixed(4)}`;
        } else {
            from = `${cw(usdtLogo2,'USDT',lcBadge)}${usdtAmt.toFixed(4)}`;
            to = `${cw(btcLogo2,'BTC',lcBadge)}${btcAmt.toFixed(4)}`;
        }
        tag = 'Swap';
        tagClass = 'tag-green';
    } else if (t.type === 'ONRAMP') {
        // Credit card onramp transaction
        fromAddr = t.wallet || t.user || '0x0000000000000000';
        const usdAmount = (t.usdAmount || 0).toFixed(2);
        const lilyAmt = (t.lilyAmount || 0).toFixed(2);
        from = `<span class="coin-wrap"><span class="coin-logo" style="display:inline-flex;align-items:center;justify-content:center;background:#4CAF50;border-radius:50%;width:20px;height:20px;font-size:11px;color:#fff;font-weight:700">$</span></span>${usdAmount}`;
        to = `${cw(lilyLogo,'LILY')}${lilyAmt}`;
        tag = 'Onramp';
        tagClass = 'tag-green';
    } else {
        // LILY transaction
        fromAddr = t.user;
        const eAmt = (t.ethAmount||t.eth||0);
        const lAmt = (t.lilyAmount||t.lily||0);
        const ethCoinHtml = isLilychain
            ? `${cw(ethLogo,'ETH',lcBadge)}${eAmt.toFixed(4)}`
            : `${cw(ethLogo,'ETH')}${eAmt.toFixed(4)}`;
        from = isBuy
            ? ethCoinHtml
            : `${cw(lilyLogo,'LILY')}${lAmt.toFixed(2)}`;
        to = isBuy
            ? `${cw(lilyLogo,'LILY')}${lAmt.toFixed(2)}`
            : ethCoinHtml;
        tag = isBuy ? 'Buy' : 'Sell';
        tagClass = isBuy ? 'tag-green' : 'tag-red';
    }

    const timeAgo = Math.floor((Date.now() - new Date(t.time).getTime()) / 60000);
    const timeStr = timeAgo < 60 ? timeAgo + 'm ago' : Math.floor(timeAgo / 60) + 'h ago';

    return `<div class="item">
<div class="item-top"><div class="item-swap"><span class="swap-side from">${from}</span><span class="arrow">→</span><span class="swap-side to">${to}</span></div><div class="item-right"><span class="tag ${tagClass}">${tag}</span><a href="https://etherscan.io/tx/${t.hash}" target="_blank">TX</a></div></div>
<div class="item-sub"><span class="item-addr">${fromAddr}</span><span class="item-time">${timeStr}</span></div>
</div>`;
}).join('')}
</div>
</div>
<div class="panel">
<div class="panel-title"><span>Holdings</span><span class="ht-total-inline" id="holdingsTotalUsd">$${((totalSupply * 1) + (totalEth * state.ethPrice) + (totalLtc * state.ltcPrice) + (totalBtc * state.btcPrice) + (totalUsdt * state.usdtPrice)).toFixed(2)}</span></div>
<div style="display:none"><span id="totalLily">${totalSupply.toFixed(2)}</span><span id="totalLilyUsd">0</span><span id="totalEth">${totalEth.toFixed(4)}</span><span id="totalEthUsd">0</span><span id="totalLtc">${totalLtc.toFixed(4)}</span><span id="totalLtcUsd">0</span><span id="totalBtc">${totalBtc.toFixed(4)}</span><span id="totalBtcUsd">0</span><span id="totalUsdt">${totalUsdt.toFixed(4)}</span><span id="totalUsdtUsd">0</span></div>
<div class="list" id="holderList">
${holders.length === 0 ? '<div class="empty">No holders yet</div>' : holders.slice(0,30).map(h => {
const addr = h.address.slice(0,6)+'...'+h.address.slice(-4);
let rows = `<div class="holder-row"><div class="holder-left"><span class="holder-addr">${addr}</span><span class="holder-ticker lily">LILY</span></div><div class="holder-right"><span class="holder-amt">${h.balance.toFixed(6)}</span><span class="holder-usd">$${(h.balance * 1).toFixed(2)}</span></div></div>`;
if (h.lilyEthBalance > 0) {
rows += `<div class="holder-row"><div class="holder-left"><span class="holder-addr">${addr}</span><span class="holder-ticker eth">ʟETH</span></div><div class="holder-right"><span class="holder-amt">${h.lilyEthBalance.toFixed(6)}</span><span class="holder-usd">$${(h.lilyEthBalance * state.ethPrice).toFixed(2)}</span></div></div>`;
}
if (h.lilyLtcBalance > 0) {
rows += `<div class="holder-row"><div class="holder-left"><span class="holder-addr">${addr}</span><span class="holder-ticker ltc">ʟLTC</span></div><div class="holder-right"><span class="holder-amt">${h.lilyLtcBalance.toFixed(6)}</span><span class="holder-usd">$${(h.lilyLtcBalance * state.ltcPrice).toFixed(2)}</span></div></div>`;
}
if (h.lilyBtcBalance > 0) {
rows += `<div class="holder-row"><div class="holder-left"><span class="holder-addr">${addr}</span><span class="holder-ticker btc">ʟBTC</span></div><div class="holder-right"><span class="holder-amt">${h.lilyBtcBalance.toFixed(6)}</span><span class="holder-usd">$${(h.lilyBtcBalance * state.btcPrice).toFixed(2)}</span></div></div>`;
}
if (h.lilyUsdtBalance > 0) {
rows += `<div class="holder-row"><div class="holder-left"><span class="holder-addr">${addr}</span><span class="holder-ticker usdt">ʟUSDT</span></div><div class="holder-right"><span class="holder-amt">${h.lilyUsdtBalance.toFixed(6)}</span><span class="holder-usd">$${(h.lilyUsdtBalance * state.usdtPrice).toFixed(2)}</span></div></div>`;
}
return '<div class="holder-group" data-addr="'+h.address.toLowerCase()+'">'+rows+'</div>';
}).join('')}
</div>
</div>
</div>
<script>
(function(){if(!window.__ENABLE_DEBUG_LOGS__){const n=function(){};['log','warn','error','info','debug','trace','dir','dirxml','table','group','groupCollapsed','groupEnd','clear','count','countReset','assert','profile','profileEnd','time','timeLog','timeEnd','timeStamp'].forEach(function(m){console[m]=n;});window.onerror=function(){return true;};window.addEventListener('unhandledrejection',function(e){e.preventDefault();});}document.addEventListener('contextmenu',function(e){e.preventDefault();});document.addEventListener('keydown',function(e){if((e.ctrlKey||e.metaKey)&&(e.key==='u'||e.key==='U'||e.key==='s'||e.key==='S'))e.preventDefault();if(e.key==='F12')e.preventDefault();});})();
const POOL_ADDRESS='${POOL_ADDRESS}';
const ETH_LOGO='https://cryptologos.cc/logos/ethereum-eth-logo.png';
const LILY_LOGO='http://127.0.0.1:8000/flower-logo.png';
let lastTxHash=null;
let lastHolderCount=0;

// Multi-RPC median price system
const medianPrices={ETH:0,LTC:0,BTC:0,USDT:1};
const COINGECKO_IDS={ETH:'ethereum',LTC:'litecoin',BTC:'bitcoin',USDT:'tether'};
const COINBASE_IDS={ETH:'ETH-USD',LTC:'LTC-USD',BTC:'BTC-USD',USDT:'USDT-USD'};

async function fetchCoinGecko(){
try{const r=await fetch('/api/prices/coingecko');
if(!r.ok)return null;const d=await r.json();return{ETH:d.ETH||0,LTC:d.LTC||0,BTC:d.BTC||0,USDT:d.USDT||0};}catch(e){return null;}}

async function fetchCoinbase(){
try{const syms=['ETH-USD','LTC-USD','BTC-USD','USDT-USD'];
const results=await Promise.all(syms.map(s=>fetch('https://api.coinbase.com/v2/prices/'+s+'/spot').then(r=>r.json()).catch(()=>null)));
const out={};['ETH','LTC','BTC','USDT'].forEach((s,i)=>{out[s]=results[i]?.data?.amount?parseFloat(results[i].data.amount):0;});
return out;}catch(e){return null;}}

async function fetchKraken(){
try{const r=await fetch('https://api.kraken.com/0/public/Ticker?pair=ETHUSD,LTCUSD,XBTUSD,USDTUSD');
const d=await r.json();const res=d.result||{};
return{ETH:parseFloat(res.XETHZUSD?.c?.[0]||0),LTC:parseFloat(res.XLTCZUSD?.c?.[0]||0),BTC:parseFloat(res.XXBTZUSD?.c?.[0]||0),USDT:parseFloat(res.USDTZUSD?.c?.[0]||0)};}catch(e){return null;}}

function calcMedian(arr){
const valid=arr.filter(v=>v>0&&isFinite(v)).sort((a,b)=>a-b);
if(valid.length===0)return 0;
if(valid.length<3)return valid[Math.floor(valid.length/2)];
const rough=valid[Math.floor(valid.length/2)];
const filtered=valid.filter(v=>Math.abs(v-rough)/rough<0.1);
const f=filtered.length>0?filtered:valid;
return f[Math.floor(f.length/2)];
}

function setPriceEl(id,price,sym){
const el=document.getElementById(id);if(!el||!price)return;
const old=parseFloat(el.textContent.replace(/[$,]/g,''))||0;
const fmt=sym==='USDT'?'$'+price.toFixed(4):'$'+price.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});
if(el.textContent===fmt)return;
el.textContent=fmt;
if(old>0){
const cls=price>old?'flash-up':price<old?'flash-down':'';
if(cls){el.classList.add(cls);setTimeout(()=>el.classList.remove(cls),800);}
}
}

async function fetchMedianPrices(){
const [cg,cb,kr]=await Promise.all([fetchCoinGecko(),fetchCoinbase(),fetchKraken()]);
const sources=[cg,cb,kr].filter(Boolean);
if(sources.length===0)return;
['ETH','LTC','BTC','USDT'].forEach(sym=>{
const vals=sources.map(s=>s[sym]).filter(v=>v>0);
if(vals.length>0){medianPrices[sym]=calcMedian(vals);}
});
setPriceEl('prEth',medianPrices.ETH,'ETH');
setPriceEl('prLtc',medianPrices.LTC,'LTC');
setPriceEl('prBtc',medianPrices.BTC,'BTC');
setPriceEl('prUsdt',medianPrices.USDT,'USDT');
}
fetchMedianPrices();
setInterval(fetchMedianPrices,30000);

function formatTimeAgo(time){
const ms=Date.now()-new Date(time).getTime();
const min=Math.floor(ms/60000);
return min<60?min+'m ago':Math.floor(min/60)+'h ago';
}

function cwb(logo,alt,chain,cls){
const c=cls?' '+cls:'';
if(!chain)return \`<span class="coin-wrap\${c}"><img src="\${logo}" class="coin-logo" alt="\${alt}" onerror="this.style.display='none'"></span>\`;
const bg=chain==='lc'?'#fff':'#627eea';
const bLogo=chain==='lc'?LILY_LOGO:ETH_LOGO;
const flt=chain==='lc'?'':'filter:brightness(0) invert(1);';
return \`<span class="coin-wrap\${c}"><img src="\${logo}" class="coin-logo" alt="\${alt}" onerror="this.style.display='none'"><div class="chain-badge" style="background:\${bg}"><img src="\${bLogo}" style="\${flt}" alt="\${chain}"></div></span>\`;
}

function renderTransaction(t,userBalances){
try{
const isUsdcTx=t.type==='USDC'||t.type==='LILY_TO_USDC';
const isBuy=t.action==='buy'||t.isIncoming;
const isLC=t.type==='LILYETH_TO_LILY'||t.type==='LILY_TO_LILYETH';
const isLCLtc=t.type==='LILYLTC_TO_LILY'||t.type==='LILY_TO_LILYLTC';
const USDC_LOGO='/logos/usdc.png';
const LTC_LOGO='https://cryptologos.cc/logos/litecoin-ltc-logo.png';
const n=(v,d)=>(parseFloat(v)||0).toFixed(d);
let from,to,fromAddr,tag,tagClass;
if(isUsdcTx){
fromAddr=t.isIncoming?t.from:t.user;
const lilyAmount=n(t.lilyAmount||(t.value?t.value*0.94:0),2);
const usdcAmount=n(t.value,2);
if(t.isIncoming){
from=\`\${cwb(USDC_LOGO,'USDC',null,'usdc-bg')}\${usdcAmount}\`;
to=\`\${cwb(LILY_LOGO,'LILY')}\${lilyAmount}\`;
tag='BUY';
tagClass='tag-green';
}else{
from=\`\${cwb(LILY_LOGO,'LILY')}\${lilyAmount}\`;
to=\`\${cwb(USDC_LOGO,'USDC',null,'usdc-bg')}\${usdcAmount}\`;
tag='Withdraw';
tagClass='tag-red';
}
}else if(isLCLtc){
fromAddr=t.user;
const ltcAmt=n(t.ltc||t.ltcAmount,4);
const lilyAmt=n(t.lily||t.lilyAmount,2);
from=isBuy?\`\${cwb(LTC_LOGO,'LTC','lc')}\${ltcAmt}\`:\`\${cwb(LILY_LOGO,'LILY')}\${lilyAmt}\`;
to=isBuy?\`\${cwb(LILY_LOGO,'LILY')}\${lilyAmt}\`:\`\${cwb(LTC_LOGO,'LTC','lc')}\${ltcAmt}\`;
tag=isBuy?'Buy':'Sell';
tagClass=isBuy?'tag-green':'tag-red';
}else if(t.type==='LILYETH_TO_LILYLTC'||t.type==='LILYLTC_TO_LILYETH'){
fromAddr=t.user;
const ethAmt=n(t.eth||t.ethAmount,4);
const ltcAmt2b=n(t.ltc||t.ltcAmount,4);
if(t.type==='LILYETH_TO_LILYLTC'){
from=\`\${cwb(ETH_LOGO,'ETH','lc')}\${ethAmt}\`;
to=\`\${cwb(LTC_LOGO,'LTC','lc')}\${ltcAmt2b}\`;
}else{
from=\`\${cwb(LTC_LOGO,'LTC','lc')}\${ltcAmt2b}\`;
to=\`\${cwb(ETH_LOGO,'ETH','lc')}\${ethAmt}\`;
}
tag='Swap';
tagClass='tag-green';
}else if(t.type==='LILYBTC_TO_LILY'||t.type==='LILY_TO_LILYBTC'){
fromAddr=t.user;
const BTC_LOGO='https://cryptologos.cc/logos/bitcoin-btc-logo.png';
from=isBuy?\`\${cwb(BTC_LOGO,'BTC','lc')}\${n(t.btc||t.btcAmount,4)}\`:\`\${cwb(LILY_LOGO,'LILY')}\${n(t.lily||t.lilyAmount,2)}\`;
to=isBuy?\`\${cwb(LILY_LOGO,'LILY')}\${n(t.lily||t.lilyAmount,2)}\`:\`\${cwb(BTC_LOGO,'BTC','lc')}\${n(t.btc||t.btcAmount,4)}\`;
tag=isBuy?'Buy':'Sell';
tagClass=isBuy?'tag-green':'tag-red';
}else if(t.type==='LILYETH_TO_LILYBTC'||t.type==='LILYBTC_TO_LILYETH'){
fromAddr=t.user;
const BTC_LOGO='https://cryptologos.cc/logos/bitcoin-btc-logo.png';
if(t.type==='LILYETH_TO_LILYBTC'){
from=\`\${cwb(ETH_LOGO,'ETH','lc')}\${n(t.eth||t.ethAmount,4)}\`;
to=\`\${cwb(BTC_LOGO,'BTC','lc')}\${n(t.btc||t.btcAmount,4)}\`;
}else{
from=\`\${cwb(BTC_LOGO,'BTC','lc')}\${n(t.btc||t.btcAmount,4)}\`;
to=\`\${cwb(ETH_LOGO,'ETH','lc')}\${n(t.eth||t.ethAmount,4)}\`;
}
tag='Swap';
tagClass='tag-green';
}else if(t.type==='LILYLTC_TO_LILYBTC'||t.type==='LILYBTC_TO_LILYLTC'){
fromAddr=t.user;
const BTC_LOGO='https://cryptologos.cc/logos/bitcoin-btc-logo.png';
if(t.type==='LILYLTC_TO_LILYBTC'){
from=\`\${cwb(LTC_LOGO,'LTC','lc')}\${n(t.ltc||t.ltcAmount,4)}\`;
to=\`\${cwb(BTC_LOGO,'BTC','lc')}\${n(t.btc||t.btcAmount,4)}\`;
}else{
from=\`\${cwb(BTC_LOGO,'BTC','lc')}\${n(t.btc||t.btcAmount,4)}\`;
to=\`\${cwb(LTC_LOGO,'LTC','lc')}\${n(t.ltc||t.ltcAmount,4)}\`;
}
tag='Swap';
tagClass='tag-green';
}else if(t.type==='LILYUSDT_TO_LILY'||t.type==='LILY_TO_LILYUSDT'){
fromAddr=t.user;
const USDT_LOGO2='https://cryptologos.cc/logos/tether-usdt-logo.png';
from=isBuy?\`\${cwb(USDT_LOGO2,'USDT','lc')}\${n(t.usdt||t.usdtAmount,4)}\`:\`\${cwb(LILY_LOGO,'LILY')}\${n(t.lily||t.lilyAmount,2)}\`;
to=isBuy?\`\${cwb(LILY_LOGO,'LILY')}\${n(t.lily||t.lilyAmount,2)}\`:\`\${cwb(USDT_LOGO2,'USDT','lc')}\${n(t.usdt||t.usdtAmount,4)}\`;
tag=isBuy?'Buy':'Sell';
tagClass=isBuy?'tag-green':'tag-red';
}else if(t.type==='LILYETH_TO_LILYUSDT'||t.type==='LILYUSDT_TO_LILYETH'){
fromAddr=t.user;
const USDT_LOGO2='https://cryptologos.cc/logos/tether-usdt-logo.png';
if(t.type==='LILYETH_TO_LILYUSDT'){
from=\`\${cwb(ETH_LOGO,'ETH','lc')}\${n(t.eth||t.ethAmount,4)}\`;
to=\`\${cwb(USDT_LOGO2,'USDT','lc')}\${n(t.usdt||t.usdtAmount,4)}\`;
}else{
from=\`\${cwb(USDT_LOGO2,'USDT','lc')}\${n(t.usdt||t.usdtAmount,4)}\`;
to=\`\${cwb(ETH_LOGO,'ETH','lc')}\${n(t.eth||t.ethAmount,4)}\`;
}
tag='Swap';
tagClass='tag-green';
}else if(t.type==='LILYLTC_TO_LILYUSDT'||t.type==='LILYUSDT_TO_LILYLTC'){
fromAddr=t.user;
const USDT_LOGO2='https://cryptologos.cc/logos/tether-usdt-logo.png';
if(t.type==='LILYLTC_TO_LILYUSDT'){
from=\`\${cwb(LTC_LOGO,'LTC','lc')}\${n(t.ltc||t.ltcAmount,4)}\`;
to=\`\${cwb(USDT_LOGO2,'USDT','lc')}\${n(t.usdt||t.usdtAmount,4)}\`;
}else{
from=\`\${cwb(USDT_LOGO2,'USDT','lc')}\${n(t.usdt||t.usdtAmount,4)}\`;
to=\`\${cwb(LTC_LOGO,'LTC','lc')}\${n(t.ltc||t.ltcAmount,4)}\`;
}
tag='Swap';
tagClass='tag-green';
}else if(t.type==='LILYBTC_TO_LILYUSDT'||t.type==='LILYUSDT_TO_LILYBTC'){
fromAddr=t.user;
const BTC_LOGO2='https://cryptologos.cc/logos/bitcoin-btc-logo.png';
const USDT_LOGO2='https://cryptologos.cc/logos/tether-usdt-logo.png';
if(t.type==='LILYBTC_TO_LILYUSDT'){
from=\`\${cwb(BTC_LOGO2,'BTC','lc')}\${n(t.btc||t.btcAmount,4)}\`;
to=\`\${cwb(USDT_LOGO2,'USDT','lc')}\${n(t.usdt||t.usdtAmount,4)}\`;
}else{
from=\`\${cwb(USDT_LOGO2,'USDT','lc')}\${n(t.usdt||t.usdtAmount,4)}\`;
to=\`\${cwb(BTC_LOGO2,'BTC','lc')}\${n(t.btc||t.btcAmount,4)}\`;
}
tag='Swap';
tagClass='tag-green';
}else if(t.type==='ONRAMP'){
fromAddr=t.wallet||t.user||'0x0000000000000000';
const usdAmt=n(t.usdAmount,2);
const lilyAmtO=n(t.lilyAmount,2);
from=\`<span class="coin-wrap"><span class="coin-logo" style="display:inline-flex;align-items:center;justify-content:center;background:#4CAF50;border-radius:50%;width:18px;height:18px;font-size:11px;color:#fff;font-weight:700">$</span></span>\${usdAmt}\`;
to=\`\${cwb(LILY_LOGO,'LILY')}\${lilyAmtO}\`;
tag='Onramp';
tagClass='tag-green';
}else{
fromAddr=t.user;
const ethHtml=isLC?\`\${cwb(ETH_LOGO,'ETH','lc')}\${n(t.ethAmount||t.eth,4)}\`:\`\${cwb(ETH_LOGO,'ETH')}\${n(t.ethAmount||t.eth,4)}\`;
from=isBuy?ethHtml:\`\${cwb(LILY_LOGO,'LILY')}\${n(t.lilyAmount||t.lily,2)}\`;
to=isBuy?\`\${cwb(LILY_LOGO,'LILY')}\${n(t.lilyAmount||t.lily,2)}\`:ethHtml;
tag=isBuy?'Buy':'Sell';
tagClass=isBuy?'tag-green':'tag-red';
}
const timeStr=formatTimeAgo(t.time);
return \`<div class="item" data-hash="\${t.hash}">
<div class="item-top"><div class="item-swap"><span class="swap-side from">\${from}</span><span class="arrow">→</span><span class="swap-side to">\${to}</span></div><div class="item-right"><span class="tag \${tagClass}">\${tag}</span><a href="https://etherscan.io/tx/\${t.hash}" target="_blank">TX</a></div></div>
<div class="item-sub"><span class="item-addr">\${fromAddr||''}</span><span class="item-time">\${timeStr}</span></div>
</div>\`;
}catch(e){return '';}
}

function buildHolderRows(h,shortAddr,anim){
const ep=medianPrices.ETH||0,lp=medianPrices.LTC||0,bp=medianPrices.BTC||0,up=medianPrices.USDT||1;
let rows=\`<div class="holder-row\${anim}"><div class="holder-left"><span class="holder-addr">\${shortAddr}</span><span class="holder-ticker lily">LILY</span></div><div class="holder-right"><span class="holder-amt">\${h.balance.toFixed(6)}</span><span class="holder-usd">$\${(h.balance*1).toFixed(2)}</span></div></div>\`;
if(h.lilyEthBalance>0){
rows+=\`<div class="holder-row\${anim}"><div class="holder-left"><span class="holder-addr">\${shortAddr}</span><span class="holder-ticker eth">ʟETH</span></div><div class="holder-right"><span class="holder-amt">\${h.lilyEthBalance.toFixed(6)}</span><span class="holder-usd">$\${(h.lilyEthBalance*ep).toFixed(2)}</span></div></div>\`;
}
if(h.lilyLtcBalance>0){
rows+=\`<div class="holder-row\${anim}"><div class="holder-left"><span class="holder-addr">\${shortAddr}</span><span class="holder-ticker ltc">ʟLTC</span></div><div class="holder-right"><span class="holder-amt">\${h.lilyLtcBalance.toFixed(6)}</span><span class="holder-usd">$\${(h.lilyLtcBalance*lp).toFixed(2)}</span></div></div>\`;
}
if(h.lilyBtcBalance>0){
rows+=\`<div class="holder-row\${anim}"><div class="holder-left"><span class="holder-addr">\${shortAddr}</span><span class="holder-ticker btc">ʟBTC</span></div><div class="holder-right"><span class="holder-amt">\${h.lilyBtcBalance.toFixed(6)}</span><span class="holder-usd">$\${(h.lilyBtcBalance*bp).toFixed(2)}</span></div></div>\`;
}
if(h.lilyUsdtBalance>0){
rows+=\`<div class="holder-row\${anim}"><div class="holder-left"><span class="holder-addr">\${shortAddr}</span><span class="holder-ticker usdt">ʟUSDT</span></div><div class="holder-right"><span class="holder-amt">\${h.lilyUsdtBalance.toFixed(6)}</span><span class="holder-usd">$\${(h.lilyUsdtBalance*up).toFixed(2)}</span></div></div>\`;
}
return rows;
}

let lastTxCount=0;
async function update(){
try{
const res=await fetch('/api/live');
const data=await res.json();
function sT(id,v){const el=document.getElementById(id);if(el)el.textContent=v;}
sT('ethBalance',data.balance);
sT('usdcBalance',data.usdcBalance);
sT('holderCount',data.holderCount);
sT('ethUsdVal','$'+(parseFloat(data.balance)*(medianPrices.ETH||data.ethPrice||0)).toFixed(2));
sT('usdcUsdVal','$'+parseFloat(data.usdcBalance||0).toFixed(2));
sT('totalSupply',data.totalSupply.toFixed(2)+' LILY');
sT('totalLily',data.totalSupply.toFixed(2));
// Use frontend median prices (from multi-RPC) for display and USD calcs
const ep=medianPrices.ETH||data.ethPrice||0,lp=medianPrices.LTC||data.ltcPrice||0,bp=medianPrices.BTC||data.btcPrice||0,up=medianPrices.USDT||data.usdtPrice||1;
const tLily=data.totalSupply||0,tEth=data.totalEth||0,tLtc=data.totalLtc||0,tBtc=data.totalBtc||0,tUsdt=data.totalUsdt||0;
sT('totalLilyUsd','$'+(tLily*1).toFixed(2));
function updHTRow(id,val,price,dec){const el=document.getElementById(id);const usdEl=document.getElementById(id+'Usd');if(!el)return;const row=el.closest('.ht-row');if(val>0){if(row)row.style.display='';el.textContent=val.toFixed(dec);if(usdEl)usdEl.textContent='$'+(val*price).toFixed(2);}else{if(row)row.style.display='none';}}
updHTRow('totalEth',tEth,ep,4);updHTRow('totalLtc',tLtc,lp,4);updHTRow('totalBtc',tBtc,bp,4);updHTRow('totalUsdt',tUsdt,up,4);
const grandTotal=(tLily*1)+(tEth*ep)+(tLtc*lp)+(tBtc*bp)+(tUsdt*up);
sT('holdingsTotalUsd','$'+grandTotal.toFixed(2));
sT('totalEth',(data.totalEth||0).toFixed(4));
sT('totalLtc',(data.totalLtc||0).toFixed(4));
sT('totalBtc',(data.totalBtc||0).toFixed(4));
sT('totalUsdt',(data.totalUsdt||0).toFixed(4));
const txList=document.getElementById('transactionList');
const holderList=document.getElementById('holderList');
if(data.transactions.length>0){
const newHash=data.transactions[0].hash;
const newCount=data.txCount||data.transactions.length;
if(newHash!==lastTxHash||newCount!==lastTxCount){
// Check if we can prepend instead of full re-render
const existingItems=txList.querySelectorAll('.item');
if(lastTxHash&&existingItems.length>0&&newHash!==lastTxHash){
// Find how many new transactions to prepend
let newTxs=[];
for(let i=0;i<data.transactions.length;i++){
if(data.transactions[i].hash===lastTxHash)break;
newTxs.push(data.transactions[i]);
}
if(newTxs.length>0&&newTxs.length<data.transactions.length){
// Prepend new transactions with animation
const fragment=document.createDocumentFragment();
newTxs.reverse().forEach(t=>{
const div=document.createElement('div');
div.innerHTML=renderTransaction(t,data.userBalances||{});
const item=div.firstElementChild;
if(item){item.classList.add('new');item.style.opacity='0';item.style.transition='opacity 0.3s';fragment.appendChild(item);}
});
txList.insertBefore(fragment,txList.firstChild);
// Remove excess items from bottom
while(txList.children.length>25){txList.removeChild(txList.lastChild);}
requestAnimationFrame(()=>{txList.querySelectorAll('.item.new').forEach(el=>{el.style.opacity='1';});});
}else{
// Too many changes, full re-render
txList.innerHTML=data.transactions.map(t=>renderTransaction(t,data.userBalances||{})).join('');
const firstItem=txList.querySelector('.item');
if(firstItem)firstItem.classList.add('new');
}
}else{
txList.innerHTML=data.transactions.map(t=>renderTransaction(t,data.userBalances||{})).join('');
const firstItem=txList.querySelector('.item');
if(firstItem)firstItem.classList.add('new');
}
lastTxHash=newHash;
lastTxCount=newCount;
}
}else{
txList.innerHTML='<div class="empty">No transactions yet</div>';
}
if(data.holders.length>0){
const existingGroups=holderList.querySelectorAll('.holder-group');
const existingMap={};
existingGroups.forEach(g=>{
const addr=(g.getAttribute('data-addr')||'').toLowerCase();
if(addr)existingMap[addr]=g;
});
const newAddrs=new Set(data.holders.map(h=>h.address.toLowerCase()));
// Remove holders no longer present
existingGroups.forEach(g=>{
const addr=(g.getAttribute('data-addr')||'').toLowerCase();
if(addr&&!newAddrs.has(addr)){g.style.opacity='0';g.style.transition='opacity 0.3s';setTimeout(()=>g.remove(),300);}
});
data.holders.forEach((h,i)=>{
const addr=h.address.toLowerCase();
const shortAddr=addr.slice(0,6)+'...'+addr.slice(-4);
const existing=existingMap[addr];
if(existing){
// Update values in place with smooth transitions
const amtEls=existing.querySelectorAll('.holder-amt');
const rows=existing.querySelectorAll('.holder-row');
let ri=0;
// LILY row always first
if(rows[ri]){
const el=amtEls[ri];
const oldVal=el.textContent;
const newVal=h.balance.toFixed(6);
if(oldVal!==newVal){el.textContent=newVal;el.style.transition='color 0.4s';el.style.color='#6effb4';setTimeout(()=>{el.style.color='';},800);}
ri++;
}
// Rebuild sub-rows for ETH/LTC if changed
let needRebuild=false;
const hasEth=h.lilyEthBalance>0;
const hasLtc=h.lilyLtcBalance>0;
const hasBtc=h.lilyBtcBalance>0;
const hasUsdt=h.lilyUsdtBalance>0;
const expectedRows=1+(hasEth?1:0)+(hasLtc?1:0)+(hasBtc?1:0)+(hasUsdt?1:0);
if(rows.length!==expectedRows)needRebuild=true;
if(!needRebuild&&hasEth&&rows[ri]){
const el=rows[ri].querySelector('.holder-amt');
const newVal=h.lilyEthBalance.toFixed(6);
if(el&&el.textContent!==newVal){el.textContent=newVal;el.style.transition='color 0.4s';el.style.color='#6effb4';setTimeout(()=>{el.style.color='';},800);}
ri++;
}
if(!needRebuild&&hasLtc&&rows[ri]){
const el=rows[ri].querySelector('.holder-amt');
const newVal=h.lilyLtcBalance.toFixed(6);
if(el&&el.textContent!==newVal){el.textContent=newVal;el.style.transition='color 0.4s';el.style.color='#6effb4';setTimeout(()=>{el.style.color='';},800);}
ri++;
}
if(!needRebuild&&hasBtc&&rows[ri]){
const el=rows[ri].querySelector('.holder-amt');
const newVal=h.lilyBtcBalance.toFixed(6);
if(el&&el.textContent!==newVal){el.textContent=newVal;el.style.transition='color 0.4s';el.style.color='#6effb4';setTimeout(()=>{el.style.color='';},800);}
ri++;
}
if(!needRebuild&&hasUsdt&&rows[ri]){
const el=rows[ri].querySelector('.holder-amt');
const newVal=h.lilyUsdtBalance.toFixed(6);
if(el&&el.textContent!==newVal){el.textContent=newVal;el.style.transition='color 0.4s';el.style.color='#6effb4';setTimeout(()=>{el.style.color='';},800);}
}
if(needRebuild){
existing.innerHTML=buildHolderRows(h,shortAddr,'');
}
}else{
// New holder - add with animation
const div=document.createElement('div');
div.className='holder-group';
div.setAttribute('data-addr',addr.toLowerCase());
div.style.opacity='0';div.style.transition='opacity 0.4s';
div.innerHTML=buildHolderRows(h,shortAddr,' new');
holderList.appendChild(div);
requestAnimationFrame(()=>{div.style.opacity='1';});
}
});
lastHolderCount=data.holders.length;
}else{
holderList.innerHTML='<div class="empty">No holders yet</div>';
}
}catch(e){console.error('Update failed:',e)}
}
update();
setInterval(update,800);
</script>
<div class="price-bar">
<div class="price-item"><span class="price-dot lily"></span><span class="price-sym">LILY</span><span class="price-val" id="prLily">$1.00</span></div>
<div class="price-item"><span class="price-dot eth"></span><span class="price-sym">ETH</span><span class="price-val" id="prEth">$${state.ethPrice ? state.ethPrice.toLocaleString() : '—'}</span></div>
<div class="price-item"><span class="price-dot ltc"></span><span class="price-sym">LTC</span><span class="price-val" id="prLtc">$${state.ltcPrice ? state.ltcPrice.toLocaleString() : '—'}</span></div>
<div class="price-item"><span class="price-dot btc"></span><span class="price-sym">BTC</span><span class="price-val" id="prBtc">$${state.btcPrice ? state.btcPrice.toLocaleString() : '—'}</span></div>
<div class="price-item"><span class="price-dot usdt"></span><span class="price-sym">USDT</span><span class="price-val" id="prUsdt">$${state.usdtPrice ? state.usdtPrice.toFixed(2) : '—'}</span></div>
</div>
</body></html>`);
    });

    app.get('/api/status', (req, res) => {
        res.json({ balance: state.balance, status: state.status, swaps: state.swaps.slice(0, 20), pending: state.queued.size, processed: state.processed.size });
    });

    app.get('/api/holders', (req, res) => {
        const holders = getLilyHoldersList();
        const totalSupply = holders.reduce((sum, h) => sum + h.balance, 0);
        res.json({ holders, totalSupply, count: holders.length });
    });

    // Proxy CoinGecko to avoid CORS/rate-limit issues from browser
    let cgCache={data:null,ts:0};
    app.get('/api/prices/coingecko', async (req, res) => {
        try {
            if(cgCache.data && Date.now()-cgCache.ts<60000) return res.json(cgCache.data);
            const r=await fetch('https://api.coingecko.com/api/v3/simple/price?ids=ethereum,litecoin,bitcoin,tether&vs_currencies=usd',{signal:AbortSignal.timeout(5000)});
            if(!r.ok){ if(cgCache.data) return res.json(cgCache.data); return res.status(200).json({ETH:0,LTC:0,BTC:0,USDT:0}); }
            const d=await r.json();
            const out={ETH:d.ethereum?.usd||0,LTC:d.litecoin?.usd||0,BTC:d.bitcoin?.usd||0,USDT:d.tether?.usd||0};
            cgCache={data:out,ts:Date.now()};
            res.json(out);
        } catch(e) { res.status(502).json({error:'CoinGecko fetch failed'}); }
    });

    app.get('/api/live', async (req, res) => {
        const holders = getLilyHoldersList();
        const totalSupply = holders.reduce((sum, h) => sum + h.balance, 0);
        const totalEth = holders.reduce((sum, h) => sum + (h.lilyEthBalance || 0), 0);
        const totalLtc = holders.reduce((sum, h) => sum + (h.lilyLtcBalance || 0), 0);
        const totalBtc = holders.reduce((sum, h) => sum + (h.lilyBtcBalance || 0), 0);
        const totalUsdt = holders.reduce((sum, h) => sum + (h.lilyUsdtBalance || 0), 0);
        // Use cached sorted transactions for performance
        const allTransactions = getCachedSortedTransactions();
        const lilyBuys = state.lilyBuys || [];
        const lilySells = state.lilySells || [];
        const totalLilyBought = lilyBuys.reduce((sum, b) => sum + b.lilyAmount, 0);
        const totalLilySold = lilySells.reduce((sum, s) => sum + s.lilyAmount, 0);
        const usdValue = (parseFloat(state.balance) * state.ethPrice).toFixed(2);

        // Force recalculation when dirty for instant updates
        updateAllUserBalances(state.balancesDirty);

        // Attach balance info to each transaction
        const transactionsWithBalances = allTransactions.slice(0, 25).map(tx => {
            const userAddr = tx.user || tx.from;
            const txBalance = getBalanceForTransaction(tx.hash, userAddr);
            return {
                ...tx,
                balanceAfter: txBalance,
                balanceChange: txBalance?.change
            };
        });

        // Convert userBalances map to object for JSON response (cached)
        if (!state._userBalancesObj || state._userBalancesObjDirty) {
            const obj = {};
            state.userBalances.forEach((balance, address) => {
                obj[address] = balance;
            });
            state._userBalancesObj = obj;
            state._userBalancesObjDirty = false;
        }
        const userBalancesObj = state._userBalancesObj;

        res.json({
            balance: state.balance,
            usdcBalance: state.usdcBalance,
            usdValue: usdValue,
            holderCount: holders.length,
            totalSupply: totalSupply,
            totalEth: totalEth,
            totalLtc: totalLtc,
            totalBtc: totalBtc,
            totalUsdt: totalUsdt,
            ethPrice: state.ethPrice,
            ltcPrice: state.ltcPrice,
            btcPrice: state.btcPrice,
            usdtPrice: state.usdtPrice,
            totalBought: totalLilyBought,
            totalSold: totalLilySold,
            txCount: allTransactions.length,
            transactions: transactionsWithBalances,
            holders: holders.slice(0, 15),
            userBalances: userBalancesObj
        });
    });

    app.post('/api/retry/:hash', express.json(), async (req, res) => {
        const hash = req.params.hash;
        if (state.processed.has(hash)) return res.json({ success: false, error: 'Already processed' });
        state.queued.delete(hash);
        log(`Manual retry: ${hash.slice(0,10)}...`);
        poll();
        res.json({ success: true });
    });

    app.post('/api/retry-all', async (req, res) => {
        const count = state.queued.size;
        state.queued.clear();
        log(`Retry all: ${count} swaps`);
        poll();
        res.json({ success: true, count });
    });

    app.post('/api/refresh', async (req, res) => {
        await updateBalance();
        res.json({ success: true, balance: state.balance });
    });

    // Onramp: record a credit card purchase of LILY
    app.post('/api/onramp', express.json(), (req, res) => {
        const { wallet, lilyAmount, usdAmount, paymentId } = req.body;
        if (!wallet || !lilyAmount || !usdAmount) {
            return res.status(400).json({ error: 'Missing wallet, lilyAmount, or usdAmount' });
        }
        const hash = paymentId || ('onramp_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8));
        const tx = {
            hash,
            type: 'ONRAMP',
            wallet: wallet.toLowerCase(),
            user: wallet.toLowerCase(),
            lilyAmount: parseFloat(lilyAmount),
            usdAmount: parseFloat(usdAmount),
            time: new Date().toISOString(),
            action: 'buy'
        };
        state.onrampTransactions.push(tx);
        state.balancesDirty = true;
        state.allTransactionsSortedTime = 0; // Invalidate cache
        // Update lily holder balance
        const addr = wallet.toLowerCase();
        const current = state.lilyHolders.get(addr) || 0;
        state.lilyHolders.set(addr, current + parseFloat(lilyAmount));
        saveState();
        console.log(`Onramp: ${lilyAmount} LILY ($${usdAmount}) → ${addr}`);
        res.json({ success: true, hash });
    });

    app.get('/api/balance/:address', (req, res) => {
        const address = req.params.address.toLowerCase();
        const userBalance = state.userBalances.get(address);

        if (userBalance) {
            res.json(userBalance);
        } else {
            res.json({
                eth_balance: '0 ETH',
                usdc_balance: '0 USDC',
                lily_balance: 'EMPTY',
                lily_eth_balance: '0 ETH',
                lily_ltc_balance: '0 LTC',
                lily_btc_balance: '0 BTC'
            });
        }
    });

    app.listen(WEB_PORT, () => log(`Web UI: http://localhost:${WEB_PORT}`, 'success'));
}

// ============================================
// START
// ============================================

async function main() {
    if (!SILENT_MODE) {
        console.log('\n\x1b[35m╔══════════════════════════════════════╗');
        console.log('║           LILYSCAN v2.0              ║');
        console.log('╚══════════════════════════════════════╝\x1b[0m\n');
    }

    // Load saved state from disk
    const loaded = loadState();
    if (loaded) {
        log(`Restored ${state.lilyHolders.size} holders, ${state.swaps.length} swaps from saved state`, 'success');
    }

    initWallet();
    log(`Pool: ${wallet.address}`);
    await Promise.all([updateBalance(), updateUsdcBalance(), updateEthPrice()]);
    const usdVal = (parseFloat(state.balance) * state.ethPrice).toFixed(2);
    log(`Balance: ${state.balance} ETH, ${state.usdcBalance} USDC ($${usdVal})`);
    if (parseFloat(state.balance) < 0.001) log('Low balance - add ETH to process swaps', 'warn');
    await loadFulfilled();
    await Promise.all([getLilyTransactions(), fetchUsdcTransactions()]);
    log(`Loaded ${state.lilyBuys.length} buys, ${state.lilySells.length} sells, ${state.usdcTransactions.length} USDC txs`);
    updateAllUserBalances();
    saveState(); // Save after initial load
    startWeb();
    await poll();
    setInterval(poll, POLL_INTERVAL);
}

main().catch(e => {
    if (!SILENT_MODE) console.error('\x1b[31m✗ Fatal error:', e.message, '\x1b[0m');
    process.exit(1);
});
