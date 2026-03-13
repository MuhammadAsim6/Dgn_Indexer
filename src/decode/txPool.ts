/**
 * @module txPool
 * @description
 * This module provides a pool of worker threads for decoding transactions in parallel.
 * It manages worker initialization, job scheduling, and result collection, enabling efficient
 * transaction decoding using multiple threads. The pool is designed to be used in environments
 * where high throughput and non-blocking transaction decoding is required.
 */
// src/decode/txPool.ts
import { Worker } from 'node:worker_threads';
import { getLogger } from '../utils/logger.js';

type ProgressMsg = { type: 'progress'; loaded: number; total: number };
type ReadyMsg = { type: 'ready'; ok: boolean; detail?: string };
type OkMsg = { id: number; ok: true; decoded: any };
type ErrMsg = { id: number; ok: false; error: string };

type AnyOut = ProgressMsg | ReadyMsg | OkMsg | ErrMsg;

/**
 * Represents a pool of worker threads for decoding transactions.
 * @typedef {Object} TxDecodePool
 * @property {(txBase64: string) => Promise<any>} submit - Submit a base64-encoded transaction for decoding.
 *   @param {string} txBase64 - The base64-encoded transaction to decode.
 *   @returns {Promise<any>} - A promise that resolves with the decoded transaction, or rejects on error.
 * @property {() => Promise<void>} close - Gracefully shuts down all worker threads in the pool.
 *   @returns {Promise<void>} - A promise that resolves when all workers have terminated.
 */
export type TxDecodePool = {
  submit: (txBase64: string) => Promise<any>;
  decodeGeneric: (typeUrl: string, base64: string) => Promise<any>;
  close: () => Promise<void>;
};

const INIT_TIMEOUT_MS = 30000;
const WAIT_IDLE_TIMEOUT_MS = 30000;
const log = getLogger('decode/txPool');

/**
 * Creates a pool of worker threads for parallel transaction decoding.
 *
 * @param {number} size - The number of worker threads to spawn in the pool.
 * @param {Object} [opts] - Optional settings.
 * @param {string} [opts.protoDir] - Directory containing protobuf definitions for the workers.
 * @returns {TxDecodePool} An object with `submit` and `close` methods for interacting with the pool.
 */
export function createTxDecodePool(size: number, opts?: { protoDir?: string }): TxDecodePool {
  const workers: Worker[] = [];
  const idle: number[] = [];
  const pending = new Map<number, { resolve: (v: any) => void; reject: (e: any) => void; workerId: number }>();
  const readyFlags: boolean[] = Array(size).fill(false);
  const readyPromises: Array<Promise<void>> = [];
  const workerAlive: boolean[] = Array(size).fill(true);
  const perWorkerProgress: Record<number, { loaded: number; total: number }> = {};
  let nextJobId = 1;
  let readySettled = false;

  log.info(`[txPool] creating ${size} worker(s)`);

  for (let i = 0; i < size; i++) {
    const w = new Worker(new URL('./txWorker.ts', import.meta.url), {
      execArgv: ['--import', 'tsx/esm'],
      stdout: true,
      stderr: true,
    });
    // @ts-ignore
    w.stdout?.pipe(process.stdout);
    // @ts-ignore
    w.stderr?.pipe(process.stderr);

    workers.push(w);

    let resolveReady!: () => void;
    const p = new Promise<void>((resolve) => {
      resolveReady = resolve;
    });
    readyPromises.push(p);

    const timer = setTimeout(() => {
      if (!readyFlags[i]) {
        log.error(`[txPool] worker #${i} init timeout after ${INIT_TIMEOUT_MS}ms`);
        workerAlive[i] = false;
        void w.terminate().catch(() => undefined);
        resolveReady();
      }
    }, INIT_TIMEOUT_MS);

    w.on('online', () => log.info(`[txPool] worker #${i} online`));

    w.on('message', (m: AnyOut | any) => {
      if (m?.type === 'progress') {
        const { loaded, total } = m as ProgressMsg;
        perWorkerProgress[i] = { loaded, total };
        const totals = Object.values(perWorkerProgress);
        if (totals.length > 0) {
          const sumLoaded = totals.reduce((a, b) => a + b.loaded, 0);
          const sumTotal = totals.reduce((a, b) => a + b.total, 0);
          const pct = sumTotal > 0 ? Math.floor((sumLoaded / sumTotal) * 100) : 0;
          log.debug(`[proto] loading: ${sumLoaded}/${sumTotal} (${pct}%)`);
        }
        return;
      }

      if (m?.type === 'ready') {
        if (!readyFlags[i]) {
          readyFlags[i] = true;
          clearTimeout(timer);
          if ((m as ReadyMsg).ok !== false) {
            log.info(`[txPool] worker #${i} ready`);
          } else {
            log.warn(`[txPool] worker #${i} init not-ok: ${(m as ReadyMsg).detail ?? ''}`);
          }
          if (workerAlive[i]) idle.push(i);
          resolveReady();
        }
        return;
      }

      if (typeof (m as OkMsg | ErrMsg)?.id === 'number') {
        const entry = pending.get((m as OkMsg | ErrMsg).id);
        if (!entry) return;
        if (entry.workerId !== i) return;
        pending.delete((m as OkMsg | ErrMsg).id);
        if (workerAlive[i]) idle.push(i);
        if ((m as OkMsg).ok) entry.resolve((m as OkMsg).decoded);
        else entry.reject(new Error((m as ErrMsg).error));
        return;
      }
    });

    w.on('error', (e) => {
      log.error(`[txPool] worker #${i} error: ${e?.message ?? e}`);
      clearTimeout(timer);
      workerAlive[i] = false;
      if (!readyFlags[i]) resolveReady();
      for (let k = idle.length - 1; k >= 0; k--) {
        if (idle[k] === i) idle.splice(k, 1);
      }
      for (const [id, p] of pending) {
        if (p.workerId !== i) continue;
        p.reject(e);
        pending.delete(id);
      }
    });

    w.on('exit', (code) => {
      log.warn(`[txPool] worker #${i} exited with code ${code}`);
      clearTimeout(timer);
      workerAlive[i] = false;
      if (!readyFlags[i]) resolveReady();
      for (let k = idle.length - 1; k >= 0; k--) {
        if (idle[k] === i) idle.splice(k, 1);
      }
      for (const [id, p] of pending) {
        if (p.workerId !== i) continue;
        p.reject(new Error(`worker ${i} exited with code ${code}`));
        pending.delete(id);
      }
    });

    w.postMessage({ type: 'init', protoDir: opts?.protoDir });
  }

  const activeWorkers = () => readyFlags.filter((v, idx) => v && workerAlive[idx]).length;

  async function waitAllReady() {
    if (!readySettled) {
      await Promise.allSettled(readyPromises);
      readySettled = true;
      const active = activeWorkers();
      if (active === 0) {
        throw new Error('No decode workers are ready');
      }
      if (active < size) {
        log.warn(`[txPool] running in degraded mode: active=${active}/${size}`);
      }
    }
  }

  async function waitForIdleWorker(): Promise<number> {
    const started = Date.now();
    for (;;) {
      if (idle.length > 0) {
        return idle.shift() as number;
      }
      const active = activeWorkers();
      if (active === 0) {
        throw new Error('No active decode workers available');
      }
      if (Date.now() - started >= WAIT_IDLE_TIMEOUT_MS) {
        throw new Error(`Timed out waiting for idle decoder worker after ${WAIT_IDLE_TIMEOUT_MS}ms`);
      }
      await new Promise((r) => setTimeout(r, 1));
    }
  }

  async function submit(txBase64: string): Promise<any> {
    await waitAllReady();
    const wid = await waitForIdleWorker();
    const w = workers[wid];

    const id = nextJobId++;
    const prom = new Promise<any>((resolve, reject) => {
      pending.set(id, { resolve, reject, workerId: wid });
    });

    w?.postMessage({ type: 'decode', id, txBase64 });
    return prom;
  }

  async function decodeGeneric(typeUrl: string, base64: string): Promise<any> {
    await waitAllReady();
    const wid = await waitForIdleWorker();
    const w = workers[wid];

    const id = nextJobId++;
    const prom = new Promise<any>((resolve, reject) => {
      pending.set(id, { resolve, reject, workerId: wid });
    });

    w?.postMessage({ type: 'decode-generic', id, typeUrl, base64 });
    return prom;
  }

  async function close() {
    await Promise.all(workers.map((w) => w.terminate()));
  }

  return { submit, decodeGeneric, close };
}
