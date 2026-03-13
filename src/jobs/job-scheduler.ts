/**
 * Job Scheduler — Lightweight background job manager using setInterval.
 * Same pattern as the existing reconcileTimer in index.ts.
 */
import type { Pool } from 'pg';
import { getLogger } from '../utils/logger.js';

const log = getLogger('jobs/scheduler');

export interface JobDefinition {
    name: string;
    intervalMs: number;
    fn: (pool: Pool) => Promise<void>;
}

interface RunningJob {
    def: JobDefinition;
    timer: NodeJS.Timeout;
    running: boolean;
}

export class JobScheduler {
    private jobs: RunningJob[] = [];
    private pool: Pool;

    constructor(pool: Pool) {
        this.pool = pool;
    }

    register(def: JobDefinition): void {
        log.info(`[scheduler] registered job: ${def.name} (every ${def.intervalMs}ms)`);
        const job: RunningJob = {
            def,
            running: false,
            timer: setInterval(async () => {
                if (job.running) {
                    log.debug(`[scheduler] ${def.name} still running, skipping tick`);
                    return;
                }
                job.running = true;
                try {
                    await def.fn(this.pool);
                } catch (err: any) {
                    log.error(`[scheduler] ${def.name} error: ${err.message}`);
                } finally {
                    job.running = false;
                }
            }, def.intervalMs),
        };
        this.jobs.push(job);
    }

    stop(): void {
        for (const job of this.jobs) {
            clearInterval(job.timer);
        }
        this.jobs = [];
        log.info(`[scheduler] all jobs stopped`);
    }
}
