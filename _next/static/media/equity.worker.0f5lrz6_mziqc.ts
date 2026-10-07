/**
 * Web Worker wrapper for the equity engine. Runs Monte Carlo in chunks,
 * posting progressive results, and abandons a job as soon as a newer one
 * arrives (rapid card entry supersedes in-flight runs).
 */

import type { EquityOptions, EquityResult, PlayerEquity } from './equity';
import { DEFAULT_ITERATIONS, calculateEquity } from './equity';

export interface EquityJobRequest {
  id: number;
  options: EquityOptions;
}

export interface EquityJobUpdate {
  id: number;
  result: EquityResult;
  done: boolean;
}

const CHUNK_ITERATIONS = 2500;

let currentJobId = 0;

function mergeResults(a: EquityResult, b: EquityResult): EquityResult {
  const total = a.samples + b.samples;
  const wa = a.samples / total;
  const wb = b.samples / total;
  const players: PlayerEquity[] = a.players.map((pa, i) => {
    const pb = b.players[i];
    return {
      perBoard: pa.perBoard.map((ba, bIdx) => {
        const bb = pb.perBoard[bIdx];
        return {
          equity: ba.equity * wa + bb.equity * wb,
          winPct: ba.winPct * wa + bb.winPct * wb,
          tiePct: ba.tiePct * wa + bb.tiePct * wb,
        };
      }),
      combined: pa.combined * wa + pb.combined * wb,
      scoopPct: pa.scoopPct * wa + pb.scoopPct * wb,
    };
  });
  return { players, method: a.method, samples: total };
}

const yieldToQueue = () => new Promise<void>((r) => setTimeout(r, 0));

async function runJob(job: EquityJobRequest): Promise<void> {
  const { id, options } = job;
  const target = options.iterations ?? DEFAULT_ITERATIONS;
  const baseSeed = options.seed ?? (Math.random() * 2 ** 32) >>> 0;

  // First chunk also tells us whether the engine chose exact enumeration.
  let merged = calculateEquity({
    ...options,
    iterations: Math.min(CHUNK_ITERATIONS, target),
    seed: baseSeed,
  });

  if (merged.method === 'exact') {
    postMessage({ id, result: merged, done: true } satisfies EquityJobUpdate);
    return;
  }

  let chunk = 1;
  while (merged.samples < target) {
    postMessage({ id, result: merged, done: false } satisfies EquityJobUpdate);
    await yieldToQueue(); // lets a newer job's message land and supersede us
    if (currentJobId !== id) return;

    const next = calculateEquity({
      ...options,
      iterations: Math.min(CHUNK_ITERATIONS, target - merged.samples),
      seed: (baseSeed + chunk * 0x9e3779b9) >>> 0,
    });
    merged = mergeResults(merged, next);
    chunk++;
  }

  postMessage({ id, result: merged, done: true } satisfies EquityJobUpdate);
}

onmessage = (e: MessageEvent<EquityJobRequest>) => {
  currentJobId = e.data.id;
  void runJob(e.data);
};
