/**
 * pr-events-worker.ts
 *
 * BullMQ worker that consumes jobs from the `pr-events` queue.
 *
 * Normalization (GitHub API enrichment, developer resolution, path analysis)
 * is performed at the webhook/API route before enqueuing, so jobs arrive
 * already normalized as `PrEventJob` payloads.
 *
 * Pipeline per job:
 *   1. Persist the pre-normalized event into the `pr_events` table
 *      (idempotent — skips on duplicate idempotency_key / delivery_id)
 *   2. If a developer_id was resolved, recompute and persist their score
 *      via saveDeveloperScore()
 *
 * Run this as a long-lived process outside Next.js:
 *
 *   npx tsx lib/queue/pr-events-worker.ts
 *
 * or as a Cloudflare Queue consumer / separate Node service in production.
 */
 
import { Worker, type Job } from "bullmq";
import { getAdminClient } from "@/lib/supabase/admin";
import { getRedisConnectionOptions } from "@/lib/redis/client";
import {
  PR_EVENTS_QUEUE_NAME,
  type PrEventJob,
} from "@/lib/github/pr-event-types";
import { saveDeveloperScore } from "@/lib/scoring/save-developer-score";
 
export const runtime = "nodejs";
 
// ---------------------------------------------------------------------------
// pr_events persistence
// ---------------------------------------------------------------------------
 
/** Row shape for insertion - maps PrEventJob 1:1 to the pr_events table. */
interface PrEventInsertRow {
  developer_id: string | null;
  repo_name: string;
  pr_number: number;
  github_pt_id: number;
  event_type: string;
  title: string;
  body: string | null;
  author_username: string;
  base_branch: string;
  head_branch: string;
  head_sha: string;
  additions: number | null;
  deletions: number | null;
  changed_files: number | null;
  commits_count: number | null;
  review_count: number | null;
  requested_reviewers_count: number | null;
  labels_json: unknown;
  tests_touched: boolean;
  docs_touched: boolean;
  risky_paths_hit: boolean;
  state: string;
  merged_at: string | null;
  created_at: string | null;
  updated_at: string | null;
  ingested_at: string;
  delivery_id: string;
  idempotency_key: string;
  github_event: string;
  github_action: string;
}
 
/**
 * Persist a normalized PR event to the `pr_events` table.
 *
 * Idempotent: if a row with the same `idempotency_key` or `delivery_id`
 * already exists (unique constraint), Supabase returns a 409 / unique
 * violation which we treat as "already processed" and return silently.
 */
async function persistPrEvent(job: PrEventJob): Promise<"inserted" | "duplicate"> {
  const supabase = getAdminClient();
 
  const row: PrEventInsertRow = {
    developer_id: job.developer_id,
    repo_name: job.repo_name,
    pr_number: job.pr_number,
    github_pt_id: job.github_pt_id,
    event_type: job.event_type,
    title: job.title,
    body: job.body,
    author_username: job.author_username,
    base_branch: job.base_branch,
    head_branch: job.head_branch,
    head_sha: job.head_sha,
    additions: job.additions,
    deletions: job.deletions,
    changed_files: job.changed_files,
    commits_count: job.commits_count,
    review_count: job.review_count,
    requested_reviewers_count: job.requested_reviewers_count,
    labels_json: job.labels_json ? JSON.parse(job.labels_json) : null,
    tests_touched: job.tests_touched,
    docs_touched: job.docs_touched,
    risky_paths_hit: job.risky_paths_hit,
    state: job.state,
    merged_at: job.merged_at,
    created_at: job.created_at,
    updated_at: job.updated_at,
    ingested_at: job.ingested_at,
    delivery_id: job.delivery_id,
    idempotency_key: job.idempotency_key,
    github_event: job.github_event,
    github_action: job.github_action,
  };
 
  const { error } = await supabase.from("pr_events").insert(row);
 
  if (error) {
    // Postgres unique violation code - row already processed
    if (error.code === "23505") {
      return "duplicate";
    }
    throw new Error(`persistPrEvent failed: ${error.message} (code ${error.code})`);
  }
 
  return "inserted";
}
 
// ---------------------------------------------------------------------------
// Job processor
// ---------------------------------------------------------------------------
 
async function processPrEventJob(job: Job<PrEventJob>): Promise<void> {
  const prEvent = job.data;
 
  // 1. Persist to pr_events (idempotent)
  const status = await persistPrEvent(prEvent);
 
  if (status === "duplicate") {
    console.log(
      `[pr-events-worker] duplicate skipped - delivery_id=${prEvent.delivery_id}`,
    );
    return;
  }
 
  console.log(
    `[pr-events-worker] persisted pr_event - ` +
      `repo=${prEvent.repo_name} pr=${prEvent.pr_number} ` +
      `type=${prEvent.event_type}`,
  );
 
  // 2. Recompute developer score if the PR author is a registered developer
  if (prEvent.developer_id) {
    const result = await saveDeveloperScore(prEvent.developer_id);
 
    if (result.success) {
      console.log(
        `[pr-events-worker] score updated - ` +
          `developer=${prEvent.developer_id} ` +
          `score=${result.score.score_total}/100 ` +
          `sufficient=${result.score.sufficient_data}`,
      );
    } else {
      console.error(
        `[pr-events-worker] score update failed - ` +
          `developer=${prEvent.developer_id} error=${result.error}`,
      );
    }
  } else {
    console.log(
      `[pr-events-worker] author ${prEvent.author_username} ` +
        "not linked to a profile - skipping score update",
    );
  }
}
 
// ---------------------------------------------------------------------------
// Worker lifecycle
// ---------------------------------------------------------------------------
 
let worker: Worker<PrEventJob> | null = null;
 
/**
 * Start the BullMQ worker that consumes `pr-events` jobs.
 *
 * @param concurrency Number of jobs to process in parallel (default 5).
 * @returns The Worker instance so callers can attach event listeners.
 */
export function startPrEventsWorker(concurrency = 5): Worker<PrEventJob> {
  if (worker) {
    return worker;
  }
 
  worker = new Worker<PrEventJob>(
    PR_EVENTS_QUEUE_NAME,
    async (job: Job<PrEventJob>) => {
      await processPrEventJob(job);
    },
    {
      connection: getRedisConnectionOptions(),
      concurrency,
    },
  );
 
  worker.on("completed", (job: Job<PrEventJob>) => {
    console.debug(`[pr-events-worker] job completed - id=${job.id}`);
  });
 
  worker.on("failed", (job: Job<PrEventJob> | undefined, err: Error) => {
    console.error(
      `[pr-events-worker] job failed - id=${job?.id} attempts=${job?.attemptsMade}`,
      err,
    );
  });
 
  worker.on("error", (err: Error) => {
    console.error("[pr-events-worker] worker error:", err);
  });
 
  console.log(
    `[pr-events-worker] started - queue=${PR_EVENTS_QUEUE_NAME} concurrency=${concurrency}`,
  );
 
  return worker;
}
 
/** Gracefully shut down the worker. */
export async function stopPrEventsWorker(): Promise<void> {
  if (worker) {
    await worker.close();
    worker = null;
    console.log("[pr-events-worker] stopped");
  }
}
 
// ---------------------------------------------------------------------------
// CLI entrypoint - run directly with `npx tsx lib/queue/pr-events-worker.ts`
// Uses an ESM-safe check instead of CommonJS `require.main`.
// ---------------------------------------------------------------------------
 
const isMainModule =
  typeof process !== "undefined" &&
  process.argv[1] &&
  import.meta.url.endsWith(process.argv[1].replace(/\\/g, "/"));
 
if (isMainModule) {
  const concurrency = Number(process.env.PR_EVENTS_WORKER_CONCURRENCY) || 5;
  startPrEventsWorker(concurrency);
 
  const shutdown = async (signal: string) => {
    console.log(`[pr-events-worker] received ${signal}, shutting down...`);
    await stopPrEventsWorker();
    process.exit(0);
  };
 
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
  process.on("SIGINT", () => void shutdown("SIGINT"));
}
