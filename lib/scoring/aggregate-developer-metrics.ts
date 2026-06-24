/**
 * aggregate-developer-metrics.ts
 *
 * Reads the last 90 days of `pr_events` rows for a given developer from
 * Supabase and reduces them into the exact `DeveloperMetrics` shape that
 * `computeDeveloperScore` expects.
 *
 * The aggregation is performed in TypeScript rather than via SQL views so
 * the logic stays co-located with the scoring engine and is testable in
 * isolation with Vitest.
 */

import { getAdminClient } from "@/lib/supabase/admin";
import type { DeveloperMetrics } from "@/lib/scoring/scoring-types";

/** Fixed evaluation window — keep in sync with sufficiency thresholds. */
const WINDOW_DAYS = 90;

/**
 * Row shape we select from `pr_events`.
 * Only the columns needed for aggregation are typed here.
 */
interface PrEventRow {
  id: string;
  github_pt_id: number;
  event_type: string;
  state: string;
  merged_at: string | null;
  created_at: string | null;
  updated_at: string | null;
  additions: number | null;
  deletions: number | null;
  changed_files: number | null;
  review_count: number | null;
  requested_reviewers_count: number | null;
  tests_touched: boolean;
  docs_touched: boolean;
  risky_paths_hit: boolean;
}

/**
 * Error thrown when the developer has no `profiles` row or Supabase returns
 * no data at all.  Callers can catch this to return a 404.
 */
export class DeveloperNotFoundError extends Error {
  constructor(developerId: string) {
    super(`Developer ${developerId} not found in profiles table`);
    this.name = "DeveloperNotFoundError";
  }
}

/**
 * Aggregate `pr_events` for a developer over the last 90 days into the
 * `DeveloperMetrics` shape.
 *
 * @throws {DeveloperNotFoundError} if no `profiles` row exists for `developerId`
 * @throws {Error} on Supabase query failures
 */
export async function aggregateDeveloperMetrics(
  developerId: string,
): Promise<DeveloperMetrics> {
  const supabase = getAdminClient();

  // -----------------------------------------------------------------------
  // 1. Verify the developer exists — avoids computing on a stale FK.
  //    .single() returns PGRST116 when zero rows match, which we map to a
  //    precise DeveloperNotFoundError.
  // -----------------------------------------------------------------------
  const { error: profileErr } = await supabase
    .from("profiles")
    .select("id")
    .eq("id", developerId)
    .single();

  if (profileErr) {
    if (profileErr.code === "PGRST116") {
      throw new DeveloperNotFoundError(developerId);
    }
    throw new Error(
      `aggregateDeveloperMetrics: profiles lookup failed — ${profileErr.message}`,
    );
  }

  // -----------------------------------------------------------------------
  // 2. Fetch pr_events from the last WINDOW_DAYS days.
  //    Filter on `updated_at` (PR activity time) rather than `ingested_at`
  //    (webhook processing time) so late webhook deliveries are not excluded
  //    from the window when the underlying PR activity was within 90 days.
  // -----------------------------------------------------------------------
  const windowStart = new Date(Date.now() - WINDOW_DAYS * 24 * 60 * 60 * 1000);

  const { data: rows, error: queryError } = await supabase
    .from("pr_events")
    .select(
      "id, github_pt_id, event_type, state, merged_at, created_at, updated_at, " +
        "additions, deletions, changed_files, review_count, " +
        "requested_reviewers_count, tests_touched, docs_touched, risky_paths_hit",
    )
    .eq("developer_id", developerId)
    .gte("updated_at", windowStart.toISOString())
    .returns<PrEventRow[]>();

  if (queryError) {
    throw new Error(
      `aggregateDeveloperMetrics: pr_events query failed — ${queryError.message}`,
    );
  }

  const events = rows ?? [];

  // -----------------------------------------------------------------------
  // 3. Reduce into DeveloperMetrics.
  // -----------------------------------------------------------------------
  // Each distinct PR can appear as multiple event rows (opened, edited,
  // synchronized, reviewed, merged, closed).  Instead of keeping only the
  // latest row (which can lose merge/review state from earlier events), we
  // accumulate per-PR state across ALL its event rows so that merge, review,
  // hygiene, and churn signals are never discarded.
  interface PrAccumulator {
    github_pt_id: number;
    is_merged: boolean;
    is_closed: boolean;
    has_review_requested: boolean;
    has_review_received: boolean;
    risky_paths_hit: boolean;
    tests_touched: boolean;
    docs_touched: boolean;
    additions: number | null;
    deletions: number | null;
    changed_files: number | null;
    latestTimestamp: number | null;
    activeWeeks: Set<string>;
  }

  const prsByGitHubId = new Map<number, PrAccumulator>();

  for (const row of events) {
    let pr = prsByGitHubId.get(row.github_pt_id);
    if (!pr) {
      pr = {
        github_pt_id: row.github_pt_id,
        is_merged: false,
        is_closed: false,
        has_review_requested: false,
        has_review_received: false,
        risky_paths_hit: false,
        tests_touched: false,
        docs_touched: false,
        additions: null,
        deletions: null,
        changed_files: null,
        latestTimestamp: null,
        activeWeeks: new Set<string>(),
      };
      prsByGitHubId.set(row.github_pt_id, pr);
    }

    // Merge / close state — accumulate across all events for this PR.
    // A "merged" event or any row with merged_at set means the PR was merged.
    if (row.event_type === "merged" || row.merged_at !== null) {
      pr.is_merged = true;
    }
    // A "closed" event without a merge means closed-without-merge.
    if (row.event_type === "closed" && row.merged_at === null) {
      pr.is_closed = true;
    }

    // Review participation — accumulate across all events.
    if ((row.requested_reviewers_count ?? 0) > 0) {
      pr.has_review_requested = true;
    }
    if ((row.review_count ?? 0) > 0) {
      pr.has_review_received = true;
    }

    // Hygiene — a PR is flagged if ANY event row indicates it.
    if (row.risky_paths_hit) {
      pr.risky_paths_hit = true;
    }
    if (row.tests_touched) {
      pr.tests_touched = true;
    }
    if (row.docs_touched) {
      pr.docs_touched = true;
    }

    // Churn — prefer the row with non-null diff stats; if a later
    // synchronized event has null stats we keep the earlier values.
    if (row.additions !== null) pr.additions = row.additions;
    if (row.deletions !== null) pr.deletions = row.deletions;
    if (row.changed_files !== null) pr.changed_files = row.changed_files;

    // Recency — track the latest timestamp across all events for this PR.
    const ts = row.updated_at ?? row.created_at;
    if (ts) {
      const parsed = Date.parse(ts);
      if (!Number.isNaN(parsed)) {
        if (pr.latestTimestamp === null || parsed > pr.latestTimestamp) {
          pr.latestTimestamp = parsed;
        }
        // ISO week key: "YYYY-W##"
        const date = new Date(parsed);
        const weekKey = getIsoWeekKey(date);
        pr.activeWeeks.add(weekKey);
      }
    }
  }

  const distinctPrs = Array.from(prsByGitHubId.values());

  // --- PR counts ---
  const total_prs = distinctPrs.length;

  let merged_prs = 0;
  let closed_without_merge = 0;
  let prs_with_review_requested = 0;
  let prs_with_review_received = 0;

  // --- hygiene ---
  let total_rule_violations = 0;
  let risky_paths_prs = 0;
  let prs_with_tests_touched = 0;
  let prs_with_docs_touched = 0;

  // --- churn (averaged over PRs that have non-null additions) ---
  let additionsSum = 0;
  let deletionsSum = 0;
  let changedFilesSum = 0;
  let churnPrCount = 0;

  // --- recency ---
  let lastPrTimestamp: number | null = null;

  // --- consistency (distinct ISO weeks) ---
  const activeWeeksSet = new Set<string>();

  for (const pr of distinctPrs) {
    // Merge / close state — accumulated across all events.
    if (pr.is_merged) {
      merged_prs++;
    } else if (pr.is_closed) {
      closed_without_merge++;
    }

    // Review participation
    if (pr.has_review_requested) {
      prs_with_review_requested++;
    }
    if (pr.has_review_received) {
      prs_with_review_received++;
    }

    // Hygiene
    if (pr.risky_paths_hit) {
      risky_paths_prs++;
      total_rule_violations++; // 1 violation per PR that hit risky paths
    }
    if (pr.tests_touched) {
      prs_with_tests_touched++;
    }
    if (pr.docs_touched) {
      prs_with_docs_touched++;
    }

    // Churn — only count PRs that actually have diff stats
    if (
      pr.additions !== null ||
      pr.deletions !== null ||
      pr.changed_files !== null
    ) {
      additionsSum += pr.additions ?? 0;
      deletionsSum += pr.deletions ?? 0;
      changedFilesSum += pr.changed_files ?? 0;
      churnPrCount++;
    }

    // Recency — use the latest timestamp across all events for this PR.
    if (pr.latestTimestamp !== null) {
      if (lastPrTimestamp === null || pr.latestTimestamp > lastPrTimestamp) {
        lastPrTimestamp = pr.latestTimestamp;
      }
      // Aggregate active weeks across all PRs.
      for (const weekKey of pr.activeWeeks) {
        activeWeeksSet.add(weekKey);
      }
    }
  }

  // --- days_since_last_pr ---
  let days_since_last_pr: number | null = null;
  if (lastPrTimestamp !== null) {
    days_since_last_pr = Math.floor(
      (Date.now() - lastPrTimestamp) / (24 * 60 * 60 * 1000),
    );
  }

  // --- averages ---
  const avg_additions = churnPrCount > 0 ? additionsSum / churnPrCount : 0;
  const avg_deletions = churnPrCount > 0 ? deletionsSum / churnPrCount : 0;
  const avg_changed_files = churnPrCount > 0 ? changedFilesSum / churnPrCount : 0;

  return {
    developer_id: developerId,
    window_days: WINDOW_DAYS,
    total_prs,
    merged_prs,
    closed_without_merge,
    prs_with_review_requested,
    prs_with_review_received,
    total_rule_violations,
    risky_paths_prs,
    prs_with_tests_touched,
    prs_with_docs_touched,
    avg_additions: Math.round(avg_additions * 100) / 100,
    avg_deletions: Math.round(avg_deletions * 100) / 100,
    avg_changed_files: Math.round(avg_changed_files * 100) / 100,
    days_since_last_pr,
    active_weeks: activeWeeksSet.size,
  };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Returns a stable ISO week key "YYYY-W##" for a given date.
 * Uses UTC to avoid timezone-dependent week boundaries.
 */
function getIsoWeekKey(date: Date): string {
  const d = new Date(
    Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()),
  );
  // Set to nearest Thursday: current date + 3 - current day as ISO day (Mon=1..Sun=7)
  const dayNum = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const weekNum = Math.ceil(
    ((d.getTime() - yearStart.getTime()) / 86_400_000 + 1) / 7,
  );
  return `${d.getUTCFullYear()}-W${String(weekNum).padStart(2, "0")}`;
}
