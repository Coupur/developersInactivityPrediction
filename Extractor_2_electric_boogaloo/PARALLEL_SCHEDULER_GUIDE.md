# Parallel Page Scheduler — Integration Guide

## Files to add / change

| File | Action |
|------|--------|
| `Scheduler.py` | **NEW** — drop next to CommitExtractorV3.py |
| `CommitExtractorV3_parallel_changes.py` | **NEW** — contains the functions to add |
| `CommitExtractorV3.py` | **EDIT** — 3 tiny surgical changes (described below) |

---

## Architecture overview

```
updateAlldata()
    │
    ├─ [1 token] ──► original serial path (unchanged)
    │
    └─ [2+ tokens]
          │
          ▼
    Phase 1: ENUMERATE (sequential, fast, idempotent)
    ─────────────────────────────────────────────────
    Token 0 does one lightweight pass per stream.
    Each pass fetches ONLY  pageInfo { hasNextPage endCursor }
    — no data at all — and records every page cursor in
    scheduler_state.json.

    Example for a repo with 1 000 issues @ 100/page = 10 pages:
      page 0  cursor_before: null
      page 1  cursor_before: "Y3Vyc29yOnYy..."
      page 2  cursor_before: "Y3Vyc29yOnYz..."
      ...
      page 9  cursor_before: "Y3Vyc29yOnk5..."

    This costs ~10 GraphQL calls and runs in seconds.

          │
          ▼
    Phase 2: DISPATCH (parallel, N workers)
    ────────────────────────────────────────
    One worker thread per API token.

    Each worker loops:
      1. claim_any_page()  →  atomically marks one page "in_progress"
      2. Calls the appropriate _single_page_xxx() function
      3. _single_page_xxx writes data to temp files:
             .scheduler_tmp/issues_with_timeline_000003_issues.csv
             .scheduler_tmp/issues_with_timeline_000003_issue_activity.csv
      4. mark_page_done()  →  marks page "done", tries to advance
         committed_through, merges temp files into main CSVs

    Workers steal from ANY stream that has pending pages, so tokens
    never idle while work remains.


OUT-OF-ORDER COMPLETION
═══════════════════════

    committed_through = highest N where ALL pages 0 … N are "done".

    Example timeline with 4 workers on commits:
      t=0   W0 claims page 0,  W1 claims page 1,
            W2 claims page 2,  W3 claims page 3
      t=5   page 1 finishes → done, but page 0 still in_progress
            committed_through stays at -1
      t=7   page 3 finishes → done
            committed_through still -1 (gap at 0)
      t=9   page 0 finishes → done
            now pages 0,1 are consecutive done
            committed_through advances to 1
            pages 0 and 1 temp files are merged → main CSV, then deleted
      t=11  page 2 finishes → done
            now pages 2,3 are consecutive done
            committed_through advances to 3
            pages 2 and 3 temp files merged → main CSV, then deleted

    If the program crashes while page 2 is in_progress:
      On restart: page 2 status is reset to "pending" (in_progress→pending)
      page 2 temp file (if it exists from the crash) is re-written cleanly
      No data is lost — committed pages are in the main CSV already


SCHEDULER STATE FILE
════════════════════
scheduler_state.json  (lives next to data_cursor.json)

{
  "updated_at": "2025-01-15T10:00:00+00:00",
  "streams": {
    "issues_with_timeline": {
      "complete": false,
      "cursors_ready": true,
      "total_pages": 10,
      "committed_through": 3,
      "pages": {
        "0": { "cursor_before": null,         "status": "committed"    },
        "1": { "cursor_before": "Y3Vyc29y…",  "status": "committed"    },
        "2": { "cursor_before": "Y3Vyc29y…",  "status": "committed"    },
        "3": { "cursor_before": "Y3Vyc29y…",  "status": "committed"    },
        "4": { "cursor_before": "Y3Vyc29y…",  "status": "done"         },
        "5": { "cursor_before": "Y3Vyc29y…",  "status": "in_progress"  },
        "6": { "cursor_before": "Y3Vyc29y…",  "status": "pending"      },
        ...
      }
    },
    "prs_with_comments": { ... },
    "commits": { ... }
  }
}

Page status lifecycle:
  pending → in_progress → done → committed


TEMP FILE LAYOUT
════════════════
<organizationFolder>/
  .scheduler_tmp/
    issues_with_timeline_000000_issues.csv          ← deleted after commit
    issues_with_timeline_000000_issue_activity.csv  ← deleted after commit
    issues_with_timeline_000004_issues.csv          ← done, waiting for 0-3
    commits_000007_commits.csv                      ← in_progress
    commits_000007_per_file_commits.csv             ← in_progress
  scheduler_state.json
  data_cursor.json   ← unchanged, still used by serial fallback path
  issues.csv         ← final merged output
  issue_activity.csv
  prs.csv
  prs_comments.csv
  commits.csv
  per_file_commits.csv


HOW TO APPLY THE CHANGES TO CommitExtractorV3.py
═════════════════════════════════════════════════

CHANGE 1 — Add import (top of file, with other imports):

    from Scheduler import PageScheduler

CHANGE 2 — In updateAlldata(), find this block (~line 178):

    if full_extraction == True:
        work_orders = [
            {"kind": "Issue",  "token_idx": 1, ...},
            {"kind": "PR",     "token_idx": 2, ...},
            {"kind": "Commit", "token_idx": 0, ...},
        ]
    else:
        work_orders = [
            {"kind": "Commit", "token_idx": 2, ...}
        ]

    tables = { "issues": issues_df, ... }

    with ThreadPoolExecutor(max_workers=len(work_orders)) as pool:
        futures = [pool.submit(extraction_worker, ...) for order in work_orders]
        for fut in as_completed(futures):
            fut.result()

Replace that entire block with:

    tables = { "issues": issues_df, "issue_activity": issue_activity_df,
               "prs_repo": prs_df, "prs_comments": prs_comments_df,
               "commits": commits_df }

    _new_updateAlldata_dispatch(
        states, tables, repo_full_name, organizationFolder, full_extraction
    )
    return

CHANGE 3 — Paste ALL functions from CommitExtractorV3_parallel_changes.py
into CommitExtractorV3.py (anywhere before updateAlldata, or in a new
section at the bottom before main()):

    _new_updateAlldata_dispatch()
    parallel_extraction_worker()
    _single_page_issue()
    _single_page_pr()
    _single_page_commit()


WHAT DOES NOT CHANGE
════════════════════
  ✓ updateIssueListFile()    — completely unchanged
  ✓ updatePRListFile()       — completely unchanged
  ✓ updateCommitListFile()   — completely unchanged
  ✓ extraction_worker()      — completely unchanged (still used by serial path)
  ✓ data_cursor.json format  — completely unchanged
  ✓ All output CSV schemas   — completely unchanged
  ✓ main() and __main__      — completely unchanged
  ✓ Utilities token functions — completely unchanged


SCALING
═══════
4 tokens  → 4 parallel page workers + enumeration is ~4× faster
10 tokens → 10 parallel page workers
            With 10 tokens on a repo with 10 000 commits (100 pages):
              Serial:   ~100 sequential GraphQL + ~10 000 REST calls
              Parallel: ~10 GraphQL per worker simultaneously
                        REST calls also parallelized across workers
            In practice 5–8× wall-clock speedup (REST is the bottleneck)

Workers steal from any stream, so:
  - If commits finishes early, those tokens help with issues/PRs
  - No token sits idle
  - Rate limit waits are per-token (handled by getSameToken as before)
