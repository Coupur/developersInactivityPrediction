"""
Scheduler.py  —  Parallel page-level scheduler for GitHub data extraction.

Architecture
------------
Phase 1  ENUMERATE
    A single lightweight sequential pass per stream that only fetches
    `pageInfo { hasNextPage endCursor }`.  No data is collected.  The result is
    a list of cursors stored in scheduler_state.json.  This pass is cheap
    (1 API call per page) and idempotent — if the program dies mid-enumeration
    it resumes from the last recorded page.

Phase 2  DISPATCH
    N worker threads (one per API token) each repeatedly:
        1.  claim_next_page() from any incomplete stream (thread-safe)
        2.  call the appropriate updateXxxListFile() in single-page mode
        3.  mark_page_done() which:
            a.  writes temp file to disk
            b.  advances committed_through (merges consecutive done pages
                into the main CSV in order)

Out-of-order completion
    Each page is written to   .scheduler_tmp/<stream>_<page_idx:06d>_<type>.csv
    committed_through = highest N such that pages 0 … N are ALL done.
    When committed_through advances, temp files for those pages are appended
    to the main CSV in order and then deleted.
    If page 51 finishes before page 50, it sits as "done" until 50 also
    finishes — then both advance committed_through together.

Restart safety
    On startup any page whose status is "in_progress" is reset to "pending"
    (the worker died without finishing).  "committed" pages are skipped
    entirely.  Temp files for "done" pages that survived a crash are kept
    and will be merged once committed_through catches up.

State file:  <organizationFolder>/scheduler_state.json
Temp dir:    <organizationFolder>/.scheduler_tmp/
"""

import csv
import json
import shutil
import threading
from datetime import datetime, timezone
from pathlib import Path

import Settings as cfg


# ---------------------------------------------------------------------------
#  Per-stream configuration
# ---------------------------------------------------------------------------

#  Maps stream_name -> list of (data_type, cfg attribute for the main CSV)
#  Order matters: first data_type is the "primary" (issues, prs, commits).
STREAM_OUTPUTS = {
    "issues_with_timeline": [
        ("issues",         "issue_list_file_name"),
        ("issue_activity", "issue_activity_file_name"),
    ],
    "prs_with_comments": [
        ("prs",          "PR_list_file_name"),
        ("prs_comments", "prs_comments_csv"),
    ],
    "commits": [
        ("commits",          "commit_list_file_name"),
        ("per_file_commits", "per_file_commits_path"),
    ],
}

# Lightweight pageInfo-only queries (one per stream) used during enumeration.
# These are separate from the full data queries in CommitExtractorV3 so the
# update functions do not need to be changed.
ENUM_QUERIES = {
    "issues_with_timeline": (
        """
        query($owner:String!, $name:String!, $first:Int!, $after:String) {
          repository(owner:$owner, name:$name) {
            issues(states:[OPEN, CLOSED], first:$first, after:$after,
                   orderBy:{field:CREATED_AT, direction:ASC}) {
              pageInfo { hasNextPage endCursor }
            }
          }
        }
        """,
        ("data", "repository", "issues"),
    ),
    "prs_with_comments": (
        """
        query($owner:String!, $name:String!, $first:Int!, $after:String) {
          repository(owner:$owner, name:$name) {
            pullRequests(states:[OPEN, MERGED, CLOSED], first:$first, after:$after,
                         orderBy:{field:CREATED_AT, direction:ASC}) {
              pageInfo { hasNextPage endCursor }
            }
          }
        }
        """,
        ("data", "repository", "pullRequests"),
    ),
    "commits": (
        """
        query($owner:String!, $name:String!, $first:Int!, $after:String) {
          repository(owner:$owner, name:$name) {
            defaultBranchRef {
              target {
                ... on Commit {
                  history(first:$first, after:$after) {
                    pageInfo { hasNextPage endCursor }
                  }
                }
              }
            }
          }
        }
        """,
        ("data", "repository", "defaultBranchRef", "target", "history"),
    ),
}


# ---------------------------------------------------------------------------
#  PageScheduler
# ---------------------------------------------------------------------------

class PageScheduler:
    STATE_FILE = "scheduler_state.json"
    TMP_DIR    = ".scheduler_tmp"

    def __init__(self, folder: Path):
        self.folder   = Path(folder)
        self.tmp_dir  = self.folder / self.TMP_DIR
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.folder / self.STATE_FILE
        self._lock    = threading.Lock()
        self.state    = self._load_or_init()

    # ------------------------------------------------------------------
    #  State management
    # ------------------------------------------------------------------

    @staticmethod
    def _empty_stream():
        return {
            "complete":         False,
            "cursors_ready":    False,
            "total_pages":      0,
            "committed_through": -1,   # highest page whose data is in the main CSV
            "pages":            {}     # str(page_idx) -> {cursor_before, status}
        }

    def _load_or_init(self) -> dict:
        if self.state_path.exists():
            with open(self.state_path, "r", encoding="utf-8") as fh:
                state = json.load(fh)
            # Heal any in_progress pages left over from a crashed run
            changed = False
            for stream in state["streams"].values():
                for page in stream["pages"].values():
                    if page["status"] == "in_progress":
                        page["status"] = "pending"
                        changed = True
            if changed:
                self._save_unlocked(state)
            return state

        state = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "streams": {name: self._empty_stream() for name in STREAM_OUTPUTS},
        }
        self._save_unlocked(state)
        return state

    def _save_unlocked(self, state=None):
        """Write state to disk WITHOUT acquiring the lock (caller must hold it)."""
        if state is None:
            state = self.state
        state["updated_at"] = datetime.now(timezone.utc).isoformat()
        with open(self.state_path, "w", encoding="utf-8") as fh:
            json.dump(state, fh, indent=2, default=str)

    def _save(self):
        """Write state to disk, acquiring the lock."""
        with self._lock:
            self._save_unlocked()

    # ------------------------------------------------------------------
    #  Phase 1 — cursor enumeration
    # ------------------------------------------------------------------

    def enumerate_cursors(self, stream_name: str, requester, owner: str, name: str):
        """
        Fast sequential pass: only fetches pageInfo, records all page cursors.
        Safe to call multiple times (idempotent).
        """
        stream = self.state["streams"][stream_name]
        if stream["cursors_ready"]:
            print(f"[Scheduler] {stream_name}: cursors already ready "
                  f"({stream['total_pages']} pages)")
            return

        query, connection_path = ENUM_QUERIES[stream_name]
        per_page = cfg.items_per_page

        print(f"[Scheduler] {stream_name}: enumerating page cursors …")
        after    = None
        page_idx = 0

        # If we crashed mid-enumeration, resume from where we left off.
        # Find the last page whose cursor_before is recorded.
        existing = stream["pages"]
        if existing:
            last_recorded = max(int(k) for k in existing)
            # Advance after to the cursor that gets us PAST the last recorded page.
            # We stored cursor_before, so to resume we need the endCursor of that page.
            # Re-run the last recorded page to get its endCursor cheaply.
            last_page = existing[str(last_recorded)]
            if last_page["status"] not in ("done", "committed"):
                # This page was only partially enumerated; re-enumerate from its cursor
                after    = last_page["cursor_before"]
                page_idx = last_recorded
            else:
                # All good — we need the endCursor of the last page to continue.
                # We don't store endCursor, so re-run that one query.
                _, data = requester.graphql_query(
                    query=query,
                    variables={"owner": owner, "name": name,
                               "first": per_page, "after": last_page["cursor_before"]}
                )
                conn = _walk(data, connection_path)
                pi   = conn.get("pageInfo", {})
                if not pi.get("hasNextPage"):
                    # Already done
                    stream["cursors_ready"] = True
                    stream["total_pages"]   = last_recorded + 1
                    self._save()
                    return
                after    = pi["endCursor"]
                page_idx = last_recorded + 1

        while True:
            variables = {"owner": owner, "name": name, "first": per_page, "after": after}
            _, data   = requester.graphql_query(query=query, variables=variables)
            conn      = _walk(data, connection_path)
            page_info = conn.get("pageInfo", {})

            idx_str = str(page_idx)
            if idx_str not in stream["pages"]:
                stream["pages"][idx_str] = {
                    "cursor_before": after,   # pass this as `after` to fetch this page
                    "status":        "pending"
                }
                # Save incrementally so a crash loses at most one page
                self._save()

            if not page_info.get("hasNextPage"):
                break

            after     = page_info["endCursor"]
            page_idx += 1

        stream["total_pages"]  = page_idx + 1
        stream["cursors_ready"] = True
        self._save()
        print(f"[Scheduler] {stream_name}: {stream['total_pages']} pages enumerated")

    # ------------------------------------------------------------------
    #  Phase 2 — work dispatch
    # ------------------------------------------------------------------

    def claim_next_page(self, stream_name: str):
        """
        Thread-safe.  Atomically marks the next pending page as in_progress.
        Returns (page_idx, cursor_before) or None if nothing to do.
        """
        with self._lock:
            stream = self.state["streams"][stream_name]
            if stream["complete"] or not stream["cursors_ready"]:
                return None
            for idx_str, page in stream["pages"].items():
                if page["status"] == "pending":
                    page["status"] = "in_progress"
                    self._save_unlocked()
                    return int(idx_str), page["cursor_before"]
            return None

    def claim_any_page(self, preferred_order=None):
        """
        Try each stream in turn and return the first available page.
        Returns (stream_name, page_idx, cursor_before) or None.
        preferred_order: list of stream names; defaults to all streams.
        """
        if preferred_order is None:
            preferred_order = list(STREAM_OUTPUTS.keys())
        for sn in preferred_order:
            result = self.claim_next_page(sn)
            if result is not None:
                page_idx, cursor = result
                return sn, page_idx, cursor
        return None

    # ------------------------------------------------------------------
    #  Temp file paths
    # ------------------------------------------------------------------

    def temp_path(self, stream_name: str, page_idx: int, data_type: str) -> Path:
        """
        Returns the path to the temp CSV for a specific (stream, page, data_type).
        e.g.  .scheduler_tmp/issues_with_timeline_000042_issues.csv
        """
        return self.tmp_dir / f"{stream_name}_{page_idx:06d}_{data_type}.csv"

    # ------------------------------------------------------------------
    #  Completion tracking
    # ------------------------------------------------------------------

    def mark_page_done(self, stream_name: str, page_idx: int):
        """
        Mark a page as done and try to advance committed_through.
        This is called by a worker after it has written all temp files for
        the page and is ready to hand them off to the merger.
        """
        with self._lock:
            stream = self.state["streams"][stream_name]
            page   = stream["pages"].get(str(page_idx))
            if page is None:
                return
            page["status"] = "done"
            self._try_advance_commit(stream, stream_name)
            self._save_unlocked()

    def mark_page_failed(self, stream_name: str, page_idx: int):
        """Reset a failed page back to pending so it can be retried."""
        with self._lock:
            stream = self.state["streams"][stream_name]
            page   = stream["pages"].get(str(page_idx))
            if page and page["status"] == "in_progress":
                page["status"] = "pending"
                self._save_unlocked()

    def _try_advance_commit(self, stream: dict, stream_name: str):
        """
        Inner method (caller must hold self._lock).
        Finds the longest consecutive run of done pages starting from
        committed_through + 1, merges their temp files, and updates the pointer.
        """
        ct = stream["committed_through"]
        to_commit = []

        while True:
            nxt = ct + 1
            if nxt >= stream["total_pages"]:
                break
            page = stream["pages"].get(str(nxt))
            if page and page["status"] == "done":
                to_commit.append(nxt)
                ct = nxt
            else:
                break  # gap — cannot advance further

        if not to_commit:
            return

        # Merge temp files -> main CSVs in ascending page order
        for data_type, cfg_attr in STREAM_OUTPUTS[stream_name]:
            main_csv = self.folder / getattr(cfg, cfg_attr)
            for pidx in to_commit:
                tmp = self.temp_path(stream_name, pidx, data_type)
                if tmp.exists():
                    _append_tmp_to_main(tmp, main_csv, sep=cfg.CSV_separator)
                    tmp.unlink()   # free disk space

        # Mark pages as committed
        for pidx in to_commit:
            stream["pages"][str(pidx)]["status"] = "committed"
        stream["committed_through"] = ct

        if ct == stream["total_pages"] - 1:
            stream["complete"] = True
            print(f"[Scheduler] {stream_name}: ✓ COMPLETE")

    # ------------------------------------------------------------------
    #  Convenience queries
    # ------------------------------------------------------------------

    def is_complete(self, stream_name: str) -> bool:
        return self.state["streams"][stream_name]["complete"]

    def all_complete(self) -> bool:
        return all(s["complete"] for s in self.state["streams"].values())

    def progress_summary(self) -> str:
        lines = []
        for sn, stream in self.state["streams"].items():
            tp   = stream["total_pages"]
            ct   = stream["committed_through"]
            done = sum(1 for p in stream["pages"].values() if p["status"] in ("done", "committed"))
            lines.append(f"  {sn:30s}  committed={ct+1}/{tp}  done={done}/{tp}  "
                         f"{'✓' if stream['complete'] else '…'}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
#  Helper utilities
# ---------------------------------------------------------------------------

def _walk(data: dict, path: tuple) -> dict:
    """Walk a nested dict along `path`, returning {} on any missing key."""
    node = data
    for key in path:
        if not isinstance(node, dict):
            return {}
        node = node.get(key) or {}
    return node


def _append_tmp_to_main(tmp_path: Path, main_path: Path, sep: str = ","):
    """
    Append all data rows from tmp_path to main_path.
    Skips the header row from tmp_path.
    Creates main_path with header if it does not exist.
    Thread-safe via portalocker if available.
    """
    if not tmp_path.exists():
        return

    if not main_path.exists():
        shutil.copy(tmp_path, main_path)
        return

    try:
        import portalocker  # type: ignore
        lock_ctx = portalocker.Lock(str(main_path), timeout=60)
    except ImportError:
        from contextlib import nullcontext
        lock_ctx = nullcontext()

    with open(tmp_path, "r", newline="", encoding="utf-8") as src, \
         lock_ctx:
        reader = csv.reader(src, delimiter=sep)
        with open(main_path, "a", newline="", encoding="utf-8") as dst:
            writer = csv.writer(dst, delimiter=sep)
            for i, row in enumerate(reader):
                if i == 0:      # skip header row from temp file
                    continue
                writer.writerow(row)
