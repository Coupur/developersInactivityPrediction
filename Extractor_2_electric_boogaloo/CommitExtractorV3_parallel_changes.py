"""
CommitExtractorV3_parallel_changes.py
======================================
This file shows ONLY the sections of CommitExtractorV3.py that need to change
to support the parallel page-level scheduler.  Everything else stays identical.

SUMMARY OF CHANGES
------------------
1.  Add `from Scheduler import PageScheduler` to imports.
2.  Replace the `work_orders` + `ThreadPoolExecutor` block in `updateAlldata`
    with the new parallel scheduler path.
3.  Add `parallel_extraction_worker()` — the new thread body.
4.  Add `_single_page_issue()`, `_single_page_pr()`, `_single_page_commit()`
    — thin wrappers that reuse the existing functions' inner logic for
    exactly one page.
5.  The three `updateIssueListFile` / `updatePRListFile` / `updateCommitListFile`
    functions keep their existing signatures and bodies 100% unchanged.
    They still work for the fallback serial path.

SEARCH-AND-REPLACE INSTRUCTIONS
---------------------------------
A.  At the top of CommitExtractorV3.py, add to the imports block:

        from Scheduler import PageScheduler

B.  In `updateAlldata`, replace ONLY this block (lines ~178-197):

        if full_extraction == True:
            work_orders = [
                {"kind": "Issue",  "token_idx": 1, "state": states["streams"]["issues_with_timeline"]},
                {"kind": "PR",     "token_idx": 2, "state": states["streams"]["prs_with_comments"]},
                {"kind": "Commit", "token_idx": 0, "state": states["streams"]["commits"]},
            ]
        else:
            work_orders = [
                {"kind": "Commit", "token_idx": 2, "state": states["streams"]["commits"]}
            ]

        tables = { ... }

        with ThreadPoolExecutor(max_workers=len(work_orders)) as pool:
            futures = [pool.submit(extraction_worker, order, tables, repo_full_name, organizationFolder)
                    for order in work_orders]
            for fut in as_completed(futures):
                fut.result()

    WITH the new block shown below (section ── A ──).

C.  Keep `extraction_worker` as-is (the fallback serial path still calls it).
    Add the new functions shown in sections ── B ── through ── E ──.
"""

# ══════════════════════════════════════════════════════════════════════════════
# ── A ──  NEW BLOCK for updateAlldata  (replaces the work_orders section)
# ══════════════════════════════════════════════════════════════════════════════
#
# Paste this in place of the old if/else work_orders + ThreadPoolExecutor block.
# It sits at the bottom of updateAlldata(), right where `tables = {…}` was.

def _new_updateAlldata_dispatch(states, tables, repo_full_name, organizationFolder, full_extraction):
    """
    Drop-in replacement for the work_orders + ThreadPoolExecutor section
    inside updateAlldata().

    Call it like:
        _new_updateAlldata_dispatch(states, tables, repo_full_name,
                                    organizationFolder, full_extraction)
    """
    from Scheduler import PageScheduler
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import Utilities as util

    tokensList, _ = util.getTokensList()
    n_tokens = len(tokensList)

    # ----------------------------------------------------------------
    #  Decide which streams need to run
    # ----------------------------------------------------------------
    streams_needed = []
    if full_extraction:
        for sn in ["issues_with_timeline", "prs_with_comments", "commits"]:
            if not states["streams"][sn].get("complete", False):
                streams_needed.append(sn)
    else:
        if not states["streams"]["commits"].get("complete", False):
            streams_needed.append("commits")

    if not streams_needed:
        print("[dispatch] All streams already complete — nothing to do.")
        return

    # ----------------------------------------------------------------
    #  If only 1 token, fall back to the original serial path
    # ----------------------------------------------------------------
    if n_tokens <= 1:
        print("[dispatch] Single token — using serial (original) mode.")
        if full_extraction:
            work_orders = [
                {"kind": "Issue",  "token_idx": 0, "state": states["streams"]["issues_with_timeline"]},
                {"kind": "PR",     "token_idx": 0, "state": states["streams"]["prs_with_comments"]},
                {"kind": "Commit", "token_idx": 0, "state": states["streams"]["commits"]},
            ]
        else:
            work_orders = [{"kind": "Commit", "token_idx": 0, "state": states["streams"]["commits"]}]

        with ThreadPoolExecutor(max_workers=len(work_orders)) as pool:
            futures = [pool.submit(extraction_worker, order, tables, repo_full_name, organizationFolder)
                       for order in work_orders]
            for fut in as_completed(futures):
                fut.result()
        return

    # ----------------------------------------------------------------
    #  Multi-token parallel path
    # ----------------------------------------------------------------
    scheduler = PageScheduler(organizationFolder)

    # Phase 1: enumerate cursors (sequential, lightweight, idempotent)
    # Use token 0 for enumeration — it's just pageInfo queries.
    token0   = util.getSpisificToken(0)
    from github import Github
    g0       = Github(token0)
    g0.per_page = cfg.items_per_page
    repo_obj  = g0.get_repo(repo_full_name)
    owner, name = repo_obj.owner.login, repo_obj.name
    requester = getattr(repo_obj, "requester", None) or getattr(repo_obj, "_requester", None)

    for sn in streams_needed:
        if not scheduler.state["streams"][sn]["cursors_ready"]:
            scheduler.enumerate_cursors(sn, requester, owner, name)

    print(f"[dispatch] Cursor enumeration done.\n{scheduler.progress_summary()}")

    # Phase 2: parallel page dispatch — one worker thread per token
    with ThreadPoolExecutor(max_workers=n_tokens) as pool:
        futures = [
            pool.submit(parallel_extraction_worker,
                        token_idx, scheduler, repo_full_name, organizationFolder,
                        streams_needed)
            for token_idx in range(n_tokens)
        ]
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as exc:
                print(f"[dispatch] Worker raised: {exc}")

    print(f"[dispatch] All workers finished.\n{scheduler.progress_summary()}")


# ══════════════════════════════════════════════════════════════════════════════
# ── B ──  NEW FUNCTION: parallel_extraction_worker
# ══════════════════════════════════════════════════════════════════════════════

def parallel_extraction_worker(token_idx: int,
    scheduler,          # PageScheduler instance
    repo_full_name: str,
    organizationFolder: str,
    streams_needed: list):
    """
    One worker thread.  Repeatedly claims the next available page from
    any stream, processes it, and marks it done.  Exits when nothing is left.

    Tokens steal work from whichever stream has pending pages, so no token
    ever idles while work remains.
    """
    import Utilities as util
    from github import Github

    token = util.getSpisificToken(token_idx)
    g     = Github(token)
    g.per_page = cfg.items_per_page

    print(f"[Worker {token_idx}] started")

    while True:
        claimed = scheduler.claim_any_page(preferred_order=streams_needed)
        if claimed is None:
            break   # all streams exhausted

        stream_name, page_idx, cursor = claimed
        print(f"[Worker {token_idx}] {stream_name} page {page_idx}  cursor={cursor!r:.40s}…")

        try:
            if stream_name == "issues_with_timeline":
                _single_page_issue(g, token, repo_full_name, organizationFolder,
                                   cursor, page_idx, scheduler, token_idx)
            elif stream_name == "prs_with_comments":
                _single_page_pr(g, token, repo_full_name, organizationFolder,
                                cursor, page_idx, scheduler, token_idx)
            elif stream_name == "commits":
                _single_page_commit(g, token, repo_full_name, organizationFolder,
                                    cursor, page_idx, scheduler, token_idx)

            scheduler.mark_page_done(stream_name, page_idx)
            print(f"[Worker {token_idx}] ✓ {stream_name} page {page_idx}")

        except Exception as exc:
            print(f"[Worker {token_idx}] ✗ {stream_name} page {page_idx}: {exc}")
            scheduler.mark_page_failed(stream_name, page_idx)
            # Re-raise only for truly fatal errors; rate-limit issues should
            # surface via getSameToken before we ever reach this point.


# ══════════════════════════════════════════════════════════════════════════════
# ── C ──  _single_page_issue
# ══════════════════════════════════════════════════════════════════════════════

def _single_page_issue(
    g, token, repo_full_name: str, organizationFolder: str,
    page_cursor, page_idx: int, scheduler, token_idx: int):
    """
    Fetch EXACTLY ONE page of issues and their timelines.
    Writes to scheduler temp files instead of the main CSVs.

    This is essentially the body of updateIssueListFile's while-True loop
    extracted into a standalone function.  The GraphQL queries are identical.
    """
    import Utilities as util
    from pathlib import Path

    g, token, _, _ = util.getSameToken(g, token, token_idx)

    repo      = g.get_repo(repo_full_name)
    owner, name = repo.owner.login, repo.name
    requester = getattr(repo, "requester", None) or getattr(repo, "_requester", None)

    per_page = cfg.items_per_page

    # ---- Queries (identical to updateIssueListFile) ----
    issue_query = """
    query($owner:String!, $name:String!, $first:Int!, $after:String) {
      repository(owner:$owner, name:$name) {
        issues(states:[OPEN, CLOSED], first:$first, after:$after,
               orderBy:{field:CREATED_AT, direction:ASC}) {
          totalCount
          pageInfo { hasNextPage endCursor }
          nodes {
            number title state createdAt closedAt
            author {
              login
              ... on User { id name email }
            }
            labels(first:20)     { nodes { name } }
            assignees(first:10)  { nodes { id login } }
            milestone            { title }
          }
        }
      }
    }"""

    issue_timeline_query = """
    query($owner:String!, $name:String!, $number:Int!, $first:Int!, $after:String) {
      repository(owner:$owner, name:$name) {
        issue(number:$number) {
          timelineItems(first:$first, after:$after) {
            pageInfo { hasNextPage endCursor }
            nodes {
              __typename
              ... on IssueComment {
                id createdAt bodyText
                author { login ... on User { id name email } }
              }
              ... on ClosedEvent   { id createdAt actor { login ... on User { id name email } } }
              ... on ReopenedEvent { id createdAt actor { login ... on User { id name email } } }
              ... on LabeledEvent  {
                id createdAt label { name }
                actor { login ... on User { id name email } }
              }
              ... on UnlabeledEvent {
                id createdAt label { name }
                actor { login ... on User { id name email } }
              }
              ... on CrossReferencedEvent {
                id createdAt actor { login ... on User { id name email } }
              }
            }
          }
        }
      }
    }"""

    # ---- Fetch the single page ----
    vars_page  = {"owner": owner, "name": name, "first": per_page, "after": page_cursor}
    _, issue_data = requester.graphql_query(query=issue_query, variables=vars_page)

    issue_conn = issue_data
    for key in ("data", "repository", "issues"):
        issue_conn = issue_conn[key]
    nodes     = issue_conn.get("nodes", []) or []

    # ---- Build issue rows ----
    issue_rows = []
    for nd in nodes:
        author_id    = (nd.get("author") or {}).get("id")
        author_login = (nd.get("author") or {}).get("login")
        author_name  = (nd.get("author") or {}).get("name")
        author_email = (nd.get("author") or {}).get("email")
        labels    = [x["name"] for x in (nd.get("labels") or {}).get("nodes", [])]
        assignees = [x["login"] for x in (nd.get("assignees") or {}).get("nodes", [])]
        issue_rows.append({
            "repo": repo_full_name,
            "created_at":    nd["createdAt"],
            "author_id":     author_id,
            "author_name":   author_name,
            "author_login":  author_login,
            "author_email":  author_email,
            "issue_number":  nd["number"],
            "title":         nd["title"],
            "state":         nd["state"],
            "closed_at":     nd["closedAt"],
            "labels":        "|".join(labels),
            "assignees":     "|".join(assignees),
            "milestone":     (nd.get("milestone") or {}).get("title"),
        })

    # ---- Fetch timelines for every issue on this page ----
    activity_rows = []
    for nd in nodes:
        num      = nd["number"]
        after_tl = None
        while True:
            g, token, _, _ = util.getSameToken(g, token, token_idx)
            vars_tl = {"owner": owner, "name": name, "number": int(num),
                       "first": 100, "after": after_tl}
            _, tl_d   = requester.graphql_query(query=issue_timeline_query, variables=vars_tl)
            tl_conn   = tl_d["data"]["repository"]["issue"]["timelineItems"]
            tl_nodes  = tl_conn.get("nodes", []) or []

            for it in tl_nodes:
                t        = it.get("__typename")
                aid      = it.get("id")
                created  = it.get("createdAt")
                body     = ""
                event_name = ""
                if t == "IssueComment":
                    body = it.get("bodyText") or ""
                elif t in ("ClosedEvent", "ReopenedEvent"):
                    event_name = "closed" if t == "ClosedEvent" else "reopened"
                elif t in ("LabeledEvent", "UnlabeledEvent"):
                    lbl = (it.get("label") or {}).get("name")
                    event_name = (("labeled" if t == "LabeledEvent" else "unlabeled")
                                  + (f":{lbl}" if lbl else ""))
                elif t == "CrossReferencedEvent":
                    event_name = "cross_referenced"

                actor_block  = it.get("author") or it.get("actor") or {}
                author_id    = actor_block.get("id")
                author_login = actor_block.get("login")
                author_name  = actor_block.get("name")
                author_email = actor_block.get("email")

                activity_rows.append({
                    "repo":         repo_full_name,
                    "created_at":   created,
                    "author_id":    author_id,
                    "author_name":  author_name,
                    "author_login": author_login,
                    "author_email": author_email,
                    "issue_number": num,
                    "activity_id":  aid,
                    "item_type":    t,
                    "event":        event_name,
                    "body":         body,
                })

            if tl_conn["pageInfo"]["hasNextPage"]:
                after_tl = tl_conn["pageInfo"]["endCursor"]
            else:
                break

    # ---- Write to scheduler temp files ----
    if issue_rows:
        util.append_rows_csv(
            scheduler.temp_path("issues_with_timeline", page_idx, "issues"),
            issue_rows, sep=cfg.CSV_separator
        )
    if activity_rows:
        util.append_rows_csv(
            scheduler.temp_path("issues_with_timeline", page_idx, "issue_activity"),
            activity_rows, sep=cfg.CSV_separator
        )


# ══════════════════════════════════════════════════════════════════════════════
# ── D ──  _single_page_pr
# ══════════════════════════════════════════════════════════════════════════════

def _single_page_pr(
    g, token, repo_full_name: str, organizationFolder: str,
    page_cursor, page_idx: int, scheduler, token_idx: int):
    """
    Fetch EXACTLY ONE page of PRs, plus all their comments and reviews.
    Writes to scheduler temp files.
    """
    import Utilities as util

    g, token, _, _ = util.getSameToken(g, token, token_idx)

    repo      = g.get_repo(repo_full_name)
    owner, name = repo.owner.login, repo.name
    requester = getattr(repo, "requester", None) or getattr(repo, "_requester", None)

    per_page = cfg.items_per_page

    # Load existing comment IDs to avoid duplicates (safe even in parallel
    # because each page's PRs are disjoint by creation-date ordering)
    prs_comments_csv_path = Path(organizationFolder, cfg.prs_comments_csv)
    existing_comments: set = set()
    try:
        import pandas
        df_ex = pandas.read_csv(prs_comments_csv_path,
                                sep=cfg.CSV_separator, usecols=["comment_id"])
        existing_comments = set(df_ex["comment_id"].astype(str))
    except Exception:
        pass

    # ---- Queries (identical to updatePRListFile) ----
    pr_query = """
    query($owner:String!, $name:String!, $first:Int!, $after:String) {
      repository(owner:$owner, name:$name) {
        pullRequests(states:[OPEN, MERGED, CLOSED],
                     orderBy:{field:CREATED_AT, direction:ASC},
                     first:$first, after:$after) {
          totalCount
          pageInfo { hasNextPage endCursor }
          nodes {
            number createdAt closedAt mergedAt merged state
            author { login ... on User { id name email } }
          }
        }
      }
    }"""

    pr_comments_query = """
    query($owner:String!, $name:String!, $number:Int!, $first:Int!, $after:String) {
      repository(owner:$owner, name:$name) {
        pullRequest(number:$number) {
          comments(first:$first, after:$after) {
            pageInfo { hasNextPage endCursor }
            nodes {
              id createdAt
              author { login ... on User { id name email } }
            }
          }
        }
      }
    }"""

    pr_reviews_query = """
    query($owner:String!, $name:String!, $number:Int!, $first:Int!, $after:String) {
      repository(owner:$owner, name:$name) {
        pullRequest(number:$number) {
          reviews(first:$first, after:$after) {
            pageInfo { hasNextPage endCursor }
            nodes {
              id submittedAt
              author { login ... on User { id name email } }
            }
          }
        }
      }
    }"""

    # ---- Fetch the single PR page ----
    vars_page = {"owner": owner, "name": name, "first": per_page, "after": page_cursor}
    _, pr_data = requester.graphql_query(query=pr_query, variables=vars_page)

    pr_conn = pr_data.get("data", {}).get("repository", {}).get("pullRequests", {}) or {}
    nodes   = pr_conn.get("nodes", []) or []

    # ---- Build PR rows ----
    pr_rows = []
    for nd in nodes:
        author_id    = (nd.get("author") or {}).get("id")
        author_login = (nd.get("author") or {}).get("login")
        author_name  = (nd.get("author") or {}).get("name")
        author_email = (nd.get("author") or {}).get("email")
        pr_rows.append({
            "repo":         repo_full_name,
            "created_at":   nd.get("createdAt"),
            "author_id":    author_id,
            "author_name":  author_name,
            "author_login": author_login,
            "author_email": author_email,
            "PR_id":        nd.get("number"),
            "state":        nd.get("state"),
            "merged":       bool(nd.get("merged")),
            "closed_at":    nd.get("closedAt"),
            "merged_at":    nd.get("mergedAt"),
        })

    # ---- Fetch comments and reviews for each PR ----
    comment_rows = []
    for nd in nodes:
        pr_number = int(nd.get("number"))

        # Comments
        after_c = None
        while True:
            g, token, _, _ = util.getSameToken(g, token, token_idx)
            vars_c = {"owner": owner, "name": name, "number": pr_number,
                      "first": 100, "after": after_c}
            _, c_d   = requester.graphql_query(query=pr_comments_query, variables=vars_c)
            c_conn   = c_d["data"]["repository"]["pullRequest"]["comments"]
            c_nodes  = c_conn.get("nodes", []) or []
            for c in c_nodes:
                cid = str(c.get("id"))
                if cid in existing_comments:
                    continue
                author_id    = (c.get("author") or {}).get("id")
                author_login = (c.get("author") or {}).get("login")
                author_name  = (c.get("author") or {}).get("name")
                author_email = (c.get("author") or {}).get("email")
                comment_rows.append({
                    "repo":         repo_full_name,
                    "created_at":   c.get("createdAt"),
                    "author_id":    author_id,
                    "author_name":  author_name,
                    "author_login": author_login,
                    "author_email": author_email,
                    "PR_id":        pr_number,
                    "comment_id":   cid,
                    "event":        "comment",
                })
                existing_comments.add(cid)
            if c_conn["pageInfo"]["hasNextPage"]:
                after_c = c_conn["pageInfo"]["endCursor"]
            else:
                break

        # Reviews
        after_r = None
        while True:
            g, token, _, _ = util.getSameToken(g, token, token_idx)
            vars_r = {"owner": owner, "name": name, "number": pr_number,
                      "first": 100, "after": after_r}
            _, r_d   = requester.graphql_query(query=pr_reviews_query, variables=vars_r)
            r_conn   = r_d["data"]["repository"]["pullRequest"]["reviews"]
            r_nodes  = r_conn.get("nodes", []) or []
            for r in r_nodes:
                rid = str(r.get("id"))
                if rid in existing_comments:
                    continue
                author_id    = (r.get("author") or {}).get("id")
                author_login = (r.get("author") or {}).get("login")
                author_name  = (r.get("author") or {}).get("name")
                author_email = (r.get("author") or {}).get("email")
                comment_rows.append({
                    "repo":         repo_full_name,
                    "created_at":   r.get("submittedAt"),
                    "author_id":    author_id,
                    "author_name":  author_name,
                    "author_login": author_login,
                    "author_email": author_email,
                    "PR_id":        pr_number,
                    "comment_id":   rid,
                    "event":        "review",
                })
                existing_comments.add(rid)
            if r_conn["pageInfo"]["hasNextPage"]:
                after_r = r_conn["pageInfo"]["endCursor"]
            else:
                break

    # ---- Write to scheduler temp files ----
    if pr_rows:
        util.append_rows_csv(
            scheduler.temp_path("prs_with_comments", page_idx, "prs"),
            pr_rows, sep=cfg.CSV_separator
        )
    if comment_rows:
        util.append_rows_csv(
            scheduler.temp_path("prs_with_comments", page_idx, "prs_comments"),
            comment_rows, sep=cfg.CSV_separator
        )


# ══════════════════════════════════════════════════════════════════════════════
# ── E ──  _single_page_commit
# ══════════════════════════════════════════════════════════════════════════════

def _single_page_commit(
    g, token, repo_full_name: str, organizationFolder: str,
    page_cursor, page_idx: int, scheduler, token_idx: int):
    """
    Fetch EXACTLY ONE page of commits plus per-file data.
    Writes to scheduler temp files.
    """
    import Utilities as util

    g, token, _, _ = util.getSameToken(g, token, token_idx)

    repo      = g.get_repo(repo_full_name)
    owner, name = repo.owner.login, repo.name
    requester = getattr(repo, "requester", None) or getattr(repo, "_requester", None)

    per_page = min(cfg.items_per_page, 100)

    # ---- Query (identical to updateCommitListFile) ----
    commit_query = """
    query($owner:String!, $name:String!, $first:Int!, $after:String) {
      repository(owner:$owner, name:$name) {
        defaultBranchRef {
          target {
            ... on Commit {
              history(first:$first, after:$after) {
                totalCount
                pageInfo { hasNextPage endCursor }
                nodes {
                  oid committedDate additions deletions changedFilesIfAvailable
                  author {
                    name email
                    user { id login name email }
                  }
                }
              }
            }
          }
        }
      }
    }"""

    # ---- Fetch the single page ----
    g, token, _, _ = util.getSameToken(g, token, token_idx)
    vars_page = {"owner": owner, "name": name, "first": per_page, "after": page_cursor}
    _, commit_data = requester.graphql_query(query=commit_query, variables=vars_page)

    repo_d = commit_data.get("data", {}).get("repository", {})
    dref   = repo_d.get("defaultBranchRef")
    if not dref or not dref.get("target"):
        print(f"[_single_page_commit] No defaultBranchRef for {repo_full_name}")
        return

    hist      = dref["target"].get("history") or {}
    nodes     = hist.get("nodes", []) or []

    # ---- Build commit & per-file rows (identical logic to updateCommitListFile) ----
    commit_rows = []
    file_rows   = []

    for nd in nodes:
        sha            = nd.get("oid")
        committed_date = nd.get("committedDate")
        author_block   = nd.get("author") or {}
        auser          = author_block.get("user") or {}

        author_id    = auser.get("id")
        author_login = auser.get("login")
        author_name  = author_block.get("name")
        author_email = author_block.get("email")

        adds    = nd.get("additions") or 0
        dels    = nd.get("deletions") or 0
        changed = nd.get("changedFilesIfAvailable") or 0

        # Per-file data via REST
        filenames = []
        try:
            g, token, _, _ = util.getSameToken(g, token, token_idx)
            c     = repo.get_commit(sha)
            files = c.files or []
            for f in files:
                file_rows.append({
                    "repo":              repo_full_name,
                    "author_id":         author_id,
                    "author_login":      author_login,
                    "author_name":       author_name,
                    "author_email":      author_email,
                    "sha":               sha,
                    "committed_at":      committed_date,
                    "committer_login":   (c.committer.login if c.committer else None),
                    "file_path":         f.filename,
                    "status":            getattr(f, "status", None),
                    "additions":         int(getattr(f, "additions", 0) or 0),
                    "deletions":         int(getattr(f, "deletions", 0) or 0),
                    "changes":           int(getattr(f, "changes", 0) or 0),
                    "previous_filename": getattr(f, "previous_filename", None),
                })
        except Exception:
            filenames = []

        if (author_email is None) and (author_name is None) and (author_id is None):
            continue

        commit_rows.append({
            "repo":              repo_full_name,
            "created_at":        committed_date,
            "author_id":         author_id,
            "author_name":       author_name,
            "author_login":      author_login,
            "author_email":      author_email,
            "sha":               sha,
            "filename_list":     "|".join(filenames),
            "fileschanged_count": int(changed),
            "additions_sum":     int(adds),
            "deletions_sum":     int(dels),
        })

    # ---- Write to scheduler temp files ----
    if commit_rows:
        util.append_rows_csv(
            scheduler.temp_path("commits", page_idx, "commits"),
            commit_rows, sep=cfg.CSV_separator
        )
    if file_rows:
        util.append_rows_csv(
            scheduler.temp_path("commits", page_idx, "per_file_commits"),
            file_rows, sep=cfg.CSV_separator
        )


# ══════════════════════════════════════════════════════════════════════════════
# HOW TO WIRE THIS IN  (exact change to updateAlldata)
# ══════════════════════════════════════════════════════════════════════════════
#
# In updateAlldata(), find the block that starts with:
#
#       if full_extraction == True:
#           work_orders = [
#
# Delete that entire if/else + ThreadPoolExecutor block, and replace it with
# this one call (tables dict stays the same):
#
#       tables = { "issues": issues_df, ... }   # ← keep this line
#
#       _new_updateAlldata_dispatch(
#           states, tables, repo_full_name, organizationFolder, full_extraction
#       )
#       return                                   # ← add this return
#
# That's the only change to the existing functions.  All three
# updateXxxListFile() functions remain completely unmodified.
