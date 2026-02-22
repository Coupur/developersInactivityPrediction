
import pandas


def label_timeline(user_timeline, breaks_df):
    """
    Make a labled timeline of devlopers breaks

    given a user_timeline and breaks_df
    user_timeline
    dev,date,commits,issues,prs,files_changed,lines_added,lines_removed,prs_review,prs_comment,issues_commented,issues_activity,labeled,closed,commented,mentioned,subscribed,referenced,renamed,issue_type_added,unsubscribed,pinned,locked,reopened,assigned,unlabeled,connected,milestoned,comment_deleted,unassigned,unpinned,demilestoned,marked_as_duplicate,transferred,unmarked_as_duplicate,unlocked,parent_issue_added,parent_issue_removed,sub_issue_added,sub_issue_removed,disconnected
    jekyllbot,2014-06-07,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0

    breaks_df
    len,dates,th
    72,2015-05-05/2015-07-16,59.25
    """
    df = user_timeline.copy()
    # We need to make coding_day,nc_day 
    # a coding day is something specific
    # coding activity both making a commit to a local repository and opening a pull request
    # that means in daily df commits > 0 AND prs > 0

    # coding activity: commit OR PR
    df["coding_day"] = ((df["commits"] > 0) | (df["prs"] > 0)).astype(int)
    # non-coding activity: any other event > 0 AND no coding
    noncoding_cols = [c for c in df.columns if c not in [ "prs", "issues"]]
    df["nc_day"] = ((df[noncoding_cols].sum(axis=1) > 0) & (df["coding_day"] == 0)).astype(int)


    df["break_day"] = pandas.Series(False, index=df.index, dtype="boolean")
    df["th"] = pandas.Series(pandas.NA, index=df.index, dtype="Float64")
    df["len"] = pandas.Series(pandas.NA, index=df.index, dtype="Int64") 

    #this marks the break days from breaks_df onto user_timeline
    for breaks in breaks_df.itertuples():
        start = breaks.dates.split('/')[0]
        end = breaks.dates.split('/')[1]

        start = pandas.to_datetime(start)
        end = pandas.to_datetime(end)
        

        if start in df.index and bool(df.at[start, "coding_day"]):
            start = start + pandas.Timedelta(days=1)

        end = pandas.to_datetime(end) - pandas.Timedelta(days=1)

        break_range = pandas.date_range(start=pandas.to_datetime(start)+pandas.Timedelta(days=1),
                                end=pandas.to_datetime(end)-pandas.Timedelta(days=1))

        if pandas.to_datetime(start) <= end:

           for date in break_range:
                date = date.strftime("%Y-%m-%d")
                df.at[date, "break_day"] = True
                df.at[date, "th"] = breaks.th
                df.at[date, "len"] = breaks.Index

    #turns all the NA into false
    for col in ["coding_day", "nc_day", "break_day"]:
        df[col] = df[col].astype("boolean").fillna(False)


    df.index = pandas.to_datetime(df.index)
    df = df.sort_index()

    # Optional: unmark the break *end* day (commit day) so it’s not counted as break
    # break end - 1 day = end non coding
    for breaks in breaks_df.itertuples():
        end = pandas.to_datetime(breaks.dates.split('/')[1])
        if end in df.index:
            df.at[end, "break_day"] = False

    gone_days = 365

    df["event_day"] = df["coding_day"] | df["nc_day"]

    df["state"] = "ACTIVE"

    # Identify contiguous break windows (groups of consecutive True in break_day)
    bd = df["break_day"]
    group_id = (bd != bd.shift(1)).cumsum()

    # Precompute last event BEFORE a given date (global, across timeline)
    all_events_idx = df.index[df["event_day"]]

    for gid, block in df.groupby(group_id):
        if not block["break_day"].iloc[0]:
            continue  # not a break chunk

        # This is one contiguous break [start .. end] (inclusive)
        start_ts = block.index[0]
        end_ts   = block.index[-1]

        # Lookahead info: is there any non-coding event in this break?
        has_nc = bool((df.loc[start_ts:end_ts, "nc_day"]).any())

        th_vals = df.loc[start_ts:end_ts, "th"].dropna()
        Tfov = int(round(th_vals.iloc[0])) if not th_vals.empty else 14

        # Anchor silence to the last event (coding or non-coding) before the break starts
        prev_nc_idx = df.index[df["nc_day"] & (df.index < start_ts)]
        last_nc_before = prev_nc_idx.max() if len(prev_nc_idx) else None

        last_nc = None  # most recent non-coding event inside this break
        if last_nc_before is not None and (start_ts - last_nc_before).days <= Tfov:
            # Seed the NON_CODING hold across the start of the break
            last_nc = last_nc_before

        # Walk day by day inside the break
        for d in block.index:

            # Non-coding event day => NON_CODING and update last_nc
            if bool(df.at[d, "nc_day"]):
                df.at[d, "state"] = "NON_CODING"
                last_nc = d
                continue

            # Silent day inside a break -> decide via Tfov and gone
            # Compute silence since the most relevant last event:
            # - Prefer last NC inside break; else use last event before break; else start-of-break as approximate anchor.
            ref_nc = last_nc if last_nc is not None else None
            if (ref_nc is not None) and ((d - ref_nc).days <= Tfov):
                df.at[d, "state"] = "NON_CODING"
                continue

            # No recent NC: INACTIVE vs GONE (since last ANY event)
            last_any = ref_nc if ref_nc is not None else (all_events_idx[all_events_idx < start_ts].max() if len(all_events_idx[all_events_idx < start_ts]) else None)
            silent_days = (d - last_any).days
            if silent_days > gone_days:
                df.at[d, "state"] = "GONE"
            else:
                df.at[d, "state"] = "INACTIVE"


    return df



