#   conda activate osslab
#   streamlit run DemoAppV2.2.py


#   cd D:\Users\samut\OneDrive\Documents\GitHub\developersInactivityAnalysisCOPY\Extractors

from asyncio import Event
import json
from operator import index
from msilib import Table
import streamlit as st
import pandas
import pandas as pd
import numpy as np
import os
import csv
import sys
import matplotlib.pyplot as plt
from sklearn.preprocessing import OneHotEncoder

import matplotlib.dates as mdates
from datetime import datetime, timezone, timedelta
from tqdm import tqdm
from typing import Iterable, Tuple, List, Set, Dict, Optional, Literal
from collections import Counter
from pathlib import Path
from git import Repo, exc as git_exc
from xgboost import XGBClassifier, XGBRegressor
from sklearn.preprocessing import LabelEncoder, label_binarize, StandardScaler
from sklearn.calibration import CalibratedClassifierCV
from sklearn.model_selection import GroupShuffleSplit, train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.ensemble import RandomForestClassifier
import random

from torch.utils.data import Dataset, DataLoader

import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
import torch.optim as optim

from dataclasses import dataclass
from github import Github, GithubException, UnknownObjectException, IncompletableObject
import joblib

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, average_precision_score, f1_score, recall_score, classification_report, confusion_matrix

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
import Settings as cfg
import Utilities as util
import KnowledgeDistribution as kd
import SocialTechnicalNetwork as stn
import ProjectHealthMetrics as phm

LAST_CALLED = [None]

ORG_BASE = PROJECT_ROOT / "Organizations"
IMAGES_BASE = PROJECT_ROOT / cfg.photo_folder
IMAGES_BASE.mkdir(parents=True, exist_ok=True)

_PR_REPO_OLD_COLS = ["repo", "created_at", "created_by", "PR_id", "state", "merged", "closed_at", "merged_at"]
_PR_REPO_NEW_COLS = ["repo", "created_at", "author_id", "author_name", "author_login", "author_email", "PR_id", "state", "merged", "closed_at", "merged_at"]
_PR_COM_OLD_COLS  = ["repo", "created_at", "created_by", "PR_id", "comment_id", "event"]
_PR_COM_NEW_COLS  = ["repo", "created_at", "author_id", "author_name", "author_login", "author_email", "PR_id", "comment_id", "event"]

# commit_list: old used 'created_by' (login) + author_name/email; new adds author_id + author_login
_COMMIT_OLD_COLS  = ["repo", "created_at", "created_by", "author_name", "author_email", 
                     "sha", "filename_list", "fileschanged_count", "additions_sum", "deletions_sum"]
_COMMIT_NEW_COLS  = ["repo", "created_at", "author_id", "author_name", "author_login", "author_email", 
                     "sha", "filename_list", "fileschanged_count", "additions_sum", "deletions_sum"]


# issues: old had only created_by; new has full author breakdown
_ISSUE_OLD_COLS   = ["repo", "created_at", "created_by", "issue_number", "title",
                     "state", "closed_at", "labels", "assignees", "milestone"]
_ISSUE_NEW_COLS   = ["repo", "created_at", "author_id", "author_name", "author_login", "author_email",
                     "issue_number", "title", "state", "closed_at", "labels", "assignees", "milestone"]

# issue_activity: old had created_at/created_by at the END; new has full author fields near the front
_ISSUE_ACT_OLD_COLS = ["repo", "issue_number", "activity_id", "item_type", "event", "body", "created_at", "created_by"]
_ISSUE_ACT_NEW_COLS = ["repo", "created_at", "author_id", "author_name", "author_login", "author_email",
                       "issue_number", "activity_id", "item_type", "event", "body"]

def _read_pr_csv_compat(file_path, old_cols, new_cols):
    """
    Read a PR CSV that may contain rows written under two different schemas.
    Old rows use 'created_by' for the author login; new rows use the full
    author_id / author_name / author_login / author_email breakdown.
    Returns a DataFrame with the new-schema columns.
    """
    # Some fields (issue body, filename_list) can exceed the default 131072-byte limit.
    try:
        csv.field_size_limit(sys.maxsize)
    except OverflowError:
        csv.field_size_limit(2 ** 31 - 1)

    records = []
    try:
        with open(file_path, 'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f)
            next(reader, None)  # skip header line (may be either schema)
            for row in reader:
                if len(row) == len(old_cols):
                    d = dict(zip(old_cols, row))
                    d['author_login'] = d.pop('created_by')
                    d.setdefault('author_id', None)
                    d.setdefault('author_name', None)
                    d.setdefault('author_email', None)
                    records.append({c: d.get(c) for c in new_cols})
                elif len(row) == len(new_cols):
                    records.append(dict(zip(new_cols, row)))
                # skip malformed rows silently
    except Exception as e:
        print(f"Warning: could not read {file_path}: {e}")
    return pandas.DataFrame(records, columns=new_cols) if records else pandas.DataFrame(columns=new_cols)


def load_users_activity(repo_full_name):
    """
        We need to FIND and load all of our raw data
    we have to get the file paths correct
    then we have to load the csv and then put them in a table

    #TODO: this is a long todo but we need to add our daily data collectiong here too
    #insted of just finding the data
    # we need to have a contiuous data collection system were every day it checks for new data and adds it to the data set
    # so we will have two parts of data collection
    # one algorithm that collects every day
    # one algorithm that gets all of that data
    """
    # in one line find the folder we are in
    orgs_dir = PROJECT_ROOT / "Organizations"

    target_files = {
        "issues": cfg.issue_list_file_name,
        "issue_activity": cfg.issue_activity_file_name,
        "prs_repo": cfg.PR_list_file_name,
        "prs_comments": cfg.prs_comments_csv,
        "commit_list": cfg.commit_list_file_name,
        "perfile_commit": cfg.per_file_commits_path
    }
    #we need to make df for each of the target files
    issues = pandas.DataFrame()
    issue_activity = pandas.DataFrame()
    prs_repo = pandas.DataFrame()
    prs_comments = pandas.DataFrame()
    commits = pandas.DataFrame()
    perfile_commits = pandas.DataFrame()

    if not orgs_dir.exists():
        st.write(f"Organizations folder not found: {orgs_dir}")
        return 0
    
    org, repo = repo_full_name.split('/')
    organization_path = orgs_dir / org

    for repo in organization_path.iterdir():
        if not repo.is_dir():
            continue
        for file_key, file_name in target_files.items():
            file_path = repo / file_name
            if file_path.exists():
                print(f"Found file: {file_path}")
                try:
                    # Files that changed schema (added author_id/login/name/email, removed created_by)
                    # use the compat reader so old and new rows are both handled.
                    if file_key == "prs_repo":
                        df = _read_pr_csv_compat(file_path, _PR_REPO_OLD_COLS, _PR_REPO_NEW_COLS)
                        prs_repo = pandas.concat([prs_repo, df], ignore_index=True)
                    elif file_key == "prs_comments":
                        df = _read_pr_csv_compat(file_path, _PR_COM_OLD_COLS, _PR_COM_NEW_COLS)
                        prs_comments = pandas.concat([prs_comments, df], ignore_index=True)
                    elif file_key == "commit_list":
                        df = _read_pr_csv_compat(file_path, _COMMIT_OLD_COLS, _COMMIT_NEW_COLS)
                        commits = pandas.concat([commits, df], ignore_index=True)
                    elif file_key == "issues":
                        df = _read_pr_csv_compat(file_path, _ISSUE_OLD_COLS, _ISSUE_NEW_COLS)
                        issues = pandas.concat([issues, df], ignore_index=True)
                    elif file_key == "issue_activity":
                        df = _read_pr_csv_compat(file_path, _ISSUE_ACT_OLD_COLS, _ISSUE_ACT_NEW_COLS)
                        issue_activity = pandas.concat([issue_activity, df], ignore_index=True)
                    else:
                        df = pandas.read_csv(file_path)
                        if file_key == "perfile_commit":
                            perfile_commits = pandas.concat([perfile_commits, df], ignore_index=True)
                except Exception as e:
                    st.write(f"Error loading {file_path}: {e}")
            else:
                st.write(f"File not found: {file_path}")

    raw_data_tables = { "issues": issues, "issue_activity": issue_activity, 
                       "prs_repo": prs_repo, "prs_comments": prs_comments, 
                       "commits": commits , "perfile_commits": perfile_commits}

    #save the data to a temp file
    
    return raw_data_tables

@st.cache_data(show_spinner=False)
def list_orgs(base: Path = ORG_BASE) -> list[str]:
    """All org folder names under Organizations/"""
    if not base.exists():
        return []
    return sorted([p.name for p in base.iterdir() if p.is_dir()], key=str.casefold)

@st.cache_data(show_spinner=True)
def list_repos_for(org: str, base: Path = ORG_BASE) -> list[str]:
    """All repo folder names under Organizations/<org>/"""
    root = base / org
    if not root.exists():
        return []
    return sorted([p.name for p in root.iterdir() if p.is_dir()], key=str.casefold)
                 
#-----------------------
# inactivity labeling
#------------------------
def label_developers_activity(repo, over_write = False) -> pandas.DataFrame:
    """
    main function for labeling developers
    sets up varables to call label timeline
    """
    
    # "../Organizations"
    organizationFolder = ORG_BASE

    win = cfg.sliding_window_size

    repos_txt = '../' + cfg.repos_file
    repos_to_process = []
        
    
    all_timelines = []
    all_diagnostics = []

    # i want to check if the end file has already been made and if it has then we can skip the whole process and just load the file
    output_folder = organizationFolder /  "Results"
    os.makedirs(output_folder, exist_ok=True)
    out_path = Path(output_folder) / "all_users_labeled_timeline.csv"
    
    if out_path.is_file() and over_write == False:
        print("we are loading the file from ", out_path)
        df = pandas.read_csv(out_path, sep=cfg.CSV_separator)
        return df
    organization, project = repo.split('/')
    if Path(organizationFolder, organization).exists() == False:
        st.write(f"Organization folder not found: {Path(organizationFolder, organization)}")

    print(f"Start Identifying inactivity periods for {organization}/{project}...")

    organizationFolder = Path(ORG_BASE) / organization / project

    commits = pd.read_csv( organizationFolder / "commit_list.csv" , sep=cfg.CSV_separator, parse_dates=["created_at"])

    if commits.empty:
        st.write(f"No commits found for {organization}/{project} at {organizationFolder / 'commit_list.csv'}")
        return pandas.DataFrame()  # Return empty DataFrame if no commits

    commits["created_at"] = pandas.to_datetime(commits["created_at"], utc=True)

    # TruckFactor.json is written by KnowledgeDistribution.py into the KnowledgeDistribution/ subfolder.
    with open(organizationFolder / "KnowledgeDistribution" / cfg.truck_factor_file, "r") as f:
        _tf_data = json.load(f)
    tf_devs = _tf_data["tf_list"]
    tf = _tf_data["tf"]
    
    pauses = write_pauses_table(commits, organizationFolder / "pauses_commits.csv", tf_devs, date_col="created_at")
    #make pauses to a csv file at this location C:\Users\samut\OneDrive\Documents\GitHub\developersInactivityAnalysisCOPY\Organizations\Rdatatable\data.table\Results
    
    count = 0


    for dev in tf_devs:
        print(f"{tf_devs.index(dev) + 1} / {len(tf_devs)}")
        if dev.startswith("author_login") or dev.startswith("author_name") or dev.startswith("author_email"):
            column, dev = dev.split('|', maxsplit=1)
        count= count+1

        timeline_folder = organizationFolder /  cfg.timeline_folder
        os.makedirs(timeline_folder, exist_ok=True)
            
        timeline_path = Path(timeline_folder, cfg.timeline_file)

        if timeline_path.is_file():
            user_timeline = pandas.read_csv(timeline_path, sep=cfg.CSV_separator)
        else:
            print(f"Timeline not found at {timeline_path}, generating timeline...")
            continue

        breaks_folder = organizationFolder /  "Breaks"
        os.makedirs(breaks_folder, exist_ok=True)
        breaks_path =  Path(breaks_folder)/  f"{dev}_breaks.csv"

        breaks_df = pandas.DataFrame(columns=['len', 'dates', 'th'])
        breaks_df, diagnostics_df = identifyBreaks(pauses, dev=dev, window=win, debug_folder=output_folder)

        breaks_df.to_csv(breaks_path, sep=cfg.CSV_separator, na_rep=cfg.CSV_missing, lineterminator="\n")

        # filter to this developer before labeling
        user_timeline = user_timeline[user_timeline["dev"] == dev]


        user_timeline = label_timeline(user_timeline, breaks_df)

        user_timeline = user_timeline.reset_index(drop=True)
        all_timelines.append(user_timeline)

        all_diagnostics.append(diagnostics_df)

        out_csv = Path(output_folder) / f"{dev}_labeled_timeline.csv"
        user_timeline.to_csv(out_csv, sep=cfg.CSV_separator, na_rep=cfg.CSV_missing, lineterminator='\n', index_label='date')

        tf_devs_df = pandas.DataFrame(tf_devs, columns=["developer"])
        tf_devs_df.to_csv(Path(output_folder) / "tf_devs.csv", sep=cfg.CSV_separator, na_rep=cfg.CSV_missing, quoting=None, lineterminator='\n')

        # WE NEED TO MAKE A MASTER USER TIMELINE that we use as the return value


    if tf == 1:
        master_user_timeline = all_timelines[0]
        master_diagnostics = all_diagnostics
    else:
        master_user_timeline = pandas.concat(all_timelines, ignore_index=True)
        master_diagnostics = pandas.concat(all_diagnostics, ignore_index=True)


    # Extract DataFrames from lists if needed
    if isinstance(master_user_timeline, list) and len(master_user_timeline) > 0:
        if isinstance(master_user_timeline[0], pandas.DataFrame):
            master_user_timeline = master_user_timeline[0]

    if isinstance(master_diagnostics, list) and len(master_diagnostics) > 0:
        if isinstance(master_diagnostics[0], pandas.DataFrame):
            master_diagnostics = master_diagnostics[0]

    # Normalize date columns
    master_user_timeline["date"] = pandas.to_datetime(master_user_timeline["date"]).dt.normalize()
    master_diagnostics["date"] = pandas.to_datetime(master_diagnostics["win_end"]).dt.normalize()



    # Merge on dev and date
    master_user_timeline = master_user_timeline.merge(
        master_diagnostics,
        left_on=["dev", "date"],
        right_on=["dev", "date"],
        how="left"
    )



    out_path = Path(output_folder) / "all_users_labeled_timeline.csv"

    master_user_timeline.to_csv(out_path, index=False)


    # visualize the breaks
    # for each developer in tf_devs we need to make a plot of their timeline with breaks marked
    devs = sorted(master_user_timeline["dev"].unique())

    return master_user_timeline

def write_pauses_table(
        df: pandas.DataFrame,
        out_path: os.PathLike,
        tf_devs: list[str] | None = None,
        *,
        date_col: str = "created_at",
        tail_to_today: bool = True
    ) -> pandas.DataFrame:

    df[date_col] = pandas.to_datetime(df[date_col]).dt.normalize()

    rows = []

    count =0
    for dev in tf_devs:
        if dev.startswith("author_login") or dev.startswith("author_name") or dev.startswith("author_email"):
            column, dev = dev.split('|', maxsplit=1)
            user_df = df[df[column] == dev]
        else:
            user_df = df[df["author_id"] == dev]

        active_days = sorted(user_df[date_col].dt.date.unique())
        current_row = [dev]

        for i in range(len(active_days) - 1):
            #if we are at the first day then there is no prev day and no period
            # so we skip it
            if i == 0:
                continue
            prev_active_day = active_days[i - 1]
            #FOUND ITTTTTTTT
            current_active_day = active_days[i]
            gap = (current_active_day - prev_active_day).days
            if gap > 1:
                # Inactivity starts the day after prev_active_day
                current_row.append(f"{(prev_active_day + pandas.Timedelta(days=1)).strftime('%Y-%m-%d')}/{(current_active_day - pandas.Timedelta(days=1)).strftime('%Y-%m-%d')}")
            else:
                count += 1

        if tail_to_today:
            today = datetime.now().date()
            gap = (today - active_days[-1]).days
            if gap > 1:
                current_row.append(f"{active_days[-1]}/{today}")
        if len(current_row) > 1:
            rows.append(current_row)
    
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="",encoding="utf-8" ) as f:
        csv.writer(f, delimiter=",", quoting=csv.QUOTE_NONE).writerows(rows)
    out = pandas.DataFrame(rows)

    return out

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
    if df.empty:
        return df

    # Ensure the index is a DatetimeIndex BEFORE any df.at[date, ...] calls.
    # If user_timeline has a plain integer index, df.at["2015-05-05", col] would
    # add phantom rows with NaN instead of setting existing ones.
    if not isinstance(df.index, pandas.DatetimeIndex):
        if "date" in df.columns:
            df.index = pandas.to_datetime(df["date"])
        else:
            df.index = pandas.to_datetime(df.index)
    df = df.sort_index()

    # coding activity: commit OR PR
    df["coding_day"] = ((df["commits"] > 0) | (df["prs"] > 0)).astype(int)

    # non-coding activity: any other event > 0 AND no coding
    noncoding_cols = [c for c in df.columns if c in ["issues", "issue_activity", "pr_activity"]]
    df["nc_day"] = ((df[noncoding_cols].sum(axis=1) > 0) & (df["coding_day"] == 0)).astype(int)

    df["break_day"] = pandas.Series(False, index=df.index, dtype="boolean")
    df["th"]        = pandas.Series(pandas.NA, index=df.index, dtype="Float64")
    df["len"]       = pandas.Series(pandas.NA, index=df.index, dtype="Int64")
    df["index"]     = pandas.Series(pandas.NA, index=df.index, dtype="Int64")

    #this marks the break days from breaks_df onto user_timeline
    for breaks in breaks_df.itertuples():
        start = breaks.dates.split('/')[0]
        end = breaks.dates.split('/')[1]

        start = pandas.to_datetime(start)
        end = pandas.to_datetime(end)

        break_range = pandas.date_range(start=pandas.to_datetime(start),
                                end=pandas.to_datetime(end))

        if pandas.to_datetime(start) <= end:

           for date in break_range:
                date = date.strftime("%Y-%m-%d")
                df.at[date, "break_day"] = True
                df.at[date, "th"] = breaks.th
                df.at[date, "len"] = breaks.len
                df.at[date, "index"] = breaks.Index

    df.index = pandas.to_datetime(df.index)
    df = df.sort_index()

    gone_days = 365

    df["state"] = "ACTIVE"


    # Identify contiguous break windows (groups of consecutive True in break_day)
    bd = df["break_day"]
    group_id = (bd != bd.shift(1)).cumsum()

    df["event_day"] = ((df["coding_day"] >= 1) | (df["nc_day"] >= 1)).astype(int)

    # Precompute last event BEFORE a given date (global, across timeline)
    all_events_idx = df.index[df["event_day"]]

    for gid, block in df.groupby(group_id):

        if not block["break_day"].iloc[0]:
            continue  # not a break chunk

        # This is one contiguous break [start .. end] (inclusive)
        start_ts = block.index[0]
        end_ts   = block.index[-1]


        th_vals = df.loc[start_ts:end_ts, "th"]
        Tfov = int(th_vals.iloc[0])

        # optional: get far-out threshold
        # Anchor silence to the last event (coding or non-coding) before the break starts

        nc_mask    = df["nc_day"].fillna(0).astype(bool)
        event_mask = df["event_day"].fillna(0).astype(bool)

        prev_nc_idx       = df.index[nc_mask    & (df.index < start_ts)]
        last_nc_before    = prev_nc_idx.max()   if len(prev_nc_idx)    else start_ts

        prev_activity_idx = df.index[event_mask & (df.index < start_ts)]
        last_event_before = prev_activity_idx.max() if len(prev_activity_idx) else start_ts

        #
        #last_nc = None  # most recent non-coding event inside this break
        #if last_nc_before is not None and (start_ts - last_nc_before).days <= Tfov:
        #    for d in range((start_ts - last_nc_before).days):
        #        current_day = last_nc_before + pandas.Timedelta(days=d + 1)
        #        df.at[current_day, "state"] = "NON_CODING"
        #    print(f"Processing break from {start_ts.date()} to {end_ts.date()} ")
        #    print("last_nc_before", last_nc_before)
        #    print("the most recent day was ", (start_ts - last_nc_before).days, "days ago")
        #    print("Tfov =", Tfov)
        #    view_df(df.loc[start_ts:end_ts], name="Break block before labeling")
        #    print("\n\n")
        

        # Walk day by day inside the break
        for d in block.index:

            # Non-coding event day => NON_CODING and update last_nc
            if bool(df.at[d, "nc_day"]):
                df.at[d, "state"] = "NON_CODING"
                last_nc_before = d
                continue

            # Silent day inside a break -> decide via Tfov and gone
            # Compute silence since the most relevant last event:
            # - Prefer last NC inside break; else use last event before break; else start-of-break as approximate anchor.
            if ((d - last_nc_before).days <= Tfov):
                df.at[d, "state"] = "NON_CODING"
                continue

            # No recent NC: INACTIVE vs GONE (since last ANY event)
            silent_days = (d - last_event_before).days
            if silent_days > gone_days:
                df.at[d, "state"] = "GONE"
            else:
                df.at[d, "state"] = "INACTIVE"
    return df

def getFarOutThreshold(values): ### If it is satisfying, move the function into UTILITIES
    th = 0
    q_3rd = np.percentile(values,75)
    q_1st = np.percentile(values,25)
    iqr = q_3rd-q_1st
    if iqr > 1:
        th = q_3rd + 3*iqr
    return th

def addToBreaksList(current_dt, pauses, intervals_list, currentBreaks, th):
    # we need to find the current pause. this is the current date and the last active day before today 
    # we need to find the previous length of pause
    # we can do this by finding the pause that has the current date as the end date
    # then we can check if the length of that pause is greater than the threshold

    #find the row where current_dt = intervals_list["date"].split('/')[1] or the end date
    for interval in intervals_list:
        int_start_str, int_end_str = interval.split('/')
        if int_end_str != current_dt.strftime('%Y-%m-%d'):
            continue
        else:           
            pause_len = util.daysBetween(int_start_str, int_end_str) + 1
            if int_end_str == current_dt.strftime('%Y-%m-%d') and pause_len >= th:
                # check if this break is already in the list
                if not ((currentBreaks['dates'] == interval).any()):
                    util.add(currentBreaks, [pause_len, interval, th])

    return currentBreaks

def cleanClearBreaks(clearBreaks, breaks):
    for _, b in breaks.iterrows():
        clearBreaks = clearBreaks[clearBreaks.dates != b['dates']] # If it was in the long_breaks list, remove ot from there
    return clearBreaks

def identifyBreaks(pauses_dates_list, dev, window, debug_folder=None):
    '''
    Removes SURE BREAKS from windows to calculate Tfov
    and — with debug_folder — writes a per-window diagnostics CSV.
    '''
    pauses_dates_list = pauses_dates_list.values.tolist()

    breaks_df = pandas.DataFrame(columns=['len', 'dates', 'th'])
    diagnostics = []                             # NEW
    count = 0

    for row in pauses_dates_list:
        # print the first few characters of the row

        if str(row[0]).strip() != str(dev).strip():            # ⬅️  ignore other developers
            continue

        count += 1
        if count % 50 == 0:  # Print progress every 50 rows
            print(count)

        intervals_list = [ x for x in row[1:]
                          if isinstance(x, str) and '/' in x and x.strip()]

        intervals_list.sort(key=lambda s: s.split('/')[0])

        if not all(a.split('/')[0] <= b.split('/')[0]
                for a, b in zip(intervals_list, intervals_list[1:])):
            print("⚠️  intervals_list UNSORTED for", dev[1])

        if not intervals_list:
            print(dev[1], 'has NO valid pauses')
            continue                      # <- don’t bail out; just skip

        clear_breaks = pandas.DataFrame(columns=['len', 'dates'])

        last_th = 0
        if intervals_list:
            FPS_dt = datetime.strptime(intervals_list[0].split('/')[0], '%Y-%m-%d')
            LPE_dt = datetime.strptime(intervals_list[-1].split('/')[1], '%Y-%m-%d')
            print(f"Dev {dev} Start date = {FPS_dt.date()}  End date ={LPE_dt.date()}")
        else:
            print("  (no intervals after filtering)")

        current_dt = FPS_dt
        current_index = 0

        while current_dt < LPE_dt:
            win_start, win_end = current_dt - timedelta(days=window), current_dt
            past_pauses_list = pandas.DataFrame(columns=['len', 'dates'])
            partially_included_pauses_list = pandas.DataFrame(columns=['len', 'dates'])

            for interval in intervals_list:
                int_start_str, int_end_str = interval.split('/')          # keep strings
                int_start_dt  = datetime.strptime(int_start_str, '%Y-%m-%d')
                int_end_dt    = datetime.strptime(int_end_str,   '%Y-%m-%d')
                pause_len = util.daysBetween(int_start_str, int_end_str) + 1
                # fully inside
                if int_start_dt >= win_start and int_end_dt <= win_end:
                    util.add(past_pauses_list, [pause_len, interval])
                # touches boundary but need to still be less than current date
                if ((int_start_dt <= win_end and int_end_dt > win_end and int_end_dt < current_dt) or
                    (int_end_dt >= win_start and int_start_dt < win_start and int_start_dt < current_dt)):
                    util.add(partially_included_pauses_list, [pause_len, interval])
                # we need to add the current pause length to the list of pauses

            
            win_pauses = len(past_pauses_list)
            pauses = pandas.concat([past_pauses_list,
                                    partially_included_pauses_list],
                                    ignore_index=True)

            # --- decision logic (unchanged) ------------------------------
            win_th = None
            added_flag = False
            if win_pauses >= 4:
                # To check if we have look head bias we can look at the
                # data that we are using to calculate the threshold
                #print(pauses)
                win_th = getFarOutThreshold(pauses['len'])
                
                if win_th < 3:
                    #print("we are seeing a very low threshold of ", win_th, "for dev ", dev, "at date ", current_dt.date(), "with window ", window)
                    #print("the pauses['len'] values are ", pauses['len'].tolist())
                    th = 0
                    q_3rd = np.percentile(pauses['len'],75)
                    q_1st = np.percentile(pauses['len'],25)
                    iqr = q_3rd-q_1st
                    if iqr > 1:
                        th = q_3rd + 3*iqr
                    
                    #print("meaning when we run through the calculation")
                    #print("q_3rd is ", q_3rd)
                    #print("q_1st is ", q_1st)
                    #print("iqr is ", iqr)
                    #print("th is ", th)
                    

                #print(win_th)
                if win_th > 0:
                    before = len(breaks_df)
                    breaks_df = addToBreaksList( current_dt, pauses, intervals_list, breaks_df, win_th)
                    added_flag = len(breaks_df) > before
                    last_th = win_th
                elif last_th > 0:
                    win_th = last_th
                    before = len(breaks_df)
                    breaks_df = addToBreaksList( current_dt, pauses, intervals_list, breaks_df, last_th)
                    added_flag = len(breaks_df) > before
                #what if the window threshold is 0?
                
            else:
            

                if last_th > 0:
                    win_th = last_th
                    before = len(breaks_df)
                    breaks_df = addToBreaksList(current_dt, pauses, intervals_list, breaks_df, last_th)
                    added_flag = len(breaks_df) > before
                else:
                    # If a user is new and doesnt have more than 4 breaks
                    # and they cannot rely on the past breaks to set a threshold
                    # we need to set a very basic threshold
                    # we set it to window size.
                    # meaning if they paused for a length of time equal to 
                    # the entire window we count it as a break
                    win_th = window
                    last_th = win_th
                    before = len(breaks_df)
                    breaks_df = addToBreaksList(current_dt, pauses, intervals_list, breaks_df, win_th)
                    added_flag = len(breaks_df) > before 



            # We need to move to the next acctive day
            # We can find this inside of the pauses list
            current_index += 1
            current_dt = datetime.strptime(intervals_list[current_index].split('/')[1], '%Y-%m-%d') 

            # this is very useful for debugging
            # you can see how each window is decied as a break or not
            diagnostics.append({
                'dev': dev,
                'win_start': win_start.date(),
                'win_end':   win_end.date(),
                'win_pauses': win_pauses,
                'pause_lengths': ';'.join(map(str, pauses['len'].tolist())),
                'partial_lengths': ';'.join(map(str, partially_included_pauses_list['len'].tolist())),
                'win_th': win_th,
                'last_th': last_th,
                'added_as_break': 'yes' if added_flag else 'no'
            })
            # -----------------------------------------------------------------

    diagnostics_df = pandas.DataFrame(diagnostics)

    return breaks_df, diagnostics_df

def timeline(tables, tf_devs, repo_full_name=None) -> pandas.DataFrame:
    # given 3 files I need you to count the rows per day per user
    # you are given commits.csv issues.csv prs.csv
    # dev (string; developer id/handle)
    # date (date at daily granularity, e.g., YYYY-MM-DD)
    # commits (non-negative integer)
    # prs (non-negative integer)
    # issues (non-negative integer)
    
    issues = tables['issues']
    commits = tables['commits']
    prs = tables['prs_repo']
    issue_activity = tables['issue_activity']
    pr_activity = tables['prs_comments']

    # Convert created_at columns to datetime
    commits['created_at'] = pandas.to_datetime(commits['created_at'])
    issues['created_at'] = pandas.to_datetime(issues['created_at'])
    prs['created_at'] = pandas.to_datetime(prs['created_at'])
    issue_activity['created_at'] = pandas.to_datetime(issue_activity['created_at'])
    pr_activity['created_at'] = pandas.to_datetime(pr_activity['created_at'])

    # We sometimes do not have a author_id column
    # we add a unique identifier ate the start for the column that we used
    # if it has the string "author_login_" at the start use author_login
    # if it has the strung "author_name_" at the start use author_name
    # if it has the string "author_email_" at the start use author_email
    # if there is nothing at the start we use id

    # Count rows per day per user
    user_activity = []
    for dev in tf_devs:
        print(f"{tf_devs.index(dev) + 1} / {len(tf_devs)}") 
        if dev.startswith("author_login") or dev.startswith("author_name") or dev.startswith("author_email"):
            column, dev = dev.split('|', maxsplit=1)
            dev_commits = commits[commits[column] == dev]
            dev_issues = issues[issues[column] == dev]
            dev_prs = prs[prs[column] == dev]
        else:
            column = "author_id"
            dev_commits = commits[commits[column] == dev]
            dev_issues = issues[issues[column] == dev]
            dev_prs = prs[prs[column] == dev]
        
        dev_issue_activity = issue_activity[issue_activity[column] == dev]
        dev_pr_activity = pr_activity[pr_activity[column] == dev]

        # Set created_at as index, then resample to daily frequency and count rows
        daily_commits = dev_commits.set_index('created_at').resample('D').size()
        daily_issues = dev_issues.set_index('created_at').resample('D').size()
        daily_prs = dev_prs.set_index('created_at').resample('D').size()
        daily_issue_activity = dev_issue_activity.set_index('created_at').resample('D').size()
        daily_pr_activity = dev_pr_activity.set_index('created_at').resample('D').size()

        # Combine all dates and fill missing values with 0
        all_dates = daily_commits.index.union(daily_issues.index).union(daily_prs.index)
        
        for date in all_dates:
            user_activity.append({
                'dev': dev,
                'date': date.strftime('%Y-%m-%d'),
                'commits': daily_commits.get(date, 0),
                'prs': daily_prs.get(date, 0),
                'issues': daily_issues.get(date, 0),
                'issue_activity': daily_issue_activity.get(date, 0),
                'pr_activity': daily_pr_activity.get(date, 0)
            })

    user_activity = pandas.DataFrame(user_activity)

    organizationFolder = Path(ORG_BASE) / repo_full_name

    timeline_folder = organizationFolder /  cfg.timeline_folder
    os.makedirs(timeline_folder, exist_ok=True)
        
    timeline_path = Path(timeline_folder, cfg.timeline_file)

    user_activity.to_csv(timeline_path, sep=cfg.CSV_separator, na_rep=cfg.CSV_missing, lineterminator="\n", index=False)

    return user_activity

#-----------------------
# Developer timeline prediction
#------------------------
def build_response(df_in, N, label_col = "state"):

    d = df_in.sort_values(["dev", "date"]).copy()

    y = d[["dev", "date", label_col]].copy()
    
    y["active_to_inactive"]      = False
    y["non_coding_to_inactive"]  = False
    y["active_to_non_coding"]    = False

    for dev, g in d.groupby("dev", sort=False):
        g = g.sort_values("date").copy()
        next_state = g[label_col].shift(-N)  # look-ahead N days

        # current-state masks
        m_active     = (g[label_col] == "ACTIVE")
        m_noncoding  = (g[label_col] == "NON_CODING")

        # next-state masks (t+N)
        m_next_inact = (next_state == "INACTIVE") | (next_state == "GONE")
        m_next_nc    = (next_state == "NON_CODING")

        # assign ONLY within this group's rows
        y.loc[g.index, "active_to_inactive"]     = (m_active & m_next_inact).values
        y.loc[g.index, "non_coding_to_inactive"] = (m_noncoding & m_next_inact).values
        y.loc[g.index, "active_to_non_coding"]   = (m_active & m_next_nc).values

    y["transition_to_inactive"] = (
        y["active_to_inactive"] | y["non_coding_to_inactive"]
    )

    # final numeric response (avoid NaN->int errors)
    y["transition_to_inactive"] = y["transition_to_inactive"].astype("int8")
    y["active_to_inactive"]    = y["active_to_inactive"].astype("int8")
    y["non_coding_to_inactive"] = y["non_coding_to_inactive"].astype("int8")
    y["active_to_non_coding"]  = y["active_to_non_coding"].astype("int8")
    
    return y

def make_confusion_mats(pred_df, thr_rf=0.6, per_dev=False, date_col="date"):
    """
    Reports:
      (A) Row-level metrics: TP, FP, TN, FN, Precision, Recall, FPR, Specificity
      (B) True-window hit rate: contiguous runs where y_true==1; hit if any pred==1 in run
      (C) Predicted-episode precision: contiguous runs where pred==1; TP episode if overlaps any y_true==1
    """
    df = pred_df.copy()
    df = df.dropna(subset=["y_true", "rf_proba"])
    df["y_true"] = df["y_true"].astype(int)
    df["rf_pred"] = (df["rf_proba"] >= thr_rf).astype(int)

    group_cols = ["dev"] if per_dev and "dev" in df.columns else []
    sort_cols = group_cols + ([date_col] if date_col in df.columns else [])
    if sort_cols:
        df = df.sort_values(sort_cols)

    def _contiguous_run_id(mask: pandas.Series) -> pandas.Series:
        # run id increments at each False->True transition
        starts = mask & (~mask.shift(fill_value=False))
        return starts.cumsum()

    def _metrics_one_group(g: pandas.DataFrame) -> pandas.Series:
        y = g["y_true"].values
        p = g["rf_pred"].values

        # --- (A) Row-level confusion
        TP = int(((y == 1) & (p == 1)).sum())
        FP = int(((y == 0) & (p == 1)).sum())
        TN = int(((y == 0) & (p == 0)).sum())
        FN = int(((y == 1) & (p == 0)).sum())

        precision = TP / (TP + FP) if (TP + FP) else np.nan
        recall    = TP / (TP + FN) if (TP + FN) else np.nan
        fpr       = FP / (FP + TN) if (FP + TN) else np.nan
        spec      = TN / (TN + FP) if (TN + FP) else np.nan

        # --- (B) True-window hit rate (your "window accuracy" is actually window recall)
        true_mask = g["y_true"].eq(1)
        if true_mask.any():
            true_run = _contiguous_run_id(true_mask)
            g2 = g.copy()
            g2["true_run_id"] = np.where(true_mask, true_run, np.nan)

            # each true run is "hit" if any pred==1 inside it
            hits = (g2.loc[true_mask]
                      .groupby("true_run_id")["rf_pred"]
                      .apply(lambda s: int((s == 1).any())))
            true_windows_total = int(hits.size)
            true_windows_hit   = int(hits.sum())
            true_windows_missed = true_windows_total - true_windows_hit
            window_recall = true_windows_hit / true_windows_total if true_windows_total else np.nan
        else:
            true_windows_total = 0
            true_windows_hit = 0
            true_windows_missed = 0
            window_recall = np.nan

        # --- (C) Predicted-episode precision (contiguous runs of pred==1)
        pred_mask = g["rf_pred"].eq(1)
        if pred_mask.any():
            pred_run = _contiguous_run_id(pred_mask)
            g3 = g.copy()
            g3["pred_run_id"] = np.where(pred_mask, pred_run, np.nan)

            # episode is TP if overlaps any y_true==1 within that predicted run
            ep = (g3.loc[pred_mask]
                    .groupby("pred_run_id")["y_true"]
                    .apply(lambda s: int((s == 1).any())))
            pred_episodes_total = int(ep.size)
            pred_episodes_tp = int(ep.sum())
            pred_episodes_fp = pred_episodes_total - pred_episodes_tp
            episode_precision = pred_episodes_tp / pred_episodes_total if pred_episodes_total else np.nan
        else:
            pred_episodes_total = 0
            pred_episodes_tp = 0
            pred_episodes_fp = 0
            episode_precision = np.nan

        return pandas.Series({
            # Row-level
            "TP": TP, "FP": FP, "TN": TN, "FN": FN,
            "precision": precision,
            "recall": recall,
            "fpr": fpr,
            "specificity": spec,

            # True-window (event recall)
            "true_windows_total": true_windows_total,
            "true_windows_hit": true_windows_hit,
            "true_windows_missed": true_windows_missed,
            "window_recall": window_recall,

            # Predicted episodes (event precision)
            "pred_episodes_total": pred_episodes_total,
            "pred_episodes_tp": pred_episodes_tp,
            "pred_episodes_fp": pred_episodes_fp,
            "episode_precision": episode_precision,
        })

    if group_cols:
        per = df.groupby(group_cols, dropna=False).apply(_metrics_one_group).reset_index()

        # overall row: sum confusion counts, recompute ratios from sums
        sums = per[["TP","FP","TN","FN",
                    "true_windows_total","true_windows_hit","true_windows_missed",
                    "pred_episodes_total","pred_episodes_tp","pred_episodes_fp"]].sum()

        TP, FP, TN, FN = [int(sums[k]) for k in ["TP","FP","TN","FN"]]
        precision = TP/(TP+FP) if (TP+FP) else np.nan
        recall = TP/(TP+FN) if (TP+FN) else np.nan
        fpr = FP/(FP+TN) if (FP+TN) else np.nan
        spec = TN/(TN+FP) if (TN+FP) else np.nan

        twt = int(sums["true_windows_total"])
        twh = int(sums["true_windows_hit"])
        window_recall = twh/twt if twt else np.nan

        pet = int(sums["pred_episodes_total"])
        petp = int(sums["pred_episodes_tp"])
        episode_precision = petp/pet if pet else np.nan

        overall = pandas.DataFrame([{
            group_cols[0]: "__ALL__",
            **{k:int(v) for k,v in sums.items()},
            "precision": precision, "recall": recall, "fpr": fpr, "specificity": spec,
            "window_recall": window_recall,
            "episode_precision": episode_precision,
        }])

        return {"rf": pandas.concat([per, overall], ignore_index=True)}
    else:
        return {"rf": _metrics_one_group(df).to_frame().T}
    
def run_prediction_pipeline(
    df,
    repo_key,
    tf_devs,
    response_col,
    predictor_cols,
    window_size=90,
    epochs=25):

    # ---- SPLIT ----
    train_df, test_df = test_train_split_method(
        df=df,
        pred_cols=predictor_cols,
        response_col=response_col,
        method="Per Dev",
        tf_devs=tf_devs
    )

    # ---- BUILD SEQUENCES ----
    Xtr_tensor, ytr_tensor = build_rolling_sequences(
        train_df,
        feature_cols=predictor_cols,
        label_col=response_col,
        window_size=window_size
    )

    Xte_tensor, yte_tensor = build_rolling_sequences(
        test_df,
        feature_cols=predictor_cols,
        label_col=response_col,
        window_size=window_size
    )

    # ---- MODEL ----
    model = DeveloperLSTM(
        input_size=Xtr_tensor.shape[2],
        hidden_size=64,
        num_layers=2,
        output_size=1
    )

    criterion = nn.BCEWithLogitsLoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)

    # ---- TRAIN LOOP ----
    model.train()

    for epoch in range(epochs):

        optimizer.zero_grad()

        outputs = model(Xtr_tensor)
        loss = criterion(outputs, ytr_tensor)

        loss.backward()
        optimizer.step()

        print(f"Epoch {epoch+1}/{epochs}, Loss: {loss.item():.4f}")

    # ---- EVALUATE ----
    model.eval()
    with torch.no_grad():
        if len(Xte_tensor) > 0:
            test_outputs = model(Xte_tensor)

    evaluate_and_plot_developer(
        model,
        test_df,
        Xte_tensor,
        yte_tensor,
        feature_cols=predictor_cols,
        window_size=window_size
    )

    return model

def view_matrix(train_df, test_df, response_col, predictor_cols):

    train_cols = predictor_cols + [response_col, "repo", "dev", "date"]
    test_cols = predictor_cols + [response_col, "repo", "dev", "date"]

    view_df(train_df[train_cols], name="Training predictors")
    view_df(train_df[test_cols], name="Training responce Data")
    view_df(test_df[test_cols], name="Testing responce")

def test_train_split_method(
    df,
    pred_cols,
    response_col,
    method="Per Dev",
    tf_devs=None,
    repos=None,
    split_ratio=0.3,
    random_seed=42):

    random.seed(random_seed)

    if method == "Per Dev":

        n = len(tf_devs)
        size_test = int(n * split_ratio)

        test_devs = random.choices(tf_devs, k=size_test)
        train_devs = random.choices(tf_devs, k=n - size_test)

        test_df = df[df["dev"].isin(test_devs)].copy()
        train_df = df[df["dev"].isin(train_devs)].copy()

    elif method == "Per Repo":

        n = len(repos)
        size_test = int(n * split_ratio)

        test_repos = random.choices(repos, k=size_test)
        train_repos = random.choices(repos, k=n - size_test)

        test_df = df[df["repo"].isin(test_repos)].copy()
        train_df = df[df["repo"].isin(train_repos)].copy()

    else:
        raise ValueError("Invalid method selected")

    # ---- SCALE AFTER SPLIT ----
    scaler = StandardScaler()
    train_df[pred_cols] = scaler.fit_transform(train_df[pred_cols])
    test_df[pred_cols] = scaler.transform(test_df[pred_cols])

    return train_df, test_df

class DeveloperLSTM(nn.Module):
    def __init__(self, input_size, hidden_size, num_layers, output_size, dropout=0.2):
        super(DeveloperLSTM, self).__init__()
        
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout
        )
        
        self.fc = nn.Linear(hidden_size, output_size)
    
    def forward(self, x):
        lstm_out, _ = self.lstm(x)
        
        # Take last time step
        last_output = lstm_out[:, -1, :]
        
        out = self.fc(last_output)
        return out 

# ── NEW helper: same as build_rolling_sequences but also tracks dev/date ──────

def build_rolling_sequences_with_meta(
    df, feature_cols, label_col,
    dev_col="dev", date_col="date", window_size=90):
    """
    Like build_rolling_sequences, but also returns the (dev, date) of the
    TARGET row (index i) so predictions can be merged back to the dataframe.
    """
    all_sequences, all_labels, all_devs, all_dates = [], [], [], []

    df = df.sort_values([dev_col, date_col]).copy()

    for dev, group in df.groupby(dev_col):
        group  = group.sort_values(date_col)
        X_vals = group[feature_cols].values
        y_vals = group[label_col].values
        dates  = group[date_col].values

        if len(group) < window_size:
            continue

        for i in range(window_size, len(group)):
            all_sequences.append(X_vals[i - window_size:i])
            all_labels.append(y_vals[i])
            all_devs.append(dev)
            all_dates.append(dates[i])

    X_tensor = torch.tensor(all_sequences, dtype=torch.float32)
    y_tensor = torch.tensor(all_labels, dtype=torch.long)

    return X_tensor, y_tensor, all_devs, all_dates


# ── NEW: build final_df with prob columns from test set ───────────────────────
def new_evaluation_fuction(test_df, model, label_encoder,
                           predictor_cols, encoded_col,
                           window_size=90):
    """
    Runs the trained model on test_df and returns a copy of the test rows
    (only those that have a full rolling window) with extra columns:
        prob_<class0>, prob_<class1>, prob_<class2>, prob_<class3>
        predicted_state

    Parameters
    ----------
    test_df        : preprocessed test DataFrame (returned by run_prediction_pipeline_2)
    model          : trained DeveloperLSTM
    label_encoder  : fitted LabelEncoder (to recover class names)
    predictor_cols : full feature column list (with one-hot dev cols)
    encoded_col    : name of the integer-encoded label column
    window_size    : must match training
    """

    X_te, y_te, meta_devs, meta_dates = build_rolling_sequences_with_meta(
        test_df,
        feature_cols=predictor_cols,
        label_col=encoded_col,
        window_size=window_size
    )

    model.eval()
    with torch.no_grad():
        logits = model(X_te)                            # (N, num_classes)
        probs  = torch.softmax(logits, dim=1)           # (N, num_classes)
        pred_classes = torch.argmax(probs, dim=1)       # (N,)

    # ── numpy fix: use cpu().detach().numpy() ─────────────────────────────────
    probs_np    = np.array(probs.cpu().detach().tolist())
    pred_cls_np = np.array(pred_classes.cpu().detach().tolist())
    pred_labels    = label_encoder.inverse_transform(pred_cls_np)
    class_names    = label_encoder.classes_             # e.g. ['active','inactive',...]

    # ── build a results dataframe keyed on (dev, date) ───────────────────────
    results = pd.DataFrame({"dev": meta_devs, "date": pd.to_datetime(meta_dates)})
    results["predicted_state"] = pred_labels

    for idx, cls in enumerate(class_names):
        results[f"prob_{cls}"] = probs_np[:, idx]

    # ── merge back onto the original test rows ────────────────────────────────
    test_df = test_df.copy()
    test_df["date"] = pd.to_datetime(test_df["date"])

    final_df = test_df.merge(results, on=["dev", "date"], how="inner")

    print(f"\nnew_evaluation_fuction: {len(final_df)} rows with predictions "
          f"across {final_df['dev'].nunique()} developers.")
    print(f"Probability columns: {[f'prob_{c}' for c in class_names]}")

    return final_df


# ── UPDATED: run_prediction_pipeline_2 now returns test_df + col info ─────────
def run_prediction_pipeline_2(df,
    repo_key,
    tf_devs,
    response_col,
    predictor_cols,
    window_size=90,
    epochs=100):

    if isinstance(response_col, list):
        response_col = response_col[0]

    # ---- Encode state labels → integers ----
    label_encoder = LabelEncoder()
    encoded_col   = response_col + "_encoded"
    df[encoded_col] = label_encoder.fit_transform(df[response_col].astype(str))

    print(f"State classes: {list(enumerate(label_encoder.classes_))}")

    # ---- One-hot encode dev ----
    dev_encoder    = OneHotEncoder(sparse_output=False)
    dev_encoded    = dev_encoder.fit_transform(df[["dev"]])
    dev_encoded_df = pd.DataFrame(dev_encoded, columns=dev_encoder.get_feature_names_out(["dev"]))
    df = pd.concat([df.reset_index(drop=True), dev_encoded_df.reset_index(drop=True)], axis=1)

    df["date"]      = pd.to_datetime(df["date"])
    df["win_start"] = pd.to_datetime(df["win_start"])
    df["win_end"]   = pd.to_datetime(df["win_end"])

    predictor_cols = predictor_cols + list(dev_encoded_df.columns)

    df["break_day"] = df["break_day"].astype(int)
    df["th"]        = df["th"].astype(float)
    df["len"]       = df["len"].astype(float)

    bool_string_cols = ["added_as_break"]
    for col in bool_string_cols:
        if col in df.columns and df[col].dtype == object:
            df[col] = df[col].map({"yes": 1, "no": 0, "True": 1, "False": 0, True: 1, False: 0})

    df = df.fillna(0)

    # ---- SPLIT ----
    train_df, test_df = test_train_split_method(
        df=df,
        pred_cols=predictor_cols,
        response_col=encoded_col,
        method="Per Dev",
        tf_devs=tf_devs
    )

    # ---- BUILD SEQUENCES ----
    Xtr_tensor, ytr_tensor = build_rolling_sequences_with_meta(
        train_df, feature_cols=predictor_cols,
        label_col=encoded_col, window_size=window_size
    )[:2]   # only need tensors for training

    Xte_tensor, yte_tensor = build_rolling_sequences_with_meta(
        test_df, feature_cols=predictor_cols,
        label_col=encoded_col, window_size=window_size
    )[:2]

    num_classes = len(label_encoder.classes_)

    # ---- MODEL ----
    model = DeveloperLSTM(
        input_size=Xtr_tensor.shape[2],
        hidden_size=64,
        num_layers=5,
        output_size=num_classes
    )

    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=0.005)

    # ---- TRAIN ----
    model.train()
    for epoch in range(epochs):
        optimizer.zero_grad()
        outputs = model(Xtr_tensor)
        loss    = criterion(outputs, ytr_tensor)
        loss.backward()
        optimizer.step()

        timer()
        acuracy = (outputs.argmax(dim=1) == ytr_tensor).float().mean().item()
        print(f"Epoch {epoch+1}/{epochs}, Loss: {loss.item():.4f}, Accuracy: {acuracy:.4f}")

    # ── return test_df so new_evaluation_fuction can use it ──────────────────
    return model, label_encoder, test_df, predictor_cols, encoded_col

def timer():
    now = datetime.now()
    print(f"timebetween calls: {(now - LAST_CALLED[0]).total_seconds() if LAST_CALLED[0] else 'N/A'} seconds")
    LAST_CALLED[0] = now

#def build_rolling_sequences(
#    df,
#    feature_cols,
#    label_col="transition_to_inactive",
#    dev_col="dev",
#    date_col="date",
#    window_size=90):
#    """
#    Builds rolling sequences per developer.
#    
#    Returns:
#        X_tensor: (num_sequences, window_size, num_features)
#        y_tensor: (num_sequences, 1)
#    """
#    
#    all_sequences = []
#    all_labels = []
#
#    # Ensure proper sorting
#    df = df.sort_values([dev_col, date_col]).copy()
#
#    for dev, group in df.groupby(dev_col):
#
#        group = group.sort_values(date_col)
#
#        X_values = group[feature_cols].values
#        y_values = group[label_col].values
#
#        # Not enough history → skip
#        if len(group) < window_size:
#            continue
#
#        # Rolling windows
#        for i in range(window_size, len(group)):
#            X_seq = X_values[i - window_size:i]
#            y_target = y_values[i]
#
#            all_sequences.append(X_seq)
#            all_labels.append(y_target)
#
#    # Convert to tensors
#    X_tensor = torch.tensor(all_sequences, dtype=torch.float32)
#    y_tensor = torch.tensor(all_labels, dtype=torch.float32).unsqueeze(1)
#
#    return X_tensor, y_tensor


#-----------------------
#visualization functions
#------------------------  


# ── state colours used by make_state_graph ────────────────────────────────────
_STATE_COLORS = {
    "ACTIVE":     "#2ecc71",   # green
    "NON_CODING": "#f1c40f",   # yellow
    "INACTIVE":   "#e74c3c",   # red
    "GONE":       "#95a5a6",   # grey
    "UNKNOWN":    "#bdc3c7",   # light grey
}

def _shade_states(ax, df, date_col, state_col, alpha=0.3):
    """Shade axis background by state, grouping consecutive equal-state rows."""
    if df.empty:
        return
    dates  = df[date_col].values
    states = df[state_col].values
    i = 0
    while i < len(states):
        j = i + 1
        while j < len(states) and states[j] == states[i]:
            j += 1
        color = _STATE_COLORS.get(str(states[i]), "#7f8c8d")
        end   = dates[j] if j < len(dates) else dates[-1] + np.timedelta64(2, "D")
        ax.axvspan(dates[i], end, color=color, alpha=alpha, zorder=1, linewidth=0)
        i = j

def make_state_graph(df):
    """
    Interactive Streamlit visualization of one developer's activity vs. state.

    Layout (3 panels):
      0 – Full-timeline state strip  (thin coloured bar, window highlighted)
      1 – Coding activity (log scale) in the selected 1-year window
      2 – Non-coding activity (log scale) in the selected 1-year window

    Widgets:
      • selectbox  – pick which developer to inspect
      • slider     – drag the 1-year window left / right over the full timeline
    """
    import matplotlib.patches as mpatches

    if df is None or df.empty:
        st.info("No data to display yet — run the pipeline first.")
        return
    # ── developer selector ─────────────────────────────────────────────────────
    devs = sorted(df["dev"].dropna().unique())

    dev  = st.selectbox("Select developer", devs, key="msg_dev_2")

    wdf = df[df["dev"] == dev].copy()
    wdf["date"]  = pd.to_datetime(wdf["date"])
    wdf = wdf.sort_values("date").reset_index(drop=True)
    wdf["state"] = wdf["state"].fillna("UNKNOWN")

    for col in ("commits", "prs", "issues", "issue_activity", "pr_activity"):
        wdf[col] = pd.to_numeric(wdf[col], errors="coerce").fillna(0)

    wdf["coding_total"]   = wdf["commits"] + wdf["prs"]
    wdf["noncoding_total"] = wdf["issues"] + wdf["issue_activity"] + wdf["pr_activity"]
    
    wdf["commits_log"] = np.log1p(wdf["commits"])
    wdf["prs_log"] = np.log1p(wdf["prs"])

    wdf["issues_log"] = np.log1p(wdf["issues"])
    wdf["issue_activity_log"] = np.log1p(wdf["issue_activity"])
    wdf["pr_activity_log"] = np.log1p(wdf["pr_activity"])

    wdf["activity_log"]   = np.log1p(wdf["coding_total"])
    wdf["non_coding_log"] = np.log1p(wdf["noncoding_total"])

    min_date   = wdf["date"].min()
    max_date   = wdf["date"].max()
    total_days = (max_date - min_date).days

    # ── 1-year window slider ───────────────────────────────────────────────────
    WINDOW = 365

    default_start  = (max_date - pd.Timedelta(days=WINDOW)).to_pydatetime().date()
    slider_max     = default_start
    win_start_date = st.slider(
        "Window start  (shows 1 year forward from this date)",
        min_value = min_date.to_pydatetime().date(),
        max_value = slider_max,
        value     = default_start,
        step      = timedelta(days=7),
        key       = "msg_window_1",
    )
    win_start = pd.Timestamp(win_start_date)


    win_end = win_start + pd.Timedelta(days=WINDOW)

    # filter to window
    mask  = (wdf["date"] >= win_start) & (wdf["date"] <= win_end)
    wdf_w = wdf[mask].copy()

    # ── figure ─────────────────────────────────────────────────────────────────
    BG  = "#1c1c2e"
    fig, axes = plt.subplots(
        3, 1, figsize=(14, 9), facecolor=BG,
        gridspec_kw={"height_ratios": [0.4, 2, 2]},
    )
    fig.suptitle(f"Developer: {dev}", color="white", fontsize=12)
    fig.subplots_adjust(hspace=0.55)

    # Panel 0 – full timeline strip -----------------------------------------
    ax0 = axes[0]
    ax0.set_facecolor(BG)
    _shade_states(ax0, wdf, "date", "state", alpha=1.0)
    ax0.axvspan(win_start, win_end, color="black", alpha=0.5, zorder=2)
    ax0.set_xlim(min_date, max_date)
    ax0.set_yticks([])
    ax0.set_title("Full timeline  (white band = current window)", color="white", fontsize=9, pad=4)
    ax0.tick_params(colors="white", labelsize=8)
    ax0.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax0.xaxis.set_major_locator(mdates.YearLocator())
    for sp in ax0.spines.values():
        sp.set_visible(False)

    # Panel 1 – coding activity ---------------------------------------------
    ax1 = axes[1]
    ax1.set_facecolor(BG)
    _shade_states(ax1, wdf_w, "date", "state", alpha=0.25)
    active = wdf_w[wdf_w["activity_log"] > 0]
    if not active.empty:
        #this is the combination of the commits_log + prs_log
        #ax1.bar(active["date"], active["activity_log"], width=1, color="#3498db", alpha=0.9, zorder=3)

        # HERE LOOK
        ax1.bar(active["date"], active["commits_log"], width=1,
                color="black", alpha=0.9, zorder=3)
        ax1.bar(active["date"], active["prs_log"], width=1,
                color="white", alpha=0.9, zorder=3)

    ax1.set_xlim(win_start, win_end)
    ax1.set_ylabel("log(commits + PRs)", color="white", fontsize=9)
    ax1.set_title("Coding Activity", color="white", fontsize=10, pad=4)
    ax1.tick_params(colors="white", labelsize=8)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax1.legend(["commits", "PRs"], loc="upper left", facecolor=BG, labelcolor="white", fontsize=8, framealpha=0)
    plt.setp(ax1.get_xticklabels(), rotation=30, ha="right")
    for sp in ax1.spines.values():
        sp.set_color("#444")

    # Panel 2 – non-coding activity -----------------------------------------
    ax2 = axes[2]
    ax2.set_facecolor(BG)
    _shade_states(ax2, wdf_w, "date", "state", alpha=0.25)
    nc = wdf_w[wdf_w["non_coding_log"] > 0]
    if not nc.empty:
        ax2.bar(nc["date"], nc["non_coding_log"], width=1,
                color="#e67e22", alpha=0.9, zorder=3)
    ax2.set_xlim(win_start, win_end)
    ax2.set_ylabel("log(issues + comments + PRs)", color="white", fontsize=9)
    ax2.set_title("Non-Coding Activity", color="white", fontsize=10, pad=4)
    ax2.tick_params(colors="white", labelsize=8)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    plt.setp(ax2.get_xticklabels(), rotation=30, ha="right")
    for sp in ax2.spines.values():
        sp.set_color("#444")

    # legend
    patches = [mpatches.Patch(color=c, label=s) for s, c in _STATE_COLORS.items()]
    fig.legend(handles=patches, loc="upper right", ncol=5,
               facecolor=BG, labelcolor="white", fontsize=8, framealpha=0)

    st.pyplot(fig)
    plt.close(fig)

# ── UPDATED make_state_graph: adds prob panel when prob cols present ───────────
def make_state_graph_2(df):
    import matplotlib.patches as mpatches

    if df is None or df.empty:
        st.info("No data to display yet — run the pipeline first.")
        return

    # detect probability columns if they exist
    prob_cols = [c for c in df.columns if c.startswith("prob_")]
    has_probs = len(prob_cols) > 0

    devs = sorted(df["dev"].dropna().unique())
    dev  = st.selectbox("Select developer", devs, key="msg_dev")

    wdf = df[df["dev"] == dev].copy()
    wdf["date"]  = pd.to_datetime(wdf["date"])
    wdf = wdf.sort_values("date").reset_index(drop=True)
    wdf["state"] = wdf["state"].fillna("UNKNOWN")

    for col in ("commits", "prs", "issues", "issue_activity", "pr_activity"):
        wdf[col] = pd.to_numeric(wdf[col], errors="coerce").fillna(0)

    wdf["coding_total"]    = wdf["commits"] + wdf["prs"]
    wdf["noncoding_total"] = wdf["issues"] + wdf["issue_activity"] + wdf["pr_activity"]
    wdf["commits_log"]     = np.log1p(wdf["commits"])
    wdf["prs_log"]         = np.log1p(wdf["prs"])
    wdf["issues_log"]      = np.log1p(wdf["issues"])
    wdf["issue_activity_log"] = np.log1p(wdf["issue_activity"])
    wdf["pr_activity_log"] = np.log1p(wdf["pr_activity"])
    wdf["activity_log"]    = np.log1p(wdf["coding_total"])
    wdf["non_coding_log"]  = np.log1p(wdf["noncoding_total"])

    min_date = wdf["date"].min()
    max_date = wdf["date"].max()
    WINDOW   = 365

    default_start  = (max_date - pd.Timedelta(days=WINDOW)).to_pydatetime().date()
    slider_max     = default_start
    win_start_date = st.slider(
        "Window start  (shows 1 year forward from this date)",
        min_value = min_date.to_pydatetime().date(),
        max_value = slider_max,
        value     = default_start,
        step      = timedelta(days=7),
        key       = "msg_window",
    )
    win_start = pd.Timestamp(win_start_date)
    win_end   = win_start + pd.Timedelta(days=WINDOW)

    mask  = (wdf["date"] >= win_start) & (wdf["date"] <= win_end)
    wdf_w = wdf[mask].copy()

    BG = "#1c1c2e"

    # ── figure layout: 3 panels always, 4th only if prob cols exist ───────────
    n_panels     = 4 if has_probs else 3
    height_ratios = [0.4, 2, 2, 2] if has_probs else [0.4, 2, 2]

    fig, axes = plt.subplots(
        n_panels, 1, figsize=(14, 11 if has_probs else 9),
        facecolor=BG,
        gridspec_kw={"height_ratios": height_ratios},
    )
    fig.suptitle(f"Developer: {dev}", color="white", fontsize=12)
    fig.subplots_adjust(hspace=0.6)

    # Panel 0 – full timeline strip
    ax0 = axes[0]
    ax0.set_facecolor(BG)
    _shade_states(ax0, wdf, "date", "state", alpha=1.0)
    ax0.axvspan(win_start, win_end, color="black", alpha=0.5, zorder=2)
    ax0.set_xlim(min_date, max_date)
    ax0.set_yticks([])
    ax0.set_title("Full timeline  (white band = current window)", color="white", fontsize=9, pad=4)
    ax0.tick_params(colors="white", labelsize=8)
    ax0.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax0.xaxis.set_major_locator(mdates.YearLocator())
    for sp in ax0.spines.values():
        sp.set_visible(False)

    # Panel 1 – coding activity
    ax1 = axes[1]
    ax1.set_facecolor(BG)
    _shade_states(ax1, wdf_w, "date", "state", alpha=0.25)
    active = wdf_w[wdf_w["activity_log"] > 0]
    if not active.empty:
        ax1.bar(active["date"], active["commits_log"], width=1, color="black", alpha=0.9, zorder=3)
        ax1.bar(active["date"], active["prs_log"],     width=1, color="white", alpha=0.9, zorder=3)
    ax1.set_xlim(win_start, win_end)
    ax1.set_ylabel("log(commits + PRs)", color="white", fontsize=9)
    ax1.set_title("Coding Activity", color="white", fontsize=10, pad=4)
    ax1.tick_params(colors="white", labelsize=8)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax1.legend(["commits", "PRs"], loc="upper left", facecolor=BG, labelcolor="white", fontsize=8, framealpha=0)
    plt.setp(ax1.get_xticklabels(), rotation=30, ha="right")
    for sp in ax1.spines.values():
        sp.set_color("#444")

    # Panel 2 – non-coding activity
    ax2 = axes[2]
    ax2.set_facecolor(BG)
    _shade_states(ax2, wdf_w, "date", "state", alpha=0.25)
    nc = wdf_w[wdf_w["non_coding_log"] > 0]
    if not nc.empty:
        ax2.bar(nc["date"], nc["non_coding_log"], width=1, color="#e67e22", alpha=0.9, zorder=3)
    ax2.set_xlim(win_start, win_end)
    ax2.set_ylabel("log(issues + comments)", color="white", fontsize=9)
    ax2.set_title("Non-Coding Activity", color="white", fontsize=10, pad=4)
    ax2.tick_params(colors="white", labelsize=8)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    plt.setp(ax2.get_xticklabels(), rotation=30, ha="right")
    for sp in ax2.spines.values():
        sp.set_color("#444")

    # Panel 3 – predicted state probabilities (only if prob cols exist) ────────
    if has_probs:
        ax3 = axes[3]
        ax3.set_facecolor(BG)

        # filter to window rows that actually have predictions
        prob_w = wdf_w.dropna(subset=prob_cols).sort_values("date")

        if not prob_w.empty:
            # colour palette for each class — reuse _STATE_COLORS where possible
            palette = ["#2ecc71", "#e74c3c", "#3498db", "#f39c12",
                       "#9b59b6", "#1abc9c", "#e67e22", "#ecf0f1"]

            dates      = prob_w["date"].values
            prob_matrix = prob_w[prob_cols].values          # (T, num_classes)
            class_labels = [c.replace("prob_", "") for c in prob_cols]

            # stacked area chart
            ax3.stackplot(
                dates,
                prob_matrix.T,                              # (num_classes, T)
                labels=class_labels,
                colors=palette[:len(prob_cols)],
                alpha=0.85
            )
            ax3.legend(loc="upper left", facecolor=BG, labelcolor="white",
                       fontsize=8, framealpha=0)
        else:
            ax3.text(0.5, 0.5, "No predictions in this window",
                     ha="center", va="center", color="white",
                     transform=ax3.transAxes, fontsize=9)

        ax3.set_xlim(win_start, win_end)
        ax3.set_ylim(0, 1)
        ax3.set_ylabel("Probability", color="white", fontsize=9)
        ax3.set_title("Predicted State Probabilities  (LSTM)", color="white", fontsize=10, pad=4)
        ax3.tick_params(colors="white", labelsize=8)
        ax3.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        ax3.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        plt.setp(ax3.get_xticklabels(), rotation=30, ha="right")
        for sp in ax3.spines.values():
            sp.set_color("#444")

    # legend for state colours
    patches = [mpatches.Patch(color=c, label=s) for s, c in _STATE_COLORS.items()]
    fig.legend(handles=patches, loc="upper right", ncol=5,
               facecolor=BG, labelcolor="white", fontsize=8, framealpha=0)

    st.pyplot(fig)
    plt.close(fig)

def view_df(df, name="DataFrame"):
    ''' Simple HTML table viewer for DataFrames '''
    import tempfile, webbrowser
    html = "\n".join([
        "<meta charset='utf-8'>",
        "<style>body{font-family:system-ui,Segoe UI,Arial}table{border-collapse:collapse}th,td{border:1px solid #ddd;padding:6px}th{position:sticky;top:0;background:#fafafa}</style>",
        f"<h3>{name}</h3>",
        df.to_html(index=False, escape=False),
    ])
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".html", encoding="utf-8") as f:
        f.write(html)
        webbrowser.open("file://" + f.name)

def save_model_artifacts(model_folder, model, label_encoder, test_df, pred_cols_full, encoded_col):
    model_folder = Path(model_folder)
    os.makedirs(model_folder, exist_ok=True)

    # model weights
    torch.save(model.state_dict(), model_folder / "model_weights.pth")

    # save model config so we can rebuild the architecture on load
    torch.save({
        "input_size":  model.lstm.input_size,
        "hidden_size": model.lstm.hidden_size,
        "num_layers":  model.lstm.num_layers,
        "output_size": model.fc.out_features,
    }, model_folder / "model_config.pth")

    # label encoder
    joblib.dump(label_encoder, model_folder / "label_encoder.pkl")

    # test dataframe
    test_df.to_csv(model_folder / "test_df.csv", index=False)

    # predictor cols + encoded col (plain text/json)
    with open(model_folder / "pred_cols_full.json", "w") as f:
        json.dump(pred_cols_full, f)

    with open(model_folder / "encoded_col.txt", "w") as f:
        f.write(encoded_col)

    print(f"Model artifacts saved to: {model_folder}")

def load_model_artifacts(model_folder):
    model_folder = Path(model_folder)

    # rebuild model architecture from saved config
    config = torch.load(model_folder / "model_config.pth")
    model  = DeveloperLSTM(
        input_size  = config["input_size"],
        hidden_size = config["hidden_size"],
        num_layers  = config["num_layers"],
        output_size = config["output_size"],
    )
    model.load_state_dict(torch.load(model_folder / "model_weights.pth"))
    model.eval()

    # label encoder
    label_encoder = joblib.load(model_folder / "label_encoder.pkl")

    # test dataframe
    test_df = pd.read_csv(model_folder / "test_df.csv")
    test_df["date"] = pd.to_datetime(test_df["date"])

    # predictor cols + encoded col
    with open(model_folder / "pred_cols_full.json", "r") as f:
        pred_cols_full = json.load(f)

    with open(model_folder / "encoded_col.txt", "r") as f:
        encoded_col = f.read().strip()

    print(f"Model artifacts loaded from: {model_folder}")
    return model, label_encoder, test_df, pred_cols_full, encoded_col

#-----------------------
# main streamlit app
#------------------------

def main():
    
    st.set_page_config(page_title="Dev Inactivity Demo", layout="wide")
    #user input
    # we need to ask the user for a few things
    # we need the test train split (leave one dev or repo out)
    # we need to know if its dev mode
    # we need to be able to add anything to this in the future
    # we are using streamlit for this

    if 'list_of_repos' not in st.session_state:
        st.session_state.list_of_repos = [
            "Rdatatable/data.table",
            "aseprite/aseprite"
        ]



    st.title("Developer Inactivity Prediction")
    st.write("Configure the settings for predicting developer inactivity.")
    st.write("Please provide the necessary inputs below:")

    # make a text box that users can write repos urls into
    repo_url = st.text_input("Enter GitHub Repository URL (e.g., https://github.com/user/repo) or (org/repo):")
    list_of_repos = ["atom/atom", "Rdatatable/data.table"]
    # if "add to queue" button is pressed
    if st.button("Add to Queue"):
        if repo_url.strip():  # Only add if input is not empty
            gitRepoName = repo_url.replace('https://github.com/', '').strip()
            # Add to session state list instead of local variable
            if gitRepoName not in st.session_state.list_of_repos:
                st.session_state.list_of_repos.append(gitRepoName)
                st.success(f"Added '{gitRepoName}' to queue!")
            else:
                st.warning(f"'{gitRepoName}' is already in the queue.")
        else:
            st.error("Please enter a repository URL or name.")

    if st.button("Clear Queue"):
        st.session_state.list_of_repos = []
        st.success("Queue cleared!")

    if st.button("Add all to Queue"):
        st.session_state.list_of_repos = [
            "Rdatatable/data.table",
            "aseprite/aseprite",
        ]


    st.caption(f"Selected repos: **{', '.join(st.session_state.list_of_repos)}**")

    # (Later buttons can use `repo_key` and `paths`, e.g., Update, Label, Predict)

    st.divider()
    if "overwrite_responce" not in st.session_state:
        st.session_state.overwrite_responce = False

    # Toggle switch
    st.session_state.overwrite_responce = st.toggle(
        "Overwrite cached KnowledgeDistribution outputs",
        value=st.session_state.overwrite_responce,
        help="If enabled, existing DOE / TF files will be recomputed.", 
        key = 1
    )

    if st.button("Responce"):

        number_of_repos = len(st.session_state.list_of_repos)

        prediction_df = pd.DataFrame()

        for repo in st.session_state.list_of_repos:

            print(f"Processing repository: {repo}")
            
            #main
            main_folder = ORG_BASE / repo 
            os.makedirs(main_folder, exist_ok=True)
            # collection data
            collection_folder = main_folder / cfg.collection_folder
            os.makedirs(collection_folder, exist_ok=True)
            # TF_developers_folder
            TF_developers_folder = Path(main_folder, cfg.TF_developers_folder)
            os.makedirs(TF_developers_folder, exist_ok=True)
            # TIMELINE FOLDER
            timeline_folder = main_folder / cfg.timeline_folder
            os.makedirs(timeline_folder, exist_ok=True)
            # LABELED TIMELINE FOLDER
            labeled_timeline_folder = main_folder / cfg.labeled_timeline_folder
            os.makedirs(labeled_timeline_folder, exist_ok=True)

            #----------------------
            # Step 1: Load Data
            #---------------------- 
            print("\n\nStep 1: Loading raw data")
            raw_data_tables = load_users_activity(repo_full_name=repo)
                        
            #----------------------
            # Step 2: Truck Factor
            #----------------------
            print("\n\nStep 2: Calculating Truck Factor")
            tf, tf_devs, author_map, DOE = kd.main(repo_full_name=repo, tables=raw_data_tables, overwrite= st.session_state.overwrite_responce)


            #----------------------
            # Basic Timeline
            #----------------------
            print("\n\nStep 3: Generating Basic Timeline")
            out = timeline(raw_data_tables, tf_devs, repo_full_name = repo)



            #----------------------
            # Label Timeline
            #----------------------
            print("\n\nStep 4: Labeling Developer Activity Timeline")
            user_labeled_timeline = label_developers_activity(repo=repo, over_write= st.session_state.overwrite_responce)
            #I want to combine all data set together long way
            prediction_df = pd.concat([prediction_df, user_labeled_timeline], ignore_index=True)

        #----------------------
        # Analyse Responce
        #----------------------
        # Persist across Streamlit reruns triggered by widget interaction
        st.session_state["prediction_df"] = prediction_df
        st.success(f"Pipeline complete — {len(prediction_df)} rows across {prediction_df['dev'].nunique()} developers.")

    # ── interactive graph — lives OUTSIDE the button so it survives reruns ────
    if "prediction_df" in st.session_state:
        st.subheader("Developer Activity Explorer")
        make_state_graph(st.session_state["prediction_df"])

    st.divider()
    if "overwrite" not in st.session_state:
        st.session_state.overwrite = False

    # Toggle switch
    st.session_state.overwrite = st.toggle(
        "Overwrite cached KnowledgeDistribution outputs",
        value=st.session_state.overwrite,
        help="If enabled, existing DOE / TF files will be recomputed.", 
        key = 2
    )

    if st.button("Predictors"):
        count = 0 

        number_of_repos = len(st.session_state.list_of_repos)

        users_labeled_timeline_combined = []


        for repo in st.session_state.list_of_repos:

            print(f"\nProcessing repository: {repo}")
            
            # file paths setup
            #main
            main_folder = ORG_BASE / repo 
            os.makedirs(main_folder, exist_ok=True)
            # collection data
            collection_folder = main_folder / cfg.collection_folder
            os.makedirs(collection_folder, exist_ok=True)
            # TF_developers_folder
            TF_developers_folder = Path(main_folder, cfg.TF_developers_folder)
            os.makedirs(TF_developers_folder, exist_ok=True)
            # TIMELINE FOLDER
            timeline_folder = main_folder / cfg.timeline_folder
            os.makedirs(timeline_folder, exist_ok=True)
            # LABELED TIMELINE FOLDER
            labeled_timeline_folder = main_folder / cfg.labeled_timeline_folder
            os.makedirs(labeled_timeline_folder, exist_ok=True)
            # social network metrics folder
            social_network_metrics_folder = main_folder / cfg.social_network_metrics_folder
            os.makedirs(social_network_metrics_folder, exist_ok=True)

            #----------------------
            # Load Data
            #---------------------- 
            print("\n\nStep 1: Loading raw data")
            raw_data_tables = load_users_activity(repo_full_name=repo)
                        
            #----------------------
            # Truck Factor
            #----------------------
            print("\n\nStep 2: Calculating Truck Factor")
            tf, tf_devs, author_map, DOE = kd.main(repo_full_name=repo, tables=raw_data_tables, overwrite= st.session_state.overwrite)
        
            #----------------------
            # Basic Timeline
            #----------------------
            print("\n\nStep 3: Generating Basic Timeline")
            out = timeline(raw_data_tables, tf_devs)
            out.to_csv(Path(timeline_folder, cfg.timeline_file), sep=cfg.CSV_separator)
            print(f"Generated basic timeline data.")

        
            #----------------------
            # Social Network Metrics
            #----------------------
        
            issue_interactions_interactions_metrics, pr_interactions_interactions_metrics, combined_interactions = stn.main(repo, tf_devs, tables= raw_data_tables)
        
            issue_interactions = issue_interactions_interactions_metrics.rename(columns={
                "from_user": "dev",
                "day": "date",
                "author_id": "author_id",
                "author_name": "author_name",
                "author_login": "author_login",
                "author_email": "author_email",
                "total_interactions": "issue_total_interactions",
                "unique_partners": "issue_unique_partners",
                "items_touched": "issue_items_touched",
                "new_interactions": "issue_new_interactions",
                "new_relationships": "issue_new_relationships",
                "total_relationships": "issue_total_relationships",
                "repeat_partners": "issue_repeat_partners",
                "avg_time_delta": "issue_avg_time_delta",
                "min_response_time": "issue_min_response_time",
                "avg_response_time_distance_1": "issue_avg_response_time_distance_1",
                "avg_interactions_per_user": "issue_avg_interactions_per_user",
                "avg_distance_away": "issue_avg_distance_away"
                })
            pr_interactions = pr_interactions_interactions_metrics.rename(columns={
                "from_user": "dev",
                "day": "date",
                "author_id": "author_id",
                "author_name": "author_name",
                "author_login": "author_login",
                "author_email": "author_email",
                "total_interactions": "pr_total_interactions",
                "unique_partners": "pr_unique_partners",
                "items_touched": "pr_items_touched",
                "new_interactions": "pr_new_interactions",
                "new_relationships": "pr_new_relationships",
                "total_relationships": "pr_total_relationships",
                "repeat_partners": "pr_repeat_partners",
                "avg_time_delta": "pr_avg_time_delta",
                "min_response_time": "pr_min_response_time",
                "avg_response_time_distance_1": "pr_avg_response_time_distance_1",
                "avg_interactions_per_user": "pr_avg_interactions_per_user",
                "avg_distance_away": "pr_avg_distance_away"
                })
            issue_interactions["date"] = pandas.to_datetime(issue_interactions["date"], utc=True).dt.tz_convert(None).dt.normalize()
            pr_interactions["date"] = pandas.to_datetime(pr_interactions["date"], utc=True).dt.tz_convert(None).dt.normalize()
            
            if tf_devs.startswith("author_login") or tf_devs.startswith("author_name") or tf_devs.startswith("author_email"):
                column, dev = tf_devs.split('|')
            else:
                column = "author_id"
                    
            out = pandas.merge(
                st.session_state.labeled_data,
                issue_interactions,
                left_on=["dev", "date"],
                right_on=[column, "date"],
                how="left",
                suffixes=('', '_issue')
            )

            out = pandas.merge(
                out,
                pr_interactions,
                left_on=["dev", "date"],
                right_on=[column, "date"],
                how="left",
                suffixes=('', '_pr')
            )

            timeline_combined = Path(social_network_metrics_folder ,cfg.social_technical_metrics_combined)
            out.to_csv(timeline_combined, sep=cfg.CSV_separator, na_rep=cfg.CSV_missing, lineterminator='\n')

            view_df(out, name=f"timeline_combined")

            #----------------------
            # Project Health Metrics
            #----------------------
            #include timeline and raw tables raw_data_tables = { "issues","issue_activity","prs_repo", "prs_comments", "commits" , "perfile_commits"}
            # were making a df for each dev in tf_devs
            # at the end we need to concatenate all the dfs into one
            print("\n\nStep 5: Calculating Project Health Metrics")
            dev_summary_df = []
            
            for dev in tf_devs:
                tables = {"user_labeled_timeline": user_labeled_timeline,
                        "commits": raw_data_tables['commits'],
                        "perfile_commits": raw_data_tables['perfile_commits'],
                        "issues": raw_data_tables['issues'],
                        "issue_activity": raw_data_tables['issue_activity'],
                        "prs_repo": raw_data_tables['prs_repo'],
                        "prs_comments": raw_data_tables['prs_comments'],
                        "author_map" : author_map,
                        "df_DOE" : DOE
                        }
                dev_summary_df, folder_summary_df, dev_health_metrics  = phm.main(repo_full_name=repo, tables=tables, dev_leaving=dev)
                #save each dev summary in dev_summary_df
                view_df(dev_health_metrics, name=f"dev_health_metrics_{dev}")
                view_df(folder_summary_df, name=f"folder_summary_df_{dev}")
                view_df(dev_summary_df, name=f"dev_summary_df_{dev}")
                dev_summary_df.append(dev_summary_df)
                



        print(f"Visualizing breaks for {len(master_user_timeline['dev'].unique())} developers...")
        devs = master_user_timeline['dev'].unique().tolist()
        for dev in devs:
            print(f"{dev}")
            plot_activity_histograms_by_dev(master_user_timeline, dev)

        combined_timeline = pandas.concat(users_labeled_timeline_combined, ignore_index=True)
        plot_activity_histograms_by_dev(combined_timeline, all_tf_devs)

    st.divider()
    st.subheader("Step 6: Predict Inactivity")

    if "overwrite_prediction_model" not in st.session_state:
        st.session_state.overwrite_prediction_model = False
    # Toggle switch
    st.session_state.overwrite_prediction_model = st.toggle(
        "Overwrite cached KnowledgeDistribution outputs",
        value=st.session_state.overwrite_prediction_model,
        help="If enabled, existing DOE / TF files will be recomputed.", 
        key = 3
    )

    if st.button("Run Inactivity Prediction"):

        for repo in st.session_state.list_of_repos:

            
            prediction_df = st.session_state["prediction_df"] 

            main_folder = ORG_BASE / repo 
            social_network_metrics_folder = Path(main_folder , cfg.social_network_metrics_folder   )
            timeline_combined = Path(social_network_metrics_folder ,cfg.social_technical_metrics_combined)


            #read csv
            #file it read ..\\Organizations\\atom\\atom\\Social_Network_Metrics\\timeline_combined.csv'
            #file to read "D:\Users\samut\OneDrive\Documents\GitHub\developersInactivityAnalysisCOPY\Organizations\atom\atom\SocialTechnicalNetwork\timeline_combined.csv"
            print("Reading timeline with social-technical metrics from:", timeline_combined)   
            print("Absolute path:", timeline_combined.resolve())
            print("Exists?", timeline_combined.exists())

            df = pandas.read_csv(timeline_combined, dtype=str)

            # load our predictions
            #model = run_prediction_pipeline(df, repo_key=repo)

            predictor_cols = [ 'commits', 'prs', 'issues', 'issue_activity',
            'pr_activity', 'coding_day', 'nc_day', 'break_day', 'th', 'len', 'event_day', 'win_pauses',
            'win_th', 'last_th', 'added_as_break']
            response_col = ['state']
            tf_devs = prediction_df['dev'].unique().tolist()

            model_folder = Path(main_folder, cfg.model_folder)
            os.makedirs(model_folder, exist_ok=True)

            # check if a saved model already exists
            model_exists = (model_folder / "model_weights.pth").exists()



            if model_exists and not st.session_state.overwrite_prediction_model:
                # ── LOAD ────────────────────────────────────────────────────────────────
                model, label_encoder, test_df, pred_cols_full, encoded_col = load_model_artifacts(model_folder)

            else:
                # ── TRAIN + SAVE ─────────────────────────────────────────────────────────
                model, label_encoder, test_df, pred_cols_full, encoded_col = run_prediction_pipeline_2(
                    prediction_df,
                    repo_key=repo,
                    tf_devs=tf_devs,
                    response_col=response_col,
                    predictor_cols=predictor_cols,
                    window_size=90,
                    epochs=150
                )
                save_model_artifacts(model_folder, model, label_encoder, test_df, pred_cols_full, encoded_col)



            final_df = new_evaluation_fuction(
                test_df, model, label_encoder,
                predictor_cols=pred_cols_full,
                encoded_col=encoded_col,
                window_size=90
            )

            st.session_state["final_df"] = final_df

            if "final_df" in st.session_state:
                st.subheader("Developer Activity Explorer (Test Set + Predictions)")
                make_state_graph_2(st.session_state["final_df"])

if __name__ == "__main__":
    main()

