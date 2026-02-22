#   conda activate osslab
#   streamlit run DemoAppV2.py


#   cd C:\Users\samut\OneDrive\Documents\GitHub\developersInactivityAnalysisCOPY\Extractors

from asyncio import Event
import json
from operator import index
from msilib import Table
from turtle import pd
import streamlit as st
import pandas
import numpy as np
import os
import csv
import tempfile
import shutil
import logging
import re
import unicodedata
import sys
import subprocess
import time
import joblib
import matplotlib.pyplot as plt
import altair as alt

import matplotlib.dates as mdates
from datetime import datetime, timezone, timedelta
from tqdm import tqdm
from pathlib import Path
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

from dataclasses import dataclass
from github import Github, GithubException, UnknownObjectException, IncompletableObject


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

ORG_BASE = PROJECT_ROOT / "Organizations"

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
                    # We need 5 different df that have the new data found appended on to them
                    df = pandas.read_csv(file_path)

                    if file_key == "issues":
                        issues = pandas.concat([issues, df], ignore_index=True)
                    elif file_key == "issue_activity":
                        issue_activity = pandas.concat([issue_activity, df], ignore_index=True)
                    elif file_key == "prs_repo":
                        prs_repo = pandas.concat([prs_repo, df], ignore_index=True)
                    elif file_key == "prs_comments":
                        prs_comments = pandas.concat([prs_comments, df], ignore_index=True)
                    elif file_key == "commit_list":
                        commits = pandas.concat([commits, df], ignore_index=True)
                    elif file_key == "perfile_commit":
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
def label_developers_activity(repo, pbar, user_pbar_percent) -> pandas.DataFrame:
    """
    main function for labeling developers
    sets up varables to call label timeline
    """
    
    # "../Organizations"
    organizationFolder = cfg.main_folder

    win = cfg.sliding_window_size

    repos_txt = '../' + cfg.repos_file
    repos_to_process = []
        
    
    all_timelines = []
    all_diagnostics = []



    organizationFolder = cfg.main_folder
    organization, project = repo.split('/')
    if Path(organizationFolder, organization).exists() == False:
        st.write(f"Organization folder not found: {Path(organizationFolder, organization)}")

    print(f"Start Identifying inactivity periods for {organization}/{project}...")

    organizationFolder = Path(organizationFolder) / organization / project

    commits =  pandas.read_csv(organizationFolder / "commit_list.csv", parse_dates=["created_at"], encoding="utf-8", header=0, sep=cfg.CSV_separator)

    # this is a json file not a csv file
    tf_devs = pandas.read_json(organizationFolder/ cfg.TF_developers_folder / cfg.TF_developers_file)
    tf_devs = tf_devs.iloc[:, 0].tolist()
    tf= len(tf_devs)
    
    pauses = write_pauses_table(commits, organizationFolder / "pauses_commits.csv", tf_devs, date_col="created_at")
    #make pauses to a csv file at this location C:\Users\samut\OneDrive\Documents\GitHub\developersInactivityAnalysisCOPY\Organizations\Rdatatable\data.table\Results

    print(f"{len(tf_devs)} Developers inactivity periods identified")

    output_folder = organizationFolder /  "Results"
    os.makedirs(output_folder, exist_ok=True)
    pbar = st.progress(0)
    count = 0

    for dev in tf_devs:
        print(f"{tf_devs.index(dev) + 1} / {len(tf_devs)}")
        pbar.progress((user_pbar_percent))
        if dev.startswith("author_login") or dev.startswith("author_name") or dev.startswith("author_email"):
            column, dev = dev.split('|')
        count= count+1

        timeline_folder = organizationFolder /  cfg.timeline_folder
        os.makedirs(timeline_folder, exist_ok=True)
            
        timeline_path = Path(timeline_folder, cfg.timeline_file)

        if timeline_path.is_file():
            #our is Timeline created at: ..\Organizations\atom\atom\Timelines\timeline.csv
            user_timeline = pandas.read_csv(timeline_path, sep=cfg.CSV_separator, parse_dates=["date"], index_col="date")
            user_timeline_change = user_timeline[user_timeline["dev"] == dev]
            
        else:
            #we need to make the timeline 
            print(f"Timeline not found at {timeline_path}, generating timeline...")
            continue

        breaks_folder = organizationFolder /  "Breaks"
        os.makedirs(breaks_folder, exist_ok=True)
        breaks_path =  Path(breaks_folder)/  f"{dev}_breaks.csv"              

        #if breaks_path.is_file():
        #    print(breaks_path)
        #    breaks_df = pandas.read_csv(breaks_path, sep=cfg.CSV_separator, index_col=0)  
        #    print(breaks_df)
        #
        #else:
        breaks_df = pandas.DataFrame(columns=['len', 'dates', 'th'])
        #print all input varables
        breaks_df, diagnostics_df = identifyBreaks(pauses, dev=dev, window=win, debug_folder=output_folder)

        breaks_df.to_csv(breaks_path, sep=cfg.CSV_separator, na_rep=cfg.CSV_missing, index=False, lineterminator="\n")
                    
        #add label timeline'
        user_timeline = user_timeline[user_timeline["dev"] == dev]
        user_timeline = label_timeline(user_timeline, breaks_df)

        user_timeline = user_timeline.reset_index()
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


    # append the diagnostics to the time line by joining on the win_end from diagnostics and the dates from timeline
    # master_diagnostics.win_end is object holding datetime.date (needs converting to pandas datetime + normalize).

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

    master_user_timeline.to_csv(Path(output_folder) / "all_users_labeled_timeline.csv", sep=cfg.CSV_separator, na_rep=cfg.CSV_missing, lineterminator='\n', index_label='date')

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
            column, dev = dev.split('|')
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

    # We need to make coding_day,nc_day 
    # a coding day is something specific
    # coding activity both making a commit to a local repository and opening a pull request
    # that means in daily df commits > 0 AND prs > 0

    # coding activity: commit OR PR
    df["coding_day"] = ((df["commits"] > 0) | (df["prs"] > 0)).astype(int)

    # non-coding activity: any other event > 0 AND no coding
    noncoding_cols = [c for c in df.columns if c in [ "issues", "issue_activity", "prs_activity" ]]
    df["nc_day"] = ((df[noncoding_cols].sum(axis=1) > 0) & (df["coding_day"] == 0)).astype(int)


    df["break_day"] = None
    df["break_day"] = pandas.Series(False, index=df.index, dtype="boolean")
    df["th"] = pandas.Series(pandas.NA, index=df.index, dtype="Float64")
    df["len"] = pandas.Series(pandas.NA, index=df.index, dtype="Int64")
    df["index"] = pandas.Series(pandas.NA, index=df.index, dtype="Int64")

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
        Tfov = int(round(th_vals.iloc[0]))

        # optional: get far-out threshold
        # Anchor silence to the last event (coding or non-coding) before the break starts
        prev_nc_idx = df.index[df["nc_day"] & (df.index < start_ts)]
        last_nc_before = prev_nc_idx.max()

        prev_activity_idx = df.index[df["event_day"] & (df.index < start_ts)]
        last_event_before = prev_activity_idx.max()

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

def timeline(tables, tf_devs, pbar=None, user_pbar_percent=None) -> pandas.DataFrame:
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
        pbar.progress((user_pbar_percent))
        if dev.startswith("author_login") or dev.startswith("author_name") or dev.startswith("author_email"):
            column, dev = dev.split('|')
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

    return pandas.DataFrame(user_activity)

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
    
def run_prediction_pipeline(df, repo_key):
    
    devs= df["dev"].unique().tolist()

    y = build_response(df, 7, label_col = "state")
    df = df.merge(y[["dev", "date", "transition_to_inactive"]], on=["dev", "date"], how="left")

    label_col = "transition_to_inactive"
    date_col = "date"
    dev_col = "dev"


    df[label_col] = pandas.to_numeric(df[label_col], errors="coerce").fillna(0).astype(int)
    df[date_col] = pandas.to_datetime(df[date_col])

    # identify numeric columns for lag features
    num_cols = [ 'commits', 'prs', 'issues',
       'issue_activity', 'pr_activity', 'break_day',
       'th', 'len', 'win_th', 'last_th',
       'issue_total_interactions', 'issue_unique_partners',
       'issue_items_touched', 'issue_new_interactions',
       'issue_new_relationships', 'issue_total_relationships',
       'issue_repeat_partners', 'issue_avg_time_delta',
       'issue_min_response_time', 'issue_avg_response_time_distance_1',
       'issue_avg_interactions_per_user', 'issue_avg_distance_away',
       'pr_total_interactions', 'pr_unique_partners', 'pr_items_touched',
       'pr_new_interactions', 'pr_new_relationships', 'pr_total_relationships',
       'pr_repeat_partners', 'pr_avg_time_delta', 'pr_min_response_time',
       'pr_avg_response_time_distance_1', 'pr_avg_interactions_per_user',
       'pr_avg_distance_away']

    # remove these items from the list num_cols: 'win_pauses', 'partial_lengths', 'win_th', 'last_th'
    excluded_cols = {'win_pauses', 'partial_lengths', 'th', 'len', 'index' , 'coding_day', 'nc_day','break_day', 'Unnamed: 0'}
    num_cols = [c for c in num_cols if c not in excluded_cols]

    state_enc = LabelEncoder()
    df["state_encoded"] = state_enc.fit_transform(df["state"].astype(str))

    # if break_day is numeric-ish:
    df["break_day"] = pandas.to_numeric(df["break_day"], errors="coerce").fillna(0).astype(int)


    LAGS=5
    combined_results = []

    for dev in devs:
        for col in num_cols:
            for l in range(0, LAGS+1):
                df[f"{col}_lag{l}"] = df.groupby(dev_col, observed=True)[col].shift(l)

        test_df = df[ df["dev"] == dev ]
        train_df = df[ df["dev"] != dev ]

        Xtr = train_df[[c for c in df.columns if any(c.endswith(f"_lag{i}") for i in range(0, LAGS+1))]]
        Xtr["state_encoded"] = train_df["state_encoded"]
        Xtr["break_day"]     = train_df["break_day"]
        Xtr = Xtr.dropna(axis=1, how='all')
        Xtr = Xtr.fillna(0)

        Xte = test_df[[c for c in df.columns if any(c.endswith(f"_lag{i}") for i in range(0, LAGS+1))]]
        Xte["state_encoded"] = test_df["state_encoded"]
        Xte["break_day"]     = test_df["break_day"]
        Xte = Xte.dropna(axis=1, how='all')
        Xte = Xte.fillna(0)

        ytr = train_df[label_col].astype(int)
        yte = test_df[label_col].astype(int)


        mask = ~Xte.isna().any(axis=1)

        
        
        rf_clf  = RandomForestClassifier(n_estimators=400, min_samples_split=5, min_samples_leaf=2,
                                        n_jobs=-1, random_state=42, class_weight="balanced_subsample")

        rf_clf.fit(Xtr,ytr)

        pred_df = pandas.DataFrame({
            "dev": test_df[dev_col].values,
            "date": test_df[date_col].values,
            "y_true": yte.values,
            "rf_proba": rf_clf.predict_proba(Xte)[:,1]        
            })


        combined_results.append(pred_df)

    combined_results = pandas.concat(combined_results, ignore_index=True)

    combined_results.to_csv(f"atom_results.csv", index=False)


    metrics = make_confusion_mats(combined_results, thr_rf=0.6, per_dev=False)

    view_df(metrics['rf'], name="Random Forest Confusion Matrix")

    plot_activity_histograms_by_dev(df, devs)  # plot for first 3 devs as example

    return combined_results, metrics

#-----------------------
#visualization functions
#------------------------  

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

STATE_COLORS = {
    "ACTIVE": "#8fd19e",      # soft green
    "NON_CODING": "#ffe58f",  # soft yellow
    "INACTIVE": "#ff9999",    # soft red
    "GONE": "#cccccc",        # grey fallback
    "UNKNOWN": "#e0e0e0",     # generic fallback
}

def plot_activity_histograms_by_dev(df: pandas.DataFrame, devs):
    """
    For each dev in `devs`, plot a timeline histogram of log activity
    with a colored background showing the user's state.

    Assumes df has at least:
      - 'dev'
      - 'date'
      - 'commits', 'prs'
      - 'issues', 'issue_activity', 'pr_activity'
      - 'state'
    """

    # Make sure devs is a list
    if isinstance(devs, (str, int)):
        devs = [devs]

    required_cols = {
        "dev", "date",
        "commits", "prs",
        "issues", "issue_activity", "pr_activity",
        "state",
    }
    missing = required_cols - set(df.columns)
    if missing:
        st.error(f"Missing required columns in dataframe: {missing}")
        return

    # Work on a copy so we don’t mutate the original
    data = df.copy()

    activity_cols = ["commits", "prs", "issues", "issue_activity", "pr_activity"]
    for c in activity_cols:
        data[c] = pandas.to_numeric(data[c], errors="coerce").fillna(0)

    # Ensure date is datetime
    data["date"] = pandas.to_datetime(data["date"])

    # Fill NA counts with 0
    for c in ["commits", "prs", "issues", "issue_activity", "pr_activity"]:
        data[c] = data[c].fillna(0)

    # Build daily sums
    # force both columns to numeric
    data["coding_total"] = pandas.to_numeric(data["commits"]) + pandas.to_numeric(data["prs"])
    data["noncoding_total"] = (
        data["issues"] + data["issue_activity"] + data["pr_activity"]
    )

    # Log transform (log(1 + x) handles zeros)
    data["coding_log"] = np.log1p(data["coding_total"])
    data["noncoding_log"] = np.log1p(data["noncoding_total"])

    # Clean up state
    data["state"] = data["state"].fillna("UNKNOWN")

    for dev in devs:
        user_df = (
            data[data["dev"] == dev]
            .sort_values("date")
            .reset_index(drop=True)
        )

        if user_df.empty:
            st.warning(f"No rows found for dev {dev!r}.")
            continue

        st.subheader(f"Developer {dev}")

        fig, ax = plt.subplots(figsize=(11, 4))

        # ---------- BACKGROUND LAYER: state bands ----------
        start_idx = 0
        for i in range(1, len(user_df) + 1):
            # close segment when state changes or at the end
            if i == len(user_df) or user_df["state"].iloc[i] != user_df["state"].iloc[start_idx]:
                start_date = user_df["date"].iloc[start_idx]
                # extend to the end of the last day in the segment
                end_date = user_df["date"].iloc[i - 1] + pandas.Timedelta(days=1)

                state = user_df["state"].iloc[start_idx]
                color = STATE_COLORS.get(state, STATE_COLORS["UNKNOWN"])

                ax.axvspan(
                    start_date,
                    end_date,
                    facecolor=color,
                    alpha=0.25,
                    zorder=0,  # behind everything
                )
                start_idx = i

        # ---------- HISTOGRAM LAYER: log activity ----------
        # Coding bars (wider)
        ax.bar(
            user_df["date"],
            user_df["coding_log"],
            width=0.8,  # ~0.8 days
            label="Coding (log(1 + commits + prs))",
            alpha=0.7,
            edgecolor="none",
            zorder=2,
        )

        # Non-coding bars (slightly narrower on top)
        ax.bar(
            user_df["date"],
            user_df["noncoding_log"],
            width=0.4,
            label="Non-coding (log(1 + issues + issue_activity + pr_activity))",
            alpha=0.8,
            edgecolor="none",
            zorder=3,
        )

        # ---------- Styling ----------
        ax.set_ylabel("log(1 + daily count)")
        ax.set_xlabel("Date")
        ax.set_title("Daily coding vs. non-coding activity (with state bands)")

        ax.grid(axis="y", alpha=0.3, linestyle="--")

        # Nice date formatting
        locator = mdates.AutoDateLocator()
        formatter = mdates.ConciseDateFormatter(locator)
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(formatter)

        ax.set_xlim(
            user_df["date"].min() - pandas.Timedelta(days=1),
            user_df["date"].max() + pandas.Timedelta(days=1),
        )

        ax.legend(loc="upper right", frameon=False)

        fig.tight_layout()
        st.pyplot(fig)
        plt.close(fig)

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
            "atom/atom",
            "Rdatatable/data.table",
            "aseprite/aseprite",
            "rails/rails",
            "nodejs/node",
            "jekyll/jekyll",
            "laravel/framework",
            "jquery/jquery",
            "fastlane/fastlane",
            "crystal-lang/crystal",
            "BabylonJS/Babylon.js",
            "elixir-lang/elixir",
            "elixirlang/elixir",
            "JabRef/jabref",
            "github/linguist",
            "MinecraftForge/MinecraftForge",
            "SpaceVim/SpaceVim",
            "flutter/flutter",
            "ionic-team/ionic-framework",
            "facebook/react",
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
            "atom/atom",
            "Rdatatable/data.table",
            "aseprite/aseprite",
            "rails/rails"
        ]


    st.caption(f"Selected repos: **{', '.join(st.session_state.list_of_repos)}**")

    # (Later buttons can use `repo_key` and `paths`, e.g., Update, Label, Predict)

    st.divider()

    if st.button("Process data for all repos in queue   "):
        pbar = st.progress(0)
        count = 0 

        number_of_repos = len(st.session_state.list_of_repos)
        repo_pbar_percent = 1 / number_of_repos if number_of_repos > 0 else 1

        users_labeled_timeline_combined = []


        for repo in st.session_state.list_of_repos:

            print(f"Processing repository: {repo}")
            
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
            
            pbar.progress(repo_pbar_percent * 0.10)
            
            #----------------------
            # Truck Factor
            #----------------------
            print("\n\nStep 2: Calculating Truck Factor")
            tf, tf_devs, author_map, DOE = kd.main(repo_full_name=repo, tables=raw_data_tables)


            DOE.to_csv(
                Path(TF_developers_folder, cfg.DOE_file),
                sep=cfg.CSV_separator,
            )

            # save author_map (convert sets to lists for JSON)
            author_map_json = {k: sorted(list(v)) for k, v in author_map.items()}
            with open(Path(TF_developers_folder, cfg.author_map_file), "w") as f:
                print("path: ", Path(TF_developers_folder, cfg.author_map_file))
                json.dump(author_map_json, f)
            print("DOE path: ", Path(TF_developers_folder, cfg.DOE_file))

            path = Path(TF_developers_folder, cfg.TF_developers_file)
            print("TF developers path: ", path)
            with open(path, "w") as f:
                json.dump(tf_devs, f)
        
            pbar.progress(repo_pbar_percent * 0.20)

            #----------------------
            # Basic Timeline
            #----------------------
            print("\n\nStep 3: Generating Basic Timeline")
            user_pbar_percent = (repo_pbar_percent * 0.40)
            out = timeline(raw_data_tables, tf_devs, pbar=pbar, user_pbar_percent=user_pbar_percent)
            out.to_csv(Path(timeline_folder, cfg.timeline_file), sep=cfg.CSV_separator)
            print(f"Generated basic timeline data.")
            #----------------------
            # Label Timeline
            #----------------------
            print("\n\nStep 4: Labeling Developer Activity Timeline")
            user_labeled_timeline = label_developers_activity(repo=repo, pbar=pbar, user_pbar_percent=user_pbar_percent)
            #plot timeline
            print(f"Generated labeled timeline data.")

            users_labeled_timeline_combined.append(user_labeled_timeline)

            view_df(user_labeled_timeline, name=f"users_labeled_timeline_combined")

            #----------------------
            # Social Network Metrics
            #----------------------
            #tf times
            
            #issue_interactions_interactions_metrics, pr_interactions_interactions_metrics, combined_interactions = stn.main(repo, tf_devs, tables= raw_data_tables)
            
            #issue_interactions = issue_interactions_interactions_metrics.rename(columns={
            #    "from_user": "dev",
            #    "day": "date",
            #    "author_id": "author_id",
            #    "author_name": "author_name",
            #    "author_login": "author_login",
            #    "author_email": "author_email",
            #    "total_interactions": "issue_total_interactions",
            #    "unique_partners": "issue_unique_partners",
            #    "items_touched": "issue_items_touched",
            #    "new_interactions": "issue_new_interactions",
            #    "new_relationships": "issue_new_relationships",
            '''
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
            '''

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
        pbar = st.progress(0)
        for dev in devs:
            pbar.progress((devs.index(dev)+1)/len(devs))
            print(f"{dev}")
            plot_activity_histograms_by_dev(master_user_timeline, dev)

        combined_timeline = pandas.concat(users_labeled_timeline_combined, ignore_index=True)
        plot_activity_histograms_by_dev(combined_timeline, all_tf_devs)

    st.divider()
    st.subheader("Step 6: Predict Inactivity")
    if st.button("Run Inactivity Prediction"):

        for repo in st.session_state.list_of_repos:
            
            main_folder = Path(cfg.main_folder) / repo
            social_network_metrics_folder = Path(main_folder , cfg.social_network_metrics_folder   )
            timeline_combined = Path(social_network_metrics_folder ,cfg.social_technical_metrics_combined)


            #read csv
            #file it read ..\\Organizations\\atom\\atom\\Social_Network_Metrics\\timeline_combined.csv'
            #file to read "D:\Users\samut\OneDrive\Documents\GitHub\developersInactivityAnalysisCOPY\Organizations\atom\atom\SocialTechnicalNetwrok\timeline_combined.csv"
            print("Reading timeline with social-technical metrics from:", timeline_combined)   

            df = pandas.read_csv(timeline_combined, dtype=str)

            # this is a hyper parameter that can be changed later
            response = build_response(df, N=7)

            data = df.merge(response[["dev", "date", "transition_to_inactive"]], on=["dev", "date"], how="left")

            # load our predictions
            csv_data, confusion = run_prediction_pipeline(df, repo_key=repo)

            data['date'] = pandas.to_datetime(data['date'])
            csv_data['date'] = pandas.to_datetime(csv_data['date'])


            data = data.merge(csv_data[["dev", "date", "rf_proba"]], on=["dev", "date"], how="left")

            # if rf_proba > 0.7 we make rf_proba_tf 1 and 0 otherwise
            data['rf_preds'] = data['rf_proba'].apply(lambda x: 1 if x > 0.7 else 0)

            view_df(data.head(), name="Data with Predictions")
            # remove columns that are not needed
            # needed columns are dev, date, state, transition_to_inactive, rf_proba, rf_preds
            data = data[["dev", "date", "commits", "prs", "issues", "issue_activity", "pr_activity", "state", "transition_to_inactive", "rf_proba", "rf_preds"]]

            data.to_csv(f"final_data_with_predictions.csv", index=False)


if __name__ == "__main__":
    main()

