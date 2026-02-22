#   conda activate CS485
#   python KnowledgeDistribution.py

### IMPORT EXCEPTION MODULES
from msilib.schema import tables
import uuid
from requests.exceptions import Timeout
from github import GithubException, UnknownObjectException, IncompletableObject

### IMPORT SYSTEM MODULES
from github import Github
import os, logging, pandas, csv, tempfile, shutil, functools
from datetime import datetime, timezone
from tqdm import tqdm, tqdm
from pathlib import Path
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import signal, threading
import json
from contextlib import contextmanager
from dateutil import tz as _tz
from collections import Counter
import numpy as np
import re

### IMPORT CUSTOM MODULES
import sys
sys.path.append('../')
import Settings as cfg
import Utilities as util
from pathlib import Path
import subprocess, tempfile, shutil, logging
from truckfactor.compute import main as compute_tf
import portalocker              # pip install portalocker
import warnings
import streamlit as st

from dataclasses import dataclass

warnings.filterwarnings("ignore")
from git import Repo, exc as git_exc

def view_df(df, name="DataFrame"):
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

def get_input_data(repo_full_name, tables):
    """
    Load per-file commits data from CSV
    
    Expected columns in CSV:
    - repo, sha, committed_at, author_login, author_name, author_email, 
      committer_login, file_path, status, additions, deletions, changes, previous_filename
    """
    
    # Load the data
    df = tables["perfile_commits"]


    # Type normalization commonly expected downstream
    if "additions" in df: df["additions"] = pandas.to_numeric(df["additions"], errors="coerce").fillna(0).astype(int)
    if "deletions" in df: df["deletions"] = pandas.to_numeric(df["deletions"], errors="coerce").fillna(0).astype(int)
    if "changes"   in df: df["changes"]   = pandas.to_numeric(df["changes"],   errors="coerce").fillna(0).astype(int)
    if "committed_at" in df:
        df["committed_at"] = pandas.to_datetime(df["committed_at"], errors="coerce", utc=True)
   
    # Check if the repo column contains numbers (indicating shifted data)
    if df['repo'].str.isnumeric().any():
        # Data is shifted - insert repo_full_name as first column
        df.insert(0, 'repo_corrected', repo_full_name)
        # Drop the old 'repo' column and rename
        df = df.drop(columns=['repo'])
        df = df.rename(columns={'repo_corrected': 'repo'})
        # Drop the last column (previous_filename will have extra data)
        df = df.iloc[:, :-1]
        # Add back previous_filename as empty
        df['previous_filename'] = ''
    
    for c in ["additions", "deletions", "changes"]:
        df[c] = pandas.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    df["committed_at"] = pandas.to_datetime(df["committed_at"], errors="coerce", utc=True)
    # Sort by file and commit date to ensure chronological order
    df = df.sort_values(['file_path', 'committed_at'])
    return df

def calculate_doe_metrics(df):
    """
    Calculate the 4 metrics needed for DOE:
    1. Adds: Total number of lines added by developer d on file f
    2. FA: 1 if developer d is the creator of file f, 0 otherwise
    3. NumDays: Number of days since the last commit of developer d on file f
    4. Size: Current number of lines of code (LOC) of file f
    
    Args:
        df: DataFrame with per-file commit data
        reference_date: Date to calculate NumDays from (defaults to max date in data)
    
    Returns:
        DataFrame with DOE metrics per (developer, file) combination
    """
    
    # Use the latest commit date as reference if not provided
    df['committed_at'] = util._norm_time(df['committed_at'])
    reference_date = df['committed_at'].max()
    
    # Ensure reference_date is timezone-aware
    if reference_date.tzinfo is None:
        reference_date = reference_date.replace(tzinfo=timezone.utc)
    
    # Create developer identifier 
    # the dev column is always the author_id
    # if the author id is missing we use the author_login with a 1 at the front with a _
    # if the author login is missing we use the author_name with a 2 at the front with a _
    # if the author name is missing we use the author_email with a 3 at the front with a _
    df['developer'] = df.apply(create_developer_id, axis=1)

    print(f"\nProcessing {len(df)} file changes across {df['developer'].nunique()} developers")
    
    # ===========================
    # Metric 1: Adds (per developer per file)
    # ===========================
    adds_df = df.groupby(['developer', 'file_path'])['additions'].sum().reset_index()
    adds_df.rename(columns={'additions': 'Adds'}, inplace=True)
    
    # ===========================
    # Metric 2: FA (First Author / File Creator)
    # ===========================
    # Get the first commit for each file (earliest committed_at)
    first_commits = df.sort_values('committed_at').groupby('file_path').first().reset_index()
    first_commits = first_commits[['file_path', 'developer']].rename(columns={'developer': 'creator'})
    
    # Create all developer-file combinations
    all_combos = df[['developer', 'file_path']].drop_duplicates()
    
    # Merge with creators
    fa_df = all_combos.merge(first_commits, on='file_path', how='left')
    fa_df['FA'] = (fa_df['developer'] == fa_df['creator']).astype(int)
    fa_df = fa_df[['developer', 'file_path', 'FA']]
    
    # ===========================
    # Metric 3: NumDays (days since last commit by developer on file)
    # ===========================
    # Get the last commit date for each developer-file combination
    last_commit_df = df.groupby(['developer', 'file_path'])['committed_at'].max().reset_index()
    last_commit_df.rename(columns={'committed_at': 'last_commit_date'}, inplace=True)
    
    # Calculate days since last commit
    last_commit_df['NumDays'] = (reference_date - last_commit_df['last_commit_date']).dt.days
    
    # Ensure non-negative
    last_commit_df['NumDays'] = last_commit_df['NumDays'].clip(lower=0)
    last_commit_df = last_commit_df[['developer', 'file_path', 'NumDays']]
    
    # ===========================
    # Metric 4: Size (current LOC of file)
    # ===========================
    # Calculate cumulative size for each file over time
    # Start with 0, then add additions and subtract deletions
    
    # Sort by file and date
    df_sorted = df.sort_values(['file_path', 'committed_at']).copy()
    
    # Calculate net changes per commit
    #TypeError: unsupported operand type(s) for -: 'str' and 'str'
    df_sorted['net_change'] = df_sorted['additions'].astype(int) - df_sorted['deletions'].astype(int)

    # Calculate cumulative size per file
    df_sorted['cumulative_size'] = df_sorted.groupby('file_path')['net_change'].cumsum()
    
    # Ensure size is non-negative (files can't have negative LOC)
    df_sorted['cumulative_size'] = df_sorted['cumulative_size'].clip(lower=0)
    
    # Get the final size for each file (last commit's cumulative size)
    size_df = df_sorted.groupby('file_path')['cumulative_size'].last().reset_index()
    size_df.rename(columns={'cumulative_size': 'Size'}, inplace=True)
    
    # ===========================
    # Combine all metrics
    # ===========================
    # Start with all developer-file combinations
    result_df = all_combos.copy()
    
    # Merge all metrics
    result_df = result_df.merge(adds_df, on=['developer', 'file_path'], how='left')
    result_df = result_df.merge(fa_df, on=['developer', 'file_path'], how='left')
    result_df = result_df.merge(last_commit_df, on=['developer', 'file_path'], how='left')
    result_df = result_df.merge(size_df, on='file_path', how='left')
    
    # Fill missing values
    result_df['Adds'] = result_df['Adds'].fillna(0).astype(int)
    result_df['FA'] = result_df['FA'].fillna(0).astype(int)
    result_df['NumDays'] = result_df['NumDays'].fillna(0).astype(int)
    result_df['Size'] = result_df['Size'].fillna(0).astype(int)
    
    # Add metadata
    result_df['repo'] = df['repo'].iloc[0]
    result_df['reference_date'] = reference_date
    
    print(f"\nMetrics calculated for {len(result_df)} developer-file pairs:")
    print(f"  - {result_df['developer'].nunique()} unique developers")
    print(f"  - {result_df['file_path'].nunique()} unique files")
    print(f"  - {result_df['FA'].sum()} file creators identified")
    

    return result_df

def create_developer_id(row):
    if 'author_id' in row.index and pandas.notna(row['author_id']):
        return row['author_id']
    elif 'author_login' in row.index and pandas.notna(row['author_login']):
        return f"author_login|{row['author_login']}"
    elif 'author_name' in row.index and pandas.notna(row['author_name']):
        return f"author_name|{row['author_name']}"
    elif 'author_email' in row.index and pandas.notna(row['author_email']):
        return f"author_email|{row['author_email']}"
    else:
        return None

def calculate_doe(df):
    """
    Calculate the Degree of Expertise (DOE) for each developer-file pair
    
    DOE formula from paper:
    DOE(d, f(v)) = 5.28223 + 0.23173 · ln(1 + Adds) + 0.36151 · FA 
                   - 0.19421 · ln(1 + NumDays) - 0.28761 · ln(Size)
    
    Args:
        df: DataFrame with columns [developer, file_path, Adds, FA, NumDays, Size]
    
    Returns:
        DataFrame with DOE column added
    """
    
   
    # Calculate DOE using the formula from the paper
    df['DOE'] = (
        5.28223 
        + 0.23173 * np.log1p(df['Adds'])      # ln(1 + Adds)
        + 0.36151 * df['FA']                   # FA (binary)
        - 0.19421 * np.log1p(df['NumDays'])   # ln(1 + NumDays)
        - 0.28761 * np.log(df['Size'] + 1)    # ln(Size) - adding 1 to avoid log(0)
    )
    
    # Identify top experts (highest DOE per file)
    top_experts = df.sort_values('DOE', ascending=False).groupby('file_path').first()
    
    return df

def runTruckFactor(df, authors_map):
    """
    Calculate truck factor with detailed debugging
    """    
    # Make a copy so we don't modify the original
    authors_map_copy = {k: v.copy() for k, v in authors_map.items()}
    
    tf_list: list[str] = []
    F = df['file_path'].nunique()

    
    # Check for NaN developers
    nan_count = df['developer'].isna().sum()
    
    dev_coverage = [(dev, len(files)) for dev, files in authors_map_copy.items()]
    dev_coverage.sort(key=lambda x: x[1], reverse=True)
    #for i, (dev, file_count) in enumerate(dev_coverage[:10]):  # Top 10
    #    print(f"  {i+1}. {dev[:20]}... : {file_count} files")
    
    tf = 0
    
    while authors_map_copy:
        coverage = getCoverage(F, authors_map_copy)
        
        if coverage <= 0.5:
            break
        # Find top author
        if not authors_map_copy:
            break 
        top_author = max(authors_map_copy.items(), key=lambda kv: len(kv[1]))[0]
        top_author_files = len(authors_map_copy[top_author])
        
        # Remove top author
        authors_map_copy.pop(top_author, None)
        tf_list.append(top_author)
        tf += 1
        
    
    return tf, tf_list

def getCoverage(rep_files_size: int, authors_map: dict[str, set]) -> float:
    """
    Calculate coverage with debugging
    """
    if not authors_map:
        return 0.0
        
    covered_files = set()
    for files in authors_map.values():
        covered_files |= files
        if len(covered_files) == rep_files_size:
            return 1.0
    
    coverage = len(covered_files) / float(rep_files_size if rep_files_size > 0 else 1)
    return coverage

def build_authors_map_from_doe(df: pandas.DataFrame) -> dict[str, set]:
    """
    Chatgpt Built 
    Build { developer -> set(files they 'own') } using DOE.
    We treat the 'owner' of a file as any developer with the max DOE for that file (ties included).
    """
    # ensure required columns exist
    required = {"developer", "file_path", "DOE"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns for authors map: {missing}")

    # compute max DOE per file
    max_doe_per_file = df.groupby("file_path")["DOE"].transform("max")
    owners = df[df["DOE"] == max_doe_per_file][["developer", "file_path"]]

    authors_map: dict[str, set] = {}
    for dev, sub in owners.groupby("developer"):
        authors_map[dev] = set(sub["file_path"].tolist())
    return authors_map

def main(repo_full_name=None, tables=None, overwrite=False):
    """
    Knowledge Distribution + Truck Factor pipeline with caching.

    Parameters
    ----------
    repo_full_name : str
        org/repo
    tables : dict
        raw input tables
    overwrite : bool
        If True, recompute even if outputs exist
    """

    org, repo = repo_full_name.split('/')
    organization_folder = Path(cfg.main_folder, org, repo)
    output_folder = Path(organization_folder, "KnowledgeDistribution")
    os.makedirs(output_folder, exist_ok=True)

    # ----------------------
    # Output paths
    # ----------------------
    doe_path = output_folder / "doe.csv"
    authors_map_path = output_folder / "authors_map.json"
    tf_path = output_folder / cfg.truck_factor_file

    # ----------------------
    # FAST PATH: LOAD IF EXISTS
    # ----------------------
    if (
        not overwrite
        and doe_path.exists()
        and authors_map_path.exists()
        and tf_path.exists()
    ):
        print(f"[KnowledgeDistribution] Loading cached results for {repo_full_name}")

        df_DOE = pandas.read_csv(doe_path)

        with open(authors_map_path, "r") as f:
            authors_map = json.load(f)
            # convert lists back to sets
            authors_map = {k: set(v) for k, v in authors_map.items()}

        with open(tf_path, "r") as f:
            tf_data = json.load(f)
            tf = tf_data["tf"]
            tf_list = tf_data["tf_list"]

        return tf, tf_list, authors_map, df_DOE

    # ----------------------
    # FULL RECOMPUTE PATH
    # ----------------------
    print(f"[KnowledgeDistribution] Computing results for {repo_full_name}")

    # Step 1: input data
    raw = get_input_data(repo_full_name, tables)

    # Step 2: DOE metrics
    df_metrics = calculate_doe_metrics(raw)
    df_DOE = calculate_doe(df_metrics)
    df_DOE.to_csv(doe_path, index=False)

    # Step 2.5: authors map
    authors_map = build_authors_map_from_doe(df_DOE)
    with open(authors_map_path, "w") as f:
        json.dump(
            {k: sorted(list(v)) for k, v in authors_map.items()},
            f,
            indent=2
        )

    # Step 3: Truck Factor
    tf, tf_list = runTruckFactor(df_DOE, authors_map)

    with open(tf_path, "w") as f:
        json.dump(
            {
                "tf": tf,
                "tf_list": tf_list,
                "repo": repo_full_name
            },
            f,
            indent=2
        )

    print(f"\nTruck Factor for {repo_full_name}: {tf_list}")

    return tf, tf_list, authors_map, df_DOE
