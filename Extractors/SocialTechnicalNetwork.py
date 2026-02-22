#   conda activate CS485
#   python SocialTechnicalNetwork.py



### IMPORT EXCEPTION MODULES
from turtle import distance, pd
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
import networkx as nx
import matplotlib.pyplot as plt

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

from dataclasses import dataclass

warnings.filterwarnings("ignore")
from git import Repo, exc as git_exc
#_________________________________________________________
#
#Social Technical Networks
#
#_________________________________________________________

def _norm_time(s):
    return pandas.to_datetime(s, utc=True, errors="coerce")

def timeline(tables):
    '''
    Create issue and PR timelines by merging main tables with their activity tables.
    We need to be ure about the author.
    '''

    issues = tables["issues"]
    issue_activity = tables["issue_activity"]
    prs_repo = tables["prs_repo"]
    prs_comments = tables["prs_comments"]
    issues['created_at_issue'] = issues['created_at']

    issues["created_by_issue"] = issues.apply(
            lambda row: row['author_id'] if row['author_id'] else f"author_login|{row['author_login']}" if row['author_login'] else f"author_name|{row['author_name']}" if row['author_name'] else f"author_email|{row['author_email']}",
            axis=1
        )
    issue_activity["created_at_activity"] = issue_activity["created_at"]
    issue_activity['created_by_activity'] = issue_activity.apply(
            lambda row: row['author_id'] if row['author_id'] else f"author_login|{row['author_login']}" if row['author_login'] else f"author_name|{row['author_name']}" if row['author_name'] else f"author_email|{row['author_email']}",
            axis=1
        )
    
    prs_repo['created_at_pr'] = prs_repo['created_at']
    prs_repo['created_by_pr'] = prs_repo.apply(
            lambda row: row['author_id'] if row['author_id'] else f"author_login|{row['author_login']}" if row['author_login'] else f"author_name|{row['author_name']}" if row['author_name'] else f"author_email|{row['author_email']}",
            axis=1
        )

    prs_comments['created_at_comment'] = prs_comments['created_at']
    
    prs_comments['created_by_comment'] = prs_comments.apply(
            lambda row: row['author_id'] if row['author_id'] else f"author_login|{row['author_login']}" if row['author_login'] else f"author_name|{row['author_name']}" if row['author_name'] else f"author_email|{row['author_email']}",
            axis=1
        )

    issue_timeline = (
        issues.merge(issue_activity, on="issue_number", how="left")
              .sort_values(["issue_number", "created_at_activity"])
              .reset_index(drop=True)
    )

    pr_timeline = (
        prs_repo.merge(prs_comments, on="PR_id", how="left")
                .sort_values(["PR_id", "created_at_comment"])
                .reset_index(drop=True)
    )

    return issue_timeline, pr_timeline

def interaction_network(timeline, type ):
    # we need to create the interaction network from the issue and pr timelines
    # we have:
    # repo_pr,created_at_pr,created_by_pr,PR_id,state,merged,closed_at,merged_at,repo_comment,created_at_comment,created_by_comment,comment_id,event
    # Rdatatable/data.table,2020-01-24T12:11:47Z,sritchie73,4196,CLOSED,False,2025-06-27T17:25:04Z,,Rdatatable/data.table,2020-05-19T11:08:35Z,sritchie73,MDEyOklzc3VlQ29tbWVudDYzMDc1MDEyOQ==,comment
    #repo_issue,created_at_issue,created_by_issue,issue_number,title,state,closed_at,labels,assignees,milestone,repo_activity,activity_id,item_type,event,body,created_at_activity,created_by_activity
    #Rdatatable/data.table,2017-12-01T14:14:47Z,MichaelChirico,2505,fread support for parquet,CLOSED,2018-08-16T11:08:24Z,,,,Rdatatable/data.table,MDEyOklzc3VlQ29tbWVudDM0OTcyODc1Mw==,IssueComment,,"Yeah, it would be nice reading parquet into R without using Spark",2017-12-06T18:18:51Z,DavidArenburg
    # we need to group by PR_id and then create an interaction of a user with all other users who commented on the PR before them and are not themself

    interactions_df = []

    created_at = "created_at_comment" if type == "PR" else "created_at_activity"
    created_at_2 = "created_at_pr" if type == "PR" else "created_at_issue"
    created_by = "created_by_comment" if type == "PR" else "created_by_activity"
    id_name = "PR_id" if type == "PR" else "issue_number"

    timeline[created_at] = _norm_time(timeline[created_at])
    # make all interactions to the created issue or pr
    # itterate through each row in the timeline

    for i, row in timeline.iterrows():
        user_1 = row["created_by_comment"] if type == "PR" else row["created_by_activity"]
        user_2 = row["created_by_pr"] if type == "PR" else row["created_by_issue"]
        event_1 = row["event"]
        event_2 = "created"
        pr_id = row[id_name]
        event_1_timestamp = row[created_at]
        event_2_timestamp = row[created_at_2]
        interactions_df.append({
                        "from_user": user_1,
                        "to_user": user_2,
                        "event_1": event_1,
                        "event_2": event_2,
                        id_name: pr_id,
                        "event_1_timestamp": event_1_timestamp,
                        "event_2_timestamp": event_2_timestamp,
                        "distance": 0,
                        'author_id_x': row['author_id_x'],
                        'author_name_x': row['author_name_x'],
                        'author_login_x': row['author_login_x'],
                        'author_email_x': row['author_email_x'],
                        'author_id_y': row['author_id_y'],
                        'author_name_y': row['author_name_y'],
                        'author_login_y': row['author_login_y'],
                        'author_email_y': row['author_email_y']

                    })

    for pr_id, group in timeline.groupby(id_name):
        users = group[created_by].tolist()
        timestamps = group[created_at].tolist()
        event = group["event"].tolist()
        for i in range(len(users)):
            user_i = users[i]
            time_i = timestamps[i]
            for j in range(len(users)):
                user_j = users[j]
                time_j = timestamps[j]
                if user_i != user_j and time_j <= time_i:
                    interactions_df.append({
                        "from_user": user_j,
                        "to_user": user_i,
                        "event_1": event[j],
                        "event_2": event[i],
                        id_name: pr_id,
                        "event_1_timestamp": time_i,
                        "event_2_timestamp": time_j,
                        "distance": i - j,
                        'author_id_x': row['author_id_x'],
                        'author_name_x': row['author_name_x'],
                        'author_login_x': row['author_login_x'],
                        'author_email_x': row['author_email_x'],
                        'author_id_y': row['author_id_y'],
                        'author_name_y': row['author_name_y'],
                        'author_login_y': row['author_login_y'],
                        'author_email_y': row['author_email_y']

                    })
    interactions_df = pandas.DataFrame(interactions_df)
    #sort by timestamp
    interactions_df = interactions_df.sort_values(by=[id_name,"event_1_timestamp"])
    interactions_df.to_csv(f"{type}_interactions.csv", index=False)

    return interactions_df

def calculate_metrics(interactions_df: pandas.DataFrame, repo_full_name , tables) -> pandas.DataFrame:
    """
    Calculate daily metrics per developer from interaction data.
    Simple and clear approach - each metric calculated separately.
    """
    
    # Make a working copy
    df = interactions_df.copy()
    
    # Normalize timestamps
    df["event_1_timestamp"] = _norm_time(df["event_1_timestamp"])
    df["event_2_timestamp"] = _norm_time(df["event_2_timestamp"])
    
    # Create day column (floor to start of day)
    df["day"] = df["event_1_timestamp"].dt.floor("D")
    
    # Convert distance to numeric
    if "distance" in df.columns:
        df["distance"] = pandas.to_numeric(df["distance"], errors="coerce")
    
    # Remove rows with missing critical data
    df = df.dropna(subset=["from_user", "day"])
    
    # Separate dataframes: one with self-interactions, one without
    df_all = df.copy()  # Keep everything for counting total interactions
    df_noself = df[df["from_user"] != df["to_user"]].copy()  # Remove self-interactions for partner metrics
    
    # Start with base: all unique (from_user, day) combinations
    base = df_all[["from_user", "day"]].drop_duplicates().sort_values(["from_user", "day"]).reset_index(drop=True)

    result = base.copy()

    #befor any metrics we need to make the relatinal columns that kind our users. 
    # for each user we need to find their author_id, author_name, author_login, author_email
    user_info = []
    for user in result["from_user"].unique():
        # find first occurrence in df_all
        user_rows = df_all[df_all["from_user"] == user]
        if not user_rows.empty:
            first_row = user_rows.iloc[0]
            user_info.append({
                "from_user": user,
                "author_id": first_row.get('author_id_x', None),
                "author_name": first_row.get('author_name_x', None),
                "author_login": first_row.get('author_login_x', None),
                "author_email": first_row.get('author_email_x', None)
            })
    user_info_df = pandas.DataFrame(user_info)
    result = result.merge(user_info_df, on="from_user", how="left")
    
    # ============================================
    # METRIC 1: total_interactions
    # Count ALL interactions (including self) per user per day
    # ============================================
    total_interactions = (
        df_all.groupby(["from_user", "day"])
        .size()
        .reset_index(name="total_interactions")
    )
    result = result.merge(total_interactions, on=["from_user", "day"], how="left")
    
    # ============================================
    # METRIC 2: unique_partners
    # Count distinct people interacted with (excluding self) per day
    # ============================================
    unique_partners = (
        df_noself.groupby(["from_user", "day"])["to_user"]
        .nunique()
        .reset_index(name="unique_partners")
    )
    result = result.merge(unique_partners, on=["from_user", "day"], how="left")
    
    # ============================================
    # METRIC 3: items_touched
    # Count distinct issues/PRs touched per day
    # ============================================
    # Figure out which ID column to use
    id_col = None
    if "issue_number" in df_all.columns:
        id_col = "issue_number"
    elif "PR_id" in df_all.columns:
        id_col = "PR_id"
    
    if id_col:
        items_touched = (
            df_all.groupby(["from_user", "day"])[id_col]
            .nunique()
            .reset_index(name="items_touched")
        )
        result = result.merge(items_touched, on=["from_user", "day"], how="left")
    
    # ============================================
    # METRICS 4-7: Relationship tracking
    # Track new vs repeat relationships over time
    # ============================================
    if not df_noself.empty:
        # For each (from_user, to_user) pair, find the FIRST day they interacted
        pair_first_day = (
            df_noself.sort_values("event_1_timestamp")
            .groupby(["from_user", "to_user"])
            .agg({"day": "first"})
            .reset_index()
            .rename(columns={"day": "first_interaction_day"})
        )
        
        # Join back to get first_interaction_day for each row
        df_noself = df_noself.merge(pair_first_day, on=["from_user", "to_user"], how="left")
        
        # Mark rows where this is a NEW interaction (first time with this partner)
        df_noself["is_new_interaction"] = (df_noself["day"] == df_noself["first_interaction_day"])
        
        # METRIC 4: new_interactions
        # Count interaction ROWS that are first-time with a partner
        new_interactions = (
            df_noself.groupby(["from_user", "day"])["is_new_interaction"]
            .sum()
            .reset_index(name="new_interactions")
        )
        result = result.merge(new_interactions, on=["from_user", "day"], how="left")
        
        # METRIC 5: new_relationships
        # Count distinct NEW partners per day
        new_relationships = (
            df_noself[df_noself["is_new_interaction"]]
            .groupby(["from_user", "day"])["to_user"]
            .nunique()
            .reset_index(name="new_relationships")
        )
        result = result.merge(new_relationships, on=["from_user", "day"], how="left")
        
        # METRIC 6: total_relationships (cumulative)
        # For each user, count total unique partners up to and including each day
        all_days = result[["from_user", "day"]].copy()
        
        # Get all unique partners for each user up to each day
        total_rels = []
        for user in all_days["from_user"].unique():
            user_pairs = pair_first_day[pair_first_day["from_user"] == user].copy()
            user_days = all_days[all_days["from_user"] == user]["day"].unique()
            
            for day in user_days:
                # Count partners whose first interaction was on or before this day
                count = (user_pairs["first_interaction_day"] <= day).sum()
                total_rels.append({"from_user": user, "day": day, "total_relationships": count})
        
        total_rels_df = pandas.DataFrame(total_rels)
        result = result.merge(total_rels_df, on=["from_user", "day"], how="left")
        
    else:
        result["new_interactions"] = 0
        result["new_relationships"] = 0
        result["total_relationships"] = 0
    
    # METRIC 7: repeat_partners
    # Partners interacted with who are NOT new
    result["repeat_partners"] = result["unique_partners"].fillna(0) - result["new_relationships"].fillna(0)
    result["repeat_partners"] = result["repeat_partners"].clip(lower=0)
    
    # METRIC 8: avg_time_delta
    # Average time between event_1 and event_2
    df_all["time_delta_seconds"] = (
        (df_all["event_1_timestamp"] - df_all["event_2_timestamp"]).abs().dt.total_seconds()
    )
    avg_time_delta = (
        df_all.groupby(["from_user", "day"])["time_delta_seconds"]
        .mean()
        .reset_index(name="avg_time_delta")
    )
    result = result.merge(avg_time_delta, on=["from_user", "day"], how="left")
    
    # METRIC 9: min_response_time
    # Minimum time between events (fastest response)
    min_response = (
        df_all.groupby(["from_user", "day"])["time_delta_seconds"]
        .min()
        .reset_index(name="min_response_time")
    )
    result = result.merge(min_response, on=["from_user", "day"], how="left")

    #metric we can we look at the average time between interaction of interactions with only distance of 1?
    first_response_distance_1 = (
        df_all[df_all["distance"] == 1].groupby(["from_user", "day"])["time_delta_seconds"]
        .mean()
    )
    result = result.merge(first_response_distance_1.reset_index(name="avg_response_time_distance_1"), on=["from_user", "day"], how="left")

    # ============================================
    # METRIC 10: avg_interactions_per_user
    # Average interactions with each unique partner
    # ============================================
    # Avoid division by zero: if no unique partners, set to 0
    result["avg_interactions_per_user"] = 0.0
    mask = result["unique_partners"].fillna(0) > 0
    result.loc[mask, "avg_interactions_per_user"] = (
        result.loc[mask, "total_interactions"] / result.loc[mask, "unique_partners"]
    )
    
    # ============================================
    # METRIC 11: avg_distance_away
    # Average "distance" in the interaction sequence
    # ============================================
    if "distance" in df_all.columns:
        avg_distance = (
            df_all.groupby(["from_user", "day"])["distance"]
            .mean()
            .reset_index(name="avg_distance_away")
        )
        result = result.merge(avg_distance, on=["from_user", "day"], how="left")
    
    # ============================================
    # Clean up: Fill NaN values and ensure proper types
    # ============================================
    int_columns = [
        "total_interactions", "unique_partners", "items_touched",
        "new_interactions", "new_relationships", "total_relationships", "repeat_partners"
    ]
    for col in int_columns:
        if col in result.columns:
            result[col] = result[col].fillna(0).astype(int)
        
    # Sort by user and day
    result = result.sort_values(["from_user", "day"]).reset_index(drop=True)
    
    
    return result

def calculate_daily_metrics(tables, interactions_metrics, type ) -> pandas.DataFrame:
    # we will anayise the tables issue and issue_activity to make daily metrics.
    out_issues = interactions_metrics.copy()
    
    return out_issues

def make_interaction_graph(
    interactions_df: pandas.DataFrame,
    repo_full_name: str,
    *,
    filter_tf_only: bool = False,
    min_weight: int = 1,
    top_n_edges: int | None = None,
    seed: int = 42):
    """
    Build a directed interaction graph and return (G, fig, graph_df_used).

    - filter_tf_only: keep only edges where from_user OR to_user is in tf_devs
    - min_weight: drop edges with count < min_weight
    - top_n_edges: optionally keep only the top N edges by weight after filtering
    """

    # 1) collapse to edge list with weights
    graph_df = (
        interactions_df
        .groupby(["from_user", "to_user"])
        .size()
        .reset_index(name="count")
    )

    # drop very weak edges
    graph_df = graph_df[graph_df["count"] >= int(min_weight)]

    # optionally keep only the top-N strongest edges
    if top_n_edges is not None:
        graph_df = (
            graph_df.sort_values("count", ascending=False)
                    .head(int(top_n_edges))
                    .copy()
        )

    # 2) create DiGraph
    G = nx.DiGraph()
    for _, row in graph_df.iterrows():
        G.add_edge(row["from_user"], row["to_user"], weight=int(row["count"]))

    # empty guard
    if G.number_of_edges() == 0:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No edges after filtering.", ha="center", va="center")
        ax.axis("off")
        return G, fig, graph_df

    # 3) positions (scale k to size to keep things readable)
    n = max(1, G.number_of_nodes())
    k = 1.0 / np.sqrt(n)
    pos = nx.spring_layout(G, k=k, iterations=50, seed=seed)

    # 4) styling
    tf_set = set(graph_df["from_user"].tolist() )

    node_colors = [
        ("#FFD166" if node in tf_set else "#A8DADC")  # TF highlighted
        for node in G.nodes()
    ]
    node_edgecolors = ["#333333"] * G.number_of_nodes()

    # reciprocal vs one-way edges
    reciprocals = {(u, v) for (u, v) in G.edges() if G.has_edge(v, u)}
    solid_edges = [(u, v) for (u, v) in G.edges() if (u, v) in reciprocals]
    dashed_edges = [(u, v) for (u, v) in G.edges() if (u, v) not in reciprocals]

    # edge widths ~ weights (cap for aesthetics)
    def edge_widths(edgelist):
        return [min(6.0, 0.8 + 0.4 * G[u][v]["weight"]) for (u, v) in edgelist]

    # 5) draw
    fig, ax = plt.subplots(figsize=(12, 12))
    nx.draw_networkx_nodes(
        G, pos, node_size=300, node_color=node_colors,
        edgecolors=node_edgecolors, linewidths=0.6, ax=ax, alpha=0.95
    )
    if solid_edges:
        nx.draw_networkx_edges(
            G, pos, edgelist=solid_edges, width=edge_widths(solid_edges),
            arrowsize=12, ax=ax
        )
    if dashed_edges:
        nx.draw_networkx_edges(
            G, pos, edgelist=dashed_edges, width=edge_widths(dashed_edges),
            style="dashed", alpha=0.8, arrowsize=12, ax=ax
        )
    nx.draw_networkx_labels(G, pos, font_size=8, ax=ax)

    ax.set_title(
        f"Interaction Network — {'TF-focused' if filter_tf_only else 'All edges'} — {repo_full_name}",
        pad=12
    )
    ax.axis("off")

    return G, fig, graph_df

def main(repo_full_name=None, tf_devs=None, tables = None):

    # Initialize
    org, repo = repo_full_name.split('/')
    organization_folder = Path(cfg.main_folder, org, repo)
    social_technical_metrics_folder = Path(organization_folder, cfg.social_technical_metrics_folder)

    os.makedirs(social_technical_metrics_folder, exist_ok=True)
    # Save output
    out_file = Path(social_technical_metrics_folder ,cfg.social_technical_metrics_file)

    # Step 1:
    # we need to create the issue timeline
    issue_timeline, pr_timeline = timeline(tables)
    print("Timelines Created.")

    # Step 2:
    # now we need to take this joined data and create the interaction network
    issue_interactions = interaction_network(issue_timeline, "issue")
    print("Issue Interactions Created.")
    pr_interactions = interaction_network(pr_timeline, "PR")
    print("PR Interactions Created.")
    #we need to combine these interaction networks into a single one
    # can you add a column to tell which file it came from
    issue_interactions["source"] = "issue"
    pr_interactions["source"] = "PR"
    combined_interactions = pandas.concat([issue_interactions, pr_interactions], ignore_index=True)

    # Step 3:
    # In step 3 of this process we will need to make a daily metric

    issue_interactions_interactions_metrics = calculate_metrics(issue_interactions, repo_full_name, tables)

    pr_interactions_interactions_metrics = calculate_metrics(pr_interactions, repo_full_name, tables)

    #Step 4: 
    # in step 4 we will anayise the tables issue and issue_activity to make daily metrics.
    out_issues = calculate_daily_metrics(tables, issue_interactions_interactions_metrics, "issues")

    issue_interactions_interactions_metrics.to_csv(out_file , index=False)

    pr_interactions_interactions_metrics.to_csv(out_file , index=False)

    return issue_interactions_interactions_metrics, pr_interactions_interactions_metrics, combined_interactions

if __name__ == "__main__":
    # let the user spesify the repo to process
    repo_full_name = "Rdatatable/data.table"  # Example: "organization/repo"

    main(repo_full_name)
