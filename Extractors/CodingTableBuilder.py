### IMPORT EXCEPTION MODULES
import csv
import logging
import os
import sys
sys.path.append('../')
from datetime import datetime  # remove 'time' here
import time

import pandas
### IMPORT SYSTEM MODULES
from github import Github, GithubException
from github.GithubException import IncompletableObject
from requests.exceptions import Timeout

### IMPORT CUSTOM MODULES
import Settings as cfg
import Utilities as util


def mergeCodingActivities(organization):
    organization_folder = os.path.join(cfg.main_folder, organization)

    for folder in os.listdir(organization_folder):
        if os.path.isdir(organization_folder + '/' + folder):
            buildCodingActivitiesLists(os.path.join(organization_folder, folder))

    # General one
    buildCodingActivitiesLists(organization_folder)




def buildCodingActivitiesLists(destination_folder):
    commits_filename = cfg.commit_list_file_name
    
    prs_filename =  cfg.PR_list_file_name
    #TODO: add missing commits
    #missing_commits_filename = cfg.missing_commits_file_name

    out_coding_list_filename = 'coding_activities_list.csv'

    coding_activities_data = pandas.DataFrame(columns=['id', 'date', 'author'])

    if commits_filename in os.listdir(destination_folder):
        commits_data = pandas.read_csv(os.path.join(destination_folder, commits_filename), sep=cfg.CSV_separator)
        columns_to_merge = pandas.DataFrame({'id': commits_data['sha'],
                                             'date': commits_data['created_at'],
                                             'author': commits_data['created_by']})
        coding_activities_data = pandas.concat([coding_activities_data,
                                                columns_to_merge[~columns_to_merge.id.isin(coding_activities_data.id)]],
                                               ignore_index=True)
    if prs_filename in os.listdir(destination_folder):
        prs_data = pandas.read_csv(os.path.join(destination_folder, prs_filename), sep=cfg.CSV_separator)
        #created_at,created_by,PR_id
        columns_to_merge = pandas.DataFrame({'id': prs_data['PR_id'],
                                             'date': prs_data['created_at'],
                                             'author': prs_data['created_by']})
        coding_activities_data = pandas.concat([coding_activities_data,
                                                columns_to_merge[~columns_to_merge.id.isin(coding_activities_data.id)]],
                                               ignore_index=True)
    #if missing_commits_filename in os.listdir(destination_folder):
    #    missing_commits_data = pandas.read_csv(os.path.join(destination_folder, missing_commits_filename),
    #                                           sep=cfg.CSV_separator)
    #    columns_to_merge = pandas.DataFrame({'id': missing_commits_data['sha'],
    #                                         'date': missing_commits_data['date'],
    #                                         'author': missing_commits_data['author_id']})
    #    coding_activities_data = pandas.concat([coding_activities_data,
    #                                            columns_to_merge[~columns_to_merge.id.isin(coding_activities_data.id)]],
    #                                           ignore_index=True)

    if not coding_activities_data.empty:
        coding_activities_data.to_csv(os.path.join(destination_folder, out_coding_list_filename),
                                      sep=cfg.CSV_separator, na_rep=cfg.CSV_missing, index=False, quoting=None,
                                      lineterminator='\n')

def buildHistoryTables(organization):
    organization_folder = os.path.join(cfg.main_folder, organization)

    for folder in os.listdir(organization_folder):
        if os.path.isdir(organization_folder + '/' + folder):
            buildTable(os.path.join(organization_folder, folder))

    # General one
    buildTable(organization_folder)

def buildTable(destination_folder):
    coding_activities_filename = 'coding_activities_list.csv'

    out_coding_history_filename = 'coding_history_table.csv'

    if coding_activities_filename not in os.listdir(destination_folder):
        return

    coding_data = pandas.read_csv(
        os.path.join(destination_folder, coding_activities_filename),
        sep=cfg.CSV_separator,
        usecols=["date", "author"]  # only what we need
    ).dropna(subset=["date", "author"])

    # Normalize to UTC -> naive -> midnight, then count per (author, date)
    coding_data["date"] = (
        pandas.to_datetime(coding_data["date"], errors="coerce", utc=True)
              .dt.tz_localize(None)
              .dt.normalize()
    )
    coding_data = coding_data.dropna(subset=["date"])

    # Group & pivot to wide (counts of coding activities per day)
    counts = (coding_data
              .assign(cnt=1)
              .groupby(["author", "date"], as_index=False)["cnt"].sum())

    wide = (counts
            .pivot(index="author", columns="date", values="cnt")
            .fillna(0)
            .astype("int32"))

    # Ensure a full continuous date range (fill missing days with 0)
    full_dates = pandas.date_range(wide.columns.min(), wide.columns.max(), freq="D")
    wide = wide.reindex(columns=full_dates, fill_value=0).astype("int32")

    # Final formatting: user_id column + YYYY-MM-DD headers
    wide = wide.reset_index().rename(columns={"author": "user_id"})
    wide.columns = [c.strftime("%Y-%m-%d") if isinstance(c, pandas.Timestamp) else c for c in wide.columns]

    wide.to_csv(
        os.path.join(destination_folder, out_coding_history_filename),
        sep=cfg.CSV_separator,
        na_rep=cfg.CSV_missing,
        index=False,
        quoting=None,
        lineterminator="\n",
    )
    

def writePauses(organization):
    organization_folder = os.path.join(cfg.main_folder, organization)

    for folder in os.listdir(organization_folder):
        if os.path.isdir(organization_folder + '/' + folder):
            computePauses(os.path.join(organization_folder, folder))

    # General one
    computePauses(organization_folder)

def computePauses(destination_folder):
    """Computes the Pauses and writes
    1. the Intervals file containing for each developer the list of its pauses' length
    2. the Breaks Dates file containing for each developer the list of date intervals"""

    coding_history_filename = 'coding_history_table.csv'
    out_coding_pauses_filename = 'coding_pauses.csv'
    out_coding_pauses_dates_filename = 'coding_pauses_dates.csv'

    if coding_history_filename in os.listdir(destination_folder):
        coding_table = pandas.read_csv(os.path.join(destination_folder, coding_history_filename), sep=cfg.CSV_separator)

        # Calcola days between coding activities, if activities are in adjacent days count 1
        pauses_duration_list = []
        pauses_dates_list = []
        for _, u in coding_table.iterrows():
            row = [u.iloc[0]]  # User_id
            current_pause_dates = [u.iloc[0]]   # User_id
            coding_dates = []
            for i in range(1, len(u)):
                if (u.iloc[i] > 0):
                    coding_dates.append(coding_table.columns[i])
            for i in range(0, len(coding_dates) - 1):
                period = util.daysBetween(coding_dates[i], coding_dates[i + 1])
                if (period > 1):
                    row.append(period)
                    current_pause_dates.append(coding_dates[i] + '/' + coding_dates[i + 1])
            # ADD LAST PAUSE
            last_coding_day = coding_dates[-1]
            collection_day=cfg.data_collection_date
            period = util.daysBetween(last_coding_day, collection_day)
            if (period > 1):
                row.append(period)
                current_pause_dates.append(last_coding_day + '/' + collection_day)

            # Wrap up the list
            pauses_duration_list.append(row)
            pauses_dates_list.append(current_pause_dates)
            user_lifespan = util.daysBetween(coding_dates[0], coding_dates[len(coding_dates) - 1]) + 1
            commit_frequency = len(coding_dates) / user_lifespan
            row.append(user_lifespan)
            row.append(commit_frequency)

        with open(os.path.join(destination_folder, out_coding_pauses_filename), 'w', newline='') as outcsv:
            writer = csv.writer(outcsv, quoting=csv.QUOTE_NONE, delimiter=cfg.CSV_separator, quotechar='"', escapechar='\\')
            for r in pauses_duration_list:
                writer.writerow(r)

        with open(os.path.join(destination_folder, out_coding_pauses_dates_filename), 'w', newline='') as outcsv:
            writer = csv.writer(outcsv, quoting=csv.QUOTE_NONE, delimiter=cfg.CSV_separator, quotechar='"', escapechar='\\')
            for r in pauses_dates_list:
                writer.writerow(r)

### MAIN FUNCTION
def main(repos_list):

    for gitRepoName in repos_list:
        slug = gitRepoName.replace('https://github.com/', '')
        organization, _ = slug.split('/')

        t0 = time.perf_counter()
        mergeCodingActivities(organization)
        print("merge:", time.perf_counter()-t0, "s")

        t1 = time.perf_counter()
        buildHistoryTables(organization)
        print("history:", time.perf_counter()-t1, "s")

        t2 = time.perf_counter()
        writePauses(organization)
        print("pauses:", time.perf_counter()-t2, "s")

    logging.info('History Tables and Pauses computing SUCCESSFULLY COMPLETED for {}'.format(organization))

if __name__ == "__main__":
    THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
    os.chdir(THIS_FOLDER)

    os.makedirs(cfg.logs_folder, exist_ok=True)
    timestamp = datetime.strftime(datetime.now(), '%Y-%m-%d_%H:%M')
    logfile = cfg.logs_folder+f"/Coding_Table_Pause_Builder_{timestamp}.log"
    logging.basicConfig(filename=logfile, level=logging.INFO)
    

    repoUrls = '../' + cfg.repos_file
    repos_list = []
    with open(repoUrls) as f:
        repoUrls = f.readlines()
        for repoUrl in repoUrls:
            repos_list.append(repoUrl.strip())


    print("Repositories to analyze:" , repos_list)
    main(repos_list)