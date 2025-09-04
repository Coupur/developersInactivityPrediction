### GitHub Settings
items_per_page = 100  # The number of results in each page of the GitHub results. Max: 100
tokens_file = "../Resources/tokens.csv"  # The relative path of the file containing the list of the github tokens
main_file_path = r"C:\Users\samut\OneDrive\Documents\GitHub\developersInactivityAnalysisCOPY"


next_page = "next_page_PR.txt"
last_page ="last_flushed_page_PR.txt"
next_page_commits = "next_page_commits.txt"  # The file where the next page of the commits will be stored
last_page_commits = "last_flushed_page_commits.txt"  # The file where the last flushed page of the commits will be stored
next_page_issues = "next_page_issues.txt"  # The file where the next page of the issues will be stored
last_page_issues = "last_flushed_page_issues.txt"  # The file where the last flushed page of the issues will be stored

### Extraction Settings
data_collection_date = "2025-08-26"  # The max date to consider for the commits and activities extraction
repos_file = "Resources/repositories.txt"  # The relative path of the file containing the list of the repos <organization/repo>
main_folder = "../Organizations"  # The main folder where results will be archived
logs_folder = "../logs"  # The folder where the logs will be archived
results_folder = "/Results"  # The folder where the results will be archived
temp_data_folder = "../temp_data"  # The folder where temporary data will be stored

model_path = "../PredictionModel/model.joblib"

supported_modes = ['tf', 'a80', 'a80mod', 'a80api']

TF_report_folder = "../Organizations/.tf_cache"  # The folder where the TF/core developers are archived
TF_developers_file = "TruckFactor.csv" # The file where the TF/core developers are listed as <name;login>ù
## WARNING: The correct path to save the <TF_developers_file> is <TF_report_folder>/<organization/mainRepo>/<TF_developers_file>

A80_report_folder = "../A80_Results"  # The folder where the TF/core developers are archived
A80_developers_file = "A80_devs.csv" # The file where the TF/core developers are listed as <name;login>

modTh = 5

pauses_list_file_name = "coding_pauses.csv"  # The file where the lists of devs' pauses durations will be archived
pauses_dates_file_name = "pauses_coredevs.csv"  # The file where the lists of devs' pauses boundary dates will be archived


commit_history_table_file_name = "commit_history_table.csv"  # The file where the 'devs by dates' table for each repo will be archived
coding_history_table_file_name = "coding_history_table.csv" # Analogous to Commit_history_table but includes PR creation and NON merged commits
issue_comments_list_file_name = "issues_comments_repo.csv"  
issue_events_list_file_name = "issues_events_repo.csv"  
issue_list_file_name = "issues_repo.csv"  
issue_timeline_file_name = "issues_timeline_repo.csv"  
PR_list_file_name= "prs_repo.csv" 
prs_comments_csv = "prs_comments.csv"  
commit_list_file_name = "commit_list.csv"  
#core_commit_coverage = 0.8 

### Files Settings
CSV_separator = ","  # Character for cell separation in the used files
CSV_missing = "NA"  # Character for the missing values in the used files

### Breaks Identification
timeline_folder_name = 'Timelines'  # The folder where the other repo activities will be archived
breaks_folder_name = 'Dev_Breaks'  # The folder where the breaks list for each developer will be archived
sliding_window_size = 90  # The size in days of the sliding window
shift = 7  # The number of days to shift the sliding window of

### Breaks Labeling
labeled_breaks_folder_name = breaks_folder_name + '/Labeled_Breaks'  # The folder where the labeled breaks list for each developer will be archived
gone_threshold = 365  # Threshold to label a break as 'GONE'

### Statistics
chains_folder_name = 'Chains'  # The folder where the %age of the transitions for each organization will be archived

### CONSTANTS
A = 'ACTIVE'  # Label of the Active status: Developers contribute commits
NC = 'NON_CODING'  # Label of the Non-coding status: Developers do not contribute commits, but show other activity signals
I = 'INACTIVE'  # Label of the Inactive status: Developers do not show any activity signal
G = 'GONE'  # Label of the Gone status: Developers have been Inactive for longer than <gone_threshold>



#key_folders = ['Activities_Plots', 'Dead&Resurrected_Users', 'Hibernated&Unfrozen_Users', 'Sleeping&Awaken_Users', 'DevStats_Plots', 'Longer_Breaks']

#commit page storage
