const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs/promises');
const { parse } = require('csv-parse/sync');

const app = express();
const PORT = process.env.PORT || 3001;

const organizationsPath = path.resolve(
  __dirname,
  '..',
  '..',
  '..',
  'Organizations'
);

// Default repository (used if none specified)
const DEFAULT_ORG = 'Rdatatable';
const DEFAULT_REPO = 'data.table';

// Helper to get the data path for a specific repository
const getDataPath = (org, repo) => {
  const orgName = org || DEFAULT_ORG;
  const repoName = repo || DEFAULT_REPO;
  return path.join(organizationsPath, orgName, repoName);
};

// Helper to extract org/repo from query params
const getRepoFromQuery = (query) => {
  return {
    org: query.org || DEFAULT_ORG,
    repo: query.repo || DEFAULT_REPO,
  };
};

app.use(cors());

const coerceValue = (value) => {
  if (value === null || value === undefined) {
    return value;
  }
  const trimmed = String(value).trim();
  if (trimmed === '') {
    return '';
  }
  const numeric = Number(trimmed);
  return Number.isNaN(numeric) ? trimmed : numeric;
};

const parseCsvContent = (content) => {
  const trimmed = content.trim();
  if (trimmed.startsWith('[')) {
    try {
      return JSON.parse(trimmed);
    } catch (error) {
      throw new Error('TruckFactor.csv is not valid JSON.');
    }
  }

  const records = parse(trimmed, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  });

  return records.map((row) => {
    const normalized = {};
    for (const [key, value] of Object.entries(row)) {
      if (typeof value === 'string' && value.trim().startsWith('[')) {
        try {
          normalized[key] = JSON.parse(value.replace(/'/g, '"'));
        } catch (error) {
          normalized[key] = value;
        }
      } else {
        normalized[key] = coerceValue(value);
      }
    }
    return normalized;
  });
};

const buildRepoTree = (rows) => {
  const repoName = rows[0]?.repo || 'Repository';
  const root = {
    id: repoName,
    name: repoName,
    path: '',
    type: 'tree',
    children: [],
    meta: {},
  };
  const nodeMap = new Map([[root.id, root]]);

  const ensureNode = (parent, nodeId, name, type) => {
    let node = nodeMap.get(nodeId);
    if (!node) {
      node = {
        id: nodeId,
        name,
        path: nodeId,
        type,
        children: [],
        meta: {},
      };
      nodeMap.set(nodeId, node);
      parent.children.push(node);
    }
    return node;
  };

  rows.forEach((row) => {
    if (!row.path) {
      return;
    }
    const parts = String(row.path).split('/').filter(Boolean);
    let parent = root;
    let currentPath = '';

    parts.forEach((part, index) => {
      currentPath = currentPath ? `${currentPath}/${part}` : part;
      const isLeaf = index === parts.length - 1;
      const nodeType = isLeaf ? row.type || 'blob' : 'tree';
      const node = ensureNode(parent, currentPath, part, nodeType);

      if (isLeaf) {
        node.type = row.type || node.type;
        node.meta = {
          repo: row.repo,
          sha: row.sha,
          size: row.size,
          mode: row.mode,
          path: row.path,
        };
      }

      parent = node;
    });
  });

  const sortTree = (node) => {
    if (!node.children?.length) {
      return;
    }
    node.children.sort((a, b) => {
      if (a.type === b.type) {
        return a.name.localeCompare(b.name);
      }
      return a.type === 'tree' ? -1 : 1;
    });
    node.children.forEach(sortTree);
  };

  sortTree(root);
  return root;
};

// List all available repositories
app.get('/api/repositories', async (req, res) => {
  try {
    const orgs = await fs.readdir(organizationsPath, { withFileTypes: true });
    const repositories = [];

    for (const org of orgs) {
      if (!org.isDirectory()) continue;
      
      const orgPath = path.join(organizationsPath, org.name);
      const repos = await fs.readdir(orgPath, { withFileTypes: true });
      
      for (const repo of repos) {
        if (!repo.isDirectory()) continue;
        
        // Check if this repo has a commit_list.csv (indicating it's a valid data repo)
        const repoPath = path.join(orgPath, repo.name);
        try {
          await fs.access(path.join(repoPath, 'commit_list.csv'));
          
          // Get some basic stats about the repo
          const files = await fs.readdir(repoPath);
          const hasTruckFactor = files.includes('TruckFactor');
          const hasTimelines = files.includes('Timelines');
          const hasSocialNetwork = files.includes('SocialTechnicalNetwrok');
          
          repositories.push({
            org: org.name,
            repo: repo.name,
            displayName: `${org.name}/${repo.name}`,
            path: repoPath,
            hasData: {
              truckFactor: hasTruckFactor,
              timelines: hasTimelines,
              socialNetwork: hasSocialNetwork,
            },
          });
        } catch {
          // Skip repos without commit_list.csv
        }
      }
    }

    // Sort alphabetically by display name
    repositories.sort((a, b) => a.displayName.localeCompare(b.displayName));

    return res.json({ repositories });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/truckfactor', async (req, res) => {
  try {
    const { org, repo } = getRepoFromQuery(req.query);
    const dataPath = getDataPath(org, repo);
    const tfPath = path.join(dataPath, 'TruckFactor', 'TruckFactor.csv');
    const content = await fs.readFile(tfPath, 'utf8');
    const devIds = parseCsvContent(content);

    if (!Array.isArray(devIds)) {
      return res.status(500).json({ error: 'Unexpected TruckFactor format.' });
    }

    return res.json({ devIds, org, repo });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/folder-summary', async (req, res) => {
  const { dev } = req.query;
  if (!dev) {
    return res.status(400).json({ error: 'Missing dev query param.' });
  }

  try {
    const { org, repo } = getRepoFromQuery(req.query);
    const dataPath = getDataPath(org, repo);
    const fileName = `${dev}_folder_summary_df.csv`;
    const filePath = path.join(dataPath, 'DevHealthMetrics', fileName);
    const content = await fs.readFile(filePath, 'utf8');
    const rows = parseCsvContent(content);

    return res.json({ rows });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/dev-health', async (req, res) => {
  const { dev } = req.query;
  if (!dev) {
    return res.status(400).json({ error: 'Missing dev query param.' });
  }

  try {
    const { org, repo } = getRepoFromQuery(req.query);
    const dataPath = getDataPath(org, repo);
    const fileName = `${dev}_dev_health_metrics.csv`;
    const filePath = path.join(dataPath, 'DevHealthMetrics', fileName);
    const content = await fs.readFile(filePath, 'utf8');
    const rows = parseCsvContent(content);
    const metrics = rows?.[0] || null;

    return res.json({ metrics });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/doe', async (req, res) => {
  try {
    const { org, repo } = getRepoFromQuery(req.query);
    const dataPath = getDataPath(org, repo);
    const filePath = path.join(dataPath, 'TruckFactor', 'DOE.csv');
    const content = await fs.readFile(filePath, 'utf8');
    const rows = parseCsvContent(content);

    return res.json({ rows });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/file-activity', async (req, res) => {
  try {
    const { org, repo } = getRepoFromQuery(req.query);
    const dataPath = getDataPath(org, repo);
    const filePath = path.join(dataPath, 'per_file_commits.csv');
    const content = await fs.readFile(filePath, 'utf8');
    const rows = parseCsvContent(content);

    const activityMap = {};
    let latestCommitAt = null;
    let earliestCommitAt = null;
    let missingCommittedAtCount = 0;
    rows.forEach((row) => {
      const file = row.file_path;
      if (!file) {
        return;
      }
      if (!activityMap[file]) {
        activityMap[file] = { commitCount: 0, lastCommitAt: null };
      }
      activityMap[file].commitCount += 1;
      const committedAt = row.committed_at;
      if (!committedAt) {
        missingCommittedAtCount += 1;
      }
      if (committedAt) {
        if (!latestCommitAt || committedAt > latestCommitAt) {
          latestCommitAt = committedAt;
        }
        if (!earliestCommitAt || committedAt < earliestCommitAt) {
          earliestCommitAt = committedAt;
        }
      }
      if (
        committedAt &&
        (!activityMap[file].lastCommitAt ||
          committedAt > activityMap[file].lastCommitAt)
      ) {
        activityMap[file].lastCommitAt = committedAt;
      }
    });

    return res.json({ activityMap });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

const parseDate = (value) => {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
};

const sumMetrics = (rows) => {
  return rows.reduce(
    (acc, row) => {
      acc.commits += Number(row.commits || 0);
      acc.prs += Number(row.prs || 0);
      acc.issues += Number(row.issues || 0);
      acc.issue_activity += Number(row.issue_activity || 0);
      acc.pr_activity += Number(row.pr_activity || 0);
      return acc;
    },
    { commits: 0, prs: 0, issues: 0, issue_activity: 0, pr_activity: 0 }
  );
};

const sumContributionTotals = (rows) => {
  return rows.reduce(
    (acc, row) => {
      acc.commits += Number(row.commits || 0);
      acc.prs += Number(row.prs || 0);
      acc.issues += Number(row.issues || 0);
      acc.issue_activity += Number(row.issue_activity || 0);
      acc.pr_activity += Number(row.pr_activity || 0);
      acc.issue_total_interactions += Number(row.issue_total_interactions || 0);
      acc.issue_unique_partners += Number(row.issue_unique_partners || 0);
      acc.pr_total_interactions += Number(row.pr_total_interactions || 0);
      acc.pr_unique_partners += Number(row.pr_unique_partners || 0);
      return acc;
    },
    {
      commits: 0,
      prs: 0,
      issues: 0,
      issue_activity: 0,
      pr_activity: 0,
      issue_total_interactions: 0,
      issue_unique_partners: 0,
      pr_total_interactions: 0,
      pr_unique_partners: 0,
    }
  );
};

const sumSocialTotals = (rows) => {
  return rows.reduce(
    (acc, row) => {
      acc.total_interactions += Number(row.total_interactions || 0);
      acc.unique_partners += Number(row.unique_partners || 0);
      acc.items_touched += Number(row.items_touched || 0);
      acc.new_interactions += Number(row.new_interactions || 0);
      acc.new_relationships += Number(row.new_relationships || 0);
      acc.total_relationships += Number(row.total_relationships || 0);
      acc.repeat_partners += Number(row.repeat_partners || 0);
      return acc;
    },
    {
      total_interactions: 0,
      unique_partners: 0,
      items_touched: 0,
      new_interactions: 0,
      new_relationships: 0,
      total_relationships: 0,
      repeat_partners: 0,
    }
  );
};

app.get('/api/timeline-summary', async (req, res) => {
  const { dev, pastMonths, futureDays } = req.query;
  if (!dev) {
    return res.status(400).json({ error: 'Missing dev query param.' });
  }

  const months = Number(pastMonths || 6);
  const days = Number(futureDays || 7);

  try {
    const { org, repo } = getRepoFromQuery(req.query);
    const dataPath = getDataPath(org, repo);
    const filePath = path.join(dataPath, 'Timelines', 'timeline.csv');
    const content = await fs.readFile(filePath, 'utf8');
    const rows = parseCsvContent(content).filter((row) => row.dev === dev);

    const dates = rows
      .map((row) => parseDate(row.date))
      .filter(Boolean)
      .sort((a, b) => a - b);
    const referenceDate = dates[dates.length - 1] || new Date();
    const pastStart = new Date(referenceDate);
    pastStart.setMonth(pastStart.getMonth() - months);
    const futureEnd = new Date(referenceDate);
    futureEnd.setDate(futureEnd.getDate() + days);

    const pastRows = rows.filter((row) => {
      const date = parseDate(row.date);
      return date && date >= pastStart && date < referenceDate;
    });

    const pastTotals = sumMetrics(pastRows);
    const pastDays =
      pastStart && referenceDate
        ? Math.max(
            1,
            Math.ceil((referenceDate.getTime() - pastStart.getTime()) / 86400000)
          )
        : Math.max(1, months * 30);
    const ratePerDay = {
      commits: pastTotals.commits / pastDays,
      prs: pastTotals.prs / pastDays,
      issues: pastTotals.issues / pastDays,
      issue_activity: pastTotals.issue_activity / pastDays,
      pr_activity: pastTotals.pr_activity / pastDays,
    };
    const expectedLoss = {
      commits: ratePerDay.commits * days,
      prs: ratePerDay.prs * days,
      issues: ratePerDay.issues * days,
      issue_activity: ratePerDay.issue_activity * days,
      pr_activity: ratePerDay.pr_activity * days,
    };

    return res.json({
      dev,
      referenceDate: referenceDate.toISOString(),
      pastMonths: months,
      futureDays: days,
      pastTotals,
      deltas: {
        commits: -expectedLoss.commits,
        prs: -expectedLoss.prs,
        issues: -expectedLoss.issues,
        issue_activity: -expectedLoss.issue_activity,
        pr_activity: -expectedLoss.pr_activity,
      },
      ratePerDay,
      expectedLoss,
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/contribution-profile', async (req, res) => {
  const { dev, pastMonths, startDate, endDate } = req.query;
  if (!dev) {
    return res.status(400).json({ error: 'Missing dev query param.' });
  }

  const months = Number(pastMonths || 6);

  try {
    const { org, repo } = getRepoFromQuery(req.query);
    const dataPath = getDataPath(org, repo);
    const timelinePath = path.join(dataPath, 'SocialTechnicalNetwrok', 'timeline_combined.csv');
    const socialPath = path.join(
      dataPath,
      'SocialTechnicalNetwrok',
      'social_technical_metrics.csv'
    );
    const timelineContent = await fs.readFile(timelinePath, 'utf8');
    const socialContent = await fs.readFile(socialPath, 'utf8');

    const timelineRows = parseCsvContent(timelineContent).filter(
      (row) => row.dev === dev
    );
    const socialRows = parseCsvContent(socialContent).filter(
      (row) => row.from_user === dev
    );

    const timelineDates = timelineRows
      .map((row) => parseDate(row.date))
      .filter(Boolean)
      .sort((a, b) => a - b);
    const socialDates = socialRows
      .map((row) => parseDate(row.day))
      .filter(Boolean)
      .sort((a, b) => a - b);

    const referenceDate = parseDate(endDate) || timelineDates.at(-1) || socialDates.at(-1);
    if (!referenceDate) {
      return res.status(404).json({ error: 'No activity data found for dev.' });
    }

    const windowStart = parseDate(startDate) || new Date(referenceDate);
    if (!startDate) {
      windowStart.setMonth(windowStart.getMonth() - months);
    }

    const windowDays = Math.max(
      1,
      Math.ceil((referenceDate.getTime() - windowStart.getTime()) / 86400000)
    );

    const windowTimeline = timelineRows.filter((row) => {
      const date = parseDate(row.date);
      return date && date >= windowStart && date <= referenceDate;
    });
    const windowSocial = socialRows.filter((row) => {
      const date = parseDate(row.day);
      return date && date >= windowStart && date <= referenceDate;
    });

    const timelineTotals = sumContributionTotals(windowTimeline);
    const socialTotals = sumSocialTotals(windowSocial);

    const coordinationTotal =
      socialTotals.total_interactions + socialTotals.unique_partners;
    const newcomerSupportTotal =
      socialTotals.new_interactions + socialTotals.new_relationships;

    const rates = {
      commits_per_day: timelineTotals.commits / windowDays,
      prs_per_day: timelineTotals.prs / windowDays,
      reviews_per_day: timelineTotals.pr_activity / windowDays,
      issues_per_day: (timelineTotals.issues + timelineTotals.issue_activity) / windowDays,
      coordination_per_day: coordinationTotal / windowDays,
      newcomer_support_per_day: newcomerSupportTotal / windowDays,
    };

    return res.json({
      dev,
      windowStart: windowStart.toISOString(),
      windowEnd: referenceDate.toISOString(),
      windowDays,
      totals: {
        commits: timelineTotals.commits,
        prs: timelineTotals.prs,
        reviews: timelineTotals.pr_activity,
        issues: timelineTotals.issues,
        issue_activity: timelineTotals.issue_activity,
        coordination: coordinationTotal,
        newcomer_support: newcomerSupportTotal,
      },
      rates,
      sources: {
        timeline: 'SocialTechnicalNetwrok/timeline_combined.csv',
        social: 'SocialTechnicalNetwrok/social_technical_metrics.csv',
      },
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/repo-tree', async (req, res) => {
  try {
    const { org, repo } = getRepoFromQuery(req.query);
    const dataPath = getDataPath(org, repo);
    const treePath = path.join(dataPath, 'repo_tree.csv');
    const content = await fs.readFile(treePath, 'utf8');
    const rows = parseCsvContent(content);
    const root = buildRepoTree(rows);

    return res.json({ root });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`API server running on http://localhost:${PORT}`);
});
