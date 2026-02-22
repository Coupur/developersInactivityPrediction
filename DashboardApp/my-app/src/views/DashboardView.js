import { useEffect, useMemo, useState } from 'react';
import Navigation from '../components/Navigation';
import { useRepo } from '../context/RepoContext';

const API_BASE = process.env.REACT_APP_API_BASE || 'http://localhost:3001';

const summarizeFileDoe = (doeRows, filePath, alpha = 0.8) => {
  const rows = doeRows
    .filter((row) => row.file_path === filePath)
    .map((row) => ({ ...row, DOE: Number(row.DOE) }))
    .sort((a, b) => b.DOE - a.DOE);
  if (!rows.length) {
    return {
      owners: [],
      topContributors: [],
      coExperts: [],
      contributorCount: 0,
    };
  }
  const maxDoe = rows[0].DOE;
  const owners = rows.filter((row) => row.DOE === maxDoe);
  const topContributors = rows.slice(0, 5);
  const coExperts = rows.filter((row) => row.DOE >= alpha * maxDoe);
  return {
    owners,
    topContributors,
    coExperts,
    contributorCount: rows.length,
  };
};

const classifyFileRisk = (summary, dev) => {
  const owners = summary.owners.map((row) => row.developer);
  const topContributors = summary.topContributors.map((row) => row.developer);
  const coExperts = summary.coExperts.map((row) => row.developer);
  const contributorCount = summary.contributorCount;
  const devIsOwner = owners.includes(dev);

  if (
    devIsOwner &&
    contributorCount === 1 &&
    owners.length === 1 &&
    topContributors.length === 1 &&
    coExperts.length === 1
  ) {
    return { tier: 1, label: 'Most Risk' };
  }

  if (devIsOwner && contributorCount > 1 && coExperts.length === 1) {
    return { tier: 2, label: 'Medium Risk' };
  }

  if (devIsOwner && coExperts.length > 1) {
    return { tier: 3, label: 'Lower Risk' };
  }

  return null;
};

const computeDaysSince = (dateString) => {
  if (!dateString) {
    return null;
  }
  const date = new Date(dateString);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  const diffMs = Date.now() - date.getTime();
  return Math.floor(diffMs / (1000 * 60 * 60 * 24));
};

export default function DashboardView() {
  const { selectedRepo, getRepoParams, loading: repoLoading } = useRepo();
  const [devIds, setDevIds] = useState([]);
  const [dev, setDev] = useState('');
  const [pastMonths, setPastMonths] = useState(6);
  const [futureDays, setFutureDays] = useState(7);
  const [summary, setSummary] = useState(null);
  const [doeRows, setDoeRows] = useState([]);
  const [fileActivity, setFileActivity] = useState({});
  const [profile, setProfile] = useState(null);
  const [loadingProfile, setLoadingProfile] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  // Reset state when repo changes
  useEffect(() => {
    setDevIds([]);
    setDev('');
    setSummary(null);
    setDoeRows([]);
    setFileActivity({});
    setProfile(null);
    setError('');
  }, [selectedRepo]);

  useEffect(() => {
    if (!selectedRepo) return;
    let isActive = true;
    const loadDevs = async () => {
      try {
        const response = await fetch(`${API_BASE}/api/truckfactor?${getRepoParams()}`);
        if (!response.ok) {
          throw new Error('Failed to load developers.');
        }
        const data = await response.json();
        if (!isActive) {
          return;
        }
        setDevIds(data.devIds || []);
        setDev((prev) => prev || data.devIds?.[0] || '');
      } catch (err) {
        if (isActive) {
          setError(err.message);
        }
      }
    };
    loadDevs();
    return () => {
      isActive = false;
    };
  }, [selectedRepo, getRepoParams]);

  useEffect(() => {
    if (!selectedRepo) return;
    let isActive = true;
    const loadDoe = async () => {
      try {
        const response = await fetch(`${API_BASE}/api/doe?${getRepoParams()}`);
        if (!response.ok) {
          throw new Error('Failed to load DOE data.');
        }
        const data = await response.json();
        if (!isActive) {
          return;
        }
        setDoeRows(data.rows || []);
      } catch (err) {
        if (isActive) {
          setError(err.message);
        }
      }
    };

    loadDoe();
    return () => {
      isActive = false;
    };
  }, [selectedRepo, getRepoParams]);

  useEffect(() => {
    if (!selectedRepo) return;
    let isActive = true;
    const loadFileActivity = async () => {
      try {
        const response = await fetch(`${API_BASE}/api/file-activity?${getRepoParams()}`);
        if (!response.ok) {
          throw new Error('Failed to load file activity.');
        }
        const data = await response.json();
        if (!isActive) {
          return;
        }
        setFileActivity(data.activityMap || {});
      } catch (err) {
        if (isActive) {
          setError(err.message);
        }
      }
    };

    loadFileActivity();
    return () => {
      isActive = false;
    };
  }, [selectedRepo, getRepoParams]);

  useEffect(() => {
    if (!dev || !selectedRepo) {
      return;
    }
    let isActive = true;
    const loadSummary = async () => {
      setLoading(true);
      setError('');
      try {
        const response = await fetch(
          `${API_BASE}/api/timeline-summary?dev=${encodeURIComponent(
            dev
          )}&pastMonths=${encodeURIComponent(pastMonths)}&futureDays=${encodeURIComponent(
            futureDays
          )}&${getRepoParams()}`
        );
        if (!response.ok) {
          throw new Error('Failed to load timeline summary.');
        }
        const data = await response.json();
        if (!isActive) {
          return;
        }
        setSummary(data);
      } catch (err) {
        if (isActive) {
          setError(err.message);
        }
      } finally {
        if (isActive) {
          setLoading(false);
        }
      }
    };
    loadSummary();
    return () => {
      isActive = false;
    };
  }, [dev, pastMonths, futureDays, selectedRepo, getRepoParams]);

  useEffect(() => {
    if (!dev || !selectedRepo) {
      return;
    }
    let isActive = true;
    const loadProfile = async () => {
      setLoadingProfile(true);
      setError('');
      try {
        const response = await fetch(
          `${API_BASE}/api/contribution-profile?dev=${encodeURIComponent(
            dev
          )}&pastMonths=${encodeURIComponent(pastMonths)}&${getRepoParams()}`
        );
        if (!response.ok) {
          throw new Error('Failed to load contribution profile.');
        }
        const data = await response.json();
        if (!isActive) {
          return;
        }
        setProfile(data);
      } catch (err) {
        if (isActive) {
          setError(err.message);
        }
      } finally {
        if (isActive) {
          setLoadingProfile(false);
        }
      }
    };

    loadProfile();
    return () => {
      isActive = false;
    };
  }, [dev, pastMonths, selectedRepo, getRepoParams]);

  const formatDelta = (value) => {
    if (value > 0) {
      return `+${value}`;
    }
    return String(value);
  };

  const changeRows = useMemo(() => {
    if (!summary) {
      return [];
    }
    const formatLoss = (value) => {
      const absValue = Math.abs(value || 0);
      return `-${absValue.toFixed(2)}`;
    };
    return [
      { label: 'Change in # commits', value: formatLoss(summary.deltas.commits) },
      { label: 'Change in # of PRs', value: formatLoss(summary.deltas.prs) },
      {
        label: 'Change in code reviews',
        value: formatLoss(summary.deltas.pr_activity),
      },
      {
        label: 'Change in backlog',
        value: formatLoss(summary.deltas.issue_activity),
      },
      { label: '# of issues', value: formatLoss(summary.deltas.issues) },
    ];
  }, [summary]);

  const topRiskFiles = useMemo(() => {
    if (!dev || !doeRows.length) {
      return [];
    }
    const filePaths = new Set(
      doeRows.filter((row) => row.developer === dev).map((row) => row.file_path)
    );
    const ranked = [];
    filePaths.forEach((filePath) => {
      const summaryData = summarizeFileDoe(doeRows, filePath, 0.8);
      const risk = classifyFileRisk(summaryData, dev);
      if (!risk) {
        return;
      }
      const activity = fileActivity[filePath] || {
        commitCount: 0,
        lastCommitAt: null,
      };
      const daysSince = computeDaysSince(activity.lastCommitAt);
      ranked.push({
        filePath,
        tier: risk.tier,
        label: risk.label,
        activity: {
          commitCount: activity.commitCount,
          lastCommitAt: activity.lastCommitAt,
          daysSince,
        },
      });
    });
    return ranked
      .sort((a, b) => {
        if (a.tier !== b.tier) {
          return a.tier - b.tier;
        }
        if (a.activity.daysSince !== b.activity.daysSince) {
          if (a.activity.daysSince === null) return 1;
          if (b.activity.daysSince === null) return -1;
          return a.activity.daysSince - b.activity.daysSince;
        }
        if (a.activity.commitCount !== b.activity.commitCount) {
          return b.activity.commitCount - a.activity.commitCount;
        }
        return a.filePath.localeCompare(b.filePath);
      })
      // #region agent log
      .map((item, index, arr) => {
        if (index === 0) {
          try {
            const sample = arr.slice(0, 5).map((entry) => ({
              filePath: entry.filePath,
              tier: entry.tier,
              daysSince: entry.activity.daysSince,
              commitCount: entry.activity.commitCount,
            }));
            fetch(
              'http://127.0.0.1:7245/ingest/b5efc590-1f42-4bcb-8587-2ad2f05f2d60',
              {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  runId: 'pre-fix',
                  hypothesisId: 'H1',
                  location: 'DashboardView.js:topRiskFiles',
                  message: 'Top risk sort order sample',
                  data: { sample },
                  timestamp: Date.now(),
                }),
              }
            ).catch(() => {});
          } catch (error) {
            // ignore logging errors
          }
        }
        return item;
      })
      // #endregion
      .slice(0, 10);
  }, [dev, doeRows, fileActivity]);

  if (repoLoading) {
    return (
      <div style={{ padding: '16px' }}>
        <Navigation />
        <h2>Dashboard View</h2>
        <p>Loading repositories...</p>
      </div>
    );
  }

  return (
    <div style={{ padding: '16px' }}>
      <Navigation />
      <h2>Dashboard View</h2>
      {selectedRepo && (
        <p style={{ color: '#666', marginBottom: '12px' }}>
          Analyzing: <strong>{selectedRepo.displayName}</strong>
        </p>
      )}
      <div style={{ marginBottom: '16px' }}>
        <label style={{ marginRight: '12px' }}>
          Developer:{' '}
          <select value={dev} onChange={(event) => setDev(event.target.value)}>
            {devIds.map((id) => (
              <option key={id} value={id}>
                {id}
              </option>
            ))}
          </select>
        </label>
        <label style={{ marginRight: '12px' }}>
          # of months:{' '}
          <input
            type="number"
            min="1"
            value={pastMonths}
            onChange={(event) => setPastMonths(Number(event.target.value))}
            style={{ width: '80px' }}
          />
        </label>
        <label>
          # of days:{' '}
          <input
            type="number"
            min="1"
            value={futureDays}
            onChange={(event) => setFutureDays(Number(event.target.value))}
            style={{ width: '80px' }}
          />
        </label>
      </div>
      {error && <p style={{ color: 'red' }}>{error}</p>}
      {loading ? (
        <p>Loading summary...</p>
      ) : summary ? (
        <div>
          <p>
            Using latest activity date:{' '}
            {new Date(summary.referenceDate).toLocaleDateString()}
          </p>
          <p>
            Past window: {summary.pastMonths} months ({summary.pastTotals.commits}{' '}
            commits). Expected loss over {summary.futureDays} days.
          </p>
          <ul>
            {changeRows.map((row) => (
              <li key={row.label}>
                {row.label}: {row.value}
              </li>
            ))}
          </ul>
          <div>
            <p>
              <strong>Most risky files</strong>
            </p>
            {topRiskFiles.length ? (
              <ol>
                {topRiskFiles.map((item) => (
                  <li key={item.filePath}>
                    {item.filePath} ({item.label}
                    {item.activity.lastCommitAt
                      ? `, last change ${new Date(
                          item.activity.lastCommitAt
                        ).toLocaleDateString()}`
                      : ', last change unknown'}
                    )
                  </li>
                ))}
              </ol>
            ) : (
              <p>No high-risk files found for this developer.</p>
            )}
          </div>
          <div style={{ marginTop: '16px' }}>
            <p>
              <strong>Contribution Profile</strong>
            </p>
            {loadingProfile ? (
              <p>Loading contribution profile...</p>
            ) : profile ? (
              <table>
                <thead>
                  <tr>
                    <th style={{ textAlign: 'left', paddingRight: '12px' }}>
                      Dimension
                    </th>
                    <th style={{ textAlign: 'right', paddingRight: '12px' }}>
                      Total
                    </th>
                    <th style={{ textAlign: 'right' }}>Per Day</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>Commits</td>
                    <td style={{ textAlign: 'right', paddingRight: '12px' }}>
                      {profile.totals.commits}
                    </td>
                    <td style={{ textAlign: 'right' }}>
                      {profile.rates.commits_per_day.toFixed(2)}
                    </td>
                  </tr>
                  <tr>
                    <td>PR creation</td>
                    <td style={{ textAlign: 'right', paddingRight: '12px' }}>
                      {profile.totals.prs}
                    </td>
                    <td style={{ textAlign: 'right' }}>
                      {profile.rates.prs_per_day.toFixed(2)}
                    </td>
                  </tr>
                  <tr>
                    <td>Reviews</td>
                    <td style={{ textAlign: 'right', paddingRight: '12px' }}>
                      {profile.totals.reviews}
                    </td>
                    <td style={{ textAlign: 'right' }}>
                      {profile.rates.reviews_per_day.toFixed(2)}
                    </td>
                  </tr>
                  <tr>
                    <td>Issues/Discussion</td>
                    <td style={{ textAlign: 'right', paddingRight: '12px' }}>
                      {profile.totals.issues + profile.totals.issue_activity}
                    </td>
                    <td style={{ textAlign: 'right' }}>
                      {profile.rates.issues_per_day.toFixed(2)}
                    </td>
                  </tr>
                  <tr>
                    <td>Coordination</td>
                    <td style={{ textAlign: 'right', paddingRight: '12px' }}>
                      {profile.totals.coordination}
                    </td>
                    <td style={{ textAlign: 'right' }}>
                      {profile.rates.coordination_per_day.toFixed(2)}
                    </td>
                  </tr>
                  <tr>
                    <td>Newcomer support</td>
                    <td style={{ textAlign: 'right', paddingRight: '12px' }}>
                      {profile.totals.newcomer_support}
                    </td>
                    <td style={{ textAlign: 'right' }}>
                      {profile.rates.newcomer_support_per_day.toFixed(2)}
                    </td>
                  </tr>
                </tbody>
              </table>
            ) : (
              <p>No profile data available.</p>
            )}
          </div>
        </div>
      ) : (
        <p>Select a developer to see summary.</p>
      )}
    </div>
  );
}
