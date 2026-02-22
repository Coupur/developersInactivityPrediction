import * as React from 'react';
import Box from '@mui/material/Box';
import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import CircularProgress from '@mui/material/CircularProgress';
import Divider from '@mui/material/Divider';
import FormControl from '@mui/material/FormControl';
import InputLabel from '@mui/material/InputLabel';
import Select from '@mui/material/Select';
import MenuItem from '@mui/material/MenuItem';
import Card from '@mui/material/Card';
import CardContent from '@mui/material/CardContent';
import { SimpleTreeView } from '@mui/x-tree-view/SimpleTreeView';
import { TreeItem } from '@mui/x-tree-view/TreeItem';
import { useRepo } from './context/RepoContext';

const API_BASE = process.env.REACT_APP_API_BASE || 'http://localhost:3001';

const formatNumber = (value, options) => {
  if (value === null || value === undefined || value === '') {
    return '-';
  }
  const numeric = Number(value);
  if (Number.isNaN(numeric)) {
    return value;
  }
  return numeric.toLocaleString(undefined, options);
};

const buildNodeMap = (node, map = new Map()) => {
  if (!node) {
    return map;
  }
  map.set(node.id, node);
  if (node.children?.length) {
    node.children.forEach((child) => buildNodeMap(child, map));
  }
  return map;
};

const riskStyles = {
  1: { label: 'Most Risk', color: '#d32f2f' },
  2: { label: 'Medium Risk', color: '#f57c00' },
  3: { label: 'Lower Risk', color: '#fbc02d' },
  4: { label: 'No Issues', color: '#2e7d32' },
};

const RiskDot = ({ tier }) => {
  const resolvedTier = tier ?? 4;
  if (!riskStyles[resolvedTier]) {
    return null;
  }
  return (
    <span
      style={{
        display: 'inline-block',
        width: '8px',
        height: '8px',
        borderRadius: '50%',
        backgroundColor: riskStyles[resolvedTier].color,
        marginRight: '6px',
      }}
      aria-label={`${riskStyles[resolvedTier].label} indicator`}
    />
  );
};

const renderTreeItems = (node, fileRiskMap, folderRiskMap) => {
  const tier =
    node.type === 'tree' ? folderRiskMap.get(node.path) : fileRiskMap.get(node.path);
  const label = (
    <span>
      <RiskDot tier={tier} />
      {node.name}
    </span>
  );
  return (
    <TreeItem key={node.id} itemId={node.id} label={label}>
      {node.children?.map((child) =>
        renderTreeItems(child, fileRiskMap, folderRiskMap)
      )}
    </TreeItem>
  );
};
const findOwnedFilesInFolder = (doeRows, dev, folder) => {
  const prefix = folder ? `${folder.replace(/\/$/, '')}/` : '';
  const owned = [];
  doeRows.forEach((row) => {
    if (row.developer !== dev) {
      return;
    }
    const filePath = row.file_path;
    if (!filePath) {
      return;
    }
    if (!prefix || filePath.startsWith(prefix)) {
      owned.push(filePath);
    }
  });
  return owned;
};

const bestBackupsForFolder = (doeRows, dev, folder, ownedFiles, alpha = 0.8) => {
  const prefix = folder ? `${folder.replace(/\/$/, '')}/` : '';
  const ownedHere = ownedFiles.filter((filePath) =>
    prefix ? filePath.startsWith(prefix) : true
  );
  const candidateScores = new Map();
  let noBackupCount = 0;

  ownedHere.forEach((filePath) => {
    const rows = doeRows
      .filter((row) => row.file_path === filePath)
      .sort((a, b) => Number(b.DOE) - Number(a.DOE));
    if (!rows.length) {
      return;
    }
    const devRow = rows.find((row) => row.developer === dev);
    if (!devRow) {
      return;
    }
    const devDoe = Number(devRow.DOE);
    const others = rows.filter((row) => row.developer !== dev);
    if (!others.length) {
      noBackupCount += 1;
      return;
    }
    const coExperts = others.filter((row) => Number(row.DOE) >= alpha * devDoe);
    if (coExperts.length) {
      coExperts.forEach((row) => {
        const prev = candidateScores.get(row.developer) || 0;
        candidateScores.set(row.developer, prev + Number(row.DOE));
      });
    } else {
      const best = others[0];
      const prev = candidateScores.get(best.developer) || 0;
      candidateScores.set(best.developer, prev + Number(best.DOE));
    }
  });

  const topCandidates = Array.from(candidateScores.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([developer, score]) => ({ developer, score }));

  return { topCandidates, noBackupCount };
};

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

export default function RepoTreeExplorer() {
  const { selectedRepo, getRepoParams, loading: repoLoading } = useRepo();
  const [treeRoot, setTreeRoot] = React.useState(null);
  const [nodeMap, setNodeMap] = React.useState(new Map());
  const [selectedId, setSelectedId] = React.useState('');
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState('');
  const [devIds, setDevIds] = React.useState([]);
  const [selectedDev, setSelectedDev] = React.useState('');
  const [devHealth, setDevHealth] = React.useState(null);
  const [folderSummary, setFolderSummary] = React.useState([]);
  const [doeRows, setDoeRows] = React.useState([]);
  const [fileActivity, setFileActivity] = React.useState({});
  const [loadingMetrics, setLoadingMetrics] = React.useState(false);

  // Reset state when repo changes
  React.useEffect(() => {
    setTreeRoot(null);
    setNodeMap(new Map());
    setSelectedId('');
    setDevIds([]);
    setSelectedDev('');
    setDevHealth(null);
    setFolderSummary([]);
    setDoeRows([]);
    setFileActivity({});
    setError('');
  }, [selectedRepo]);

  React.useEffect(() => {
    if (!selectedRepo) return;
    let isActive = true;
    const loadRepoTree = async () => {
      setLoading(true);
      try {
        const response = await fetch(`${API_BASE}/api/repo-tree?${getRepoParams()}`);
        if (!response.ok) {
          throw new Error('Failed to load repository tree.');
        }
        const data = await response.json();
        if (!isActive) {
          return;
        }
        const root = data.root;
        const map = buildNodeMap(root);
        setTreeRoot(root);
        setNodeMap(map);
        setSelectedId(root?.id || '');
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

    loadRepoTree();
    return () => {
      isActive = false;
    };
  }, [selectedRepo, getRepoParams]);

  React.useEffect(() => {
    if (!selectedRepo) return;
    let isActive = true;
    const loadDevIds = async () => {
      try {
        const response = await fetch(`${API_BASE}/api/truckfactor?${getRepoParams()}`);
        if (!response.ok) {
          throw new Error('Failed to load truck factor developers.');
        }
        const data = await response.json();
        if (!isActive) {
          return;
        }
        const ids = data.devIds || [];
        setDevIds(ids);
        setSelectedDev((prev) => prev || ids[0] || '');
      } catch (err) {
        if (isActive) {
          setError(err.message);
        }
      }
    };

    loadDevIds();
    return () => {
      isActive = false;
    };
  }, [selectedRepo, getRepoParams]);

  React.useEffect(() => {
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

  React.useEffect(() => {
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

  React.useEffect(() => {
    if (!selectedDev || !selectedRepo) {
      return;
    }
    let isActive = true;
    const loadMetrics = async () => {
      setLoadingMetrics(true);
      setError('');
      try {
        const [healthResponse, folderResponse] = await Promise.all([
          fetch(`${API_BASE}/api/dev-health?dev=${encodeURIComponent(selectedDev)}&${getRepoParams()}`),
          fetch(
            `${API_BASE}/api/folder-summary?dev=${encodeURIComponent(selectedDev)}&${getRepoParams()}`
          ),
        ]);
        if (!healthResponse.ok) {
          throw new Error('Failed to load dev health metrics.');
        }
        if (!folderResponse.ok) {
          throw new Error('Failed to load folder summary data.');
        }
        const healthData = await healthResponse.json();
        const folderData = await folderResponse.json();
        if (!isActive) {
          return;
        }
        setDevHealth(healthData.metrics || null);
        setFolderSummary(folderData.rows || []);
      } catch (err) {
        if (isActive) {
          setError(err.message);
        }
      } finally {
        if (isActive) {
          setLoadingMetrics(false);
        }
      }
    };

    loadMetrics();
    return () => {
      isActive = false;
    };
  }, [selectedDev, selectedRepo, getRepoParams]);

  const selectedNode = selectedId ? nodeMap.get(selectedId) : null;

  const topRiskFolders = React.useMemo(() => {
    return [...folderSummary]
      .filter((row) => row.folder)
      .sort((a, b) => Number(b.risk_score) - Number(a.risk_score))
      .slice(0, 8);
  }, [folderSummary]);

  const ownedFiles = React.useMemo(
    () => findOwnedFilesInFolder(doeRows, selectedDev, ''),
    [doeRows, selectedDev]
  );

  const folderMetrics = React.useMemo(() => {
    if (!selectedNode || selectedNode.type !== 'tree') {
      return null;
    }
    const folderRow = folderSummary.find(
      (row) => row.folder === selectedNode.path
    );
    if (!folderRow) {
      return null;
    }
    const backups = bestBackupsForFolder(
      doeRows,
      selectedDev,
      selectedNode.path,
      ownedFiles,
      0.8
    );
    return { folderRow, backups };
  }, [selectedNode, folderSummary, doeRows, selectedDev, ownedFiles]);

  const fileMetrics = React.useMemo(() => {
    if (!selectedNode || selectedNode.type !== 'blob') {
      return null;
    }
    return summarizeFileDoe(doeRows, selectedNode.meta?.path, 0.8);
  }, [selectedNode, doeRows]);

  const treeFileSet = React.useMemo(() => {
    const set = new Set();
    if (!treeRoot) {
      return set;
    }
    const walk = (node) => {
      if (node.type === 'blob' && node.path) {
        set.add(node.path);
      }
      node.children?.forEach(walk);
    };
    walk(treeRoot);
    return set;
  }, [treeRoot]);

  const riskRankedFiles = React.useMemo(() => {
    if (!selectedDev || !doeRows.length) {
      return [];
    }
    const filePaths = new Set(
      doeRows.filter((row) => row.developer === selectedDev).map((row) => row.file_path)
    );
    const ranked = [];
    filePaths.forEach((filePath) => {
      if (treeFileSet.size && !treeFileSet.has(filePath)) {
        return;
      }
      const summary = summarizeFileDoe(doeRows, filePath, 0.8);
      const risk = classifyFileRisk(summary, selectedDev);
      if (!risk) {
        return;
      }
      const activity = fileActivity[filePath] || { commitCount: 0, lastCommitAt: null };
      const daysSince = computeDaysSince(activity.lastCommitAt);
      ranked.push({
        filePath,
        tier: risk.tier,
        label: risk.label,
        summary,
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
        return a.summary.contributorCount - b.summary.contributorCount;
      });
  }, [doeRows, selectedDev, fileActivity, treeFileSet]);

  const topRiskFiles = React.useMemo(
    () => riskRankedFiles.slice(0, 10),
    [riskRankedFiles]
  );

  const fileRiskMap = React.useMemo(() => {
    const map = new Map();
    riskRankedFiles.forEach((item) => {
      map.set(item.filePath, item.tier);
    });
    // mark remaining files as "No Issues" so they show green
    doeRows.forEach((row) => {
      if (
        row.file_path &&
        !map.has(row.file_path) &&
        (!treeFileSet.size || treeFileSet.has(row.file_path))
      ) {
        map.set(row.file_path, 4);
      }
    });
    return map;
  }, [riskRankedFiles, doeRows, treeFileSet]);

  const folderRiskMap = React.useMemo(() => {
    const map = new Map();
    fileRiskMap.forEach((tier, filePath) => {
      const parts = String(filePath).split('/').filter(Boolean);
      let prefix = '';
      parts.slice(0, -1).forEach((part) => {
        prefix = prefix ? `${prefix}/${part}` : part;
        const existing = map.get(prefix);
        if (!existing || tier < existing) {
          map.set(prefix, tier);
        }
      });
    });
    // default folders to green when they have no risky files
    if (treeRoot) {
      const applyDefaults = (node) => {
        if (node.type === 'tree' && !map.has(node.path)) {
          map.set(node.path, 4);
        }
        node.children?.forEach(applyDefaults);
      };
      applyDefaults(treeRoot);
    }
    return map;
  }, [fileRiskMap, treeRoot]);

  const detailRow = (label, value) => (
    <Box
      key={label}
      sx={{ display: 'flex', justifyContent: 'space-between', py: 0.5 }}
    >
      <Typography variant="body2" color="text.secondary">
        {label}
      </Typography>
      <Typography variant="body2">{value || '-'}</Typography>
    </Box>
  );

  if (repoLoading) {
    return (
      <Box sx={{ p: 2 }}>
        <Typography variant="h5" gutterBottom>
          Repository Explorer
        </Typography>
        <Typography>Loading repositories...</Typography>
      </Box>
    );
  }

  return (
    <Box sx={{ p: 2 }}>
      <Typography variant="h5" gutterBottom>
        Repository Explorer
      </Typography>
      {selectedRepo && (
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          Analyzing: <strong>{selectedRepo.displayName}</strong>
        </Typography>
      )}
      <FormControl size="small" sx={{ minWidth: 320 }}>
        <InputLabel id="dev-select-label">Developer</InputLabel>
        <Select
          labelId="dev-select-label"
          value={selectedDev}
          label="Developer"
          onChange={(event) => setSelectedDev(event.target.value)}
        >
          {devIds.map((devId) => (
            <MenuItem key={devId} value={devId}>
              {devId}
            </MenuItem>
          ))}
        </Select>
      </FormControl>
      {error && (
        <Typography color="error" sx={{ mt: 1 }}>
          {error}
        </Typography>
      )}
      <Box sx={{ mt: 2 }}>
        <Typography variant="h6" gutterBottom>
          Top Files at Risk of Undermaintenance
        </Typography>
        {topRiskFiles.length ? (
          <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
            {topRiskFiles.map((item) => (
              <Card key={item.filePath} sx={{ width: 320 }}>
                <CardContent>
                  <Typography variant="subtitle2" color="text.secondary">
                    <RiskDot tier={item.tier} />
                    {item.label}
                  </Typography>
                  <Typography variant="body1" sx={{ mb: 1 }}>
                    {item.filePath}
                  </Typography>
                  <Typography variant="body2">
                    Owners: {item.summary.owners.map((row) => row.developer).join(', ')}
                  </Typography>
                  <Typography variant="body2">
                    Co-experts:{' '}
                    {item.summary.coExperts.map((row) => row.developer).join(', ') ||
                      '-'}
                  </Typography>
                  <Typography variant="body2">
                    Contributors: {item.summary.contributorCount}
                  </Typography>
                  <Typography variant="body2">
                    Commits: {item.activity.commitCount}
                  </Typography>
                  <Typography variant="body2">
                    Last change:{' '}
                    {item.activity.lastCommitAt
                      ? new Date(item.activity.lastCommitAt).toLocaleDateString()
                      : 'Unknown'}
                  </Typography>
                </CardContent>
              </Card>
            ))}
          </Box>
        ) : (
          <Typography variant="body2">
            No high-risk files found for this developer.
          </Typography>
        )}
      </Box>
      <Box sx={{ display: 'flex', gap: 2, mt: 2, alignItems: 'flex-start' }}>
        <Paper sx={{ width: 320, p: 1, maxHeight: '75vh', overflow: 'auto' }}>
          <Typography variant="subtitle1" sx={{ mb: 1 }}>
            Repository
          </Typography>
          {loading ? (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <CircularProgress size={18} />
              <Typography variant="body2">Loading repository tree...</Typography>
            </Box>
          ) : treeRoot ? (
            <SimpleTreeView
              selectedItems={selectedId ? [selectedId] : []}
              onSelectedItemsChange={(event, itemIds) => {
                const items = Array.isArray(itemIds) ? itemIds : [itemIds];
                setSelectedId(items[0] || '');
              }}
            >
              {renderTreeItems(treeRoot, fileRiskMap, folderRiskMap)}
            </SimpleTreeView>
          ) : (
            <Typography variant="body2">No tree data found.</Typography>
          )}
        </Paper>
        <Paper sx={{ flex: 1, p: 2, minHeight: '40vh' }}>
          {loadingMetrics ? (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <CircularProgress size={18} />
              <Typography variant="body2">Loading metrics...</Typography>
            </Box>
          ) : selectedNode ? (
            <Box>
              <Typography variant="h6" gutterBottom>
                {selectedNode.type === 'tree'
                  ? 'Folder Overview'
                  : 'File Overview'}
              </Typography>
              {detailRow('Name', selectedNode.name)}
              {detailRow(
                'Path',
                selectedNode.meta?.path || selectedNode.path || '-'
              )}
              {detailRow('Type', selectedNode.type)}
              {detailRow('Size', formatNumber(selectedNode.meta?.size))}
              <Divider sx={{ my: 2 }} />
              {selectedNode.type === 'tree' && folderMetrics ? (
                <Box>
                  <Typography variant="subtitle1" gutterBottom>
                    Folder Metrics
                  </Typography>
                  {detailRow('Total Files', folderMetrics.folderRow.total_files)}
                  {detailRow('Owned Files', folderMetrics.folderRow.n_owned)}
                  {detailRow(
                    'Owned Share',
                    formatNumber(folderMetrics.folderRow.owned_share, {
                      minimumFractionDigits: 2,
                      maximumFractionDigits: 2,
                    })
                  )}
                  {detailRow(
                    'Only Contributor Files',
                    folderMetrics.folderRow.n_only_contributor
                  )}
                  {detailRow(
                    'Only Contributor Share',
                    formatNumber(folderMetrics.folderRow.only_contributor_share, {
                      minimumFractionDigits: 2,
                      maximumFractionDigits: 2,
                    })
                  )}
                  {detailRow(
                    'Single Expert Files',
                    folderMetrics.folderRow.n_single_expert
                  )}
                  {detailRow(
                    'Single Expert Share',
                    formatNumber(folderMetrics.folderRow.single_expert_share, {
                      minimumFractionDigits: 2,
                      maximumFractionDigits: 2,
                    })
                  )}
                  {detailRow(
                    'Multi Expert Files',
                    folderMetrics.folderRow.n_multi_expert
                  )}
                  {detailRow(
                    'Multi Expert Share',
                    formatNumber(folderMetrics.folderRow.multi_expert_share, {
                      minimumFractionDigits: 2,
                      maximumFractionDigits: 2,
                    })
                  )}
                  {detailRow('Risk Score', folderMetrics.folderRow.risk_score)}
                  {detailRow('TF', folderMetrics.folderRow.tf)}
                  {detailRow(
                    'TF Devs',
                    Array.isArray(folderMetrics.folderRow.tf_devs)
                      ? folderMetrics.folderRow.tf_devs.join(', ')
                      : folderMetrics.folderRow.tf_devs || '-'
                  )}
                  <Divider sx={{ my: 2 }} />
                  <Typography variant="subtitle1" gutterBottom>
                    Who Can Cover?
                  </Typography>
                  {folderMetrics.backups.topCandidates.length ? (
                    folderMetrics.backups.topCandidates.map((candidate) =>
                      detailRow(
                        candidate.developer,
                        formatNumber(candidate.score, {
                          minimumFractionDigits: 2,
                          maximumFractionDigits: 2,
                        })
                      )
                    )
                  ) : (
                    <Typography variant="body2">
                      No cover available for owned files.
                    </Typography>
                  )}
                  {detailRow(
                    'Files with no backups',
                    folderMetrics.backups.noBackupCount
                  )}
                </Box>
              ) : null}
              {selectedNode.type === 'blob' && fileMetrics ? (
                <Box>
                  <Typography variant="subtitle1" gutterBottom>
                    File Ownership
                  </Typography>
                  {detailRow(
                    'Owners',
                    fileMetrics.owners.map((row) => row.developer).join(', ') ||
                      '-'
                  )}
                  {detailRow(
                    'Top Contributors',
                    fileMetrics.topContributors
                      .map((row) => row.developer)
                      .join(', ') || '-'
                  )}
                  {detailRow(
                    'Co-experts',
                    fileMetrics.coExperts
                      .map((row) => row.developer)
                      .join(', ') || '-'
                  )}
                  {detailRow('Contributor Count', fileMetrics.contributorCount)}
                </Box>
              ) : null}
            </Box>
          ) : devHealth ? (
            <Box>
              <Typography variant="h6" gutterBottom>
                Developer Overview
              </Typography>
              {detailRow('Owned Files', devHealth.n_owned_files)}
              {detailRow(
                'Only Contributor Files',
                devHealth.n_only_contributor_files
              )}
              {detailRow(
                'Single Expert Files',
                devHealth.n_single_expert_files
              )}
              <Divider sx={{ my: 2 }} />
              <Typography variant="subtitle1" gutterBottom>
                Top Risk Folders
              </Typography>
              {topRiskFolders.length ? (
                topRiskFolders.map((row) =>
                  detailRow(
                    row.folder,
                    formatNumber(row.risk_score, {
                      minimumFractionDigits: 0,
                      maximumFractionDigits: 2,
                    })
                  )
                )
              ) : (
                <Typography variant="body2">No folder data available.</Typography>
              )}
            </Box>
          ) : (
            <Typography variant="body2">
              Select a file or folder to view details.
            </Typography>
          )}
        </Paper>
      </Box>
    </Box>
  );
}
