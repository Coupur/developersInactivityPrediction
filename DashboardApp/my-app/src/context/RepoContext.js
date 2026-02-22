import { createContext, useContext, useState, useEffect, useMemo, useCallback } from 'react';

const API_BASE = process.env.REACT_APP_API_BASE || 'http://localhost:3001';

const RepoContext = createContext(null);

export function RepoProvider({ children }) {
  const [repositories, setRepositories] = useState([]);
  const [selectedRepo, setSelectedRepo] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  // Load available repositories on mount
  useEffect(() => {
    let isActive = true;
    const loadRepositories = async () => {
      setLoading(true);
      try {
        const response = await fetch(`${API_BASE}/api/repositories`);
        if (!response.ok) {
          throw new Error('Failed to load repositories.');
        }
        const data = await response.json();
        if (!isActive) return;

        const repos = data.repositories || [];
        setRepositories(repos);

        // Try to restore from localStorage or use first repo
        const savedRepo = localStorage.getItem('selectedRepo');
        if (savedRepo) {
          try {
            const parsed = JSON.parse(savedRepo);
            const found = repos.find(
              (r) => r.org === parsed.org && r.repo === parsed.repo
            );
            if (found) {
              setSelectedRepo(found);
            } else if (repos.length > 0) {
              setSelectedRepo(repos[0]);
            }
          } catch {
            if (repos.length > 0) {
              setSelectedRepo(repos[0]);
            }
          }
        } else if (repos.length > 0) {
          setSelectedRepo(repos[0]);
        }
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

    loadRepositories();
    return () => {
      isActive = false;
    };
  }, []);

  // Persist selection to localStorage
  useEffect(() => {
    if (selectedRepo) {
      localStorage.setItem(
        'selectedRepo',
        JSON.stringify({ org: selectedRepo.org, repo: selectedRepo.repo })
      );
    }
  }, [selectedRepo]);

  const selectRepo = useCallback((repo) => {
    setSelectedRepo(repo);
  }, []);

  // Helper to build query params for API calls
  const getRepoParams = useCallback(() => {
    if (!selectedRepo) return '';
    return `org=${encodeURIComponent(selectedRepo.org)}&repo=${encodeURIComponent(selectedRepo.repo)}`;
  }, [selectedRepo]);

  // Helper to append repo params to existing query string
  const appendRepoParams = useCallback(
    (queryString = '') => {
      const repoParams = getRepoParams();
      if (!repoParams) return queryString;
      if (!queryString) return repoParams;
      return `${queryString}&${repoParams}`;
    },
    [getRepoParams]
  );

  const value = useMemo(
    () => ({
      repositories,
      selectedRepo,
      selectRepo,
      loading,
      error,
      getRepoParams,
      appendRepoParams,
    }),
    [repositories, selectedRepo, selectRepo, loading, error, getRepoParams, appendRepoParams]
  );

  return <RepoContext.Provider value={value}>{children}</RepoContext.Provider>;
}

export function useRepo() {
  const context = useContext(RepoContext);
  if (!context) {
    throw new Error('useRepo must be used within a RepoProvider');
  }
  return context;
}

export default RepoContext;
