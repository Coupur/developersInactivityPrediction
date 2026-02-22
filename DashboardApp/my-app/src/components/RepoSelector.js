import { useRepo } from '../context/RepoContext';

const selectorStyle = {
  padding: '6px 12px',
  fontSize: '14px',
  borderRadius: '4px',
  border: '1px solid #ccc',
  backgroundColor: '#fff',
  minWidth: '280px',
  cursor: 'pointer',
};

const containerStyle = {
  display: 'flex',
  alignItems: 'center',
  gap: '8px',
};

const labelStyle = {
  fontSize: '14px',
  fontWeight: '500',
  color: '#444',
};

const loadingStyle = {
  fontSize: '14px',
  color: '#666',
  fontStyle: 'italic',
};

export default function RepoSelector() {
  const { repositories, selectedRepo, selectRepo, loading, error } = useRepo();

  if (loading) {
    return <span style={loadingStyle}>Loading repositories...</span>;
  }

  if (error) {
    return <span style={{ ...loadingStyle, color: '#d32f2f' }}>{error}</span>;
  }

  if (!repositories.length) {
    return <span style={loadingStyle}>No repositories found</span>;
  }

  const handleChange = (event) => {
    const value = event.target.value;
    const [org, repo] = value.split('/');
    const found = repositories.find((r) => r.org === org && r.repo === repo);
    if (found) {
      selectRepo(found);
    }
  };

  return (
    <div style={containerStyle}>
      <label style={labelStyle} htmlFor="repo-selector">
        Repository:
      </label>
      <select
        id="repo-selector"
        style={selectorStyle}
        value={selectedRepo ? `${selectedRepo.org}/${selectedRepo.repo}` : ''}
        onChange={handleChange}
      >
        {repositories.map((r) => (
          <option key={`${r.org}/${r.repo}`} value={`${r.org}/${r.repo}`}>
            {r.displayName}
            {r.hasData.truckFactor ? '' : ' (limited data)'}
          </option>
        ))}
      </select>
    </div>
  );
}
