import Navigation from '../components/Navigation';
import { useRepo } from '../context/RepoContext';

const cardStyle = {
  border: '1px solid #e0e0e0',
  borderRadius: '8px',
  padding: '16px',
  marginBottom: '12px',
  backgroundColor: '#fafafa',
};

const selectedCardStyle = {
  ...cardStyle,
  border: '2px solid #1976d2',
  backgroundColor: '#e3f2fd',
};

const tagStyle = {
  display: 'inline-block',
  padding: '2px 8px',
  borderRadius: '12px',
  fontSize: '12px',
  marginRight: '6px',
  backgroundColor: '#e8f5e9',
  color: '#2e7d32',
};

const missingTagStyle = {
  ...tagStyle,
  backgroundColor: '#fff3e0',
  color: '#e65100',
};

export default function MainView() {
  const { repositories, selectedRepo, selectRepo, loading, error } = useRepo();

  return (
    <div style={{ padding: '16px' }}>
      <Navigation />
      <h2>Developer Inactivity Analysis Dashboard</h2>
      <p style={{ color: '#666', marginBottom: '24px' }}>
        Select a project to analyze developer activity patterns and assess risk when contributors step away.
        This dashboard helps quantify metrics for core and regular contributors across dimensions: commits, PR creation, reviews, issues/discussion, coordination, and newcomer support.
      </p>

      <h3>Available Projects ({repositories.length})</h3>
      
      {loading && <p>Loading repositories...</p>}
      {error && <p style={{ color: 'red' }}>{error}</p>}
      
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(350px, 1fr))', gap: '12px' }}>
        {repositories.map((repo) => {
          const isSelected = selectedRepo?.org === repo.org && selectedRepo?.repo === repo.repo;
          return (
            <div
              key={`${repo.org}/${repo.repo}`}
              style={isSelected ? selectedCardStyle : cardStyle}
              onClick={() => selectRepo(repo)}
              role="button"
              tabIndex={0}
              onKeyDown={(e) => e.key === 'Enter' && selectRepo(repo)}
            >
              <h4 style={{ margin: '0 0 8px 0' }}>
                {repo.displayName}
                {isSelected && <span style={{ marginLeft: '8px', color: '#1976d2' }}>✓ Selected</span>}
              </h4>
              <div>
                {repo.hasData.truckFactor && <span style={tagStyle}>Truck Factor</span>}
                {repo.hasData.timelines && <span style={tagStyle}>Timelines</span>}
                {repo.hasData.socialNetwork && <span style={tagStyle}>Social Network</span>}
                {!repo.hasData.truckFactor && <span style={missingTagStyle}>Missing TF</span>}
                {!repo.hasData.timelines && <span style={missingTagStyle}>Missing Timelines</span>}
              </div>
            </div>
          );
        })}
      </div>

      {selectedRepo && (
        <div style={{ marginTop: '24px', padding: '16px', backgroundColor: '#e8f5e9', borderRadius: '8px' }}>
          <h3 style={{ margin: '0 0 8px 0' }}>Currently Selected: {selectedRepo.displayName}</h3>
          <p style={{ margin: '0 0 12px 0' }}>
            Use the navigation links above to explore the Dashboard, File View, or Social View for this repository.
          </p>
        </div>
      )}
    </div>
  );
}
