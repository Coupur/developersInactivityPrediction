import Navigation from '../components/Navigation';
import { useRepo } from '../context/RepoContext';

export default function SocialView() {
  const { selectedRepo, loading } = useRepo();

  return (
    <div style={{ padding: '16px' }}>
      <Navigation />
      <h2>Social View</h2>
      {loading ? (
        <p>Loading repositories...</p>
      ) : selectedRepo ? (
        <>
          <p style={{ color: '#666', marginBottom: '12px' }}>
            Analyzing: <strong>{selectedRepo.displayName}</strong>
          </p>
          <p>Social metrics for this repository will be displayed here.</p>
        </>
      ) : (
        <p>Please select a repository from the Main page.</p>
      )}
    </div>
  );
}
