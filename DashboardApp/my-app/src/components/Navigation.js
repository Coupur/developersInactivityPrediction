import { NavLink } from 'react-router-dom';
import RepoSelector from './RepoSelector';

const linkStyle = ({ isActive }) => ({
  marginRight: '12px',
  textDecoration: 'none',
  fontWeight: isActive ? '600' : '400',
});

const navContainerStyle = {
  display: 'flex',
  justifyContent: 'space-between',
  alignItems: 'center',
  marginBottom: '16px',
  flexWrap: 'wrap',
  gap: '12px',
};

const linksStyle = {
  display: 'flex',
  alignItems: 'center',
};

export default function Navigation() {
  return (
    <nav style={navContainerStyle}>
      <div style={linksStyle}>
        <NavLink to="/" end style={linkStyle}>
          Main
        </NavLink>
        <NavLink to="/dashboard" style={linkStyle}>
          Dashboard
        </NavLink>
        <NavLink to="/files" style={linkStyle}>
          File View
        </NavLink>
        <NavLink to="/social" style={linkStyle}>
          Social View
        </NavLink>
      </div>
      <RepoSelector />
    </nav>
  );
}
