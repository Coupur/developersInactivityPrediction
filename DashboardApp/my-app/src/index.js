import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import reportWebVitals from './reportWebVitals';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { RepoProvider } from './context/RepoContext';
import MainView from './views/MainView';
import FileView from './views/FileView';
import SocialView from './views/SocialView';
import DashboardView from './views/DashboardView';

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(
  <React.StrictMode>
    <RepoProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<MainView />} />
          <Route path="/dashboard" element={<DashboardView />} />
          <Route path="/files" element={<FileView />} />
          <Route path="/social" element={<SocialView />} />
        </Routes>
      </BrowserRouter>
    </RepoProvider>
  </React.StrictMode>
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
