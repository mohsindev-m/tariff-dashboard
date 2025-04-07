import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import Dashboard from './components/Dashboard';
import { TariffProvider } from './context/TariffContext';
import './App.css';

function App() {
  return (
    <TariffProvider>
      <Router>
        <div className="app bg-gray-100 min-h-screen">
          <Routes>
            <Route path="/" element={<Dashboard />} />
          </Routes>
        </div>
      </Router>
    </TariffProvider>
  );
}

export default App;