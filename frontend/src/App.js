import React from 'react';
import Dashboard from './components/Dashboard';
import { TariffProvider } from './context/TariffContext';
import './App.css';

function App() {
  return (
    <TariffProvider>
      <Dashboard />
    </TariffProvider>
  );
}

export default App;