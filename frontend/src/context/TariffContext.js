import React, { createContext, useContext, useState, useEffect } from 'react';
import { fetchDashboardData, refreshData } from '../services/api';

const TariffContext = createContext();

export const TariffProvider = ({ children }) => {
  const [dashboardData, setDashboardData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);

  const loadDashboardData = async (forceRefresh = false) => {
    try {
      setLoading(true);
      if (forceRefresh) {
        await refreshData();
      }
      const data = await fetchDashboardData();
      setDashboardData(data);
      setLastUpdated(new Date());
      setError(null);
    } catch (err) {
      setError('Failed to load dashboard data. Please try again later.');
      console.error('Error loading dashboard data:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadDashboardData();
  }, []);

  const value = {
    dashboardData,
    loading,
    error,
    lastUpdated,
    refreshData: () => loadDashboardData(true),
  };

  return <TariffContext.Provider value={value}>{children}</TariffContext.Provider>;
};

export const useTariffData = () => {
  const context = useContext(TariffContext);
  if (!context) {
    throw new Error('useTariffData must be used within a TariffProvider');
  }
  return context;
};