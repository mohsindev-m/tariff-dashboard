import React, { useState } from 'react';
import GlobalHeatmap from './GlobalHeatmap';
import SectorPieChart from './SectorPieChart';
import HistoricalTrends from './HistoricalTrends';
import DetailedMetricsTable from './DetailedMetricsTable';
import { useTariffData } from '../context/TariffContext';
import { formatDate } from '../utils/format';

const Dashboard = () => {
  const { dashboardData, loading, error, lastUpdated, refreshData } = useTariffData();
  const [isRefreshing, setIsRefreshing] = useState(false);

  const handleRefresh = async () => {
    setIsRefreshing(true);
    try {
      await refreshData();
    } finally {
      setIsRefreshing(false);
    }
  };

  if (error) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="text-center p-8 bg-red-100 rounded-lg max-w-md">
          <h2 className="text-2xl font-bold text-red-700 mb-4">Error Loading Dashboard</h2>
          <p className="mb-4">{error}</p>
          <button
            className="px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700"
            onClick={handleRefresh}
          >
            Try Again
          </button>
        </div>
      </div>
    );
  }

  if (loading && !dashboardData) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="text-center">
          <div className="animate-spin rounded-full h-16 w-16 border-t-2 border-b-2 border-blue-500 mx-auto"></div>
          <p className="mt-4 text-xl">Loading dashboard data...</p>
        </div>
      </div>
    );
  }

  return (
    // Continuing frontend/src/components/Dashboard.js
    <div className="container mx-auto px-4 py-8">
      <div className="flex justify-between items-center mb-8">
        <h1 className="text-3xl font-bold text-gray-800">Tariff Impact Dashboard</h1>
        <div className="flex items-center space-x-4">
          {lastUpdated && (
            <span className="text-sm text-gray-600">
              Last updated: {formatDate(lastUpdated)}
            </span>
          )}
          <button
            className={`px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 flex items-center ${
              isRefreshing ? 'opacity-75 cursor-not-allowed' : ''
            }`}
            onClick={handleRefresh}
            disabled={isRefreshing}
          >
            {isRefreshing ? (
              <>
                <svg className="animate-spin -ml-1 mr-2 h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
                Refreshing...
              </>
            ) : (
              'Refresh Data'
            )}
          </button>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <div className="bg-white rounded-lg shadow p-4">
          <h2 className="text-xl font-bold mb-4">Global Tariff Impact Map</h2>
          <GlobalHeatmap />
        </div>
        <div className="bg-white rounded-lg shadow p-4">
          <h2 className="text-xl font-bold mb-4">Sector Impact Distribution</h2>
          <SectorPieChart />
        </div>
      </div>

      <div className="bg-white rounded-lg shadow p-4 mb-6">
        <h2 className="text-xl font-bold mb-4">Historical Trade Trends</h2>
        <HistoricalTrends />
      </div>

      <div className="bg-white rounded-lg shadow p-4">
        <DetailedMetricsTable />
      </div>
    </div>
  );
};

export default Dashboard;