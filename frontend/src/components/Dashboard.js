import React, { useState } from 'react';
import GlobalHeatmap from './GlobalHeatmap';
import SectorPieChart from './SectorPieChart';
import HistoricalTrends from './HistoricalTrends';
import DetailedMetricsTable from './DetailedMetricsTable';
import { useTariffData } from '../context/TariffContext';
import { formatDate } from '../utils/format';

const Dashboard = () => {
  const { loading, error, updateStatus, dashboardData, triggerUpdate } = useTariffData();
  const [isUpdating, setIsUpdating] = useState(false);
  
  // Handle manual update
  const handleUpdate = async () => {
    setIsUpdating(true);
    try {
      await triggerUpdate();
    } finally {
      setIsUpdating(false);
    }
  };
  
  // Get metadata for footer
  const metadata = dashboardData?.metadata || {};
  
  return (
    <div className="flex flex-col h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow-sm">
        <div className="max-w-7xl mx-auto px-4 py-4 sm:px-6">
          <div className="flex justify-between items-center">
            <h1 className="text-2xl font-bold text-gray-900">Global Tariff Impact Dashboard</h1>
            <div className="flex items-center space-x-4">
              <div className="text-sm text-gray-500">
                {updateStatus.lastUpdated ? (
                  <span>Last updated: {formatDate(updateStatus.lastUpdated)}</span>
                ) : (
                  <span>Data as of {formatDate(metadata.generated_at || new Date())}</span>
                )}
              </div>
              <button
                className={`px-4 py-2 rounded-md text-white font-medium ${
                  isUpdating ? 'bg-gray-400' : 'bg-blue-600 hover:bg-blue-700'
                }`}
                onClick={handleUpdate}
                disabled={isUpdating}
              >
                {isUpdating ? 'Updating...' : 'Update Data'}
              </button>
            </div>
          </div>
        </div>
      </header>
      
      {/* Main content */}
      <main className="flex-grow overflow-hidden px-4 py-4 sm:px-6">
        {loading ? (
          <div className="h-full flex items-center justify-center">
            <div className="text-lg font-semibold">Loading dashboard data...</div>
          </div>
        ) : error ? (
          <div className="h-full flex items-center justify-center text-red-500">
            <div className="text-lg font-semibold">{error}</div>
          </div>
        ) : (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 h-full">
            {/* Top row */}
            <div className="bg-white shadow rounded-lg p-4 h-96">
              <GlobalHeatmap />
            </div>
            <div className="bg-white shadow rounded-lg p-4 h-96">
              <SectorPieChart />
            </div>
            
            {/* Bottom row */}
            <div className="bg-white shadow rounded-lg p-4 h-96">
              <HistoricalTrends />
            </div>
            <div className="bg-white shadow rounded-lg p-4 h-96">
              <DetailedMetricsTable />
            </div>
          </div>
        )}
      </main>
      
      {/* Footer */}
      <footer className="bg-white shadow-inner">
        <div className="max-w-7xl mx-auto px-4 py-3 sm:px-6">
          <div className="flex justify-between items-center text-xs text-gray-500">
            <div>
              <span>Data sources: U.S. Census Bureau, BEA, WTO, White House Press Releases</span>
            </div>
            <div>
              <span>© 2025 Tariff Dashboard Project</span>
            </div>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default Dashboard;