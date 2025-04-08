import React, { createContext, useState, useEffect, useContext } from 'react';
import { 
  fetchDashboardData, 
  fetchHeatmapData, 
  fetchSectorData, 
  fetchTimeSeriesData, 
  fetchTableData,
  triggerUpdate
} from '../services/api';

const TariffContext = createContext();

export const TariffProvider = ({ children }) => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [dashboardData, setDashboardData] = useState(null);
  const [heatmapData, setHeatmapData] = useState([]);
  const [sectorData, setSectorData] = useState([]);
  const [timeSeriesData, setTimeSeriesData] = useState([]);
  const [tableData, setTableData] = useState({ countries: [], industries: [] });
  const [updateStatus, setUpdateStatus] = useState({ isUpdating: false, lastUpdated: null });
  const [selectedCountry, setSelectedCountry] = useState(null);
  const [selectedIndustry, setSelectedIndustry] = useState(null);

  const fetchAllData = async () => {
    setLoading(true);
    setError(null);
    try {
      // Fetch all data in parallel
      const dashboardResponse = await fetchDashboardData();
      setDashboardData(dashboardResponse);

      if (dashboardResponse) {
        if (dashboardResponse.heatmap_data) setHeatmapData(dashboardResponse.heatmap_data);
        if (dashboardResponse.sector_data) setSectorData(dashboardResponse.sector_data);
        if (dashboardResponse.time_series) setTimeSeriesData(dashboardResponse.time_series);
        if (dashboardResponse.detail_table) setTableData(dashboardResponse.detail_table);
      } else {
        // Fetch individual endpoints if dashboard doesn't return all data
        const [heatmap, sectors, timeSeries, table] = await Promise.all([
          fetchHeatmapData(),
          fetchSectorData(),
          fetchTimeSeriesData(),
          fetchTableData()
        ]);
        
        setHeatmapData(heatmap || []);
        setSectorData(sectors || []);
        setTimeSeriesData(timeSeries || []);
        setTableData(table || { countries: [], industries: [] });
      }
    } catch (err) {
      setError('Failed to fetch dashboard data. Please try again later.');
      console.error('Error fetching data:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchAllData();
    // Set up an interval to refresh data every 5 minutes
    const intervalId = setInterval(fetchAllData, 5 * 60 * 1000);
    return () => clearInterval(intervalId);
  }, []);

  const handleUpdate = async () => {
    setUpdateStatus({ ...updateStatus, isUpdating: true });
    try {
      const result = await triggerUpdate();
      setUpdateStatus({ 
        isUpdating: false, 
        lastUpdated: new Date().toISOString(),
        message: result.message
      });
      
      // Fetch updated data after a short delay to allow processing
      setTimeout(fetchAllData, 5000);
    } catch (err) {
      setUpdateStatus({ 
        isUpdating: false, 
        lastUpdated: new Date().toISOString(),
        error: 'Update failed. Please try again.'
      });
    }
  };

  const value = {
    loading,
    error,
    dashboardData,
    heatmapData,
    sectorData,
    timeSeriesData,
    tableData,
    updateStatus,
    selectedCountry,
    setSelectedCountry,
    selectedIndustry,
    setSelectedIndustry,
    refreshData: fetchAllData,
    triggerUpdate: handleUpdate
  };

  return (
    <TariffContext.Provider value={value}>
      {children}
    </TariffContext.Provider>
  );
};

export const useTariffData = () => {
  const context = useContext(TariffContext);
  if (context === undefined) {
    throw new Error('useTariffData must be used within a TariffProvider');
  }
  return context;
};

export default TariffContext;