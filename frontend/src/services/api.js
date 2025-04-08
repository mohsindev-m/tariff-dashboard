import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8004';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json'
  }
});

export const fetchDashboardData = async () => {
  try {
    const response = await api.get('/api/dashboard');
    return response.data.data;
  } catch (error) {
    console.error('Error fetching dashboard data:', error);
    throw error;
  }
};

export const fetchHeatmapData = async () => {
  try {
    const response = await api.get('/api/heatmap');
    return response.data.data;
  } catch (error) {
    console.error('Error fetching heatmap data:', error);
    throw error;
  }
};

export const fetchSectorData = async () => {
  try {
    const response = await api.get('/api/sectors');
    return response.data.data;
  } catch (error) {
    console.error('Error fetching sector data:', error);
    throw error;
  }
};

export const fetchTimeSeriesData = async () => {
  try {
    const response = await api.get('/api/timeseries');
    return response.data.data;
  } catch (error) {
    console.error('Error fetching time series data:', error);
    throw error;
  }
};

export const fetchTableData = async () => {
  try {
    const response = await api.get('/api/table');
    return response.data.data;
  } catch (error) {
    console.error('Error fetching table data:', error);
    throw error;
  }
};

export const fetchCountries = async () => {
  try {
    const response = await api.get('/api/countries');
    return response.data.data;
  } catch (error) {
    console.error('Error fetching countries:', error);
    throw error;
  }
};

export const fetchIndustries = async () => {
  try {
    const response = await api.get('/api/industries');
    return response.data.data;
  } catch (error) {
    console.error('Error fetching industries:', error);
    throw error;
  }
};

export const fetchMeasures = async () => {
  try {
    const response = await api.get('/api/measures');
    return response.data.data;
  } catch (error) {
    console.error('Error fetching measures:', error);
    throw error;
  }
};

export const triggerUpdate = async () => {
  try {
    const response = await api.post('/api/update');
    return response.data;
  } catch (error) {
    console.error('Error triggering update:', error);
    throw error;
  }
};

export default api;