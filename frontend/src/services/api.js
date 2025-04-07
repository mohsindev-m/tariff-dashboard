import axios from 'axios';

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000/api';

const api = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

export const fetchDashboardData = async () => {
  try {
    const response = await api.get('/dashboard');
    return response.data;
  } catch (error) {
    console.error('Error fetching dashboard data:', error);
    throw error;
  }
};

export const fetchMapData = async () => {
  try {
    const response = await api.get('/map');
    return response.data;
  } catch (error) {
    console.error('Error fetching map data:', error);
    throw error;
  }
};

export const fetchSectorData = async () => {
  try {
    const response = await api.get('/sectors');
    return response.data;
  } catch (error) {
    console.error('Error fetching sector data:', error);
    throw error;
  }
};

export const fetchTrendsData = async () => {
  try {
    const response = await api.get('/trends');
    return response.data;
  } catch (error) {
    console.error('Error fetching trends data:', error);
    throw error;
  }
};

export const fetchMetricsData = async () => {
  try {
    const response = await api.get('/metrics');
    return response.data;
  } catch (error) {
    console.error('Error fetching metrics data:', error);
    throw error;
  }
};

export const refreshData = async () => {
  try {
    const response = await api.post('/refresh');
    return response.data;
  } catch (error) {
    console.error('Error refreshing data:', error);
    throw error;
  }
};

export default api;