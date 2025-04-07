import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, ReferenceLine } from 'recharts';
import { useTariffData } from '../context/TariffContext';
import { formatCurrency } from '../utils/format';

const HistoricalTrends = () => {
  const { dashboardData, loading } = useTariffData();
  const [trendsData, setTrendsData] = useState([]);
  const [activeMetric, setActiveMetric] = useState('trade_deficit');

  useEffect(() => {
    if (dashboardData && dashboardData.historical_trends) {
      // Sort by year to ensure chronological order
      const sortedData = [...dashboardData.historical_trends].sort((a, b) => 
        parseInt(a.year) - parseInt(b.year)
      );
      setTrendsData(sortedData);
    }
  }, [dashboardData]);

  if (loading) {
    return <div className="loading">Loading historical trends...</div>;
  }

  const metrics = [
    { id: 'trade_deficit', label: 'Trade Deficit', color: '#FF4136' },
    { id: 'exports', label: 'Exports', color: '#2ECC40' },
    { id: 'imports', label: 'Imports', color: '#0074D9' },
    { id: 'bea_balance', label: 'BEA Balance', color: '#B10DC9' }
  ];

  // Convert values to billions for better readability
  const dataInBillions = trendsData.map(item => ({
    ...item,
    exports_billions: item.exports / 1000000000,
    imports_billions: item.imports / 1000000000,
    trade_deficit_billions: item.trade_deficit / 1000000000,
    bea_balance_billions: item.bea_balance / 1000000000
  }));

  // Determine yAxis domain based on data and selected metric
  const getYAxisDomain = () => {
    const values = dataInBillions.map(item => item[`${activeMetric}_billions`]);
    const minValue = Math.min(...values);
    const maxValue = Math.max(...values);
    
    // Add some padding to the domain
    const padding = (maxValue - minValue) * 0.1;
    return [minValue - padding, maxValue + padding];
  };

  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white p-3 border border-gray-200 shadow-md rounded">
          <p className="font-bold">{label}</p>
          {payload.map((entry, index) => (
            <p key={index} style={{ color: entry.color }}>
              {entry.name}: {formatCurrency(entry.value * 1000000000)}
            </p>
          ))}
        </div>
      );
    }
    return null;
  };

  return (
    <div className="trends-container h-[60vh] w-full bg-white rounded-lg shadow-md p-4">
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-xl font-bold">Historical Trade Trends</h2>
        <div className="flex space-x-2">
          {metrics.map(metric => (
            <button
              key={metric.id}
              className={`px-3 py-1 text-sm rounded ${
                activeMetric === metric.id
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-200 text-gray-800'
              }`}
              onClick={() => setActiveMetric(metric.id)}
            >
              {metric.label}
            </button>
          ))}
        </div>
      </div>
      <ResponsiveContainer width="100%" height="90%">
        <LineChart
          data={dataInBillions}
          margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="year" />
          <YAxis domain={getYAxisDomain()} />
          <Tooltip content={<CustomTooltip />} />
          <Legend />
          <ReferenceLine y={0} stroke="#000" />
          {metrics.map(metric => (
            <Line
              key={metric.id}
              type="monotone"
              dataKey={`${metric.id}_billions`}
              name={metric.label}
              stroke={metric.color}
              activeDot={{ r: 8 }}
              strokeWidth={activeMetric === metric.id ? 3 : 1}
              opacity={activeMetric === metric.id ? 1 : 0.3}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
};

export default HistoricalTrends;