import React, { useState, useEffect } from 'react';
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from 'recharts';
import { useTariffData } from '../context/TariffContext';
import { formatCurrency } from '../utils/format';

// Define colors for sectors
const COLORS = [
  '#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#A28BFF', 
  '#FF6B6B', '#54C1E3', '#67C36E', '#FFD166', '#F45D9E'
];

const RADIAN = Math.PI / 180;
const renderCustomizedLabel = ({ cx, cy, midAngle, innerRadius, outerRadius, percent, index }) => {
  const radius = innerRadius + (outerRadius - innerRadius) * 0.5;
  const x = cx + radius * Math.cos(-midAngle * RADIAN);
  const y = cy + radius * Math.sin(-midAngle * RADIAN);

  return (
    <text 
      x={x} 
      y={y} 
      fill="white" 
      textAnchor={x > cx ? 'start' : 'end'} 
      dominantBaseline="central"
    >
      {`${(percent * 100).toFixed(0)}%`}
    </text>
  );
};

const SectorPieChart = () => {
  const { dashboardData, loading } = useTariffData();
  const [sectorData, setSectorData] = useState([]);

  useEffect(() => {
    if (dashboardData && dashboardData.sector_impact_data) {
      setSectorData(dashboardData.sector_impact_data);
    }
  }, [dashboardData]);

  if (loading) {
    return <div className="loading">Loading sector data...</div>;
  }

  const CustomTooltip = ({ active, payload }) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-white p-3 border border-gray-200 shadow-md rounded">
          <p className="font-bold">{data.sector}</p>
          <p>Export Value: {formatCurrency(data.export_value)}</p>
          <p>GDP Contribution: {formatCurrency(data.gdp_contribution)}</p>
          <p>Tariff Impact: {data.tariff_impact.toFixed(2)}%</p>
          {data.jobs_impact > 0 && (
            <p>Jobs Impact: {data.jobs_impact.toLocaleString()} jobs</p>
          )}
        </div>
      );
    }
    return null;
  };

  return (
    <div className="sector-chart-container h-[50vh] w-full bg-white rounded-lg shadow-md p-4">
      <h2 className="text-xl font-bold mb-4">Sector Impact Distribution</h2>
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie
            data={sectorData}
            cx="50%"
            cy="50%"
            labelLine={false}
            label={renderCustomizedLabel}
            outerRadius="70%"
            fill="#8884d8"
            dataKey="export_value"
            nameKey="sector"
          >
            {sectorData.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
            ))}
          </Pie>
          <Tooltip content={<CustomTooltip />} />
          <Legend />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
};

export default SectorPieChart;