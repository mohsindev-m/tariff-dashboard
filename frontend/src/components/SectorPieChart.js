import React from 'react';
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from 'recharts';
import { useTariffData } from '../context/TariffContext';
import { formatPercent, formatCurrency } from '../utils/format';

// Modern color palette
const COLORS = [
  '#6366f1', '#8b5cf6', '#d946ef', '#ec4899', '#f43f5e',
  '#f97316', '#eab308', '#84cc16', '#22c55e', '#10b981',
  '#14b8a6', '#06b6d4', '#0ea5e9', '#3b82f6'
];

const SectorPieChart = () => {
  const { sectorData, loading, error, selectedIndustry, setSelectedIndustry } = useTariffData();
  
  if (loading) return <div className="h-full flex items-center justify-center text-gray-500">Loading sector data...</div>;
  if (error) return <div className="h-full flex items-center justify-center text-red-500">{error}</div>;
  
  // Ensure we have valid data
  if (!sectorData || !Array.isArray(sectorData) || sectorData.length === 0) {
    return <div className="h-full flex items-center justify-center text-gray-500">No sector data available</div>;
  }
  
  // Format data for the pie chart
  const chartData = sectorData.map(sector => ({
    name: sector.sector || sector.SECTOR,
    value: sector.percentage || sector.PERCENTAGE || 0,
    tradeVolume: sector.trade_volume || sector.ALL_VAL_MO || 0,
    tariff: sector.average_tariff || 0,
    jobs: sector.jobs_impact || 0
  }));
  
  // Custom tooltip component
  const CustomTooltip = ({ active, payload }) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-white p-3 border border-gray-100 rounded shadow-lg text-sm">
          <p className="font-semibold text-gray-800 mb-1">{data.name}</p>
          <p className="text-gray-700">Share: {formatPercent(data.value)}</p>
          <p className="text-gray-700">Trade Volume: {formatCurrency(data.tradeVolume, 0)}</p>
          {data.tariff > 0 && <p className="text-gray-700">Avg Tariff: {formatPercent(data.tariff)}</p>}
          {data.jobs !== 0 && <p className="text-gray-700">Jobs Impact: {data.jobs.toLocaleString()}</p>}
        </div>
      );
    }
    return null;
  };
  
  return (
    <div className="h-full w-full flex flex-col">
      <h2 className="text-lg font-medium text-gray-800 mb-2">Sector Impact Distribution</h2>
      <div className="flex-grow">
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie
              data={chartData}
              dataKey="value"
              nameKey="name"
              cx="50%"
              cy="50%"
              outerRadius={80}
              innerRadius={40}
              paddingAngle={2}
              onClick={(_, index) => {
                const sector = chartData[index].name;
                setSelectedIndustry(selectedIndustry === sector ? null : sector);
              }}
            >
              {chartData.map((entry, index) => (
                <Cell 
                  key={`cell-${index}`} 
                  fill={COLORS[index % COLORS.length]} 
                  stroke={selectedIndustry === entry.name ? "#000" : "#fff"}
                  strokeWidth={selectedIndustry === entry.name ? 2 : 1}
                />
              ))}
            </Pie>
            <Tooltip content={<CustomTooltip />} />
            <Legend 
              layout="horizontal"
              verticalAlign="bottom"
              align="center"
              wrapperStyle={{ fontSize: '12px' }}
            />
          </PieChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default SectorPieChart;