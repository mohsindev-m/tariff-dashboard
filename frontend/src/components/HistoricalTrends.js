import React, { useState } from 'react';
import { 
  LineChart, Line, XAxis, YAxis, CartesianGrid, 
  Tooltip, Legend, ResponsiveContainer, ReferenceLine 
} from 'recharts';
import { useTariffData } from '../context/TariffContext';
import { formatCurrency } from '../utils/format';

const HistoricalTrends = () => {
  const { timeSeriesData, loading, error } = useTariffData();
  const [metrics, setMetrics] = useState(['trade_deficit']);
  
  if (loading) return <div className="h-full flex items-center justify-center text-gray-500">Loading trend data...</div>;
  if (error) return <div className="h-full flex items-center justify-center text-red-500">{error}</div>;
  
  // Ensure we have valid data
  if (!timeSeriesData || !Array.isArray(timeSeriesData) || timeSeriesData.length === 0) {
    return <div className="h-full flex items-center justify-center text-gray-500">No historical data available</div>;
  }
  
  // Determine available metrics from the first data point
  const availableMetrics = Object.keys(timeSeriesData[0]).filter(key => key !== 'year');
  
  // Custom tooltip
  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white p-3 border border-gray-100 rounded shadow-lg text-sm">
          <p className="font-semibold text-gray-800 mb-1">{label}</p>
          {payload.map((entry, index) => (
            <p key={index} style={{ color: entry.color }} className="flex items-center">
              <span className="w-3 h-3 mr-2 inline-block" style={{ backgroundColor: entry.color }}></span>
              {entry.name}: {formatCurrency(entry.value, 1)}B
            </p>
          ))}
        </div>
      );
    }
    return null;
  };
  
  // Toggle metric selection
  const toggleMetric = (metric) => {
    if (metrics.includes(metric)) {
      setMetrics(metrics.filter(m => m !== metric));
    } else {
      setMetrics([...metrics, metric]);
    }
  };
  
  // Define colors for each metric - using more modern colors
  const metricColors = {
    trade_deficit: '#f43f5e', // rose
    exports: '#10b981',       // emerald
    imports: '#3b82f6',       // blue
    balance: '#8b5cf6',       // violet
  };
  
  // Format the metric label for display
  const formatMetricLabel = (metric) => {
    return metric
      .split('_')
      .map(word => word.charAt(0).toUpperCase() + word.slice(1))
      .join(' ');
  };
  
  return (
    <div className="h-full w-full flex flex-col">
      <div className="flex justify-between items-center mb-3">
        <h2 className="text-lg font-medium text-gray-800">Historical Trade Trends</h2>
        <div className="flex flex-wrap gap-2">
          {availableMetrics.map(metric => (
            <button
              key={metric}
              className={`px-2 py-1 text-xs rounded-full transition-colors ${
                metrics.includes(metric) 
                  ? `bg-${metric === 'trade_deficit' ? 'rose-500' : 
                       metric === 'exports' ? 'emerald-500' : 
                       metric === 'imports' ? 'blue-500' : 'violet-500'} text-white` 
                  : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
              }`}
              style={{
                backgroundColor: metrics.includes(metric) ? metricColors[metric] : undefined,
                color: metrics.includes(metric) ? 'white' : undefined
              }}
              onClick={() => toggleMetric(metric)}
            >
              {formatMetricLabel(metric)}
            </button>
          ))}
        </div>
      </div>
      
      <div className="flex-grow">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart
            data={timeSeriesData}
            margin={{ top: 5, right: 10, left: 10, bottom: 5 }}
          >
            <CartesianGrid strokeDasharray="3 3" opacity={0.2} />
            <XAxis 
              dataKey="year" 
              axisLine={{ stroke: '#e5e7eb' }} 
              tickLine={false}
              dy={10}
            />
            <YAxis
              tickFormatter={(value) => `${(value).toFixed(0)}B`}
              domain={['auto', 'auto']}
              axisLine={{ stroke: '#e5e7eb' }}
              tickLine={false}
              dx={-10}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend 
              iconType="circle" 
              iconSize={8}
              wrapperStyle={{ fontSize: '12px', paddingTop: '10px' }}
            />
            <ReferenceLine y={0} stroke="#9ca3af" strokeDasharray="3 3" />
            
            {metrics.includes('trade_deficit') && (
              <Line
                type="monotone"
                dataKey="trade_deficit"
                name="Trade Deficit"
                stroke={metricColors.trade_deficit}
                strokeWidth={2}
                dot={{ r: 3, strokeWidth: 1 }}
                activeDot={{ r: 5, strokeWidth: 1 }}
              />
            )}
            
            {metrics.includes('exports') && (
              <Line
                type="monotone"
                dataKey="exports"
                name="Exports"
                stroke={metricColors.exports}
                strokeWidth={2}
                dot={{ r: 3, strokeWidth: 1 }}
                activeDot={{ r: 5, strokeWidth: 1 }}
              />
            )}
            
            {metrics.includes('imports') && (
              <Line
                type="monotone"
                dataKey="imports"
                name="Imports"
                stroke={metricColors.imports}
                strokeWidth={2}
                dot={{ r: 3, strokeWidth: 1 }}
                activeDot={{ r: 5, strokeWidth: 1 }}
              />
            )}
            
            {metrics.includes('balance') && (
              <Line
                type="monotone"
                dataKey="balance"
                name="Trade Balance"
                stroke={metricColors.balance}
                strokeWidth={2}
                dot={{ r: 3, strokeWidth: 1 }}
                activeDot={{ r: 5, strokeWidth: 1 }}
              />
            )}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default HistoricalTrends;