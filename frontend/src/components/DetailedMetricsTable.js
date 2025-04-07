import React, { useState, useEffect } from 'react';
import { useTariffData } from '../context/TariffContext';
import { formatCurrency } from '../utils/format';

const DetailedMetricsTable = () => {
  const { dashboardData, loading } = useTariffData();
  const [metricsData, setMetricsData] = useState([]);
  const [filteredData, setFilteredData] = useState([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortConfig, setSortConfig] = useState({ key: 'hs_code', direction: 'ascending' });
  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 10;

  useEffect(() => {
    if (dashboardData && dashboardData.detailed_metrics) {
      setMetricsData(dashboardData.detailed_metrics);
      setFilteredData(dashboardData.detailed_metrics);
    }
  }, [dashboardData]);

  useEffect(() => {
    // Filter data based on search term
    const filtered = metricsData.filter(
      item =>
        item.hs_code.toLowerCase().includes(searchTerm.toLowerCase()) ||
        item.description.toLowerCase().includes(searchTerm.toLowerCase())
    );

    // Sort data based on current sort configuration
    const sortedData = [...filtered].sort((a, b) => {
      if (a[sortConfig.key] < b[sortConfig.key]) {
        return sortConfig.direction === 'ascending' ? -1 : 1;
      }
      if (a[sortConfig.key] > b[sortConfig.key]) {
        return sortConfig.direction === 'ascending' ? 1 : -1;
      }
      return 0;
    });

    setFilteredData(sortedData);
    setCurrentPage(1); // Reset to first page on filter change
  }, [searchTerm, sortConfig, metricsData]);

  const requestSort = (key) => {
    let direction = 'ascending';
    if (sortConfig.key === key && sortConfig.direction === 'ascending') {
      direction = 'descending';
    }
    setSortConfig({ key, direction });
  };

  const getSortIndicator = (key) => {
    if (sortConfig.key !== key) return null;
    return sortConfig.direction === 'ascending' ? '↑' : '↓';
  };

  if (loading) {
    return <div className="loading">Loading detailed metrics...</div>;
  }

  // Pagination
  const totalPages = Math.ceil(filteredData.length / itemsPerPage);
  const indexOfLastItem = currentPage * itemsPerPage;
  const indexOfFirstItem = indexOfLastItem - itemsPerPage;
  const currentItems = filteredData.slice(indexOfFirstItem, indexOfLastItem);

  return (
    <div className="metrics-table-container w-full bg-white rounded-lg shadow-md p-4">
      <h2 className="text-xl font-bold mb-4">Detailed Trade Metrics</h2>
      
      {/* Search and filter controls */}
      <div className="flex mb-4">
        <input
          type="text"
          placeholder="Search by HS code or description..."
          className="px-4 py-2 border rounded-md w-full"
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
        />
      </div>
      
      {/* Table */}
      <div className="overflow-x-auto">
        <table className="min-w-full bg-white">
          <thead className="bg-gray-100">
            <tr>
              <th 
                className="px-4 py-2 cursor-pointer"
                onClick={() => requestSort('hs_code')}
              >
                HS Code {getSortIndicator('hs_code')}
              </th>
              <th 
                className="px-4 py-2 cursor-pointer"
                onClick={() => requestSort('description')}
              >
                Description {getSortIndicator('description')}
              </th>
              <th 
                className="px-4 py-2 cursor-pointer"
                onClick={() => requestSort('export_value')}
              >
                Export Value {getSortIndicator('export_value')}
              </th>
              <th 
                className="px-4 py-2 cursor-pointer"
                onClick={() => requestSort('tariff_rate')}
              >
                Tariff Rate (%) {getSortIndicator('tariff_rate')}
              </th>
              <th 
                className="px-4 py-2 cursor-pointer"
                onClick={() => requestSort('supply_chain_risk')}
              >
                Supply Chain Risk {getSortIndicator('supply_chain_risk')}
              </th>
            </tr>
          </thead>
          <tbody>
            {currentItems.map((item, index) => (
              <tr key={item.hs_code} className={index % 2 === 0 ? 'bg-gray-50' : 'bg-white'}>
                <td className="border px-4 py-2">{item.hs_code}</td>
                <td className="border px-4 py-2">{item.description}</td>
                <td className="border px-4 py-2">{formatCurrency(item.export_value)}</td>
                <td className="border px-4 py-2">{item.tariff_rate.toFixed(2)}%</td>
                <td className="border px-4 py-2">
                  <div className="flex items-center">
                    <div className="w-full bg-gray-200 rounded-full h-2.5">
                      <div 
                        className="bg-blue-600 h-2.5 rounded-full" 
                        style={{ width: `${item.supply_chain_risk * 10}%` }}
                      ></div>
                    </div>
                    <span className="ml-2">{item.supply_chain_risk.toFixed(1)}</span>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      
      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex justify-between items-center mt-4">
          <button
            className="px-4 py-2 bg-gray-200 rounded disabled:opacity-50"
            onClick={() => setCurrentPage(Math.max(1, currentPage - 1))}
            disabled={currentPage === 1}
          >
            Previous
          </button>
          <span>
            Page {currentPage} of {totalPages}
          </span>
          <button
            className="px-4 py-2 bg-gray-200 rounded disabled:opacity-50"
            onClick={() => setCurrentPage(Math.min(totalPages, currentPage + 1))}
            disabled={currentPage === totalPages}
          >
            Next
          </button>
        </div>
      )}
    </div>
  );
};

export default DetailedMetricsTable;