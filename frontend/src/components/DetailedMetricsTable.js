import React, { useState, useEffect } from 'react';
import { useTariffData } from '../context/TariffContext';
import { formatPercent, formatCurrency, formatNumber } from '../utils/format';

const DetailedMetricsTable = () => {
  const { tableData, loading, error, selectedCountry, setSelectedCountry, selectedIndustry, setSelectedIndustry } = useTariffData();
  const [activeTab, setActiveTab] = useState('countries');
  const [searchTerm, setSearchTerm] = useState('');
  const [sortConfig, setSortConfig] = useState({ key: 'effective_tariff', direction: 'descending' });
  const [tableRows, setTableRows] = useState([]);
  
  // Process table data whenever it changes
  useEffect(() => {
    if (!tableData) return;
    
    // Get the right data based on active tab
    let rawData = [];
    if (activeTab === 'countries' && tableData.countries) {
      rawData = Array.isArray(tableData.countries) ? tableData.countries : [];
    } else if (activeTab === 'industries' && tableData.industries) {
      rawData = Array.isArray(tableData.industries) ? tableData.industries : [];
    }
    
    // Filter by search term
    let filtered = rawData;
    if (searchTerm) {
      filtered = rawData.filter(item => {
        // Determine which fields to search based on the active tab
        const searchFields = activeTab === 'countries' 
          ? [item.country_name, item.country_code, item.region] 
          : [item.industry_name, item.sector, item.industry_code];
          
        return searchFields.some(field => 
          field && String(field).toLowerCase().includes(searchTerm.toLowerCase())
        );
      });
    }
    
    // Sort data
    if (sortConfig.key) {
      filtered.sort((a, b) => {
        // Handle undefined, null or NaN values
        const aValue = a[sortConfig.key] !== undefined ? a[sortConfig.key] : -Infinity;
        const bValue = b[sortConfig.key] !== undefined ? b[sortConfig.key] : -Infinity;
        
        // Handle numeric comparisons
        const aNum = parseFloat(aValue);
        const bNum = parseFloat(bValue);
        
        if (!isNaN(aNum) && !isNaN(bNum)) {
          return sortConfig.direction === 'ascending' 
            ? aNum - bNum 
            : bNum - aNum;
        }
        
        // Handle string comparisons
        if (aValue < bValue) {
          return sortConfig.direction === 'ascending' ? -1 : 1;
        }
        if (aValue > bValue) {
          return sortConfig.direction === 'ascending' ? 1 : -1;
        }
        return 0;
      });
    }
    
    setTableRows(filtered);
  }, [tableData, activeTab, searchTerm, sortConfig]);
  
  // Handle sort request
  const requestSort = (key) => {
    let direction = 'ascending';
    if (sortConfig.key === key && sortConfig.direction === 'ascending') {
      direction = 'descending';
    }
    setSortConfig({ key, direction });
  };
  
  if (loading) return <div className="h-full flex items-center justify-center">Loading detailed metrics...</div>;
  if (error) return <div className="h-full flex items-center justify-center text-red-500">{error}</div>;
  
  // Check for empty data
  if (!tableData || (!tableData.countries?.length && !tableData.industries?.length)) {
    return <div className="h-full flex items-center justify-center">No detailed metrics available</div>;
  }
  
  const renderCountryTable = () => {
    if (!tableRows.length) {
      return (
        <div className="py-10 text-center text-gray-500">
          No countries match your search criteria
        </div>
      );
    }
    
    return (
      <table className="min-w-full divide-y divide-gray-200">
        <thead className="bg-gray-50">
          <tr>
            <th 
              className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer"
              onClick={() => requestSort('country_name')}
            >
              Country
              {sortConfig.key === 'country_name' && (
                <span>{sortConfig.direction === 'ascending' ? ' ↑' : ' ↓'}</span>
              )}
            </th>
            <th 
              className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer"
              onClick={() => requestSort('effective_tariff')}
            >
              Effective Tariff
              {sortConfig.key === 'effective_tariff' && (
                <span>{sortConfig.direction === 'ascending' ? ' ↑' : ' ↓'}</span>
              )}
            </th>
            <th 
              className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer"
              onClick={() => requestSort('tariff_impact')}
            >
              GVA Impact
              {sortConfig.key === 'tariff_impact' && (
                <span>{sortConfig.direction === 'ascending' ? ' ↑' : ' ↓'}</span>
              )}
            </th>
            <th 
              className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer"
              onClick={() => requestSort('jobs_impact')}
            >
              Jobs Impact
              {sortConfig.key === 'jobs_impact' && (
                <span>{sortConfig.direction === 'ascending' ? ' ↑' : ' ↓'}</span>
              )}
            </th>
            <th 
              className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer"
              onClick={() => requestSort('supply_chain_risk')}
            >
              Supply Chain Risk
              {sortConfig.key === 'supply_chain_risk' && (
                <span>{sortConfig.direction === 'ascending' ? ' ↑' : ' ↓'}</span>
              )}
            </th>
          </tr>
        </thead>
        <tbody className="bg-white divide-y divide-gray-200">
          {tableRows.map((country, index) => (
            <tr 
              key={country.country_code || `country-${index}`}
              className={`${selectedCountry === country.country_code ? 'bg-blue-50' : ''} hover:bg-gray-50 cursor-pointer`}
              onClick={() => setSelectedCountry(
                selectedCountry === country.country_code ? null : country.country_code
              )}
            >
              <td className="px-6 py-4 whitespace-nowrap">
                <div className="text-sm font-medium text-gray-900">
                  {country.country_name || 'Unknown Country'}
                </div>
              </td>
              <td className="px-6 py-4 whitespace-nowrap">
                <div className="text-sm text-gray-900">
                  {formatPercent(country.effective_tariff)}
                </div>
              </td>
              <td className="px-6 py-4 whitespace-nowrap">
                <div className="text-sm text-gray-900">
                  {formatPercent(country.tariff_impact)}
                </div>
              </td>
              <td className="px-6 py-4 whitespace-nowrap">
                <div className="text-sm text-gray-900">
                  {formatNumber(country.jobs_impact, 0)}
                </div>
              </td>
              <td className="px-6 py-4 whitespace-nowrap">
                <div className="text-sm text-gray-900">
                  {formatPercent(country.supply_chain_risk)}
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    );
  };

  const renderIndustryTable = () => {
    if (!tableRows.length) {
      return (
        <div className="py-10 text-center text-gray-500">
          No industries match your search criteria
        </div>
      );
    }
    
    return (
      <table className="min-w-full divide-y divide-gray-200">
        <thead className="bg-gray-50">
          <tr>
            <th 
              className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer"
              onClick={() => requestSort('industry_name')}
            >
              Industry
              {sortConfig.key === 'industry_name' && (
                <span>{sortConfig.direction === 'ascending' ? ' ↑' : ' ↓'}</span>
              )}
            </th>
            <th 
              className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer"
              onClick={() => requestSort('sector')}
            >
              Sector
              {sortConfig.key === 'sector' && (
                <span>{sortConfig.direction === 'ascending' ? ' ↑' : ' ↓'}</span>
              )}
            </th>
            <th 
              className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer"
              onClick={() => requestSort('initial_tariff')}
            >
              Initial Tariff
              {sortConfig.key === 'initial_tariff' && (
                <span>{sortConfig.direction === 'ascending' ? ' ↑' : ' ↓'}</span>
              )}
            </th>
            <th 
              className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer"
              onClick={() => requestSort('effective_tariff')}
            >
              Effective Tariff
              {sortConfig.key === 'effective_tariff' && (
                <span>{sortConfig.direction === 'ascending' ? ' ↑' : ' ↓'}</span>
              )}
            </th>
            <th 
              className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer"
              onClick={() => requestSort('gva_impact')}
            >
              GVA Impact
              {sortConfig.key === 'gva_impact' && (
                <span>{sortConfig.direction === 'ascending' ? ' ↑' : ' ↓'}</span>
              )}
            </th>
            <th 
              className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer"
              onClick={() => requestSort('jobs_impact')}
            >
              Jobs Impact
              {sortConfig.key === 'jobs_impact' && (
                <span>{sortConfig.direction === 'ascending' ? ' ↑' : ' ↓'}</span>
              )}
            </th>
          </tr>
        </thead>
        <tbody className="bg-white divide-y divide-gray-200">
          {tableRows.map((industry, index) => {
            // Find display name for selection - either industry_name or name or sector
            const displayName = industry.industry_name || industry.name || industry.sector || '';
            
            return (
              <tr 
                key={industry.industry_code || `industry-${index}`}
                className={`${selectedIndustry === displayName ? 'bg-blue-50' : ''} hover:bg-gray-50 cursor-pointer`}
                onClick={() => setSelectedIndustry(
                  selectedIndustry === displayName ? null : displayName
                )}
              >
                <td className="px-6 py-4 whitespace-nowrap">
                  <div className="text-sm font-medium text-gray-900">
                    {industry.industry_name || 'Unknown Industry'}
                  </div>
                </td>
                <td className="px-6 py-4 whitespace-nowrap">
                  <div className="text-sm text-gray-900">
                    {industry.sector || 'N/A'}
                  </div>
                </td>
                <td className="px-6 py-4 whitespace-nowrap">
                  <div className="text-sm text-gray-900">
                    {formatPercent(industry.initial_tariff)}
                  </div>
                </td>
                <td className="px-6 py-4 whitespace-nowrap">
                  <div className="text-sm text-gray-900">
                    {formatPercent(industry.effective_tariff)}
                  </div>
                </td>
                <td className="px-6 py-4 whitespace-nowrap">
                  <div className="text-sm text-gray-900">
                    {formatPercent(industry.gva_impact)}
                  </div>
                </td>
                <td className="px-6 py-4 whitespace-nowrap">
                  <div className="text-sm text-gray-900">
                    {formatNumber(industry.jobs_impact, 0)}
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    );
  };
  
  return (
    <div className="h-full w-full flex flex-col">
      <div className="flex justify-between items-center mb-4">
        <div className="flex space-x-4">
          <button
            className={`px-4 py-2 text-sm font-medium rounded-t-lg ${
              activeTab === 'countries' 
                ? 'bg-white border-t border-l border-r border-gray-200 text-blue-600' 
                : 'bg-gray-100 text-gray-700'
            }`}
            onClick={() => setActiveTab('countries')}
          >
            Countries
          </button>
          <button
            className={`px-4 py-2 text-sm font-medium rounded-t-lg ${
              activeTab === 'industries' 
                ? 'bg-white border-t border-l border-r border-gray-200 text-blue-600' 
                : 'bg-gray-100 text-gray-700'
            }`}
            onClick={() => setActiveTab('industries')}
          >
            Industries
          </button>
        </div>
        <div>
          <input
            type="text"
            placeholder="Search..."
            className="px-3 py-2 border border-gray-300 rounded-md text-sm"
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
        </div>
      </div>
      
      <div className="flex-grow overflow-auto border border-gray-200 rounded-md">
        <div className="min-w-full">
          {activeTab === 'countries' ? renderCountryTable() : renderIndustryTable()}
        </div>
      </div>
    </div>
  );
};

export default DetailedMetricsTable;