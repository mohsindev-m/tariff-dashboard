/**
 * Format a number as a currency string
 * @param {number} value - The value to format
 * @param {string} currency - The currency code
 * @returns {string} Formatted currency string
 */
export const formatCurrency = (value, currency = 'USD') => {
    if (value === undefined || value === null) return 'N/A';
    
    const formatter = new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency,
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    });
    
    return formatter.format(value);
  };
  
  /**
   * Format a date into a readable string
   * @param {Date|string} date - The date to format
   * @returns {string} Formatted date string
   */
  export const formatDate = (date) => {
    if (!date) return '';
    
    const dateObj = typeof date === 'string' ? new Date(date) : date;
    
    return dateObj.toLocaleString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  };
  
  /**
   * Format a percentage value
   * @param {number} value - The percentage value
   * @param {number} decimals - Number of decimal places
   * @returns {string} Formatted percentage string
   */
  export const formatPercentage = (value, decimals = 2) => {
    if (value === undefined || value === null) return 'N/A';
    return `${value.toFixed(decimals)}%`;
  };