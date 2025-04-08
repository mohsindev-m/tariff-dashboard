export const formatCurrency = (value, precision = 2) => {
  if (value === null || value === undefined) return 'N/A';
  
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: precision,
    maximumFractionDigits: precision
  }).format(value);
};

export const formatNumber = (value, precision = 2) => {
  if (value === null || value === undefined) return 'N/A';
  
  return new Intl.NumberFormat('en-US', {
    minimumFractionDigits: precision,
    maximumFractionDigits: precision
  }).format(value);
};

export const formatPercent = (value, precision = 1) => {
  if (value === null || value === undefined) return 'N/A';
  
  return new Intl.NumberFormat('en-US', {
    style: 'percent',
    minimumFractionDigits: precision,
    maximumFractionDigits: precision
  }).format(value / 100);
};

export const formatDate = (dateString) => {
  if (!dateString) return 'N/A';
  
  const date = new Date(dateString);
  return new Intl.DateTimeFormat('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric'
  }).format(date);
};

export const formatLargeNumber = (value) => {
  if (value === null || value === undefined) return 'N/A';
  
  if (Math.abs(value) >= 1e12) {
    return `${(value / 1e12).toFixed(2)}T`;
  } else if (Math.abs(value) >= 1e9) {
    return `${(value / 1e9).toFixed(2)}B`;
  } else if (Math.abs(value) >= 1e6) {
    return `${(value / 1e6).toFixed(2)}M`;
  } else if (Math.abs(value) >= 1e3) {
    return `${(value / 1e3).toFixed(2)}K`;
  }
  
  return value.toFixed(2);
};

export const getColorScale = (value, min = 0, max = 100) => {
  // Normalize value between 0 and 1
  const normalized = Math.max(0, Math.min(1, (value - min) / (max - min)));
  
  // Generate color scale from blue (low) to red (high)
  // This is a simple RGB interpolation from blue to red
  const r = Math.round(normalized * 255);
  const b = Math.round((1 - normalized) * 255);
  
  return `rgb(${r}, 0, ${b})`;
};