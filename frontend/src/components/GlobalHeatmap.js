import React, { useState, useEffect } from 'react';
import { MapContainer, TileLayer, CircleMarker, Tooltip, Popup } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import { useTariffData } from '../context/TariffContext';
import { formatCurrency } from '../utils/format';

// Map districts to coordinates (approximate)
const districtCoordinates = {
  // US Customs districts with lat/long coordinates
  '01': { lat: 45.2538, lng: -69.4455, name: 'Portland, ME' },
  '02': { lat: 44.9281, lng: -72.9514, name: 'St. Albans, VT' },
  '04': { lat: 42.3601, lng: -71.0589, name: 'Boston, MA' },
  '05': { lat: 41.8240, lng: -71.4128, name: 'Providence, RI' },
  '07': { lat: 44.6943, lng: -75.4588, name: 'Ogdensburg, NY' },
  '09': { lat: 42.8864, lng: -78.8784, name: 'Buffalo, NY' },
  '10': { lat: 40.7128, lng: -74.0060, name: 'New York, NY' },
  '11': { lat: 39.9526, lng: -75.1652, name: 'Philadelphia, PA' },
  '13': { lat: 39.2904, lng: -76.6122, name: 'Baltimore, MD' },
  '14': { lat: 36.8508, lng: -76.2859, name: 'Norfolk, VA' },
  '15': { lat: 34.2257, lng: -77.9447, name: 'Wilmington, NC' },
  '16': { lat: 32.7765, lng: -79.9311, name: 'Charleston, SC' },
  '17': { lat: 32.0809, lng: -81.0912, name: 'Savannah, GA' },
  '18': { lat: 26.7153, lng: -80.0534, name: 'West Palm Beach, FL' },
  '19': { lat: 25.7617, lng: -80.1918, name: 'Miami, FL' },
  '20': { lat: 27.9506, lng: -82.4572, name: 'Tampa, FL' },
  '21': { lat: 30.3322, lng: -81.6557, name: 'Jacksonville, FL' },
  '22': { lat: 29.9511, lng: -90.0715, name: 'New Orleans, LA' },
  '23': { lat: 27.8006, lng: -97.3964, name: 'Corpus Christi, TX' },
  '24': { lat: 29.7604, lng: -95.3698, name: 'Houston, TX' },
  '25': { lat: 32.7767, lng: -96.7970, name: 'Dallas, TX' },
  '26': { lat: 29.4241, lng: -98.4936, name: 'San Antonio, TX' },
  '27': { lat: 31.7587, lng: -106.4869, name: 'El Paso, TX' },
  '28': { lat: 32.2217, lng: -110.9265, name: 'Tucson, AZ' },
  '29': { lat: 33.4484, lng: -112.0740, name: 'Phoenix, AZ' },
  '30': { lat: 34.0522, lng: -118.2437, name: 'Los Angeles, CA' },
  '31': { lat: 32.7157, lng: -117.1611, name: 'San Diego, CA' },
  '32': { lat: 37.7749, lng: -122.4194, name: 'San Francisco, CA' },
  '33': { lat: 45.5051, lng: -122.6750, name: 'Portland, OR' },
  '35': { lat: 47.6062, lng: -122.3321, name: 'Seattle, WA' },
  '36': { lat: 61.2181, lng: -149.9003, name: 'Anchorage, AK' },
  '37': { lat: 21.3069, lng: -157.8583, name: 'Honolulu, HI' },
  '38': { lat: 43.0731, lng: -89.4012, name: 'Great Falls, MT' },
  '39': { lat: 46.8772, lng: -96.7898, name: 'Pembina, ND' },
  '41': { lat: 44.9778, lng: -93.2650, name: 'Minneapolis, MN' },
  '42': { lat: 41.2565, lng: -95.9345, name: 'Duluth, MN' },
  '45': { lat: 41.8781, lng: -87.6298, name: 'Chicago, IL' },
  '46': { lat: 42.3314, lng: -83.0458, name: 'Detroit, MI' },
  '47': { lat: 41.4993, lng: -81.6944, name: 'Cleveland, OH' },
  '49': { lat: 35.3478, lng: -80.9317, name: 'Cincinnati, OH' },
  '51': { lat: 35.2271, lng: -80.8431, name: 'Charlotte, NC' },
  '52': { lat: 25.8876, lng: -80.2241, name: 'Miami, FL (Zone)' },
  '53': { lat: 38.2527, lng: -85.7585, name: 'Louisville, KY' },
  '54': { lat: 18.2188, lng: -66.5210, name: 'San Juan, PR' },
  '55': { lat: 17.7366, lng: -64.7322, name: 'St. Thomas, VI' },
};

const GlobalHeatmap = () => {
  const { dashboardData, loading } = useTariffData();
  const [mapData, setMapData] = useState([]);

  useEffect(() => {
    if (dashboardData && dashboardData.global_map_data) {
      setMapData(dashboardData.global_map_data);
    }
  }, [dashboardData]);

  if (loading) {
    return <div className="loading">Loading global heatmap...</div>;
  }

  // Find the maximum trade deficit for scaling
  const maxDeficit = Math.max(...mapData.map(item => Math.abs(item.trade_deficit) || 0));

  // Calculate color based on trade balance (red for deficit, blue for surplus)
  const getColor = (value) => {
    const normalizedValue = value / maxDeficit; // Normalize value by maxDeficit
    if (value < 0) {
        return `rgba(255, 65, 54, ${Math.abs(normalizedValue)})`; // Red for deficit
    } else {
        return `rgba(0, 116, 217, ${normalizedValue})`; // Blue for surplus
    }
};

  // Calculate radius based on trade volume (imports + exports)
  const getRadius = (exports, imports) => {
    const volume = exports + imports;
    return Math.sqrt(volume) / 10000; // Scale down to reasonable size
  };

  return (
    <div className="map-container h-[70vh] w-full bg-gray-100 rounded-lg shadow-md">
      <MapContainer
        center={[38.0, -97.0]}
        zoom={4}
        style={{ height: '100%', width: '100%' }}
      >
        <TileLayer
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        />
        {mapData.map((district) => {
          const coords = districtCoordinates[district.code];
          if (!coords) return null;

          return (
            <CircleMarker
              key={district.code}
              center={[coords.lat, coords.lng]}
              radius={getRadius(district.exports, district.imports)}
              fillColor={getColor(district.trade_deficit)}
              color="#555"
              weight={1}
              opacity={0.8}
              fillOpacity={0.6}
            >
              <Tooltip>
                <div className="font-bold">{district.region}</div>
                <div>Trade Balance: {formatCurrency(district.trade_deficit)}</div>
              </Tooltip>
              <Popup>
                <div className="text-lg font-bold mb-2">{district.region}</div>
                <div className="mb-1">Exports: {formatCurrency(district.exports)}</div>
                <div className="mb-1">Imports: {formatCurrency(district.imports)}</div>
                <div className="mb-1">Trade Balance: {formatCurrency(district.trade_deficit)}</div>
                <div className="mb-1">Effective Tariff: {district.effective_tariff.toFixed(2)}%</div>
              </Popup>
            </CircleMarker>
          );
        })}
      </MapContainer>
    </div>
  );
};

export default GlobalHeatmap;