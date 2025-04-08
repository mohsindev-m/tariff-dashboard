import React, { useRef, useEffect, useState } from 'react';
import { MapContainer, TileLayer, Circle, Tooltip, useMap } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import { useTariffData } from '../context/TariffContext';
import { formatPercent, formatCurrency, formatNumber } from '../utils/format';

// Fix for Leaflet marker icons
import L from 'leaflet';
delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: require('leaflet/dist/images/marker-icon-2x.png'),
  iconUrl: require('leaflet/dist/images/marker-icon.png'),
  shadowUrl: require('leaflet/dist/images/marker-shadow.png'),
});

// Country coordinates database - will be used as fallback
const countryCoordinates = {
  "United States": { lat: 37.0902, lng: -95.7129 },
  "China": { lat: 35.8617, lng: 104.1954 },
  "European Union": { lat: 50.8503, lng: 4.3517 },
  "Canada": { lat: 56.1304, lng: -106.3468 },
  "Mexico": { lat: 23.6345, lng: -102.5528 },
  "Japan": { lat: 36.2048, lng: 138.2529 },
  "South Korea": { lat: 35.9078, lng: 127.7669 },
  "United Kingdom": { lat: 55.3781, lng: -3.4360 },
  "Brazil": { lat: -14.2350, lng: -51.9253 },
  "India": { lat: 20.5937, lng: 78.9629 },
  "Australia": { lat: -25.2744, lng: 133.7751 },
  "Vietnam": { lat: 14.0583, lng: 108.2772 },
  "Taiwan": { lat: 23.6978, lng: 120.9605 },
  "Russia": { lat: 61.5240, lng: 105.3188 },
  "Germany": { lat: 51.1657, lng: 10.4515 }
};

// Component to handle map view updates
const MapUpdater = ({ countries }) => {
  const map = useMap();
  
  useEffect(() => {
    if (countries && countries.length) {
      // Create bounds that include all countries with valid coordinates
      const validCountries = countries.filter(c => 
        c.lat && c.lng && 
        !isNaN(c.lat) && !isNaN(c.lng) && 
        Math.abs(c.lat) <= 90 && Math.abs(c.lng) <= 180
      );
      
      if (validCountries.length > 0) {
        const bounds = L.latLngBounds(validCountries.map(c => [c.lat, c.lng]));
        if (bounds.isValid()) {
          map.fitBounds(bounds, { padding: [50, 50] });
          return;
        }
      }
    }
    
    // Default world view if no valid bounds
    map.setView([20, 0], 2);
  }, [countries, map]);
  
  return null;
};

const GlobalHeatmap = () => {
  const { heatmapData, loading, error, selectedCountry, setSelectedCountry } = useTariffData();
  const [countriesWithCoords, setCountriesWithCoords] = useState([]);
  const mapRef = useRef(null);
  
  useEffect(() => {
    if (!heatmapData || !Array.isArray(heatmapData)) {
      setCountriesWithCoords([]);
      return;
    }
    
    // Process country data and add coordinates
    const processedCountries = heatmapData.map(country => {
      // Extract country name - handle different API response formats
      const countryName = country.country_name || country.name || '';
      
      // Check if coordinates are already in the API response
      if (country.lat && country.lng && !isNaN(country.lat) && !isNaN(country.lng)) {
        return country;
      }
      
      // Look up coordinates from our database or approximate by country code
      const coords = countryCoordinates[countryName] || { lat: 0, lng: 0 };
      
      // Return country with added coordinates
      return {
        ...country,
        lat: coords.lat,
        lng: coords.lng
      };
    });
    
    setCountriesWithCoords(processedCountries);
  }, [heatmapData]);
  
  if (loading) return <div className="h-full flex items-center justify-center">Loading map data...</div>;
  if (error) return <div className="h-full flex items-center justify-center text-red-500">{error}</div>;
  if (!countriesWithCoords.length) return <div className="h-full flex items-center justify-center">No country data available for map</div>;
  
  // Find min and max values for scaling circles and colors
  const values = countriesWithCoords
    .map(c => parseFloat(c.value || c.tariff_impact || c.trade_deficit || 0))
    .filter(v => !isNaN(v));
  
  const maxValue = values.length ? Math.max(...values, 1) : 1;
  const minValue = values.length ? Math.min(...values, 0) : 0;
  
  // Generate color based on value
  const getColorScale = (value, min = minValue, max = maxValue) => {
    // Normalize value between 0 and 1
    const normalized = Math.max(0, Math.min(1, (value - min) / (max - min || 1)));
    
    // Generate color scale from blue (low) to red (high)
    const r = Math.round(normalized * 255);
    const b = Math.round((1 - normalized) * 255);
    
    return `rgb(${r}, 0, ${b})`;
  };
  
  return (
    <div className="h-full w-full">
      <MapContainer 
        ref={mapRef}
        style={{ height: '100%', width: '100%', minHeight: '400px' }}
        center={[20, 0]} 
        zoom={2} 
        scrollWheelZoom={true}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        
        <MapUpdater countries={countriesWithCoords} />
        
        {countriesWithCoords.map((country, index) => {
          // Skip countries without proper coordinates
          if (!country.lat || !country.lng || isNaN(country.lat) || isNaN(country.lng)) return null;
          
          // Get value for sizing and coloring - try different possible fields
          const value = parseFloat(country.value || country.tariff_impact || country.trade_deficit || 0);
          const tradeDeficit = parseFloat(country.trade_deficit || 0);
          const tariffImpact = parseFloat(country.tariff_impact || 0);
          const supplyChainRisk = parseFloat(country.supply_chain_risk || 0);
          const jobsImpact = parseFloat(country.jobs_impact || 0);
          
          // Size circle based on trade volume or impact value
          const circleRadius = 20000 + (Math.abs(value) / (maxValue || 1)) * 200000;
          
          return (
            <Circle
              key={country.country_code || `country-${index}`}
              center={[country.lat, country.lng]}
              radius={circleRadius}
              pathOptions={{
                fillColor: getColorScale(value),
                color: selectedCountry === country.country_code ? '#000' : '#666',
                fillOpacity: 0.7,
                weight: selectedCountry === country.country_code ? 3 : 1,
              }}
              eventHandlers={{
                click: () => setSelectedCountry(
                  selectedCountry === country.country_code ? null : country.country_code
                ),
              }}
            >
              <Tooltip>
                <div>
                  <h3 className="font-bold">{country.country_name}</h3>
                  {tariffImpact !== undefined && (
                    <p>Tariff Impact: {formatPercent(tariffImpact)}</p>
                  )}
                  {tradeDeficit !== undefined && (
                    <p>Trade Deficit: {formatCurrency(tradeDeficit, 0)}</p>
                  )}
                  {supplyChainRisk !== undefined && (
                    <p>Supply Chain Risk: {formatPercent(supplyChainRisk)}</p>
                  )}
                  {jobsImpact !== undefined && (
                    <p>Jobs Impact: {formatNumber(jobsImpact, 0)}</p>
                  )}
                </div>
              </Tooltip>
            </Circle>
          );
        })}
      </MapContainer>
    </div>
  );
};

export default GlobalHeatmap;