from sqlalchemy import Column, String, Float, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TariffMeasure(Base):
    __tablename__ = "tariff_measures"
    
    id = Column(String, primary_key=True)
    source_type = Column(String, index=True)
    source_url = Column(String)
    title = Column(String)
    publication_date = Column(String, index=True)
    implementation_date = Column(String, nullable=True)
    expiration_date = Column(String, nullable=True)
    tariff_type = Column(String, index=True)
    affected_countries = Column(Text) 
    affected_industries = Column(Text)
    tariff_rates = Column(Text)
    full_text = Column(Text)
    extracted_highlights = Column(Text)
    status = Column(String, index=True)
    last_updated = Column(String)

class CountryProfile(Base):
    __tablename__ = "country_profiles"
    
    country_code = Column(String, primary_key=True)
    country_name = Column(String, index=True)
    region = Column(String, index=True)
    latest_trade_deficit = Column(Float)
    trade_deficit_trend = Column(Text) 
    total_exports = Column(Float)
    total_imports = Column(Float)
    tariff_measures = Column(Text)
    affected_industries = Column(Text) 
    supply_chain_risk = Column(Float)
    tariff_impact = Column(Float)
    jobs_impact = Column(Float)
    last_updated = Column(String)

class IndustryProfile(Base):
    __tablename__ = "industry_profiles"
    
    industry_code = Column(String, primary_key=True)
    industry_name = Column(String, index=True)
    sector = Column(String, index=True)
    countries_affected = Column(Text) 
    initial_tariff = Column(Float)
    effective_tariff = Column(Float)
    trade_volume = Column(Float)
    gva_impact = Column(Float)
    jobs_impact = Column(Float)
    last_updated = Column(String)

class EconomicTimeSeries(Base):
    __tablename__ = "economic_time_series"
    
    id = Column(String, primary_key=True)
    metric = Column(String, index=True)
    country_code = Column(String, index=True, nullable=True)
    industry_code = Column(String, index=True, nullable=True)
    frequency = Column(String)
    time_points = Column(Text)  
    values = Column(Text) 
    source = Column(String)
    last_updated = Column(String)