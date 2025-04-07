from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from datetime import datetime

class TradeBalance(BaseModel):
    region: str
    trade_deficit: float
    exports: float
    imports: float
    effective_tariff: float = 0
    code: str

class SectorImpact(BaseModel):
    sector: str
    export_value: float
    percentage: float
    tariff_impact: float = 0
    gdp_contribution: float = 0
    jobs_impact: float = 0

class HistoricalTrend(BaseModel):
    year: str
    exports: float
    imports: float
    trade_deficit: float
    bea_balance: float = 0

class DetailedMetric(BaseModel):
    hs_code: str
    description: str
    export_value: float
    export_year: float
    tariff_rate: float = 0
    supply_chain_risk: float = 0

class DashboardData(BaseModel):
    metadata: Dict[str, Any]
    global_map_data: List[TradeBalance]
    sector_impact_data: List[SectorImpact]
    historical_trends: List[HistoricalTrend]
    detailed_metrics: List[DetailedMetric]