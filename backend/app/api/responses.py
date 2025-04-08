from typing import Any, Dict, Generic, List, Optional, TypeVar
from pydantic import BaseModel, Field

DataT = TypeVar('DataT')

class SuccessResponse(BaseModel, Generic[DataT]):
    status: str = Field(default="success", description="Status of the response")
    data: Optional[DataT] = Field(default=None, description="Payload of the response")

class ErrorResponse(BaseModel):
    status: str = Field(default="error", description="Status of the response")
    message: str = Field(..., description="Error message")
    details: Optional[Dict[str, Any]] = Field(default=None, description="Additional error details")

class CountryProfileResponse(BaseModel):
    country_code: str = Field(..., description="ISO code of the country")
    country_name: str = Field(..., description="Name of the country")
    region: str = Field(..., description="Geographical region of the country")
    trade_deficit: float = Field(..., description="Trade deficit value")
    exports: float = Field(..., description="Exports value")
    imports: float = Field(..., description="Imports value")
    supply_chain_risk: float = Field(..., description="Supply chain risk factor")
    tariff_impact: float = Field(..., description="Impact of tariffs")
    jobs_impact: float = Field(..., description="Impact on jobs")
    value: float = Field(..., description="Value for heatmap intensity")

class SectorDataResponse(BaseModel):
    sector: str = Field(..., description="Name of the economic sector")
    trade_volume: float = Field(..., description="Trade volume of the sector")
    average_tariff: float = Field(..., description="Average tariff in the sector")
    jobs_impact: float = Field(..., description="Impact on jobs in the sector")
    percentage: float = Field(..., description="Percentage representation of the sector")

class TimeSeriesPointResponse(BaseModel):
    year: str = Field(..., description="Year of the data point")
    trade_deficit: Optional[float] = Field(default=None, description="Trade deficit value for the year")
    exports: Optional[float] = Field(default=None, description="Exports value for the year")
    imports: Optional[float] = Field(default=None, description="Imports value for the year")

class MeasureResponse(BaseModel):
    id: str = Field(..., description="Identifier of the measure")
    title: str = Field(..., description="Title of the measure")
    publication_date: str = Field(..., description="Publication date of the measure")
    affected_countries: List[str] = Field(..., description="List of affected countries")
    affected_industries: List[str] = Field(..., description="List of affected industries")
    tariff_type: str = Field(..., description="Type of tariff")
    status: str = Field(..., description="Current status of the measure")
