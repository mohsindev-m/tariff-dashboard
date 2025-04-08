from fastapi import APIRouter, Depends, BackgroundTasks
from app.services.tariff_pipeline import get_pipeline, TariffDataPipeline

router = APIRouter(prefix="/api", tags=["dashboard"])

# Get dashboard data
@router.get("/dashboard")
async def get_dashboard_data(pipeline: TariffDataPipeline = Depends(get_pipeline)):
    """Return all dashboard data"""
    data = pipeline.get_dashboard_api_data()
    return {"status": "success", "data": data}

# Heatmap data
@router.get("/heatmap")
async def get_heatmap_data(pipeline: TariffDataPipeline = Depends(get_pipeline)):
    """Return data for the global heatmap"""
    data = pipeline.get_dashboard_api_data()
    return {"status": "success", "data": data.get('heatmap_data', [])}

# Sector chart data
@router.get("/sectors")
async def get_sector_data(pipeline: TariffDataPipeline = Depends(get_pipeline)):
    """Return data for the sector pie chart"""
    data = pipeline.get_dashboard_api_data()
    return {"status": "success", "data": data.get('sector_data', [])}

# Historical trends data
@router.get("/timeseries")
async def get_timeseries_data(pipeline: TariffDataPipeline = Depends(get_pipeline)):
    """Return data for the historical line chart"""
    data = pipeline.get_dashboard_api_data()
    return {"status": "success", "data": data.get('time_series', [])}

# Detailed table data
@router.get("/table")
async def get_table_data(pipeline: TariffDataPipeline = Depends(get_pipeline)):
    """Return data for the detailed metrics table"""
    data = pipeline.get_dashboard_api_data()
    return {"status": "success", "data": data.get('detail_table', {})}

# Country list
@router.get("/countries")
async def get_countries(pipeline: TariffDataPipeline = Depends(get_pipeline)):
    """Return list of all countries with tariff measures"""
    data = pipeline.get_dashboard_api_data()
    if 'detail_table' in data and 'countries' in data['detail_table']:
        return {"status": "success", "data": data['detail_table']['countries']}
    return {"status": "success", "data": []}

# Industry list
@router.get("/industries")
async def get_industries(pipeline: TariffDataPipeline = Depends(get_pipeline)):
    """Return list of all industries with tariff measures"""
    data = pipeline.get_dashboard_api_data()
    if 'detail_table' in data and 'industries' in data['detail_table']:
        return {"status": "success", "data": data['detail_table']['industries']}
    return {"status": "success", "data": []}

# Recent tariff measures
@router.get("/measures")
async def get_measures(pipeline: TariffDataPipeline = Depends(get_pipeline)):
    """Return recent tariff measures"""
    import sqlite3
    import json
    
    conn = sqlite3.connect(pipeline.db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, title, publication_date, affected_countries, affected_industries, 
               tariff_type, status
        FROM tariff_measures
        ORDER BY publication_date DESC
        LIMIT 50
    """)
    measures = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    # Process JSON fields
    for measure in measures:
        if 'affected_countries' in measure:
            measure['affected_countries'] = json.loads(measure['affected_countries'])
        if 'affected_industries' in measure:
            measure['affected_industries'] = json.loads(measure['affected_industries'])
    
    return {"status": "success", "data": measures}

# Update endpoint
@router.post("/update")
async def trigger_update(background_tasks: BackgroundTasks, pipeline: TariffDataPipeline = Depends(get_pipeline)):
    """Trigger a pipeline update in the background"""
    # Add the pipeline update as a background task
    background_tasks.add_task(pipeline.run_full_pipeline)
    return {"status": "success", "message": "Pipeline update has been started in the background"}