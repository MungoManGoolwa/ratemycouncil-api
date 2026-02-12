from database import SessionLocal
from models import CouncilMetrics, CouncilUniqueData

db = SessionLocal()
try:
    # Add sample metrics for Melbourne (council_id=1)
    metrics = CouncilMetrics(
        council_id=1,
        year=2023,
        rates_revenue=120000000,
        total_revenue=150000000,
        total_expenditure=145000000,
        operating_deficit=5000000,
        population_served=100000,
        area_km2=50,
        roads_maintained_km=500,
        customer_satisfaction=85,
        service_delivery_score=82
    )
    db.add(metrics)

    # Add sample unique data
    unique_data = CouncilUniqueData(
        council_id=1,
        data_type='environmental',
        data_key='carbon_emissions_reduction',
        data_value=15.5,
        data_text='15.5% reduction in carbon emissions since 2020'
    )
    db.add(unique_data)

    unique_data2 = CouncilUniqueData(
        council_id=1,
        data_type='infrastructure',
        data_key='bike_paths_km',
        data_value=120,
        data_text='120km of dedicated bike paths'
    )
    db.add(unique_data2)

    db.commit()
    print('Sample metrics and unique data added successfully!')

except Exception as e:
    print(f'Error: {e}')
    db.rollback()
finally:
    db.close()