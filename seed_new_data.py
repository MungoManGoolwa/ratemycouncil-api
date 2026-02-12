"""
Sample Data Seeder for New Entities
Populates database with sample data for testing new features
"""

from database import SessionLocal
from models import (
    IssueReport, InfrastructureProject, FinancialData, PerformanceMetric,
    ElectionEvent, BusinessPermit, TourismAmenity
)
from datetime import datetime, timedelta
import random

def seed_new_entities():
    """Seed sample data for new entities"""
    db = SessionLocal()
    try:
        # Sample Issue Reports
        issues = [
            IssueReport(
                council_id=1,  # Melbourne
                category="potholes",
                description="Large pothole on Main Street causing traffic hazards",
                latitude=-37.8136,
                longitude=144.9631,
                status="reported",
                priority="high",
                created_at=datetime.now() - timedelta(days=5)
            ),
            IssueReport(
                council_id=2,  # Geelong
                category="waste",
                description="Overflowing rubbish bin at the park",
                latitude=-38.1499,
                longitude=144.3617,
                status="in_progress",
                priority="medium",
                created_at=datetime.now() - timedelta(days=2)
            )
        ]

        # Sample Infrastructure Projects
        projects = [
            InfrastructureProject(
                council_id=1,
                name="CBD Pedestrian Mall Upgrade",
                category="roads",
                description="Upgrading pedestrian areas in central Melbourne",
                status="in_progress",
                budget=5000000,
                start_date=datetime.now() - timedelta(days=30),
                completion_date=datetime.now() + timedelta(days=180),
                latitude=-37.8136,
                longitude=144.9631
            ),
            InfrastructureProject(
                council_id=2,
                name="Geelong Waterfront Development",
                category="parks",
                description="New recreational facilities along the waterfront",
                status="planned",
                budget=12000000,
                start_date=datetime.now() + timedelta(days=60),
                latitude=-38.1499,
                longitude=144.3617
            )
        ]

        # Sample Financial Data
        financial_data = [
            FinancialData(
                council_id=1,
                year=2023,
                total_revenue=150000000,
                total_expenditure=145000000,
                rates_revenue=120000000,
                grants_revenue=20000000,
                rate_capping_impact=2000000,
                value_for_money_score=85.5
            ),
            FinancialData(
                council_id=2,
                year=2023,
                total_revenue=80000000,
                total_expenditure=78000000,
                rates_revenue=65000000,
                grants_revenue=10000000,
                rate_capping_impact=1500000,
                value_for_money_score=82.3
            )
        ]

        # Sample Performance Metrics
        metrics = [
            PerformanceMetric(
                council_id=1,
                metric_name="complaint_response_time",
                category="complaints",
                value=3.2,
                unit="days",
                year=2023,
                quarter=4
            ),
            PerformanceMetric(
                council_id=1,
                metric_name="planning_approval_time",
                category="planning",
                value=45.8,
                unit="days",
                year=2023,
                quarter=4
            ),
            PerformanceMetric(
                council_id=2,
                metric_name="waste_collection_efficiency",
                category="waste",
                value=96.5,
                unit="percentage",
                year=2023,
                quarter=4
            )
        ]

        # Sample Election Events
        elections = [
            ElectionEvent(
                council_id=1,
                event_type="council_election",
                title="Melbourne City Council Election 2024",
                description="Regular council election for all wards",
                event_date=datetime.now() + timedelta(days=120),
                status="upcoming"
            ),
            ElectionEvent(
                council_id=2,
                event_type="policy_change",
                title="Geelong Rate Capping Review",
                description="Community consultation on rate capping policy",
                event_date=datetime.now() + timedelta(days=60),
                status="upcoming"
            )
        ]

        # Sample Business Permits
        permits = [
            BusinessPermit(
                council_id=1,
                permit_type="food_premises",
                application_date=datetime.now() - timedelta(days=30),
                approval_date=datetime.now() - timedelta(days=15),
                processing_time_days=15,
                status="approved"
            ),
            BusinessPermit(
                council_id=2,
                permit_type="building",
                application_date=datetime.now() - timedelta(days=45),
                status="pending"
            )
        ]

        # Sample Tourism Amenities
        amenities = [
            TourismAmenity(
                council_id=1,
                name="Royal Exhibition Building",
                category="museum",
                description="Historic UNESCO World Heritage site",
                rating=4.5,
                latitude=-37.8047,
                longitude=144.9717,
                accessibility_features=["wheelchair_access", "audio_guide"],
                multilingual_support=True
            ),
            TourismAmenity(
                council_id=1,
                name="Federation Square",
                category="park",
                description="Cultural and events precinct",
                rating=4.2,
                latitude=-37.8179,
                longitude=144.9689,
                accessibility_features=["wheelchair_access"],
                multilingual_support=True
            ),
            TourismAmenity(
                council_id=2,
                name="Geelong Waterfront",
                category="park",
                description="Scenic waterfront promenade",
                rating=4.0,
                latitude=-38.1499,
                longitude=144.3617,
                accessibility_features=["bike_paths", "picnic_areas"]
            )
        ]

        # Add all data to database
        for item in issues + projects + financial_data + metrics + elections + permits + amenities:
            db.add(item)

        db.commit()
        print("Sample data seeded successfully!")

        # Print summary
        print(f"Issues: {len(issues)}")
        print(f"Projects: {len(projects)}")
        print(f"Financial records: {len(financial_data)}")
        print(f"Performance metrics: {len(metrics)}")
        print(f"Election events: {len(elections)}")
        print(f"Business permits: {len(permits)}")
        print(f"Tourism amenities: {len(amenities)}")

    except Exception as e:
        print(f"Error seeding data: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    seed_new_entities()