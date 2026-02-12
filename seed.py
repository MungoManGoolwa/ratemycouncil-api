from database import SessionLocal
from models import Council, Indicator, CouncilIndicatorValue, ServiceScore, CouncilIndex, Rating
from datetime import datetime

def seed_data():
    db = SessionLocal()
    try:
        # Sample councils
        councils = [
            Council(name="Melbourne City Council", state="Victoria", population=100000, area_km2=50, peer_group="Metro established", region_type="Urban"),
            Council(name="Geelong City Council", state="Victoria", population=200000, area_km2=100, peer_group="Regional town", region_type="Urban"),
            Council(name="Ballarat City Council", state="Victoria", population=150000, area_km2=80, peer_group="Regional town", region_type="Urban"),
        ]
        for council in councils:
            db.add(council)
        db.commit()

        # Sample indicators
        indicators = [
            Indicator(canonical_name="Road Condition", service_category="Roads", description="Average road condition score", unit="Score", lower_is_better=False),
            Indicator(canonical_name="Waste Collection Efficiency", service_category="Waste", description="Percentage of on-time collections", unit="%", lower_is_better=False),
            Indicator(canonical_name="Planning Approval Time", service_category="Planning", description="Average days for approval", unit="Days", lower_is_better=True),
        ]
        for indicator in indicators:
            db.add(indicator)
        db.commit()

        # Sample indicator values
        values = [
            CouncilIndicatorValue(council_id=1, indicator_id=1, year=2023, raw_value=85, normalised_value=85, percentile_rank=80),
            CouncilIndicatorValue(council_id=2, indicator_id=1, year=2023, raw_value=75, normalised_value=75, percentile_rank=60),
            CouncilIndicatorValue(council_id=3, indicator_id=1, year=2023, raw_value=90, normalised_value=90, percentile_rank=90),
        ]
        for value in values:
            db.add(value)
        db.commit()

        # Sample service scores
        scores = [
            ServiceScore(council_id=1, service_category="Roads", year=2023, score=85),
            ServiceScore(council_id=2, service_category="Roads", year=2023, score=75),
            ServiceScore(council_id=3, service_category="Roads", year=2023, score=90),
        ]
        for score in scores:
            db.add(score)
        db.commit()

        # Sample council index
        indices = [
            CouncilIndex(council_id=1, year=2023, score=82),
            CouncilIndex(council_id=2, year=2023, score=78),
            CouncilIndex(council_id=3, year=2023, score=88),
        ]
        for index in indices:
            db.add(index)
        db.commit()

        print("Sample data seeded successfully")
    except Exception as e:
        print(f"Error seeding data: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    seed_data()