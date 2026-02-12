#!/usr/bin/env python3
"""
Check current database contents
"""

from database import SessionLocal
from models import Council, IssueReport, InfrastructureProject, FinancialData, PerformanceMetric, ElectionEvent, BusinessPermit, TourismAmenity

def check_database():
    db = SessionLocal()

    print('=== CURRENT DATABASE STATUS ===')
    print(f'Councils: {db.query(Council).count()}')
    print(f'Issue Reports: {db.query(IssueReport).count()}')
    print(f'Infrastructure Projects: {db.query(InfrastructureProject).count()}')
    print(f'Financial Records: {db.query(FinancialData).count()}')
    print(f'Performance Metrics: {db.query(PerformanceMetric).count()}')
    print(f'Election Events: {db.query(ElectionEvent).count()}')
    print(f'Business Permits: {db.query(BusinessPermit).count()}')
    print(f'Tourism Amenities: {db.query(TourismAmenity).count()}')

    print('\n=== SAMPLE COUNCIL DATA ===')
    councils = db.query(Council).limit(3).all()
    for c in councils:
        print(f'{c.name} ({c.state}): pop={c.population}, area={c.area_km2}km²')

    print('\n=== SAMPLE ISSUES ===')
    issues = db.query(IssueReport).limit(2).all()
    for i in issues:
        print(f'{i.category}: {i.description[:50]}... (status: {i.status})')

    print('\n=== SAMPLE PROJECTS ===')
    projects = db.query(InfrastructureProject).limit(2).all()
    for p in projects:
        print(f'{p.name}: {p.status} - ${p.budget:,}' if p.budget else f'{p.name}: {p.status}')

    print('\n=== SAMPLE AMENITIES ===')
    amenities = db.query(TourismAmenity).limit(3).all()
    for a in amenities:
        print(f'{a.name}: {a.category} (rating: {a.rating})')

    db.close()

if __name__ == "__main__":
    check_database()