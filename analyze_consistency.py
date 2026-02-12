#!/usr/bin/env python3
"""
Analyze data consistency across councils
"""

from database import SessionLocal
from models import PerformanceMetric, Council, CouncilMetrics, CouncilUniqueData
from collections import defaultdict
import json

def analyze_data_consistency():
    db = SessionLocal()
    try:
        print('=== DATA CONSISTENCY ANALYSIS ===')

        # Get all councils
        councils = db.query(Council).all()
        print(f'Total councils: {len(councils)}')

        # Analyze PerformanceMetric inconsistencies
        print('\n=== PERFORMANCE METRICS ANALYSIS ===')
        perf_metrics = db.query(PerformanceMetric).all()
        print(f'Total performance metrics: {len(perf_metrics)}')

        # Group by category and council
        category_by_council = defaultdict(lambda: defaultdict(list))
        metric_names = set()

        for m in perf_metrics:
            council = db.query(Council).filter(Council.id == m.council_id).first()
            council_name = council.name if council else f'Council {m.council_id}'
            category_by_council[m.category][council_name].append(m.metric_name)
            metric_names.add(m.metric_name)

        print(f'Unique metric names: {len(metric_names)}')
        print(f'Categories: {sorted(category_by_council.keys())}')

        for category, councils_data in category_by_council.items():
            print(f'\n{category.upper()}:')
            for council_name, metrics in councils_data.items():
                print(f'  {council_name}: {len(metrics)} metrics - {metrics[:3]}...')

        # Analyze CouncilMetrics (standardized)
        print('\n=== COUNCIL METRICS ANALYSIS ===')
        std_metrics = db.query(CouncilMetrics).all()
        print(f'Total standardized metrics: {len(std_metrics)}')

        metric_fields = [
            'rates_revenue', 'total_revenue', 'total_expenditure', 'operating_deficit',
            'population_served', 'area_km2', 'roads_maintained_km',
            'customer_satisfaction', 'service_delivery_score'
        ]

        coverage = {}
        for field in metric_fields:
            count = sum(1 for m in std_metrics if getattr(m, field) is not None)
            coverage[field] = count

        print('Standardized metrics coverage:')
        for field, count in coverage.items():
            percentage = (count / len(councils)) * 100 if councils else 0
            print(f'  {field}: {count}/{len(councils)} councils ({percentage:.1f}%)')

        # Analyze CouncilUniqueData
        print('\n=== UNIQUE DATA ANALYSIS ===')
        unique_data = db.query(CouncilUniqueData).all()
        print(f'Total unique data points: {len(unique_data)}')

        data_types = defaultdict(int)
        data_keys = defaultdict(int)

        for ud in unique_data:
            data_types[ud.data_type] += 1
            data_keys[ud.data_key] += 1

        print('Data types distribution:')
        for dt, count in sorted(data_types.items()):
            print(f'  {dt}: {count}')

        print('\nTop data keys:')
        for key, count in sorted(data_keys.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f'  {key}: {count}')

        # Identify gaps and opportunities
        print('\n=== RECOMMENDATIONS ===')

        # Find councils with missing standardized metrics
        councils_with_metrics = set(m.council_id for m in std_metrics)
        councils_without_metrics = [c for c in councils if c.id not in councils_with_metrics]

        if councils_without_metrics:
            print(f'Councils missing standardized metrics ({len(councils_without_metrics)}):')
            for c in councils_without_metrics[:5]:  # Show first 5
                print(f'  - {c.name} ({c.state})')

        # Find inconsistent metric naming
        print('\nPotential metric name inconsistencies:')
        name_groups = defaultdict(list)
        for name in metric_names:
            # Group similar names
            base_name = name.lower().replace('_', ' ').replace('-', ' ')
            name_groups[base_name].append(name)

        inconsistent_groups = {k: v for k, v in name_groups.items() if len(v) > 1}
        if inconsistent_groups:
            print(f'Found {len(inconsistent_groups)} groups of similar metric names:')
            for base, variants in list(inconsistent_groups.items())[:5]:
                print(f'  {base}: {variants}')

    except Exception as e:
        print(f'Error: {e}')
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    analyze_data_consistency()