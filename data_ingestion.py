"""
Comprehensive Data Ingestion and Normalization System
Ingests data from multiple sources and normalizes it for consistent comparison
"""

import pandas as pd
import requests
import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from database import SessionLocal
from models import (
    Council, PerformanceMetric, CouncilMetrics, CouncilUniqueData,
    Indicator, CouncilIndicatorValue
)
from metrics_framework import metric_normalizer, STANDARDIZED_METRICS, MetricCategory
import re

logger = logging.getLogger(__name__)

class DataIngester:
    """Handles ingestion from various data sources"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'RateMyCouncil-DataIngestion/1.0'
        })

    def ingest_council_reports(self, council_id: int, report_url: str = None) -> Dict[str, Any]:
        """Ingest data from council annual reports"""
        try:
            if not report_url:
                # Generate likely report URL
                council = self._get_council_info(council_id)
                if council:
                    report_url = self._generate_report_url(council)

            if report_url:
                response = self.session.get(report_url, timeout=10)
                if response.status_code == 200:
                    return self._parse_council_report(response.content, council_id)

        except Exception as e:
            logger.error(f"Error ingesting council report for {council_id}: {e}")

        return {}

    def ingest_state_government_data(self, state: str) -> Dict[str, Any]:
        """Ingest data from state government sources"""
        try:
            state_sources = {
                'Victoria': 'https://www.localgovernment.vic.gov.au/performance-reporting',
                'NSW': 'https://www.olg.nsw.gov.au/performance',
                'Queensland': 'https://www.dlg.qld.gov.au/performance',
                'WA': 'https://www.dlg.wa.gov.au/performance',
                'SA': 'https://www.dpti.sa.gov.au/performance',
                'Tasmania': 'https://www.dpac.tas.gov.au/performance',
                'NT': 'https://www.localgovernment.nt.gov.au/performance',
                'ACT': 'https://www.act.gov.au/performance'
            }

            if state in state_sources:
                url = state_sources[state]
                response = self.session.get(url, timeout=10)
                if response.status_code == 200:
                    return self._parse_state_data(response.content, state)

        except Exception as e:
            logger.error(f"Error ingesting state data for {state}: {e}")

        return {}

    def _get_council_info(self, council_id: int) -> Optional[Dict]:
        """Get council information from database"""
        db = SessionLocal()
        try:
            council = db.query(Council).filter(Council.id == council_id).first()
            if council:
                return {
                    'id': council.id,
                    'name': council.name,
                    'state': council.state,
                    'population': council.population,
                    'area_km2': council.area_km2
                }
        finally:
            db.close()
        return None

    def _generate_report_url(self, council_info: Dict) -> str:
        """Generate likely annual report URL for council"""
        name_slug = council_info['name'].lower().replace(' council', '').replace(' ', '')
        state = council_info['state'].lower()

        # Common URL patterns
        patterns = [
            f"https://www.{name_slug}.{state}.gov.au/annual-report",
            f"https://www.{name_slug}.vic.gov.au/about-council/annual-report",
            f"https://{name_slug}.org.au/annual-report",
            f"https://www.{name_slug}.com.au/annual-report"
        ]

        return patterns[0]  # Return first pattern for now

    def _parse_council_report(self, content: bytes, council_id: int) -> Dict[str, Any]:
        """Parse council annual report for metrics"""
        # This would use BeautifulSoup and regex to extract data
        # For now, return mock data structure
        return {
            'financial': {
                'rates_revenue': 120000000,
                'total_revenue': 150000000,
                'total_expenditure': 145000000
            },
            'performance': {
                'complaint_response_time': 3.2,
                'waste_collection_efficiency': 95.5,
                'customer_satisfaction': 82
            },
            'source': 'council_report',
            'year': datetime.now().year
        }

    def _parse_state_data(self, content: bytes, state: str) -> Dict[str, Any]:
        """Parse state government performance data"""
        # Parse state-level aggregated data
        return {
            'state_averages': {
                'complaint_response_time': 4.1,
                'waste_collection_efficiency': 93.2,
                'planning_approval_time': 45.6
            },
            'source': f'{state}_government',
            'year': datetime.now().year
        }

class DataNormalizer:
    """Normalizes and standardizes council data"""

    def __init__(self):
        self.ingester = DataIngester()

    def normalize_council_data(self, council_id: int) -> Dict[str, Any]:
        """Comprehensive data normalization for a council"""
        council_info = self.ingester._get_council_info(council_id)
        if not council_info:
            return {}

        normalized_data = {
            'council_id': council_id,
            'council_info': council_info,
            'standardized_metrics': {},
            'unique_data': {},
            'data_sources': [],
            'normalization_date': datetime.now(),
            'coverage_score': 0.0
        }

        # 1. Ingest from multiple sources
        sources_data = self._ingest_all_sources(council_id, council_info)

        # 2. Map raw metrics to standardized metrics
        raw_metrics = {}
        for source_name, source_data in sources_data.items():
            raw_metrics.update(self._extract_raw_metrics(source_data))
            normalized_data['data_sources'].append(source_name)

        # 3. Normalize each metric
        available_metrics = 0
        for std_metric in STANDARDIZED_METRICS:
            canonical_name = std_metric.canonical_name

            # Try to find matching raw metric
            matched_value = self._find_matching_raw_metric(
                canonical_name, raw_metrics, council_info['state']
            )

            if matched_value is not None:
                # Normalize the value
                normalized_value = metric_normalizer.normalize_value(
                    matched_value, canonical_name, council_info
                )
                normalized_data['standardized_metrics'][canonical_name] = {
                    'value': normalized_value,
                    'raw_value': matched_value,
                    'source': self._identify_source(canonical_name, sources_data),
                    'confidence': 'high'
                }
                available_metrics += 1
            else:
                # Try to estimate missing metric
                estimated_value = self._estimate_missing_metric(
                    canonical_name, council_info, sources_data
                )
                if estimated_value is not None:
                    normalized_data['standardized_metrics'][canonical_name] = {
                        'value': estimated_value,
                        'raw_value': None,
                        'source': 'estimated',
                        'confidence': 'medium'
                    }
                    available_metrics += 1

        # 4. Store unique data that doesn't fit standard metrics
        normalized_data['unique_data'] = self._extract_unique_data(
            raw_metrics, normalized_data['standardized_metrics']
        )

        # 5. Calculate coverage score
        normalized_data['coverage_score'] = available_metrics / len(STANDARDIZED_METRICS)

        return normalized_data

    def _ingest_all_sources(self, council_id: int, council_info: Dict) -> Dict[str, Any]:
        """Ingest data from all available sources"""
        sources_data = {}

        # Council annual report
        report_data = self.ingester.ingest_council_reports(council_id)
        if report_data:
            sources_data['council_report'] = report_data

        # State government data
        state_data = self.ingester.ingest_state_government_data(council_info['state'])
        if state_data:
            sources_data['state_government'] = state_data

        # Existing database data
        db_data = self._get_existing_db_data(council_id)
        if db_data:
            sources_data['existing_db'] = db_data

        return sources_data

    def _get_existing_db_data(self, council_id: int) -> Dict[str, Any]:
        """Get existing metrics from database"""
        db = SessionLocal()
        try:
            existing_metrics = {}
            existing_unique = {}

            # Get performance metrics
            perf_metrics = db.query(PerformanceMetric).filter(
                PerformanceMetric.council_id == council_id
            ).all()

            for metric in perf_metrics:
                key = f"{metric.category}_{metric.metric_name}"
                existing_metrics[key] = {
                    'value': metric.value,
                    'unit': metric.unit,
                    'year': metric.year
                }

            # Get standardized metrics
            std_metrics = db.query(CouncilMetrics).filter(
                CouncilMetrics.council_id == council_id
            ).first()

            if std_metrics:
                existing_metrics.update({
                    'rates_revenue': std_metrics.rates_revenue,
                    'total_revenue': std_metrics.total_revenue,
                    'total_expenditure': std_metrics.total_expenditure,
                    'operating_deficit': std_metrics.operating_deficit,
                    'population_served': std_metrics.population_served,
                    'area_km2': std_metrics.area_km2,
                    'roads_maintained_km': std_metrics.roads_maintained_km,
                    'customer_satisfaction': std_metrics.customer_satisfaction,
                    'service_delivery_score': std_metrics.service_delivery_score
                })

            # Get unique data
            unique_data = db.query(CouncilUniqueData).filter(
                CouncilUniqueData.council_id == council_id
            ).all()

            for ud in unique_data:
                existing_unique[ud.data_key] = {
                    'value': ud.data_value,
                    'text': ud.data_text,
                    'type': ud.data_type
                }

            return {
                'metrics': existing_metrics,
                'unique_data': existing_unique
            }

        finally:
            db.close()

    def _extract_raw_metrics(self, source_data: Dict) -> Dict[str, Any]:
        """Extract raw metrics from source data"""
        raw_metrics = {}

        # Flatten nested structures
        def flatten_dict(d, prefix=''):
            for k, v in d.items():
                new_key = f"{prefix}_{k}" if prefix else k
                if isinstance(v, dict):
                    flatten_dict(v, new_key)
                elif isinstance(v, (int, float)):
                    raw_metrics[new_key] = v

        flatten_dict(source_data)
        return raw_metrics

    def _find_matching_raw_metric(self, canonical_name: str, raw_metrics: Dict,
                                 state: str = None) -> Optional[float]:
        """Find raw metric that matches standardized metric"""
        # Try direct mapping
        if canonical_name in raw_metrics:
            return raw_metrics[canonical_name]

        # Try metric normalizer matching
        match_result = metric_normalizer.find_matching_metric(canonical_name, state)
        if match_result:
            std_name, _ = match_result
            if std_name in raw_metrics:
                return raw_metrics[std_name]

        # Try fuzzy matching on raw metric names
        for raw_name, value in raw_metrics.items():
            if self._names_similar(canonical_name, raw_name):
                return value

        return None

    def _names_similar(self, name1: str, name2: str) -> bool:
        """Check if two metric names are similar"""
        # Simple similarity check - can be enhanced with NLP
        n1 = name1.lower().replace('_', ' ').replace('-', ' ')
        n2 = name2.lower().replace('_', ' ').replace('-', ' ')

        # Exact match after normalization
        if n1 == n2:
            return True

        # Contains key terms
        key_terms_1 = set(n1.split())
        key_terms_2 = set(n2.split())

        overlap = len(key_terms_1.intersection(key_terms_2))
        return overlap >= min(len(key_terms_1), len(key_terms_2)) * 0.6

    def _identify_source(self, metric_name: str, sources_data: Dict) -> str:
        """Identify which source provided a metric"""
        # This would track source attribution - simplified for now
        return 'multiple_sources'

    def _estimate_missing_metric(self, metric_name: str, council_info: Dict,
                               sources_data: Dict) -> Optional[float]:
        """Estimate missing metric using available data and peer comparisons"""
        # Get peer council data for estimation
        peer_data = self._get_peer_council_data(council_info)

        return metric_normalizer.estimate_missing_metric(
            metric_name, council_info, peer_data
        )

    def _get_peer_council_data(self, council_info: Dict) -> List[Dict]:
        """Get data from similar councils for peer comparison"""
        db = SessionLocal()
        try:
            # Find councils with similar characteristics
            population = council_info.get('population', 0)
            state = council_info.get('state')

            # Get councils within 50% population range in same state
            peers = db.query(Council).filter(
                Council.state == state,
                Council.population.between(population * 0.5, population * 1.5)
            ).limit(5).all()

            peer_data = []
            for peer in peers:
                peer_metrics = db.query(CouncilMetrics).filter(
                    CouncilMetrics.council_id == peer.id
                ).first()

                if peer_metrics:
                    peer_data.append({
                        'population_served': peer_metrics.population_served,
                        'rates_revenue_per_capita': (peer_metrics.rates_revenue / peer_metrics.population_served) if peer_metrics.rates_revenue and peer_metrics.population_served else None,
                        'total_revenue_per_capita': (peer_metrics.total_revenue / peer_metrics.population_served) if peer_metrics.total_revenue and peer_metrics.population_served else None,
                        'customer_satisfaction_score': peer_metrics.customer_satisfaction,
                        'waste_collection_efficiency': None,  # Would need to get from performance metrics
                    })

            return peer_data

        finally:
            db.close()

    def _extract_unique_data(self, raw_metrics: Dict, standardized_metrics: Dict) -> Dict[str, Any]:
        """Extract data that doesn't fit standardized metrics"""
        unique_data = {}

        for raw_name, value in raw_metrics.items():
            # Check if this raw metric was used in standardization
            used_in_standardization = False
            for std_metric in standardized_metrics.values():
                if std_metric.get('raw_value') == value:
                    used_in_standardization = True
                    break

            if not used_in_standardization:
                # This is unique data
                unique_data[raw_name] = {
                    'value': value,
                    'type': self._infer_data_type(raw_name),
                    'description': f"Raw metric: {raw_name}"
                }

        return unique_data

    def _infer_data_type(self, metric_name: str) -> str:
        """Infer data type from metric name"""
        name_lower = metric_name.lower()

        if any(term in name_lower for term in ['carbon', 'emission', 'environment', 'sustainability']):
            return 'environmental'
        elif any(term in name_lower for term in ['bike', 'path', 'park', 'infrastructure', 'road']):
            return 'infrastructure'
        elif any(term in name_lower for term in ['economic', 'business', 'employment', 'job']):
            return 'economic'
        elif any(term in name_lower for term in ['community', 'engagement', 'participation']):
            return 'community'
        else:
            return 'performance'

class DataAggregator:
    """Aggregates normalized data for comparisons and analysis"""

    def __init__(self):
        self.normalizer = DataNormalizer()

    def aggregate_state_data(self, state: str) -> Dict[str, Any]:
        """Aggregate data across all councils in a state"""
        db = SessionLocal()
        try:
            councils = db.query(Council).filter(Council.state == state).all()

            state_aggregation = {
                'state': state,
                'total_councils': len(councils),
                'metrics_coverage': {},
                'averages': {},
                'medians': {},
                'best_performers': {},
                'worst_performers': {}
            }

            # Collect all normalized data
            all_council_data = []
            for council in councils:
                normalized = self.normalizer.normalize_council_data(council.id)
                if normalized:
                    all_council_data.append(normalized)

            # Aggregate metrics
            for metric in STANDARDIZED_METRICS:
                canonical_name = metric.canonical_name
                values = []

                for council_data in all_council_data:
                    std_metrics = council_data.get('standardized_metrics', {})
                    if canonical_name in std_metrics:
                        value = std_metrics[canonical_name]['value']
                        if value is not None:
                            values.append(value)

                if values:
                    state_aggregation['metrics_coverage'][canonical_name] = len(values) / len(councils)
                    state_aggregation['averages'][canonical_name] = sum(values) / len(values)
                    state_aggregation['medians'][canonical_name] = sorted(values)[len(values) // 2]

                    # Find best and worst performers
                    if not metric.lower_is_better:
                        state_aggregation['best_performers'][canonical_name] = max(values)
                        state_aggregation['worst_performers'][canonical_name] = min(values)
                    else:
                        state_aggregation['best_performers'][canonical_name] = min(values)
                        state_aggregation['worst_performers'][canonical_name] = max(values)

            return state_aggregation

        finally:
            db.close()

    def generate_comparison_data(self, council_ids: List[int]) -> Dict[str, Any]:
        """Generate comparison data for multiple councils"""
        comparison = {
            'councils': [],
            'metrics': {},
            'ranking': {}
        }

        for council_id in council_ids:
            normalized = self.normalizer.normalize_council_data(council_id)
            if normalized:
                comparison['councils'].append(normalized)

        # Create comparison matrix
        for metric in STANDARDIZED_METRICS:
            canonical_name = metric.canonical_name
            comparison['metrics'][canonical_name] = {
                'display_name': metric.display_name,
                'unit': metric.unit,
                'lower_is_better': metric.lower_is_better,
                'values': {}
            }

            # Collect values from all councils
            values = []
            for council_data in comparison['councils']:
                std_metrics = council_data.get('standardized_metrics', {})
                if canonical_name in std_metrics:
                    value = std_metrics[canonical_name]['value']
                    council_id = council_data['council_id']
                    comparison['metrics'][canonical_name]['values'][council_id] = value
                    if value is not None:
                        values.append((council_id, value))

            # Calculate rankings
            if values:
                sorted_values = sorted(values, key=lambda x: x[1], reverse=not metric.lower_is_better)
                comparison['ranking'][canonical_name] = {
                    council_id: rank for rank, (council_id, _) in enumerate(sorted_values, 1)
                }

        return comparison

# Global instances
data_ingester = DataIngester()
data_normalizer = DataNormalizer()
data_aggregator = DataAggregator()