"""
Scoring Methodology for RateMyCouncil
Implements fair, normalized scoring with anti-gaming controls.
"""

import math
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from models import Rating, IssueReport, Council, CouncilMetrics

class ScoringEngine:
    """Core scoring engine with anti-gaming controls"""

    def __init__(self):
        self.weights = {
            'customer_satisfaction': 0.4,
            'service_delivery': 0.3,
            'value_for_rates': 0.2,
            'responsiveness': 0.1
        }

    def calculate_overall_score(self, council_id: int, db: Session) -> Dict[str, any]:
        """Calculate comprehensive council score"""

        # Get base metrics
        metrics = self._get_base_metrics(council_id, db)
        ratings = self._get_rating_metrics(council_id, db)
        issues = self._get_issue_metrics(council_id, db)

        # Calculate component scores
        customer_score = self._calculate_customer_satisfaction_score(ratings)
        service_score = self._calculate_service_delivery_score(metrics, ratings)
        value_score = self._calculate_value_for_rates_score(council_id, db)
        responsiveness_score = self._calculate_responsiveness_score(issues)

        # Weighted overall score
        overall_score = (
            customer_score['score'] * self.weights['customer_satisfaction'] +
            service_score['score'] * self.weights['service_delivery'] +
            value_score['score'] * self.weights['value_for_rates'] +
            responsiveness_score['score'] * self.weights['responsiveness']
        )

        # Calculate confidence
        confidence = self._calculate_confidence(ratings, issues)

        return {
            'overall_score': round(overall_score, 1),
            'components': {
                'customer_satisfaction': customer_score,
                'service_delivery': service_score,
                'value_for_rates': value_score,
                'responsiveness': responsiveness_score
            },
            'confidence': confidence,
            'sample_size': {
                'ratings': len(ratings),
                'issues': len(issues)
            },
            'last_updated': datetime.now()
        }

    def _get_base_metrics(self, council_id: int, db: Session) -> Dict:
        """Get council metrics from database"""
        metrics = db.query(CouncilMetrics).filter(
            CouncilMetrics.council_id == council_id
        ).order_by(CouncilMetrics.year.desc()).first()

        council = db.query(Council).filter(Council.id == council_id).first()

        return {
            'customer_satisfaction': metrics.customer_satisfaction if metrics else None,
            'service_delivery_score': metrics.service_delivery_score if metrics else None,
            'rates_revenue': metrics.rates_revenue if metrics else None,
            'total_revenue': metrics.total_revenue if metrics else None,
            'population': council.population if council else None
        }

    def _get_rating_metrics(self, council_id: int, db: Session) -> List[Dict]:
        """Get approved ratings with anti-gaming filters"""
        # Only include approved ratings from the last 2 years
        cutoff_date = datetime.now() - timedelta(days=730)

        ratings = db.query(Rating).filter(
            Rating.council_id == council_id,
            Rating.moderation_status == 'approved',
            Rating.created_at >= cutoff_date
        ).all()

        return [{
            'rating': r.rating,
            'service_category': r.service_category,
            'created_at': r.created_at,
            'user_id': r.user_id
        } for r in ratings]

    def _get_issue_metrics(self, council_id: int, db: Session) -> List[Dict]:
        """Get issue reports for responsiveness calculation"""
        issues = db.query(IssueReport).filter(
            IssueReport.council_id == council_id
        ).all()

        return [{
            'status': i.status,
            'created_at': i.created_at,
            'resolution_time_days': i.resolution_time_days,
            'priority': i.priority
        } for i in issues]

    def _calculate_customer_satisfaction_score(self, ratings: List[Dict]) -> Dict:
        """Calculate customer satisfaction score (0-100)"""
        if not ratings:
            return {'score': 50, 'confidence': 'low', 'reason': 'insufficient_data'}

        # Convert 1-5 star ratings to 0-100 scale
        scores = [r['rating'] * 20 for r in ratings]  # 1*20=20, 5*20=100

        # Apply anti-gaming: detect suspicious patterns
        filtered_scores = self._filter_suspicious_ratings(scores, ratings)

        if not filtered_scores:
            return {'score': 50, 'confidence': 'low', 'reason': 'filtered_data'}

        avg_score = sum(filtered_scores) / len(filtered_scores)

        return {
            'score': round(avg_score, 1),
            'confidence': self._get_confidence_level(len(filtered_scores)),
            'sample_size': len(filtered_scores)
        }

    def _calculate_service_delivery_score(self, metrics: Dict, ratings: List[Dict]) -> Dict:
        """Calculate service delivery score"""
        score = 0
        data_points = 0

        # Official metrics (higher weight)
        if metrics.get('service_delivery_score'):
            score += metrics['service_delivery_score'] * 0.7
            data_points += 1

        # User ratings by service category
        service_ratings = {}
        for r in ratings:
            cat = r['service_category']
            if cat not in service_ratings:
                service_ratings[cat] = []
            service_ratings[cat].append(r['rating'] * 20)  # Convert to 0-100

        if service_ratings:
            avg_service_rating = sum(
                sum(ratings_list) / len(ratings_list)
                for ratings_list in service_ratings.values()
            ) / len(service_ratings)

            score += avg_service_rating * 0.3
            data_points += 1

        if data_points == 0:
            return {'score': 50, 'confidence': 'low', 'reason': 'insufficient_data'}

        final_score = score / data_points

        return {
            'score': round(final_score, 1),
            'confidence': self._get_confidence_level(data_points),
            'data_sources': data_points
        }

    def _calculate_value_for_rates_score(self, council_id: int, db: Session) -> Dict:
        """Calculate value for rates score (satisfaction relative to rates burden)"""
        metrics = self._get_base_metrics(council_id, db)

        if not all([metrics.get('customer_satisfaction'), metrics.get('rates_revenue'), metrics.get('population')]):
            return {'score': 50, 'confidence': 'low', 'reason': 'insufficient_data'}

        # Rates per capita
        rates_per_capita = metrics['rates_revenue'] / metrics['population']

        # Normalize satisfaction score
        satisfaction = metrics['customer_satisfaction']

        # Calculate value score: higher satisfaction + lower rates = better value
        # Scale rates_per_capita (assuming $500-2000 range is typical)
        rates_factor = max(0, min(100, 100 - (rates_per_capita - 500) / 15))  # Normalize to 0-100

        value_score = (satisfaction + rates_factor) / 2

        return {
            'score': round(value_score, 1),
            'rates_per_capita': round(rates_per_capita, 2),
            'confidence': 'medium'
        }

    def _calculate_responsiveness_score(self, issues: List[Dict]) -> Dict:
        """Calculate responsiveness score based on issue resolution times"""
        if not issues:
            return {'score': 50, 'confidence': 'low', 'reason': 'insufficient_data'}

        resolved_issues = [i for i in issues if i['status'] == 'resolved' and i['resolution_time_days']]

        if not resolved_issues:
            return {'score': 50, 'confidence': 'low', 'reason': 'no_resolved_issues'}

        # Calculate average resolution time
        avg_resolution = sum(i['resolution_time_days'] for i in resolved_issues) / len(resolved_issues)

        # Score based on resolution time (faster = better)
        # Assuming 1 day = 100, 30 days = 50, 90+ days = 0
        if avg_resolution <= 1:
            score = 100
        elif avg_resolution <= 7:
            score = 90
        elif avg_resolution <= 14:
            score = 75
        elif avg_resolution <= 30:
            score = 50
        elif avg_resolution <= 60:
            score = 25
        else:
            score = 10

        return {
            'score': score,
            'avg_resolution_days': round(avg_resolution, 1),
            'resolved_issues': len(resolved_issues),
            'confidence': self._get_confidence_level(len(resolved_issues))
        }

    def _filter_suspicious_ratings(self, scores: List[float], ratings: List[Dict]) -> List[float]:
        """Apply anti-gaming filters to ratings"""
        if len(scores) < 3:
            return scores

        # Remove obvious outliers (more than 3 SD from mean)
        mean_score = sum(scores) / len(scores)
        variance = sum((x - mean_score) ** 2 for x in scores) / len(scores)
        std_dev = math.sqrt(variance)

        filtered = [s for s in scores if abs(s - mean_score) <= 3 * std_dev]

        # If we filtered too many, be more lenient
        if len(filtered) < len(scores) * 0.5:
            filtered = scores

        return filtered

    def _calculate_confidence(self, ratings: List[Dict], issues: List[Dict]) -> str:
        """Calculate overall confidence level"""
        rating_count = len(ratings)
        issue_count = len(issues)

        total_signals = rating_count + issue_count

        if total_signals >= 50:
            return 'high'
        elif total_signals >= 20:
            return 'medium'
        elif total_signals >= 5:
            return 'low'
        else:
            return 'very_low'

    def _get_confidence_level(self, sample_size: int) -> str:
        """Get confidence level based on sample size"""
        if sample_size >= 30:
            return 'high'
        elif sample_size >= 10:
            return 'medium'
        elif sample_size >= 3:
            return 'low'
        else:
            return 'very_low'

    def calculate_red_flag_index(self, council_id: int, db: Session) -> Dict:
        """Calculate red flag index based on complaint spikes"""
        # Get issues from last 90 days vs previous 90 days
        now = datetime.now()
        recent_cutoff = now - timedelta(days=90)
        previous_cutoff = now - timedelta(days=180)

        recent_issues = db.query(IssueReport).filter(
            IssueReport.council_id == council_id,
            IssueReport.created_at >= recent_cutoff
        ).count()

        previous_issues = db.query(IssueReport).filter(
            IssueReport.council_id == council_id,
            IssueReport.created_at >= previous_cutoff,
            IssueReport.created_at < recent_cutoff
        ).count()

        if previous_issues == 0:
            spike_ratio = recent_issues * 2  # Arbitrary multiplier if no previous data
        else:
            spike_ratio = recent_issues / previous_issues

        # Normalize to 0-100 scale (higher = more red flags)
        red_flag_score = min(100, spike_ratio * 25)  # 4x increase = 100

        return {
            'score': round(red_flag_score, 1),
            'recent_issues': recent_issues,
            'previous_issues': previous_issues,
            'spike_ratio': round(spike_ratio, 2),
            'confidence': 'medium' if (recent_issues + previous_issues) >= 10 else 'low'
        }