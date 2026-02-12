"""
Comprehensive Council Metrics Framework
Standardizes metrics across all Australian councils for fair comparison
"""

from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum
import json
import logging

logger = logging.getLogger(__name__)

class MetricCategory(Enum):
    """Standard metric categories"""
    FINANCIAL = "financial"
    SERVICE_DELIVERY = "service_delivery"
    INFRASTRUCTURE = "infrastructure"
    ENVIRONMENTAL = "environmental"
    COMMUNITY = "community"
    ECONOMIC = "economic"

class DataSource(Enum):
    """Available data sources"""
    COUNCIL_REPORTS = "council_reports"
    STATE_GOVERNMENT = "state_government"
    FEDERAL_GOVERNMENT = "federal_government"
    ABS = "abs"
    ESTIMATED = "estimated"
    USER_REPORTED = "user_reported"

@dataclass
class StandardizedMetric:
    """Definition of a standardized metric"""
    canonical_name: str
    display_name: str
    category: MetricCategory
    description: str
    unit: str
    lower_is_better: bool = False
    expected_availability: float = 0.8  # Expected % of councils with this data
    calculation_method: Optional[str] = None  # How to calculate if not directly available
    alternative_sources: List[str] = None  # Alternative metric names to map from

    def __post_init__(self):
        if self.alternative_sources is None:
            self.alternative_sources = []

# Comprehensive metric definitions
STANDARDIZED_METRICS = [
    # Financial Metrics (High availability expected)
    StandardizedMetric(
        canonical_name="rates_revenue_per_capita",
        display_name="Rates Revenue per Capita",
        category=MetricCategory.FINANCIAL,
        description="Annual rates revenue divided by population served",
        unit="$ per person",
        expected_availability=0.95,
        calculation_method="rates_revenue / population_served",
        alternative_sources=["rates_revenue", "general_rates", "council_rates"]
    ),

    StandardizedMetric(
        canonical_name="total_revenue_per_capita",
        display_name="Total Revenue per Capita",
        category=MetricCategory.FINANCIAL,
        description="Total council revenue per capita",
        unit="$ per person",
        expected_availability=0.90,
        calculation_method="total_revenue / population_served"
    ),

    StandardizedMetric(
        canonical_name="operating_deficit_ratio",
        display_name="Operating Deficit Ratio",
        category=MetricCategory.FINANCIAL,
        description="Operating deficit as percentage of total revenue",
        unit="%",
        lower_is_better=True,
        expected_availability=0.85,
        calculation_method="(total_expenditure - total_revenue) / total_revenue * 100"
    ),

    # Service Delivery Metrics
    StandardizedMetric(
        canonical_name="complaint_response_time",
        display_name="Average Complaint Response Time",
        category=MetricCategory.SERVICE_DELIVERY,
        description="Average time to respond to citizen complaints",
        unit="days",
        lower_is_better=True,
        expected_availability=0.70,
        alternative_sources=["complaint_response_time", "service_response_time", "complaints_handling_time"]
    ),

    StandardizedMetric(
        canonical_name="waste_collection_efficiency",
        display_name="Waste Collection Efficiency",
        category=MetricCategory.SERVICE_DELIVERY,
        description="Percentage of scheduled waste collections completed on time",
        unit="%",
        expected_availability=0.75,
        alternative_sources=["waste_collection_efficiency", "kerbside_collection_rate", "waste_service_efficiency"]
    ),

    StandardizedMetric(
        canonical_name="planning_approval_time",
        display_name="Planning Application Approval Time",
        category=MetricCategory.SERVICE_DELIVERY,
        description="Average time for planning application approval",
        unit="days",
        lower_is_better=True,
        expected_availability=0.65,
        alternative_sources=["planning_approval_time", "dap_approval_time", "development_approval_time"]
    ),

    # Infrastructure Metrics
    StandardizedMetric(
        canonical_name="roads_maintained_per_capita",
        display_name="Roads Maintained per Capita",
        category=MetricCategory.INFRASTRUCTURE,
        description="Length of roads maintained per capita",
        unit="metres per person",
        expected_availability=0.80,
        calculation_method="roads_maintained_km * 1000 / population_served",
        alternative_sources=["roads_maintained_km", "sealed_roads_km", "maintained_roads"]
    ),

    StandardizedMetric(
        canonical_name="infrastructure_investment_ratio",
        display_name="Infrastructure Investment Ratio",
        category=MetricCategory.INFRASTRUCTURE,
        description="Infrastructure spending as percentage of total expenditure",
        unit="%",
        expected_availability=0.60,
        calculation_method="infrastructure_expenditure / total_expenditure * 100"
    ),

    # Environmental Metrics
    StandardizedMetric(
        canonical_name="waste_recycling_rate",
        display_name="Waste Recycling Rate",
        category=MetricCategory.ENVIRONMENTAL,
        description="Percentage of waste that is recycled or composted",
        unit="%",
        expected_availability=0.70,
        alternative_sources=["recycling_rate", "waste_diversion_rate", "resource_recovery_rate"]
    ),

    StandardizedMetric(
        canonical_name="carbon_emissions_reduction",
        display_name="Carbon Emissions Reduction",
        category=MetricCategory.ENVIRONMENTAL,
        description="Percentage reduction in carbon emissions since baseline",
        unit="%",
        expected_availability=0.50,
        alternative_sources=["emissions_reduction", "carbon_reduction", "ghg_reduction"]
    ),

    # Community Metrics
    StandardizedMetric(
        canonical_name="customer_satisfaction_score",
        display_name="Customer Satisfaction Score",
        category=MetricCategory.COMMUNITY,
        description="Overall customer satisfaction with council services",
        unit="score out of 100",
        expected_availability=0.55,
        alternative_sources=["customer_satisfaction", "resident_satisfaction", "service_satisfaction"]
    ),

    StandardizedMetric(
        canonical_name="community_engagement_rate",
        display_name="Community Engagement Rate",
        category=MetricCategory.COMMUNITY,
        description="Percentage of residents participating in council consultations",
        unit="%",
        expected_availability=0.40,
        alternative_sources=["engagement_rate", "participation_rate", "consultation_participation"]
    ),

    # Economic Metrics
    StandardizedMetric(
        canonical_name="business_permit_approval_time",
        display_name="Business Permit Approval Time",
        category=MetricCategory.ECONOMIC,
        description="Average time for business permit approval",
        unit="days",
        lower_is_better=True,
        expected_availability=0.60,
        alternative_sources=["permit_approval_time", "business_licence_time", "development_permit_time"]
    ),

    StandardizedMetric(
        canonical_name="local_employment_rate",
        display_name="Local Employment Rate",
        category=MetricCategory.ECONOMIC,
        description="Percentage of working age population in local employment",
        unit="%",
        expected_availability=0.45,
        alternative_sources=["employment_rate", "local_jobs_rate", "workforce_participation"]
    )
]

# State-specific metric mappings
STATE_METRIC_MAPPINGS = {
    "Victoria": {
        "complaint_response_time": ["complaints_resolution_time", "service_request_response"],
        "waste_collection_efficiency": ["waste_service_performance", "kerbside_service_level"],
        "planning_approval_time": ["planning_decision_time", "development_assessment_time"],
        "waste_recycling_rate": ["resource_recovery_rate", "diversion_rate"],
        "customer_satisfaction_score": ["resident_satisfaction", "community_satisfaction"]
    },
    "NSW": {
        "complaint_response_time": ["complaints_handling_time", "customer_service_time"],
        "waste_collection_efficiency": ["waste_collection_performance", "domestic_waste_service"],
        "planning_approval_time": ["development_consent_time", "da_processing_time"],
        "waste_recycling_rate": ["recycling_diversion_rate", "waste_diversion"],
        "customer_satisfaction_score": ["resident_survey_score", "customer_feedback_score"]
    },
    "Queensland": {
        "complaint_response_time": ["complaints_response_time", "enquiry_resolution_time"],
        "waste_collection_efficiency": ["waste_management_performance", "collection_service_level"],
        "planning_approval_time": ["development_approval_time", "planning_scheme_time"],
        "waste_recycling_rate": ["material_recovery_rate", "recycling_performance"],
        "customer_satisfaction_score": ["community_satisfaction", "resident_experience_score"]
    },
    "WA": {
        "complaint_response_time": ["complaints_management_time", "service_response_time"],
        "waste_collection_efficiency": ["waste_service_delivery", "collection_efficiency"],
        "planning_approval_time": ["development_assessment_time", "planning_approval_period"],
        "waste_recycling_rate": ["recycling_rate", "waste_recovery_rate"],
        "customer_satisfaction_score": ["customer_satisfaction", "resident_satisfaction"]
    },
    "SA": {
        "complaint_response_time": ["complaints_response_time", "customer_complaints_time"],
        "waste_collection_efficiency": ["waste_collection_service", "domestic_waste_efficiency"],
        "planning_approval_time": ["development_approval_time", "planning_consent_time"],
        "waste_recycling_rate": ["recycling_performance", "waste_diversion_rate"],
        "customer_satisfaction_score": ["community_satisfaction", "resident_satisfaction"]
    },
    "Tasmania": {
        "complaint_response_time": ["complaints_handling", "service_complaints_time"],
        "waste_collection_efficiency": ["waste_service_performance", "collection_service"],
        "planning_approval_time": ["planning_approval_time", "development_approval"],
        "waste_recycling_rate": ["recycling_rate", "resource_recovery"],
        "customer_satisfaction_score": ["customer_satisfaction", "community_feedback"]
    },
    "NT": {
        "complaint_response_time": ["complaints_response", "service_request_time"],
        "waste_collection_efficiency": ["waste_collection", "waste_management_service"],
        "planning_approval_time": ["development_approval", "planning_permission_time"],
        "waste_recycling_rate": ["recycling_rate", "waste_recycling"],
        "customer_satisfaction_score": ["resident_satisfaction", "customer_service_score"]
    },
    "ACT": {
        "complaint_response_time": ["complaints_resolution", "service_response"],
        "waste_collection_efficiency": ["waste_collection_efficiency", "waste_service"],
        "planning_approval_time": ["development_approval", "da_approval_time"],
        "waste_recycling_rate": ["recycling_rate", "waste_diversion"],
        "customer_satisfaction_score": ["resident_satisfaction", "community_satisfaction"]
    }
}

class MetricNormalizer:
    """Handles metric normalization and standardization"""

    def __init__(self):
        self.metrics_by_name = {m.canonical_name: m for m in STANDARDIZED_METRICS}
        self.metrics_by_category = {}
        for metric in STANDARDIZED_METRICS:
            if metric.category not in self.metrics_by_category:
                self.metrics_by_category[metric.category] = []
            self.metrics_by_category[metric.category].append(metric)

    def get_metric_definition(self, canonical_name: str) -> Optional[StandardizedMetric]:
        """Get metric definition by canonical name"""
        return self.metrics_by_name.get(canonical_name)

    def find_matching_metric(self, raw_name: str, state: str = None) -> Optional[Tuple[str, StandardizedMetric]]:
        """Find standardized metric that matches raw metric name"""
        raw_name_lower = raw_name.lower().replace('_', ' ').replace('-', ' ')

        # Direct match
        if raw_name in self.metrics_by_name:
            return raw_name, self.metrics_by_name[raw_name]

        # Check alternative sources
        for canonical_name, metric in self.metrics_by_name.items():
            for alt_source in metric.alternative_sources:
                if alt_source.lower().replace('_', ' ').replace('-', ' ') == raw_name_lower:
                    return canonical_name, metric

        # Check state-specific mappings
        if state and state in STATE_METRIC_MAPPINGS:
            state_mappings = STATE_METRIC_MAPPINGS[state]
            for std_name, alternatives in state_mappings.items():
                for alt in alternatives:
                    if alt.lower().replace('_', ' ').replace('-', ' ') == raw_name_lower:
                        return std_name, self.metrics_by_name[std_name]

        return None

    def normalize_value(self, raw_value: float, metric_name: str, council_data: Dict = None) -> Optional[float]:
        """Normalize a raw metric value to standardized format"""
        metric = self.get_metric_definition(metric_name)
        if not metric:
            return raw_value

        # Apply calculation method if needed
        if metric.calculation_method and council_data:
            try:
                # Simple expression evaluator for calculation methods
                calc_expr = metric.calculation_method
                for key, value in council_data.items():
                    if value is not None:
                        calc_expr = calc_expr.replace(key, str(value))

                # Evaluate the expression (basic arithmetic only)
                normalized_value = eval(calc_expr, {"__builtins__": {}})
                return normalized_value
            except:
                logger.warning(f"Failed to calculate {metric_name} using {metric.calculation_method}")

        return raw_value

    def get_missing_metrics_for_council(self, council_id: int, available_metrics: List[str]) -> List[str]:
        """Identify which standardized metrics are missing for a council"""
        missing = []
        for metric in STANDARDIZED_METRICS:
            if metric.canonical_name not in available_metrics:
                missing.append(metric.canonical_name)
        return missing

    def estimate_missing_metric(self, metric_name: str, council_data: Dict, peer_councils_data: List[Dict]) -> Optional[float]:
        """Estimate a missing metric using peer council data and correlations"""
        metric = self.get_metric_definition(metric_name)
        if not metric:
            return None

        # Simple estimation based on population scaling
        if "per_capita" in metric_name and "population_served" in council_data:
            # Find similar councils by population
            population = council_data.get("population_served")
            if population and peer_councils_data:
                similar_councils = [
                    peer for peer in peer_councils_data
                    if abs(peer.get("population_served", 0) - population) / population < 0.5
                ]

                if similar_councils:
                    values = [c.get(metric_name) for c in similar_councils if c.get(metric_name) is not None]
                    if values:
                        return sum(values) / len(values)

        # Default estimation methods could be added here
        return None

# Global normalizer instance
metric_normalizer = MetricNormalizer()

def get_comprehensive_metrics_framework():
    """Get the complete metrics framework"""
    return {
        "standardized_metrics": STANDARDIZED_METRICS,
        "state_mappings": STATE_METRIC_MAPPINGS,
        "normalizer": metric_normalizer,
        "categories": [cat.value for cat in MetricCategory]
    }