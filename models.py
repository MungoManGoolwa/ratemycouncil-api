from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Text, JSON
from sqlalchemy.orm import relationship
from database import Base
import datetime

class Council(Base):
    __tablename__ = "councils"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), index=True)
    state = Column(String(50))
    population = Column(Integer)
    area_km2 = Column(Float)
    peer_group = Column(String(100))
    region_type = Column(String(50))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    # Relationships
    ratings = relationship("Rating", back_populates="council")
    issues = relationship("IssueReport", back_populates="council")
    projects = relationship("InfrastructureProject", back_populates="council")
    financial = relationship("FinancialData", back_populates="council")
    performance = relationship("PerformanceMetric", back_populates="council")
    permits = relationship("BusinessPermit", back_populates="council")
    amenities = relationship("TourismAmenity", back_populates="council")
    indices = relationship("CouncilIndex", back_populates="council")
    metrics = relationship("CouncilMetrics", back_populates="council")
    unique_data = relationship("CouncilUniqueData", back_populates="council")
    verification_tokens = relationship("VerificationToken", back_populates="council")
    aggregated_metrics = relationship("AggregatedMetric", back_populates="council")

class Indicator(Base):
    __tablename__ = "indicators"

    id = Column(Integer, primary_key=True, index=True)
    canonical_name = Column(String(255), unique=True, index=True)
    service_category = Column(String(100))
    description = Column(Text)
    unit = Column(String(50))
    lower_is_better = Column(Boolean, default=False)

class CouncilIndicatorValue(Base):
    __tablename__ = "council_indicator_values"

    id = Column(Integer, primary_key=True, index=True)
    council_id = Column(Integer, ForeignKey("councils.id"))
    indicator_id = Column(Integer, ForeignKey("indicators.id"))
    year = Column(Integer)
    raw_value = Column(Float)
    normalised_value = Column(Float)
    percentile_rank = Column(Float)

    council = relationship("Council")
    indicator = relationship("Indicator")

class ServiceScore(Base):
    __tablename__ = "service_scores"

    id = Column(Integer, primary_key=True, index=True)
    council_id = Column(Integer, ForeignKey("councils.id"))
    service_category = Column(String(100))
    year = Column(Integer)
    score = Column(Float)

    council = relationship("Council")

class CouncilIndex(Base):
    __tablename__ = "council_index"

    id = Column(Integer, primary_key=True, index=True)
    council_id = Column(Integer, ForeignKey("councils.id"))
    year = Column(Integer)
    score = Column(Float)

    council = relationship("Council")

class Rating(Base):
    __tablename__ = "ratings"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)  # Allow anonymous ratings
    council_id = Column(Integer, ForeignKey("councils.id"))
    service_category = Column(String(100))
    rating = Column(Float)  # 1-5
    postcode = Column(String(10))
    comment = Column(Text, nullable=True)
    moderation_status = Column(String(20), default="pending")
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    # Relationships
    user = relationship("User", back_populates="ratings")
    council = relationship("Council", back_populates="ratings")

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), unique=True, index=True)
    username = Column(String(100), unique=True, index=True)
    password_hash = Column(String(255))
    role = Column(String(20), default="user")  # user, moderator, admin
    is_verified = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    # Relationships
    ratings = relationship("Rating", back_populates="user")

class IssueReport(Base):
    __tablename__ = "issue_reports"

    id = Column(Integer, primary_key=True, index=True)
    council_id = Column(Integer, ForeignKey("councils.id"))
    category = Column(String(100))  # potholes, waste, infrastructure, etc.
    description = Column(Text)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    images = Column(JSON, nullable=True)  # Array of image URLs
    status = Column(String(20), default="reported")  # reported, in_progress, resolved
    priority = Column(String(10), default="medium")  # low, medium, high
    resolution_time_days = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow)

    council = relationship("Council")

class InfrastructureProject(Base):
    __tablename__ = "infrastructure_projects"

    id = Column(Integer, primary_key=True, index=True)
    council_id = Column(Integer, ForeignKey("councils.id"))
    name = Column(String(255))
    category = Column(String(100))  # roads, parks, waste, water, etc.
    description = Column(Text)
    status = Column(String(20))  # planned, in_progress, completed
    budget = Column(Float, nullable=True)
    start_date = Column(DateTime, nullable=True)
    completion_date = Column(DateTime, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    council = relationship("Council")

class FinancialData(Base):
    __tablename__ = "financial_data"

    id = Column(Integer, primary_key=True, index=True)
    council_id = Column(Integer, ForeignKey("councils.id"))
    year = Column(Integer)
    total_revenue = Column(Float)
    total_expenditure = Column(Float)
    rates_revenue = Column(Float)
    grants_revenue = Column(Float)
    rate_capping_impact = Column(Float, nullable=True)
    value_for_money_score = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    council = relationship("Council")

class PerformanceMetric(Base):
    __tablename__ = "performance_metrics"

    id = Column(Integer, primary_key=True, index=True)
    council_id = Column(Integer, ForeignKey("councils.id"))
    metric_name = Column(String(100))  # response_time, approval_time, etc.
    category = Column(String(100))  # complaints, planning, waste, etc.
    value = Column(Float)
    unit = Column(String(50))  # days, percentage, etc.
    year = Column(Integer)
    quarter = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    council = relationship("Council")

class ElectionEvent(Base):
    __tablename__ = "election_events"

    id = Column(Integer, primary_key=True, index=True)
    council_id = Column(Integer, ForeignKey("councils.id"))
    event_type = Column(String(50))  # election, by-election, policy_change
    title = Column(String(255))
    description = Column(Text)
    event_date = Column(DateTime)
    status = Column(String(20), default="upcoming")  # upcoming, completed
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    council = relationship("Council")

class BusinessPermit(Base):
    __tablename__ = "business_permits"

    id = Column(Integer, primary_key=True, index=True)
    council_id = Column(Integer, ForeignKey("councils.id"))
    permit_type = Column(String(100))  # building, food, liquor, etc.
    application_date = Column(DateTime)
    approval_date = Column(DateTime, nullable=True)
    processing_time_days = Column(Integer, nullable=True)
    status = Column(String(20), default="pending")  # pending, approved, rejected
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    council = relationship("Council")

class TourismAmenity(Base):
    __tablename__ = "tourism_amenities"

    id = Column(Integer, primary_key=True, index=True)
    council_id = Column(Integer, ForeignKey("councils.id"))
    name = Column(String(255))
    category = Column(String(100))  # park, beach, museum, transport, etc.
    description = Column(Text)
    rating = Column(Float, nullable=True)  # Average user rating
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    accessibility_features = Column(JSON, nullable=True)
    multilingual_support = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    council = relationship("Council")

# New models for standardized council metrics
class CouncilMetrics(Base):
    """Standardized metrics available across most councils"""
    __tablename__ = "council_metrics"

    id = Column(Integer, primary_key=True, index=True)
    council_id = Column(Integer, ForeignKey("councils.id"), nullable=False)
    year = Column(Integer, nullable=False)
    
    # Common financial metrics (available in 90%+ councils)
    rates_revenue = Column(Float)  # Annual rates income
    total_revenue = Column(Float)  # Total council revenue
    total_expenditure = Column(Float)  # Total council expenditure
    operating_deficit = Column(Float)  # Annual deficit/surplus
    
    # Common service metrics
    population_served = Column(Integer)  # Population served
    area_km2 = Column(Float)  # Geographic area
    roads_maintained_km = Column(Float)  # Road network length
    
    # Performance indicators (where available)
    customer_satisfaction = Column(Float)  # Overall satisfaction score
    service_delivery_score = Column(Float)  # Service delivery rating
    
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    council = relationship("Council", back_populates="metrics")

class CouncilUniqueData(Base):
    """Unique data specific to individual councils (not for comparison)"""
    __tablename__ = "council_unique_data"

    id = Column(Integer, primary_key=True, index=True)
    council_id = Column(Integer, ForeignKey("councils.id"), nullable=False)
    
    # Unique metrics that vary by council/state
    data_type = Column(String(100), nullable=False)  # e.g., 'performance_framework', 'infrastructure_projects'
    data_key = Column(String(255), nullable=False)   # e.g., 'waste_recycling_rate', 'bike_paths_km'
    data_value = Column(Float)                  # Numeric value
    data_text = Column(Text)                    # Text description
    data_json = Column(Text)                    # JSON data for complex structures
    
    year = Column(Integer)
    source = Column(String(100))  # Data source (e.g., 'victoria_gov', 'qld_gov')
    
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    council = relationship("Council", back_populates="unique_data")

class DataSource(Base):
    """Data source provenance tracking"""
    __tablename__ = "data_sources"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, index=True)
    url = Column(String(500))
    last_retrieved = Column(DateTime)
    license_notes = Column(Text)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

class VerificationToken(Base):
    """Resident verification tokens"""
    __tablename__ = "verification_tokens"

    id = Column(Integer, primary_key=True, index=True)
    token_hash = Column(String(255), unique=True, index=True)  # Hashed token
    postcode = Column(String(10))
    council_id = Column(Integer, ForeignKey("councils.id"))
    token_type = Column(String(20), default="rates_notice")  # rates_notice, email, manual
    is_used = Column(Boolean, default=False)
    expires_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    council = relationship("Council")

class AggregatedMetric(Base):
    """Materialized aggregated metrics for performance"""
    __tablename__ = "aggregated_metrics"

    id = Column(Integer, primary_key=True, index=True)
    council_id = Column(Integer, ForeignKey("councils.id"))
    metric_type = Column(String(100))  # overall_score, customer_satisfaction, etc.
    time_bucket = Column(String(20))  # daily, weekly, monthly
    bucket_date = Column(DateTime)
    value = Column(Float)
    sample_size = Column(Integer)
    confidence = Column(String(20))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    council = relationship("Council")

class AuditLog(Base):
    """Audit log for moderation and admin actions"""
    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    action = Column(String(100))  # approve_rating, hide_issue, ban_user, etc.
    target_type = Column(String(50))  # rating, issue, user
    target_id = Column(Integer)
    details = Column(JSON)
    ip_address = Column(String(45))
    user_agent = Column(String(500))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    user = relationship("User")

class ServiceCategory(Base):
    """Service categories for ratings and issues"""
    __tablename__ = "service_categories"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True)
    display_name = Column(String(255))
    description = Column(Text)
    icon = Column(String(100))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

class IssueStatusUpdate(Base):
    """Status updates for issues"""
    __tablename__ = "issue_status_updates"

    id = Column(Integer, primary_key=True, index=True)
    issue_id = Column(Integer, ForeignKey("issue_reports.id"))
    old_status = Column(String(20))
    new_status = Column(String(20))
    updated_by = Column(Integer, ForeignKey("users.id"), nullable=True)  # Council staff
    notes = Column(Text)
    is_public = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    issue = relationship("IssueReport")
    updated_by_user = relationship("User")