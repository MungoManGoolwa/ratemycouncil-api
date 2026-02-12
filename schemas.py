from pydantic import BaseModel
from typing import Optional

class UserBase(BaseModel):
    email: str
    username: str

class UserCreate(UserBase):
    password: str

class UserLogin(BaseModel):
    username: str
    password: str

class User(UserBase):
    id: int
    role: str
    is_verified: bool
    created_at: str

    class Config:
        from_attributes = True

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None

class CouncilBase(BaseModel):
    name: str
    state: str
    population: Optional[int] = None
    area_km2: Optional[float] = None
    peer_group: Optional[str] = None
    region_type: Optional[str] = None

class CouncilCreate(CouncilBase):
    pass

class Council(CouncilBase):
    id: int

    class Config:
        from_attributes = True

class RatingBase(BaseModel):
    council_id: int
    service_category: str
    rating: float
    postcode: str
    comment: Optional[str] = None

class RatingCreate(RatingBase):
    pass

class Rating(RatingBase):
    id: int
    moderation_status: str
    created_at: str

    class Config:
        from_attributes = True

class IssueReportBase(BaseModel):
    council_id: int
    category: str
    description: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    images: Optional[list] = None

class IssueReportCreate(IssueReportBase):
    pass

class IssueReport(IssueReportBase):
    id: int
    status: str
    priority: str
    resolution_time_days: Optional[int] = None
    created_at: str
    updated_at: str

    class Config:
        from_attributes = True

class InfrastructureProjectBase(BaseModel):
    council_id: int
    name: str
    category: str
    description: Optional[str] = None
    budget: Optional[float] = None
    start_date: Optional[str] = None
    completion_date: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

class InfrastructureProjectCreate(InfrastructureProjectBase):
    pass

class InfrastructureProject(InfrastructureProjectBase):
    id: int
    status: str
    created_at: str

    class Config:
        from_attributes = True

class FinancialDataBase(BaseModel):
    council_id: int
    year: int
    total_revenue: float
    total_expenditure: float
    rates_revenue: float
    grants_revenue: Optional[float] = None
    rate_capping_impact: Optional[float] = None
    value_for_money_score: Optional[float] = None

class FinancialDataCreate(FinancialDataBase):
    pass

class FinancialData(FinancialDataBase):
    id: int
    created_at: str

    class Config:
        from_attributes = True

class PerformanceMetricBase(BaseModel):
    council_id: int
    metric_name: str
    category: str
    value: float
    unit: str
    year: int
    quarter: Optional[int] = None

class PerformanceMetricCreate(PerformanceMetricBase):
    pass

class PerformanceMetric(PerformanceMetricBase):
    id: int
    created_at: str

    class Config:
        from_attributes = True

class ElectionEventBase(BaseModel):
    council_id: int
    event_type: str
    title: str
    description: Optional[str] = None
    event_date: str

class ElectionEventCreate(ElectionEventBase):
    pass

class ElectionEvent(ElectionEventBase):
    id: int
    status: str
    created_at: str

    class Config:
        from_attributes = True

class BusinessPermitBase(BaseModel):
    council_id: int
    permit_type: str
    application_date: str
    approval_date: Optional[str] = None
    processing_time_days: Optional[int] = None

class BusinessPermitCreate(BusinessPermitBase):
    pass

class BusinessPermit(BusinessPermitBase):
    id: int
    status: str
    created_at: str

    class Config:
        from_attributes = True

class TourismAmenityBase(BaseModel):
    council_id: int
    name: str
    category: str
    description: Optional[str] = None
    rating: Optional[float] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    accessibility_features: Optional[list] = None
    multilingual_support: bool = False

class TourismAmenityCreate(TourismAmenityBase):
    pass

class TourismAmenity(TourismAmenityBase):
    id: int
    created_at: str

    class Config:
        from_attributes = True