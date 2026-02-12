from sqlalchemy.orm import Session
from models import Council, User, Rating, ServiceScore, CouncilIndex, IssueReport, InfrastructureProject, FinancialData, PerformanceMetric, ElectionEvent, BusinessPermit, TourismAmenity, CouncilMetrics, CouncilUniqueData
from schemas import CouncilCreate, RatingCreate, UserCreate
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_service_scores(db: Session, council_id: int):
    return db.query(ServiceScore).filter(ServiceScore.council_id == council_id).all()

def get_councils_with_index(db: Session):
    return db.query(Council, CouncilIndex).filter(Council.id == CouncilIndex.council_id).all()

def get_councils(db: Session, skip: int = 0, limit: int = 100):
    return db.query(Council).offset(skip).limit(limit).all()

def get_council(db: Session, council_id: int):
    return db.query(Council).filter(Council.id == council_id).first()

def create_council(db: Session, council: CouncilCreate):
    db_council = Council(**council.dict())
    db.add(db_council)
    db.commit()
    db.refresh(db_council)
    return db_council

def get_ratings(db: Session, council_id: int, service_category: str = None, skip: int = 0, limit: int = 100):
    query = db.query(Rating).filter(Rating.council_id == council_id)
    if service_category:
        query = query.filter(Rating.service_category == service_category)
    return query.offset(skip).limit(limit).all()

def create_rating(db: Session, rating: RatingCreate):
    db_rating = Rating(**rating.dict())
    db.add(db_rating)
    db.commit()
    db.refresh(db_rating)
    return db_rating

# Issue Reports
def create_issue_report(db: Session, issue):
    db_issue = IssueReport(**issue.dict())
    db.add(db_issue)
    db.commit()
    db.refresh(db_issue)
    return db_issue

def get_issue_reports(db: Session, council_id: int, status: str = None, skip: int = 0, limit: int = 100):
    query = db.query(IssueReport).filter(IssueReport.council_id == council_id)
    if status:
        query = query.filter(IssueReport.status == status)
    return query.offset(skip).limit(limit).all()

# Infrastructure Projects
def get_infrastructure_projects(db: Session, council_id: int, status: str = None):
    query = db.query(InfrastructureProject).filter(InfrastructureProject.council_id == council_id)
    if status:
        query = query.filter(InfrastructureProject.status == status)
    return query.all()

# Financial Data
def get_financial_data(db: Session, council_id: int, year: int = None):
    query = db.query(FinancialData).filter(FinancialData.council_id == council_id)
    if year:
        query = query.filter(FinancialData.year == year)
    return query.all()

# Performance Metrics
def get_performance_metrics(db: Session, council_id: int, category: str = None, year: int = None):
    query = db.query(PerformanceMetric).filter(PerformanceMetric.council_id == council_id)
    if category:
        query = query.filter(PerformanceMetric.category == category)
    if year:
        query = query.filter(PerformanceMetric.year == year)
    return query.all()

# Election Events
def get_election_events(db: Session, council_id: int, upcoming_only: bool = True):
    from datetime import datetime
    query = db.query(ElectionEvent).filter(ElectionEvent.council_id == council_id)
    if upcoming_only:
        query = query.filter(ElectionEvent.event_date >= datetime.now())
    return query.order_by(ElectionEvent.event_date).all()

# Business Permits
def get_business_permits(db: Session, council_id: int, status: str = None):
    query = db.query(BusinessPermit).filter(BusinessPermit.council_id == council_id)
    if status:
        query = query.filter(BusinessPermit.status == status)
    return query.all()

# Tourism Amenities
def get_tourism_amenities(db: Session, council_id: int, category: str = None):
    query = db.query(TourismAmenity).filter(TourismAmenity.council_id == council_id)
    if category:
        query = query.filter(TourismAmenity.category == category)
    return query.all()

# Council Metrics (Standardized)
def get_council_metrics(db: Session, council_id: int, year: int = None):
    query = db.query(CouncilMetrics).filter(CouncilMetrics.council_id == council_id)
    if year:
        query = query.filter(CouncilMetrics.year == year)
    return query.all()

def create_council_metrics(db: Session, metrics_data: dict):
    db_metrics = CouncilMetrics(**metrics_data)
    db.add(db_metrics)
    db.commit()
    db.refresh(db_metrics)
    return db_metrics

# Council Unique Data
def get_council_unique_data(db: Session, council_id: int, data_type: str = None):
    query = db.query(CouncilUniqueData).filter(CouncilUniqueData.council_id == council_id)
    if data_type:
        query = query.filter(CouncilUniqueData.data_type == data_type)
    return query.all()

def create_council_unique_data(db: Session, unique_data: dict):
    db_unique = CouncilUniqueData(**unique_data)
    db.add(db_unique)
    db.commit()
    db.refresh(db_unique)
    return db_unique

# User functions
def get_user(db: Session, user_id: int = None, username: str = None, email: str = None):
    query = db.query(User)
    if user_id:
        query = query.filter(User.id == user_id)
    if username:
        query = query.filter(User.username == username)
    if email:
        query = query.filter(User.email == email)
    return query.first()

def create_user(db: Session, user: UserCreate):
    hashed_password = pwd_context.hash(user.password)
    db_user = User(
        email=user.email,
        username=user.username,
        password_hash=hashed_password
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

def authenticate_user(db: Session, username: str, password: str):
    user = get_user(db, username=username)
    if not user:
        return False
    if not verify_password(password, user.password_hash):
        return False
    return user