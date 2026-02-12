from typing import Optional, Dict, List
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from database import get_db, engine
from models import (
    Base, Council, User, IssueReport, InfrastructureProject, FinancialData,
    PerformanceMetric, ElectionEvent, BusinessPermit, TourismAmenity,
    CouncilMetrics, CouncilUniqueData
)
from crud import (
    get_councils, get_council, create_rating, get_ratings, get_service_scores,
    get_councils_with_index, create_issue_report, get_issue_reports,
    get_infrastructure_projects, get_financial_data, get_performance_metrics,
    get_election_events, get_business_permits, get_tourism_amenities,
    get_council_metrics, create_council_metrics, get_council_unique_data, create_council_unique_data,
    create_user, authenticate_user, get_user
)
from schemas import Council, RatingCreate, IssueReportCreate, InfrastructureProjectCreate, UserCreate, UserLogin, Token
# from data_sources import DATA_SOURCES  # Lazy load to avoid heavy imports
from metrics_framework import MetricCategory
import jwt
from datetime import datetime, timedelta
from passlib.context import CryptContext

# Authentication
SECRET_KEY = "your-secret-key-here"  # In production, use environment variable
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def get_metric_source(council_id: int, metric_name: str, db) -> Dict[str, str]:
    """Get source information for a metric"""
    # Map metric names to source keys
    metric_source_map = {
        'customer_satisfaction_score': 'customer_satisfaction',
        'rates_revenue_per_capita': 'rates_revenue',
        'total_revenue_per_capita': 'total_revenue',
        'complaint_response_time': 'complaint_response_time',
        'waste_collection_efficiency': 'waste_collection_efficiency',
        'planning_approval_time': 'planning_approval_time',
        'waste_recycling_rate': 'waste_recycling_rate',
        'business_permit_approval_time': 'business_permit_approval_time'
    }
    
    source_key = metric_source_map.get(metric_name, metric_name)
    
    # Try to find source attribution in CouncilUniqueData
    attribution = db.query(CouncilUniqueData).filter(
        CouncilUniqueData.council_id == council_id,
        CouncilUniqueData.data_type == 'source_attribution',
        CouncilUniqueData.data_key == source_key
    ).first()
    
    if attribution:
        return {
            "name": attribution.source,
            "description": attribution.data_text,
            "last_updated": attribution.year
        }
    
    # Fallback to default sources based on metric type
    default_sources = {
        'customer_satisfaction_score': {
            "name": "Community Survey",
            "description": "Annual community satisfaction survey conducted by local council",
            "last_updated": 2023
        },
        'rates_revenue_per_capita': {
            "name": "Annual Financial Report",
            "description": "Council annual financial statements and reports",
            "last_updated": 2023
        },
        'population_served': {
            "name": "Australian Bureau of Statistics",
            "description": "ABS Census data and population estimates",
            "last_updated": 2021
        }
    }
    
    return default_sources.get(metric_name, {
        "name": "Council Reports",
        "description": "Local council annual reports and performance data",
        "last_updated": 2023
    })

import jwt
from datetime import datetime, timedelta
from passlib.context import CryptContext

def get_default_source_for_metric(metric_name: str) -> str:
    """Get default data source description for a metric"""
    source_map = {
        'customer_satisfaction_score': 'Community satisfaction surveys conducted by local councils',
        'rates_revenue_per_capita': 'Annual financial reports and council budgets',
        'total_revenue_per_capita': 'Annual financial reports and council budgets',
        'operating_deficit_ratio': 'Annual financial reports and council budgets',
        'complaint_response_time': 'Council service delivery reports and performance frameworks',
        'waste_collection_efficiency': 'Environmental reports and waste management data',
        'planning_approval_time': 'Planning and development reports',
        'roads_maintained_per_capita': 'Infrastructure reports and asset management data',
        'waste_recycling_rate': 'Environmental reports and recycling programs data',
        'population_served': 'Australian Bureau of Statistics census data'
    }
    return source_map.get(metric_name, 'Local council annual reports and performance data')

def get_metric_source(council_id: int, metric_name: str, db: Session) -> dict:
    """Get source attribution for a specific metric"""
    # Try to find source attribution in CouncilUniqueData
    source_key = f"source_{metric_name}"
    attribution = db.query(CouncilUniqueData).filter(
        CouncilUniqueData.council_id == council_id,
        CouncilUniqueData.data_type == 'source_attribution',
        CouncilUniqueData.data_key == source_key
    ).first()
    
    if attribution:
        return {
            "name": attribution.source,
            "description": attribution.data_text,
            "last_updated": attribution.year
        }
    
    # Fallback to default sources based on metric type
    default_sources = {
        'customer_satisfaction_score': {
            "name": "Community Survey",
            "description": "Annual community satisfaction survey conducted by local council",
            "last_updated": 2023
        },
        'rates_revenue_per_capita': {
            "name": "Annual Financial Report",
            "description": "Council annual financial statements and reports",
            "last_updated": 2023
        },
        'population_served': {
            "name": "Australian Bureau of Statistics",
            "description": "ABS Census data and population estimates",
            "last_updated": 2021
        },
        'complaint_response_time': {
            "name": "Council Performance Reports",
            "description": "Local council service delivery and performance reports",
            "last_updated": 2023
        },
        'waste_collection_efficiency': {
            "name": "Environmental Reports",
            "description": "Council environmental and waste management reports",
            "last_updated": 2023
        }
    }
    
    return default_sources.get(metric_name, {
        "name": "Council Reports",
        "description": "Local council annual reports and performance data",
        "last_updated": 2023
    })

# Create tables
Base.metadata.create_all(bind=engine)

app = FastAPI(title="RateMyCouncil API", version="1.0.0")

# CORS middleware for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Authentication utilities
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(token: Optional[str] = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    if not token:
        return None
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except jwt.PyJWTError:
        raise credentials_exception
    user = get_user(db, username=username)
    if user is None:
        raise credentials_exception
    return user

@app.get("/")
async def root():
    return {"message": "Welcome to RateMyCouncil API"}

# Authentication endpoints
@app.post("/register", response_model=Token)
async def register_user(user: UserCreate, db: Session = Depends(get_db)):
    db_user = get_user(db, email=user.email)
    if db_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    db_user = get_user(db, username=user.username)
    if db_user:
        raise HTTPException(status_code=400, detail="Username already registered")
    
    db_user = create_user(db, user)
    access_token = create_access_token(data={"sub": db_user.username})
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = authenticate_user(db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/users/me")
async def read_users_me(current_user: User = Depends(get_current_user)):
    return {"username": current_user.username, "email": current_user.email, "role": current_user.role}

@app.get("/councils", response_model=List[Council])
async def read_councils(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    councils = get_councils(db, skip=skip, limit=limit)
    return councils

@app.get("/councils/{council_id}", response_model=Council)
async def read_council(council_id: int, db: Session = Depends(get_db)):
    council = get_council(db, council_id)
    if council is None:
        raise HTTPException(status_code=404, detail="Council not found")
    return council

@app.post("/ratings")
async def submit_rating(rating: RatingCreate, current_user: Optional[User] = Depends(get_current_user), db: Session = Depends(get_db)):
    rating_data = rating.dict()
    if current_user:
        rating_data["user_id"] = current_user.id
    return create_rating(db, rating_data)

@app.get("/councils/{council_id}/ratings")
async def read_ratings(council_id: int, service_category: str = None, skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return get_ratings(db, council_id, service_category, skip, limit)

@app.get("/councils/{council_id}/scores")
async def read_service_scores(council_id: int, db: Session = Depends(get_db)):
    return get_service_scores(db, council_id)

@app.get("/rankings")
async def read_rankings(db: Session = Depends(get_db)):
    results = get_councils_with_index(db)
    # Sort by score descending
    sorted_results = sorted(results, key=lambda x: x[1].score, reverse=True)
    # Format for frontend
    rankings = [{"council": council, "index": {"score": index.score}} for council, index in sorted_results]
    return rankings

# Issue Reports
@app.post("/issues")
async def submit_issue(issue: IssueReportCreate, db: Session = Depends(get_db)):
    return create_issue_report(db, issue)

@app.get("/councils/{council_id}/issues")
async def read_issues(council_id: int, status: str = None, skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return get_issue_reports(db, council_id, status, skip, limit)

# Infrastructure Projects
@app.get("/councils/{council_id}/projects")
async def read_projects(council_id: int, status: str = None, db: Session = Depends(get_db)):
    return get_infrastructure_projects(db, council_id, status)

# Financial Data
@app.get("/councils/{council_id}/financial")
async def read_financial_data(council_id: int, year: int = None, db: Session = Depends(get_db)):
    return get_financial_data(db, council_id, year)

# Performance Metrics
@app.get("/councils/{council_id}/performance")
async def read_performance_metrics(council_id: int, category: str = None, year: int = None, db: Session = Depends(get_db)):
    return get_performance_metrics(db, council_id, category, year)

# Election Events
@app.get("/councils/{council_id}/elections")
async def read_election_events(council_id: int, upcoming_only: bool = True, db: Session = Depends(get_db)):
    return get_election_events(db, council_id, upcoming_only)

# Business Permits
@app.get("/councils/{council_id}/permits")
async def read_business_permits(council_id: int, status: str = None, db: Session = Depends(get_db)):
    return get_business_permits(db, council_id, status)

# Tourism Amenities
@app.get("/councils/{council_id}/amenities")
async def read_tourism_amenities(council_id: int, category: str = None, db: Session = Depends(get_db)):
    return get_tourism_amenities(db, council_id, category)

# News Articles
@app.get("/councils/{council_id}/news")
async def read_council_news(council_id: int, db: Session = Depends(get_db)):
    from data_sources import DATA_SOURCES  # Lazy import
    council = get_council(db, council_id)
    if council is None:
        raise HTTPException(status_code=404, detail="Council not found")
    
    news_source = DATA_SOURCES['news_api']
    news_data = news_source.fetch_council_news(council.name)
    return news_data or {"articles": [], "total_results": 0}

# Council Metrics (Standardized)
@app.get("/councils/{council_id}/metrics")
async def read_council_metrics(council_id: int, year: int = None, db: Session = Depends(get_db)):
    return get_council_metrics(db, council_id, year)

# Council Unique Data
@app.get("/councils/{council_id}/unique-data")
async def read_council_unique_data(council_id: int, data_type: str = None, db: Session = Depends(get_db)):
    return get_council_unique_data(db, council_id, data_type)

# Comprehensive Normalized Data Endpoints
# from data_ingestion import data_normalizer, data_aggregator  # Lazy load
from metrics_framework import STANDARDIZED_METRICS, metric_normalizer

@app.get("/councils/{council_id}/normalized-metrics")
async def get_normalized_metrics(council_id: int):
    """Get comprehensive normalized metrics for a council"""
    try:
        from database import SessionLocal
        from models import Council, CouncilMetrics, PerformanceMetric, CouncilUniqueData

        db = SessionLocal()
        try:
            # Get council info
            council = db.query(Council).filter(Council.id == council_id).first()
            if not council:
                raise HTTPException(status_code=404, detail="Council not found")

            # Get stored metrics
            council_metrics = db.query(CouncilMetrics).filter(CouncilMetrics.council_id == council_id).first()
            performance_metrics = db.query(PerformanceMetric).filter(PerformanceMetric.council_id == council_id).all()
            unique_data = db.query(CouncilUniqueData).filter(CouncilUniqueData.council_id == council_id).all()

            # Build standardized metrics from stored data
            standardized_metrics = {}

            if council_metrics:
                # Map stored metrics to standardized format
                metric_mappings = {
                    'customer_satisfaction_score': council_metrics.customer_satisfaction,
                    'service_delivery_score': council_metrics.service_delivery_score,
                    'rates_revenue_per_capita': council_metrics.rates_revenue / council.population if council.population else None,
                    'total_revenue_per_capita': council_metrics.total_revenue / council.population if council.population else None,
                    'roads_maintained_per_capita': (council_metrics.roads_maintained_km * 1000) / council.population if council.population else None,
                }

                # Add direct mappings
                if council_metrics.customer_satisfaction is not None:
                    source_info = get_metric_source(council.id, 'customer_satisfaction_score', db)
                    standardized_metrics['customer_satisfaction_score'] = {
                        'value': round(council_metrics.customer_satisfaction, 1),
                        'raw_value': council_metrics.customer_satisfaction,
                        'source': source_info,
                        'confidence': 'high'
                    }

                if council_metrics.service_delivery_score is not None:
                    source_info = get_metric_source(council.id, 'service_delivery_score', db)
                    standardized_metrics['service_delivery_score'] = {
                        'value': round(council_metrics.service_delivery_score, 1),
                        'raw_value': council_metrics.service_delivery_score,
                        'source': source_info,
                        'confidence': 'high'
                    }

                # Calculate derived metrics
                if council_metrics.rates_revenue is not None and council.population:
                    standardized_metrics['rates_revenue_per_capita'] = {
                        'value': council_metrics.rates_revenue / council.population,
                        'raw_value': council_metrics.rates_revenue,
                        'source': 'calculated',
                        'confidence': 'high'
                    }

                if council_metrics.total_revenue is not None and council.population:
                    standardized_metrics['total_revenue_per_capita'] = {
                        'value': council_metrics.total_revenue / council.population,
                        'raw_value': council_metrics.total_revenue,
                        'source': 'calculated',
                        'confidence': 'high'
                    }

                if council_metrics.roads_maintained_km is not None and council.population:
                    standardized_metrics['roads_maintained_per_capita'] = {
                        'value': (council_metrics.roads_maintained_km * 1000) / council.population,
                        'raw_value': council_metrics.roads_maintained_km,
                        'source': 'calculated',
                        'confidence': 'high'
                    }

                # Calculate operating deficit ratio
                if council_metrics.total_revenue is not None and council_metrics.total_expenditure is not None and council_metrics.total_revenue > 0:
                    deficit_ratio = ((council_metrics.total_expenditure - council_metrics.total_revenue) / council_metrics.total_revenue) * 100
                    standardized_metrics['operating_deficit_ratio'] = {
                        'value': deficit_ratio,
                        'raw_value': deficit_ratio,
                        'source': 'calculated',
                        'confidence': 'high'
                    }

            # Add performance metrics
            for pm in performance_metrics:
                source_info = get_metric_source(council.id, pm.metric_name, db)
                standardized_metrics[pm.metric_name] = {
                    'value': pm.value,
                    'raw_value': pm.value,
                    'source': source_info,
                    'confidence': 'high'
                }

            # Build unique data
            unique_data_dict = {}
            for ud in unique_data:
                unique_data_dict[f"{ud.data_type}_{ud.data_key}"] = {
                    'value': ud.data_value,
                    'text': ud.data_text,
                    'source': ud.source,
                    'year': ud.year
                }

            return {
                "council_info": {
                    'id': council.id,
                    'name': council.name,
                    'state': council.state,
                    'population': council.population,
                    'area_km2': council.area_km2
                },
                "standardized_metrics": standardized_metrics,
                "unique_data": unique_data_dict,
                "coverage_score": len(standardized_metrics) / len(STANDARDIZED_METRICS) if STANDARDIZED_METRICS else 0,
                "data_sources": ["stored_data"],
                "attribution": {
                    "primary_source": "RateMyCouncil Database",
                    "data_collection_method": "Aggregated from Australian state government reports, council annual reports, and standardized metrics framework",
                    "last_updated": "2024",
                    "disclaimer": "Data is normalized and may not reflect real-time council performance. Always verify with official sources."
                }
            }

        finally:
            db.close()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving data: {str(e)}")

@app.get("/metrics/comparison")
async def compare_councils(council_ids: str, metrics: str = None):
    """Compare multiple councils on selected metrics"""
    try:
        from data_ingestion import data_aggregator  # Lazy import
        ids = [int(x.strip()) for x in council_ids.split(",") if x.strip()]
        if len(ids) < 2 or len(ids) > 10:
            raise HTTPException(status_code=400, detail="Must compare 2-10 councils")

        # Parse requested metrics
        requested_metrics = None
        if metrics:
            requested_metrics = [m.strip() for m in metrics.split(",") if m.strip()]

        comparison = data_aggregator.generate_comparison_data(ids)

        # Filter to requested metrics if specified
        if requested_metrics:
            filtered_metrics = {}
            for metric_name in requested_metrics:
                if metric_name in comparison["metrics"]:
                    filtered_metrics[metric_name] = comparison["metrics"][metric_name]
            comparison["metrics"] = filtered_metrics

        return comparison

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid council IDs")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating comparison: {str(e)}")

@app.get("/metrics/state/{state}")
async def get_state_metrics(state: str):
    """Get aggregated metrics for all councils in a state"""
    try:
        from data_ingestion import data_aggregator  # Lazy import
        state_data = data_aggregator.aggregate_state_data(state)
        return state_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error aggregating state data: {str(e)}")

@app.get("/metrics/definitions")
async def get_metrics_definitions():
    """Get definitions of all standardized metrics"""
    return {
        "metrics": [
            {
                "canonical_name": m.canonical_name,
                "display_name": m.display_name,
                "category": m.category.value,
                "description": m.description,
                "unit": m.unit,
                "lower_is_better": m.lower_is_better,
                "expected_availability": m.expected_availability,
                "primary_data_source": get_default_source_for_metric(m.canonical_name)
            }
            for m in STANDARDIZED_METRICS
        ],
        "categories": [cat.value for cat in MetricCategory],
        "data_sources": {
            "description": "Metrics are sourced from Australian state government reports, council annual reports, and standardized data collection frameworks",
            "last_updated": "2024",
            "methodology": "Data normalization and standardization applied for fair comparison across councils"
        }
    }

@app.get("/metrics/top-performers")
async def get_top_performers(metric: str, limit: int = 10, state: str = None):
    """Get top performing councils for a specific metric"""
    try:
        from database import SessionLocal
        from models import Council, CouncilMetrics, PerformanceMetric

        db = SessionLocal()
        try:
            # Get metric definition
            metric_def = metric_normalizer.get_metric_definition(metric)
            if not metric_def:
                raise HTTPException(status_code=404, detail="Metric not found")

            # Get all councils (or filter by state)
            query = db.query(Council)
            if state:
                query = query.filter(Council.state == state)

            councils = query.all()

            # Get metric values from stored data instead of real-time computation
            performers = []
            for council in councils:
                value = None
                unit = metric_def.unit

                # Check if metric is in CouncilMetrics table
                if hasattr(CouncilMetrics, metric.replace('_score', '').replace('_rate', '').replace('_time', '').replace('_ratio', '').replace('_per_capita', '')):
                    # Map canonical names to column names
                    column_map = {
                        'customer_satisfaction_score': 'customer_satisfaction',
                        'service_delivery_score': 'service_delivery_score',
                        'rates_revenue_per_capita': 'rates_revenue',  # Will need to divide by population
                        'total_revenue_per_capita': 'total_revenue',  # Will need to divide by population
                        'operating_deficit_ratio': None,  # Calculated field
                        'roads_maintained_per_capita': 'roads_maintained_km',  # Will need to calculate
                    }

                    column_name = column_map.get(metric, metric.replace('_score', '').replace('_rate', '').replace('_time', '').replace('_ratio', '').replace('_per_capita', ''))

                    if column_name and hasattr(CouncilMetrics, column_name):
                        metrics_record = db.query(CouncilMetrics).filter(
                            CouncilMetrics.council_id == council.id
                        ).first()

                        if metrics_record:
                            raw_value = getattr(metrics_record, column_name)
                            if raw_value is not None:
                                # Handle calculated metrics
                                if metric == 'rates_revenue_per_capita' and council.population:
                                    value = raw_value / council.population
                                elif metric == 'total_revenue_per_capita' and council.population:
                                    value = raw_value / council.population
                                elif metric == 'roads_maintained_per_capita' and council.population:
                                    value = (raw_value * 1000) / council.population  # Convert km to meters
                                elif metric == 'operating_deficit_ratio':
                                    total_rev = getattr(metrics_record, 'total_revenue')
                                    total_exp = getattr(metrics_record, 'total_expenditure')
                                    if total_rev and total_rev > 0:
                                        value = ((total_exp - total_rev) / total_rev) * 100
                                else:
                                    value = raw_value

                # If not found in CouncilMetrics, check PerformanceMetric table
                if value is None:
                    perf_metric = db.query(PerformanceMetric).filter(
                        PerformanceMetric.council_id == council.id,
                        PerformanceMetric.metric_name == metric
                    ).first()

                    if perf_metric:
                        value = perf_metric.value
                        unit = perf_metric.unit or metric_def.unit

                # If we have a value, add to performers
                if value is not None:
                    # Get source information
                    source_info = get_metric_source(council.id, metric, db)
                    
                    performers.append({
                        "council_id": council.id,
                        "council_name": council.name,
                        "state": council.state,
                        "value": round(value, 1) if metric in ['customer_satisfaction_score', 'service_delivery_score'] else value,
                        "unit": unit,
                        "source": source_info
                    })

            # Sort by value (higher is better unless lower_is_better is True)
            reverse_sort = not metric_def.lower_is_better
            performers.sort(key=lambda x: x["value"], reverse=reverse_sort)

            return {
                "metric": {
                    "canonical_name": metric,
                    "display_name": metric_def.display_name,
                    "unit": metric_def.unit,
                    "lower_is_better": metric_def.lower_is_better
                },
                "top_performers": performers[:limit],
                "total_councils": len(performers),
                "data_source": {
                    "name": "RateMyCouncil Database",
                    "description": "Pre-computed normalized metrics from multiple Australian council data sources",
                    "last_updated": "2024",
                    "methodology": "Data aggregated from state government reports, council annual reports, and standardized metrics framework"
                }
            }

        finally:
            db.close()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting top performers: {str(e)}")

@app.get("/metrics/benchmark/{council_id}")
async def benchmark_council(council_id: int):
    """Benchmark a council against state and national averages"""
    try:
        from database import SessionLocal
        from models import Council, CouncilMetrics, PerformanceMetric

        db = SessionLocal()
        try:
            # Get council info
            council = db.query(Council).filter(Council.id == council_id).first()
            if not council:
                raise HTTPException(status_code=404, detail="Council not found")

            # Get stored metrics for this council
            council_metrics = db.query(CouncilMetrics).filter(CouncilMetrics.council_id == council_id).first()
            council_performance = db.query(PerformanceMetric).filter(PerformanceMetric.council_id == council_id).all()

            if not council_metrics:
                raise HTTPException(status_code=404, detail="No metrics found for council")

            # Get all councils in the same state
            state_councils = db.query(Council).filter(Council.state == council.state).all()

            # Get stored metrics for all state councils
            state_metrics_data = {}
            for sc in state_councils:
                sc_metrics = db.query(CouncilMetrics).filter(CouncilMetrics.council_id == sc.id).first()
                sc_performance = db.query(PerformanceMetric).filter(PerformanceMetric.council_id == sc.id).all()

                if sc_metrics:
                    # Add standardized metrics
                    if sc_metrics.customer_satisfaction is not None:
                        if 'customer_satisfaction_score' not in state_metrics_data:
                            state_metrics_data['customer_satisfaction_score'] = []
                        state_metrics_data['customer_satisfaction_score'].append(sc_metrics.customer_satisfaction)

                    if sc_metrics.service_delivery_score is not None:
                        if 'service_delivery_score' not in state_metrics_data:
                            state_metrics_data['service_delivery_score'] = []
                        state_metrics_data['service_delivery_score'].append(sc_metrics.service_delivery_score)

                    # Add calculated metrics
                    if sc_metrics.rates_revenue is not None and sc.population:
                        if 'rates_revenue_per_capita' not in state_metrics_data:
                            state_metrics_data['rates_revenue_per_capita'] = []
                        state_metrics_data['rates_revenue_per_capita'].append(sc_metrics.rates_revenue / sc.population)

                    if sc_metrics.total_revenue is not None and sc.population:
                        if 'total_revenue_per_capita' not in state_metrics_data:
                            state_metrics_data['total_revenue_per_capita'] = []
                        state_metrics_data['total_revenue_per_capita'].append(sc_metrics.total_revenue / sc.population)

                    if sc_metrics.roads_maintained_km is not None and sc.population:
                        if 'roads_maintained_per_capita' not in state_metrics_data:
                            state_metrics_data['roads_maintained_per_capita'] = []
                        state_metrics_data['roads_maintained_per_capita'].append((sc_metrics.roads_maintained_km * 1000) / sc.population)

                # Add performance metrics
                for pm in sc_performance:
                    if pm.metric_name not in state_metrics_data:
                        state_metrics_data[pm.metric_name] = []
                    state_metrics_data[pm.metric_name].append(pm.value)

            # Build council metrics dict
            council_metrics_dict = {}

            # Add standardized metrics
            if council_metrics.customer_satisfaction is not None:
                council_metrics_dict['customer_satisfaction_score'] = council_metrics.customer_satisfaction

            if council_metrics.service_delivery_score is not None:
                council_metrics_dict['service_delivery_score'] = council_metrics.service_delivery_score

            # Add calculated metrics
            if council_metrics.rates_revenue is not None and council.population:
                council_metrics_dict['rates_revenue_per_capita'] = council_metrics.rates_revenue / council.population

            if council_metrics.total_revenue is not None and council.population:
                council_metrics_dict['total_revenue_per_capita'] = council_metrics.total_revenue / council.population

            if council_metrics.roads_maintained_km is not None and council.population:
                council_metrics_dict['roads_maintained_per_capita'] = (council_metrics.roads_maintained_km * 1000) / council.population

            # Add performance metrics
            for pm in council_performance:
                council_metrics_dict[pm.metric_name] = pm.value

            benchmark = {
                "council": {
                    "id": council.id,
                    "name": council.name,
                    "state": council.state
                },
                "metrics": {},
                "state_comparison": {
                    "total_councils_in_state": len(state_councils),
                    "rankings": {}
                }
            }

            # Calculate benchmarks for each metric
            for std_metric in STANDARDIZED_METRICS:
                canonical_name = std_metric.canonical_name
                council_value = council_metrics_dict.get(canonical_name)

                if council_value is not None:
                    state_values = state_metrics_data.get(canonical_name, [])

                    if state_values:
                        state_avg = sum(state_values) / len(state_values)
                        state_median = sorted(state_values)[len(state_values) // 2]

                        # Calculate percentile ranking
                        better_count = sum(1 for v in state_values if
                                         (v > council_value if not std_metric.lower_is_better else v < council_value))
                        percentile = (better_count / len(state_values)) * 100

                        benchmark["metrics"][canonical_name] = {
                            "council_value": round(council_value, 1) if canonical_name in ['customer_satisfaction_score', 'service_delivery_score'] else council_value,
                            "state_average": round(state_avg, 1) if canonical_name in ['customer_satisfaction_score', 'service_delivery_score'] else state_avg,
                            "state_median": round(state_median, 1) if canonical_name in ['customer_satisfaction_score', 'service_delivery_score'] else state_median,
                            "percentile_rank": percentile,
                            "unit": std_metric.unit,
                            "display_name": std_metric.display_name
                        }

                        benchmark["state_comparison"]["rankings"][canonical_name] = {
                            "rank": better_count + 1,
                            "total": len(state_values),
                            "better_than": f"{percentile:.1f}%"
                        }

            return {
                **benchmark,
                "data_source": {
                    "name": "RateMyCouncil Database",
                    "description": "Pre-computed normalized metrics from multiple Australian council data sources",
                    "last_updated": "2024",
                    "methodology": "Data aggregated from state government reports, council annual reports, and standardized metrics framework",
                    "disclaimer": "Benchmarking is based on available data and may not reflect all council performance aspects."
                }
            }

        finally:
            db.close()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error benchmarking council: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)