"""
API Configuration for RateMyCouncil
External service API keys and settings
"""

import os
from typing import Dict, Optional

class APIConfig:
    """Configuration for external APIs"""

    def __init__(self):
        # Load from environment variables (in production, use .env file)
        self.google_places_api_key = os.getenv('GOOGLE_PLACES_API_KEY', 'AIzaSyAZDdCVRslSaXkmxblKenGi8zEcHqHbuqA')
        self.domain_api_key = os.getenv('DOMAIN_API_KEY', 'YOUR_DOMAIN_API_KEY')
        self.news_api_key = os.getenv('NEWS_API_KEY', 'cb1e429bf43d4b11882a711e25f7a8f7')
        self.abs_api_key = os.getenv('ABS_API_KEY', '')  # ABS may not require key

        # API endpoints
        self.endpoints = {
            'google_places': 'https://maps.googleapis.com/maps/api',
            'domain': 'https://api.domain.com.au',
            'news_api': 'https://newsapi.org/v2',
            'abs': 'https://data.api.abs.gov.au',
            'victoria_gov': 'https://www.data.vic.gov.au'
        }

        # Rate limits (requests per minute)
        self.rate_limits = {
            'google_places': 60,
            'domain': 30,
            'news_api': 100,
            'abs': 100,
            'victoria_gov': 60
        }

    def get_api_key(self, service: str) -> Optional[str]:
        """Get API key for a service"""
        key_map = {
            'google_places': self.google_places_api_key,
            'domain': self.domain_api_key,
            'news_api': self.news_api_key,
            'abs': self.abs_api_key
        }
        return key_map.get(service)

    def get_endpoint(self, service: str) -> Optional[str]:
        """Get API endpoint for a service"""
        return self.endpoints.get(service)

    def get_rate_limit(self, service: str) -> int:
        """Get rate limit for a service"""
        return self.rate_limits.get(service, 60)

# Global config instance
api_config = APIConfig()

# For development/demo purposes, here are placeholder keys
# In production, set these as environment variables
DEMO_API_KEYS = {
    'google_places': 'DEMO_GOOGLE_PLACES_KEY',
    'domain': 'DEMO_DOMAIN_KEY',
    'news_api': 'DEMO_NEWS_API_KEY'
}

def setup_demo_keys():
    """Set up demo API keys for development"""
    for service, key in DEMO_API_KEYS.items():
        os.environ[f'{service.upper()}_API_KEY'] = key
    print("Demo API keys set up for development")

if __name__ == "__main__":
    setup_demo_keys()
    print("API Configuration:")
    print(f"Google Places: {api_config.get_api_key('google_places')[:10]}...")
    print(f"Domain API: {api_config.get_api_key('domain')[:10]}...")
    print(f"News API: {api_config.get_api_key('news_api')[:10]}...")