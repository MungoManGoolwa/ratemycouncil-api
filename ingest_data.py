import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from sqlalchemy.orm import Session
from database import SessionLocal
from models import Council, CouncilIndex, ServiceScore, Indicator
from crud import create_council
import random

def scrape_alga_directory():
    """Scrape Australian Local Government Association directory for council data"""
    base_url = "https://www.alga.asn.au/directory/state/"
    states = ['nsw', 'vic', 'qld', 'wa', 'sa', 'tas', 'nt', 'act']

    councils = []

    for state in states:
        url = f"{base_url}{state}/"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'lxml')

            # Find council links - they are in divs with class 'directory-item' or similar
            council_divs = soup.find_all('div', class_=re.compile(r'.*directory.*'))

            if not council_divs:
                # Try different selector
                council_divs = soup.find_all('a', href=re.compile(r'/directory/council/'))

            for div in council_divs:
                name = None
                if div.name == 'a':
                    name = div.get_text(strip=True)
                else:
                    link = div.find('a')
                    if link:
                        name = link.get_text(strip=True)

                if name and 'Council' in name:
                    councils.append({
                        'name': name,
                        'state': state.upper(),
                        'population': None,  # Will fill later
                        'area_km2': None,
                        'peer_group': None,
                        'region_type': 'Urban'  # Default
                    })

        except Exception as e:
            print(f"Error scraping {state}: {e}")
            continue

    return councils

def get_population_data():
    """Get population data from ABS or approximate"""
    # For now, use approximate populations based on state
    state_populations = {
        'NSW': 8200000,
        'VIC': 6700000,
        'QLD': 5300000,
        'WA': 2800000,
        'SA': 1800000,
        'TAS': 570000,
        'NT': 250000,
        'ACT': 450000
    }

    # Approximate number of councils per state
    state_council_counts = {
        'NSW': 128,
        'VIC': 79,
        'QLD': 77,
        'WA': 139,
        'SA': 68,
        'TAS': 29,
        'NT': 17,
        'ACT': 1
    }

    return state_populations, state_council_counts

def assign_population_and_area(councils):
    """Assign approximate population and area based on state"""
    state_pop, state_counts = get_population_data()

    for council in councils:
        state = council['state']
        if state in state_pop:
            # Distribute population roughly
            avg_pop = state_pop[state] // state_counts[state]
            council['population'] = random.randint(avg_pop // 2, avg_pop * 2)

            # Area based on population (rough estimate)
            council['area_km2'] = council['population'] * random.uniform(0.1, 2.0)

            # Assign peer group based on population
            pop = council['population']
            if pop > 500000:
                council['peer_group'] = 'Metro established'
            elif pop > 100000:
                council['peer_group'] = 'Regional city'
            elif pop > 50000:
                council['peer_group'] = 'Regional town'
            else:
                council['peer_group'] = 'Rural'

    return councils

def ingest_councils():
    """Main function to ingest council data"""
    print("Scraping ALGA directory...")
    councils = scrape_alga_directory()

    if not councils:
        print("No councils scraped, using fallback data")
        # Fallback with known councils - expanded list
        councils = [
            {'name': 'Sydney City Council', 'state': 'NSW', 'population': 250000, 'area_km2': 25.0, 'peer_group': 'Metro established', 'region_type': 'Urban'},
            {'name': 'Melbourne City Council', 'state': 'VIC', 'population': 100000, 'area_km2': 50.0, 'peer_group': 'Metro established', 'region_type': 'Urban'},
            {'name': 'Brisbane City Council', 'state': 'QLD', 'population': 130000, 'area_km2': 30.0, 'peer_group': 'Metro established', 'region_type': 'Urban'},
            {'name': 'Perth City Council', 'state': 'WA', 'population': 220000, 'area_km2': 40.0, 'peer_group': 'Metro established', 'region_type': 'Urban'},
            {'name': 'Adelaide City Council', 'state': 'SA', 'population': 140000, 'area_km2': 35.0, 'peer_group': 'Metro established', 'region_type': 'Urban'},
            {'name': 'Hobart City Council', 'state': 'TAS', 'population': 55000, 'area_km2': 20.0, 'peer_group': 'Regional city', 'region_type': 'Urban'},
            {'name': 'Darwin City Council', 'state': 'NT', 'population': 85000, 'area_km2': 45.0, 'peer_group': 'Regional city', 'region_type': 'Urban'},
            {'name': 'Canberra City Council', 'state': 'ACT', 'population': 95000, 'area_km2': 50.0, 'peer_group': 'Regional city', 'region_type': 'Urban'},
            {'name': 'Newcastle City Council', 'state': 'NSW', 'population': 170000, 'area_km2': 55.0, 'peer_group': 'Regional city', 'region_type': 'Urban'},
            {'name': 'Wollongong City Council', 'state': 'NSW', 'population': 220000, 'area_km2': 60.0, 'peer_group': 'Regional city', 'region_type': 'Urban'},
            {'name': 'Geelong City Council', 'state': 'VIC', 'population': 200000, 'area_km2': 100.0, 'peer_group': 'Regional town', 'region_type': 'Urban'},
            {'name': 'Ballarat City Council', 'state': 'VIC', 'population': 150000, 'area_km2': 80.0, 'peer_group': 'Regional town', 'region_type': 'Urban'},
            {'name': 'Bendigo City Council', 'state': 'VIC', 'population': 100000, 'area_km2': 70.0, 'peer_group': 'Regional town', 'region_type': 'Urban'},
            {'name': 'Gold Coast City Council', 'state': 'QLD', 'population': 650000, 'area_km2': 120.0, 'peer_group': 'Metro established', 'region_type': 'Urban'},
            {'name': 'Sunshine Coast Council', 'state': 'QLD', 'population': 350000, 'area_km2': 90.0, 'peer_group': 'Regional city', 'region_type': 'Urban'},
            {'name': 'Townsville City Council', 'state': 'QLD', 'population': 180000, 'area_km2': 85.0, 'peer_group': 'Regional city', 'region_type': 'Urban'},
            {'name': 'Cairns Regional Council', 'state': 'QLD', 'population': 170000, 'area_km2': 150.0, 'peer_group': 'Regional city', 'region_type': 'Urban'},
            {'name': 'Toowoomba Regional Council', 'state': 'QLD', 'population': 180000, 'area_km2': 130.0, 'peer_group': 'Regional city', 'region_type': 'Urban'},
            {'name': 'Logan City Council', 'state': 'QLD', 'population': 350000, 'area_km2': 95.0, 'peer_group': 'Regional city', 'region_type': 'Urban'},
            {'name': 'Ipswich City Council', 'state': 'QLD', 'population': 240000, 'area_km2': 110.0, 'peer_group': 'Regional city', 'region_type': 'Urban'},
            {'name': 'Redland City Council', 'state': 'QLD', 'population': 160000, 'area_km2': 75.0, 'peer_group': 'Regional city', 'region_type': 'Urban'},
            {'name': 'Caboolture Shire Council', 'state': 'QLD', 'population': 140000, 'area_km2': 120.0, 'peer_group': 'Regional town', 'region_type': 'Urban'},
            {'name': 'Rockhampton Regional Council', 'state': 'QLD', 'population': 85000, 'area_km2': 180.0, 'peer_group': 'Regional town', 'region_type': 'Urban'},
            {'name': 'Bundaberg Regional Council', 'state': 'QLD', 'population': 100000, 'area_km2': 160.0, 'peer_group': 'Regional town', 'region_type': 'Urban'},
            {'name': 'Hervey Bay City Council', 'state': 'QLD', 'population': 60000, 'area_km2': 90.0, 'peer_group': 'Regional town', 'region_type': 'Urban'},
            {'name': 'Gladstone Regional Council', 'state': 'QLD', 'population': 65000, 'area_km2': 170.0, 'peer_group': 'Regional town', 'region_type': 'Urban'},
            {'name': 'Mount Isa City Council', 'state': 'QLD', 'population': 22000, 'area_km2': 43000.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Alice Springs Town Council', 'state': 'NT', 'population': 28000, 'area_km2': 450.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Palmerston City Council', 'state': 'NT', 'population': 38000, 'area_km2': 55.0, 'peer_group': 'Regional town', 'region_type': 'Urban'},
            {'name': 'Katherine Town Council', 'state': 'NT', 'population': 10000, 'area_km2': 120.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Tennant Creek Town Council', 'state': 'NT', 'population': 3000, 'area_km2': 50000.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Greater Darwin', 'state': 'NT', 'population': 145000, 'area_km2': 120.0, 'peer_group': 'Regional city', 'region_type': 'Urban'},
            {'name': 'Alice Springs', 'state': 'NT', 'population': 28000, 'area_km2': 450.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Katherine', 'state': 'NT', 'population': 10000, 'area_km2': 120.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Tennant Creek', 'state': 'NT', 'population': 3000, 'area_km2': 50000.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Barkly', 'state': 'NT', 'population': 5000, 'area_km2': 300000.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Central Desert', 'state': 'NT', 'population': 4000, 'area_km2': 280000.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'East Arnhem', 'state': 'NT', 'population': 10000, 'area_km2': 33000.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'MacDonnell', 'state': 'NT', 'population': 6000, 'area_km2': 268000.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Roper Gulf', 'state': 'NT', 'population': 8000, 'area_km2': 185000.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Tiwi Islands', 'state': 'NT', 'population': 2500, 'area_km2': 7500.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Victoria Daly', 'state': 'NT', 'population': 3000, 'area_km2': 153000.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'West Arnhem', 'state': 'NT', 'population': 7000, 'area_km2': 49000.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'West Daly', 'state': 'NT', 'population': 3500, 'area_km2': 14000.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Unincorporated NT', 'state': 'NT', 'population': 1000, 'area_km2': 100000.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Launceston City Council', 'state': 'TAS', 'population': 70000, 'area_km2': 45.0, 'peer_group': 'Regional city', 'region_type': 'Urban'},
            {'name': 'Devonport City Council', 'state': 'TAS', 'population': 25000, 'area_km2': 25.0, 'peer_group': 'Regional town', 'region_type': 'Urban'},
            {'name': 'Burnie City Council', 'state': 'TAS', 'population': 19000, 'area_km2': 20.0, 'peer_group': 'Regional town', 'region_type': 'Urban'},
            {'name': 'Clarence City Council', 'state': 'TAS', 'population': 60000, 'area_km2': 40.0, 'peer_group': 'Regional city', 'region_type': 'Urban'},
            {'name': 'Glenorchy City Council', 'state': 'TAS', 'population': 48000, 'area_km2': 35.0, 'peer_group': 'Regional city', 'region_type': 'Urban'},
            {'name': 'Break O\'Day Council', 'state': 'TAS', 'population': 6500, 'area_km2': 3500.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Brighton Council', 'state': 'TAS', 'population': 18000, 'area_km2': 15.0, 'peer_group': 'Regional town', 'region_type': 'Urban'},
            {'name': 'Central Coast Council', 'state': 'TAS', 'population': 22000, 'area_km2': 20.0, 'peer_group': 'Regional town', 'region_type': 'Urban'},
            {'name': 'Central Highlands Council', 'state': 'TAS', 'population': 2200, 'area_km2': 2000.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Circular Head Council', 'state': 'TAS', 'population': 8000, 'area_km2': 4900.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Derwent Valley Council', 'state': 'TAS', 'population': 10000, 'area_km2': 4100.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Dorset Council', 'state': 'TAS', 'population': 6700, 'area_km2': 3200.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Flinders Council', 'state': 'TAS', 'population': 900, 'area_km2': 2000.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'George Town Council', 'state': 'TAS', 'population': 7000, 'area_km2': 10.0, 'peer_group': 'Regional town', 'region_type': 'Urban'},
            {'name': 'Glamorgan-Spring Bay Council', 'state': 'TAS', 'population': 4500, 'area_km2': 2600.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Huon Valley Council', 'state': 'TAS', 'population': 18000, 'area_km2': 5500.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Kentish Council', 'state': 'TAS', 'population': 6500, 'area_km2': 1200.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'King Island Council', 'state': 'TAS', 'population': 1600, 'area_km2': 1100.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Kingborough Council', 'state': 'TAS', 'population': 39000, 'area_km2': 720.0, 'peer_group': 'Regional city', 'region_type': 'Urban'},
            {'name': 'Latrobe Council', 'state': 'TAS', 'population': 12000, 'area_km2': 550.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Meander Valley Council', 'state': 'TAS', 'population': 20000, 'area_km2': 1300.0, 'peer_group': 'Regional town', 'region_type': 'Urban'},
            {'name': 'Northern Midlands Council', 'state': 'TAS', 'population': 13000, 'area_km2': 5100.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Sorell Council', 'state': 'TAS', 'population': 17000, 'area_km2': 600.0, 'peer_group': 'Regional town', 'region_type': 'Urban'},
            {'name': 'Southern Midlands Council', 'state': 'TAS', 'population': 6200, 'area_km2': 2600.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Tasman Council', 'state': 'TAS', 'population': 2500, 'area_km2': 660.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'Waratah-Wynyard Council', 'state': 'TAS', 'population': 14000, 'area_km2': 3500.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'West Coast Council', 'state': 'TAS', 'population': 4200, 'area_km2': 9600.0, 'peer_group': 'Rural', 'region_type': 'Rural'},
            {'name': 'West Tamar Council', 'state': 'TAS', 'population': 25000, 'area_km2': 690.0, 'peer_group': 'Regional town', 'region_type': 'Urban'},
        ]

    print(f"Found {len(councils)} councils")

    councils = assign_population_and_area(councils)

    # Save to database
    db = SessionLocal()
    try:
        for council_data in councils[:50]:  # Limit for testing
            # Check if exists
            existing = db.query(Council).filter(Council.name == council_data['name']).first()
            if not existing:
                council = Council(
                    name=council_data['name'],
                    state=council_data['state'],
                    population=council_data['population'],
                    area_km2=council_data['area_km2'],
                    peer_group=council_data['peer_group'],
                    region_type=council_data['region_type']
                )
                db.add(council)
                db.commit()
                db.refresh(council)

                # Create index and scores
                index = CouncilIndex(council_id=council.id, score=random.randint(60, 95))
                db.add(index)

                # Add some service scores
                services = ['Education', 'Health', 'Transport', 'Environment', 'Community']
                for service in services:
                    score = ServiceScore(
                        council_id=council.id,
                        service_category=service,
                        year=2023,
                        score=random.randint(50, 100)
                    )
                    db.add(score)

                db.commit()
                print(f"Added {council.name}")

    except Exception as e:
        print(f"Error: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    ingest_councils()