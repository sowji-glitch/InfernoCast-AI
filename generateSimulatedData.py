import pandas as pd
import random
import numpy as np
from datetime import datetime, timedelta

def generate_weather_data(num_records=500):
    """Generate realistic weather data for Napa Valley"""
    print(f"Generating {num_records} simulated weather records...")
    
    locations = ["Napa_Airport", "St_Helena", "Calistoga", "Yountville", "American_Canyon"]
    weather_data = []
    
    seasonal_patterns = {
        'spring': {'temp_base': 70, 'temp_var': 15, 'humidity_base': 65, 'humidity_var': 20, 'wind_base': 8, 'risk_base': 0.2},
        'summer': {'temp_base': 85, 'temp_var': 10, 'humidity_base': 35, 'humidity_var': 15, 'wind_base': 12, 'risk_base': 0.5},
        'fall_high_risk': {'temp_base': 95, 'temp_var': 8, 'humidity_base': 15, 'humidity_var': 10, 'wind_base': 25, 'risk_base': 0.8},
        'winter': {'temp_base': 60, 'temp_var': 12, 'humidity_base': 75, 'humidity_var': 15, 'wind_base': 6, 'risk_base': 0.1}
    }
    
    extreme_fire_days = [
        '2017-10-08', '2017-10-09', '2017-10-10',
        '2020-09-27', '2020-09-28', '2020-09-29',
        '2023-09-10', '2023-09-11',
        '2024-08-25', '2024-08-26'
    ]
    
    start_date = datetime(2017, 1, 1)
    end_date = datetime(2024, 12, 31)
    current_date = start_date
    
    record_count = 0
    while current_date <= end_date and record_count < num_records:
        month = current_date.month
        if month in [3, 4, 5]:
            pattern = 'spring'
        elif month in [6, 7, 8]:
            pattern = 'summer'
        elif month in [9, 10, 11]:
            pattern = 'fall_high_risk' if random.random() < 0.3 else 'summer'
        else:
            pattern = 'winter'
        
        is_extreme_day = current_date.strftime('%Y-%m-%d') in extreme_fire_days
        if is_extreme_day:
            pattern = 'fall_high_risk'
        
        for location in locations:
            if record_count >= num_records:
                break
                
            params = seasonal_patterns[pattern]
            
            if is_extreme_day:
                temp_max = random.normalvariate(100, 5)
                humidity = random.randint(5, 20)
                wind_speed = random.normalvariate(35, 8)
                fire_risk_score = random.uniform(0.85, 1.0)
            else:
                temp_max = random.normalvariate(params['temp_base'], params['temp_var'])
                humidity = max(5, min(95, random.normalvariate(params['humidity_base'], params['humidity_var'])))
                wind_speed = max(0, random.normalvariate(params['wind_base'], 5))
                
                risk = params['risk_base']
                if temp_max > 85: risk += 0.2
                if temp_max > 95: risk += 0.3
                if humidity < 30: risk += 0.2
                if humidity < 15: risk += 0.2
                if wind_speed > 15: risk += 0.15
                if wind_speed > 25: risk += 0.15
                fire_risk_score = min(1.0, risk + random.uniform(-0.1, 0.1))
            
            pressure = random.normalvariate(1015, 8)
            visibility = random.choice([10000, 12000, 15000, 20000]) if humidity > 20 else random.choice([5000, 8000, 10000])
            wind_deg = random.randint(0, 359)
            uvi = max(0, min(10, (temp_max - 40) / 8 + random.uniform(-1, 1)))
            
            weather_record = {
                'location': location,
                'date': current_date.strftime('%Y-%m-%d'),
                'temp_max': round(max(30, min(115, temp_max)), 1),
                'humidity': int(max(5, min(95, humidity))),
                'wind_speed': round(max(0, min(60, wind_speed)), 1),
                'wind_deg': wind_deg,
                'pressure': int(max(980, min(1040, pressure))),
                'visibility': int(visibility),
                'uvi': round(max(0, min(10, uvi)), 1),
                'fire_risk_score': round(max(0, min(1, fire_risk_score)), 2)
            }
            weather_data.append(weather_record)
            record_count += 1
        
        current_date += timedelta(days=random.choice([1, 2, 3, 5, 7]))
    
    return pd.DataFrame(weather_data)

def generate_fire_data(num_records=25):
    """Generate additional fire data to supplement real data"""
    print(f"Generating {num_records} simulated fire records...")
    
    fire_causes = ['Equipment Use', 'Lightning', 'Campfire', 'Vehicle', 'Arson', 'Power Lines', 'Debris Burning', 'Under Investigation']
    fire_names = ['Ridge Fire', 'Valley Fire', 'Oak Fire', 'Summit Fire', 'Meadow Fire', 'Creek Fire', 'Hill Fire', 
                 'Trail Fire', 'Canyon Fire', 'Silverado Fire', 'Vineyard Fire', 'Pine Fire', 'Rock Fire', 'Lake Fire']
    
    fire_data = []
    
    for i in range(num_records):
        year = random.choice([2018, 2019, 2020, 2021, 2022, 2023, 2024])
        
        start_month = random.choice([6, 7, 8, 9, 10, 11]) if year >= 2020 else random.choice([7, 8, 9, 10])
        start_day = random.randint(1, 28)
        alarm_date = datetime(year, start_month, start_day)
        
        duration_days = random.choice([1, 2, 3, 5, 7, 10, 14, 21])
        contained_date = alarm_date + timedelta(days=duration_days)
        
        acres = random.choice([
            random.uniform(1, 50),      # Small fires
            random.uniform(50, 500),    # Medium fires  
            random.uniform(500, 5000),  # Large fires
            random.uniform(5000, 25000) # Very large fires
        ])
        
        lat_base = 38.5 + random.uniform(-0.3, 0.3)
        lon_base = -122.4 + random.uniform(-0.3, 0.3)
        
        fire_record = {
            'fire_name': f"{random.choice(fire_names)} {year}-{i+1:02d}",
            'fire_year': year,
            'alarm_date': alarm_date.strftime('%Y-%m-%d'),
            'contained_date': contained_date.strftime('%Y-%m-%d'),
            'acres': round(acres, 1),
            'cause': random.choice(fire_causes),
            'latitude': round(lat_base, 4),
            'longitude': round(lon_base, 4),
            'county': 'Napa'
        }
        fire_data.append(fire_record)
    
    return pd.DataFrame(fire_data)

def generate_all_simulated_data():
    """Generate all simulated data"""
    weather_df = generate_weather_data(500)
    fire_df = generate_fire_data(25)
    return weather_df, fire_df

if __name__ == "__main__":
    weather_data, fire_data = generate_all_simulated_data()
    print(f"Generated {len(weather_data)} weather records")
    print(f"Generated {len(fire_data)} fire records")
    
    weather_data.to_csv('simulated_weather_data.csv', index=False)
    fire_data.to_csv('simulated_fire_data.csv', index=False)