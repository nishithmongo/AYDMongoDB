#!/usr/bin/env python3
import random
import datetime
import time
import threading
import sys
from pymongo import MongoClient
from bson import ObjectId

#################################################
# BATCH SIZE CONFIGURATION
#################################################

# Data Insertion Settings
BATCH_SIZE = 1000          # Number of documents per batch insert

# Define devices for our smart home
DEVICES = [
    {"id": "HVAC", "name": "HVAC System", "type": "climate"},
    {"id": "FRIDGE", "name": "Refrigerator", "type": "appliance"},
    {"id": "LIGHTING", "name": "Home Lighting", "type": "lighting"},
    {"id": "EV_CHARGER", "name": "EV Charging Station", "type": "transportation"},
    {"id": "WASHER", "name": "Washing Machine", "type": "appliance"}
]

# Define US holidays with dramatic power consumption changes
HOLIDAYS = [
    {"month": 1, "day": 1, "name": "New Year's Day"},
    {"month": 1, "day": 16, "name": "Martin Luther King Jr. Day", "day_of_week": 0},
    {"month": 2, "day": 14, "name": "Valentine's Day"},
    {"month": 3, "day": 17, "name": "St. Patrick's Day"},
    {"month": 4, "day": 15, "name": "Tax Day"},
    {"month": 5, "day": 5, "name": "Cinco de Mayo"},
    {"month": 5, "day": 29, "name": "Memorial Day", "day_of_week": 0},
    {"month": 6, "day": 19, "name": "Juneteenth"},
    {"month": 7, "day": 4, "name": "Independence Day"},
    {"month": 9, "day": 4, "name": "Labor Day", "day_of_week": 0},
    {"month": 10, "day": 31, "name": "Halloween"},
    {"month": 11, "day": 23, "name": "Thanksgiving", "day_of_week": 3},
    {"month": 12, "day": 24, "name": "Christmas Eve"},
    {"month": 12, "day": 25, "name": "Christmas Day"},
    {"month": 12, "day": 31, "name": "New Year's Eve"}
]

# Special events with dramatic consumption changes
SPECIAL_EVENTS = [
    # Major power outage - complete blackout for several hours
    {"name": "Power Outage", "month": 3, "day": 15, "hours": [14, 15, 16, 17, 18, 19], "multiplier": 0.0},
    
    # Severe heat wave - extreme HVAC usage
    {"name": "Heat Wave", "month": 7, "start_day": 10, "end_day": 16, "device_id": "HVAC", "multiplier": 3.5},
    
    # Winter storm - increased heating needs
    {"name": "Winter Storm", "month": 2, "start_day": 5, "end_day": 8, "device_id": "HVAC", "multiplier": 2.8},
    
    # Family vacation - dramatically reduced usage
    {"name": "Vacation", "month": 8, "start_day": 5, "end_day": 12, "all_devices": True, "multiplier": 0.2},
    
    # Big game day - large viewing party
    {"name": "Super Bowl Party", "month": 2, "day": 11, "hours": [14, 15, 16, 17, 18, 19, 20, 21], "device_id": "LIGHTING", "multiplier": 2.5},
    
    # House guests for a week - increased usage for all devices
    {"name": "House Guests", "month": 6, "start_day": 15, "end_day": 22, "all_devices": True, "multiplier": 1.8},
    
    # Home renovation - strange usage patterns with periods of no usage and high usage
    {"name": "Home Renovation", "month": 4, "start_day": 10, "end_day": 20, "random_pattern": True},
]

# Seasonal variations
SEASONAL_MULTIPLIERS = {
    # Winter months - more heating, more indoor lighting
    1: {"HVAC": 2.0, "LIGHTING": 1.5, "EV_CHARGER": 0.9, "WASHER": 1.1, "FRIDGE": 0.9},  # January
    2: {"HVAC": 1.8, "LIGHTING": 1.4, "EV_CHARGER": 0.9, "WASHER": 1.1, "FRIDGE": 0.9},  # February
    
    # Spring transition
    3: {"HVAC": 1.0, "LIGHTING": 1.0, "EV_CHARGER": 1.0, "WASHER": 1.2, "FRIDGE": 1.0},  # March
    4: {"HVAC": 0.8, "LIGHTING": 0.8, "EV_CHARGER": 1.1, "WASHER": 1.3, "FRIDGE": 1.0},  # April
    5: {"HVAC": 1.2, "LIGHTING": 0.7, "EV_CHARGER": 1.2, "WASHER": 1.2, "FRIDGE": 1.1},  # May
    
    # Summer months - more cooling, less lighting
    6: {"HVAC": 1.5, "LIGHTING": 0.6, "EV_CHARGER": 1.2, "WASHER": 1.0, "FRIDGE": 1.3},  # June
    7: {"HVAC": 2.2, "LIGHTING": 0.5, "EV_CHARGER": 1.3, "WASHER": 1.0, "FRIDGE": 1.4},  # July
    8: {"HVAC": 2.0, "LIGHTING": 0.5, "EV_CHARGER": 1.3, "WASHER": 1.0, "FRIDGE": 1.4},  # August
    
    # Fall transition
    9: {"HVAC": 1.0, "LIGHTING": 0.8, "EV_CHARGER": 1.1, "WASHER": 1.1, "FRIDGE": 1.2},  # September
    10: {"HVAC": 1.2, "LIGHTING": 1.2, "EV_CHARGER": 1.0, "WASHER": 1.1, "FRIDGE": 1.0}, # October
    11: {"HVAC": 1.5, "LIGHTING": 1.4, "EV_CHARGER": 0.9, "WASHER": 1.2, "FRIDGE": 0.9}, # November
    
    # Holiday season
    12: {"HVAC": 1.8, "LIGHTING": 2.0, "EV_CHARGER": 0.8, "WASHER": 1.4, "FRIDGE": 1.1}  # December
}

# Progress Reporting Frequency (in documents)
PROGRESS_REPORT_FREQUENCY = 300000  # Report progress every 300k documents

#################################################
# SHARED VARIABLES AND INTERACTIVE PROMPTS
#################################################

# Shared variables
lock = threading.Lock()
total_inserted = 0
process_start = time.time()

def get_user_inputs():
    """Prompt user for all required inputs"""
    print("Welcome to the Thermostat Data Generator")
    print("----------------------------------------")
    print("Please provide the following information:")
    
    # MongoDB connection details
    mongodb_url = input("\nMongoDB connection URL: ")
    database_name = input("Database name: ")
    collection_name = input("Collection name: ")
    
    # Date range
    while True:
        start_date_str = input("\nStart date (YYYY-MM-DD): ")
        try:
            start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
            break
        except ValueError:
            print("Invalid date format. Please use YYYY-MM-DD.")
    
    while True:
        end_date_str = input("End date (YYYY-MM-DD): ")
        try:
            end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d")
            if end_date < start_date:
                print("End date must be after start date.")
                continue
            break
        except ValueError:
            print("Invalid date format. Please use YYYY-MM-DD.")
    
    # Interval
    while True:
        try:
            interval_seconds = int(input("\nInterval between data points (in seconds): "))
            if interval_seconds <= 0:
                print("Interval must be a positive number.")
                continue
            break
        except ValueError:
            print("Please enter a valid number.")
    
    # Convert seconds to minutes for internal calculations
    interval_minutes = interval_seconds / 60
    
    return {
        "mongodb_url": mongodb_url,
        "database_name": database_name,
        "collection_name": collection_name,
        "start_date": start_date,
        "end_date": end_date,
        "interval_seconds": interval_seconds,
        "interval_minutes": interval_minutes
    }

#################################################
# DATA GENERATION FUNCTIONS
#################################################

def is_holiday(timestamp):
    """Check if the timestamp is on a holiday"""
    for holiday in HOLIDAYS:
        if timestamp.month == holiday["month"]:
            # Handle fixed-date holidays
            if "day_of_week" not in holiday and timestamp.day == holiday["day"]:
                return holiday["name"]
            
            # Handle holidays that fall on specific days of week (e.g., Thanksgiving - 4th Thursday)
            elif "day_of_week" in holiday:
                # For simplification, we'll just handle a few special cases
                # A full implementation would calculate the exact dates for each year
                
                # Memorial Day (last Monday in May)
                if holiday["name"] == "Memorial Day" and timestamp.day >= 25 and timestamp.weekday() == 0:
                    return holiday["name"]
                
                # Labor Day (first Monday in September)
                if holiday["name"] == "Labor Day" and timestamp.day <= 7 and timestamp.weekday() == 0:
                    return holiday["name"]
                
                # Thanksgiving (fourth Thursday in November)
                if holiday["name"] == "Thanksgiving" and 22 <= timestamp.day <= 28 and timestamp.weekday() == 3:
                    return holiday["name"]
                
                # MLK Day (third Monday in January)
                if holiday["name"] == "Martin Luther King Jr. Day" and 15 <= timestamp.day <= 21 and timestamp.weekday() == 0:
                    return holiday["name"]
    
    return None

def is_special_event(device, timestamp):
    """Check if the timestamp is during a special event and get the multiplier"""
    for event in SPECIAL_EVENTS:
        if timestamp.month == event["month"]:
            # Single day events with specific hours
            if "day" in event and "hours" in event:
                if timestamp.day == event["day"] and timestamp.hour in event["hours"]:
                    # Check if event applies to all devices or this specific device
                    if "all_devices" in event and event["all_devices"]:
                        return event["name"], event["multiplier"]
                    elif "device_id" in event and event["device_id"] == device["id"]:
                        return event["name"], event["multiplier"]
                    elif "device_id" not in event:
                        return event["name"], event["multiplier"]
            
            # Multi-day events
            elif "start_day" in event and "end_day" in event:
                if event["start_day"] <= timestamp.day <= event["end_day"]:
                    # Random pattern events (e.g., home renovation)
                    if "random_pattern" in event and event["random_pattern"]:
                        # Create a random but consistent pattern for this event
                        seed = timestamp.day + timestamp.hour
                        random.seed(seed)
                        multiplier = random.choice([0.1, 0.5, 2.0, 3.0, 0.0])
                        random.seed()  # Reset seed
                        return event["name"], multiplier
                    
                    # Check if event applies to all devices or this specific device
                    if "all_devices" in event and event["all_devices"]:
                        return event["name"], event["multiplier"]
                    elif "device_id" in event and event["device_id"] == device["id"]:
                        return event["name"], event["multiplier"]
    
    return None, 1.0

def apply_holiday_patterns(device, power, timestamp, holiday_name):
    """Apply holiday-specific patterns to device power consumption with more dramatic changes"""
    
    # Each holiday has different energy usage patterns
    if holiday_name == "Christmas Day" or holiday_name == "Christmas Eve":
        # Christmas: More cooking, lights, people at home
        if device["id"] == "HVAC":
            power *= 2.0  # Much more heating with people at home
        elif device["id"] == "LIGHTING":
            power *= 5.0  # Dramatic Christmas lights!
        elif device["id"] == "WASHER":
            power *= 3.0  # Many more dishes and laundry with guests
        elif device["id"] == "EV_CHARGER":
            power *= 0.3  # Much less commuting
    
    elif holiday_name == "Thanksgiving":
        # Thanksgiving: Heavy cooking day
        if device["id"] == "HVAC":
            power *= 2.0  # More heating with people at home
        elif device["id"] == "LIGHTING":
            power *= 2.5  # More lights on for gathering
        elif device["id"] == "WASHER":
            power *= 4.0  # Many more dishes
    
    elif holiday_name == "New Year's Eve" or holiday_name == "New Year's Day":
        # New Year's: Party time
        if device["id"] == "LIGHTING":
            power *= 3.0  # Party lighting
        elif device["id"] == "HVAC":
            power *= 2.0  # More heating with people at home
        elif device["id"] == "EV_CHARGER":
            power *= 0.4  # Much less driving on New Year's Day
    
    elif holiday_name == "Independence Day" or holiday_name == "Memorial Day" or holiday_name == "Labor Day":
        # Summer holidays: Different pattern
        if device["id"] == "HVAC":
            if 13 <= timestamp.hour <= 18:  # Hot summer afternoon
                power *= 2.5  # Much more AC use during summer holidays
        elif device["id"] == "LIGHTING":
            power *= 0.3  # Much less indoor lighting, more time outside
    
    elif holiday_name == "Halloween":
        # Halloween: Evening spike
        if device["id"] == "LIGHTING" and timestamp.hour >= 17 and timestamp.hour <= 22:
            power *= 5.0  # Dramatic Halloween decorations and porch lights
    
    elif holiday_name == "Valentine's Day":
        # Valentine's Day: Evening at home
        if 18 <= timestamp.hour <= 23:
            if device["id"] == "LIGHTING":
                power *= 2.0  # Romantic lighting
            elif device["id"] == "HVAC":
                power *= 1.5  # Keeping it warm and cozy
    
    elif holiday_name == "St. Patrick's Day":
        # St. Patrick's Day: Party effects
        if 17 <= timestamp.hour <= 23:
            if device["id"] == "LIGHTING":
                power *= 2.0  # Party lighting
    
    return power

def apply_device_specific_metrics(device, reading):
    """Add simple device-specific metrics to the reading"""
    
    # Add status field for all devices - simple on/off
    reading["status"] = "on" if reading["power_kw"] > 0.05 else "off"
    
    # Add device temperature for applicable devices
    if device["id"] in ["HVAC", "FRIDGE"]:
        if device["id"] == "HVAC":
            reading["temperature"] = random.randint(65, 80)  # Room temperature in F
        else:  # FRIDGE
            reading["temperature"] = random.randint(33, 40)  # Fridge temperature in F
    
    # Add runtime for all devices (how long it's been on in current state)
    # Simple random value that doesn't need to be consistent across time series
    if reading["status"] == "on":
        reading["runtime_minutes"] = random.randint(1, 120)
    else:
        reading["runtime_minutes"] = 0
    
    # Add efficiency rating for all devices (percentage)
    reading["efficiency"] = random.randint(70, 99)
    
    # Add maintenance_needed flag (simple boolean for all devices)
    reading["maintenance_needed"] = random.random() < 0.05  # 5% chance of maintenance flag
    
    return reading

def generate_reading(device, timestamp):
    """Generate a reading with realistic patterns including dramatic holiday and special events"""
    
    hour = timestamp.hour
    is_weekend = timestamp.weekday() >= 5
    
    # Simple power values based on device type and time
    if device["id"] == "HVAC":
        # HVAC: higher during afternoon, lower at night
        if 13 <= hour <= 18:
            power = random.uniform(2.5, 3.5)  # Peak afternoon
        elif 23 <= hour or hour <= 5:
            power = random.uniform(0.1, 0.5)  # Low overnight
        else:
            power = random.uniform(1.0, 2.0)  # Normal
    elif device["id"] == "FRIDGE":
        power = random.uniform(0.1, 0.2)  # Consistent
    elif device["id"] == "LIGHTING":
        # Lighting: higher when dark
        if 6 <= hour <= 8 or 18 <= hour <= 23:
            power = random.uniform(0.2, 0.5)
        else:
            power = random.uniform(0.0, 0.1)
    elif device["id"] == "EV_CHARGER":
        # EV Charger: primarily evening/overnight
        if 19 <= hour <= 23 or 0 <= hour <= 5:
            power = random.uniform(6.0, 7.5) if random.random() < 0.5 else 0
        else:
            power = 0
    elif device["id"] == "WASHER":
        # Washing machine: used mostly mornings and evenings
        if (7 <= hour <= 10) or (18 <= hour <= 21):
            # 30% chance of being active during these hours
            power = random.uniform(0.5, 1.2) if random.random() < 0.3 else 0.01
        else:
            power = 0.01  # Standby power
    else:
        power = random.uniform(0.1, 0.5)
    
    # Weekend variations
    if is_weekend and device["id"] in ["LIGHTING", "HVAC", "WASHER"]:
        power *= 1.5  # Increased weekend usage
    
    # Apply seasonal multipliers (different for each month)
    month = timestamp.month
    if month in SEASONAL_MULTIPLIERS and device["id"] in SEASONAL_MULTIPLIERS[month]:
        power *= SEASONAL_MULTIPLIERS[month][device["id"]]
    
    # Check for holidays
    holiday_name = is_holiday(timestamp)
    if holiday_name:
        power = apply_holiday_patterns(device, power, timestamp, holiday_name)
    
    # Check for special events
    event_name, event_multiplier = is_special_event(device, timestamp)
    if event_name:
        power *= event_multiplier
    
    # Create the base reading
    reading = {
        "_id": ObjectId(),
        "metadata": {
            "device_id": device["id"],
            "device_name": device["name"],
            "device_type": device["type"]
        },
        "timestamp": timestamp,
        "power_kw": round(power, 3)
    }
    
    # Add device-specific fields
    reading = apply_device_specific_metrics(device, reading)
    
    # Add event tagging for easier analytics
    if holiday_name:
        reading["holiday"] = holiday_name
    if event_name:
        reading["special_event"] = event_name
    
    return reading

def worker_thread(device, start_date, end_date, interval_minutes, batch_size, mongodb_url, database_name, collection_name):
    """Worker thread that generates and inserts data for a specific device for a date range"""
    
    global total_inserted
    
    # Connect to MongoDB
    client = MongoClient(mongodb_url)
    db = client[database_name]
    collection = db[collection_name]
    
    print(f"Worker started for {device['name']}: generating data from {start_date} to {end_date}")
    
    # Generate and insert data in batches
    device_inserted = 0
    batch = []
    
    current_time = start_date
    
    while current_time <= end_date:
        # Generate reading for this device
        reading = generate_reading(device, current_time)
        batch.append(reading)
        
        # Insert when batch is full
        if len(batch) >= batch_size:
            try:
                collection.insert_many(batch)
                
                with lock:
                    device_inserted += len(batch)
                    total_inserted += len(batch)
                    
                    # Print progress based on configured frequency
                    if total_inserted % PROGRESS_REPORT_FREQUENCY < batch_size:
                        elapsed = time.time() - process_start
                        docs_per_second = total_inserted / elapsed if elapsed > 0 else 0
                        time_diff = (end_date - start_date).total_seconds()
                        if time_diff > 0:
                            percent_time = ((current_time - start_date).total_seconds() / time_diff) * 100
                            est_remaining_secs = (elapsed / percent_time * 100) - elapsed if percent_time > 0 else 0
                        else:
                            percent_time = 100
                            est_remaining_secs = 0
                        
                        print(f"\n[{datetime.datetime.now().strftime('%H:%M:%S')}] PROGRESS UPDATE:")
                        print(f"  Inserted: {total_inserted:,} documents")
                        print(f"  Date progress: {current_time.strftime('%Y-%m-%d %H:%M')} ({percent_time:.1f}%)")
                        print(f"  Elapsed time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
                        print(f"  Insertion rate: {docs_per_second:.1f} docs/sec")
                        print(f"  Estimated remaining: {est_remaining_secs/60:.1f} minutes")
                        print("-" * 60)
            except Exception as e:
                print(f"Error inserting batch for {device['name']}: {e}")
                # Try to continue with the next batch
            
            batch = []
        
        # Move to next timestamp
        current_time += datetime.timedelta(minutes=interval_minutes)
    
    # Insert any remaining documents
    if batch:
        try:
            collection.insert_many(batch)
            with lock:
                device_inserted += len(batch)
                total_inserted += len(batch)
        except Exception as e:
            print(f"Error inserting final batch for {device['name']}: {e}")
    
    print(f"Worker for {device['name']} completed. Inserted {device_inserted:,} documents.")
    client.close()

def main():
    """Main function to set up and run the data generation process"""
    # Get user inputs through interactive prompts
    inputs = get_user_inputs()
    
    mongodb_url = inputs["mongodb_url"]
    database_name = inputs["database_name"]
    collection_name = inputs["collection_name"]
    start_date = inputs["start_date"]
    end_date = inputs["end_date"]
    interval_seconds = inputs["interval_seconds"]
    interval_minutes = inputs["interval_minutes"]
    
    # Add a day to end_date and subtract the interval to include the full end date
    end_date = end_date + datetime.timedelta(days=1) - datetime.timedelta(seconds=interval_seconds)
    
    # Connect to MongoDB
    print(f"\nConnecting to MongoDB at: {datetime.datetime.now().strftime('%H:%M:%S')}")
    print(f"MongoDB URL: {mongodb_url}")
    print(f"Database: {database_name}")
    print(f"Collection: {collection_name}")
    
    try:
        client = MongoClient(mongodb_url)
        # Quick test of the connection
        client.server_info()
        # Create database reference
        db = client[database_name]
        print("MongoDB connection successful!")
    except Exception as e:
        print(f"\nError connecting to MongoDB: {e}")
        sys.exit(1)
    
    # Create collection with time series configuration if it doesn't exist
    if collection_name not in db.list_collection_names():
        print(f"\nCreating time series collection: {collection_name}")
        try:
            db.create_collection(
                collection_name,
                timeseries={
                    "timeField": "timestamp",
                    "metaField": "metadata",
                    "granularity": "minutes"
                }
            )
            print(f"Created time series collection: {collection_name}")
        except Exception as e:
            print(f"Warning: Could not create time series collection: {e}")
            print("Will use regular collection instead.")
    
    # Calculate total documents and estimate data size
    total_seconds = int((end_date - start_date).total_seconds()) + 1
    total_data_points = (total_seconds // interval_seconds) * len(DEVICES)
    estimated_size_mb = total_data_points * 0.0005  # Rough estimate of 500 bytes per document
    
    print(f"\nData Generation Plan:")
    print(f"  Time range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"  Resolution: Every {interval_seconds} second(s)")
    print(f"  Devices: {len(DEVICES)}")
    print(f"  Estimated data points: {total_data_points:,}")
    print(f"  Estimated data size: {estimated_size_mb:.1f} MB")
    print(f"  Threads: {len(DEVICES)}")
    print("-" * 60)
    
    # Get confirmation for large datasets
    if total_data_points > 1000000:
        confirm = input(f"\nWarning: This will generate a large dataset ({total_data_points:,} data points). Continue? (y/n): ")
        if confirm.lower() != 'y':
            print("Operation cancelled.")
            return
    
    # Start worker threads for each device
    threads = []
    for device in DEVICES:
        thread = threading.Thread(
            target=worker_thread,
            args=(device, start_date, end_date, interval_minutes, BATCH_SIZE, mongodb_url, database_name, collection_name)
        )
        thread.daemon = True  # Set daemon to True so main program can exit if threads are still running
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to complete
    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Waiting for threads to complete...")
        print("Please wait to ensure data integrity.")
        # Let threads finish their current batch
        for thread in threads:
            if thread.is_alive():
                thread.join(timeout=10)
        print("Process terminated.")
        sys.exit(0)
    
    print(f"\nData generation complete! Total documents inserted: {total_inserted:,}")
    print(f"Total runtime: {(time.time() - process_start)/60:.1f} minutes")
    client.close()

# Call the main function if the script is run directly
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        sys.exit(1)
