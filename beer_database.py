import psycopg2
import requests
import yaml
import time
from tqdm import tqdm
from psycopg2.extras import execute_values
from datetime import datetime

# Load configuration from YAML file
def load_config(filepath="config.yaml"):
    try:
        with open(filepath, "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        print(f"Error loading config: {e}")
        raise

# Fetch all rated beers from the API, handling pagination
def fetch_rated_beers(api_config):
    print("Fetching rated beers...")
    url = f"{api_config['base_url']}/user/beers/{api_config['username']}"
    params = {
        "client_id": api_config["client_id"],
        "client_secret": api_config["client_secret"],
    }
    all_beers = []
    offset = 0

    while True:
        params["offset"] = offset
        response = requests.get(url, params=params)
        print(f"Fetching beers with offset {offset}... Response: {response.status_code}")
        if response.status_code == 429:
            print("Rate limit reached. Waiting for 60 seconds...")
            time.sleep(60)
            continue

        response.raise_for_status()

        data = response.json()
        beers = data["response"]["beers"]["items"]
        all_beers.extend(beers)

        # Check if there are more beers to fetch
        if len(beers) < 25:  # Default page size is 25; if less, we've reached the end
            break

        offset += 25  # Move to the next page

    print(f"Total beers fetched: {len(all_beers)}")
    return all_beers

# Fetch detailed beer information by beer_id
def fetch_beer_details(api_config, beer_id):
    url = f"{api_config['base_url']}/beer/info/{beer_id}"
    params = {
        "client_id": api_config["client_id"],
        "client_secret": api_config["client_secret"],
    }

    while True:
        response = requests.get(url, params=params)
        print(f"Fetching details for beer_id {beer_id}... Response: {response.status_code}")
        
        if response.status_code == 429:  # Handle rate limiting
            print("Rate limit reached. Waiting for 60 seconds...")
            time.sleep(60)
            continue

        response.raise_for_status()
        return response.json()

# Insert or update brewery details
def upsert_brewery(cursor, brewery_details):
    query = """
    INSERT INTO breweries (
        brewery_id, name, slug, brewery_type, page_url, label,
        country, city, state, latitude, longitude, description, website
    ) VALUES (
        %(brewery_id)s, %(name)s, %(slug)s, %(brewery_type)s, %(page_url)s, %(label)s,
        %(country)s, %(city)s, %(state)s, %(latitude)s, %(longitude)s, %(description)s, %(website)s
    )
    ON CONFLICT (brewery_id) DO UPDATE SET
        name = EXCLUDED.name,
        slug = EXCLUDED.slug,
        brewery_type = EXCLUDED.brewery_type,
        page_url = EXCLUDED.page_url,
        label = EXCLUDED.label,
        country = EXCLUDED.country,
        city = EXCLUDED.city,
        state = EXCLUDED.state,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        description = EXCLUDED.description,
        website = EXCLUDED.website;
    """
    brewery_data = {
        "brewery_id": brewery_details["brewery_id"],
        "name": brewery_details["brewery_name"],
        "slug": brewery_details["brewery_slug"],
        "brewery_type": brewery_details["brewery_type"],
        "page_url": brewery_details.get("brewery_page_url", ""),  
        "label": brewery_details.get("brewery_label", ""),
        "country": brewery_details["country_name"],
        "city": brewery_details["location"]["brewery_city"],
        "state": brewery_details["location"]["brewery_state"],
        "latitude": brewery_details["location"].get("lat", None),
        "longitude": brewery_details["location"].get("lng", None),
        "description": brewery_details.get("brewery_description", ""),
        "website": brewery_details["contact"].get("url", ""),
    }
    cursor.execute(query, brewery_data)

# Main function to process beers and breweries
def main():
    print("Starting the script...")
    # Load configuration
    config = load_config()

    # Connect to the database
    print("Connecting to database...")
    try:
        connection = psycopg2.connect(**config["database"])
        print("Connected to database successfully.")
    except Exception as e:
        print(f"Database connection failed: {e}")
        return

    cursor = connection.cursor()

    try:
        # Fetch user-rated beers
        rated_beers = fetch_rated_beers(config["api"])

        # Use tqdm for progress tracking
        for rated_beer in tqdm(rated_beers, desc="Processing Beers"):
            beer_id = rated_beer["beer"]["bid"]

            # Fetch beer details
            beer_details = fetch_beer_details(config["api"], beer_id)["response"]["beer"]

            # Upsert brewery details
            brewery_details = beer_details["brewery"]
            upsert_brewery(cursor, brewery_details)

            # Prepare beer data
            beer_data = {
                "beer_id": beer_details["bid"],
                "name": beer_details["beer_name"],
                "label": beer_details["beer_label"],
                "label_hd": beer_details["beer_label_hd"],
                "abv": beer_details["beer_abv"],
                "ibu": beer_details["beer_ibu"],
                "style": beer_details["beer_style"],
                "description": beer_details["beer_description"],
                "is_in_production": bool(beer_details["is_in_production"]),
                "is_homebrew": bool(beer_details["is_homebrew"]),
                "slug": beer_details["beer_slug"],
                "created_at": beer_details["created_at"],
                "rating_count": beer_details["rating_count"],
                "rating_score": beer_details["rating_score"],
                "total_count": beer_details["stats"]["total_count"],
                "monthly_count": beer_details["stats"]["monthly_count"],
                "total_user_count": beer_details["stats"]["total_user_count"],
                "user_count": beer_details["stats"]["user_count"],
                "weighted_rating_score": beer_details["weighted_rating_score"],
                "active": bool(beer_details["beer_active"]),
            }

            # Insert or update beer record
            # Your existing logic for beer upsert should go here

            connection.commit()  # Commit after processing each beer

        print("Beer and brewery data updated successfully!")

    except Exception as e:
        print(f"Error: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Unexpected error: {e}")