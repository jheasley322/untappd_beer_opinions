import psycopg2
import requests
import yaml
import time
from datetime import datetime

# Load configuration from YAML file
def load_config(filepath="config.yaml"):
    with open(filepath, "r") as file:
        return yaml.safe_load(file)

# Fetch all rated beers from the API
def fetch_rated_beers(api_config):
    url = f"{api_config['base_url']}/user/beers/{api_config['username']}"
    params = {
        "client_id": api_config["client_id"],
        "client_secret": api_config["client_secret"],
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# Fetch detailed beer information by beer_id
def fetch_beer_details(api_config, beer_id):
    url = f"{api_config['base_url']}/beer/info/{beer_id}"
    params = {
        "client_id": api_config["client_id"],
        "client_secret": api_config["client_secret"],
    }
    while True:
        response = requests.get(url, params=params)
        
        # Handle rate limiting
        remaining = int(response.headers.get("X-Ratelimit-Remaining", 1))
        limit = int(response.headers.get("X-Ratelimit-Limit", 100))
        if response.status_code == 429 or remaining == 0:
            print(f"Rate limit reached. Waiting for {3600 / limit} seconds...")
            time.sleep(3600 / limit)
            continue
        
        response.raise_for_status()  # Raise exception for other HTTP errors
        time.sleep(max(1, 3600 / limit))  # Add delay based on rate limits
        return response.json()

# Check if the beer exists in the database
def fetch_beer_from_db(cursor, beer_id):
    query = "SELECT beer_id, updated_at FROM beers WHERE beer_id = %s"
    cursor.execute(query, (beer_id,))
    return cursor.fetchone()

# Insert or update a beer record in the database
def upsert_beer(cursor, beer_data, user_data):
    query = """
    INSERT INTO beers (
        beer_id, name, label, label_hd, abv, ibu, style, description,
        is_in_production, is_homebrew, slug, created_at, rating_count,
        rating_score, total_count, monthly_count, total_user_count,
        user_count, weighted_rating_score, active, user_first_had,
        user_recent_checkin, user_timezone, user_rating, user_checkin_count, updated_at
    ) VALUES (
        %(beer_id)s, %(name)s, %(label)s, %(label_hd)s, %(abv)s, %(ibu)s, %(style)s,
        %(description)s, %(is_in_production)s, %(is_homebrew)s, %(slug)s,
        %(created_at)s, %(rating_count)s, %(rating_score)s, %(total_count)s,
        %(monthly_count)s, %(total_user_count)s, %(user_count)s,
        %(weighted_rating_score)s, %(active)s, %(user_first_had)s,
        %(user_recent_checkin)s, %(user_timezone)s, %(user_rating)s,
        %(user_checkin_count)s, NOW()
    )
    ON CONFLICT (beer_id) DO UPDATE SET
        name = EXCLUDED.name,
        label = EXCLUDED.label,
        label_hd = EXCLUDED.label_hd,
        abv = EXCLUDED.abv,
        ibu = EXCLUDED.ibu,
        style = EXCLUDED.style,
        description = EXCLUDED.description,
        is_in_production = EXCLUDED.is_in_production,
        is_homebrew = EXCLUDED.is_homebrew,
        slug = EXCLUDED.slug,
        created_at = EXCLUDED.created_at,
        rating_count = EXCLUDED.rating_count,
        rating_score = EXCLUDED.rating_score,
        total_count = EXCLUDED.total_count,
        monthly_count = EXCLUDED.monthly_count,
        total_user_count = EXCLUDED.total_user_count,
        user_count = EXCLUDED.user_count,
        weighted_rating_score = EXCLUDED.weighted_rating_score,
        active = EXCLUDED.active,
        user_first_had = EXCLUDED.user_first_had,
        user_recent_checkin = EXCLUDED.user_recent_checkin,
        user_timezone = EXCLUDED.user_timezone,
        user_rating = EXCLUDED.user_rating,
        user_checkin_count = EXCLUDED.user_checkin_count,
        updated_at = NOW();
    """
    cursor.execute(query, {**beer_data, **user_data})

# Main function to populate the database
def main():
    # Load configuration
    config = load_config()

    # Connect to the database
    connection = psycopg2.connect(**config["database"])
    cursor = connection.cursor()

    try:
        # Fetch user-rated beers
        rated_beers = fetch_rated_beers(config["api"])["response"]["beers"]["items"]

        # Iterate over each rated beer
        for rated_beer in rated_beers:
            beer_id = rated_beer["beer"]["bid"]

            # Check if beer exists in the database
            db_beer = fetch_beer_from_db(cursor, beer_id)
            if db_beer:
                print(f"Beer {beer_id} already in database. Skipping API call.")
                continue

            # Fetch detailed beer information
            print(f"Fetching details for Beer ID {beer_id}...")
            beer_details = fetch_beer_details(config["api"], beer_id)["response"]["beer"]

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

            # Prepare user data
            user_data = {
                "user_first_had": rated_beer["first_had"],
                "user_recent_checkin": rated_beer["recent_created_at"],
                "user_timezone": rated_beer["recent_created_at_timezone"],
                "user_rating": rated_beer["rating_score"],
                "user_checkin_count": rated_beer["count"],
            }

            # Insert or update beer record
            upsert_beer(cursor, beer_data, user_data)
            connection.commit()  # Commit immediately after processing each beer

        print("Beer data updated successfully!")

    except Exception as e:
        print(f"Error: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    main()