import time
import psycopg2
import requests
import yaml
from tqdm import tqdm

# Load configuration from YAML file
def load_config(filepath="config.yaml"):
    with open(filepath, "r") as file:
        return yaml.safe_load(file)

# Fetch beer details from the Untappd API
def fetch_beer_details(api_config, beer_id):
    url = f"{api_config['base_url']}/beer/info/{beer_id}"
    params = {
        "client_id": api_config["client_id"],
        "client_secret": api_config["client_secret"],
    }
    while True:
        response = requests.get(url, params=params)
        if response.status_code == 429:  # Rate limit exceeded
            print("Rate limit reached. Waiting for 60 seconds...")
            time.sleep(60)
            continue
        response.raise_for_status()
        return response.json()

# Update brewery_id for a beer in the database
def update_brewery_id(cursor, beer_id, brewery_id):
    query = """
    UPDATE beers
    SET brewery_id = %s
    WHERE beer_id = %s;
    """
    cursor.execute(query, (brewery_id, beer_id))

# Main function to populate brewery_id in beers table
def main():
    print("Starting the script...")
    
    # Load configuration
    config = load_config()

    # Connect to the database
    print("Connecting to database...")
    connection = psycopg2.connect(**config["database"])
    cursor = connection.cursor()
    print("Connected to database successfully.")

    try:
        # Fetch all beers from the database
        cursor.execute("SELECT beer_id FROM beers WHERE brewery_id IS NULL;")
        beers_to_update = cursor.fetchall()
        print(f"Found {len(beers_to_update)} beers to update.")

        # Iterate through beers and fetch brewery_id
        for beer_id, in tqdm(beers_to_update, desc="Updating Brewery IDs"):
            try:
                # Fetch beer details from the API
                beer_details = fetch_beer_details(config["api"], beer_id)["response"]["beer"]
                brewery_id = beer_details["brewery"]["brewery_id"]

                # Update the brewery_id in the database
                update_brewery_id(cursor, beer_id, brewery_id)
                connection.commit()

            except Exception as e:
                print(f"Error processing beer_id {beer_id}: {e}")
                connection.rollback()

        print("Brewery IDs updated successfully.")

    except Exception as e:
        print(f"Error: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    main()