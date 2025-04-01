import requests
from bs4 import BeautifulSoup
import json
import pandas as pd # For easier data handling

url = 'https://agenz.ma/fr/prix-immobilier-maroc/rabat-sale-kenitra/temara/harhoura?layer=plan&lat=33.91171471416537&lng=-6.971995400829911&zoom=14' # Example URL from your JSON

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    # Add other headers if needed
}

try:
    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status() # Check for HTTP errors

    soup = BeautifulSoup(response.content, 'html.parser')
    script_tag = soup.find('script', {'id': '__NEXT_DATA__'})

    if script_tag:
        json_data = json.loads(script_tag.string)

        # --- Explore and Extract ---
        page_props = json_data.get('props', {}).get('pageProps', {})

        # Example: Get quartier prices
        quartiers_data = page_props.get('quartiersSSR', [])
        if quartiers_data:
            df_quartiers = pd.DataFrame(quartiers_data)
            print("Quartier Prices:")
            print(df_quartiers[['quartier', 'prix_appartement', 'prix_villa', 'indice_confiance_appartement', 'indice_confiance_villa']])
            # df_quartiers.to_csv('quartiers_harhoura.csv', index=False)

        # Example: Get initial listings (Annonces de vente)
        # Look for listings within the nested structure, e.g., under props.pageProps or similar keys
        # The exact key might vary depending on the page structure within Next.js
        # Based on your HTML, it seems listings might be under a component's props within pageProps
        # Let's assume 'annonces' is the key (as seen in your sample HTML)
        listings_data = page_props.get('annonces', []) # Adjust key if necessary by inspecting the JSON
        if not listings_data:
             # Sometimes data is nested deeper, inspect the json_data['props']['pageProps'] carefully
             # It might be under a key like 'initialState', 'carteprix', etc.
             # In your sample, 'annonces' is inside 'carteprix'
             listings_data = json_data.get('props', {}).get('initialState', {}).get('carteprix', {}).get('annonces', [])

        if listings_data:
            df_listings = pd.DataFrame(listings_data)
            print("\nInitial Listings:")
            # Select relevant columns
            print(df_listings[['_id', 'type', 'surface', 'typologie', 'sdb', 'prix', 'quartier', 'url']])
            # df_listings.to_csv('listings_harhoura.csv', index=False)

        # Example: Get initial sold properties (Dernières ventes)
        sold_data = page_props.get('transactions', []) # Adjust key if needed
        if not sold_data:
            sold_data = json_data.get('props', {}).get('initialState', {}).get('carteprix', {}).get('transactions', [])

        if sold_data:
             df_sold = pd.DataFrame(sold_data)
             print("\nSold Properties:")
             print(df_sold[['_id', 'type', 'surface', 'typologie', 'sdb', 'prix', 'quartier', 'url', 'dateTransactions']])
             # df_sold.to_csv('sold_harhoura.csv', index=False)


    else:
        print("Could not find __NEXT_DATA__ script tag.")

except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")
except json.JSONDecodeError:
    print("Failed to parse JSON from __NEXT_DATA__.")
except Exception as e:
    print(f"An error occurred: {e}")