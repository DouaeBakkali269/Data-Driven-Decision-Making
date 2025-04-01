import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import time

url = 'https://agenz.ma/fr/prix-immobilier-maroc'

# Use a realistic browser User-Agent
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en-US,en;q=0.9,fr;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    # Add other headers if necessary, like Referer, depending on site requirements
}

print(f"Attempting to fetch data from: {url}")

try:
    # Make the request
    response = requests.get(url, headers=headers, timeout=20) # Increased timeout
    response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
    print("Successfully fetched page content.")

    # Parse the HTML
    soup = BeautifulSoup(response.content, 'html.parser')
    print("HTML parsed successfully.")

    # Find the __NEXT_DATA__ script tag
    script_tag = soup.find('script', {'id': '__NEXT_DATA__'})

    if script_tag:
        print("Found __NEXT_DATA__ script tag.")
        # Load the JSON data from the script tag's content
        json_data = json.loads(script_tag.string)
        print("JSON data loaded successfully.")

        # Navigate to the relevant data - based on your provided JSON structure
        page_props = json_data.get('props', {}).get('pageProps', {})
        provinces_data = page_props.get('provincesSSR', []) # Key found in your JSON

        if provinces_data:
            print(f"Found {len(provinces_data)} provinces/cities in the JSON data.")
            # Create a Pandas DataFrame
            df = pd.DataFrame(provinces_data)

            # Select and rename relevant columns for clarity
            df_filtered = df[[
                'province',
                'prix_appartement',
                'prix_villa',
                'region',
                'last_update',
                'indice_confiance_appartement',
                'indice_confiance_villa'
            ]].copy() # Use .copy() to avoid SettingWithCopyWarning

            df_filtered.rename(columns={
                'province': 'City/Province',
                'prix_appartement': 'Avg Price Apartment (MAD/m²)',
                'prix_villa': 'Avg Price Villa (MAD/m²)',
                'region': 'Region',
                'last_update': 'Last Updated',
                'indice_confiance_appartement': 'Confidence Index Apt.',
                'indice_confiance_villa': 'Confidence Index Villa'
            }, inplace=True)

            # Optional: Round the prices for cleaner display
            pd.options.display.float_format = '{:,.2f}'.format # Format numbers in pandas output
            # df_filtered['Avg Price Apartment (MAD/m²)'] = df_filtered['Avg Price Apartment (MAD/m²)'].round(2)
            # df_filtered['Avg Price Villa (MAD/m²)'] = df_filtered['Avg Price Villa (MAD/m²)'].round(2)


            print("\n--- Morocco Real Estate Prices by City/Province ---")
            print(df_filtered.to_string(index=False)) # Use to_string to see all rows/cols if needed

            # --- Save to CSV (optional) ---
            try:
                output_filename = 'morocco_city_real_estate_prices.csv'
                df_filtered.to_csv(output_filename, index=False, encoding='utf-8-sig')
                print(f"\nData successfully saved to {output_filename}")
            except Exception as e:
                print(f"\nError saving data to CSV: {e}")

        else:
            print("Could not find 'provincesSSR' data within the pageProps JSON.")
            print("Page structure might have changed. Inspect the __NEXT_DATA__ content.")

    else:
        print("Error: Could not find the __NEXT_DATA__ script tag in the HTML.")
        print("The website structure might have changed, or Cloudflare might be blocking.")

except requests.exceptions.Timeout:
    print(f"Error: The request to {url} timed out.")
except requests.exceptions.RequestException as e:
    print(f"Error during requests to {url}: {e}")
except json.JSONDecodeError:
    print("Error: Failed to parse JSON data from the __NEXT_DATA__ tag.")
except KeyError as e:
    print(f"Error: Could not find key '{e}' while navigating the JSON structure. Check the structure.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")