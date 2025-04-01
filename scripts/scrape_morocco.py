import requests
from bs4 import BeautifulSoup
import pandas as pd

def scrape_real_estate_prices():
    url = "https://agenz.ma/fr/prix-immobilier-maroc"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        # Send a GET request to the website
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error for bad status codes
        
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the table containing the prices
        table = soup.find('table', class_='AveragePricesTable_price__table__dZRIO')
        
        if not table:
            print("Could not find the price table on the page.")
            return None
        
        # Extract table headers
        headers = [th.text.strip() for th in table.find('thead').find_all('th')]
        
        # Extract table rows
        rows = []
        for tr in table.find('tbody').find_all('tr'):
            row = []
            for td in tr.find_all('td'):
                # Get city name from link if available
                if td.find('a'):
                    row.append(td.find('a').text.strip())
                else:
                    # Get price value (remove 'MAD' and any whitespace)
                    price_text = td.find('span', class_='AveragePricesTable_price__table__item__uar_X').text
                    price_value = price_text.replace('MAD', '').strip()
                    row.append(price_value)
            rows.append(row)
        
        # Create a DataFrame
        df = pd.DataFrame(rows, columns=headers)
        
        # Clean up the data
        df['Prix m² moyen Appartement'] = df['Prix m² moyen Appartement'].str.replace(' ', '').astype(float)
        df['Prix m² moyen Villa'] = df['Prix m² moyen Villa'].str.replace(' ', '').astype(float)
        
        return df
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching the webpage: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Run the scraper
prices_df = scrape_real_estate_prices()

if prices_df is not None:
    print("Real Estate Prices by City in Morocco:")
    print(prices_df)
    
    # Save to CSV
    prices_df.to_csv('morocco_real_estate_prices.csv', index=False)
    print("\nData saved to 'morocco_real_estate_prices.csv'")