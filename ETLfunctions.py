import requests, csv, ast, os, re, json
import pandas as pd
from pandas import DataFrame

# Temporal objects

# Cities and Countries data base
df_cities = pd.read_csv("cities.csv", encoding='utf-8')
df_cities = df_cities[['name', 'country_name']]
df_cities = df_cities.dropna()
df_cities.drop_duplicates(subset='name')

# Country codes data base
df_countrycodes = pd.read_csv("country-codes.csv", encoding='utf-8')
df_countrycodes['country'] = df_countrycodes['UNTERM English Short']
df_countrycodes = df_countrycodes[['Dial', 'country']]
df_countrycodes = df_countrycodes.dropna()
df_countrycodes.drop_duplicates(subset='country')

def collect_all_contacts(token, params=None):
    """
    Collects all contacts from a HubSpot account, paginating through the results.

    Args:
        token (str): The HubSpot API key.
        params (dict): Additional parameters for the request (optional).

    Returns:
        str: A message indicating the successful collection of all contacts.
    """

    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    all_contacts = []

    # Loop to paginate through the results
    while True:
        response = requests.post(url, headers=headers, json=params)
        response.raise_for_status()

        contacts = response.json()["results"]
        all_contacts.extend(contacts)

        # Exit the loop if there are no more pages
        paging = response.json().get("paging")
        if not paging or not paging.get("next"):
            break

        # Get the next batch of contacts
        params["after"] = paging["next"]["after"]
    
    # Convert the list of contacts to a DataFrame
    all_contacts_df = pd.DataFrame(all_contacts)

    # Save the DataFrame to a CSV file
    all_contacts_df.to_csv('all_contacts.csv', index=False, encoding='utf-8')

    return 'All contacts have been collected.'

def collectContacts(token, params=None):
    """
    Collects the specified contacts from a HubSpot account, paginating through the results.

    Args:
      token (str): HubSpot API token.
      params (dict): Additional parameters for the request (optional).

    Returns:
      Generator: A generator for each contacts batch. Additionally, this function saves a file 
      named contacts_xxx.csv in the root path with 100 contacts' information in each iteration.
    """

    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    batch_num = 1
    # Loop to paginate through the results
    while True:
        response = requests.post(url, headers=headers, json=params)
        response.raise_for_status()

        contacts = response.json()["results"]
        
        # Generate a unique filename
        filename = f"contacts_{batch_num:03}.csv"
        df = pd.DataFrame(contacts)
        df.to_csv(filename, index=False, encoding='utf-8')
        
        yield df
        # Exit the loop if there are no more pages or after two iterations (for testing purposes)
        paging = response.json().get("paging")
        if not paging or not paging.get("next") or batch_num == 2:
            break

        # Get the next batch of contacts
        params["after"] = paging["next"]["after"]
        batch_num += 1

    return df

def orderContacts(filename):
    """
    Reads a CSV file containing contact information, converts and orders the data, and saves it to a new CSV file.

    Args:
      filename (str): The name of the CSV file to process.

    Returns:
      None: The function doesn't return a value, but it prints messages about the file processing steps.
    """

    # Reading the CSV file with pandas
    df_contacts = pd.read_csv(filename)
    
    # Converting strings from the 'properties' field to dictionaries
    registers = [ast.literal_eval(register) for register in df_contacts['properties']]

    # Converting the list of dictionaries to a DataFrame
    df_contacts = pd.DataFrame(registers)

    # Filtering and ordering fields
    column_names = ["hs_object_id", "raw_email", "address", "country", "phone", "industry", "createdate"]
    df_contacts = df_contacts[column_names]

    # Removing the processed CSV file
    path_old_csv = os.path.join(os.getcwd(), filename)
    # Check if the file exists before attempting to delete it
    if os.path.exists(filename):
        # Delete the file
        os.remove(filename)
        print(f"The file {filename} has been deleted.")
    else:
        print(f"The file {filename} does not exist in the folder.")
    
    # Saving a new CSV file with the correct fields
    new_filename = f'New_{filename}'
    df_contacts.to_csv(new_filename, index=False, encoding='utf-8')
    print(f"The file {new_filename} has been generated.")

def countryCity(register):
    """
    Searches for a country and city pair based on the provided input.

    Args:
      register (str): The name of a country or city to search for.

    Returns:
      tuple: A tuple containing the country and city names. If not found, returns an empty string for both.
    """

    # Searching for the country in the 'country_name' column
    df_pair = df_cities[df_cities['country_name'] == register]
    if len(df_pair['country_name']) >= 1:
        country_city = (register, '')
    else:
        # Searching for the city in the 'name' column
        df_pair = df_cities[df_cities['name'] == register]
        if len(df_pair['name']) >= 1:
            city_country = tuple(df_pair.iloc[0])
            country_city = (city_country[1], city_country[0])
        else:
            country_city = ('', '')
    return country_city

def findEmail(register):
    """
    Finds an email address in the provided text using a regular expression.

    Args:
      register (str): The text to search for email addresses.

    Returns:
      str: The first email address found in the text, or an empty string if none is found.
    """

    # Regular expression pattern to search for email addresses
    email_chain = r'\b[A-Za-z0-9._-]+@[A-Za-z0-9.-]+\.[A-Za-z0-9.-]{2,}\b'

    # Search for matches in the text
    email = re.findall(email_chain, register)
    
    # Return the first match if email addresses are found, otherwise return an empty string
    return email[0] if email else ''

def fixPhoneNumber(phone, country):
    """
    Fixes the phone number format based on the country's dialing code.

    Args:
      phone (str): The original phone number to be formatted.
      country (str): The country associated with the phone number.

    Returns:
      str: The formatted phone number, including the country's dialing code.
    """

    # Remove non-numeric characters from the phone number
    phone = re.sub(r'[^0-9]', '', phone)
    
    # Convert the country name to lowercase and strip leading/trailing whitespaces
    country = country.lower().strip()
    
    # Retrieve country codes DataFrame
    df_codes = df_countrycodes
    df_codes['country'] = df_codes['country'].str.lower()
    df_codes['country'] = df_codes['country'].str.strip()
    
    if len(phone) == 0:
        new_phone = ''
    else:
        # Find the dialing code for the given country
        df_indicative = df_codes[df_codes['country'].str.contains(country)]
        if not df_indicative.empty:
            indicative = df_indicative['Dial'].iloc[0]
            number = str(int(phone))
            new_phone = '(+' + indicative + ') ' + number[:-6] + ' ' + number[-6:]
        else:
            # If the country code is unknown, include 'unknow' in the formatted phone number
            number = str(int(phone))
            new_phone = '(+' + 'unknow' + ') ' + number[:-6] + ' ' + number[-6:]

    return new_phone

def formatDate(register):
    """
    Formats a date string by removing hyphens and converting it to an integer.

    Args:
      register (str): The date string to be formatted.

    Returns:
      int: The formatted date as an integer.
        Example: 20240217
    """
    # Convert the input to string to ensure consistency
    register = str(register)
    
    # Extract the first 10 characters, replace hyphens, and convert to an integer
    formatted_date = int(register[:10].replace('-', ''))

    return formatted_date

def duplicatesManagement(df):
    """
    Manages duplicate records in a DataFrame based on the 'email' column.

    Args:
      df (pandas.DataFrame): The DataFrame to process.

    Returns:
      pandas.DataFrame: The DataFrame without duplicate records based on the 'email' column,
                       keeping the first occurrence.
    """

    # Sort by 'original_create_date' in ascending order
    df.sort_values(by='createdate', ascending=False, inplace=True)
    
    # Drop duplicates based on the 'email' column, keeping the first occurrence
    df = df.drop_duplicates(subset=['email'], keep='first')

    # # Identify duplicate records based on 'email'
    # duplicates = df[df.duplicated(subset=['email'], keep=False)]

    # # Iterate over duplicate records and merge information
    # for _, group in duplicates.groupby(['email']):
    #     # Get the index of the most recent record
    #     newer_register = group.idxmax()['createdate']

    #     # Merge information from all rows into the most recent one
    #     for i in group.index:
    #         if i != newer_register:
    #             for col in df.columns:
    #                 if pd.isna(df.loc[newer_register, col]) and not pd.isna(df.loc[i, col]):
    #                     df.loc[newer_register, col] = df.loc[i, col]

    #         df.loc[newer_register, 'Industry'] += ';' + df.loc[i, 'Industry'].split(';')

    # # Drop duplicate records
    # df.drop(group.index, inplace=True)

    # # Remove ';' at the beginning of the 'Industry' column
    # df['Industry'] = df['Industry'].str.lstrip(';')

    return df

def uploadContacts(token, df_contacts, batch_size=100):
    """
    Uploads contact data to the specified HubSpot API endpoint.

    Args:
        token (str): HubSpot API token for authentication.
        df_contacts (pandas.DataFrame): DataFrame containing contact data.
        batch_size (int, optional): Number of contacts to upload in each batch. Defaults to 100.

    Returns:
        None
    """

    # Drop the 'createdate' column from df_contacts
    df_contacts = df_contacts.drop(columns='createdate')

    # API endpoint URL
    url = "https://api.hubapi.com/crm/v3/objects/contacts"

    # Headers for the API request
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Loop through each row in df_contacts
    for i, row in df_contacts.iterrows():
        # Try statement for possibles errors, e.g. when contact exist in the account
        try:
            # Extract data for each contact from the DataFrame
            hs_object_id = row['hs_object_id']
            email = row['email']
            address = row['address']
            country = row['country']
            phone = row['phone']
            original_industry = row.get('original_industry', None)  # Using .get() to handle missing column
            city = row['city']
            original_create_date = row['original_create_date']

            # Create the JSON payload with the required structure
            params = {
                "properties": {
                    "temporary_id": hs_object_id,
                    "email": email,
                    "address": address,
                    "country": country,
                    "phone": phone,
                    "original_industry": original_industry,
                    "city": city,
                    "original_create_date": original_create_date,
                }
            }

            # Send data to the API using POST request
            response = requests.post(url, headers=headers, json=params)

            # Check the response status code and print messages accordingly
            if response.status_code > 400:
                print(f"Error uploading data. Status code: {response.status_code}")
                print(response.text)

        except Exception as e:
            print(f"An error occurred: {e}")
            continue