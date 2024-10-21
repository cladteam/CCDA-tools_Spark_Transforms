#!/usr/bin/env python3
import requests

def get_oid_title(oid):
    """
    Accepts an OID and performs an HTTP GET request to fetch the corresponding
    JSON data from the provided URL, then returns the 'title' element from the response.

    Parameters:
    oid (str): The OID value to lookup

    Returns:
    str: The title associated with the OID, or an error message if not found
    """
    url = f'https://phinvads.cdc.gov/baseStu3/CodeSystem/{oid}'
    
    try:
        # Perform the HTTP GET request
        response = requests.get(url)
        
        # Raise an HTTPError if the request was unsuccessful
        response.raise_for_status()
        
        # Parse the JSON response
        data = response.json()
        
        # Return the 'title' element from the JSON response
        return data.get('title', 'Title not found in response')
    
    except requests.exceptions.RequestException as e:
        return f"An error occurred: {e}"

# Example usage (comment out or remove this if you're importing the script)
#if __name__ == '__main__':
#    oid = '2.16.840.1.113883.5.4'  # Replace with the actual OID for testing
#    print(get_oid_title(oid))
