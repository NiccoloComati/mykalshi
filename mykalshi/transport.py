from .session import sign_request, BASE_URL
import requests

# Generic HTTP methods

# Generic GET request
# `endpoint` should be like '/communications/id'
# `params` is a dictionary of URL parameters
def kalshi_get(endpoint, params=None):
    full_path = "/trade-api/v2" + endpoint  # for signing
    url = BASE_URL + endpoint               # for actual request
    headers = sign_request("GET", full_path)
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

# Generic POST request
# `body` is a JSON-serializable dictionary
def kalshi_post(endpoint, body=None):
    full_path = "/trade-api/v2" + endpoint
    url = BASE_URL + endpoint
    headers = sign_request("POST", full_path)
    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()
    return response.json() if response.content else {}

# Generic PUT request
# Used for actions like confirming or accepting quotes
def kalshi_put(endpoint, body=None):
    full_path = "/trade-api/v2" + endpoint
    url = BASE_URL + endpoint
    headers = sign_request("PUT", full_path)
    response = requests.put(url, headers=headers, json=body)
    response.raise_for_status()
    return response.json() if response.content else {}

# Generic DELETE request
# Deletes the resource at the endpoint (quote, RFQ, etc.)
def kalshi_delete(endpoint):
    full_path = "/trade-api/v2" + endpoint
    url = BASE_URL + endpoint
    headers = sign_request("DELETE", full_path)
    response = requests.delete(url, headers=headers)
    response.raise_for_status()
    return response.status_code