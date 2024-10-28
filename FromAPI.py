import requests
from dotenv import load_dotenv
import os

load_dotenv()
URL = os.getenv("URL")
APIKey = os.getenv("APIKey")


def get_Vessels():
    LocalURL = URL + "ships"
    Auth = f"ApiKey {APIKey}"
    header = {
        "content-type": "application/json",
        "authorization": Auth
    }
    response = requests.get(LocalURL, headers=header)
    if response.status_code == 200:
        return response.json()
    else:
        return response


def get_Report(IMONum):
    LocalURL = URL + "ships/" + IMONum + "/reportsummaries"
    Auth = f"ApiKey {APIKey}"
    header = {
        "content-type": "application/json",
        "authorization": Auth
    }
    response = requests.get(LocalURL, headers=header)
    if response.status_code == 200:
        return response.json()
    else:
        return response


def get_vessels(params=None):
    LocalURL = URL + "ships"
    Auth = f"ApiKey {APIKey}"
    header = {
        "content-type": "application/json",
        "authorization": Auth
    }
    response = requests.get(LocalURL, headers=header, params=params)
    # Check if the status code indicates an error
    if response.status_code != 200:
        try:
            # Try to print the JSON error response
            error_json = response.json()
            print(f"Error response JSON: {error_json}")
        except ValueError:
            # If the response is not JSON, print the raw text
            print(f"Error response text: {response.text}")

        # Raise an HTTPError with the status code and response content
        raise requests.exceptions.HTTPError(
            f"Request failed with status code {response.status_code}"
        )

    # If successful, return the JSON content
    return response.json()


def get_reports(IMONum, params=None):
    LocalURL = f"{URL}ships/{IMONum}/reportsummaries"
    Auth = f"ApiKey {APIKey}"
    header = {
        "content-type": "application/json",
        "authorization": Auth
    }
    response = requests.get(LocalURL, headers=header, params=params)

    # Check if the status code indicates an error
    if response.status_code != 200:
        try:
            # Try to print the JSON error response
            error_json = response.json()
            print(f"Error response JSON: {error_json}")
        except ValueError:
            # If the response is not JSON, print the raw text
            print(f"Error response text: {response.text}")

        # Raise an HTTPError with the status code and response content
        raise requests.exceptions.HTTPError(
            f"Request failed with status code {response.status_code}"
        )

    # If successful, return the JSON content
    return response.json()


def get_Events(IMONum):
    LocalURL = URL + "ships/" + IMONum + "/events"
    Auth = f"ApiKey {APIKey}"
    header = {
        "content-type": "application/json",
        "authorization": Auth
    }
    response = requests.get(LocalURL, headers=header)
    if response.status_code == 200:
        return response.json()
    else:
        return response


def get_Voyage(IMONum):
    LocalURL = URL + "ships/" + IMONum + "/voyages"
    Auth = f"ApiKey {APIKey}"
    header = {
        "content-type": "application/json",
        "authorization": Auth
    }
    response = requests.get(LocalURL, headers=header)
    if response.status_code == 200:
        return response.json()
    else:
        return response


def Get_VoyageSummary(VoyageID):
    print(VoyageID)
    LocalURL = URL + "voyages/" + VoyageID + "/summary"
    Auth = f"ApiKey {APIKey}"
    header = {
        "content-type": "application/json",
        "authorization": Auth
    }
    response = requests.get(LocalURL, headers=header)
    if response.status_code == 200:
        return response.json()
    else:
        return response


def Get_LegSummary(EventId):
    LocalURL = URL + "legs/" + EventId + "/legSummary"
    Auth = f"ApiKey {APIKey}"
    header = {
        "content-type": "application/json",
        "authorization": Auth
    }
    response = requests.get(LocalURL, headers=header)
    if response.status_code == 200:
        return response.json()
    else:
        return [response, response.text]
