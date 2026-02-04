"""
Automation:
- Fetch data from Metabase query
- Write results to Google Sheets using Sheet ID (URL-based)
"""

import os
import json
import time
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe


def get_env_var(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"❌ Missing required environment variable: {name}")
    return value


def create_metabase_session(metabase_url: str, username: str, password: str) -> dict:
    response = requests.post(
        metabase_url,
        headers={"Content-Type": "application/json"},
        json={"username": username, "password": password},
        timeout=60,
    )
    response.raise_for_status()
    token = response.json()["id"]
    return {
        "Content-Type": "application/json",
        "X-Metabase-Session": token,
    }


def fetch_metabase_query(query_url: str, headers: dict) -> pd.DataFrame:
    response = requests.post(query_url, headers=headers, timeout=120)
    response.raise_for_status()
    return pd.DataFrame(response.json())


def connect_to_gsheet(service_account_json: str) -> gspread.Client:
    creds_info = json.loads(service_account_json)
    creds = Credentials.from_service_account_info(
        creds_info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def update_sheet(
    gc: gspread.Client,
    sheet_key: str,
    worksheet_name: str,
    df: pd.DataFrame,
) -> None:
    sheet = gc.open_by_key(sheet_key)
    worksheet = sheet.worksheet(worksheet_name)
    worksheet.clear()
    set_with_dataframe(
        worksheet,
        df,
        include_index=False,
        include_column_header=True,
    )


def main() -> None:
    start_time = time.time()

    metabase_url = get_env_var("METABASE_URL")
    username = get_env_var("USERNAME")
    password = get_env_var("PRABHAT_SECRET_KEY")
    query_url = get_env_var("Dummy_Automation_Query")
    service_account_json = get_env_var("SERVICE_ACCOUNT_JSON")
    sheet_key = get_env_var("SHEET_ACCESS_KEY")

    worksheet_name = "Test Taken"

    headers = create_metabase_session(
        metabase_url=metabase_url,
        username=username,
        password=password,
    )

    df = fetch_metabase_query(query_url, headers)

    if df.empty:
        print("⚠️ Query returned no data. Sheet update skipped.")
        return

    gc = connect_to_gsheet(service_account_json)
    update_sheet(gc, sheet_key, worksheet_name, df)

    elapsed = time.time() - start_time
    print(f"✅ Automation completed successfully in {elapsed:.2f}s")


if __name__ == "__main__":
    main()
