import os
from dotenv import load_dotenv
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

# -----------------------------
# Load API key from .env
# -----------------------------
dotenv_path = "d:/Nasa Space/Data_Analisys/.env"  # adjust path if needed
load_dotenv(dotenv_path)
API_KEY = os.getenv("OPENAQ_API_KEY")

if not API_KEY:
    raise ValueError("❌ Missing OPENAQ_API_KEY. Check your .env file path or contents.")

print(f"🔐 Loaded API key: {API_KEY[:10]}... (hidden)")

BASE_URL = "https://api.openaq.org/v3"
HEADERS = {"X-API-Key": API_KEY}
PARAMETERS = ["no2", "o3", "pm25"]

# -----------------------------
# Helper functions
# -----------------------------
def get_nearby_sensor(lat, lon, parameter, radius_km):
    """Get the nearest sensor ID for a specific pollutant."""
    url = f"{BASE_URL}/locations"
    params = {
        "coordinates": f"{lat},{lon}",
        "radius": radius_km * 1000,
        "parameter": parameter,
        "limit": 5
    }
    response = requests.get(url, params=params, headers=HEADERS)
    response.raise_for_status()
    data = response.json()

    if not data.get("results"):
        return None, None

    for loc in data["results"]:
        for sensor in loc.get("sensors", []):
            param_info = sensor.get("parameter", {})
            if param_info.get("id") == parameter or param_info.get("name") == parameter:
                return sensor["id"], loc["name"]

    return None, None


def get_measurements(sensor_id, start_time, end_time):
    """Fetch hourly measurements for a sensor, handling varied JSON structures."""
    url = f"{BASE_URL}/sensors/{sensor_id}/measurements/hourly"
    params = {"date_from": start_time, "date_to": end_time, "limit": 1000}
    response = requests.get(url, params=params, headers=HEADERS)
    response.raise_for_status()
    data = response.json()

    if not data.get("results"):
        return pd.DataFrame()

    records = []
    for r in data["results"]:
        ts = (
            r.get("datetime") or r.get("timestamp") or r.get("date") or r.get("datetimeUtc")
        )
        if isinstance(ts, dict):
            ts = ts.get("utc") or ts.get("local") or ts.get("utc_datetime")
        val = r.get("value") or r.get("measurement") or None
        if ts and val is not None:
            records.append({"time": ts, "value": val})

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["time"])
    return df


def get_pollutant_data(lat, lon, parameter):
    """Try different radii and time windows to get data for one pollutant."""
    radius_options = [25, 50, 100]
    day_options = [1, 3, 7]

    for radius in radius_options:
        for days in day_options:
            try:
                sensor_id, loc_name = get_nearby_sensor(lat, lon, parameter, radius)
                if not sensor_id:
                    continue

                start = "2025-07-11T00:00:00Z"
                end = "2025-07-12T23:59:59Z"

                print(f"🌫️ Trying {parameter.upper()} | {loc_name} | Radius={radius} km | Days={days}")
                df = get_measurements(sensor_id, start, end)
                if not df.empty:
                    df = df.rename(columns={"value": parameter})
                    print(f"✅ Found {len(df)} records for {parameter.upper()} at {loc_name}")
                    return df
            except Exception as e:
                print(f"⚠️ Error retrieving {parameter.upper()} ({radius} km/{days} days): {e}")

    print(f"⚠️ No {parameter.upper()} data found after expanding radius/time.")
    return pd.DataFrame()


# -----------------------------
# Main ETL
# -----------------------------
def get_measurements_all(lat, lon):
    pollutant_dfs = []
    for parameter in PARAMETERS:
        df = get_pollutant_data(lat, lon, parameter)
        if not df.empty:
            pollutant_dfs.append(df.set_index("time"))

    if not pollutant_dfs:
        print("❌ No pollutant data found even after expanding radius/time.")
        return pd.DataFrame()

    merged = pd.concat(pollutant_dfs, axis=1, join="outer").sort_index().reset_index()
    merged["latitude"] = lat
    merged["longitude"] = lon

    cols = ["latitude", "longitude", "time", "no2", "o3", "pm25"]
    for c in cols:
        if c not in merged.columns:
            merged[c] = None

    merged = merged[cols]
    print(f"✅ Final dataset with {len(merged)} records.")
    return merged


# -----------------------------
# Run Example
# -----------------------------
if __name__ == "__main__":
    lat, lon = 49.2827, -123.1207  # Vancouver
    df_meas = get_measurements_all(lat, lon)

    if not df_meas.empty:
        output_file = "measurements_vancouver.csv"
        df_meas.to_csv(output_file, index=False)
        print(f"💾 Saved to {output_file}")
        print(df_meas.head())
    else:
        print("⚠️ No measurement data available for this region at this time.")
