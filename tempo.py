"""
TEMPO NO2 Data Extraction to CSV

This script extracts NO2 data from TEMPO satellite measurements for North America.
It retrieves all available NO2 pixels without sampling and exports them to CSV format.

Main features:
- Authenticates with NASA Earthdata
- Downloads and processes TEMPO NO2 Level-3 data
- Filters data by geographical bounds and quality flags
- Exports results to CSV with full spatial resolution
"""

import earthaccess
import xarray as xr
import numpy as np
import pandas as pd
from datetime import datetime, timedelta


def get_all_tempo_no2(bbox, datetime_str):
    """
    Extract all valid TEMPO NO2 pixels for a given region and time.

    Parameters
    ----------
    bbox : tuple
        Bounding box coordinates as (min_lon, min_lat, max_lon, max_lat).
        Longitude range: -180 to 180
        Latitude range: -90 to 90
    datetime_str : str
        UTC datetime in format "YYYY-MM-DD HH:MM:SS"

    Returns
    -------
    pandas.DataFrame
        DataFrame containing extracted NO2 data with columns:
        - latitude : float
            Pixel latitude in degrees
        - longitude : float
            Pixel longitude in degrees
        - NO2_molec_cm2 : float
            NO2 vertical column density in molecules/cm²

    Notes
    -----
    - Searches ±3 hours around the target time to ensure data coverage
    - Only returns pixels that pass quality control (quality flag = 0)
    - Removes duplicate pixels if there's overlap between granules
    - Returns empty DataFrame if no valid data found
    """
    
    print("Authenticating with NASA Earthdata...")
    try:
        earthaccess.login(persist=True)
    except:
        earthaccess.login(strategy="interactive", persist=True)
    
    min_lon, min_lat, max_lon, max_lat = bbox
    target_dt = datetime.fromisoformat(datetime_str.replace(" ", "T"))
    
    # Expand search window to ensure we get data
    start_time = (target_dt - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
    end_time = (target_dt + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"Searching for TEMPO NO2 data...")
    print(f"Area: {bbox}")
    print(f"Time: {datetime_str} UTC (±3 hours)")
    
    # Search for NO2 data
    results = earthaccess.search_data(
        short_name="TEMPO_NO2_L3",
        version="V03",
        bounding_box=(min_lon, min_lat, max_lon, max_lat),
        temporal=(start_time, end_time)
    )
    
    if not results:
        print("No TEMPO data found in this time window")
        return pd.DataFrame()
    
    print(f"Found {len(results)} granule(s)")
    
    all_rows = []
    
    for idx, granule in enumerate(results):
        print(f"\n Processing granule {idx + 1}/{len(results)}...")
        
        try:
            # Open file directly from cloud
            files = earthaccess.open([granule])
            ds = xr.open_dataset(files[0], group=None, engine='h5netcdf')
            
            # Open product group
            try:
                ds_product = xr.open_dataset(files[0], group='product', engine='h5netcdf')
            except:
                ds_product = ds
            
            # Get coordinates
            if 'latitude' in ds.coords:
                lat_vals = ds.coords['latitude'].values
                lon_vals = ds.coords['longitude'].values
            elif 'latitude' in ds.variables:
                lat_vals = ds.variables['latitude'].values
                lon_vals = ds.variables['longitude'].values
            else:
                print("No coordinates found")
                continue
            
            # Get NO2 data
            var_name = "vertical_column_troposphere"
            if var_name not in ds_product.variables:
                print(f"Variable {var_name} not found")
                continue
            
            # Extract data (first time step)
            data = ds_product[var_name].isel(time=0).values
            
            # Get quality flag
            if "main_data_quality_flag" in ds_product.variables:
                qf = ds_product["main_data_quality_flag"].isel(time=0).values
            else:
                qf = np.zeros_like(data, dtype=int)
            
            print(f"   Data shape: {data.shape}")
            print(f"   Lat range: {lat_vals.min():.2f} to {lat_vals.max():.2f}")
            print(f"   Lon range: {lon_vals.min():.2f} to {lon_vals.max():.2f}")
            
            # Subset to bounding box
            lat_mask = (lat_vals >= min_lat) & (lat_vals <= max_lat)
            lon_mask = (lon_vals >= min_lon) & (lon_vals <= max_lon)
            
            if not np.any(lat_mask) or not np.any(lon_mask):
                print("No data in bounding box")
                continue
            
            # Apply spatial mask
            subset_data = data[lat_mask, :][:, lon_mask]
            subset_qf = qf[lat_mask, :][:, lon_mask]
            subset_lat = lat_vals[lat_mask]
            subset_lon = lon_vals[lon_mask]
            
            # Create 2D coordinate grids
            lon_grid, lat_grid = np.meshgrid(subset_lon, subset_lat)
            
            # Filter for valid data
            # Quality flag = 0 (good)
            # Data is finite and positive
            valid_mask = (subset_qf == 0) & np.isfinite(subset_data) & (subset_data > 0)
            
            valid_count = np.sum(valid_mask)
            print(f"   Valid pixels: {valid_count:,} / {valid_mask.size:,}")
            
            if valid_count == 0:
                print("No valid pixels")
                continue
            
            # Extract valid pixels
            valid_lats = lat_grid[valid_mask]
            valid_lons = lon_grid[valid_mask]
            valid_no2 = subset_data[valid_mask]
            
            # Create DataFrame for this granule
            df_granule = pd.DataFrame({
                'latitude': valid_lats,
                'longitude': valid_lons,
                'NO2_molec_cm2': valid_no2
            })
            
            all_rows.append(df_granule)
            print(f"Extracted {len(df_granule):,} pixels")
            
        except Exception as e:
            print(f"Error: {str(e)}")
            continue
    
    if not all_rows:
        print("\n No valid data extracted")
        return pd.DataFrame()
    
    # Combine all granules
    df_final = pd.concat(all_rows, ignore_index=True)
    
    # Remove duplicates (if any overlap between granules)
    initial_count = len(df_final)
    df_final = df_final.drop_duplicates(subset=['latitude', 'longitude'])
    final_count = len(df_final)
    
    if initial_count > final_count:
        print(f"\n Removed {initial_count - final_count:,} duplicate pixels")
    
    print(f"\n Total valid NO2 pixels: {final_count:,}")
    
    return df_final


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    """
    Main execution block for TEMPO NO2 data extraction.
    
    Configures the extraction parameters and runs the process:
    1. Sets geographical bounds for North America
    2. Specifies target datetime (UTC)
    3. Defines output CSV filename
    4. Calls extraction function
    5. Saves and summarizes results
    
    Notes
    -----
    - Best coverage times are 18:00-20:00 UTC (afternoon in North America)
    - Data available from September 2023 onwards
    - Output includes data summary and spatial coverage analysis
    """
    
    # Configuration
    NORTH_AMERICA_BBOX = (-160, 10, -40, 60)
    
    # Use a date with good TEMPO coverage
    # TEMPO launched August 2023, use dates from Sept 2023 onwards
    # Afternoon UTC (18:00-20:00) typically has best coverage
    DATETIME = "2025-07-11 19:00:00"  # UTC - 3PM EST, good coverage time
    
    # Output file
    OUTPUT_CSV = f"tempo_no2_north_america{DATETIME}.csv"
    
    print("="*70)
    print("TEMPO NO2 Data Extraction - Full Resolution")
    print("="*70)
    print(f"Coverage: North America {NORTH_AMERICA_BBOX}")
    print(f"DateTime: {DATETIME} UTC")
    print(f"Output: {OUTPUT_CSV}")
    print(f"Mode: Extract ALL valid pixels (no sampling)")
    print("="*70 + "\n")
    
    # Get ALL NO2 data
    df = get_all_tempo_no2(
        bbox=NORTH_AMERICA_BBOX,
        datetime_str=DATETIME
    )
    
    # Save to CSV
    if len(df) > 0:
        # Sort by latitude, longitude for cleaner output
        df = df.sort_values(['latitude', 'longitude']).reset_index(drop=True)
        
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"\n Saved {len(df):,} rows to {OUTPUT_CSV}")
        
        print(f"\nFirst 10 rows:")
        print(df.head(10))
        
        print(f"\nData Summary:")
        print("="*70)
        print(f"Total pixels: {len(df):,}")
        print(f"\nLatitude range: {df['latitude'].min():.2f}° to {df['latitude'].max():.2f}°")
        print(f"Longitude range: {df['longitude'].min():.2f}° to {df['longitude'].max():.2f}°")
        print(f"\nNO2 Statistics (molec/cm²):")
        print(f"  Min:  {df['NO2_molec_cm2'].min():.2e}")
        print(f"  Max:  {df['NO2_molec_cm2'].max():.2e}")
        print(f"  Mean: {df['NO2_molec_cm2'].mean():.2e}")
        print(f"  Median: {df['NO2_molec_cm2'].median():.2e}")
        
        # Show spatial coverage
        print(f"\nSpatial Coverage:")
        lat_bins = pd.cut(df['latitude'], bins=10)
        print(f"  Pixels per latitude band:")
        print(lat_bins.value_counts().sort_index())
        
    else:
        print("\n No data to save")
        print("\n Tips:")
        print("  • Try a different date/time")
        print("  • TEMPO data available from August 2023 onwards")
        print("  • Best coverage: 18:00-20:00 UTC (afternoon North America)")
        print("  • Check https://www.earthdata.nasa.gov/learn/articles/tempo for data availability")