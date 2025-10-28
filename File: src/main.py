import pandas as pd
import requests
from datetime import datetime

TOKEN = "xiXORcxkXP3ggwy4cc2v"
ZONE = "US-CAL-CISO"

def read_sacct_file(filepath):
    df = pd.read_csv(filepath, sep="|")

    df.columns = df.columns.str.strip()  # clean column names

    columns_needed = [
        "User",
        "Start",
        "End",
        "ConsumedEnergyRaw",
        "AllocTRES"
    ]
    df = df[columns_needed]

    df["Start"] = pd.to_datetime(df["Start"], errors="coerce")
    df["End"] = pd.to_datetime(df["End"], errors="coerce")

    df = df.dropna(subset=["Start", "End", "ConsumedEnergyRaw"])
    df = df[df["ConsumedEnergyRaw"] > 0]

    df["energy_kWh"] = df["ConsumedEnergyRaw"] / 3_600_000

    # Temporary job key
    df["job_key"] = df["User"] + "_" + df["Start"].astype(str)

    df["is_gpu_job"] = df["AllocTRES"].str.contains("gres/gpu", na=False)
    df["job_type"] = df["is_gpu_job"].apply(lambda x: "GPU" if x else "CPU")

    return df

def fetch_carbon_intensity_past_range(start, end):
    # Ensure start and end are in ISO format
    if isinstance(start, (str, datetime)):
        start = pd.to_datetime(start).isoformat()
    if isinstance(end, (str, datetime)):
        end = pd.to_datetime(end).isoformat()
    
    url = "https://api.electricitymaps.com/v3/carbon-intensity/past-range"
    headers = {"auth-token": TOKEN}
    params = {
        "zone": ZONE,
        "start": start,
        "end": end
    }
    
    r = requests.get(url, headers=headers, params=params)

    if r.status_code == 200:
        data = r.json()
        # Extract data from the response
        ci_df = pd.DataFrame(data["data"])
        ci_df["datetime"] = pd.to_datetime(ci_df["datetime"])
        ci_df.rename(columns={"carbonIntensity": "ci_g_per_kWh"}, inplace=True)
        return ci_df[["datetime", "ci_g_per_kWh"]]
    else:
        print("API error:", r.status_code, r.text)
        return None


def main():
    df = read_sacct_file("sacct_serc_9-16_to_10-16.csv")

    # Round job times to hours first to determine what we need
    if df["Start"].dt.tz is None:
        df["Start_rounded"] = df["Start"].dt.floor("1h").dt.tz_localize('UTC')
    else:
        df["Start_rounded"] = df["Start"].dt.tz_convert('UTC').dt.floor("1h")
    
    # Determine the date range from rounded data with buffer
    min_start = df["Start_rounded"].min() - pd.Timedelta(hours=1)
    max_end = df["Start_rounded"].max() + pd.Timedelta(hours=1)
    
    print(f"Fetching CI data for range: {min_start} to {max_end}")
    
    # Split into 10-day chunks due to API limit
    ci_dfs = []
    current_start = min_start
    while current_start < max_end:
        current_end = min(current_start + pd.Timedelta(days=10), max_end)
        print(f"Fetching chunk: {current_start} to {current_end}")
        chunk_df = fetch_carbon_intensity_past_range(
            start=current_start.isoformat(),
            end=current_end.isoformat()
        )
        if chunk_df is not None:
            ci_dfs.append(chunk_df)
        current_start = current_end
    
    if not ci_dfs:
        print("No carbon intensity data retrieved")
        return None, None
    
    ci_df = pd.concat(ci_dfs).drop_duplicates(subset=['datetime']).sort_values('datetime')
    
    print(f"Fetched CI data: {len(ci_df)} records from {ci_df['datetime'].min()} to {ci_df['datetime'].max()}")
    print(f"Required job times: {df['Start_rounded'].min()} to {df['Start_rounded'].max()}")
    
    df = df.merge(ci_df, left_on="Start_rounded", right_on="datetime", how="left")

    # compute emissions (kg CO2e)
    df["emissions_kg"] = (df["energy_kWh"] * df["ci_g_per_kWh"]) / 1000

    print(f"\nAfter merge: {df['ci_g_per_kWh'].notna().sum()}/{len(df)} jobs matched with CI data")
    
    # Calculate total emissions
    total_emissions = df['emissions_kg'].sum()
    total_energy = df['energy_kWh'].sum()
    jobs_with_emissions = df['emissions_kg'].notna().sum()
    
    # Calculate weighted average CI for jobs with emissions data
    df_with_ci = df[df['ci_g_per_kWh'].notna()]
    if len(df_with_ci) > 0:
        weighted_avg_ci = (df_with_ci['energy_kWh'] * df_with_ci['ci_g_per_kWh']).sum() / df_with_ci['energy_kWh'].sum()
    else:
        weighted_avg_ci = 0
    
    print(f"\n{'='*60}")
    print(f"SUMMARY:")
    print(f"{'='*60}")
    print(f"Total jobs: {len(df)}")
    print(f"Jobs with emissions data: {jobs_with_emissions}")
    print(f"Jobs without emissions data: {df['emissions_kg'].isna().sum()}")
    print(f"Total energy consumed: {total_energy:.2f} kWh")
    print(f"Total emissions: {total_emissions:.3f} kg CO2e")
    print(f"Weighted average CI: {weighted_avg_ci:.1f} g CO2e/kWh")
    print(f"{'='*60}")
    
    # Save to CSV
    output_file = "job_emissions_output.csv"
    df.to_csv(output_file, index=False)
    print(f"\nResults saved to: {output_file}")
    
    return df, total_emissions



if __name__ == "__main__":
    df, total_emissions = main()
