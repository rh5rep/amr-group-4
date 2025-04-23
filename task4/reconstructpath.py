import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

def load_gps_data(file_path):
    """
    Load GPS data from a CSV file.
    
    Args:
        file_path: Path to the GPS data CSV file
    
    Returns:
        DataFrame containing GPS data
    """
    try:
        data = pd.read_csv(file_path)
        print(f"Successfully loaded GPS data from {file_path}")
        print(f"Data shape: {data.shape}")
        print(f"Columns: {data.columns.tolist()}")
        return data
    except Exception as e:
        print(f"Error loading GPS data: {e}")
        return None

def reconstruct_path(gps_data):
    """
    Reconstruct path from GPS data.
    
    Args:
        gps_data: DataFrame containing GPS data
        
    Returns:
        Path as a series of points (latitude, longitude)
    """
    # Identify latitude and longitude columns based on common naming patterns
    lat_col = None
    lon_col = None
    
    for col in gps_data.columns:
        col_lower = col.lower()
        if any(lat_term in col_lower for lat_term in ['lat', 'latitude']):
            lat_col = col
        elif any(lon_term in col_lower for lon_term in ['lon', 'long', 'longitude']):
            lon_col = col
    
    if not lat_col or not lon_col:
        # Fallback to first two numerical columns if column names aren't clear
        numeric_cols = gps_data.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) >= 2:
            lat_col, lon_col = numeric_cols[:2]
            print(f"Using columns {lat_col} and {lon_col} as latitude and longitude")
        else:
            raise ValueError("Could not identify latitude and longitude columns")
    
    # Extract coordinates as a list of tuples (lat, lon)
    path = list(zip(gps_data[lat_col], gps_data[lon_col]))
    return path, lat_col, lon_col

def visualize_path(path, lat_col, lon_col):
    """
    Visualize the reconstructed path.
    
    Args:
        path: List of (latitude, longitude) points
        lat_col: Name of the latitude column
        lon_col: Name of the longitude column
    """
    if not path:
        print("No path data to visualize")
        return
    
    # Extract lat and lon for plotting
    lats, lons = zip(*path)
    
    plt.figure(figsize=(10, 8))
    plt.plot(lons, lats, 'b-', linewidth=2)  # Plot the path
    plt.plot(lons[0], lats[0], 'go', markersize=10, label='Start')  # Start point
    plt.plot(lons[-1], lats[-1], 'ro', markersize=10, label='End')  # End point
    
    plt.title('Reconstructed ROV Path')
    plt.xlabel(f'Longitude ({lon_col})')
    plt.ylabel(f'Latitude ({lat_col})')
    plt.grid(True)
    plt.legend()
    
    # Save the figure
    plt.savefig('reconstructed_path.png')
    print("Path visualization saved as 'reconstructed_path.png'")
    
    # Show the plot
    plt.show()

def main():
    # Define the path to the GPS data
    gps_file = "gps_data/antenna2_gps.csv"
    
    # Load the GPS data
    gps_data = load_gps_data(gps_file)
    if gps_data is None:
        return
    
    # Reconstruct the path
    path, lat_col, lon_col = reconstruct_path(gps_data)
    print(f"Reconstructed path with {len(path)} points")
    
    # Visualize the path
    visualize_path(path, lat_col, lon_col)

if __name__ == "__main__":
    main()
