from pathlib import Path
import pandas as pd
import pynmea2
from rosbags.highlevel import AnyReader
from rosbags.typesys import Stores, get_typestore

def process_gps_data(bag_file_path):
    """
    Process GPS data from NMEA sentences in the bag file
    
    Args:
        bag_file_path: Path to the bag file containing NMEA data
    
    Returns:
        Tuple of DataFrames containing processed GPS data for each antenna
    """
    # Create empty lists to store data for each antenna
    gps1_data = []  # $GPGGA - antenna 1
    gps2_data = []  # $GNGGA - antenna 2
    gps3_data = []  # $GZGGA - antenna 3
    
    # Set up ROS bag reader
    typestore = get_typestore(Stores.ROS2_FOXY)
    
    # Open the bag file
    with AnyReader([Path(bag_file_path)], default_typestore=typestore) as reader:
        # Get connections for the NMEA topic
        connections = {c.topic: c for c in reader.connections}
        
        if '/nmea_sentence' not in connections:
            print("Error: /nmea_sentence topic not found in the bag file")
            return None, None, None
        
        # Process messages from the NMEA topic
        for con, ts, raw in reader.messages(connections=[connections['/nmea_sentence']]):
            msg = reader.deserialize(raw, con.msgtype)
            timestamp = ts * 1e-9  # Convert nanoseconds to seconds
            
            try:
                # Parse the NMEA sentence
                nmea_str = msg.sentence
                # print(f"Raw NMEA sentence: {nmea_str}")  
                if nmea_str.startswith('$GPGGA'):  # Antenna 1
                    msg_parsed = pynmea2.parse(nmea_str)
                    if msg_parsed.lat and msg_parsed.lon:  # Only add if lat/lon data exists
                        lat = convert_latitude(msg_parsed.lat, msg_parsed.lat_dir)
                        lon = convert_longitude(msg_parsed.lon, msg_parsed.lon_dir)
                        gps1_data.append([timestamp, lat, lon, msg_parsed.altitude])
                        
                elif nmea_str.startswith('$GNGGA'):  # Antenna 2
                    msg_parsed = pynmea2.parse(nmea_str)
                    if msg_parsed.lat and msg_parsed.lon:
                        lat = convert_latitude(msg_parsed.lat, msg_parsed.lat_dir)
                        lon = convert_longitude(msg_parsed.lon, msg_parsed.lon_dir)
                        gps2_data.append([timestamp, lat, lon, msg_parsed.altitude])
                        
                elif nmea_str.startswith('$GZGGA'):  # Antenna 3
                    msg_parsed = pynmea2.parse(nmea_str)
                    if msg_parsed.lat and msg_parsed.lon:
                        lat = convert_latitude(msg_parsed.lat, msg_parsed.lat_dir)
                        lon = convert_longitude(msg_parsed.lon, msg_parsed.lon_dir)
                        gps3_data.append([timestamp, lat, lon, msg_parsed.altitude])
                        
            except Exception as e:
                # Skip problematic sentences
                continue
    
    # Convert to DataFrames
    columns = ['timestamp', 'latitude', 'longitude', 'altitude']
    df_gps1 = pd.DataFrame(gps1_data, columns=columns)
    df_gps2 = pd.DataFrame(gps2_data, columns=columns)
    df_gps3 = pd.DataFrame(gps3_data, columns=columns)
    
    return df_gps1, df_gps2, df_gps3

def convert_latitude(lat_str, direction):
    """
    Convert latitude from NMEA format to decimal degrees
    """
    # NMEA format is DDMM.MMMM
    if not lat_str:
        return None
        
    degrees = int(lat_str[:2])
    minutes = float(lat_str[2:])
    latitude = degrees + minutes/60
    
    if direction == 'S':
        latitude = -latitude
        
    return latitude

def convert_longitude(lon_str, direction):
    """
    Convert longitude from NMEA format to decimal degrees
    """
    # NMEA format is DDDMM.MMMM
    if not lon_str:
        return None
        
    degrees = int(lon_str[:3])
    minutes = float(lon_str[3:])
    longitude = degrees + minutes/60
    
    if direction == 'W':
        longitude = -longitude
        
    return longitude

def save_gps_data(df_gps1, df_gps2, df_gps3, output_dir='.'):
    """
    Save the GPS data to CSV files
    """
    Path(output_dir).mkdir(exist_ok=True)
    
    # Save each antenna's data to a separate file
    if not df_gps1.empty:
        df_gps1.to_csv(f"{output_dir}/antenna1_gps.csv", index=False)
        print(f"✅ Antenna 1 (GPGGA) data saved with {len(df_gps1)} points")
    else:
        print("❌ No data found for Antenna 1 (GPGGA)")
        
    if not df_gps2.empty:
        df_gps2.to_csv(f"{output_dir}/antenna2_gps.csv", index=False)
        print(f"✅ Antenna 2 (GNGGA) data saved with {len(df_gps2)} points")
    else:
        print("❌ No data found for Antenna 2 (GNGGA)")
        
    if not df_gps3.empty:
        df_gps3.to_csv(f"{output_dir}/antenna3_gps.csv", index=False)
        print(f"✅ Antenna 3 (GZGGA) data saved with {len(df_gps3)} points")
    else:
        print("❌ No data found for Antenna 3 (GZGGA)")

def main():
    bag_file = "../bags/blueview_2025-03-19-13-19-16.bag"
    output_dir = "gps_data"
    
    print(f"Processing GPS data from {bag_file}...")
    df_gps1, df_gps2, df_gps3 = process_gps_data(bag_file)
    
    # Print some stats about the data
    for i, df in enumerate([df_gps1, df_gps2, df_gps3], 1):
        if df is not None and not df.empty:
            antenna_type = ["GPGGA", "GNGGA", "GZGGA"][i-1]
            print(f"\nAntenna {i} ({antenna_type}) stats:")
            print(f"  Number of data points: {len(df)}")
            print(f"  Latitude range: {df['latitude'].min():.6f} to {df['latitude'].max():.6f}")
            print(f"  Longitude range: {df['longitude'].min():.6f} to {df['longitude'].max():.6f}")
            print(f"  Altitude range: {df['altitude'].min():.2f}m to {df['altitude'].max():.2f}m")
    
    save_gps_data(df_gps1, df_gps2, df_gps3, output_dir)

if __name__ == "__main__":
    main()