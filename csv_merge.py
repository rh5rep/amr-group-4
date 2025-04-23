from pathlib import Path
from rosbags.highlevel import AnyReader
from rosbags.typesys import Stores, get_typestore
import pandas as pd

# Paths to the bag files
# Change these paths to your actual bag file paths
bag_inputs = Path("C:/Users/akinm/OneDrive/Masaüstü/otter/inputs.bag")
bag_imu = Path("C:/Users/akinm/OneDrive/Masaüstü/otter/ouster.bag")
typestore = get_typestore(Stores.ROS2_FOXY)

# Data lists
u_data, twist_data, imu_data = [], [], []

### 1. u_actual and twist data from inputs.bag ###
with AnyReader([bag_inputs], default_typestore=typestore) as reader:
    connections = {c.topic: c for c in reader.connections}

    for con, ts, raw in reader.messages(connections=[connections["/otter/u_actual"]]):
        msg = reader.deserialize(raw, con.msgtype)
        t = ts * 1e-9
        u0, u1 = msg.data[1], msg.data[2]
        u_data.append([t, u0, u1])

    for con, ts, raw in reader.messages(connections=[connections["/otter/twist"]]):
        msg = reader.deserialize(raw, con.msgtype)
        t = ts * 1e-9
        twist = msg.twist
        twist_data.append([t, twist.linear.x, twist.linear.y, twist.angular.z])

### 2. IMU data from ouster.bag (Sampled in blocks of 10) ###
imu_temp = []
with AnyReader([bag_imu], default_typestore=typestore) as reader:
    connections = {c.topic: c for c in reader.connections}

    for con, ts, raw in reader.messages(connections=[connections["/ouster/imu"]]):
        msg = reader.deserialize(raw, con.msgtype)
        t = ts * 1e-9
        imu_temp.append([
            t,
            msg.linear_acceleration.x,
            msg.linear_acceleration.y,
            msg.linear_acceleration.z,
            msg.angular_velocity.x,
            msg.angular_velocity.y,
            msg.angular_velocity.z
        ])

# Sampling IMU data in blocks of 10 to get mean values
for i in range(0, len(imu_temp), 10):
    block = imu_temp[i:i+10]
    if len(block) == 10:
        times = [b[0] for b in block]
        data = [b[1:] for b in block]
        mean_vals = pd.DataFrame(data).mean().tolist()
        imu_data.append([times[0]] + mean_vals)

### 3. Converting to Data Frames ###
df_u = pd.DataFrame(u_data, columns=["time", "u0", "u1"]).iloc[5:].reset_index(drop=True)
df_twist = pd.DataFrame(twist_data, columns=["time", "vx", "vy", "wz"]).iloc[5:].reset_index(drop=True)
df_imu = pd.DataFrame(imu_data, columns=["time", "ax", "ay", "az", "wx", "wy", "wz_imu"]).iloc[5:].reset_index(drop=True)

### 4. Time normalization (elapsed time) ###
# Normalizing the time to start from 0 for each DataFrame
# (assuming the first time value is the start time for each DataFrame)
t0 = df_u["time"].iloc[0]
t1 = df_twist["time"].iloc[0]
t2 = df_imu["time"].iloc[0]
df_u["time"] -= t0
df_twist["time"] -= t1
df_imu["time"] -= t2

print(df_u["time"].iloc[-1])
print(df_twist["time"].iloc[-1])
print(df_imu["time"].iloc[-1])

### 5. Concatenation part ###
min_len = min(len(df_u), len(df_twist), len(df_imu))
df_u = df_u.iloc[:min_len].reset_index(drop=True)
df_twist = df_twist.iloc[:min_len].reset_index(drop=True)
df_imu = df_imu.iloc[:min_len].reset_index(drop=True)

df_merged = pd.concat([
    df_u,
    df_twist[["vx", "vy", "wz"]],
    df_imu[["ax", "ay", "az", "wx", "wy", "wz_imu"]]
], axis=1)  # This code concatenates the DataFrames with respect to the timestamp columns.

### 6. Save as CSV ###
df_merged.to_csv("model_data.csv", index=False)
print("✅ model_data.csv is created")
