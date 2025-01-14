import streamlit as st
import pulsar
import json
import pandas as pd
import matplotlib.pyplot as plt
import threading
import time
import numpy as np
import pydeck as pdk

# Thread-safe data storage
data_lock = threading.Lock()
total_duration = 0
trip_count = 0
average_durations = []
trip_durations = []
trip_timestamps = []
pickup_locations = []
dropoff_locations = []

def consume_messages():
    global total_duration, trip_count, average_durations, trip_durations, trip_timestamps, pickup_locations, dropoff_locations
    client = pulsar.Client('pulsar://localhost:6650')
    consumer = client.subscribe('trip-topic', subscription_name='streamlit-sub', consumer_type=pulsar.ConsumerType.Shared)
    
    try:
        while True:
            msg = consumer.receive(timeout_millis=1000)
            if msg:
                data = json.loads(msg.data().decode('utf-8'))
                trip_duration = data.get('trip_duration', 0)
                timestamp = data.get('timestamp', time.time())
                pickup_lat = data.get('pickup_latitude')
                pickup_lon = data.get('pickup_longitude')
                dropoff_lat = data.get('dropoff_latitude')
                dropoff_lon = data.get('dropoff_longitude')
                
                with data_lock:
                    total_duration += trip_duration
                    trip_count += 1
                    average_duration = total_duration / trip_count
                    average_durations.append(average_duration)
                    trip_durations.append(trip_duration)
                    trip_timestamps.append(timestamp)
                    
                    if pickup_lat and pickup_lon:
                        pickup_locations.append([pickup_lon, pickup_lat])
                    if dropoff_lat and dropoff_lon:
                        dropoff_locations.append([dropoff_lon, dropoff_lat])
                
                consumer.acknowledge(msg)
    except:
        pass
    finally:
        consumer.close()
        client.close()

# Start consumer thread
thread = threading.Thread(target=consume_messages, daemon=True)
thread.start()

st.title("Real-Time Taxi Trip Duration Dashboard")

placeholder = st.empty()

while True:
    with data_lock:
        current_trip = trip_count
        current_avg = (total_duration / trip_count) if trip_count else 0
        current_avg_durations = average_durations.copy()
        current_trip_durations = trip_durations.copy()
        current_timestamps = trip_timestamps.copy()
        current_pickups = pickup_locations.copy()
        current_dropoffs = dropoff_locations.copy()
    
    # Calculate additional metrics
    if current_trip_durations:
        median_duration = np.median(current_trip_durations)
        percentile_90 = np.percentile(current_trip_durations, 90)
    else:
        median_duration = 0
        percentile_90 = 0
    
    # Calculate trips per minute
    current_time = time.time()
    one_minute_ago = current_time - 60
    trips_last_minute = [t for t in current_timestamps if t >= one_minute_ago]
    trips_per_minute = len(trips_last_minute)
    
    # Calculate moving average (last 50 trips)
    window_size = 50
    if len(current_trip_durations) >= window_size:
        moving_average = np.convolve(current_trip_durations, np.ones(window_size)/window_size, mode='valid')
    else:
        moving_average = []
    
    with placeholder.container():
        st.metric("Total Trips", current_trip)
        st.metric("Average Duration (s)", f"{current_avg:.2f}")
        st.metric("Median Duration (s)", f"{median_duration:.2f}")
        st.metric("90th Percentile Duration (s)", f"{percentile_90:.2f}")
        st.metric("Trips Per Minute", trips_per_minute)
        
        fig, ax = plt.subplots(3, 1, figsize=(10, 12))
        
        # Plot Average Duration
        ax[0].plot(current_avg_durations, label='Average Duration', color='blue')
        ax[0].set_title('Average Trip Duration Over Time')
        ax[0].set_xlabel('Trips')
        ax[0].set_ylabel('Average Duration (s)')
        ax[0].legend()
        
        # Plot Trip Duration Distribution
        ax[1].hist(current_trip_durations, bins=50, color='green', alpha=0.7)
        ax[1].set_title('Trip Duration Distribution')
        ax[1].set_xlabel('Duration (s)')
        ax[1].set_ylabel('Frequency')
        
        # Plot Moving Average
        ax[2].plot(moving_average, label='Moving Average (50 trips)', color='orange')
        ax[2].set_title('Moving Average of Trip Duration')
        ax[2].set_xlabel('Trips')
        ax[2].set_ylabel('Moving Average Duration (s)')
        ax[2].legend()
        
        plt.tight_layout()
        st.pyplot(fig)
        
        # Map Visualization
        st.subheader("Trip Locations")
        
        # Limit the number of points for performance
        max_points = 1000
        pickup_data = pickup_locations[-max_points:]
        dropoff_data = dropoff_locations[-max_points:]
        
        # Define map layers
        pickup_layer = pdk.Layer(
            "ScatterplotLayer",
            data=pd.DataFrame(pickup_data, columns=["lon", "lat"]),
            get_position=["lon", "lat"],
            get_color="[0, 128, 255, 160]",
            get_radius=50,
            pickable=True,
            auto_highlight=True,
        )
        
        dropoff_layer = pdk.Layer(
            "ScatterplotLayer",
            data=pd.DataFrame(dropoff_data, columns=["lon", "lat"]),
            get_position=["lon", "lat"],
            get_color="[255, 0, 0, 160]",
            get_radius=50,
            pickable=True,
            auto_highlight=True,
        )
        
        # Set the viewport location
        if pickup_data:
            midpoint = np.mean(pickup_data, axis=0)
            view_state = pdk.ViewState(latitude=midpoint[1], longitude=midpoint[0], zoom=11, pitch=0)
        else:
            view_state = pdk.ViewState(latitude=40.7128, longitude=-74.0060, zoom=11, pitch=0)  # Default to NYC
        
        # Create Deck
        r = pdk.Deck(
            layers=[pickup_layer, dropoff_layer],
            initial_view_state=view_state,
            tooltip={"text": "Latitude: {lat}\nLongitude: {lon}"},
        )
        
        st.pydeck_chart(r)
    
    time.sleep(1)