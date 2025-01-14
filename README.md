# Realtime Taxi Trip Analysis and Visualization

A private application designed to analyze and visualize taxi trip data in real time using Apache Pulsar and Streamlit. This application uses a producer-consumer architecture to handle data streams and a web interface for interactive visualizations.

## Features
- **Real-time data ingestion and processing** with Apache Pulsar.
- **Interactive visualization** using Streamlit.
- **Scalable architecture** to handle large volumes of taxi trip data.

---

## Prerequisites

Before running the application, ensure you have the following installed:
1. [Docker](https://www.docker.com/get-started) for running Apache Pulsar.
2. Python 3.8+ and the required dependencies.

---

## Getting Started

### 1. Install Apache Pulsar via Docker
Follow these steps to set up Apache Pulsar:

```bash
docker pull apachepulsar/pulsar:latest
docker run -it -p 6650:6650 -p 8080:8080 apachepulsar/pulsar:latest bin/pulsar standalone
```

Verify that Apache Pulsar is running by accessing the Pulsar dashboard at `http://localhost:8080`.

---

### 2. Set Up the Project Environment

1. Clone this repository:
   ```bash
   git clone <repository-url>
   cd Realtime-Taxi-Trip-Analysis-and-Visualization
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

---

### 3. Running the Application

#### 3.1 Start the Streamlit Analysis App

Run the following command to launch the real-time visualization app:
```bash
python streamlit_app.py
```

The app will be accessible at `http://localhost:6650`.

#### 3.2 Start the Taxi Trip Consumer

The consumer listens to data streams and processes messages from Apache Pulsar:
```bash
python taxi_trip_consumer.py
```

#### 3.3 Start the Taxi Trip Producer

The producer generates and sends taxi trip data to Apache Pulsar:
```bash
python taxi_trip_producer.py
```

---

## Application Workflow

1. **Producer**: Sends simulated taxi trip data to Apache Pulsar.
2. **Consumer**: Listens to the data streams, processes messages, and sends updates to the Streamlit app.
3. **Streamlit App**: Displays real-time analysis and visualization of taxi trip data.

---

Happy analyzing! 🚖📊
