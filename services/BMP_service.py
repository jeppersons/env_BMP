from flask import Flask, jsonify
import adafruit_bmp280
import board
import mysql.connector
from mysql.connector import Error
import threading
import time

app = Flask(__name__)

# Database configuration
DB_CONFIG = {
    'host': '192.168.8.161',
    'database': 'MCP',
    'user': 'ned',
    'password': 'ned'
}
POST_INTERVAL = 60  # Interval in seconds
HARDCODED_MACHINE_NAME = 'dusty'

# Initialize BMP280 Sensor
i2c = board.I2C()
bmp280 = adafruit_bmp280.Adafruit_BMP280_I2C(i2c)

def get_db_connection():
    """Create and return a database connection."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        return None

def write_to_database(major, machine, temperature, pressure):
    """Function to write BMP280 data to the database."""
    conn = get_db_connection()
    if conn is not None:
        try:
            cursor = conn.cursor()
            sql = """INSERT INTO STATS_Machine (timestamp, Major, Minor, Value, Machine, Notes)
                     VALUES (NOW(), %s, %s, %s, %s, %s)"""
            # Insert temperature
            cursor.execute(sql, (major, 'Temperature', temperature, machine, 'Temperature'))
            # Insert pressure
            cursor.execute(sql, (major, 'Pressure', pressure, machine, 'Pressure'))
            conn.commit()
        except Error as e:
            print(f"Error: {e}")
        finally:
            conn.close()
    else:
        print("Failed to get database connection")

def get_bmp280_data():
    """Collect BMP280 sensor data."""
    return {
        "temperature": bmp280.temperature,
        "pressure": bmp280.pressure
    }

def post_bmp280_data_periodically():
    """Function to post BMP280 data to the database periodically."""
    while True:
        major = "BMP"
        machine = HARDCODED_MACHINE_NAME
        bmp280_data = get_bmp280_data()
        write_to_database(major, machine, bmp280_data['temperature'], bmp280_data['pressure'])
        time.sleep(POST_INTERVAL)

@app.route('/', methods=['GET'])
def bmp280_endpoint():
    """Endpoint to manually get BMP280 data."""
    return jsonify(get_bmp280_data())

if __name__ == '__main__':
    threading.Thread(target=post_bmp280_data_periodically, daemon=True).start()
    app.run(host='0.0.0.0', port=5003)

