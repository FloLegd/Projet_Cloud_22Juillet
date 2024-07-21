import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

# Load les data
def load_data(file_path):
    return pd.read_parquet(file_path)

st.title('Weather Forecast Data Visualization')
file_pathLYON = "https://weatherefrei.blob.core.windows.net/app/exec/exposed/computed/LYON/WEATHER_LYON_20240720.parquet"
file_pathMARSEILLE = "https://weatherefrei.blob.core.windows.net/app/exec/exposed/computed/MARSEILLE/WEATHER_MARSEILLE_20240720.parquet"
file_pathPARIS = "https://weatherefrei.blob.core.windows.net/app/exec/exposed/computed/PARIS/WEATHER_PARIS_20240720.parquet"

data_LYON = load_data(file_pathLYON)
data_MARSEILLE = load_data(file_pathMARSEILLE)
data_PARIS = load_data(file_pathPARIS)

# Ajout d'une colonne 'city' pour chaque dataframe
data_LYON['city'] = 'Lyon'
data_MARSEILLE['city'] = 'Marseille'
data_PARIS['city'] = 'Paris'

# Concatenate
data = pd.concat([data_LYON, data_MARSEILLE, data_PARIS], axis=0)

st.write('### Weather Data')
st.write(data.head())

# Convertir colonne 'date' 
data['date'] = pd.to_datetime(data['date'])

#Plots (Température et humidité)

def plot_temperature_humidity(city_data, city_name):
    fig, ax1 = plt.subplots()

    ax1.set_xlabel('Date')
    ax1.set_ylabel('Temperature', color='orange')
    ax1.plot(city_data['date'], city_data['temperature'], label='Temperature', color='orange')
    ax1.tick_params(axis='y', labelcolor='orange')
    ax1.tick_params(axis='x', rotation=90) 

    ax2 = ax1.twinx()
    ax2.set_ylabel('Humidity Level', color='blue')
    ax2.plot(city_data['date'], city_data['humidity_level'], label='Humidity Level', color='blue')
    ax2.tick_params(axis='y', labelcolor='blue')

    plt.title(f'Temperature and Humidity Level Over Time in {city_name}')
    fig.tight_layout()
    st.pyplot(fig)

#Plots (fréquence weather description)

def plot_weather_description(city_data, city_name):
    weather_counts = city_data['weather_description'].value_counts()
    fig, ax = plt.subplots()
    weather_counts.plot(kind='bar', ax=ax, color='green')
    ax.set_xlabel('Weather Description')
    ax.set_ylabel('Frequency')
    ax.set_title(f'Weather Description Frequency in {city_name}')
    plt.xticks(rotation=90)
    st.pyplot(fig)

# Split l'analyse par 'city'

for city in data['city'].unique():
    st.write(f'## Analysis for {city}')
    city_data = data[data['city'] == city]

    plot_temperature_humidity(city_data, city)

    plot_weather_description(city_data, city)
