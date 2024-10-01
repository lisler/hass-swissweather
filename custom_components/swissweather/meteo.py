import csv
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import itertools
import logging
from typing import List, NewType

import requests

logger = logging.getLogger(__name__)

CURRENT_CONDITION_URL= 'https://data.geo.admin.ch/ch.meteoschweiz.messwerte-aktuell/VQHA80.csv'

FORECAST_URL= "https://app-prod-ws.meteoswiss-app.ch/v1/plzDetail?plz={:<06d}"
FORECAST_USER_AGENT = "android-31 ch.admin.meteoswiss-2160000"

CONDITION_CLASSES = {
    "clear-night": [101],
    "cloudy": [5,35,105,135],
    "fog": [27,28,127,128],
    "hail": [],
    "lightning": [12,112],
    "lightning-rainy": [13,23,24,25,32,113,123,124,125,132],
    "partlycloudy": [2,3,4,102,103,104],
    "pouring": [20,120],
    "rainy": [6,9,14,17,29,33,106,109,114,117,129,133],
    "snowy": [8,11,16,19,22,30,34,108,111,116,119,122,130,134],
    "snowy-rainy": [7,10,15,18,21,31,107,110,115,118,121,131],
    "sunny": [1,26,126],
    "windy": [],
    "windy-variant": [],
    "exceptional": [],
}

ICON_TO_CONDITION_MAP : dict[int, str] =  {i: k for k, v in CONDITION_CLASSES.items() for i in v}

"""
Returns float or None
"""
def to_float(string: str) -> float | None:
    if string is None:
        return None

    try:
        return float(string)
    except ValueError:
        return None

def to_int(string: str) -> int | None:
    if string is None:
        return None

    try:
        return int(string)
    except ValueError:
        return None

FloatValue = NewType('FloatValue', tuple[float | None, str])

@dataclass
class StationInfo:
    name: str
    abbreviation: str
    type: str
    altitude: float
    lat: float
    lng: float
    canton: str

    def __str__(self) -> str:
        return f"Station {self.abbreviation} - [Name: {self.name}, Lat: {self.lat}, Lng: {self.lng}, Canton: {self.canton}]"

@dataclass
class CurrentWeather:
    station: StationInfo
    date: datetime
    airTemperature: FloatValue
    precipitation: FloatValue
    sunshine: FloatValue
    globalRadiation: FloatValue
    relativeHumidity: FloatValue
    dewPoint: FloatValue
    windDirection: FloatValue
    windSpeed: FloatValue
    gustPeak1s: FloatValue
    pressureStationLevel: FloatValue
    pressureSeaLevel: FloatValue
    pressureSeaLevelAtStandardAtmosphere: FloatValue

@dataclass
class CurrentState:
    currentTemperature: FloatValue
    currentIcon: int
    currentCondition: str | None # None if icon is unrecognized.

@dataclass
class Forecast:
    timestamp: datetime
    icon: int
    condition: str | None # None if icon is unrecognized.
    windSpeed: FloatValue | None = None
    windDirection: FloatValue | None = None
    windGustSpeed: FloatValue | None = None
    temperatureMin: FloatValue | None = None
    temperatureMean: FloatValue | None = None
    temperatureMax: FloatValue | None = None
    precipitationMin: FloatValue | None = None
    precipitation: FloatValue | None = None
    precipitationMax: FloatValue | None = None

@dataclass
class WeatherForecast(object):
    current: CurrentState
    dailyForecast: list[Forecast]
    hourlyForecast: list[Forecast]
    sunrise: list[datetime]
    sunset: list[datetime]

class MeteoClient(object):
    language: str = "en"

    """
    Initializes the client.

    Languages available are en, de, fr and it.
    """
    def __init__(self, language="en"):
        self.language = language

    def get_current_weather_for_all_stations(self) -> list[CurrentWeather] | None:
        logger.debug("Retrieving current weather for all stations ...")
        data = self._get_csv_dictionary_for_url(CURRENT_CONDITION_URL)
        weather = []
        for row in data:
            weather.append(self._get_current_data_for_row(row))
        return weather

    def get_current_weather_for_station(self, station: str) -> CurrentWeather | None:
        logger.debug("Retrieving current weather...")
        data = self._get_current_weather_line_for_station(station)
        if data is None:
            logger.warning("Couldn't find data for station %s", station)
            return None

        return self._get_current_data_for_row(data)

    def _get_current_data_for_row(self, csv_row) -> CurrentWeather:
        timestamp = None
        timestamp_raw = csv_row.get('Date', None)
        if timestamp_raw is not None:
            timestamp = datetime.strptime(timestamp_raw, '%Y%m%d%H%M').replace(tzinfo=UTC)

        return CurrentWeather(
            csv_row.get('Station/Location'),
            timestamp,
            (to_float(csv_row.get('tre200s0', None)), "°C") ,
            (to_float(csv_row.get('rre150z0', None)), "mm"),
            (to_float(csv_row.get('sre000z0', None)), "min"),
            (to_float(csv_row.get('gre000z0', None)), "W/m²"),
            (to_float(csv_row.get('ure200s0', None)), '%'),
            (to_float(csv_row.get('tde200s0', None)), '°C'),
            (to_float(csv_row.get('dkl010z0', None)), '°'),
            (to_float(csv_row.get('fu3010z0', None)), 'km/h'),
            (to_float(csv_row.get('fu3010z1', None)), 'km/h'),
            (to_float(csv_row.get('prestas0', None)), 'hPa'),
            (to_float(csv_row.get('prestas0', None)), 'hPa'),
            (to_float(csv_row.get('pp0qnhs0', None)), 'hPa'),
        )


    ## Forecast
    def get_forecast(self, postCode) -> WeatherForecast | None:
        forecastJson = self._get_forecast_json(postCode, self.language)
        logger.debug("Forecast JSON: %s", forecastJson)
        if forecastJson is None:
            return None

        currentState = self._get_current_state(forecastJson)
        dailyForecast = self._get_daily_forecast(forecastJson)
        hourlyForecast = self._get_hourly_forecast(forecastJson)

        sunrises = None
        sunriseJson = forecastJson.get("graph", {}).get("sunrise", None)
        if sunriseJson is not None:
            sunrises = [datetime.fromtimestamp(epoch / 1000, UTC) for epoch in sunriseJson]

        sunsets = None
        sunsetJson = forecastJson.get("graph", {}).get("sunset", None)
        if sunsetJson is not None:
            sunsets = [datetime.fromtimestamp(epoch / 1000, UTC) for epoch in sunsetJson]

        return WeatherForecast(currentState, dailyForecast, hourlyForecast, sunrises, sunsets)

    def _get_current_state(self, forecastJson) -> CurrentState | None:
        if "currentWeather" not in forecastJson:
            return None

        currentIcon = to_int(forecastJson.get('currentWeather', {}).get('icon', None))
        currentCondition = None
        if currentIcon is not None:
            currentCondition = ICON_TO_CONDITION_MAP.get(currentIcon)
        return CurrentState(
            (to_float(forecastJson.get('currentWeather', {}).get('temperature')), "°C"),
            currentIcon, currentCondition)

    def _get_daily_forecast(self, forecastJson) -> list[Forecast] | None:
        forecast: List[Forecast] = []
        if "forecast" not in forecastJson:
            return forecast

        for dailyJson in forecastJson["forecast"]:
            timestamp = None
            if "dayDate" in dailyJson:
                timestamp = datetime.strptime(dailyJson["dayDate"], '%Y-%m-%d')
            icon = to_int(dailyJson.get('iconDay', None))
            condition = ICON_TO_CONDITION_MAP.get(icon)
            temperatureMax = (to_float(dailyJson.get('temperatureMax', None)), "°C")
            temperatureMin = (to_float(dailyJson.get('temperatureMin', None)), "°C")
            precipitation = (to_float(dailyJson.get('precipitation', None)), "mm")
            forecast.append(Forecast(timestamp, icon, condition, temperatureMax, temperatureMin, precipitation))
        return forecast

    def _get_hourly_forecast(self, forecastJson) -> list[Forecast] | None:
        graphJson = forecastJson.get("graph", None)
        if graphJson is None:
            return None

        startTimestampEpoch = to_int(graphJson.get('start', None))
        if startTimestampEpoch is None:
            return None
        startTimestamp = datetime.fromtimestamp(startTimestampEpoch / 1000, UTC)

        # Set a second start time
        startLowResolutionTimestampEpoch = to_int(graphJson.get('startLowResolution', None))
        if startLowResolutionTimestampEpoch is None:
            return None
        startLowResolutionTimestamp = datetime.fromtimestamp(startLowResolutionTimestampEpoch / 1000, UTC)

        forecast = []
        temperatureMax1hList = [ (value, "°C") for value in graphJson.get("temperatureMax1h", [])]
        temperatureMean1hList = [ (value, "°C") for value in graphJson.get("temperatureMean1h", [])]
        temperatureMin1hList = [ (value, "°C") for value in graphJson.get("temperatureMin1h", [])]
        precipitation1hList = [ (value, "mm") for value in graphJson.get("precipitation1h", [])]
        preciptiationMax1hList = [ (value, "mm") for value in graphJson.get("precipitationMax1h", [])]
        preciptiationMin1hList = [ (value, "mm") for value in graphJson.get("precipitationMin1h", [])]
        windGustSpeed1hList = [ (value, "km/h") for value in graphJson.get("gustSpeed1h", [])]
        windSpeed1hList = [ (value, "km/h") for value in graphJson.get("windSpeed1h", [])]

        # Add precipitation10m with 10 minute resolution forecast
        precipitation10mList = [ (value, "mm") for value in graphJson.get("precipitation10m", [])]
        precipitationMin10mList = [ (value, "mm") for value in graphJson.get("precipitationMin10m", [])]
        precipitationMax10mList = [ (value, "mm") for value in graphJson.get("precipitationMax10m", [])]
        deltaListLength = len(temperatureMean1hList) - len(precipitation1hList)
        
        # Add weatherIcon3h, windDirection3h, windSpeed3h with 3 hour resolution forecast
        weatherIcon3hList = [ value for value in graphJson.get("weatherIcon3h", []) ]
        windDirection3hlist = [ (value, "°") for value in graphJson.get("windDirection3h", []) ]

        # TimestampList
        timestamp10mList = [ startTimestamp + timedelta(minutes=10*value) for value in range(0, min(len(precipitation10mList), len(precipitationMin10mList), len(precipitationMax10mList))) ]
        timestamp1hList = [ startTimestamp + timedelta(hours=value) for value in range(0, min(len(temperatureMean1hList), len(temperatureMax1hList), len(temperatureMin1hList))) ]
        timestamp1hLowResolutionList = [ startLowResolutionTimestamp + timedelta(hours=value) for value in range(0, min(len(precipitation1hList), len(preciptiationMax1hList), len(preciptiationMin1hList), len(windGustSpeed1hList), len(windSpeed1hList))) ]
        timestamp3hList = [ startTimestamp + timedelta(hours=3*value) for value in range(0, min(len(windDirection3hlist), len(weatherIcon3hList))) ]
        # for ts, icon, tMax, tMean, tMin, precipitation, windDirection, windSpeed, windGustSpeed in zip(timestampList, weatherIcon3hList, temperatureMax1hList, 
                                                        # temperatureMean1hList, temperatureMin1hList, precipitation1hList, windDirection3hlist, windSpeed3hList):
            # forecast.append(Forecast(ts, icon, ICON_TO_CONDITION_MAP.get(icon, None), tMax, tMin, precipitation, windSpeed=windSpeed, windDirection=windDirection,
                                    #   windGustSpeed=windGustSpeed, temperatureMean=tMean))
        
        # Add to forecast, depending on resolution
        ts = min(startTimestamp, startLowResolutionTimestamp)
        i10m = i1h = i1hLowResolution = i3h = 0
        logger.info(f"Merging the following: ")
        logger.info(f"10min resolution: {len(timestamp10mList)} items from {timestamp10mList[0]} to {timestamp10mList[-1]}")
        logger.info(f"1h low resolution: {len(timestamp1hLowResolutionList)} items from {timestamp1hLowResolutionList[0]} to {timestamp1hLowResolutionList[-1]}")
        logger.info(f"1h resolution: {len(timestamp1hList)} items from {timestamp1hList[0]} to {timestamp1hList[-1]}")
        logger.info(f"3h resolution: {len(timestamp3hList)} items from {timestamp3hList[0]} to {timestamp3hList[-1]}")
        lastTimestamp = max(timestamp10mList[-1], timestamp1hList[-1], timestamp1hLowResolutionList[-1], timestamp3hList[-1])
        while ts <= lastTimestamp:
            weatherIcon = windDirection = windSpeed = temperatureMin = temperatureMean = temperatureMax = precipitationMin = precipitation = precipitationMax = windGustSpeed = windSpeed = None
            nextTimestamp = lastTimestamp + timedelta(minutes=10)
            if  i3h < len(timestamp3hList) and ts == timestamp3hList[i3h]:
                weatherIcon = weatherIcon3hList[i3h]
                windDirection = windDirection3hlist[i3h]
                i3h += 1
            if i3h < len(timestamp3hList):
                nextTimestamp = min(nextTimestamp, timestamp3hList[i3h])
            if i1h < len(timestamp1hList) and ts == timestamp1hList[i1h]:
                temperatureMin = temperatureMin1hList[i1h]
                temperatureMean = temperatureMean1hList[i1h]
                temperatureMax = temperatureMax1hList[i1h]
                i1h += 1
            if i1h < len(timestamp1hList):
                nextTimestamp = min(nextTimestamp, timestamp1hList[i1h])
            if i1hLowResolution < len(timestamp1hLowResolutionList) and ts == timestamp1hLowResolutionList[i1hLowResolution]:
                precipitationMin = preciptiationMin1hList[i1hLowResolution]
                precipitation = precipitation1hList[i1hLowResolution]
                precipitationMax = preciptiationMax1hList[i1hLowResolution]
                windGustSpeed = windGustSpeed1hList[i1hLowResolution]
                windSpeed = windSpeed1hList[i1hLowResolution]
                i1hLowResolution += 1
            if i1hLowResolution < len(timestamp1hLowResolutionList):
                nextTimestamp = min(nextTimestamp, timestamp1hLowResolutionList[i1hLowResolution])
            if i10m < len(timestamp10mList) and ts == timestamp10mList[i10m]:
                precipitationMin = precipitationMin10mList[i10m]
                precipitation = precipitation10mList[i10m]
                precipitationMax = precipitationMax10mList[i10m]
                i10m += 1
            if i10m < len(timestamp10mList):
                nextTimestamp = min(nextTimestamp, timestamp10mList[i10m])
            if ts >= (datetime.now(UTC) - timedelta(hours=1)):
                forecast.append(Forecast(timestamp=ts,
                                        icon=weatherIcon, 
                                        condition=ICON_TO_CONDITION_MAP.get(weatherIcon, None), 
                                        windSpeed=windSpeed, 
                                        windDirection=windDirection,
                                        windGustSpeed=windGustSpeed, 
                                        temperatureMin=temperatureMin,
                                        temperatureMean=temperatureMean,
                                        temperatureMax=temperatureMax,
                                        precipitationMin=precipitationMin,
                                        precipitation=precipitation,
                                        precipitationMax=precipitationMax))
            ts = nextTimestamp
        logger.info(f"merged to {len(forecast)} items from {forecast[0].timestamp} to {forecast[-1].timestamp}")
        return forecast

    def _get_current_weather_line_for_station(self, station):
        if station is None:
            return None
        return next((row for row in self._get_csv_dictionary_for_url(CURRENT_CONDITION_URL)
            if row['Station/Location'].casefold() == station.casefold()), None)

    def _get_csv_dictionary_for_url(self, url, encoding='utf-8'):
        try:
            logger.debug("Requesting station data...")
            with requests.get(url, stream = True) as r:
                lines = (line.decode(encoding) for line in r.iter_lines())
                yield from csv.DictReader(lines, delimiter=';')
        except requests.exceptions.RequestException as e:
            logger.error("Connection failure.", exc_info=1)
            return None

    def _get_forecast_json(self, postCode, language):
        try:
            url = FORECAST_URL.format(int(postCode))
            logger.debug("Requesting forecast data from %s...", url)
            return requests.get(url, headers =
                { "User-Agent": FORECAST_USER_AGENT,
                    "Accept-Language": language,
                    "Accept": "application/json" }).json()
        except requests.exceptions.RequestException as e:
            logger.error("Connection failure.", exc_info=1)
            return None
