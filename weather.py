from __future__ import annotations

import logging
import datetime

from typing import Any
from config.custom_components.swissweather import SwissWeatherDataCoordinator

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    LENGTH_MILLIMETERS,
    PRESSURE_HPA,
    TEMP_CELSIUS,
    SPEED_KILOMETERS_PER_HOUR
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import CONF_POST_CODE, DOMAIN
from .meteo import WeatherForecast, CurrentWeather

from homeassistant.components.weather import (
    Forecast,
    WeatherEntity,
)

_LOGGER = logging.getLogger(__name__)

async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: SwissWeatherDataCoordinator = hass.data[DOMAIN][config_entry.entry_id]
    async_add_entities(
        [
            SwissWeather(coordinator, config_entry.data[CONF_POST_CODE], False),
            SwissWeather(coordinator, config_entry.data[CONF_POST_CODE], True),
        ]
    )

class SwissWeather(CoordinatorEntity[SwissWeatherDataCoordinator], WeatherEntity):

    def __init__(
        self,
        coordinator: SwissWeatherDataCoordinator,
        postCode: str,
        hourly: bool,
    ) -> None:
        super().__init__(coordinator)
        self._postCode = postCode
        self._hourly = hourly

    @property
    def _current_state(self) -> CurrentWeather:
        if self.coordinator.data is None:
            return None
        return self.coordinator.data[0]

    @property
    def _current_forecast(self) -> WeatherForecast:
        if self.coordinator.data is None:
            return None
        return self.coordinator.data[1]

    @property
    def unique_id(self) -> str | None:
        if self._hourly:
            return f"swiss_weather.{self._postCode}.hourly"
        else:
            return f"swiss_weather.{self._postCode}.daily"

    @property
    def name(self):
        if self._hourly:
            return f"Weather at {self._postCode} (Hourly)"
        else:
            return f"Weather at {self._postCode} (Daily)"

    @property
    def condition(self) -> str | None:
        if self._current_forecast is None:
            return None
        return self._current_forecast.current.currentCondition

    @property
    def native_temperature(self) -> float | None:
        if self._current_state is not None and self._current_state.airTemperature is not None:
            return self._current_state.airTemperature[0]
        if self._current_forecast is None:
            return None
        return self._current_forecast.current.currentTemperature[0]

    @property
    def native_temperature_unit(self) -> str | None:
        return TEMP_CELSIUS

    @property
    def native_precipitation_unit(self) -> str | None:
        return LENGTH_MILLIMETERS

    @property
    def native_wind_speed(self) -> float | None:
        if self._current_state is not None:
            return self._current_state.windSpeed[0]
        return None

    @property
    def native_wind_speed_unit(self) -> str | None:
        return SPEED_KILOMETERS_PER_HOUR

    @property
    def humidity(self) -> float | None:
        if self._current_state is None:
            return None
        return self._current_state.relativeHumidity[0]

    @property
    def wind_bearing(self) -> float | str | None:
        if self._current_state is None:
            return None
        return self._current_state.windDirection[0]

    @property
    def native_pressure(self) -> float | None:
        if self._current_state is None:
            return None
        return self._current_state.pressureStationLevel[0]

    @property
    def native_pressure_unit(self) -> str | None:
        return PRESSURE_HPA

    @property
    def forecast(self) -> list[Forecast] | None:
        if self._current_forecast is None:
            return None

        if self._hourly:
            now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
            _LOGGER.info(f"{now} vs. {self._current_forecast.hourlyForecast[0].timestamp}")
            forecast_data = list(filter(lambda forecast: forecast.timestamp >= now, self._current_forecast.hourlyForecast))
            _LOGGER.info(forecast_data)
        else:
            forecast_data = self._current_forecast.dailyForecast
        
        return list(map(lambda entry: self.meteo_forecast_to_forecast(entry), forecast_data))

    def meteo_forecast_to_forecast(self, meteo_forecast) -> Forecast:
        if self._hourly:
            temperature = meteo_forecast.temperatureMean[0]
            wind_speed = meteo_forecast.windSpeed[0]
            wind_bearing = meteo_forecast.windDirection[0]
        else: 
            temperature = meteo_forecast.temperatureMax[0]
            wind_speed = None
            wind_bearing = None

        return Forecast(condition=meteo_forecast.condition, 
                datetime=meteo_forecast.timestamp,
                native_precipitation=meteo_forecast.precipitation[0],
                native_temperature=temperature,
                native_templow=meteo_forecast.temperatureMin[0],
                native_wind_speed=wind_speed,
                wind_bearing=wind_bearing)