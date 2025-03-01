"""Support for OPP Energy sensors."""
from __future__ import annotations

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CURRENCY_DOLLAR
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN


# Add buy and sell sensors
async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the OPP Energy sensors."""
    coordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([
        OppEnergyBuyPriceSensor(coordinator, entry),
        OppEnergySellPriceSensor(coordinator, entry)
    ])

# Create buy price sensor
class OppEnergyBuyPriceSensor(CoordinatorEntity, SensorEntity):
    """Representation of an OPP Energy buy price sensor."""

    def __init__(self, coordinator, entry: ConfigEntry) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator)
        self._entry = entry
        self._attr_unique_id = f"{coordinator.instance_id}_buy_price"
        self._attr_name = f"Energy Buy Price {coordinator.user_name}"
        self._attr_native_unit_of_measurement = CURRENCY_DOLLAR
        self._attr_device_class = SensorDeviceClass.MONETARY
        self._attr_state_class = SensorStateClass.TOTAL

    @property
    def native_value(self):
        """Return the buy price."""
        if self.coordinator.data and isinstance(self.coordinator.data, dict):
            return self.coordinator.data.get("buy_price")
        return None

    @property
    def device_info(self):
        """Return device information."""
        return {
            "identifiers": {(DOMAIN, self.coordinator.instance_id)},
            "name": f"OPP Energy {self.coordinator.user_name}",
            "manufacturer": "Open Peer Power",
            "model": "Energy Price Monitor",
            "sw_version": "1.0.0",
        }

# Create sell price sensor
class OppEnergySellPriceSensor(CoordinatorEntity, SensorEntity):
    """Representation of an OPP Energy sell price sensor."""

    def __init__(self, coordinator, entry: ConfigEntry) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator)
        self._entry = entry
        self._attr_unique_id = f"{coordinator.instance_id}_sell_price"
        self._attr_name = f"Energy Sell Price {coordinator.user_name}"
        self._attr_native_unit_of_measurement = CURRENCY_DOLLAR
        self._attr_device_class = SensorDeviceClass.MONETARY
        self._attr_state_class = SensorStateClass.TOTAL

    @property
    def native_value(self):
        """Return the sell price."""
        if self.coordinator.data and isinstance(self.coordinator.data, dict):
            return self.coordinator.data.get("sell_price")
        return None

    @property
    def device_info(self):
        """Return device information."""
        return {
            "identifiers": {(DOMAIN, self.coordinator.instance_id)},
            "name": f"OPP Energy {self.coordinator.user_name}",
            "manufacturer": "Open Peer Power",
            "model": "Energy Price Monitor",
            "sw_version": "1.0.0",
        }
