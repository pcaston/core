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


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the OPP Energy sensors."""
    coordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([OppEnergyPriceSensor(coordinator, entry)])


class OppEnergyPriceSensor(CoordinatorEntity, SensorEntity):
    """Representation of an OPP Energy price sensor."""

    def __init__(
        self,
        coordinator,
        entry: ConfigEntry,
    ) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator)
        self._entry = entry
        self._attr_unique_id = f"{coordinator.instance_id}_price"
        self._attr_name = f"Energy Price {coordinator.user_name}"
        self._attr_native_unit_of_measurement = CURRENCY_DOLLAR
        self._attr_device_class = SensorDeviceClass.MONETARY
        self._attr_state_class = SensorStateClass.TOTAL

    @property
    def native_value(self):
        """Return the state of the sensor."""
        if self.coordinator.data:
            if isinstance(self.coordinator.data, dict):
                # Check for both price formats
                if "buy_price" in self.coordinator.data:
                    return self.coordinator.data.get("buy_price")
                return self.coordinator.data.get("price")
            return self.coordinator.data
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

    @property
    def available(self) -> bool:
        """Return if entity is available."""
        return self.coordinator.last_update_success and self.coordinator.data is not None

    @property
    def extra_state_attributes(self):
        """Return additional state attributes."""
        if not self.coordinator.data or not isinstance(self.coordinator.data, dict):
            return {}

        attributes = {}
        # Include both buy and sell prices in attributes
        if "buy_price" in self.coordinator.data:
            attributes["buy_price"] = self.coordinator.data["buy_price"]
        if "sell_price" in self.coordinator.data:
            attributes["sell_price"] = self.coordinator.data["sell_price"]
        if "timestamp" in self.coordinator.data:
            attributes["last_update"] = self.coordinator.data["timestamp"]

        return attributes
