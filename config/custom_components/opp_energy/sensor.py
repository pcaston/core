from homeassistant.components.sensor import (  # noqa: D100
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CURRENCY_EURO
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import OppEnergyDataUpdateCoordinator


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

    def __init__(  # noqa: D107
        self,
        coordinator: OppEnergyDataUpdateCoordinator,
        entry: ConfigEntry,
    ) -> None:
        super().__init__(coordinator)
        self._attr_unique_id = f"{entry.entry_id}_price"
        self._attr_name = "Energy Price"
        self._attr_native_unit_of_measurement = CURRENCY_EURO
        self._attr_device_class = SensorDeviceClass.MONETARY
        self._attr_state_class = SensorStateClass.MEASUREMENT

    @property
    def native_value(self):
        """Return the state of the sensor."""
        if self.coordinator.data:
            return self.coordinator.data.get("price")
        return None

    @property
    def device_info(self):
        """Return device information."""
        return {
            "identifiers": {(DOMAIN, self.coordinator.instance_id)},
            "name": "OPP Energy",
            "manufacturer": "Your Company",
            "model": "OPP Energy Integration",
            "sw_version": "1.0.0",
        }
