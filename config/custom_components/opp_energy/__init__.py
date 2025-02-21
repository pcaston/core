"""The OPP Energy integration."""
import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant

from .const import CONF_DEVICE_NAME, CONF_EMAIL, CONF_PASSWORD, CONF_USER_NAME, DOMAIN
from .coordinator import OppEnergyDataUpdateCoordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [Platform.SENSOR]

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up OPP Energy from a config entry."""
    try:
        # Extract configuration data
        user_name = entry.data[CONF_USER_NAME]
        password = entry.data[CONF_PASSWORD]
        email = entry.data[CONF_EMAIL]
        device_name = entry.data[CONF_DEVICE_NAME]

        coordinator = OppEnergyDataUpdateCoordinator(
            hass,
            user_name=user_name,
            password = password,
            email = email,
            device_name=device_name,
        )

        await coordinator.async_config_entry_first_refresh()

        hass.data.setdefault(DOMAIN, {})
        hass.data[DOMAIN][entry.entry_id] = coordinator

        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

        return True  # noqa: TRY300

    except Exception as err:
        _LOGGER.error("Error setting up OPP Energy integration: %s", err)
        raise

async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)

    if unload_ok:
        coordinator = hass.data[DOMAIN].pop(entry.entry_id)
        await coordinator.async_shutdown()  # Use the new shutdown method

    return unload_ok
