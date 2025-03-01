"""The OPP Energy integration."""
import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant

from .const import CONF_EMAIL, CONF_PASSWORD, CONF_SITE_NAME, CONF_USER_NAME, DOMAIN
from .coordinator import OppEnergyDataUpdateCoordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [Platform.SENSOR]

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up OPP Energy from a config entry."""
    user_name = entry.data[CONF_USER_NAME]
    email = entry.data[CONF_EMAIL]
    password = entry.data[CONF_PASSWORD]
    site_name = entry.data[CONF_SITE_NAME]

    coordinator = OppEnergyDataUpdateCoordinator(
        hass,
        user_name=user_name,
        email=email,
        password=password,
        site_name=site_name,
    )

    await coordinator.async_config_entry_first_refresh()

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = coordinator

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    return True

async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)

    if unload_ok:
        coordinator = hass.data[DOMAIN].pop(entry.entry_id)
        await coordinator.async_shutdown()  # Use the new shutdown method

    return unload_ok
