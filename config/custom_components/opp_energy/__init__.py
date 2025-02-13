import logging  # noqa: D104

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import CONF_API_TOKEN, CONF_CLOUD_URL, CONF_INSTANCE_ID, DOMAIN
from .coordinator import OppEnergyDataUpdateCoordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS = ["sensor"]

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up OPP Energy from a config entry."""
    instance_id = entry.data[CONF_INSTANCE_ID]
    api_token = entry.data[CONF_API_TOKEN]
    cloud_url = entry.data[CONF_CLOUD_URL]

    coordinator = OppEnergyDataUpdateCoordinator(
        hass,
        cloud_url,
        instance_id,
        api_token,
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
        if coordinator.ws:
            await coordinator.ws.close()

    return unload_ok
