"""OPP Energy integration for Home Assistant."""
import logging
from typing import Any

import aiohttp
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import (
    CONF_EMAIL,
    CONF_PASSWORD,
    CONF_SITE_NAME,
    CONF_USER_NAME,
    DEFAULT_CLOUD_URL,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)

class OppEnergyConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Config flow for OPP Energy integration."""

    async def validate_input(self, hass: HomeAssistant, user_input: dict[str, Any]) -> dict[str, Any]:
        """Validate the user input allows us to connect."""
        session = async_get_clientsession(hass)

        # Convert HTTP URL to WebSocket URL for initial test
        ws_url = f"{DEFAULT_CLOUD_URL.replace('http', 'ws').rstrip('/')}"

        try:
            # Test WebSocket connection
            async with session.ws_connect(ws_url) as ws:
                # Send initial registration request
                # Using email as username, display_name for first/last name splitting
                await ws.send_json({
                    "type": "user_registration",
                    "user_name": user_input[CONF_USER_NAME],  # This is used as display name
                    "email": user_input[CONF_EMAIL],  # This becomes the username
                    "password": user_input[CONF_PASSWORD],
                    "site_name": user_input[CONF_SITE_NAME]
                })

                # Wait for response
                msg = await ws.receive_json()
                if msg.get("type") == "error":
                    raise aiohttp.ClientError(msg.get("message", "Registration failed"))  # noqa: TRY301

            return {"title": f"OPP Energy ({user_input[CONF_EMAIL]})"}
        except aiohttp.ClientError as err:
            _LOGGER.error("Failed to connect to OPP Energy service: %s", err)
            raise

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle a flow initiated by the user."""
        errors = {}

        if user_input is not None:
            try:
                info = await self.validate_input(self.hass, user_input)

                # Make sure we store the data in the format expected by the coordinator
                entry_data = {
                    CONF_USER_NAME: user_input[CONF_USER_NAME],
                    CONF_EMAIL: user_input[CONF_EMAIL],
                    CONF_PASSWORD: user_input[CONF_PASSWORD],
                    CONF_SITE_NAME: user_input[CONF_SITE_NAME]
                }

                return self.async_create_entry(
                    title=info["title"],
                    data=entry_data,
                )
            except aiohttp.ClientError:
                errors["base"] = "cannot_connect"
            except Exception as err:
                _LOGGER.exception("Unexpected exception: %s", err)  # noqa: TRY401
                errors["base"] = "unknown"

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_EMAIL): str,
                    vol.Required(CONF_PASSWORD): str,
                    vol.Required(CONF_USER_NAME, default=""): str,
                    vol.Required(CONF_SITE_NAME): str,
                }
            ),
            errors=errors,
        )
