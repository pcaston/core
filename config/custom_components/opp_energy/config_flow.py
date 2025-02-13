import voluptuous as vol  # noqa: D100

from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResult

from .const import (
    CONF_API_TOKEN,
    CONF_CLOUD_URL,
    CONF_INSTANCE_ID,
    DEFAULT_CLOUD_URL,
    DOMAIN,
)


class OppEnergyConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):  # noqa: D101
    VERSION = 1

    async def async_step_user(  # noqa: D102
        self, user_input: dict[str, any] | None = None
    ) -> FlowResult:
        errors = {}

        if user_input is not None:
            # Validate the input
            try:
                # You could add validation here
                return self.async_create_entry(
                    title=f"OPP Energy ({user_input[CONF_INSTANCE_ID]})",
                    data=user_input,
                )
            except Exception:  # noqa: BLE001
                errors["base"] = "connection_error"

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_INSTANCE_ID): str,
                    vol.Required(CONF_API_TOKEN): str,
                    vol.Required(CONF_CLOUD_URL, default=DEFAULT_CLOUD_URL): str,
                    vol.Required("user_name"): str,
                    vol.Required("device_id"): str,
                }
            ),
            errors=errors,
        )
