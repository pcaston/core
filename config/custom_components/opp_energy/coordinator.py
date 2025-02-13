from datetime import timedelta  # noqa: D100
import json
import logging
from typing import Any

import async_timeout  # noqa: TID251
import websockets

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import DOMAIN, UPDATE_INTERVAL

_LOGGER = logging.getLogger(__name__)

class OppEnergyDataUpdateCoordinator(DataUpdateCoordinator):  # noqa: D101
    def __init__(  # noqa: D107
        self,
        hass: HomeAssistant,
        websocket_url: str,
        instance_id: str,
        api_token: str,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=UPDATE_INTERVAL),
        )
        self.websocket_url = websocket_url
        self.instance_id = instance_id
        self.api_token = api_token
        self.ws = None

    async def _async_update_data(self) -> dict[str, Any]:
        try:
            async with async_timeout.timeout(10):
                if self.ws is None or self.ws.closed:
                    self.ws = await websockets.connect(
                        f"{self.websocket_url}{self.instance_id}/"
                    )

                await self.ws.send(json.dumps({
                    "type": "register_device",
                    "instance_id": self.instance_id,
                    "token": self.api_token,
                    "user_name": "your_user_name",  # Replace with dynamic value
                    "device_id": "your_device_id"  # Replace with dynamic value
                }))

                response = await self.ws.recv()
                data = json.loads(response)

                if data["type"] == "registration_success":
                    return {"status": "registered"}
                raise UpdateFailed(f"Unexpected response type: {data['type']}")  # noqa: TRY301

        except Exception as error:  # noqa: BLE001
            if self.ws:
                await self.ws.close()
            self.ws = None
            raise UpdateFailed(f"Error communicating with OPP Cloud: {error}")  # noqa: B904
