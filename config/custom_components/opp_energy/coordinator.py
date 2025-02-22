"""OPP Energy Data Update Coordinator."""
import asyncio
from datetime import timedelta
import json
import logging

from websockets.client import WebSocketClientProtocol, connect
from websockets.exceptions import WebSocketException

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import DEFAULT_CLOUD_URL, DOMAIN, UPDATE_INTERVAL

_LOGGER = logging.getLogger(__name__)

class OppEnergyDataUpdateCoordinator(DataUpdateCoordinator):
    """Class to manage fetching OPP Energy data."""

    def __init__(
        self,
        hass: HomeAssistant,
        user_name: str,
        email: str,
        password: str,
        device_name: str,
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=UPDATE_INTERVAL),
        )
        self.websocket_url = DEFAULT_CLOUD_URL
        self.user_name = user_name
        self.email = email
        self.password = password
        self.device_name = device_name
        self.ws: WebSocketClientProtocol | None = None
        self._authenticated = False
        self._last_data = None
        self._reconnect_task = None
        self._keep_alive_task = None
        self.instance_id = f"opp_energy_{user_name}_{device_name}"

    def _is_connected(self) -> bool:
        """Check if the WebSocket connection is active."""
        return self.ws is not None and not self.ws.closed

    async def _connect(self) -> None:
        """Establish WebSocket connection."""
        if self._is_connected():
            return

        try:
            _LOGGER.debug("Attempting to connect to WebSocket at: %s", self.websocket_url)
            self.ws = await connect(
                self.websocket_url,
                ping_interval=20,
                ping_timeout=20,
            )
            _LOGGER.debug("WebSocket connection established")

            # Start keep-alive task
            if self._keep_alive_task is None or self._keep_alive_task.done():
                self._keep_alive_task = asyncio.create_task(self._keep_alive())

            if not self._authenticated:
                await self._authenticate()

        except WebSocketException as error:
            self.ws = None
            raise UpdateFailed(f"Failed to connect to WebSocket: {error}") from error
        except Exception as error:
            self.ws = None
            raise UpdateFailed(f"Unexpected error during connection: {error}") from error

    async def _keep_alive(self) -> None:
        """Keep the WebSocket connection alive."""
        while True:
            try:
                if self._is_connected():
                    await self._send_message({"type": "ping"})
                await asyncio.sleep(30)  # Send message every 30 seconds
            except Exception as e:  # noqa: BLE001
                _LOGGER.error("Keep-alive error: %s", e)
                self.ws = None
                await asyncio.sleep(5)  # Wait before retrying

    async def _process_message(self, message: dict) -> dict | None:
        """Process incoming WebSocket message."""
        msg_type = message.get("type")

        if msg_type == "pong":
            _LOGGER.debug("Received pong response")
            return None
        if msg_type == "auth_success":
            _LOGGER.debug("Authentication successful")
            self._authenticated = True
            return None
        if msg_type == "auth_failed":
            raise UpdateFailed(f"Authentication failed: {message.get('message', 'Unknown error')}")
        if msg_type == "price_update":
            _LOGGER.debug("Received price data")
            return message.get("data", {})
        _LOGGER.debug("Received message of type: %s", msg_type)
        return message

    async def _authenticate(self) -> None:
        """Authenticate with the WebSocket server."""
        if not self._is_connected():
            await self._connect()

        try:
            auth_message = {
                "type": "authenticate",
                "user_name": self.user_name,
                "email": self.email,
                "password": self.password,
                "device_name": self.device_name
            }
            _LOGGER.debug("Sending authentication message")

            await self._send_message(auth_message)
            response = await self._receive_message()

            await self._process_message(response)

            if self._authenticated:
                # Subscribe to price updates after successful authentication
                await self._send_message({
                    "type": "subscribe_prices",
                    "user_name": self.user_name
                })
            else:
                raise UpdateFailed("Authentication failed")  # noqa: TRY301

        except Exception as error:
            self._authenticated = False
            _LOGGER.error("Authentication error: %s", error)
            self.ws = None
            raise UpdateFailed(f"Authentication error: {error}") from error

    async def _send_message(self, message: dict) -> None:
        """Send a message through the WebSocket connection."""
        if not self._is_connected():
            await self._connect()

        try:
            await self.ws.send(json.dumps(message))
            _LOGGER.debug("Sent message: %s", message)
        except Exception as error:
            _LOGGER.error("Error sending message: %s", error)
            self.ws = None
            raise UpdateFailed(f"Failed to send message: {error}") from error

    async def _receive_message(self) -> dict:
        """Receive a message from the WebSocket connection."""
        if not self._is_connected():
            await self._connect()

        try:
            message = await self.ws.recv()
            parsed_message = json.loads(message)
            _LOGGER.debug("Received message: %s", parsed_message)
            return parsed_message  # noqa: TRY300
        except Exception as error:
            _LOGGER.error("Error receiving message: %s", error)
            self.ws = None
            raise UpdateFailed(f"Failed to receive message: {error}") from error

    async def _async_update_data(self) -> dict:
        """Fetch latest data from the WebSocket connection."""
        try:
            if not self._is_connected():
                await self._connect()

            # Request latest prices
            await self._send_message({
                "type": "get_prices",
                "user_name": self.user_name
            })

            while True:  # Keep receiving messages until we get price data
                response = await self._receive_message()
                processed_data = await self._process_message(response)

                if processed_data is not None:
                    self._last_data = processed_data
                    return self._last_data

        except Exception as error:
            _LOGGER.error("Error updating data: %s", error)
            # If we have previous data, return it instead of failing
            if self._last_data is not None:
                return self._last_data
            raise UpdateFailed(f"Failed to update data: {error}") from error
