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
        site_name: str,
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
        self.site_name = site_name
        self.ws: WebSocketClientProtocol | None = None
        self._authenticated = False
        self._last_data = None
        self._reconnect_task = None
        self._keep_alive_task = None
        self._listener_task = None
        self.instance_id = f"opp_energy_{email}_{site_name}"

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

            # Start message listener task
            if self._listener_task is None or self._listener_task.done():
                self._listener_task = asyncio.create_task(self._listen_for_messages())

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

    async def _listen_for_messages(self) -> None:
        """Listen for incoming WebSocket messages continuously."""
        while self._is_connected():
            try:
                message = await self.ws.recv()
                parsed_message = json.loads(message)
                _LOGGER.debug("Received message: %s", parsed_message)
                processed_data = await self._process_message(parsed_message)

                if processed_data is not None:
                    self._last_data = processed_data
                    # Update the coordinator's data and notify entities
                    self.async_set_updated_data(self._last_data)
            except Exception as error:  # noqa: BLE001
                _LOGGER.error("Error in message listener: %s", error)
                self.ws = None
                # Schedule reconnection
                if self._reconnect_task is None or self._reconnect_task.done():
                    self._reconnect_task = asyncio.create_task(self._reconnect())
                break

    async def _reconnect(self) -> None:
        """Handle reconnection to WebSocket server."""
        retry_count = 0
        max_retries = 5
        retry_delay = 5  # seconds

        while retry_count < max_retries:
            _LOGGER.info("Attempting to reconnect to WebSocket (attempt %s of %s)",
                        retry_count + 1, max_retries)
            try:
                await self._connect()
                if self._is_connected():
                    _LOGGER.info("Successfully reconnected to WebSocket")
                    return
            except Exception as error:  # noqa: BLE001
                _LOGGER.error("Reconnection attempt failed: %s", error)

            retry_count += 1
            await asyncio.sleep(retry_delay)

        _LOGGER.error("Failed to reconnect after %s attempts", max_retries)

    async def _keep_alive(self) -> None:
        """Keep the WebSocket connection alive."""
        while True:
            try:
                if self._is_connected():
                    await self._send_message({"type": "ping"})
                # Try to reconnect if connection is lost
                elif self._reconnect_task is None or self._reconnect_task.done():
                    self._reconnect_task = asyncio.create_task(self._reconnect())
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
                "user_name": self.user_name,  # Pass as display name
                "email": self.email,  # This is used as username
                "password": self.password,
                "site_name": self.site_name
            }
            _LOGGER.debug("Sending authentication message")

            await self._send_message(auth_message)
            response = await self._receive_message()

            await self._process_message(response)

            if self._authenticated:
                # Subscribe to price updates after successful authentication
                await self._send_message({
                    "type": "subscribe_prices",
                    "user_name": self.email  # Use email as username for subscription
                })
                _LOGGER.debug("Subscribed to price updates")
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
        """Check connection health and return latest data."""
        try:
            if not self._is_connected():
                await self._connect()

            # Make sure we're authenticated before requesting data
            if not self._authenticated:
                await self._authenticate()

            # The actual data updates should come from the continuous message listener
            if self._last_data is None:
                # Only request data if we don't have any yet
                await self._send_message({
                    "type": "get_prices",
                    "user_name": self.user_name
                })
                # Wait briefly for a response
                await asyncio.sleep(1)

            return self._last_data or {}  # noqa: TRY300

        except Exception as error:
            _LOGGER.error("Error in update coordinator: %s", error)
            if self._last_data is not None:
                return self._last_data
            raise UpdateFailed(f"Failed to update data: {error}") from error

    async def async_shutdown(self) -> None:
        """Close WebSocket connection on shutdown."""
        _LOGGER.debug("Shutting down OPP Energy coordinator")

        # Cancel background tasks
        if self._keep_alive_task and not self._keep_alive_task.done():
            self._keep_alive_task.cancel()

        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()

        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()

        # Close WebSocket connection
        if self.ws is not None and not self.ws.closed:
            await self.ws.close()
            self.ws = None
