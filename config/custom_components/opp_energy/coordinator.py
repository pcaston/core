import asyncio  # noqa: D100
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
    """Class to manage fetching OPP Energy data and providing remote access."""

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

        # Store reference to the Home Assistant instance
        self._hass = hass

        # For remote access session tracking
        self._remote_sessions = {}
        self._remote_command_handlers = {
            "get_states": self._handle_get_states,
            "call_service": self._handle_call_service,
            "get_services": self._handle_get_services,
            "get_config": self._handle_get_config,
            "get_areas": self._handle_get_areas,
            "get_devices": self._handle_get_devices,
            "get_entities": self._handle_get_entities,
            "subscribe_events": self._handle_subscribe_events,
            "unsubscribe_events": self._handle_unsubscribe_events,
        }

        # Message queue for synchronization
        self._message_queue = asyncio.Queue()
        self._response_futures = {}
        self._next_message_id = 0

        # Event subscription tracking
        self._event_subscriptions = {}

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
                ping_interval=60,
                ping_timeout=20,
            )
            _LOGGER.debug("WebSocket connection established")

            # Start message listener task
            if self._listener_task is None or self._listener_task.done():
                self._listener_task = asyncio.create_task(self._listen_for_messages())

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

    async def _listen_for_messages(self) -> None:
        """Listen for incoming WebSocket messages continuously."""
        while self._is_connected():
            try:
                message = await self.ws.recv()
                parsed_message = json.loads(message)
                _LOGGER.debug("Received message: %s", parsed_message)

                # Handle the message based on its type
                msg_type = parsed_message.get("type")

                if msg_type == "pong":
                    # Handle pong directly - no need to process further
                    _LOGGER.debug("Received pong response")
                    continue

                if msg_type == "auth_success":
                    # Handle authentication success
                    _LOGGER.debug("Authentication successful")
                    self._authenticated = True
                    continue

                if msg_type == "price_update":
                    # Process price updates
                    _LOGGER.debug("Received price data")
                    price_data = parsed_message.get("data", {})
                    if not price_data and "buy_price" in parsed_message:
                        # Handle case where price data is in root of message
                        price_data = {
                            "buy_price": parsed_message.get("buy_price"),
                            "sell_price": parsed_message.get("sell_price"),
                            "timestamp": parsed_message.get("timestamp")
                        }

                    # Update our last data and notify entities
                    if price_data:
                        self._last_data = price_data
                        self.async_set_updated_data(self._last_data)
                    continue

                # Handle remote access commands
                if msg_type == "remote_command":
                    await self._handle_remote_command(parsed_message)
                    continue

                # Handle Home Assistant state request
                if msg_type == "get_hass_state_request":
                    await self._handle_hass_state_request(parsed_message)
                    continue

                # For other message types, check if it's a response to a pending request
                message_id = parsed_message.get("id")
                if message_id and message_id in self._response_futures:
                    # This is a response to a specific request
                    future = self._response_futures.pop(message_id)
                    future.set_result(parsed_message)

            except Exception as error:  # noqa: BLE001
                _LOGGER.error("Error in message listener: %s", error)
                self.ws = None
                # Schedule reconnection
                if self._reconnect_task is None or self._reconnect_task.done():
                    self._reconnect_task = asyncio.create_task(self._reconnect())
                break

    async def _handle_hass_state_request(self, message: dict) -> None:
        """Handle request for Home Assistant state."""
        instance_id = message.get("instance_id")
        entity_id = message.get("entity_id")
        message_id = message.get("id")

        # Verify this request is for us
        if instance_id != self.instance_id:
            _LOGGER.debug("Ignoring state request for different instance: %s", instance_id)
            return

        _LOGGER.debug("Processing Home Assistant state request: %s", message)

        try:
            # Collect states
            if entity_id:
                # Get specific entity state
                state = self._hass.states.get(entity_id)
                if state is None:
                    await self._send_message_without_response({
                        "type": "hass_state_response",
                        "success": False,
                        "error": f"Entity not found: {entity_id}",
                        "id": message_id
                    })
                    return

                states_data = {
                    entity_id: {
                        "state": state.state,
                        "attributes": dict(state.attributes),
                        "last_changed": state.last_changed.isoformat(),
                        "last_updated": state.last_updated.isoformat()
                    }
                }
            else:
                # Get all states
                states_data = {}
                for state in self._hass.states.async_all():
                    states_data[state.entity_id] = {
                        "state": state.state,
                        "attributes": dict(state.attributes),
                        "last_changed": state.last_changed.isoformat(),
                        "last_updated": state.last_updated.isoformat()
                    }

            # Send response with states
            await self._send_message_without_response({
                "type": "hass_state_response",
                "success": True,
                "data": states_data,
                "id": message_id
            })

        except Exception as error:  # noqa: BLE001
            _LOGGER.error("Error handling Home Assistant state request: %s", error)
            await self._send_message_without_response({
                "type": "hass_state_response",
                "success": False,
                "error": str(error),
                "id": message_id
            })

    async def _keep_alive(self) -> None:
        """Keep the WebSocket connection alive."""
        while True:
            try:
                if self._is_connected():
                    await self._send_message_without_response({"type": "ping"})
                # Try to reconnect if connection is lost
                elif self._reconnect_task is None or self._reconnect_task.done():
                    self._reconnect_task = asyncio.create_task(self._reconnect())
                await asyncio.sleep(30)  # Send message every 30 seconds
            except Exception as e:  # noqa: BLE001
                _LOGGER.error("Keep-alive error: %s", e)
                self.ws = None
                await asyncio.sleep(60)  # Wait before retrying

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

    async def _send_message_without_response(self, message: dict) -> None:
        """Send a message through the WebSocket connection without expecting a response."""
        if not self._is_connected():
            await self._connect()

        try:
            await self.ws.send(json.dumps(message))
            _LOGGER.debug("Sent message: %s", message)
        except Exception as error:
            _LOGGER.error("Error sending message: %s", error)
            self.ws = None
            raise UpdateFailed(f"Failed to send message: {error}") from error

    async def _send_message_with_response(self, message: dict, timeout: int = 5) -> dict:
        """Send a message and wait for a response with matching ID."""
        if not self._is_connected():
            await self._connect()

        try:
            # Add a unique ID to the message
            message_id = self._next_message_id
            self._next_message_id += 1
            message["id"] = message_id

            # Create a future to receive the response
            response_future = asyncio.Future()
            self._response_futures[message_id] = response_future

            # Send the message
            await self.ws.send(json.dumps(message))
            _LOGGER.debug("Sent message with ID %s: %s", message_id, message)

            # Wait for response with timeout
            try:
                return await asyncio.wait_for(response_future, timeout)
            except TimeoutError:
                self._response_futures.pop(message_id, None)
                raise UpdateFailed(f"Timeout waiting for response to message ID {message_id}")  # noqa: B904

        except Exception as error:
            _LOGGER.error("Error sending message: %s", error)
            self.ws = None
            raise UpdateFailed(f"Failed to send message: {error}") from error

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
                "site_name": self.site_name
            }
            _LOGGER.debug("Sending authentication message")

            # Authentication doesn't use the ID mechanism, so we send without response
            await self._send_message_without_response(auth_message)

            # The listener task will set self._authenticated if auth succeeds
            # Wait for authentication to complete or timeout
            start_time = asyncio.get_event_loop().time()
            while not self._authenticated:
                await asyncio.sleep(0.1)
                if asyncio.get_event_loop().time() - start_time > 5:  # 5 seconds timeout
                    raise UpdateFailed("Authentication timed out")  # noqa: TRY301

            # Subscribe to price updates after successful authentication
            await self._send_message_without_response({
                "type": "subscribe_prices",
                "user_name": self.email
            })
            _LOGGER.debug("Subscribed to price updates")

            # Register this instance for remote access
            await self._send_message_without_response({
                "type": "register_remote_access",
                "user_name": self.email,
                "site_name": self.site_name,
                "instance_id": self.instance_id
            })
            _LOGGER.debug("Registered for remote access")

        except Exception as error:
            self._authenticated = False
            _LOGGER.error("Authentication error: %s", error)
            self.ws = None
            raise UpdateFailed(f"Authentication error: {error}") from error

    async def _async_update_data(self) -> dict:
        """Check connection health and return latest data."""
        try:
            if not self._is_connected():
                await self._connect()

            # If we don't have data yet, request it explicitly
            if self._last_data is None:
                # Only request data if we're authenticated
                if self._authenticated:
                    await self._send_message_without_response({
                        "type": "get_prices",
                        "user_name": self.email
                    })
                    # Wait briefly for a response which will be handled by the listener
                    await asyncio.sleep(1)
                else:
                    _LOGGER.warning("Not requesting prices because not authenticated")

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

        # Clean up response futures
        for future in self._response_futures.values():
            if not future.done():
                future.cancel()
        self._response_futures.clear()

        # Clean up event subscriptions
        for unsub in self._event_subscriptions.values():
            if callable(unsub):
                unsub()
        self._event_subscriptions.clear()

        # Close WebSocket connection
        if self.ws is not None and not self.ws.closed:
            await self.ws.close()
            self.ws = None

    # Remote access methods

    async def _handle_remote_command(self, message: dict) -> None:
        """Process remote command from the cloud server."""
        command = message.get("command")
        session_id = message.get("session_id")
        command_id = message.get("command_id")

        if not session_id:
            _LOGGER.error("Received remote command without session_id")
            return

        if command not in self._remote_command_handlers:
            _LOGGER.error("Unknown remote command: %s", command)
            await self._send_message_without_response({
                "type": "remote_response",
                "session_id": session_id,
                "command_id": command_id,
                "success": False,
                "error": f"Unknown command: {command}"
            })
            return

        try:
            # Register this session if it's new
            if session_id not in self._remote_sessions:
                self._remote_sessions[session_id] = {
                    "created_at": asyncio.get_event_loop().time(),
                    "last_activity": asyncio.get_event_loop().time(),
                    "subscriptions": []
                }
            else:
                # Update last activity
                self._remote_sessions[session_id]["last_activity"] = asyncio.get_event_loop().time()

            # Execute the command handler
            handler = self._remote_command_handlers[command]
            result = await handler(message)

            # Send the response
            await self._send_message_without_response({
                "type": "remote_response",
                "session_id": session_id,
                "command_id": command_id,
                "success": True,
                "result": result
            })

        except Exception as error:  # noqa: BLE001
            _LOGGER.error("Error handling remote command %s: %s", command, error)
            await self._send_message_without_response({
                "type": "remote_response",
                "session_id": session_id,
                "command_id": command_id,
                "success": False,
                "error": str(error)
            })

    async def _handle_get_states(self, message: dict) -> dict:
        """Handle get_states command."""
        entity_id = message.get("entity_id")

        if entity_id:
            state = self._hass.states.get(entity_id)
            if state is None:
                raise ValueError(f"Entity not found: {entity_id}")
            return {
                "entity_id": state.entity_id,
                "state": state.state,
                "attributes": dict(state.attributes),
                "last_changed": state.last_changed.isoformat(),
                "last_updated": state.last_updated.isoformat()
            }
        result = []
        for state in self._hass.states.async_all():
            result.append({  # noqa: PERF401
                "entity_id": state.entity_id,
                "state": state.state,
                "attributes": dict(state.attributes),
                "last_changed": state.last_changed.isoformat(),
                "last_updated": state.last_updated.isoformat()
            })
        return result

    async def _handle_call_service(self, message: dict) -> dict:
        """Handle call_service command."""
        domain = message.get("domain")
        service = message.get("service")
        service_data = message.get("service_data", {})
        target = message.get("target")

        if not domain or not service:
            raise ValueError("Domain and service are required")

        # Construct service call parameters
        params = {"domain": domain, "service": service}

        if service_data:
            params["service_data"] = service_data

        if target:
            params["target"] = target

        # Make the service call
        await self._hass.services.async_call(**params, blocking=True)

        return {"success": True}

    async def _handle_get_services(self, message: dict) -> dict:
        """Handle get_services command."""
        domain = message.get("domain")
        services = {}  # noqa: F841

        services_domains = await self._hass.services.async_get_services()

        if domain:
            if domain not in services_domains:
                raise ValueError(f"Domain not found: {domain}")
            return {domain: services_domains[domain]}

        return services_domains

    async def _handle_get_config(self, message: dict) -> dict:
        """Handle get_config command."""
        return {
            "location_name": self._hass.config.location_name,
            "latitude": self._hass.config.latitude,
            "longitude": self._hass.config.longitude,
            "elevation": self._hass.config.elevation,
            "time_zone": str(self._hass.config.time_zone),
            "unit_system": self._hass.config.units.as_dict(),
            "version": self._hass.config.version,
            "state": self._hass.state.value
        }

    async def _handle_get_areas(self, message: dict) -> dict:
        """Handle get_areas command."""
        area_registry = self._hass.data["area_registry"]
        areas = []

        for area in area_registry.async_list_areas():
            areas.append({  # noqa: PERF401
                "area_id": area.id,
                "name": area.name,
                "picture": area.picture
            })

        return areas

    async def _handle_get_devices(self, message: dict) -> dict:
        """Handle get_devices command."""
        device_registry = self._hass.data["device_registry"]
        devices = []

        for device in device_registry.devices.values():
            devices.append({  # noqa: PERF401
                "id": device.id,
                "name": device.name_by_user or device.name,
                "manufacturer": device.manufacturer,
                "model": device.model,
                "sw_version": device.sw_version,
                "area_id": device.area_id,
                "connections": [list(conn) for conn in device.connections],
                "identifiers": [list(ident) for ident in device.identifiers],
                "disabled": device.disabled,
                "disabled_by": device.disabled_by,
                "via_device_id": device.via_device_id
            })

        return devices

    async def _handle_get_entities(self, message: dict) -> dict:
        """Handle get_entities command."""
        entity_registry = self._hass.data["entity_registry"]
        entities = []

        for entity in entity_registry.entities.values():
            entities.append({  # noqa: PERF401
                "entity_id": entity.entity_id,
                "name": entity.name,
                "device_id": entity.device_id,
                "area_id": entity.area_id,
                "disabled": entity.disabled,
                "disabled_by": entity.disabled_by,
                "platform": entity.platform,
                "domain": entity.domain,
                "unique_id": entity.unique_id,
                "has_entity_name": entity.has_entity_name,
                "original_name": entity.original_name
            })

        return entities

    async def _forward_event(self, session_id, subscription_id, event):
        """Forward events to the WebSocket client."""
        if not self._is_connected():
            return

        event_data = {
            "event_type": event.event_type,
            "data": dict(event.data),
            "origin": event.origin,
            "time_fired": event.time_fired.isoformat(),
            "context": {
                "id": event.context.id,
                "parent_id": event.context.parent_id,
                "user_id": event.context.user_id
            }
        }

        await self._send_message_without_response({
            "type": "remote_event",
            "session_id": session_id,
            "subscription_id": subscription_id,
            "event": event_data
        })

    async def _handle_subscribe_events(self, message: dict) -> dict:
        """Handle subscribe_events command."""
        session_id = message.get("session_id")
        event_type = message.get("event_type")
        subscription_id = f"{session_id}_{event_type or 'all'}"

        # Check if already subscribed
        if subscription_id in self._event_subscriptions:
            return {"subscription_id": subscription_id}

        # Create the event callback
        callback = lambda event: asyncio.create_task(  # noqa: E731
            self._forward_event(session_id, subscription_id, event)
        )

        # Subscribe to the event
        unsub = self._hass.bus.async_listen(event_type, callback)

        # Store the subscription
        self._event_subscriptions[subscription_id] = unsub

        # Add to session tracking
        if session_id in self._remote_sessions:
            self._remote_sessions[session_id]["subscriptions"].append(subscription_id)

        return {"subscription_id": subscription_id}

    async def _handle_unsubscribe_events(self, message: dict) -> dict:
        """Handle unsubscribe_events command."""
        subscription_id = message.get("subscription_id")

        if not subscription_id or subscription_id not in self._event_subscriptions:
            raise ValueError(f"Subscription not found: {subscription_id}")

        # Unsubscribe
        unsub = self._event_subscriptions.pop(subscription_id)
        unsub()

        # Remove from session tracking
        session_id = subscription_id.split("_")[0]
        if session_id in self._remote_sessions:
            if subscription_id in self._remote_sessions[session_id]["subscriptions"]:
                self._remote_sessions[session_id]["subscriptions"].remove(subscription_id)

        return {"success": True}
