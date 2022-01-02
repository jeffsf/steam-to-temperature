"""
Copyright © 2021-2022 Jeff Kletsky. All Rights Reserved.
"""

import asyncio
import enum
import json
import inspect
import logging
import os
import queue
import time
import traceback
from asyncio import Task

from datetime import datetime
from operator import mul
from socket import gethostname
from struct import unpack, unpack_from, pack
from typing import Optional, Union, NamedTuple, Coroutine, Awaitable, Callable

import requests

from bleak import BleakError, BleakClient, BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData

import paho.mqtt.client as mqtt
from bleak.exc import BleakDBusError
from paho.mqtt.client import MQTTMessage, MQTTv5, MQTT_CLEAN_START_FIRST_ONLY

import pyDE1  # for pyDE1.getLogger()
import pyDE1.shutdown_manager as sm
import pyDE1.pyde1_logging as pyde1_logging
from pyDE1.de1.c_api import API_MachineStates, API_Substates
from pyDE1.config import ConfigYAML, ConfigLoadable
from pyDE1.dispatcher.resource import DE1ModeEnum, Resource
from pyDE1.event_manager import SequencerGateName  # Move out


class Config(ConfigYAML):
    DEFAULT_CONFIG_FILE = '/usr/local/etc/pyde1/bluedot.conf'

    def __init__(self):
        super(Config, self).__init__()
        self.mqtt = _MQTT()
        self.http = _HTTP()
        self.steam = _Steam()
        self.thermometer = _Thermometer()
        self.logging = _Logging()


class _MQTT(ConfigLoadable):
    def __init__(self):
        self.TOPIC_ROOT = 'pyDE1'
        self.CLIENT_ID_PREFIX = 'pyde1-bluedot'
        self.BROKER_HOSTNAME = '::'
        self.BROKER_PORT = 1883
        self.TRANSPORT = 'tcp'
        self.KEEPALIVE = 60
        self.USERNAME = None
        self.PASSWORD = None
        self.DEBUG = False
        self.TLS = False  # Set True, or rest of TLS is ignored
        # See paho Client.tls_set() for details
        self.TLS_CA_CERTS = None
        self.TLS_CERTFILE = None
        self.TLS_KEYFILE = None
        self.TLS_CERT_REQS = None
        self.TLS_VERSION = None
        self.TLS_CIPHERS = None


class _HTTP(ConfigLoadable):
    def __init__(self):
        # No trailing slash
        self.PYDE1_BASE_URL = 'http://localhost:1234'


class _Steam(ConfigLoadable):
    def __init__(self):
        self.STOP_LAG = 1.0   # 0.530 is from API call on localhost
        self.SKIP_INITIAL_SECONDS = 4.0
        self.MAX_SAMPLES_FOR_ESTIMATE = 5


class _Thermometer(ConfigLoadable):
    def __init__(self):
        self.BT_ADDRESS = '00:a0:50:aa:bb:cc'


class _Logging (pyde1_logging.ConfigLogging):
    def __init__(self):
        super(_Logging, self).__init__()
        # NB: The log file name is matched against [a-zA-Z0-9._-]
        self.LOG_FILENAME = 'bluedot.log'
        self.LOGGERS = {
            'Lock':             'WARNING',
            'MQTTClient':       'INFO',
            'root.asyncio':     'INFO',
            'root.bleak':       'INFO',
        }
        self.formatters.STYLE = '%'
        self.formatters.LOGFILE = \
            '%(asctime)s %(levelname)s %(name)s: %(message)s'
        self.formatters.STDERR = \
                        '%(levelname)s %(name)s: %(message)s'


config = Config()

#
# End of Config
#

# TODO: Make this a property of SequencerGateName

GATE_NAMES_AFTER_FLOW = (
    SequencerGateName.GATE_FLOW_END.value,
    SequencerGateName.GATE_FLOW_STATE_EXIT.value,
    SequencerGateName.GATE_LAST_DROPS.value,
    SequencerGateName.GATE_SEQUENCE_COMPLETE.value,
)

#
# The MQTT and Bluetooth listeners will queue their packets
# for later processing. While this doesn't get around the GIL,
# it does provide for more asynchronous processing.
#


# "Temporarily" repeated and modified here (again)
async def async_queue_get(from_queue: queue.Queue):
    """
    Awaitable queue watcher

    Returns a queue entry or None when exited
    """
    loop = asyncio.get_running_loop()
    done = False
    data = None  # For exit on shutdown
    while not done and not sm.shutdown_underway.is_set():
        try:
            data = await loop.run_in_executor(
                None,
                from_queue.get, True, 1.0)
                            # blocking, timeout
            done = True
        except queue.Empty:
            pass
    if sm.shutdown_underway.is_set():
        logger.info("Shut down async_queue_get")
    return data


class LockLogger:

    def __init__(self, lock: asyncio.Lock, name: str):
        self._lock = lock
        self._name = name
        self._logger = pyDE1.getLogger(f'Lock.{self._name}')
        self._checked = None
        self._acquired = None

    def check(self):
        if self._lock.locked():
            self._checked = time.time()
            self._logger.warning(
                f"Waiting for lock {self.call_str()}")
        else:
            self._checked = None
        return self  # Allows lock_logger = LockLogger(...).check()

    def acquired(self, full_trace=False):
        self._acquired = time.time()
        if self._checked:
            dt = (self._acquired - self._checked) * 1000
            # Warning here allows setting log level to warning
            # and still seeing how long the waits were
            self._logger.warning(
                f"Acquired lock after {dt:.0f} ms {self.call_str(full_trace)}")
        else:
            self._logger.info(
                f"Acquired lock {self.call_str()}")

    def released(self, full_trace=False):
        dt = (time.time() - self._acquired) * 1000
        if self._lock.locked():
            self._logger.error(
                f"NOT RELEASED after {dt:.0f} ms {self.call_str()}")
        else:
            self._logger.info(
                f"Released lock after {dt:.0f} ms {self.call_str(full_trace)}")

    @staticmethod
    def call_str(full_trace=True):
        stack = inspect.stack()[2]
        retval = f"at {stack.function}:{stack.lineno}"
        if full_trace:
            idx = 3
            while True:
                try:
                    stack = inspect.stack()[idx]
                    next_caller = f"{stack.function}:{stack.lineno}"
                    if stack.function.startswith('_run'):
                        break
                    retval = f"{retval} < {next_caller}"
                except IndexError:
                    break
                idx += 1
        return retval


# The queue will either have a BDNotification or an MQTTPacket
# BDUnit is specific to the thermometer protocol and is declared later

class BDNotification (NamedTuple):
    arrival_time: float
    raw_data: bytearray
    temperature: float
    high_alarm: float
    units: "BDUnit"
    alarm_byte: Union[bytearray, int]


class MQTTPacket (NamedTuple):
    arrival_time: float
    message: MQTTMessage


class ReconnectingBTDevice:
    """
    Mix-in to manage connection, disconnection, and reconnection
    """

    def __init__(self, address_or_ble_device: Union[BLEDevice, str]):
        self.should_auto_reconnect = True

        self._client = BleakClient(address_or_ble_device)
        self._connection_lock = asyncio.Lock()
        self._connection_lock_last_checked: Optional[float] = None
        self._connection_task: Optional[asyncio.Task] = None
        self._connection_event = asyncio.Event()
        self._ready_event = asyncio.Event()
        self._willful_disconnect = False
        self._reconnect_count = 0  # internal state
        self._reconnect_log_limit = 10 # count after which use gap_long w/o log
        self._connect_timeout = 10 # seconds, default is 10
        self._recheck_gap = 0  # seconds, "0" gives 10-second scans with no gap
        self._recheck_gap_long = 20  # seconds (every 30 sec for 10 seconds)
        self._logger = pyDE1.getLogger(self.__class__.__name__)

        self._client.set_disconnected_callback(
            self._create_bluetooth_disconnected_callback())

        # _connection_lock handles the primary use case of wait_for_connected()
        # running in "slow mode" and wanting to "try now", such as a steam
        # sequence starting.
        #
        # It should be acquired internally in the process of:
        #   * Creating a wait_for_connected() task
        #   * Terminating an existing wait_for_connected() task
        #   * Explicit connection attempt and subsequent initialization
        #   * Explicit disconnection
        #
        # Assuming that this is obeyed, an in-process connection/initialization
        # would not be able to be cancelled by a new wait_for_connected()
        # call cancelling the previous task

    @property
    def address(self):
        return self._client.address

    @property
    def logstr(self):
        return "{} at {}".format(
            self.__class__.__name__,
            self._client.address
        )

    @property
    def is_connected(self):
        return self._client.is_connected

    async def wait_for_connected(self):
        return await self._connection_event.wait()

    def _mark_connected(self):
        # Note that "ready" needs to be handled by the subclass
        self._willful_disconnect = False
        self._connection_event.set()

    def _mark_disconnected(self):
        self._ready_event.clear()
        self._connection_event.clear()

    @property
    def is_ready(self) -> Optional[bool]:
        """
        NB: Subclass needs to manage setting and clearing _ready_event()
        """
        return self._ready_event.is_set()

    async def wait_for_ready(self) -> bool:
        return await self._ready_event.wait()

    async def connect_with_retry(self):
        """
        Start or restart a task to connect when the device becomes available
        Always returns "immediately", can use wait_for_connected()
        or poll is_connected (or "ready" if implemented by subclass)
        """
        ll = LockLogger(self._connection_lock, 'connection_lock').check()
        async with self._connection_lock:
            ll.acquired()
            await self._connect_with_retry_have_lock()
        ll.released()

    async def connect_with_retry_abandon(self):
        """
        Cancel any pending connect_with_retry() task

        should_auto_reconnect is unmodified,
        but nothing should remain that is trying to reconnect
        """
        ll = LockLogger(self._connection_lock, 'connection_lock').check()
        async with self._connection_lock:
            ll.acquired()
            await self._connect_with_retry_abandon_have_lock()
        ll.released()

    async def connect(self, log_attempts=False) -> bool:
        """
        Make a single connection attempt. See also connect_with_retry()

        Raises TimeoutError or BleakError if unable to connect.

        If the device isn't available yet, <class 'bleak.exc.BleakError'>:
        BleakError('Device with address 00:a0:50:aa:bb:cc was not found.')
        or possibly 'Device with address {0} could not be found. ...'

        Also can return "Connection was not successful! (...)"

        Returns connected state as Boolean
        """
        if log_attempts:
            self._logger.info(
                f"Connect requested for {self.logstr}")

        ll = LockLogger(self._connection_lock, 'connection_lock').check()
        async with self._connection_lock:
            ll.acquired()

            # MessageBus._message_reader() seems to take up to 150 ms
            # especially when connecting.
            old_duration = loop.slow_callback_duration
            new_duration = 0.150
            if log_attempts:
                self._logger.debug(
                    "Changing slow_callback_duration from "
                    f"{old_duration:.3f} to {new_duration:.3f}")
            loop.slow_callback_duration = new_duration

            if not self._client.is_connected:
                try:
                    await self._client.connect(timeout=self._connect_timeout)
                finally:
                    if log_attempts:
                        self._logger.debug(
                            "Restoring slow_callback_duration to "
                            f"{old_duration:.3f}")
                    loop.slow_callback_duration = old_duration

                if self._client.is_connected:
                    self._mark_connected()
                    self._logger.info(
                        f"Connected {self.logstr}, calling _after_connect()")
                    await self._after_connect()
                    self._logger.info(
                        f"Initialized {self.logstr}")
                elif log_attempts:
                    self._logger.warning(
                        f"Failed to connect {self.logstr}")

        ll.released()
        return self._client.is_connected

    async def disconnect(self):
        # Lock helps prevent race condition where connect is in progress
        # and another connect() or disconnect() is called
        self._logger.info(
            f"Disconnect requested for {self.logstr}")

        ll = LockLogger(self._connection_lock, 'connection_lock').check()
        async with self._connection_lock:
            ll.acquired()
            await self._connect_with_retry_abandon_have_lock()
            if self._client.is_connected:
                await self._before_disconnect()
                self._willful_disconnect = True
                await self._client.disconnect()
            if self._client.is_connected:
                self._logger.warning(
                    f"Failed to disconnect {self.logstr}")
            else:
                self._mark_disconnected()
                self._logger.info(
                    f"Disconnected {self.logstr}")
        ll.released()
        return self._client.is_connected

    async def _after_connect(self):
        pass

    async def _before_disconnect(self):
        pass

    # Internals:

    async def _connect_with_retry_abandon_have_lock(self):
        if self._connection_task is None:
            return
        if not self._connection_task.done():
            self._logger.info(
                f"Terminating {self._connection_task.get_name()}")
            self._connection_task.cancel()
            # Give it 50 ms to clean up
            delay_ms = 50
            try:
                await asyncio.wait_for(self._connection_task, delay_ms/1000)
            except asyncio.TimeoutError:
                self._logger.error(
                    f"Not done after {delay_ms} ms, "
                    f"removing reference anyways: {self._connection_task}")
        self._connection_task = None

    async def _connect_with_retry_have_lock(self):

        # Stop any already running connection task
        if self._connection_task is None:
            self._logger.info("Starting connect_with_retry()")
        else:
            self._logger.info("Retarting connect_with_retry()")
        await self._connect_with_retry_abandon_have_lock()

        # Create the new task
        self._connection_task = asyncio.create_task(
            self._connect_with_retry_inner(),
            name = 'ConnectWithRetry')
        self._connection_task.add_done_callback(
            self._create_connection_task_done_cb())

    def _create_connection_task_done_cb(self) -> Callable:
        device = self

        def connection_task_done_cb(task: Task):
            if task.cancelled():
                self._logger.info(
                    f"Connection task cancelled")
            elif (e := task.exception()):
                tbe = ''.join(
                    traceback.TracebackException.from_exception(e).format())
                self._logger.error(
                    f"Connection task raised exception: {tbe}")
            else:
                self._logger.info(
                    f"Connection task completed, returned '{task.result()}'")
            try:
                # NB: All bets are off is exception was raised
                #     Potentially should not shift, but retry
                # asyncio.run_coroutine_threadsafe(device._cr_shift(), loop)
                asyncio.create_task(
                    self._cr_process_callback(ConnectivityRequest.CAPTURE))
            except AttributeError:
                pass
            if task == device._connection_task:
                device._connection_task = None

        return connection_task_done_cb

    # TODO: Why does this timeout so quickly on a reconnect?
    # (Seems related to --experimental, see client.py line 175)
    # Opened as https://github.com/hbldh/bleak/issues/713
    #
    # 2021-12-25 19:04:49,451 INFO BlueDOT:
    #       Connect requested for BlueDOT at 00:a0:50:e2:f6:49
    # 2021-12-25 19:04:49,630 INFO BlueDOT:
    #       Device with address 00:a0:50:e2:f6:49 could not be found.
    #       Try increasing `timeout` value or moving the device closer.
    # 2021-12-25 19:04:49,632 INFO BlueDOT:
    #       Connect requested for BlueDOT at 00:a0:50:e2:f6:49
    # 2021-12-25 19:04:49,787 INFO BlueDOT:
    #       Device with address 00:a0:50:e2:f6:49 could not be found.
    #       Try increasing `timeout` value or moving the device closer.


    async def _connect_with_retry_inner(self) -> bool:
        """
        This is the task that gets run by _connect_with_retry_have_lock
        """
        self._reconnect_count = 0
        while not self._client.is_connected and self.should_auto_reconnect \
                and not sm.shutdown_underway.is_set():
            self._reconnect_count += 1
            log_attempts = self._reconnect_count <= self._reconnect_log_limit
            try:
                # The lock should prevent this connect/initialize call
                # from being inadvertently cancelled with a public method
                await self.connect(log_attempts=log_attempts)
            except (asyncio.TimeoutError, TimeoutError, BleakError) as e:
                if isinstance(e, BleakError):
                    # Something of a hack, but need to distinguish
                    # from other BleakError conditions
                    # See https://github.com/hbldh/bleak/issues/527
                    msg: str = e.args[0]
                    if msg.startswith(('Device with address',
                                      'Connection was not successful!')):
                        if log_attempts:
                            self._logger.info(e)
                        if msg.endswith('device closer.'):
                            # Remove _device_path to force scan on connect
                            # See https://github.com/hbldh/bleak/issues/713
                            self._logger.debug(
                                "Issue 527: path: '{}' info: '{}'".format(
                                    self._client._device_path,
                                    self._client._device_info,
                                ))
                            self._client._device_path = None
                            self._client._device_info = None
                    else:
                        raise e

            if self.is_connected:
                break
            else:
                if self._reconnect_count == self._reconnect_log_limit:
                    self._logger.info(
                        "No further logging of connection attempts to "
                        f"{self.logstr}, continuing at "
                        f"{self._recheck_gap_long + self._connect_timeout} "
                        "sec intervals")
                if self._reconnect_count < self._reconnect_log_limit:
                    await asyncio.sleep(self._recheck_gap)
                else:
                    await asyncio.sleep(self._recheck_gap_long)

        if not self.should_auto_reconnect and not self.is_connected:
            self._logger.info(f"Reconnection disabled for {self.logstr}")

        return self._client.is_connected

    def _create_bluetooth_disconnected_callback(self):
        device = self

        def bt_disconnected_cb(client: BleakClient):
            nonlocal device
            # Marking here is not protected by the lock
            # There's some risk of a race condition,
            # marking as disconnected seems less disastrous
            # than ending up with deadlock during disconnect
            # or failing to reflect the change promptly.
            device._mark_disconnected()
            # Remove _device_path to force scan on connect
            # See https://github.com/hbldh/bleak/issues/713
            device._client._device_path = None
            device._client._device_info = None
            if not device._willful_disconnect:
                device._logger.warning(
                    f"Unexpected disconnection from {device.logstr}")
            asyncio.create_task(device._cr_process_callback(
                ConnectivityRequest.RELEASE))

        return bt_disconnected_cb

    async def _cr_process_callback(self, cb_from: "ConnectivityRequest"):
        raise NotImplementedError



class ConnectivityRequest (enum.Enum):
    CAPTURE = 'capture'
    RELEASE = 'release'


# class ConnectivityState (enum.IntEnum):
#     DISCONNECTED = 0
#     CONNECTING = 1
#     WAITING = 2
#     CONNECTED = 3
#     INITIALIZING = 4
#     READY = 5
#     DISCONNECTING = 6


class STCDevice (ReconnectingBTDevice):
    """
    Defines what the SteamTempController needs to manage
    """

    def __init__(self, address_or_ble_device: Union[BLEDevice, str]):
        super(STCDevice, self).__init__(
            address_or_ble_device=address_or_ble_device)

        # These end up getting run from the MQTT thread
        # use asyncio here, and use either
        #   loop.call_soon_threadsafe(callback, *args)
        #   asyncio.run_coroutine_threadsafe(coro_func(), loop)
        self._cr_lock = asyncio.Lock()
        self._cr_queue = asyncio.Queue()
        self._cr_now = ConnectivityRequest.RELEASE
        self._cr_next: Optional[ConnectivityRequest] = None
        self._cr_then: Optional[ConnectivityRequest] = None
        self._cr_task = asyncio.create_task(self._cr_queue_watcher(),
                                            name='CRQueueWatcher')

    def capture(self):
        # This gets called from the MQTT thread
        loop.call_soon_threadsafe(
            self._cr_queue.put_nowait,
            ConnectivityRequest.CAPTURE)

    def release(self):
        # This gets called from the MQTT thread
        loop.call_soon_threadsafe(
            self._cr_queue.put_nowait,
            ConnectivityRequest.RELEASE)

    async def at_steaming_start(self):
        """
        Gets called when entering a steam sequence.

        At completion, device should be providing updates to the queue
        appropriate for rate-of-rise estimation
        """
        raise NotImplementedError

    async def at_steaming_end(self):
        """
        Gets called after leaving a steam sequence.

        At completion, device may continue to provide updates, but not required
        """
        raise NotImplementedError

    async def _cr_queue_watcher(self):
        """
        Runs as a task and manages incoming ConnectionRequest from queue
        """
        logger = pyDE1.getLogger('CRQueue')
        logger.info('Starting CRQueueWatcher')
        while not sm.shutdown_underway.is_set():
            request = await self._cr_queue.get()
            logger.debug(f"CRQueueWatcher got {request}")
            ll = LockLogger(self._cr_lock, 'CaptureRelease').check()
            async with self._cr_lock:
                ll.acquired()

                self._cr_add_request_have_lock(request=request)

            ll.released()

    async def _start_capture(self):
        self._logger.info("_start_capture() called")
        self.should_auto_reconnect = True
        if not self.is_connected:
            # Start looking
            await self.connect_with_retry()
            # connection_task_done_cb does self._clock_down
        elif not self._ready_event.is_set():
            self._logger.info("_start_capture() is calling _after_connect()")
            await self._after_connect()

    async def _start_release(self):
        self._logger.info("_start_release() called")
        self.should_auto_reconnect = False
        if self.is_connected:
            await self.disconnect()

    # ----- New implementation -----

    async def _cr_process_callback(self,cb_from: ConnectivityRequest):
        logger = pyDE1.getLogger('CRManage')
        logger.debug(
            f"Process {cb_from.name} callback entry {self._cr_status_string()}")
        ll = LockLogger(self._cr_lock, 'CaptureRelease').check()
        async with self._cr_lock:
            ll.acquired()

            if cb_from == ConnectivityRequest.CAPTURE:
                assert self._cr_next == ConnectivityRequest.CAPTURE, \
                    f"CAPTURE callback from {self._cr_status_string()}"
                if not self.is_connected:
                    logger.warning(
                        f"{self.logstr} is disconnected after CAPTURE")
                    # Make state consistent
                    self._cr_next = ConnectivityRequest.RELEASE
                self._cr_shift_have_lock()

            elif cb_from == ConnectivityRequest.RELEASE \
                    and self._willful_disconnect:
                assert self._cr_next == ConnectivityRequest.RELEASE, \
                    f"RELEASE callback from {self._cr_status_string()}"
                if self.is_connected:
                    logger.warning(
                        f"{self.logstr} is connected after RELEASE")
                    # Make state consistent
                    self._cr_next = ConnectivityRequest.CAPTURE
                self._cr_shift_have_lock()

            # Written for clarity, effectively an "else" clause
            elif cb_from == ConnectivityRequest.RELEASE \
                    and not self._willful_disconnect:
                # unexpected disconnect
                assert not self.is_connected, \
                    "Unexpected disconnect while connected {}".format(
                        self._cr_status_string())
                if self._cr_now != ConnectivityRequest.CAPTURE:
                    logger.error(
                        "Logic puzzle, unexpected disconnect while {}".format(
                            self._cr_status_string()
                        )
                    )
                logger.debug(
                    "Changing 'now' to RELEASE from unexpected disconnect")
                # Don't shift, as next is still pending
                self._cr_now = ConnectivityRequest.RELEASE
                # With the lock, we're "next", so no need to queue
                if self.should_auto_reconnect \
                        and not sm.shutdown_underway.is_set():
                    logger.info("Requesting reconnection by adding CAPTURE")
                    self._cr_add_request_have_lock(ConnectivityRequest.CAPTURE)

            else:
                logger.error(
                    "Logic error fallthrough for "
                    f"{cb_from}, willful: {self._willful_disconnect}")

        logger.debug(
            f"Process {cb_from.name} callback exit {self._cr_status_string()}")

    def _cr_shift_have_lock(self):
        logger = pyDE1.getLogger('CRManage')
        self._cr_now = self._cr_next
        self._cr_next = self._cr_then
        self._cr_then = None
        logger.debug(
            f"Shifted to {self._cr_status_string()}")
        # As making state consistent might have caused a duplicate
        if self._cr_next == self._cr_now:
            self._cr_now = self._cr_next
            self._cr_next = self._cr_then
            self._cr_then = None
            logger.debug(
                f"Reduced to {self._cr_status_string()}")
        if self._cr_next:
            self._cr_start_action_sync()

    def _cr_start_action_sync(self):
        if self._cr_next == ConnectivityRequest.CAPTURE:
            asyncio.create_task(self._start_capture())
        elif self._cr_next == ConnectivityRequest.RELEASE:
            asyncio.create_task(self._start_release())

    def _cr_status_string(self):
        now = 'None'
        next = 'None'
        then = 'None'
        if self._cr_now:
            now = self._cr_now.name
        if self._cr_next is not None:
            next = self._cr_next.name
        if self._cr_then is not None:
            then = self._cr_then.name
        return '{}, {}, {}'.format(now, next, then)

    def _cr_add_request_have_lock(self, request: ConnectivityRequest):
        logger = pyDE1.getLogger('CRManage')
        logger.debug(f"Adding {request.name} to {self._cr_status_string()}")

        # What is the final state before adding?
        terminal = self._cr_now
        if self._cr_next:
            terminal = self._cr_next
        if self._cr_then:
            terminal = self._cr_then

        if request == terminal:
            start_action = False

        elif self._cr_next is None:
            logger.info(f"Setting next to {request.name}")
            self._cr_next = request
            self._cr_then = None
            start_action = True

        elif self._cr_next == terminal:
            logger.info(f"Setting then to {request.name}")
            self._cr_then = request
            start_action = False
            if self._cr_next == ConnectivityRequest.CAPTURE \
                    and request == ConnectivityRequest.RELEASE:
                logger.info("Aborting CAPTURE in favor of RELEASE")
                asyncio.create_task(self.connect_with_retry_abandon())

        elif self._cr_next == request:
            logger.info(
                f"Next is already {request.name}, clearing then")
            self._cr_then = None
            start_action = False

        else:
            logger.error(
                f"Logic error adding {request} to {self._cr_status_string()}")
            start_action = False

        logger.debug(
            f"Updated to {self._cr_status_string()}, start: {start_action}")

        if start_action:
            self._cr_start_action_sync()


# Sigh, the messiness of nested classes
class BDUnit(enum.IntFlag):
    C = 0
    F = 1

    @property
    def freezing(self):
        if self == BDUnit.C:
            return 0
        else:
            return 32


class BlueDOT (STCDevice):

    def __init__(self, address_or_ble_device: Union[BLEDevice, str],
                 event_queue: queue.Queue):
        super(BlueDOT, self).__init__(
            address_or_ble_device=address_or_ble_device)
        self._event_queue = event_queue
        self._last_time = time.time()
        # CUUID locks
        self._interval_lock = asyncio.Lock()
        self._updates_lock = asyncio.Lock()
        self._high_alarm_lock = asyncio.Lock()
        self._units_lock = asyncio.Lock()

        # NB: This is not threadsafe, pick up from queue
        self._first_update: Optional[BDNotification] = None
        self._have_high_alarm = asyncio.Event()

    def _create_notification_callback(self) -> Callable:
        bluedot = self

        def bluedot_notification_cb(sender: int, data: bytearray):
            nonlocal bluedot

            now = time.time()
            dt = now - bluedot._last_time
            bluedot._last_time = now

            byte_0 = data[0:1].hex()
            (current, high_alarm) = unpack_from('<ii', data, offset=1)
            (units_byte,) = unpack_from('B', data, offset=11)
            byte_12 = data[12:13].hex()
            bluedot_mac = data[13:19]
            alarm_byte = data[19:20]

            ustr = BDUnit(units_byte).name

            self._logger.debug(
                f"{dt:0.3f} {current} {high_alarm} {ustr} "
                f"{byte_0} {byte_12} {alarm_byte.hex()}")

            bluedot._event_queue.put_nowait(
                BDNotification(
                    arrival_time=now,
                    raw_data=data,
                    temperature=current,
                    high_alarm=high_alarm,
                    units=BDUnit(units_byte),
                    alarm_byte=alarm_byte,
                )
            )
        return bluedot_notification_cb

    # _service_uuid = "6e400001-b5a3-f393-e0a9-e50e24dcca9e"

    _interval_cuuid = '60721D99-6698-4EEC-8E0A-50D0C37F17B9'
    _updates_cuuid = '783F2991-23E0-4BDC-AC16-78601BD84B39'
    _high_alarm_cuuid = 'DE0415CF-D54A-4EA4-A58F-C7AA07F79BAA'
    _low_alarm_cuuid = 'BFDBEB45-11A3-4406-BCB4-ED7C6F939FBC'
    _units_cuuid = 'C86CF012-5C33-48AA-82D1-84A57C908CA0'

    async def set_updates_on(self, state=True):
        if not self._client.is_connected:
            self._logger.warning(
                f"Not connected, can't set_updates_on({state})")
            return
        ll = LockLogger(self._updates_lock, 'updates_lock').check()
        async with self._updates_lock:
            ll.acquired()
            if state:
                await self._client.start_notify(
                    self._updates_cuuid,
                    self._create_notification_callback())
            else:
                await self._client.stop_notify(
                    self._updates_cuuid)
            self._logger.info(f"Notification enabled: {state}")
        ll.released()

    # TODO: Should catch not-connected exceptions in
    #       set_update_rate and set_high_alarm
    #       (as well as any new set_*)
    #
    # To some extent, not a big problem if run as a service
    # as long as the app exits and gets restarted.
    # That's about what would happen with a handler anyways.

    async def set_update_rate(self, period: Union[int, float]):
        # tb = "".join(traceback.format_stack(limit=5))
        # self._logger.debug(tb)
        if not self._client.is_connected:
            self._logger.warning(
                f"Not connected, can't set_update_rate({period})")
            return
        ll = LockLogger(self._interval_lock, 'interval_lock').check()
        async with self._interval_lock:
            ll.acquired()
            val = int(round(period))
            await self._client.write_gatt_char(
                self._interval_cuuid, pack('B', val))
            self._logger.info(f"Notification period set to {val}")
        ll.released()

    async def set_high_alarm(self, temperature: Union[int, float]):
        if not self._client.is_connected:
            self._logger.warning(
                f"Not connected, can't set_high_alarm({temperature})")
            return
        ll = LockLogger(self._high_alarm_lock, 'high_alarm_lock').check()
        async with self._high_alarm_lock:
            ll.acquired()
            val = int(round(temperature))
            await self._client.write_gatt_char(
                self._high_alarm_cuuid, pack('B', val))
            self._logger.info(f"High alarm set: {val}")
        ll.released()

    async def go_slow(self):
        await self.set_update_rate(60)

    async def go_fast(self):
        await self.set_update_rate(1)

    async def _after_connect(self):
        # logger.debug(
        #     "after_connect() "
        #     f"is_connected: {self.is_connected}, "
        #     f"is_ready: {self.is_ready} "
        # )
        # for task in asyncio.all_tasks():
        #     logger.debug(task)
        try:
            self._have_high_alarm.clear()
            self._first_update = None
            await self.set_updates_on()
            await asyncio.sleep(1)  # Is this needed, and, if so, when?
            await self.set_update_rate(1)
            wait_for_first = 2.5 # sec
            try:
                await asyncio.wait_for(
                    self._have_high_alarm.wait(),
                    timeout=wait_for_first)
                self._first_update: BDNotification
                old_ha = self._first_update.high_alarm
                new_ha = self._first_update.units.freezing
                await self.set_high_alarm(new_ha)
                await asyncio.sleep(1.0)
                await self.set_high_alarm(old_ha)
            except asyncio.TimeoutError:
                self._logger.warning(
                    f"Did not get update within {wait_for_first:.1f} sec, "
                    "no audio alarm for connection.")
                pass
            await self.go_slow()
        except BleakDBusError as e:
            self._logger.exception("in after_connect()", exc_info=e)
        self._ready_event.set()

    async def _before_disconnect(self):
        self._ready_event.clear()
        await self.go_slow()
        await self.set_updates_on(state=False)

    async def at_steaming_start(self):
        await self.go_fast()

    async def at_steaming_end(self):
        await self.go_slow()


class TimeAtTargetEstimator:
    """
    Class that encapsulates being able to estimate
    when the rate of rise will reach the target
    """

    logger = pyDE1.getLogger('TATest')

    def __init__(self):
        self._history_time = []
        self._history_temperature = []
        self._history_lock = asyncio.Lock()
        self.big_gap = 2  # if gap exceeds, reset estimator
        self.target = None
        self._bhat = None
        self._mhat = None

    async def reset(self):
        async with self._history_lock:
            self._reset_have_lock()

    async def new_sample(self, sample_time: float,
                         temperature: Union[int, float]) -> Optional[float]:
        """
        Call when a new sample arrives

        Returns the time at target estimate as absolute time.time() reference
        """
        async with self._history_lock:
            if self._history_available:
                if (dt := sample_time - self._history_time[-1]) > self.big_gap:
                    self.logger.warning(
                        f"Resetting history after {dt:.1f} sec gap")
                    self._reset_have_lock()
            self._history_time.append(sample_time)
            while len(self._history_time) \
                    > config.steam.MAX_SAMPLES_FOR_ESTIMATE:
                self._history_time.pop(0)
            self._history_temperature.append(temperature)
            while len(self._history_temperature) \
                    > config.steam.MAX_SAMPLES_FOR_ESTIMATE:
                self._history_temperature.pop(0)

            return self._estimate_time_at_target_have_lock()

    async def estimate_time_at_target(self) -> Optional[float]:
        """
        Returns the time at target estimate as reference
        on same timescale as samples are reported
        """
        async with self._history_lock:
            return self._estimate_time_at_target_have_lock()

    @property
    def rate_of_rise(self):
        return self._mhat

    @property
    def current_est(self):
        return self._bhat

    def _reset_have_lock(self):
        self._history_time = []
        self._history_temperature = []
        self._mhat = None
        self._bhat = None
        self.logger.info("Reset history")

    @property
    def _history_available(self):
        # Ultra safe
        return min(len(self._history_temperature), len(self._history_time))

    def _estimate_time_at_target_have_lock(self) -> Optional[float]:
        # Use a least-squares estimator
        ns = self._history_available
        if ns < 2:
            t_target = None
        else:
            # Keep time scaled reasonably
            t0 = self._history_time[-1]
            t_norm = [t - t0 for t in self._history_time[-ns:]]
            s_x = sum(t_norm)
            s_y = sum(self._history_temperature[-ns:])
            s_xx = sum([x * x for x in t_norm])
            # s_yy = sum([y * y for y in self._history_temperature[-ns:]])
            s_xy = sum(map(mul,
                           t_norm,
                           self._history_temperature[-ns:]))
            self._mhat = (ns * s_xy - (s_x * s_y)) \
                   / (ns * s_xx - (s_x * s_x))
            if self._mhat:
                self._bhat = (s_y / ns) - self._mhat * (s_x / ns)
                t_target = t0 + (self.target - self._bhat) / self._mhat
            else:
                self._bhat = None
                t_target = None

        return t_target


class SteamTempController:
    """
    Watches the queue for BDNotifications and SGNotifications
    Manages the state, picks target from BDNotifications
    Triggers a stop properly in advance of reaching the target
    """

    def __init__(self, event_queue: queue.Queue,
                 thermometer: BlueDOT):
        self._event_queue = event_queue
        self._thermometer = thermometer
        self._control_active_time = 0    # 0 or time after which to accept samples
        self._on_trigger_event = asyncio.Event()
        self._trigger_time = None
        self._tat_estimator = TimeAtTargetEstimator()
        self._logger = pyDE1.getLogger('SteamTemp')
        self._notification_queue_watcher_task \
            = asyncio.create_task(self._notification_queue_watcher(),
                                  name='NotificationQueueWatcher')
        self._trigger_checker_task: Optional[Task] = None

    @property
    def target(self):
        return self._tat_estimator.target

    @target.setter
    def target(self, value):
        self._logger.info(f"Target set: {value}")
        self._tat_estimator.target = value

    @property
    def control_active(self):
        return self._control_active_time != 0

    # @control_active.setter
    # def control_active(self, value):
    #     """
    #     What does "activating" explicitly involve? Should it be allowed
    #     Deactivating makes sense
    #     """
    #     raise NotImplementedError

    @property
    def has_triggered(self):
        return self._on_trigger_event.is_set()

    async def on_trigger_event_wait(self):
        await self._on_trigger_event.wait()

    async def _control_activate(self):
        now = time.time()
        asyncio.create_task(self._thermometer.at_steaming_start())
        if self.control_active:
            self._logger.warning(
                "control_activate() called when active, reinitializing")
        try:
            self._trigger_checker_task.cancel()
        except (AttributeError, asyncio.CancelledError):
            pass
        self._trigger_time = None
        await self._tat_estimator.reset()
        self._on_trigger_event.clear()
        self._control_active_time = now + config.steam.SKIP_INITIAL_SECONDS
        self._trigger_checker_task = asyncio.create_task(
            self._trigger_checker(), name='TriggerChecker')
        self._logger.info("Control activated")
        if not self._thermometer.is_ready:
            self._logger.warning(
                f"{self._thermometer.logstr} is not ready")

    async def _control_deactivate(self):
        self._trigger_time = None
        self._on_trigger_event.clear()
        await self._tat_estimator.reset()
        try:
            self._trigger_checker_task.cancel()
        except (AttributeError, asyncio.CancelledError):
            pass
        self._control_active_time = 0
        self._logger.info("No longer being controlled")
        asyncio.create_task(self._thermometer.at_steaming_end())

    async def _trigger_checker(self):

        while (not self.has_triggered
               and not sm.shutdown_underway.is_set()):
            now = time.time()
            if self._trigger_time and \
                    now >= self._trigger_time - config.steam.STOP_LAG:
                self._on_trigger_event.set()
                await asyncio.to_thread(stop_de1_sync)
            else:
                # At 250 cal/sec, 100 g is 2.5°C / sec
                # 0.1 seconds should be well within 1°C
                await asyncio.sleep(0.1)

    async def _notification_queue_watcher(self):

        self._logger.info("Starting notification queue watcher")
        while not sm.shutdown_underway.is_set():
            item: Union[BDNotification, MQTTPacket] = await async_queue_get(
                from_queue=self._event_queue)
            # None gets returned on termination of async_queue_get()
            if item is None:
                if not sm.shutdown_underway.is_set():
                    raise RuntimeError(
                        "async_queue_get() unexpectedly returned None")
                else:
                    break
            elif isinstance(item, BDNotification):
                    await self._process_temperature_update(item)
            elif isinstance(item, MQTTPacket):
                await self._process_mqtt_update(item)
        self._logger.info("Notification queue watcher done")

    async def _process_temperature_update(self, update: BDNotification):

        # Looking for data for "hello"?
        # TODO: This is specific to the BlueDOT
        if self._thermometer._first_update is None:
            self._thermometer._first_update = update
            self._thermometer._have_high_alarm.set()
            self.target = update.high_alarm

        if not self._thermometer.is_ready:
            return

        if self.target != update.high_alarm:
            self.target = update.high_alarm

        if (self.control_active
                and not self.has_triggered
                and update.arrival_time > self._control_active_time):
            self._trigger_time = await self._tat_estimator.new_sample(
                sample_time=update.arrival_time,
                temperature=update.temperature
            )
            if self._trigger_time:
                self._logger.info(
                    "TAT: {} in {:.3f} sec, {:.1f} °/s "
                    "at {:.1f} est {:.1f} rpt".format(
                        datetime.fromtimestamp(
                            self._trigger_time).time().isoformat(
                            timespec='milliseconds'),
                        self._trigger_time - time.time(),
                        self._tat_estimator.rate_of_rise,
                        self._tat_estimator.current_est,
                        update.temperature
                    )
                )

    async def _process_mqtt_update(self, update: MQTTPacket):

        packet_dict = json.loads(update.message.payload)

        if packet_dict['class'] == 'SequencerGateNotification':

            active_state = packet_dict['active_state']
            gate_name = packet_dict['name']

            if active_state == API_MachineStates.Steam.name:

                if packet_dict['action'] != 'set':
                    pass

                elif not self.control_active:
                    if gate_name not in GATE_NAMES_AFTER_FLOW:
                        await self._control_activate()

                else:   # is active
                    if gate_name in GATE_NAMES_AFTER_FLOW:
                        await self._control_deactivate()

            elif self.control_active:
                self._logger.warning(
                    "Deactivating as in a non-steam state: "
                    f"{active_state}, {gate_name}"
                )
                await self._control_deactivate()

            self._logger.debug(
                f"Processed: {packet_dict['action']} {gate_name} {active_state}")

        elif packet_dict['class'] == 'StateUpdate':

            if (state := packet_dict['state']) == API_MachineStates.Sleep.name:
                self._logger.info("DE1 reported Sleep")
                self._thermometer.release()

            else:
                self._logger.info(
                    f"DE1 reported {state},{packet_dict['substate']}")
                self._thermometer.capture()


def stop_de1_sync():
    logger = pyDE1.getLogger('Requests')
    logger.info("Stopping DE1")
    try:
        stop_json = f'{{"mode": "{DE1ModeEnum.END_STEAM.value}"}}'
    except AttributeError:
        # TODO: This should really check the running pyDE1 version
        # stop_json = f'{{"mode": "EndSteam"}}'
        stop_json = f'{{"mode": "{DE1ModeEnum.STOP.value}"}}'
    resp = requests.patch(
        url=f"{config.http.PYDE1_BASE_URL}/{Resource.DE1_MODE.value}",
        data=stop_json)
    if not resp.ok:
        logger.error(f"{resp.status_code} {resp.reason} {resp.content}")
    else:
        logger.info(f"{resp.status_code} {resp.reason}")


def configure_mqtt(queue: queue.Queue) -> mqtt.Client:
    """
    Configure the MQTT client and return it.
    Does not connect or start the loop, see run_mqtt_client_sync()
    """

    logger_mqtt = pyDE1.getLogger('MQTTClient')

    # Set up the MQTT client

    sub_list = [
        (f"{config.mqtt.TOPIC_ROOT}/StateUpdate", 0),
        (f"{config.mqtt.TOPIC_ROOT}/SequencerGateNotification", 0),
    ]

    class UserData (NamedTuple):
        # Internal only, at this time
        queue: queue.Queue

    def on_connect_callback(client: mqtt.Client, userdata,
                            flags, reasonCode, properties):
        logger_mqtt.info(f"MQTT Connect: flags: {flags}, "
                          f"reasonCode: {reasonCode}, properties {properties}")
        client.subscribe(sub_list)

    def on_subscribe_callback(client, userdata, mid,
                              reasonCodes: list[mqtt.ReasonCodes],
                              properties):
        for rc in reasonCodes:
            logger_mqtt.info(f"MQTT Subscribe: "
                              f"reasonCodes: {rc.json()}, properties {properties}")

    def on_message_callback(client: mqtt.Client, userdata,
                            message: MQTTMessage):
        userdata.queue.put_nowait(
            MQTTPacket(arrival_time=time.time(), message=message)
        )

    mqtt_client = mqtt.Client(
        client_id="{}@{}[{}]".format(
            config.mqtt.CLIENT_ID_PREFIX,
            gethostname(),
            os.getpid(),
        ),
        clean_session=None,  # Required for MQTT5
        userdata=UserData(queue=queue),
        protocol=MQTTv5,
        transport=config.mqtt.TRANSPORT,
    )

    if config.mqtt.TLS:
        mqtt_client.tls_set(ca_certs=config.mqtt.TLS_CA_CERTS,
                            certfile=config.mqtt.TLS_CERTFILE,
                            keyfile=config.mqtt.TLS_KEYFILE,
                            cert_reqs=config.mqtt.TLS_CERT_REQS,
                            tls_version=config.mqtt.TLS_VERSION,
                            ciphers=config.mqtt.TLS_CIPHERS)

    if config.mqtt.USERNAME is not None:
        logger_mqtt.info(f"Connecting with username '{config.mqtt.USERNAME}'")
        mqtt_client.username_pw_set(
            username=config.mqtt.USERNAME,
            password=config.mqtt.PASSWORD
        )

    if config.mqtt.DEBUG:
        paho_logger = pyDE1.getLogger('Paho')
        paho_logger.setLevel(logging.DEBUG)
        mqtt_client.enable_logger(paho_logger)

    mqtt_client.on_connect = on_connect_callback
    mqtt_client.on_subscribe = on_subscribe_callback
    mqtt_client.on_message = on_message_callback

    return mqtt_client


def run_mqtt_client_sync(mqtt_client: mqtt.Client):

    mqtt_client.connect(host=config.mqtt.BROKER_HOSTNAME,
                   port=config.mqtt.BROKER_PORT,
                   keepalive=config.mqtt.KEEPALIVE,
                   bind_address="",
                   bind_port=0,
                   clean_start=MQTT_CLEAN_START_FIRST_ONLY,
                   properties=None)

    # "start" returns immediately
    # mqtt_client.loop_start()
    # "forever" returns on exception
    # No other reasonable way to catch, see
    # https://stackoverflow.com/questions/2829329/
    #   catch-a-threads-exception-in-the-caller-thread
    mqtt_client.loop_forever()


async def run():

    #
    # Signal handler to help ensure Bluez is disconnected
    # and threads complete
    #

    loop = asyncio.get_running_loop()

    mqtt_client: Optional[mqtt.Client] = None
    event_queue = queue.Queue()
    bluedot = BlueDOT(address_or_ble_device=config.thermometer.BT_ADDRESS,
                      event_queue=event_queue)

    def on_shutdown_underway_cleanup():
        logger = pyDE1.getLogger('Shutdown')
        logger.info("Watching for shutdown event")
        sm.shutdown_underway.wait()
        if mqtt_client is not None:
            if mqtt_client.is_connected():
                logger.info(f"Disconnecting {mqtt_client}")
                mqtt_client.disconnect()
            logger.info(f"Stopping {mqtt_client}")
            mqtt_client.loop_stop()

        async def _the_rest():
            if bluedot is not None and bluedot.is_connected:
                logger.info(f"Disconnecting {bluedot.logstr}")
                await bluedot.disconnect()
            logger.info("Setting cleanup_complete")
            sm.cleanup_complete.set()

        asyncio.run_coroutine_threadsafe(_the_rest(), loop)

    loop.run_in_executor(None, on_shutdown_underway_cleanup)

    #
    # Run the controller
    #

    controller = SteamTempController(event_queue=event_queue,
                                     thermometer=bluedot)

    if de1_reports_awake():
        bluedot.capture()
    else:
        bluedot.release()

    #
    # MQTT client
    #
    # Don't start until after the controller is draining the queue
    #

    mqtt_client = configure_mqtt(queue=event_queue)
    loop.run_in_executor(None, run_mqtt_client_sync, mqtt_client)


def de1_reports_awake() -> bool:
    logger = pyDE1.getLogger('Requests')
    logger.info("Querying DE1 state")
    resp = requests.get(
        url=f"{config.http.PYDE1_BASE_URL}/{Resource.DE1_STATE.value}")
    if not resp.ok:
        logger.error(f"{resp.status_code} {resp.reason} {resp.content}")
        retval = None
    else:
        logger.info(f"{resp.status_code} {resp.reason}")
        response_dict = resp.json()
        retval = response_dict['state']['state'] \
                 != API_MachineStates.Sleep.name
    logger.info(f"de1_reports_awake: {retval}")
    return retval


if __name__ == '__main__':
    import argparse

    ap = argparse.ArgumentParser(
        description="""Service to provide steam-to-temperature functionality

        Using a running instance of pyDE1, utilizes a connected thermometer
        to estimate time at which the target temperature will be reached
        and shut off steaming appropriately in advance.      
        """
        f"Default configuration file is at {config.DEFAULT_CONFIG_FILE}"
    )
    ap.add_argument('-c', type=str, help='Use as alternate config file')

    args = ap.parse_args()

    pyde1_logging.setup_initial_logger()

    config.load_from_yaml(args.c)

    pyde1_logging.setup_direct_logging(config.logging)
    pyde1_logging.config_logger_levels(config.logging)

    logger = pyDE1.getLogger('Main')

    # Set up the loop and shutdown_manager

    loop = asyncio.get_event_loop()
    loop.set_debug(True)

    loop.set_exception_handler(sm.exception_handler)
    sm.attach_signal_handler_to_loop(sm.shutdown, loop)

    loop.create_task(run())
    loop.run_forever()
    exit(sm.exit_value)
