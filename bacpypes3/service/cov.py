"""
Change Of Value Services
"""
from __future__ import annotations

import asyncio
from functools import partial

from typing import (
    Any as _Any,
    Callable,
    Optional,
    Tuple,
)

from ..settings import settings
from ..debugging import bacpypes_debugging, DebugContents, ModuleLogger

from ..pdu import Address

from ..errors import (
    DecodingError,
    ExecutionError,
    ServicesError,
)
from ..primitivedata import Unsigned, ObjectIdentifier
from ..constructeddata import Array
from ..basetypes import (
    PropertyIdentifier,
    PropertyReference,
    PropertyValue,
    StatusFlags,
)
from ..apdu import (
    SimpleAckPDU,
    ErrorRejectAbortNack,
    SubscribeCOVPropertyRequest,
    SubscribeCOVRequest,
    ConfirmedCOVNotificationRequest,
    UnconfirmedCOVNotificationRequest,
)
from ..vendor import get_vendor_info
from ..local.cov import PropertyCOVCriteria

# some debugging
_debug = 0
_log = ModuleLogger(globals())


@bacpypes_debugging
class SubscriptionContextManager:
    _debug: Callable[..., None]

    app: "Application"  # noqa: F821
    address: Address
    monitored_object_identifier: ObjectIdentifier
    subscriber_process_identifier: int
    issue_confirmed_notifications: bool
    lifetime: int
    monitored_property_identifier: Optional[PropertyIdentifier]
    property_array_index: Optional[int]
    cov_increment: Optional[float]

    def __init__(
        self,
        app: "Application",  # noqa: F821
        address: Address,
        monitored_object_identifier: ObjectIdentifier,
        subscriber_process_identifier: int,
        issue_confirmed_notifications: bool,
        lifetime: int,
        monitored_property_identifier: Optional[PropertyIdentifier] = None,
        property_array_index: Optional[int] = None,
        cov_increment: Optional[float] = None,
    ):
        if _debug:
            SubscriptionContextManager._debug("__init__ ...")

        # reference to the application for sending requests
        self.app = app

        # keep a copy of the request parameters
        self.address = address
        self.monitored_object_identifier = monitored_object_identifier
        self.subscriber_process_identifier = subscriber_process_identifier
        self.issue_confirmed_notifications = issue_confirmed_notifications
        self.lifetime = lifetime
        self.monitored_property_identifier = monitored_property_identifier
        self.property_array_index = property_array_index
        self.cov_increment = cov_increment

        # queue of PropertyValue returned
        self.queue = asyncio.Queue()

        # timer handle to refresh the subscription
        self.refresh_subscription_handle = None

        # result of refresh task to check if exception occurred
        self.refresh_subscription_task = None

    async def __aenter__(self) -> SubscriptionContextManager:
        if _debug:
            SubscriptionContextManager._debug("__aenter__")

        # kick off the subscription
        await self.refresh_subscription()

        # save this in the app
        scm_key = self.key
        if scm_key in self.app._cov_contexts:
            raise ValueError("existing context")
        self.app._cov_contexts[scm_key] = self

        return self

    @property
    def key(self) -> Tuple[Address, int, Optional[str], Optional[int]]:
        """
        The identity of a subscription context: the peer address, the
        subscriber process identifier, the monitored property (or None for
        the whole object COV property group), and the optional array index.
        """
        property_attr = (
            self.monitored_property_identifier.attr
            if self.monitored_property_identifier is not None
            else None
        )
        return (
            self.address,
            self.subscriber_process_identifier,
            property_attr,
            self.property_array_index,
        )

    async def __aexit__(self, *exc_details):
        """
        Exiting the context, cancel the subscription.
        """
        if _debug:
            SubscriptionContextManager._debug("__aexit__ %r", exc_details)

        # cancel the refresh
        if self.refresh_subscription_handle:
            if _debug:
                SubscriptionContextManager._debug("    - cancel refresh")
            self.refresh_subscription_handle.cancel()

        # delete the reference to this context
        del self.app._cov_contexts[self.key]

        if exc_details != (None, None, None):
            if _debug:
                SubscriptionContextManager._debug("    - abandon context")
            return

        # create a request to cancel the subscription
        unsubscribe_cov_request = self._make_subscription_request()

        # send the request, wait for the response
        response = await self.app.request(unsubscribe_cov_request)
        if _debug:
            SubscriptionContextManager._debug("    - response: %r", response)
        if isinstance(response, ErrorRejectAbortNack):
            if _debug:
                SubscriptionContextManager._debug(
                    "    - error/reject/abort: %r", response
                )

    async def refresh_subscription(self):
        """
        Send a subscription request.
        """
        if _debug:
            SubscriptionContextManager._debug("refresh_subscription")

        # create a request
        subscribe_cov_request = self._make_subscription_request(
            issueConfirmedNotifications=self.issue_confirmed_notifications,
            lifetime=self.lifetime,
        )
        if _debug:
            SubscriptionContextManager._debug(
                "    - subscribe_cov_request: %r", subscribe_cov_request
            )

        # send the request, wait for the response
        response = await self.app.request(subscribe_cov_request)
        if _debug:
            SubscriptionContextManager._debug("    - response: %r", response)
        if isinstance(response, ErrorRejectAbortNack):
            if _debug:
                SubscriptionContextManager._debug(
                    "    - error/reject/abort: %r", response
                )
            raise response

        # check for infinite lifetime
        if self.lifetime == 0:
            if _debug:
                SubscriptionContextManager._debug("    - infinite lifetime")
            return

        # get the loop to schedule a time to refresh
        loop = asyncio.get_event_loop()
        if _debug:
            SubscriptionContextManager._debug("    - loop time: %r", loop.time())

        # refresh time before it expires
        self.refresh_subscription_handle = loop.call_later(
            max(1.0, self.lifetime - 2.0), self.create_refresh_task
        )
        if _debug:
            SubscriptionContextManager._debug(
                "    - refresh_subscription_handle: %r",
                self.refresh_subscription_handle,
            )

        return

    def _make_subscription_request(self, **kwargs):
        """
        Build a SubscribeCOVRequest for the default COV property group or a
        SubscribeCOVPropertyRequest for a specific property and optional
        array index.
        """
        if _debug:
            SubscriptionContextManager._debug("_make_subscription_request %r", kwargs)

        common_kwargs = dict(
            subscriberProcessIdentifier=self.subscriber_process_identifier,
            monitoredObjectIdentifier=self.monitored_object_identifier,
            destination=self.address,
        )
        common_kwargs.update(kwargs)

        if self.monitored_property_identifier is None:
            return SubscribeCOVRequest(**common_kwargs)

        common_kwargs[
            "monitoredPropertyIdentifier"
        ] = PropertyReference(
            propertyIdentifier=self.monitored_property_identifier,
            propertyArrayIndex=self.property_array_index,
        )
        if self.cov_increment is not None:
            common_kwargs["covIncrement"] = self.cov_increment

        return SubscribeCOVPropertyRequest(**common_kwargs)

    def create_refresh_task(self):
        """
        Create a refresh task.  The `loop.call_later()` function does
        not take a coroutine so this function creates a task wrapping
        the `refresh_subscription()` coroutine.
        """
        if _debug:
            SubscriptionContextManager._debug("create_refresh_task")

        self.refresh_subscription_task = asyncio.create_task(
            self.refresh_subscription()
        )

    async def put(self, property_value: PropertyValue) -> None:
        """
        Add a property value to that has been received from a notification
        to the queue."""
        if _debug:
            SubscriptionContextManager._debug("put %r", property_value)

        await self.queue.put(property_value)

    async def get(self) -> PropertyValue:
        """
        Get the next property value from the queue or wait until one
        is available.  See `get_value()` for a specialized version.
        """
        if _debug:
            SubscriptionContextManager._debug("get")

        return await self.queue.get()

    async def get_value(self) -> Tuple[PropertyIdentifier, _Any]:
        """
        Get the next property value from the queue and interpret the
        propertyValue element to simplify the result.  Note that this
        will drop the property-index and priority elements after the
        interpretation.
        """
        if _debug:
            SubscriptionContextManager._debug("get_value")

        # get the next thing in the queue
        property_value_element = await self.get()

        # get information about the device from the application cache
        device_info = await self.app.device_info_cache.get_device_info(self.address)
        if _debug:
            SubscriptionContextManager._debug("    - device_info: %r", device_info)

        # using the device info, look up the vendor information
        if device_info:
            vendor_info = get_vendor_info(device_info.vendor_identifier)
        else:
            vendor_info = get_vendor_info(0)
        if _debug:
            SubscriptionContextManager._debug(
                "    - vendor_info (%d): %r", vendor_info.vendor_identifier, vendor_info
            )

        # using the vendor information, look up the class
        object_class = vendor_info.get_object_class(self.monitored_object_identifier[0])
        if not object_class:
            return (
                property_value_element.propertyIdentifier,
                DecodingError(
                    f"no object class: {self.monitored_object_identifier[0]}"
                ),
            )

        # now get the property type from the class
        property_type = object_class.get_property_type(
            property_value_element.propertyIdentifier
        )
        if _debug:
            SubscriptionContextManager._debug("    - property_type: %r", property_type)
        if not property_type:
            return (
                property_value_element.propertyIdentifier,
                DecodingError(
                    f"no property type: {property_value_element.propertyIdentifier}"
                ),
            )

        # filter the array index reference
        if issubclass(property_type, Array):
            if property_value_element.propertyArrayIndex is None:
                pass
            elif property_value_element.propertyArrayIndex == 0:
                property_type = Unsigned
            else:
                property_type = property_type._subtype
            if _debug:
                SubscriptionContextManager._debug(
                    "    - other property_type: %r", property_type
                )

        # cast it out of the Any
        property_value = property_value_element.value.cast_out(property_type)
        if _debug:
            SubscriptionContextManager._debug(
                "    - property_value: %r %r", property_value, property_type.__class__
            )

        return (property_value_element.propertyIdentifier, property_value)


#
#   Subscription
#


@bacpypes_debugging
class Subscription(DebugContents):
    _debug_contents = (
        "obj_ref",
        "client_addr",
        "proc_id",
        "obj_id",
        "prop_id",
        "array_index",
        "confirmed",
        "lifetime",
        "covIncrement",
    )

    cancel_handle: Optional[asyncio.TimerHandle]

    def __init__(
        self,
        obj_ref,
        client_addr,
        proc_id,
        obj_id,
        confirmed,
        lifetime,
        cov_inc,
        prop_id=None,
        array_index=None,
    ):
        if _debug:
            Subscription._debug(
                "__init__ %r %r %r %r %r %r %r",
                obj_ref,
                client_addr,
                proc_id,
                obj_id,
                confirmed,
                lifetime,
                cov_inc,
            )
        # save the reference to the related object
        self.obj_ref = obj_ref

        # save the parameters
        self.client_addr = client_addr
        self.proc_id = proc_id
        self.obj_id = obj_id
        self.prop_id = prop_id
        self.array_index = array_index
        self.confirmed = confirmed
        self.lifetime = lifetime
        self.covIncrement = cov_inc

        # if lifetime is zero this is a permanent subscription
        if lifetime > 0:
            loop = asyncio.get_running_loop()
            self.cancel_handle = loop.call_later(
                lifetime, self.obj_ref._app.cancel_subscription, self
            )
        else:
            self.cancel_handle = None

    @property
    def detection_key(self) -> Tuple:
        """
        Map this subscription to its detection algorithm: the object
        identifier plus the monitored property and optional array index.
        """
        return (self.obj_id, self.prop_id, self.array_index)

    def cancel_subscription(self):
        if _debug:
            Subscription._debug("cancel_subscription")

        # if this is scheduled to call later, cancel it
        if self.cancel_handle:
            self.cancel_handle.cancel()
            self.cancel_handle = None

        # break the object reference
        self.obj_ref = None

    def renew_subscription(self, lifetime, cov_inc=None):
        if _debug:
            Subscription._debug("renew_subscription %r", lifetime)

        # if this is scheduled to call later, cancel it
        if self.cancel_handle:
            self.cancel_handle.cancel()
            self.cancel_handle = None

        # save the new lifetime
        self.lifetime = lifetime

        # save the new caller provided COV increment
        if cov_inc is not None:
            self.covIncrement = cov_inc

        # reschedule a cancel if it's not infinite
        if lifetime > 0:
            loop = asyncio.get_running_loop()
            self.cancel_handle = loop.call_later(
                lifetime, self.obj_ref._app.cancel_subscription, self
            )


#
#   Change Of Value
#


@bacpypes_debugging
class ChangeOfValueServices:
    def __init__(self):
        if _debug:
            ChangeOfValueServices._debug("__init__")

        self._cov_next_id = 1
        self._cov_contexts = {}
        self._cov_detections = {}

    # -----

    def change_of_value(
        self,
        address: Address,
        monitored_object_identifier: ObjectIdentifier,
        subscriber_process_identifier: Optional[int] = None,
        issue_confirmed_notifications: Optional[bool] = True,
        lifetime: Optional[int] = None,
        monitored_property_identifier: _Any = None,
        property_array_index: Optional[int] = None,
        cov_increment: Optional[float] = None,
    ) -> SubscriptionContextManager:
        """
        Create and return an async subscription context manager.
        """
        if _debug:
            ChangeOfValueServices._debug(
                "change_of_value %r %r %r %r %r %r %r %r",
                address,
                monitored_object_identifier,
                subscriber_process_identifier,
                issue_confirmed_notifications,
                lifetime,
                monitored_property_identifier,
                property_array_index,
                cov_increment,
            )

        # normalize the property reference to a PropertyIdentifier
        if monitored_property_identifier is not None:
            monitored_property_identifier = PropertyIdentifier(
                monitored_property_identifier
            )

        if subscriber_process_identifier is None:
            while True:
                subscriber_process_identifier = self._cov_next_id
                self._cov_next_id = (self._cov_next_id + 1) % (1 << 22)
                scm_key = (
                    address,
                    subscriber_process_identifier,
                    monitored_property_identifier.attr
                    if monitored_property_identifier is not None
                    else None,
                    property_array_index,
                )
                if scm_key not in self._cov_contexts:
                    break
            if _debug:
                ChangeOfValueServices._debug(
                    "    - subscriber_process_identifier: %r",
                    subscriber_process_identifier,
                )

        if lifetime is None:
            lifetime = settings.cov_lifetime
            if _debug:
                ChangeOfValueServices._debug("    - lifetime: %r", lifetime)

        scm = SubscriptionContextManager(
            self,
            address,
            monitored_object_identifier,
            subscriber_process_identifier,
            issue_confirmed_notifications,
            lifetime,
            monitored_property_identifier,
            property_array_index,
            cov_increment,
        )
        if _debug:
            ChangeOfValueServices._debug("    - scm: %r", scm)

        return scm

    async def do_ConfirmedCOVNotificationRequest(
        self, apdu: ConfirmedCOVNotificationRequest
    ) -> None:
        if _debug:
            ChangeOfValueServices._debug("do_ConfirmedCOVNotificationRequest %r", apdu)

        address = apdu.pduSource
        subscriber_process_identifier = apdu.subscriberProcessIdentifier

        # find the context
        scm = self._find_notification_context(apdu)
        if not scm:
            if _debug:
                ChangeOfValueServices._debug("    - scm not found")
            raise ServicesError(errorCode="unknownSubscription")

        # queue the property values
        for property_value in apdu.listOfValues:
            await scm.put(property_value)

        # success
        resp = SimpleAckPDU(context=apdu)
        if _debug:
            ChangeOfValueServices._debug("    - resp: %r", resp)

        # return the result
        await self.response(resp)

    async def do_UnconfirmedCOVNotificationRequest(
        self, apdu: UnconfirmedCOVNotificationRequest
    ) -> None:
        if _debug:
            ChangeOfValueServices._debug(
                "do_UnconfirmedCOVNotificationRequest %r", apdu
            )

        address = apdu.pduSource
        subscriber_process_identifier = apdu.subscriberProcessIdentifier

        # find the context
        scm = self._find_notification_context(apdu)
        if not scm:
            if _debug:
                ChangeOfValueServices._debug("    - scm not found")
            return

        # queue the property values
        for property_value in apdu.listOfValues:
            await scm.put(property_value)

    def _find_notification_context(self, apdu):
        """
        Find the subscription context matching a COV notification.  A
        property-specific subscription context matches on the property and
        optional array index; a whole-object subscription context matches
        every notification for the object.
        """
        if _debug:
            ChangeOfValueServices._debug("_find_notification_context %r", apdu)

        base_key = (apdu.pduSource, apdu.subscriberProcessIdentifier)

        # check for property-specific subscription contexts matching the
        # values in the notification
        for property_value in apdu.listOfValues:
            property_attr = property_value.propertyIdentifier.attr
            array_index = property_value.propertyArrayIndex
            scm = self._cov_contexts.get(
                base_key + (property_attr, array_index), None
            )
            if scm and (
                apdu.monitoredObjectIdentifier == scm.monitored_object_identifier
            ):
                if _debug:
                    ChangeOfValueServices._debug("    - property scm: %r", scm)
                return scm

        # fall back to a whole-object subscription context
        scm = self._cov_contexts.get(base_key + (None, None), None)
        if scm and (
            apdu.monitoredObjectIdentifier == scm.monitored_object_identifier
        ):
            if _debug:
                ChangeOfValueServices._debug("    - object scm: %r", scm)
            return scm

        return None

    # -----

    def add_subscription(self, cov):
        if _debug:
            ChangeOfValueServices._debug("add_subscription %r", cov)

        # let the detection algorithm know this is a new or additional subscription
        self._cov_detections[cov.detection_key].add_subscription(cov)

    def cancel_subscription(self, cov):
        if _debug:
            ChangeOfValueServices._debug("cancel_subscription %r", cov)

        # stash the detection key and object reference
        detection_key = cov.detection_key
        obj_ref = cov.obj_ref

        # get the detection algorithm object
        cov_detection = self._cov_detections[detection_key]

        # let the detection algorithm know this subscription is going away
        cov_detection.cancel_subscription(cov)

        # if the detection algorithm doesn't have any subscriptions, remove it
        if not len(cov_detection.cov_subscriptions):
            if _debug:
                ChangeOfValueServices._debug("    - no more subscriptions")

            # unbind all the hooks into the object
            cov_detection.unbind()

            # delete it from the object map
            del self._cov_detections[detection_key]

        # the object may have already been deleted
        if obj_ref is None:
            return

    def cancel_object_subscriptions(self, obj_ref):
        """
        Cancel all of the subscriptions, both the default COV property group
        and property-specific ones, associated with an object.
        """
        if _debug:
            ChangeOfValueServices._debug("cancel_object_subscriptions %r", obj_ref)

        object_identifier = obj_ref.objectIdentifier
        detection_keys = [
            key
            for key in self._cov_detections
            if key[0] == object_identifier
        ]
        for detection_key in detection_keys:
            cov_detection = self._cov_detections[detection_key]
            if _debug:
                ChangeOfValueServices._debug(
                    "    - cov_detection: %r", cov_detection
                )

            # cancel each subscription using a copy of the list
            for cov in list(cov_detection.cov_subscriptions):
                self.cancel_subscription(cov)

    # -----

    def cov_notification(self, cov, apdu):
        """
        Schedule a task to send out a notification related to a COV subscription.
        """
        if _debug:
            ChangeOfValueServices._debug("cov_notification %r %r", cov, apdu)

        # if this is a confirmed service, this function call will return an
        # APDUFuture and the callback will get the error/reject/abort that
        # comes back, otherwise this is None.
        confirmed_service = self.request(apdu)
        if confirmed_service:
            confirmed_service.add_done_callback(partial(self.cov_confirmation, cov))

    def cov_confirmation(self, cov, future) -> None:
        """
        Callback function for sending out notifications.
        """
        if _debug:
            ChangeOfValueServices._debug("cov_confirmation %r %r", cov, future)

        apdu = future.result()
        if _debug:
            ChangeOfValueServices._debug("    - apdu: %r", apdu)
        if not apdu:
            ChangeOfValueServices._debug("    - simple ack")
            return

    # -----

    async def do_SubscribeCOVRequest(self, apdu):
        """
        Subscribe to the default COV property group of an object.
        """
        if _debug:
            ChangeOfValueServices._debug("do_SubscribeCOVRequest %r", apdu)

        await self._do_subscribe_cov(apdu)

    async def do_SubscribeCOVPropertyRequest(self, apdu):
        """
        Subscribe to a specific property of an object, potentially an array
        element, with an optional caller provided COV increment.
        """
        if _debug:
            ChangeOfValueServices._debug(
                "do_SubscribeCOVPropertyRequest %r", apdu
            )

        await self._do_subscribe_cov(apdu)

    async def _do_subscribe_cov(self, apdu):
        if _debug:
            ChangeOfValueServices._debug("_do_subscribe_cov %r", apdu)

        # extract the pieces
        client_addr = apdu.pduSource
        proc_id = apdu.subscriberProcessIdentifier
        obj_id = apdu.monitoredObjectIdentifier
        confirmed = apdu.issueConfirmedNotifications
        lifetime = apdu.lifetime

        # property-specific subscription parameters
        property_subscription = hasattr(apdu, "monitoredPropertyIdentifier")
        if property_subscription:
            property_reference = apdu.monitoredPropertyIdentifier
            prop_id = property_reference.propertyIdentifier
            array_index = property_reference.propertyArrayIndex
            cov_inc = apdu.covIncrement
            if _debug:
                ChangeOfValueServices._debug(
                    "    - prop_id, array_index, cov_inc: %r %r %r",
                    prop_id,
                    array_index,
                    cov_inc,
                )
        else:
            prop_id = None
            array_index = None
            cov_inc = None

        # request is to cancel the subscription
        cancel_subscription = (confirmed is None) and (lifetime is None)

        # find the object
        obj = self.get_object_id(obj_id)
        if _debug:
            ChangeOfValueServices._debug("    - object: %r", obj)
        if not obj:
            if cancel_subscription:
                # always return a Result(+), the subscription may have been
                # previously successful and the object deleted, then this
                # cancelation would have been successful
                await self.response(SimpleAckPDU(context=apdu))
                return
            else:
                raise ExecutionError(errorClass="object", errorCode="unknownObject")

        if property_subscription:
            # make sure the property exists
            property_type = obj.get_property_type(prop_id)
            if _debug:
                ChangeOfValueServices._debug(
                    "    - property_type: %r", property_type
                )
            if not property_type:
                raise ExecutionError(
                    errorClass="services", errorCode="covSubscriptionFailed"
                )

            # normalize the property identifier to its attribute name
            prop_attr = prop_id.attr

            if array_index is not None:
                if not issubclass(property_type, Array):
                    raise ExecutionError(
                        errorClass="property", errorCode="propertyIsNotAnArray"
                    )
                if array_index == 0:
                    raise ExecutionError(
                        errorClass="property", errorCode="invalidArrayIndex"
                    )

                current_value = getattr(obj, prop_attr, None)
                if (current_value is None) or (array_index > len(current_value)):
                    raise ExecutionError(
                        errorClass="property", errorCode="invalidArrayIndex"
                    )
        else:
            prop_attr = None

        # the key for finding the detection algorithm
        detection_map_key = (obj_id, prop_attr, array_index)

        # look for an algorithm already associated with this property
        cov_detection = self._cov_detections.get(detection_map_key, None)

        # if there isn't one, make one and associate it with the object
        if not cov_detection:
            if property_subscription:
                cov_detection = PropertyCOVCriteria(obj, prop_attr, array_index)
            else:
                # look for an associated class and if it's not there it's not supported
                criteria_class = getattr(obj, "_cov_criteria", None)
                if _debug:
                    ChangeOfValueServices._debug(
                        "    - criteria_class: %r", criteria_class
                    )

                if not criteria_class:
                    raise ExecutionError(
                        errorClass="services", errorCode="covSubscriptionFailed"
                    )

                # make one of these and bind it to the object
                cov_detection = criteria_class(obj)

            # keep track of it for other subscriptions
            self._cov_detections[detection_map_key] = cov_detection
        if _debug:
            ChangeOfValueServices._debug("    - cov_detection: %r", cov_detection)

        # can a match be found?
        for cov in cov_detection.cov_subscriptions:
            all_equal = (
                (cov.client_addr == client_addr)
                and (cov.proc_id == proc_id)
                and (cov.obj_id == obj_id)
                and (cov.prop_id == prop_attr)
                and (cov.array_index == array_index)
            )
            if _debug:
                ChangeOfValueServices._debug(
                    "    - cov, all_equal: %r %r", cov, all_equal
                )

            if all_equal:
                break
        else:
            cov = None
        if _debug:
            ChangeOfValueServices._debug("    - cov: %r", cov)

        # if a match was found, update the subscription
        if cov:
            if cancel_subscription:
                if _debug:
                    ChangeOfValueServices._debug("    - cancel the subscription")
                self.cancel_subscription(cov)
            else:
                if _debug:
                    ChangeOfValueServices._debug("    - renew the subscription")
                cov.renew_subscription(lifetime, cov_inc)
        else:
            if cancel_subscription:
                if _debug:
                    ChangeOfValueServices._debug(
                        "    - cancel a subscription that doesn't exist"
                    )
            else:
                if _debug:
                    ChangeOfValueServices._debug("    - create a subscription")

                # make a subscription
                cov = Subscription(
                    obj,
                    client_addr,
                    proc_id,
                    obj_id,
                    confirmed,
                    lifetime,
                    cov_inc,
                    prop_attr,
                    array_index,
                )
                if _debug:
                    ChangeOfValueServices._debug("    - cov: %r", cov)

                # add it to our subscriptions lists
                self.add_subscription(cov)

        # success
        response = SimpleAckPDU(context=apdu)

        # return the result
        await self.response(response)

        # if the subscription is not being canceled, it is new or renewed,
        # so send it a notification when you get a chance.
        if not cancel_subscription:
            if _debug:
                ChangeOfValueServices._debug("    - send a notification")
            loop = asyncio.get_running_loop()
            loop.call_soon(cov_detection.send_cov_notifications, cov)
