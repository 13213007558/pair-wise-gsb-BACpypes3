#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test COMMAND_FAILURE Event Algorithm
------------------------------------

Test the Clause 13.3.4 command failure event algorithm with a controllable
clock: brief deviations are not alarms, sustained deviations transition to
offnormal after the time delay, recovery uses the normal time delay, and
stale timers never fire after configuration changes, out-of-service changes,
or object deletion.
"""

import pytest

from bacpypes3.debugging import bacpypes_debugging, ModuleLogger

from bacpypes3.app import Application
from bacpypes3.primitivedata import Real
from bacpypes3.basetypes import (
    BinaryPV,
    DeviceObjectPropertyReference,
    EngineeringUnits,
    EventParameter,
    EventParameterCommandFailure,
    EventState,
    EventType,
    NotifyType,
    Polarity,
    PropertyIdentifier,
    Reliability,
    TimeStamp,
)
from bacpypes3.object import NotificationClassObject
from bacpypes3.local.device import DeviceObject
from bacpypes3.local.analog import AnalogValueObject
from bacpypes3.local.binary import BinaryOutputObjectIR
from bacpypes3.local.event import EventEnrollmentObject

from .clocked_test import ClockedTest, clocked_test  # noqa: F401

# some debugging
_debug = 0
_log = ModuleLogger(globals())


def make_device_object() -> DeviceObject:
    return DeviceObject(
        objectIdentifier="device,999",
        objectName="test-device",
    )


def make_notification_class() -> NotificationClassObject:
    return NotificationClassObject(
        objectIdentifier="notification-class,1",
        objectName="nc1",
        notificationClass=1,
        priority=[9, 9, 9],  # toOffNormal, toFault, toNormal
        ackRequired=[0, 0, 0],
        recipientList=[],
    )


def make_binary_output() -> BinaryOutputObjectIR:
    """
    A binary output object with intrinsic command failure reporting,
    time delay 10 seconds, normal time delay 5 seconds.
    """
    return BinaryOutputObjectIR(
        objectIdentifier="binary-output,1",
        objectName="bo1",
        presentValue=BinaryPV.inactive,
        eventState=EventState.normal,
        outOfService=False,
        polarity=Polarity.normal,
        relinquishDefault=BinaryPV.inactive,
        timeDelay=10,
        timeDelayNormal=5,
        notificationClass=1,
        feedbackValue=BinaryPV.inactive,
        eventEnable=[1, 1, 1],
        ackedTransitions=[0, 0, 0],
        notifyType=NotifyType.alarm,
        eventTimeStamps=[TimeStamp(time=(255, 255, 255, 255)) for _ in range(3)],
        eventDetectionEnable=True,
    )


def make_analog_value(instance: int, present_value: float = 0.0) -> AnalogValueObject:
    return AnalogValueObject(
        objectIdentifier=f"analog-value,{instance}",
        objectName=f"av{instance}",
        presentValue=present_value,
        eventState=EventState.normal,
        outOfService=False,
        reliability=Reliability.noFaultDetected,
        units=EngineeringUnits.degreesFahrenheit,
    )


def make_event_enrollment(feedback_instance: int, time_delay: int = 10):
    return EventEnrollmentObject(
        objectIdentifier="event-enrollment,1",
        objectName="ee1",
        eventType=EventType.commandFailure,
        notifyType=NotifyType.alarm,
        eventParameters=EventParameter(
            commandFailure=EventParameterCommandFailure(
                timeDelay=time_delay,
                feedbackPropertyReference=DeviceObjectPropertyReference(
                    objectIdentifier=f"analog-value,{feedback_instance}",
                    propertyIdentifier=PropertyIdentifier.presentValue,
                ),
            ),
        ),
        objectPropertyReference=DeviceObjectPropertyReference(
            objectIdentifier="analog-value,1",
            propertyIdentifier=PropertyIdentifier.presentValue,
        ),
        eventState=EventState.normal,
        eventEnable=[1, 1, 1],
        ackedTransitions=[0, 0, 0],
        notificationClass=1,
        eventTimeStamps=[TimeStamp(time=(255, 255, 255, 255)) for _ in range(3)],
        eventDetectionEnable=True,
        reliability=Reliability.noFaultDetected,
    )


def trap_notifications(algorithm) -> list:
    """
    Replace the send_notifications() coroutine of an event algorithm with
    one that simply records the notification parameters.
    """
    notifications = []

    async def send_notifications(notification_parameters):
        notifications.append(notification_parameters)

    algorithm.send_notifications = send_notifications
    return notifications


@bacpypes_debugging
class TestIntrinsicCommandFailure:
    """
    Intrinsic reporting with a Binary Output Object, Clause 13.3.4.
    """

    @pytest.mark.asyncio
    async def test_brief_deviation(self, clocked_test: ClockedTest):
        """
        A feedback value that lags the commanded value for less than the
        time delay is not a fault; the pending transition is revoked when
        the values match again.
        """
        if _debug:
            TestIntrinsicCommandFailure._debug("test_brief_deviation")

        bvo = make_binary_output()
        app = Application.from_object_list(
            [make_device_object(), make_notification_class(), bvo]
        )
        notifications = trap_notifications(bvo._event_algorithm)
        await clocked_test(0)

        # command active, feedback still inactive
        bvo.presentValue = BinaryPV.active
        await clocked_test(5)
        assert bvo.eventState == EventState.normal

        # feedback catches up before the time delay expires
        bvo.feedbackValue = BinaryPV.active
        await clocked_test(10)

        # well past the original deadline, nothing happened
        assert bvo.eventState == EventState.normal
        assert notifications == []

        app.close()

    @pytest.mark.asyncio
    async def test_sustained_deviation(self, clocked_test: ClockedTest):
        """
        A sustained mismatch transitions to offnormal after the time delay,
        recovery transitions back to normal after the normal time delay.
        """
        if _debug:
            TestIntrinsicCommandFailure._debug("test_sustained_deviation")

        bvo = make_binary_output()
        app = Application.from_object_list(
            [make_device_object(), make_notification_class(), bvo]
        )
        notifications = trap_notifications(bvo._event_algorithm)
        await clocked_test(0)

        # command active, feedback stays inactive
        bvo.presentValue = BinaryPV.active
        await clocked_test(9)
        assert bvo.eventState == EventState.normal

        # time delay expires
        await clocked_test(1)
        assert bvo.eventState == EventState.offnormal

        # notification carries the command and feedback values at the time
        # the transition was triggered
        assert len(notifications) == 1
        notification = notifications[0]
        assert notification["fromState"] == EventState.normal
        assert notification["toState"] == EventState.offnormal
        assert notification["eventType"] == EventType.commandFailure
        command_failure = notification["eventValues"].commandFailure
        assert command_failure.commandValue.cast_out(BinaryPV) == BinaryPV.active
        assert command_failure.feedbackValue.cast_out(BinaryPV) == BinaryPV.inactive

        # feedback catches up, normal time delay is 5 seconds
        bvo.feedbackValue = BinaryPV.active
        await clocked_test(4)
        assert bvo.eventState == EventState.offnormal
        await clocked_test(1)
        assert bvo.eventState == EventState.normal

        assert len(notifications) == 2
        notification = notifications[1]
        assert notification["fromState"] == EventState.offnormal
        assert notification["toState"] == EventState.normal
        command_failure = notification["eventValues"].commandFailure
        assert command_failure.commandValue.cast_out(BinaryPV) == BinaryPV.active
        assert command_failure.feedbackValue.cast_out(BinaryPV) == BinaryPV.active

        app.close()

    @pytest.mark.asyncio
    async def test_deviate_during_recovery(self, clocked_test: ClockedTest):
        """
        Deviating again while a transition back to normal is pending revokes
        the pending transition; the object stays offnormal.
        """
        if _debug:
            TestIntrinsicCommandFailure._debug("test_deviate_during_recovery")

        bvo = make_binary_output()
        app = Application.from_object_list(
            [make_device_object(), make_notification_class(), bvo]
        )
        notifications = trap_notifications(bvo._event_algorithm)
        await clocked_test(0)

        # get into the offnormal state
        bvo.presentValue = BinaryPV.active
        await clocked_test(10)
        assert bvo.eventState == EventState.offnormal
        assert len(notifications) == 1

        # feedback catches up, transition to normal pending
        bvo.feedbackValue = BinaryPV.active
        await clocked_test(3)
        assert bvo.eventState == EventState.offnormal

        # deviate again before the normal time delay expires
        bvo.feedbackValue = BinaryPV.inactive
        await clocked_test(10)

        # the pending transition was revoked, still offnormal
        assert bvo.eventState == EventState.offnormal
        assert len(notifications) == 1

        # recover again, this time long enough
        bvo.feedbackValue = BinaryPV.active
        await clocked_test(5)
        assert bvo.eventState == EventState.normal
        assert len(notifications) == 2

        app.close()

    @pytest.mark.asyncio
    async def test_out_of_service_cancels_timer(self, clocked_test: ClockedTest):
        """
        Changing out-of-service cancels a pending transition; the old timer
        must not fire an alarm.
        """
        if _debug:
            TestIntrinsicCommandFailure._debug("test_out_of_service_cancels_timer")

        bvo = make_binary_output()
        app = Application.from_object_list(
            [make_device_object(), make_notification_class(), bvo]
        )
        notifications = trap_notifications(bvo._event_algorithm)
        await clocked_test(0)

        # mismatch starts a 10 second timer
        bvo.presentValue = BinaryPV.active
        await clocked_test(8)

        # out-of-service change cancels the old timer, the mismatch is
        # re-evaluated and a fresh timer starts
        bvo.outOfService = True
        await clocked_test(2)

        # the original timer would have fired by now
        assert bvo.eventState == EventState.normal
        assert notifications == []

        # the fresh timer fires 10 seconds after the re-evaluation
        await clocked_test(8)
        assert bvo.eventState == EventState.offnormal
        assert len(notifications) == 1

        app.close()

    @pytest.mark.asyncio
    async def test_delete_object_cancels_timer(self, clocked_test: ClockedTest):
        """
        Deleting the object cancels a pending transition; the old timer
        must not fire an alarm.
        """
        if _debug:
            TestIntrinsicCommandFailure._debug("test_delete_object_cancels_timer")

        bvo = make_binary_output()
        app = Application.from_object_list(
            [make_device_object(), make_notification_class(), bvo]
        )
        notifications = trap_notifications(bvo._event_algorithm)
        await clocked_test(0)

        # mismatch starts a 10 second timer
        bvo.presentValue = BinaryPV.active
        await clocked_test(5)

        # delete the object before the timer expires
        app.delete_object(bvo)
        await clocked_test(10)

        assert notifications == []

        app.close()


@bacpypes_debugging
class TestAlgorithmicCommandFailure:
    """
    Algorithmic reporting with an Event Enrollment Object, Clause 13.3.4.
    """

    @pytest.mark.asyncio
    async def test_switch_feedback_reference(self, clocked_test: ClockedTest):
        """
        Switching the feedback reference to an object whose value matches
        the commanded value just before the delay expires cancels the
        pending transition; the old timer must not fire.
        """
        if _debug:
            TestAlgorithmicCommandFailure._debug("test_switch_feedback_reference")

        command = make_analog_value(1, 0.0)
        feedback_a = make_analog_value(2, 0.0)
        feedback_b = make_analog_value(3, 1.0)
        ee = make_event_enrollment(feedback_instance=2, time_delay=10)
        app = Application.from_object_list(
            [
                make_device_object(),
                make_notification_class(),
                command,
                feedback_a,
                feedback_b,
                ee,
            ]
        )
        await clocked_test(0)
        notifications = trap_notifications(ee._event_algorithm)

        # command 1.0, feedback-a 0.0: mismatch, timer expires at t=10
        command.presentValue = Real(1.0)
        await clocked_test(9)
        assert ee.eventState == EventState.normal

        # switch the feedback reference to feedback-b which matches
        ee.eventParameters = EventParameter(
            commandFailure=EventParameterCommandFailure(
                timeDelay=10,
                feedbackPropertyReference=DeviceObjectPropertyReference(
                    objectIdentifier="analog-value,3",
                    propertyIdentifier=PropertyIdentifier.presentValue,
                ),
            ),
        )

        # the old timer must not fire
        await clocked_test(5)
        assert ee.eventState == EventState.normal
        assert notifications == []

        app.close()

    @pytest.mark.asyncio
    async def test_switch_feedback_reference_still_mismatched(
        self, clocked_test: ClockedTest
    ):
        """
        Switching the feedback reference to an object that still does not
        match restarts the full time delay; the old timer must not fire.
        """
        if _debug:
            TestAlgorithmicCommandFailure._debug(
                "test_switch_feedback_reference_still_mismatched"
            )

        command = make_analog_value(1, 0.0)
        feedback_a = make_analog_value(2, 0.0)
        feedback_b = make_analog_value(3, 5.0)
        ee = make_event_enrollment(feedback_instance=2, time_delay=10)
        app = Application.from_object_list(
            [
                make_device_object(),
                make_notification_class(),
                command,
                feedback_a,
                feedback_b,
                ee,
            ]
        )
        await clocked_test(0)
        notifications = trap_notifications(ee._event_algorithm)

        # command 1.0, feedback-a 0.0: mismatch, timer expires at t=10
        command.presentValue = Real(1.0)
        await clocked_test(9)

        # switch the feedback reference to feedback-b, still mismatched
        ee.eventParameters = EventParameter(
            commandFailure=EventParameterCommandFailure(
                timeDelay=10,
                feedbackPropertyReference=DeviceObjectPropertyReference(
                    objectIdentifier="analog-value,3",
                    propertyIdentifier=PropertyIdentifier.presentValue,
                ),
            ),
        )

        # the old timer must not fire at t=10
        await clocked_test(5)
        assert ee.eventState == EventState.normal
        assert notifications == []

        # the fresh timer fires 10 seconds after the reference switch
        await clocked_test(5)
        assert ee.eventState == EventState.offnormal

        # notification carries the values at the time the transition fired
        assert len(notifications) == 1
        command_failure = notifications[0]["eventValues"].commandFailure
        assert command_failure.commandValue.cast_out(Real) == 1.0
        assert command_failure.feedbackValue.cast_out(Real) == 5.0

        app.close()

    @pytest.mark.asyncio
    async def test_invalid_feedback_reference(self, clocked_test: ClockedTest):
        """
        A feedback reference that cannot be resolved is reflected in the
        reliability and the object transitions to fault.
        """
        if _debug:
            TestAlgorithmicCommandFailure._debug("test_invalid_feedback_reference")

        command = make_analog_value(1, 0.0)
        ee = make_event_enrollment(feedback_instance=99, time_delay=10)
        app = Application.from_object_list(
            [make_device_object(), make_notification_class(), command, ee]
        )
        await clocked_test(0)
        notifications = trap_notifications(ee._event_algorithm)

        # the reference cannot be resolved
        assert ee.reliability == Reliability.configurationError

        # fault transition after the time delay
        await clocked_test(10)
        assert ee.eventState == EventState.fault

        app.close()

    @pytest.mark.asyncio
    async def test_delete_feedback_object(self, clocked_test: ClockedTest):
        """
        Deleting the object referenced for feedback values cancels the
        pending offnormal transition and the reference becomes invalid.
        """
        if _debug:
            TestAlgorithmicCommandFailure._debug("test_delete_feedback_object")

        command = make_analog_value(1, 0.0)
        feedback = make_analog_value(2, 0.0)
        ee = make_event_enrollment(feedback_instance=2, time_delay=10)
        app = Application.from_object_list(
            [make_device_object(), make_notification_class(), command, feedback, ee]
        )
        await clocked_test(0)
        notifications = trap_notifications(ee._event_algorithm)

        # mismatch, timer expires at t=10
        command.presentValue = Real(1.0)
        await clocked_test(5)

        # delete the feedback object
        app.delete_object(feedback)
        assert ee.reliability == Reliability.configurationError

        # the old offnormal timer must not fire
        await clocked_test(5)
        assert ee.eventState != EventState.offnormal
        assert not any(
            notification["toState"] == EventState.offnormal
            for notification in notifications
        )

        # the invalid reference transitions to fault instead
        await clocked_test(5)
        assert ee.eventState == EventState.fault

        app.close()
