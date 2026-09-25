#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test Command Failure Event Algorithm
------------------------------------

Clause 13.3.4 COMMAND_FAILURE: a short mismatch between the commanded
value and the feedback value is not a fault; only when the two disagree
continuously for Time_Delay does the object go offnormal (and back to
normal after Time_Delay_Normal once they agree again).

These tests use the ClockedTest fixture so that the event loop time is
advanced manually.
"""

from typing import Any, Dict, List

import pytest

from bacpypes3.app import Application
from bacpypes3.object import DeviceObject, NotificationClassObject
from bacpypes3.local.binary import (
    BinaryOutputObject,
    BinaryOutputObjectIR,
    BinaryValueObject,
)
from bacpypes3.local.event import EventEnrollmentObject
from bacpypes3.basetypes import (
    BinaryPV,
    DeviceObjectPropertyReference,
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

from ..clocked_test import ClockedTest


TIME_DELAY = 10
TIME_DELAY_NORMAL = 5


def _time_stamps() -> List[TimeStamp]:
    return [TimeStamp(time=(255, 255, 255, 255)) for _ in range(3)]


def _device_object() -> DeviceObject:
    return DeviceObject(
        objectIdentifier="device,500",
        objectName="test-device",
    )


def _notification_class_object() -> NotificationClassObject:
    return NotificationClassObject(
        objectIdentifier="notification-class,1",
        objectName="nc1",
        notificationClass=1,
        priority=[9, 9, 9],  # toOffNormal, toFault, toNormal
        ackRequired=[0, 0, 0],
        recipientList=[],
    )


def _binary_output_ir(**overrides: Any) -> BinaryOutputObjectIR:
    kwargs: Dict[str, Any] = dict(
        objectIdentifier="binary-output,1",
        objectName="bo1",
        presentValue=BinaryPV("inactive"),
        eventState=EventState.normal,
        reliability=Reliability("noFaultDetected"),
        outOfService=False,
        polarity=Polarity("normal"),
        timeDelay=TIME_DELAY,
        timeDelayNormal=TIME_DELAY_NORMAL,
        notificationClass=1,
        feedbackValue=BinaryPV("inactive"),
        eventEnable=[1, 1, 1],
        ackedTransitions=[0, 0, 0],
        notifyType=NotifyType("event"),
        eventTimeStamps=_time_stamps(),
        eventDetectionEnable=True,
    )
    kwargs.update(overrides)
    return BinaryOutputObjectIR(**kwargs)


def _feedback_event_parameters(
    feedback_object_identifier: str, time_delay: int = TIME_DELAY
) -> EventParameter:
    return EventParameter(
        commandFailure=EventParameterCommandFailure(
            timeDelay=time_delay,
            feedbackPropertyReference=DeviceObjectPropertyReference(
                objectIdentifier=feedback_object_identifier,
                propertyIdentifier=PropertyIdentifier.presentValue,
            ),
        )
    )


def _event_enrollment_setup(feedback_object_identifier: str = "binary-value,2"):
    """
    Build a commandable binary output monitored by an event enrollment
    object whose feedback reference points at a binary value object.
    """
    bo = BinaryOutputObject(
        objectIdentifier="binary-output,1",
        objectName="bo1",
        presentValue=BinaryPV("inactive"),
        eventState=EventState.normal,
        reliability=Reliability("noFaultDetected"),
        outOfService=False,
        polarity=Polarity("normal"),
    )
    bv2 = BinaryValueObject(
        objectIdentifier="binary-value,2",
        objectName="bv2",
        presentValue=BinaryPV("inactive"),
        eventState=EventState.normal,
        outOfService=False,
    )
    bv3 = BinaryValueObject(
        objectIdentifier="binary-value,3",
        objectName="bv3",
        presentValue=BinaryPV("active"),
        eventState=EventState.normal,
        outOfService=False,
    )
    ee = EventEnrollmentObject(
        objectIdentifier="event-enrollment,1",
        objectName="ee1",
        eventType=EventType.commandFailure,
        notifyType=NotifyType("event"),
        eventParameters=_feedback_event_parameters(feedback_object_identifier),
        objectPropertyReference=DeviceObjectPropertyReference(
            objectIdentifier="binary-output,1",
            propertyIdentifier=PropertyIdentifier.presentValue,
        ),
        eventState=EventState.normal,
        eventEnable=[1, 1, 1],
        ackedTransitions=[0, 0, 0],
        notificationClass=1,
        eventTimeStamps=_time_stamps(),
        eventDetectionEnable=True,
    )
    return bo, bv2, bv3, ee


def _make_app(*objects: Any) -> Application:
    return Application.from_object_list(
        [_device_object(), *objects, _notification_class_object()]
    )


class NotificationSpy:
    """
    Replace the send_notifications() method of an event algorithm and
    capture the notification parameters of every transition.
    """

    def __init__(self, algorithm) -> None:
        self.calls: List[Dict[str, Any]] = []
        algorithm.send_notifications = self.capture

    async def capture(self, notification_parameters: Dict[str, Any]) -> None:
        self.calls.append(notification_parameters)


@pytest.mark.asyncio
async def test_brief_deviation_no_alarm(clocked_test: ClockedTest) -> None:
    """
    A mismatch that is corrected before Time_Delay expires never
    transitions to offnormal.
    """
    bo = _binary_output_ir()
    _make_app(bo)
    await clocked_test(0)
    spy = NotificationSpy(bo._event_algorithm)

    assert bo.eventState == EventState.normal

    # command changes, feedback does not follow
    bo.presentValue = BinaryPV("active")
    await clocked_test(0)
    assert bo.eventState == EventState.normal

    # feedback catches up before the time delay expires
    await clocked_test(TIME_DELAY - 4)
    bo.feedbackValue = BinaryPV("active")
    await clocked_test(0)

    # well past the original deadline, still normal, no notifications
    await clocked_test(2 * TIME_DELAY)
    assert bo.eventState == EventState.normal
    assert spy.calls == []


@pytest.mark.asyncio
async def test_sustained_deviation_alarm_and_recovery(
    clocked_test: ClockedTest,
) -> None:
    """
    A mismatch held for Time_Delay transitions to offnormal; once the
    values agree again for Time_Delay_Normal it transitions back.
    """
    bo = _binary_output_ir()
    _make_app(bo)
    await clocked_test(0)
    spy = NotificationSpy(bo._event_algorithm)

    # command changes, feedback does not follow
    bo.presentValue = BinaryPV("active")
    await clocked_test(0)
    assert bo.eventState == EventState.normal

    # just before the time delay, still normal
    await clocked_test(TIME_DELAY - 1)
    assert bo.eventState == EventState.normal

    # time delay expires
    await clocked_test(1)
    assert bo.eventState == EventState.offnormal

    # the notification carries the command and feedback values at the
    # time the transition was triggered
    assert len(spy.calls) == 1
    notification = spy.calls[0]
    assert notification["eventType"] == EventType.commandFailure
    assert notification["fromState"] == EventState.normal
    assert notification["toState"] == EventState.offnormal
    command_failure = notification["eventValues"].commandFailure
    assert command_failure.commandValue.cast_out(BinaryPV) == BinaryPV("active")
    assert command_failure.feedbackValue.cast_out(BinaryPV) == BinaryPV("inactive")

    # feedback catches up; recovery takes Time_Delay_Normal
    bo.feedbackValue = BinaryPV("active")
    await clocked_test(TIME_DELAY_NORMAL - 1)
    assert bo.eventState == EventState.offnormal
    await clocked_test(1)
    assert bo.eventState == EventState.normal

    assert len(spy.calls) == 2
    notification = spy.calls[1]
    assert notification["fromState"] == EventState.offnormal
    assert notification["toState"] == EventState.normal
    command_failure = notification["eventValues"].commandFailure
    assert command_failure.commandValue.cast_out(BinaryPV) == BinaryPV("active")
    assert command_failure.feedbackValue.cast_out(BinaryPV) == BinaryPV("active")


@pytest.mark.asyncio
async def test_deviation_during_recovery(clocked_test: ClockedTest) -> None:
    """
    A new mismatch while the return to normal is pending cancels the
    pending transition; the object stays offnormal.
    """
    bo = _binary_output_ir()
    _make_app(bo)
    await clocked_test(0)
    spy = NotificationSpy(bo._event_algorithm)

    # go offnormal
    bo.presentValue = BinaryPV("active")
    await clocked_test(TIME_DELAY)
    assert bo.eventState == EventState.offnormal
    assert len(spy.calls) == 1

    # feedback catches up, return to normal pending
    bo.feedbackValue = BinaryPV("active")
    await clocked_test(2)

    # command changes again before the normal delay expires
    bo.presentValue = BinaryPV("inactive")
    await clocked_test(0)

    # the pending return to normal must have been canceled
    await clocked_test(2 * TIME_DELAY)
    assert bo.eventState == EventState.offnormal
    assert len(spy.calls) == 1

    # feedback follows again, normal after Time_Delay_Normal
    bo.feedbackValue = BinaryPV("inactive")
    await clocked_test(TIME_DELAY_NORMAL - 1)
    assert bo.eventState == EventState.offnormal
    await clocked_test(1)
    assert bo.eventState == EventState.normal
    assert len(spy.calls) == 2


@pytest.mark.asyncio
async def test_out_of_service_cancels_pending(clocked_test: ClockedTest) -> None:
    """
    An outOfService change cancels a pending transition; the old timer
    must not fire.
    """
    bo = _binary_output_ir()
    _make_app(bo)
    await clocked_test(0)
    spy = NotificationSpy(bo._event_algorithm)

    # mismatch, transition pending
    bo.presentValue = BinaryPV("active")
    await clocked_test(4)

    # going out of service cancels the pending transition and the
    # evaluation starts over
    bo.outOfService = True
    await clocked_test(0)

    # the original deadline passes with no alarm
    await clocked_test(TIME_DELAY - 4)
    assert bo.eventState == EventState.normal
    assert spy.calls == []

    # the mismatch still holds, so the fresh delay expires
    await clocked_test(4)
    assert bo.eventState == EventState.offnormal


@pytest.mark.asyncio
async def test_deleted_object_timer_abandoned(clocked_test: ClockedTest) -> None:
    """
    Deleting the monitored object abandons a pending transition; the old
    timer must not fire.
    """
    bo = _binary_output_ir()
    app = _make_app(bo)
    await clocked_test(0)
    spy = NotificationSpy(bo._event_algorithm)

    # mismatch, transition pending
    bo.presentValue = BinaryPV("active")
    await clocked_test(4)

    # delete the object before the delay expires
    app.delete_object(bo)
    await clocked_test(2 * TIME_DELAY)

    assert bo.eventState == EventState.normal
    assert spy.calls == []


@pytest.mark.asyncio
async def test_time_delay_change_cancels_pending(clocked_test: ClockedTest) -> None:
    """
    Changing the timeDelay configuration cancels a pending transition;
    the old timer must not fire and the new delay applies.
    """
    bo = _binary_output_ir()
    _make_app(bo)
    await clocked_test(0)
    spy = NotificationSpy(bo._event_algorithm)

    # mismatch, transition pending
    bo.presentValue = BinaryPV("active")
    await clocked_test(4)

    # change the time delay before it expires
    bo.timeDelay = 2 * TIME_DELAY
    await clocked_test(0)

    # the original deadline passes with no alarm
    await clocked_test(TIME_DELAY - 4)
    assert bo.eventState == EventState.normal
    assert spy.calls == []

    # the mismatch still holds, the new delay is measured from the
    # configuration change (t=4 + 2*TIME_DELAY)
    await clocked_test(TIME_DELAY + 3)
    assert bo.eventState == EventState.normal
    await clocked_test(1)
    assert bo.eventState == EventState.offnormal


@pytest.mark.asyncio
async def test_feedback_reference_switch_cancels_pending(
    clocked_test: ClockedTest,
) -> None:
    """
    Switching the feedback reference shortly before the delay expires
    cancels the pending transition; the new reference is supervised
    from then on.
    """
    bo, bv2, bv3, ee = _event_enrollment_setup()
    _make_app(bo, bv2, bv3, ee)
    await clocked_test(0)
    spy = NotificationSpy(ee._event_algorithm)

    assert ee.eventState == EventState.normal

    # command changes, feedback (bv2) does not follow
    bo.presentValue = BinaryPV("active")
    await clocked_test(TIME_DELAY - 2)

    # just before the delay expires, point the feedback reference at
    # bv3 whose value matches the command
    ee.eventParameters = _feedback_event_parameters("binary-value,3")
    await clocked_test(0)

    # the old timer must not fire
    await clocked_test(2 * TIME_DELAY)
    assert ee.eventState == EventState.normal
    assert spy.calls == []

    # the new reference is the one being supervised
    bv3.presentValue = BinaryPV("inactive")
    await clocked_test(TIME_DELAY - 1)
    assert ee.eventState == EventState.normal
    await clocked_test(1)
    assert ee.eventState == EventState.offnormal
    assert len(spy.calls) == 1


@pytest.mark.asyncio
async def test_invalid_feedback_reference_reliability(
    clocked_test: ClockedTest,
) -> None:
    """
    A feedback reference that cannot be resolved is reflected in the
    reliability of the event enrollment object.
    """
    bo, bv2, bv3, ee = _event_enrollment_setup(
        feedback_object_identifier="binary-value,99"
    )
    _make_app(bo, bv2, bv3, ee)
    await clocked_test(0)
    spy = NotificationSpy(ee._event_algorithm)

    # the invalid reference is reflected in the reliability
    assert ee.reliability == Reliability.configurationError

    # and the event state goes to fault
    await clocked_test(TIME_DELAY)
    assert ee.eventState == EventState.fault
    assert len(spy.calls) == 1
    assert spy.calls[0]["toState"] == EventState.fault

    # point the reference at a real object and the reliability clears
    ee.eventParameters = _feedback_event_parameters("binary-value,2")
    await clocked_test(0)
    assert ee.reliability == Reliability.noFaultDetected

    # back to normal after the delay
    await clocked_test(TIME_DELAY)
    assert ee.eventState == EventState.normal
    assert len(spy.calls) == 2
    assert spy.calls[1]["toState"] == EventState.normal
