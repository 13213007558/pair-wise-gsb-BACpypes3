#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test the local trend log buffer and the ReadRange services.
"""

import asyncio

import pytest

from bacpypes3.debugging import bacpypes_debugging, ModuleLogger

from bacpypes3.errors import ExecutionError, MissingRequiredParameter
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import Date, Time
from bacpypes3.basetypes import (
    DateTime,
    LogRecord,
    LogRecordLogDatum,
    Range,
    RangeByPosition,
    RangeBySequenceNumber,
    RangeByTime,
)
from bacpypes3.apdu import APDU, APCISequence, ReadRangeRequest
from bacpypes3.app import Application

from bacpypes3.local.trendlog import TrendLogBuffer, TrendLogObject

# some debugging
_debug = 0
_log = ModuleLogger(globals())

ADDRESS = Address("1.2.3.4")
TREND_LOG_ID = "trendLog,1"


def make_timestamp(second):
    return DateTime(date=Date("2026-09-26"), time=Time("10:00:%02d" % second))


def make_record(value, second):
    return LogRecord(
        timestamp=make_timestamp(second),
        logDatum=LogRecordLogDatum(realValue=float(value)),
    )


def record_values(records):
    return [record.logDatum.realValue for record in records]


@bacpypes_debugging
class LoopbackApplication(Application):
    """
    An application that serves its own client requests; the response APDU
    is encoded and decoded as if it had gone over the wire.
    """

    def __init__(self, *args, **kwargs):
        self.acks = []
        self._pending = None
        super().__init__(*args, **kwargs)

    def request(self, apdu):
        if _debug:
            LoopbackApplication._debug("request %r", apdu)

        # give the request an invoke id like the ASE would
        apdu.apduInvokeID = 1

        self._pending = asyncio.Future()
        asyncio.ensure_future(self.do_ReadRangeRequest(apdu))

        return self._pending

    async def response(self, apdu):
        if _debug:
            LoopbackApplication._debug("response %r", apdu)

        # encode and decode like the stack does
        generic_apdu = apdu.encode()
        generic_apdu.apduSeg = False
        generic_apdu.apduMor = False
        wire_apdu = APCISequence.decode(APDU.decode(generic_apdu.encode()))
        self.acks.append(wire_apdu)
        self._pending.set_result(wire_apdu)


@bacpypes_debugging
class TestTrendLogBuffer:
    def test_ring_overwrite(self):
        if _debug:
            TestTrendLogBuffer._debug("test_ring_overwrite")

        buf = TrendLogBuffer(3)
        for i in range(5):
            assert buf.append(make_record(i, i)) == i

        # the oldest records were overwritten, sequence numbers continue
        contents = buf.records()
        assert [sequence_number for sequence_number, _ in contents] == [2, 3, 4]
        assert record_values([record for _, record in contents]) == [2.0, 3.0, 4.0]
        assert buf.total_record_count == 5
        assert buf.next_sequence_number == 5

    def test_clear_continues_sequence(self):
        if _debug:
            TestTrendLogBuffer._debug("test_clear_continues_sequence")

        buf = TrendLogBuffer(3)
        for i in range(2):
            buf.append(make_record(i, i))
        buf.clear()
        assert len(buf) == 0

        # sequence numbers do not restart after a clear
        assert buf.append(make_record(2, 2)) == 2
        assert buf.append(make_record(3, 3)) == 3
        assert [sequence_number for sequence_number, _ in buf.records()] == [2, 3]

    def test_read_range_same_segment_three_ways(self):
        if _debug:
            TestTrendLogBuffer._debug("test_read_range_same_segment_three_ways")

        # small ring buffer, more samples than capacity
        buf = TrendLogBuffer(5)
        for i in range(8):
            buf.append(make_record(i, i))

        # current contents are sequence numbers 3..7, read 4..6
        by_position = buf.read_range(
            Range(byPosition=RangeByPosition(referenceIndex=2, count=3))
        )
        by_sequence_number = buf.read_range(
            Range(
                bySequenceNumber=RangeBySequenceNumber(
                    referenceSequenceNumber=4, count=3
                )
            )
        )
        by_time = buf.read_range(
            Range(byTime=RangeByTime(referenceTime=make_timestamp(4), count=3))
        )

        for result in (by_position, by_sequence_number, by_time):
            assert record_values(result.records) == [4.0, 5.0, 6.0]
            assert result.first_item is False
            assert result.last_item is False
            assert result.more_items is True
            assert result.first_sequence_number == 4

    def test_read_range_negative_count(self):
        if _debug:
            TestTrendLogBuffer._debug("test_read_range_negative_count")

        buf = TrendLogBuffer(5)
        for i in range(8):
            buf.append(make_record(i, i))

        # records up to and including the reference, chronological order
        by_position = buf.read_range(
            Range(byPosition=RangeByPosition(referenceIndex=4, count=-2))
        )
        by_sequence_number = buf.read_range(
            Range(
                bySequenceNumber=RangeBySequenceNumber(
                    referenceSequenceNumber=6, count=-2
                )
            )
        )
        by_time = buf.read_range(
            Range(byTime=RangeByTime(referenceTime=make_timestamp(6), count=-2))
        )

        for result in (by_position, by_sequence_number, by_time):
            assert record_values(result.records) == [5.0, 6.0]
            assert result.first_item is False
            assert result.last_item is False
            assert result.more_items is True
            assert result.first_sequence_number == 5

    def test_read_range_before_oldest(self):
        if _debug:
            TestTrendLogBuffer._debug("test_read_range_before_oldest")

        buf = TrendLogBuffer(5)
        for i in range(8):
            buf.append(make_record(i, i))

        # the reference sequence number has already been overwritten
        result = buf.read_range(
            Range(
                bySequenceNumber=RangeBySequenceNumber(
                    referenceSequenceNumber=1, count=2
                )
            )
        )
        assert record_values(result.records) == [3.0, 4.0]
        assert result.first_item is True
        assert result.last_item is False
        assert result.more_items is True
        assert result.first_sequence_number == 3

        # a reference time before the first record starts at the beginning
        result = buf.read_range(
            Range(byTime=RangeByTime(referenceTime=make_timestamp(0), count=2))
        )
        assert record_values(result.records) == [3.0, 4.0]
        assert result.first_item is True

    def test_read_range_past_the_end(self):
        if _debug:
            TestTrendLogBuffer._debug("test_read_range_past_the_end")

        buf = TrendLogBuffer(5)
        for i in range(3):
            buf.append(make_record(i, i))

        # nothing at or after the reference
        result = buf.read_range(
            Range(
                bySequenceNumber=RangeBySequenceNumber(
                    referenceSequenceNumber=10, count=2
                )
            )
        )
        assert result.records == []
        assert result.first_item is False
        assert result.last_item is True
        assert result.more_items is False
        assert result.first_sequence_number is None

        # a negative count from past the end returns the last records
        result = buf.read_range(
            Range(
                bySequenceNumber=RangeBySequenceNumber(
                    referenceSequenceNumber=10, count=-2
                )
            )
        )
        assert record_values(result.records) == [1.0, 2.0]
        assert result.last_item is True

    def test_sequence_number_wraparound(self):
        if _debug:
            TestTrendLogBuffer._debug("test_sequence_number_wraparound")

        buf = TrendLogBuffer(3)
        buf.next_sequence_number = 0xFFFFFFFE
        for i in range(4):
            buf.append(make_record(i, i))

        # contents are sequence numbers FFFFFFFF, 0, 1
        assert [sequence_number for sequence_number, _ in buf.records()] == [
            0xFFFFFFFF,
            0,
            1,
        ]

        result = buf.read_range(
            Range(
                bySequenceNumber=RangeBySequenceNumber(
                    referenceSequenceNumber=0xFFFFFFFF, count=3
                )
            )
        )
        assert record_values(result.records) == [1.0, 2.0, 3.0]
        assert result.first_item is True
        assert result.last_item is True
        assert result.more_items is False
        assert result.first_sequence_number == 0xFFFFFFFF

        result = buf.read_range(
            Range(
                bySequenceNumber=RangeBySequenceNumber(
                    referenceSequenceNumber=0, count=2
                )
            )
        )
        assert record_values(result.records) == [2.0, 3.0]
        assert result.first_sequence_number == 0

        # backward across the wrap boundary
        result = buf.read_range(
            Range(
                bySequenceNumber=RangeBySequenceNumber(
                    referenceSequenceNumber=0, count=-2
                )
            )
        )
        assert record_values(result.records) == [1.0, 2.0]
        assert result.first_sequence_number == 0xFFFFFFFF

    def test_same_timestamp(self):
        if _debug:
            TestTrendLogBuffer._debug("test_same_timestamp")

        buf = TrendLogBuffer(10)
        buf.append(make_record(1, 1))
        buf.append(make_record(2, 5))
        buf.append(make_record(3, 5))
        buf.append(make_record(4, 9))

        # forward starts with the first record at the reference time
        result = buf.read_range(
            Range(byTime=RangeByTime(referenceTime=make_timestamp(5), count=2))
        )
        assert record_values(result.records) == [2.0, 3.0]
        assert result.first_sequence_number == 1

        # backward ends with the last record at the reference time
        result = buf.read_range(
            Range(byTime=RangeByTime(referenceTime=make_timestamp(5), count=-2))
        )
        assert record_values(result.records) == [2.0, 3.0]
        assert result.first_sequence_number == 1


@bacpypes_debugging
class TestReadRangeServices:
    async def make_application(self):
        app = LoopbackApplication()
        trend_log = TrendLogObject(
            objectIdentifier=TREND_LOG_ID,
            objectName="tlog",
            bufferSize=5,
        )
        app.add_object(trend_log)
        return app, trend_log

    async def test_read_range_three_ways(self):
        if _debug:
            TestReadRangeServices._debug("test_read_range_three_ways")

        app, trend_log = await self.make_application()

        # more samples than the capacity of the ring buffer
        for i in range(8):
            trend_log.log_value(float(i), timestamp=make_timestamp(i))

        # the ring really rotated, the oldest record is not the first slot
        assert trend_log._log_buffer._head != 0
        assert trend_log.recordCount == 5
        assert trend_log.totalRecordCount == 8

        # read the same segment (sequence numbers 4..6) from three entries
        by_position = await app.read_range(
            ADDRESS, TREND_LOG_ID, "logBuffer", range_params=("p", 2, None, None, 3)
        )
        by_sequence_number = await app.read_range(
            ADDRESS, TREND_LOG_ID, "logBuffer", range_params=("s", 4, None, None, 3)
        )
        by_time = await app.read_range(
            ADDRESS,
            TREND_LOG_ID,
            "logBuffer",
            range_params=("t", None, "2026-09-26", "10:00:04", 3),
        )

        for result in (by_position, by_sequence_number, by_time):
            assert record_values(result) == [4.0, 5.0, 6.0]

        # the flags and first sequence number of the encoded ACKs
        for ack in app.acks:
            assert ack.resultFlags["first-item"] == 0
            assert ack.resultFlags["last-item"] == 0
            assert ack.resultFlags["more-items"] == 1
            assert ack.itemCount == 3
            assert ack.firstSequenceNumber == 4

        app.close()

    async def test_read_range_negative_count(self):
        if _debug:
            TestReadRangeServices._debug("test_read_range_negative_count")

        app, trend_log = await self.make_application()
        for i in range(8):
            trend_log.log_value(float(i), timestamp=make_timestamp(i))

        result = await app.read_range(
            ADDRESS, TREND_LOG_ID, "logBuffer", range_params=("s", 6, None, None, -2)
        )
        assert record_values(result) == [5.0, 6.0]
        assert app.acks[-1].resultFlags["more-items"] == 1
        assert app.acks[-1].firstSequenceNumber == 5

        # reading to the end of the buffer
        result = await app.read_range(
            ADDRESS, TREND_LOG_ID, "logBuffer", range_params=("s", 7, None, None, 5)
        )
        assert record_values(result) == [7.0]
        assert app.acks[-1].resultFlags["first-item"] == 0
        assert app.acks[-1].resultFlags["last-item"] == 1
        assert app.acks[-1].resultFlags["more-items"] == 0
        assert app.acks[-1].firstSequenceNumber == 7

        # reading from the beginning of the buffer
        result = await app.read_range(
            ADDRESS, TREND_LOG_ID, "logBuffer", range_params=("s", 3, None, None, 2)
        )
        assert record_values(result) == [3.0, 4.0]
        assert app.acks[-1].resultFlags["first-item"] == 1
        assert app.acks[-1].resultFlags["more-items"] == 1
        assert app.acks[-1].firstSequenceNumber == 3

        app.close()

    async def test_disabled_and_purged(self):
        if _debug:
            TestReadRangeServices._debug("test_disabled_and_purged")

        app, trend_log = await self.make_application()
        for i in range(3):
            trend_log.log_value(float(i), timestamp=make_timestamp(i))

        # disabling the log adds a status record
        trend_log.enable = False
        assert trend_log.recordCount == 4

        # logging while disabled is a no-op
        assert trend_log.log_value(9.0, timestamp=make_timestamp(9)) is None
        assert trend_log.recordCount == 4

        result = await app.read_range(
            ADDRESS, TREND_LOG_ID, "logBuffer", range_params=("p", 1, None, None, 10)
        )
        assert len(result) == 4
        assert result[-1].logDatum.logStatus["log-disabled"] == 1
        assert app.acks[-1].resultFlags["first-item"] == 1
        assert app.acks[-1].resultFlags["last-item"] == 1
        assert app.acks[-1].resultFlags["more-items"] == 0

        # purging clears the buffer and leaves a status record, the
        # sequence numbers continue where they left off
        trend_log.recordCount = 0
        assert trend_log.recordCount == 1

        result = await app.read_range(
            ADDRESS, TREND_LOG_ID, "logBuffer", range_params=("p", 1, None, None, 10)
        )
        assert len(result) == 1
        assert result[0].logDatum.logStatus["buffer-purged"] == 1
        assert app.acks[-1].firstSequenceNumber == 4

        # the next record continues the sequence
        trend_log.enable = True
        assert trend_log.log_value(42.0, timestamp=make_timestamp(10)) == 5

        app.close()

    async def test_errors(self):
        if _debug:
            TestReadRangeServices._debug("test_errors")

        app, trend_log = await self.make_application()

        # unknown object
        with pytest.raises(ExecutionError):
            await app.do_ReadRangeRequest(
                ReadRangeRequest(
                    objectIdentifier="trendLog,99",
                    propertyIdentifier="logBuffer",
                    range=Range(byPosition=RangeByPosition(referenceIndex=1, count=1)),
                )
            )

        # missing range parameter
        with pytest.raises(MissingRequiredParameter):
            await app.do_ReadRangeRequest(
                ReadRangeRequest(
                    objectIdentifier=TREND_LOG_ID,
                    propertyIdentifier="logBuffer",
                )
            )

        app.close()
