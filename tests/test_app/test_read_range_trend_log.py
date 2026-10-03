"""
Test Read Range access to a local trend log.
"""

import asyncio
import uuid

import pytest

from bacpypes3.apdu import ReadRangeRequest
from bacpypes3.app import Application
from bacpypes3.basetypes import (
    DateTime,
    LogRecord,
    LogRecordLogDatum,
    LogStatus,
    Range,
    RangeByPosition,
    RangeBySequenceNumber,
    RangeByTime,
)
from bacpypes3.local.device import DeviceObject
from bacpypes3.local.trend import ListOfLogRecord, TrendLogObject
from bacpypes3.pdu import LocalStation
from bacpypes3.vlan import VirtualNetwork
from bacpypes3.vlan.link import VirtualLinkLayer


def make_record(value: int) -> LogRecord:
    return LogRecord(
        timestamp=DateTime(f"2024-01-01 00:00:{value:02d}"),
        logDatum=LogRecordLogDatum(realValue=float(value)),
    )


async def make_applications():
    network_name = f"test-{uuid.uuid4()}"
    VirtualNetwork(network_name)

    server_device = DeviceObject(
        objectIdentifier=("device", 1),
        objectName="server",
    )
    trend_log = TrendLogObject(
        objectIdentifier=("trendLog", 1),
        objectName="small ring",
        bufferSize=4,
    )
    server = Application.from_object_list([server_device, trend_log])
    server_link = VirtualLinkLayer(LocalStation(1), network_name)
    server.nsap.bind(server_link, address=LocalStation(1))

    client_device = DeviceObject(
        objectIdentifier=("device", 2),
        objectName="client",
    )
    client = Application.from_object_list([client_device])
    client_link = VirtualLinkLayer(LocalStation(2), network_name)
    client.nsap.bind(client_link, address=LocalStation(2))

    return server, client, trend_log


@pytest.mark.asyncio
async def test_trend_log_read_range_same_segment():
    server, client, trend_log = await make_applications()

    try:
        for value in range(8):
            trend_log.append_log_record(make_record(value))

        await asyncio.sleep(0)

        request = ReadRangeRequest(
            objectIdentifier=("trendLog", 1),
            propertyIdentifier="logBuffer",
            destination=LocalStation(1),
        )
        request.range = Range(
            byPosition=RangeByPosition(referenceIndex=2, count=2)
        )
        position_ack = await client.request(request)

        assert list(position_ack.resultFlags) == [0, 0, 1]
        assert position_ack.itemCount == 2
        assert int(position_ack.firstSequenceNumber) == 5
        by_position = position_ack.itemData.cast_out(type(trend_log.logBuffer))
        request.range = Range(
            byPosition=RangeByPosition(referenceIndex=3, count=-2)
        )
        reverse_ack = await client.request(request)
        assert list(reverse_ack.resultFlags) == [0, 0, 1]
        assert reverse_ack.itemCount == 2
        assert int(reverse_ack.firstSequenceNumber) == 5
        request.range = Range(
            byPosition=RangeByPosition(referenceIndex=2, count=-2)
        )
        reverse_start_ack = await client.request(request)
        assert list(reverse_start_ack.resultFlags) == [1, 0, 0]
        assert int(reverse_start_ack.firstSequenceNumber) == 4
        by_sequence = await client.read_range(
            LocalStation(1),
            ("trendLog", 1),
            "logBuffer",
            range_params=("s", 5, None, None, 2),
        )
        by_time = await client.read_range(
            LocalStation(1),
            ("trendLog", 1),
            "logBuffer",
            range_params=("t", None, "2024-01-01", "00:00:05", 2),
        )

        assert [record.logDatum.realValue for record in by_position] == [5.0, 6.0]
        assert [record.logDatum.realValue for record in by_sequence] == [5.0, 6.0]
        reverse_sequence = await client.read_range(
            LocalStation(1),
            ("trendLog", 1),
            "logBuffer",
            range_params=("s", 6, None, None, -2),
        )
        assert [record.logDatum.realValue for record in reverse_sequence] == [
            5.0,
            6.0,
        ]

        assert [record.logDatum.realValue for record in by_time] == [5.0, 6.0]
        reverse_time = await client.read_range(
            LocalStation(1),
            ("trendLog", 1),
            "logBuffer",
            range_params=("t", None, "2024-01-01", "00:00:06", -2),
        )
        assert [record.logDatum.realValue for record in reverse_time] == [
            5.0,
            6.0,
        ]
    finally:
        client.close()
        server.close()


@pytest.mark.asyncio
async def test_trend_log_same_timestamp_and_missing_history():
    trend_log = TrendLogObject(
        objectIdentifier=("trendLog", 2),
        objectName="same timestamp",
        bufferSize=3,
    )

    for value in range(3):
        trend_log.append_log_record(make_record(value))
    for value in range(3):
        trend_log.append_log_record(
            LogRecord(
                timestamp=DateTime("2024-01-01 00:00:10"),
                logDatum=LogRecordLogDatum(realValue=10.0 + value),
            )
        )

    items, first_sequence, flags = trend_log.read_range(
        Range(
            byTime=RangeByTime(
                referenceTime=DateTime("2024-01-01 00:00:10"),
                count=3,
            )
        )
    )
    assert first_sequence == 3
    assert flags == [1, 1, 0]
    assert [record.logDatum.realValue for record in items] == [10.0, 11.0, 12.0]

    items, first_sequence, flags = trend_log.read_range(
        Range(
            byTime=RangeByTime(
                referenceTime=DateTime("2024-01-01 00:00:10"),
                count=-1,
            )
        )
    )
    assert first_sequence == 3
    assert flags == [1, 1, 0]
    assert [record.logDatum.realValue for record in items] == [10.0, 11.0, 12.0]

    items, first_sequence, flags = trend_log.read_range(
        Range(
            bySequenceNumber=RangeBySequenceNumber(
                referenceSequenceNumber=0,
                count=2,
            )
        )
    )
    assert first_sequence == 3
    assert flags == [1, 0, 1]
    assert [record.logDatum.realValue for record in items] == [10.0, 11.0]


@pytest.mark.asyncio
async def test_trend_log_sequence_number_wrap():
    trend_log = TrendLogObject(
        objectIdentifier=("trendLog", 3),
        objectName="wrap",
        bufferSize=3,
    )
    trend_log._next_sequence_number = (1 << 32) - 2
    trend_log._first_sequence_number = trend_log._next_sequence_number

    for value in range(4):
        trend_log.append_log_record(make_record(100 + value))

    assert trend_log.totalRecordCount == 2
    items, first_sequence, flags = trend_log.read_range(
        Range(
            bySequenceNumber=RangeBySequenceNumber(
                referenceSequenceNumber=(1 << 32) - 1,
                count=4,
            )
        )
    )
    assert first_sequence == (1 << 32) - 1
    assert flags == [1, 1, 0]
    assert [record.logDatum.realValue for record in items] == [101.0, 102.0, 103.0]


@pytest.mark.asyncio
async def test_trend_log_status_records_and_bounds():
    server, client, trend_log = await make_applications()

    try:
        for value in range(8):
            trend_log.append_log_record(make_record(value))

        purge_sequence = trend_log.purge_buffer(DateTime("2024-01-02 00:00:00"))
        trend_log.enable = False

        records = await client.read_range(
            LocalStation(1),
            ("trendLog", 1),
            "logBuffer",
            range_params=("s", purge_sequence, None, None, 2),
        )

        assert len(records) == 2
        assert records[0].logDatum.logStatus[LogStatus.bufferPurged] == 1
        assert records[1].logDatum.logStatus[LogStatus.logDisabled] == 1
        assert trend_log.logStatus[LogStatus.logDisabled] == 1

        reverse_records = await client.read_range(
            LocalStation(1),
            ("trendLog", 1),
            "logBuffer",
            range_params=("s", purge_sequence + 1, None, None, -2),
        )
        assert [record.logDatum.logStatus for record in reverse_records] == [
            record.logDatum.logStatus for record in records
        ]
    finally:
        client.close()
        server.close()


@pytest.mark.asyncio
async def test_empty_trend_log_ack():
    server, client, _ = await make_applications()

    try:
        request = ReadRangeRequest(
            objectIdentifier=("trendLog", 1),
            propertyIdentifier="logBuffer",
            destination=LocalStation(1),
        )
        request.range = Range(
            byPosition=RangeByPosition(referenceIndex=1, count=10)
        )
        ack = await client.request(request)

        assert list(ack.resultFlags) == [1, 1, 0]
        assert ack.itemCount == 0
        assert ack.firstSequenceNumber is None
        records = ack.itemData.cast_out(ListOfLogRecord)
        assert records == []
    finally:
        client.close()
        server.close()
