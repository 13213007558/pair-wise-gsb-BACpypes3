"""
Trend Log Object
"""

from __future__ import annotations

from collections import deque
from typing import Callable, List, Optional, Sequence, Tuple

from ..basetypes import (
    DateTime,
    LogRecord,
    LogRecordLogDatum,
    LogStatus,
    LoggingType,
    Range,
    RangeByPosition,
    RangeBySequenceNumber,
    RangeByTime,
)
from ..constructeddata import ListOf
from ..debugging import ModuleLogger, bacpypes_debugging
from ..errors import PropertyError
from ..object import TrendLogObject as _TrendLogObject
from ..primitivedata import ObjectType

from .object import Object as _Object

# some debugging
_debug = 0
_log = ModuleLogger(globals())

# this is for sample applications
_vendor_id = 999

SEQUENCE_NUMBER_MODULUS = 1 << 32
ListOfLogRecord = ListOf(LogRecord)


@bacpypes_debugging
class TrendLogObject(_Object, _TrendLogObject):
    """
    A local trend log object with a fixed-capacity ring buffer.
    """

    _debug: Callable[..., None]

    objectType = ObjectType.trendLog

    def __init__(self, *args, **kwargs) -> None:
        if _debug:
            TrendLogObject._debug("__init__ %r %r", args, kwargs)

        buffer_size = int(kwargs.get("bufferSize", 0))
        if buffer_size < 0:
            raise ValueError("bufferSize")

        self._ring_buffer: deque[LogRecord] = deque(
            (), buffer_size if buffer_size else None
        )
        self._first_sequence_number = 0
        self._next_sequence_number = 0
        self._initialized = False
        self._log_status = LogStatus([0, 0, 0])

        kwargs.setdefault("enable", True)
        kwargs.setdefault("stopWhenFull", False)
        kwargs.setdefault("bufferSize", buffer_size)
        kwargs.setdefault("loggingType", LoggingType.polled)
        kwargs.setdefault("statusFlags", [0, 0, 0, 0])
        kwargs.setdefault("reliability", "no-fault-detected")
        kwargs.setdefault("logBuffer", ListOfLogRecord())
        kwargs.setdefault("recordCount", 0)
        kwargs.setdefault("totalRecordCount", 0)
        kwargs.setdefault("logStatus", self._log_status)

        super().__init__(*args, **kwargs)
        self._initialized = True

    @property
    def enable(self) -> bool:  # type: ignore[override]
        return not bool(self._log_status[LogStatus.logDisabled])

    @enable.setter
    def enable(self, value) -> None:
        enabled = bool(value)
        if not self._initialized:
            self._log_status[LogStatus.logDisabled] = int(not enabled)
            return
        if enabled == self.enable:
            return
        self.append_status_record(
            DateTime.now(), log_disabled=not enabled
        )

    @property
    def logBuffer(self) -> ListOfLogRecord:  # type: ignore[override]
        if _debug:
            TrendLogObject._debug("logBuffer(getter)")
        return ListOfLogRecord(self._ring_buffer)

    @logBuffer.setter
    def logBuffer(self, value) -> None:
        if _debug:
            TrendLogObject._debug("logBuffer(setter) %r", value)
        if not self._initialized:
            return
        raise PropertyError("writeAccessDenied")

    @property
    def bufferSize(self) -> int:  # type: ignore[override]
        return self._ring_buffer.maxlen or 0

    @bufferSize.setter
    def bufferSize(self, value) -> None:
        if _debug:
            TrendLogObject._debug("bufferSize(setter) %r", value)
        if not self._initialized:
            return
        raise PropertyError("writeAccessDenied")

    @property
    def recordCount(self) -> int:  # type: ignore[override]
        return len(self._ring_buffer)

    @recordCount.setter
    def recordCount(self, value) -> None:
        if _debug:
            TrendLogObject._debug("recordCount(setter) %r", value)
        if self._initialized and int(value) == 0:
            self.purge_buffer(DateTime.now())

    @property
    def totalRecordCount(self) -> int:  # type: ignore[override]
        return self._next_sequence_number

    @totalRecordCount.setter
    def totalRecordCount(self, value) -> None:
        if _debug:
            TrendLogObject._debug("totalRecordCount(setter) %r", value)

    @property
    def logStatus(self) -> LogStatus:
        return LogStatus(self._log_status)

    @logStatus.setter
    def logStatus(self, value) -> None:
        if _debug:
            TrendLogObject._debug("logStatus(setter) %r", value)
        self._log_status = LogStatus(value)

    def append_log_record(self, record: LogRecord) -> int:
        if _debug:
            TrendLogObject._debug("append_log_record %r", record)
        if not isinstance(record, LogRecord):
            raise TypeError("LogRecord expected")
        if not self.enable:
            raise PropertyError("writeAccessDenied")
        return self._append_record(record)

    def append_status_record(
        self,
        timestamp: DateTime,
        log_disabled: Optional[bool] = None,
        buffer_purged: bool = False,
        log_interrupted: bool = False,
    ) -> int:
        if _debug:
            TrendLogObject._debug(
                "append_status_record %r %r %r %r",
                timestamp,
                log_disabled,
                buffer_purged,
                log_interrupted,
            )

        status = LogStatus(self._log_status)
        if log_disabled is not None:
            status[LogStatus.logDisabled] = int(log_disabled)
        status[LogStatus.bufferPurged] = int(buffer_purged)
        status[LogStatus.logInterrupted] = int(log_interrupted)
        self._log_status = status

        record = LogRecord(
            timestamp=timestamp,
            logDatum=LogRecordLogDatum(logStatus=LogStatus(status)),
        )
        return self._append_record(record)

    def purge_buffer(self, timestamp: DateTime) -> int:
        if _debug:
            TrendLogObject._debug("purge_buffer %r", timestamp)
        self._ring_buffer.clear()
        self._first_sequence_number = self._next_sequence_number
        return self.append_status_record(timestamp, buffer_purged=True)

    def _append_record(self, record: LogRecord) -> int:
        if (
            bool(self.stopWhenFull)
            and self._ring_buffer.maxlen is not None
            and len(self._ring_buffer) == self._ring_buffer.maxlen
        ):
            raise IndexError("trend log buffer is full")

        sequence_number = self._next_sequence_number
        if len(self._ring_buffer) == self._ring_buffer.maxlen:
            self._first_sequence_number = (
                self._first_sequence_number + 1
            ) % SEQUENCE_NUMBER_MODULUS
        self._ring_buffer.append(record)
        self._next_sequence_number = (
            sequence_number + 1
        ) % SEQUENCE_NUMBER_MODULUS
        return sequence_number

    def _ordered_records(self) -> List[Tuple[int, LogRecord]]:
        return [
            (
                (self._first_sequence_number + index) % SEQUENCE_NUMBER_MODULUS,
                record,
            )
            for index, record in enumerate(self._ring_buffer)
        ]

    def read_range(
        self, range_value: Optional[Range]
    ) -> Tuple[ListOfLogRecord, Optional[int], List[int]]:
        records = self._ordered_records()
        count = len(records)

        if not records:
            return ListOfLogRecord(), None, [1, 1, 0]

        first_sequence = self._first_sequence_number

        if range_value is None:
            start = 0
            end = count
        elif range_value.byPosition:
            start, end = self._range_by_position(
                range_value.byPosition, count
            )
        elif range_value.bySequenceNumber:
            start, end = self._range_by_sequence_number(
                range_value.bySequenceNumber, records, count
            )
        elif range_value.byTime:
            start, end = self._range_by_time(
                range_value.byTime, records
            )
        else:
            start = 0
            end = count

        selected = ListOfLogRecord(
            [record for _, record in records[start:end]]
        )
        first_item = int(start == 0)
        last_item = int(end == count)
        if range_value is None or self._requested_direction_positive(range_value):
            more_items = int(end < count)
        else:
            more_items = int(start > 0)
        result_first_sequence = (
            first_sequence + start
        ) % SEQUENCE_NUMBER_MODULUS
        if start == end:
            result_first_sequence = None

        return selected, result_first_sequence, [
            first_item,
            last_item,
            more_items,
        ]

    @staticmethod
    def _requested_direction_positive(range_value: Range) -> bool:
        if range_value.byPosition:
            return int(range_value.byPosition.count) > 0
        if range_value.bySequenceNumber:
            return int(range_value.bySequenceNumber.count) > 0
        return int(range_value.byTime.count) > 0

    @staticmethod
    def _range_by_position(
        range_by_position: RangeByPosition, record_count: int
    ) -> Tuple[int, int]:
        reference_index = int(range_by_position.referenceIndex)
        requested_count = int(range_by_position.count)
        if requested_count == 0:
            return 0, 0

        if requested_count > 0:
            start = max(0, reference_index - 1)
            start = min(start, record_count)
            end = min(record_count, start + requested_count)
            return start, end

        end = min(record_count, max(0, reference_index))
        start = max(0, end + requested_count)
        return start, end

    @staticmethod
    def _range_by_sequence_number(
        range_by_sequence_number: RangeBySequenceNumber,
        records: Sequence[Tuple[int, LogRecord]],
        record_count: int,
    ) -> Tuple[int, int]:
        reference = int(range_by_sequence_number.referenceSequenceNumber)
        requested_count = int(range_by_sequence_number.count)
        if requested_count == 0:
            return 0, 0

        first_sequence = records[0][0]
        distance = (reference - first_sequence) % SEQUENCE_NUMBER_MODULUS
        if distance <= record_count:
            anchor = distance
        elif distance > SEQUENCE_NUMBER_MODULUS // 2:
            anchor = 0
        else:
            anchor = record_count

        if requested_count > 0:
            start = anchor
            end = min(record_count, start + requested_count)
            return start, end

        end = max(0, min(record_count, anchor + 1))
        start = max(0, end + requested_count)
        return start, end

    @staticmethod
    def _range_by_time(
        range_by_time: RangeByTime,
        records: Sequence[Tuple[int, LogRecord]],
    ) -> Tuple[int, int]:
        reference = range_by_time.referenceTime
        requested_count = int(range_by_time.count)
        if requested_count == 0:
            return 0, 0

        record_count = len(records)
        if requested_count > 0:
            start = record_count
            for index, (_, record) in enumerate(records):
                if _date_time_key(record.timestamp) >= _date_time_key(reference):
                    start = index
                    break
            if start >= record_count:
                return record_count, record_count
            end = min(record_count, start + requested_count)
            return _expand_time_range(records, start, end)

        end = 0
        for index, (_, record) in enumerate(records):
            if _date_time_key(record.timestamp) <= _date_time_key(reference):
                end = index + 1
        if end == 0:
            return 0, 0
        start = max(0, end + requested_count)
        return _expand_time_range(records, start, end)


def _date_time_key(value: DateTime) -> Tuple:
    return tuple(value.date) + tuple(value.time)


def _expand_time_range(
    records: Sequence[Tuple[int, LogRecord]], start: int, end: int
) -> Tuple[int, int]:
    record_count = len(records)
    start = max(0, min(start, record_count))
    end = max(0, min(end, record_count))
    if start >= end:
        return start, end

    first_time = _date_time_key(records[start][1].timestamp)
    while start > 0 and _date_time_key(records[start - 1][1].timestamp) == first_time:
        start -= 1

    last_time = _date_time_key(records[end - 1][1].timestamp)
    while (
        end < record_count
        and _date_time_key(records[end][1].timestamp) == last_time
    ):
        end += 1

    return start, end
