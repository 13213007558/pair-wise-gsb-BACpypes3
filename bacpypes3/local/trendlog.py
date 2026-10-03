"""
Local Trend Log Object

A local trend log object keeps its records in a ring buffer.  Each record
is associated with a sequence number that continues to increase (modulo
2**32) as records are added, even after old records have been overwritten
or the buffer has been cleared, so a collector can continue reading from
the sequence number of the last record it received.
"""

from __future__ import annotations

from typing import Callable, List, NamedTuple, Optional, Tuple

from ..debugging import bacpypes_debugging, ModuleLogger
from ..errors import PropertyError
from ..primitivedata import PropertyIdentifier
from ..basetypes import (
    DateTime,
    LogRecord,
    LogRecordLogDatum,
    LogStatus,
    Range,
)

from ..object import TrendLogObject as _TrendLogObject
from .object import Object as _Object

# some debugging
_debug = 0
_log = ModuleLogger(globals())

# sequence numbers are modulo 2**32, a difference of less than half of the
# modulus is considered "in the future" of the reference
SEQUENCE_NUMBER_MODULUS = 1 << 32
SEQUENCE_NUMBER_HALF = 1 << 31


def _sequence_delta(a: int, b: int) -> int:
    """Return (a - b) modulo 2**32."""
    return (a - b) % SEQUENCE_NUMBER_MODULUS


def _timestamp_key(timestamp: DateTime) -> Tuple[int, ...]:
    """Return a comparable key for a DateTime value."""
    return tuple(timestamp.date)[:3] + tuple(timestamp.time)


class ReadRangeResult(NamedTuple):
    """
    The result of a read_range() query: the selected records in
    chronological order, the result flags, and the sequence number of the
    first selected record (None if no records were selected).
    """

    records: List[LogRecord]
    first_item: bool
    last_item: bool
    more_items: bool
    first_sequence_number: Optional[int]


@bacpypes_debugging
class TrendLogBuffer:
    """
    A ring buffer of (sequence number, LogRecord) pairs.  The internal
    slots are reused as records are added, so the oldest record is not
    necessarily the first element of the internal list; use records() for
    the contents in chronological order.
    """

    _debug: Callable[..., None]

    capacity: int
    total_record_count: int

    def __init__(self, capacity: int) -> None:
        if _debug:
            TrendLogBuffer._debug("__init__ %r", capacity)
        if capacity < 1:
            raise ValueError("capacity must be at least one")

        self.capacity = capacity
        self._slots: List[Optional[Tuple[int, LogRecord]]] = [None] * capacity
        self._head = 0
        self._size = 0
        self._next_sequence_number = 0
        self.total_record_count = 0

    def __len__(self) -> int:
        return self._size

    @property
    def full(self) -> bool:
        return self._size == self.capacity

    @property
    def next_sequence_number(self) -> int:
        return self._next_sequence_number

    @next_sequence_number.setter
    def next_sequence_number(self, value: int) -> None:
        self._next_sequence_number = value % SEQUENCE_NUMBER_MODULUS

    def append(self, record: LogRecord) -> int:
        """Add a record to the buffer, return its sequence number."""
        if _debug:
            TrendLogBuffer._debug("append %r", record)

        sequence_number = self._next_sequence_number
        self._slots[self._head] = (sequence_number, record)
        self._head = (self._head + 1) % self.capacity
        if self._size < self.capacity:
            self._size += 1
        self._next_sequence_number = (sequence_number + 1) % SEQUENCE_NUMBER_MODULUS
        self.total_record_count += 1

        return sequence_number

    def clear(self) -> None:
        """
        Delete all of the records in the buffer.  The sequence number
        sequence is not reset, the next record added continues where the
        previous one left off.
        """
        if _debug:
            TrendLogBuffer._debug("clear")

        self._slots = [None] * self.capacity
        self._head = 0
        self._size = 0

    def records(self) -> List[Tuple[int, LogRecord]]:
        """Return the contents as (sequence number, record) pairs, oldest
        first, independent of the internal slot arrangement."""
        if self._size < self.capacity:
            start = 0
        else:
            start = self._head
        return [
            self._slots[(start + i) % self.capacity]  # type: ignore[misc]
            for i in range(self._size)
        ]

    def read_range(self, range_spec: Range) -> ReadRangeResult:
        """
        Select records by position, sequence number, or time.

        Positions are one-based over the current contents, the oldest
        record is at position one.  A positive count selects records
        forward from the reference, a negative count selects records
        backward up to and including the reference; the selected records
        are always returned in chronological order.
        """
        if _debug:
            TrendLogBuffer._debug("read_range %r", range_spec)

        contents = self.records()
        content_count = len(contents)

        by_position = range_spec.byPosition
        by_sequence_number = range_spec.bySequenceNumber
        by_time = range_spec.byTime

        if by_position is not None:
            reference = by_position.referenceIndex
            count = by_position.count
            if count >= 0:
                start = min(max(reference - 1, 0), content_count)
                end = min(start + count, content_count)
            else:
                end = min(max(reference, 0), content_count)
                start = max(end + count, 0)

        elif by_sequence_number is not None:
            reference = by_sequence_number.referenceSequenceNumber
            count = by_sequence_number.count
            if count >= 0:
                # first record whose sequence number is at or after the
                # reference, modulo 2**32
                start = content_count
                for i, (sequence_number, _) in enumerate(contents):
                    if _sequence_delta(sequence_number, reference) < (
                        SEQUENCE_NUMBER_HALF
                    ):
                        start = i
                        break
                end = min(start + count, content_count)
            else:
                # one past the last record whose sequence number is at or
                # before the reference, modulo 2**32
                end = 0
                for i, (sequence_number, _) in enumerate(contents):
                    if _sequence_delta(reference, sequence_number) < (
                        SEQUENCE_NUMBER_HALF
                    ):
                        end = i + 1
                start = max(end + count, 0)

        elif by_time is not None:
            reference_key = _timestamp_key(by_time.referenceTime)
            count = by_time.count
            keys = [_timestamp_key(record.timestamp) for _, record in contents]
            if count >= 0:
                # first record at or after the reference time
                start = content_count
                for i, key in enumerate(keys):
                    if key >= reference_key:
                        start = i
                        break
                end = min(start + count, content_count)
            else:
                # one past the last record at or before the reference time
                end = 0
                for i, key in enumerate(keys):
                    if key <= reference_key:
                        end = i + 1
                start = max(end + count, 0)

        else:
            raise ValueError("range choice required")

        selected = contents[start:end]
        if _debug:
            TrendLogBuffer._debug("    - selected: %r", selected)

        return ReadRangeResult(
            records=[record for _, record in selected],
            first_item=(start == 0),
            last_item=(end == content_count),
            more_items=(end < content_count) if count >= 0 else (start > 0),
            first_sequence_number=selected[0][0] if selected else None,
        )


@bacpypes_debugging
class TrendLogObject(_Object, _TrendLogObject):
    """
    A local trend log object with a ring buffer of log records.
    """

    _debug: Callable[..., None]

    _log_buffer: TrendLogBuffer

    def __init__(self, **kwargs) -> None:
        if _debug:
            TrendLogObject._debug("__init__ %r", kwargs)

        # make sure the buffer size was provided
        if "bufferSize" not in kwargs:
            raise RuntimeError("bufferSize required")

        # logging is enabled by default
        kwargs.setdefault("enable", True)
        kwargs.setdefault("stopWhenFull", False)

        # continue initialization
        super().__init__(**kwargs)

        # create the ring buffer and synchronize the log properties
        self._log_buffer = TrendLogBuffer(int(self.bufferSize))
        self._sync_log_properties()

        # monitor changes
        self._property_monitors["enable"].append(self._enable_changed)
        self._property_monitors["recordCount"].append(self._record_count_changed)

    def _sync_log_properties(self) -> None:
        """Copy the buffer contents and counters into the properties."""
        if _debug:
            TrendLogObject._debug("_sync_log_properties")

        contents = self._log_buffer.records()
        self.logBuffer = [record for _, record in contents]
        self.recordCount = len(contents)
        self.totalRecordCount = self._log_buffer.total_record_count

    def _enable_changed(self, old_value, new_value) -> None:
        if _debug:
            TrendLogObject._debug("_enable_changed %r %r", old_value, new_value)

        # record that logging was disabled
        if not new_value:
            self.log_status(log_disabled=True)

    def _record_count_changed(self, old_value, new_value) -> None:
        if _debug:
            TrendLogObject._debug("_record_count_changed %r %r", old_value, new_value)

        # writing a zero record count purges the buffer
        if not new_value:
            self.purge()

    def purge(self) -> None:
        """
        Delete all of the records in the buffer and add a status record
        with bufferPurged set so the purge is visible when the log is
        read back.  Record sequence numbers are not reset.
        """
        if _debug:
            TrendLogObject._debug("purge")

        self._log_buffer.clear()
        self.log_status(buffer_purged=True)

    def log_record(self, record: LogRecord) -> Optional[int]:
        """
        Add a record to the log buffer if logging is enabled, returning
        its sequence number, or None if the record was not logged.
        """
        if _debug:
            TrendLogObject._debug("log_record %r", record)

        if not self.enable:
            if _debug:
                TrendLogObject._debug("    - logging disabled")
            return None
        if self._log_buffer.full and self.stopWhenFull:
            if _debug:
                TrendLogObject._debug("    - buffer full")
            return None

        sequence_number = self._log_buffer.append(record)
        self._sync_log_properties()

        return sequence_number

    def log_value(self, value, timestamp: Optional[DateTime] = None) -> Optional[int]:
        """Log a value, mapping its Python type to a log datum choice."""
        if _debug:
            TrendLogObject._debug("log_value %r %r", value, timestamp)

        if timestamp is None:
            timestamp = DateTime.now()

        if isinstance(value, LogRecordLogDatum):
            log_datum = value
        elif isinstance(value, bool):
            log_datum = LogRecordLogDatum(booleanValue=value)
        elif isinstance(value, float):
            log_datum = LogRecordLogDatum(realValue=value)
        elif isinstance(value, int):
            if value >= 0:
                log_datum = LogRecordLogDatum(unsignedValue=value)
            else:
                log_datum = LogRecordLogDatum(signedValue=value)
        else:
            raise TypeError(f"unsupported value type: {type(value)}")

        return self.log_record(LogRecord(timestamp=timestamp, logDatum=log_datum))

    def log_status(
        self,
        log_disabled: bool = False,
        buffer_purged: bool = False,
        log_interrupted: bool = False,
        timestamp: Optional[DateTime] = None,
    ) -> int:
        """
        Add a status record to the log buffer.  Status records are added
        even when logging is disabled or the buffer is full so that the
        condition remains visible when the log is read back.
        """
        if _debug:
            TrendLogObject._debug(
                "log_status %r %r %r %r",
                log_disabled,
                buffer_purged,
                log_interrupted,
                timestamp,
            )

        if timestamp is None:
            timestamp = DateTime.now()

        flags = []
        if log_disabled:
            flags.append("log-disabled")
        if buffer_purged:
            flags.append("buffer-purged")
        if log_interrupted:
            flags.append("log-interrupted")

        record = LogRecord(
            timestamp=timestamp,
            logDatum=LogRecordLogDatum(logStatus=LogStatus(flags)),
        )
        sequence_number = self._log_buffer.append(record)
        self._sync_log_properties()

        return sequence_number

    def log_time_change(
        self, time_change: float, timestamp: Optional[DateTime] = None
    ) -> Optional[int]:
        """Log a time change record."""
        if _debug:
            TrendLogObject._debug("log_time_change %r %r", time_change, timestamp)

        if timestamp is None:
            timestamp = DateTime.now()

        return self.log_record(
            LogRecord(
                timestamp=timestamp,
                logDatum=LogRecordLogDatum(timeChange=time_change),
            )
        )

    def read_range(self, property_identifier, range_spec: Range) -> ReadRangeResult:
        """
        Read a range of records from the log buffer; see
        TrendLogBuffer.read_range().
        """
        if _debug:
            TrendLogObject._debug("read_range %r %r", property_identifier, range_spec)

        if PropertyIdentifier(property_identifier).attr != "logBuffer":
            raise PropertyError(errorCode="unknownProperty")

        return self._log_buffer.read_range(range_spec)
