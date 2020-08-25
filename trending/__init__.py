from statistics import stdev
from dataclasses import dataclass
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional, Dict, Callable, List, Iterable


@dataclass(frozen=True)
class SupportedDocumentType:
    type: type
    interpreter: Callable
    weight_scale: Callable


class Window:

    def __init__(self, window_start: datetime, window_size_ms: int):
        self.window_start: datetime = window_start
        self.window_start_ms = self.window_start.timestamp() * 1000
        self.window_end: datetime = window_start + timedelta(milliseconds=window_size_ms)

    def __eq__(self, other):
        if type(other) == Window:
            return self.window_start == other.window_start
        return False

    def __hash__(self):
        return int(self.window_start_ms)

    def __lt__(self, other):
        return self.window_start < other.window_start


class Token:

    def __init__(self, val):
        self.val = val
        self.window_to_score: defaultdict = defaultdict(float)

    def __eq__(self, other):
        if type(other) == Token:
            return self.val == other.val

    def __hash__(self):
        return self.val.__hash__()


class Document:

    def __init__(self, time: datetime, tokens: List, supported_document_type: SupportedDocumentType):
        self.time = time
        self.tokens = tokens
        self.supported_document_type = supported_document_type


class Trending:

    def __init__(self,
                 window_size_seconds: int,
                 granularity_seconds: int,
                 ignore_empty_windows: bool = False,
                 ):

        self.window_size_seconds: int = window_size_seconds
        self.granularity_seconds: int = granularity_seconds
        self.ignore_empty_windows: bool = ignore_empty_windows

        self.earliest_window: Optional[Window] = None
        self.latest_window: Optional[datetime] = None

        self.supported_types_dict: Dict[type, SupportedDocumentType] = {}
        self.datetime_to_window: Dict[datetime, Window] = {}
        self.token_val_to_token: Dict[object, Token] = {}

    def get_trending(self, objects: List) -> List:
        token_to_score: defaultdict = defaultdict(float)
        trending: List = []
        for obj in objects:
            document: Document = self.__get_document_from_object(obj)
            for token_val in document.tokens:
                token = Token(token_val)
                token_to_score[token] += document.supported_document_type.weight_scale(document, token_val)
        for token, score in token_to_score.items():
            trending.append((token.val, self.get_zscore_for(token, score)))
        return trending

    def get_zscore_for(self, token: Token, score: float):
        token_val = token.val
        if token_val not in self.token_val_to_token:
            return 0
        token_scores: List[float] = list(self.token_val_to_token[token_val].window_to_score.values())
        average_window_score: float = sum(token_scores) / len(token_scores)
        return (score - average_window_score) / stdev(token_scores)

    def finalize_historical_data(self,
                                 earliest_window: Window,
                                 latest_window: Window,
                                 ) -> None:
        datetime_timestamp = earliest_window.window_start.timestamp()
        closest_window_start: float = datetime_timestamp - (datetime_timestamp % self.granularity_seconds)
        datetime_pointer: datetime = datetime.fromtimestamp(closest_window_start)

        while datetime_pointer < latest_window.window_start:
            current_window = Window(datetime_pointer, self.window_size_seconds)

            for token in self.token_val_to_token.values():
                if datetime_pointer not in self.datetime_to_window and not self.ignore_empty_windows:
                    token.window_to_score[current_window] = 0
                elif datetime_pointer in self.datetime_to_window and current_window not in token.window_to_score:
                    token.window_to_score[current_window] = 0

            datetime_pointer += timedelta(seconds=self.granularity_seconds)

    def add_interpreter(self, t: type, interpreter: Callable, weight_scale: Callable):
        self.supported_types_dict[t] = SupportedDocumentType(
            type=t,
            interpreter=interpreter,
            weight_scale=weight_scale,
        )

    def add_historical_documents(self, objects: Iterable) -> None:
        for obj in objects:
            self.add_historical_document(obj)

    def add_historical_document(self, obj: object) -> None:
        document: Document = self.__get_document_from_object(obj)

        windows: List[Window] = self.get_sorted_windows_for_datetime(document.time)

        if self.earliest_window is None:
            self.earliest_window = windows[0]
        if self.latest_window is None:
            self.latest_window = windows[-1]

        self.earliest_window = min(self.earliest_window, windows[0])
        self.latest_window = max(self.latest_window, windows[-1])

        for token_val in document.tokens:

            if token_val not in self.token_val_to_token:
                self.token_val_to_token[token_val] = Token(token_val)

            token_weight = document.supported_document_type.weight_scale(document, token_val)
            for window in windows:
                self.token_val_to_token[token_val].window_to_score[window] += token_weight

    def get_sorted_windows_for_datetime(self, time: datetime) -> List[Window]:
        timestamp: float = time.timestamp()
        window_start_timestamps: List[float] = self.get_window_beginnings_for_timestamp(timestamp)
        window_starts_datetime: List[datetime] = [
            datetime.fromtimestamp(timestamp) for timestamp in window_start_timestamps
        ]

        windows: List[Window] = []
        for window_start_datetime in window_starts_datetime:
            if window_start_datetime not in self.datetime_to_window:
                self.datetime_to_window[window_start_datetime] = Window(window_start_datetime, self.window_size_seconds)
            windows.append(self.datetime_to_window[window_start_datetime])

        return windows

    def get_window_beginnings_for_timestamp(self, timestamp: float) -> List[float]:
        window_starts: List[float] = []
        timestamp_minus_window: float = timestamp - self.window_size_seconds
        to_make_divisible: float = self.granularity_seconds - (timestamp_minus_window % self.granularity_seconds)
        window_start: float = timestamp_minus_window + to_make_divisible
        while window_start <= timestamp:
            window_starts.append(window_start)
            window_start += self.granularity_seconds
        return window_starts

    def __get_document_from_object(self, o: object):
        o_type: type = type(o)
        self.__ensure_type_supported(o_type)
        supported_document_type: SupportedDocumentType = self.supported_types_dict[o_type]
        time, tokens = supported_document_type.interpreter(o)
        return Document(time=time, tokens=tokens, supported_document_type=supported_document_type)

    def __ensure_type_supported(self, t: type):
        if t not in self.supported_types_dict:
            raise TypeError(f"No interpreter added for type {t}")
