from statistics import stdev
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional, Dict, Callable, List, Iterable

from pyztrending.models import SupportedDocumentType, Window, Token, Document


class Trending:

    def __init__(self,
                 window_size_seconds: int,
                 granularity_seconds: int,
                 ignore_empty_windows: bool = False,
                 ):

        self.__window_size_seconds: int = window_size_seconds
        self.__granularity_seconds: int = granularity_seconds
        self.__ignore_empty_windows: bool = ignore_empty_windows

        self.__earliest_window: Optional[Window] = None
        self.__latest_window: Optional[Window] = None

        self.__supported_types_dict: Dict[type, SupportedDocumentType] = {}
        self.__datetime_to_window: Dict[datetime, Window] = {}
        self.__token_val_to_token: Dict[object, Token] = {}

        self.__historical_data_finalized = False

    def add_class_support(self, t: type, interpreter: Callable, weight_scale: Callable):
        self.__supported_types_dict[t] = SupportedDocumentType(
            t,
            interpreter,
            weight_scale,
        )

    def get_trending(self, objects: List) -> List:
        if not self.__historical_data_finalized:
            self.__finalize_historical_data(
                self.__earliest_window,
                self.__latest_window,
            )
        token_to_score: defaultdict = defaultdict(float)
        trending: List = []
        for obj in objects:
            document: Document = self.__get_document_from_object(obj)
            for token_val in document.tokens:
                token = Token(token_val)
                token_to_score[token] += document.supported_document_type.weight_scale(document, token_val)
        for token, score in token_to_score.items():
            trending.append((token.val, self.__get_zscore_for(token, score)))
        return trending

    def __get_zscore_for(self, token: Token, score: float):
        token_val = token.val
        if token_val not in self.__token_val_to_token:
            return 0
        token_scores: List[float] = list(self.__token_val_to_token[token_val].window_to_score.values())
        average_window_score: float = sum(token_scores) / len(token_scores)
        return (score - average_window_score) / stdev(token_scores)

    def __finalize_historical_data(self,
                                   earliest_window: Window,
                                   latest_window: Window,
                                   ) -> None:
        datetime_timestamp = earliest_window.window_start.timestamp()
        closest_window_start: float = datetime_timestamp - (datetime_timestamp % self.__granularity_seconds)
        datetime_pointer: datetime = datetime.fromtimestamp(closest_window_start)

        while datetime_pointer < latest_window.window_start:
            current_window = Window(datetime_pointer, self.__window_size_seconds)

            for token in self.__token_val_to_token.values():
                if datetime_pointer not in self.__datetime_to_window and not self.__ignore_empty_windows:
                    token.window_to_score[current_window] = 0
                elif datetime_pointer in self.__datetime_to_window and current_window not in token.window_to_score:
                    token.window_to_score[current_window] = 0

            datetime_pointer += timedelta(seconds=self.__granularity_seconds)

    def __add_historical_document(self, obj: object) -> None:
        self.historical_data_finalized = False
        document: Document = self.__get_document_from_object(obj)

        windows: List[Window] = self.__get_sorted_windows_for_datetime(document.time)

        if self.earliest_window is None:
            self.earliest_window = windows[0]
        if self.latest_window is None:
            self.latest_window = windows[-1]

        self.earliest_window = min(self.earliest_window, windows[0])
        self.latest_window = max(self.latest_window, windows[-1])

        for token_val in document.tokens:

            if token_val not in self.__token_val_to_token:
                self.__token_val_to_token[token_val] = Token(token_val)

            token_weight = document.supported_document_type.weight_scale(document, token_val)
            for window in windows:
                self.__token_val_to_token[token_val].window_to_score[window] += token_weight

    def __get_sorted_windows_for_datetime(self, time: datetime) -> List[Window]:
        timestamp: float = time.timestamp()
        window_start_timestamps: List[float] = self.__get_window_beginnings_for_timestamp(timestamp)
        window_starts_datetime: List[datetime] = [
            datetime.fromtimestamp(timestamp) for timestamp in window_start_timestamps
        ]

        windows: List[Window] = []
        for window_start_datetime in window_starts_datetime:
            if window_start_datetime not in self.__datetime_to_window:
                self.__datetime_to_window[window_start_datetime] = Window(
                    window_start_datetime,
                    self.__window_size_seconds
                )
            windows.append(self.__datetime_to_window[window_start_datetime])

        return windows

    def __get_window_beginnings_for_timestamp(self, timestamp: float) -> List[float]:
        window_starts: List[float] = []
        timestamp_minus_window: float = timestamp - self.__window_size_seconds
        to_make_divisible: float = self.__granularity_seconds - (timestamp_minus_window % self.__granularity_seconds)
        window_start: float = timestamp_minus_window + to_make_divisible
        while window_start <= timestamp:
            window_starts.append(window_start)
            window_start += self.__granularity_seconds
        return window_starts

    def __get_document_from_object(self, o: object):
        o_type: type = type(o)
        self.__ensure_type_supported(o_type)
        supported_document_type: SupportedDocumentType = self.__supported_types_dict[o_type]
        time, tokens = supported_document_type.interpreter(o)
        return Document(time=time, tokens=tokens, supported_document_type=supported_document_type)

    def __ensure_type_supported(self, t: type):
        if t not in self.__supported_types_dict:
            raise TypeError(f"No interpreter added for type {t}")
