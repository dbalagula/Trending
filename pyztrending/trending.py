from statistics import stdev
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional, Dict, Callable, List, Iterable, Set

from pyztrending.exceptions import NonNormalDistributionError
from pyztrending.models import SupportedDocumentType, Window, Token, Document


class Trending:

    def __init__(self,
                 window_size_seconds: int,
                 granularity_seconds: int,
                 should_ignore_empty_windows: bool = False,
                 ):

        if granularity_seconds > window_size_seconds:
            raise ValueError("Can't have step size be greater tha window size!")

        self.__window_size_seconds: int = window_size_seconds
        self.__granularity_seconds: int = granularity_seconds
        self.__should_ignore_empty_windows: bool = should_ignore_empty_windows

        self.__earliest_window: Optional[Window] = None
        self.__latest_window: Optional[Window] = None

        self.__supported_types_dict: Dict[type, SupportedDocumentType] = {}
        self.__datetime_to_window: Dict[datetime, Window] = {}
        self.__token_val_to_token: Dict[object, Token] = {}

        self.__is_historical_data_finalized: bool = False

    def add_type_support(self, t: type, interpreter: Callable, weight_scale: Callable):
        self.__supported_types_dict[t] = SupportedDocumentType(
            t,
            interpreter,
            weight_scale,
        )

    def add_historical_documents(self, objects: Iterable) -> None:
        for obj in objects:
            self.__add_historical_document(obj)

    def get_trending_and_ingest(self, objects: List) -> List:
        trending: List = self.get_trending(objects)
        self.__token_val_to_token = {}
        self.add_historical_documents(objects)
        return trending

    def get_trending(self, objects: List) -> List:
        if not self.__is_historical_data_finalized:
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

    @property
    def tokens(self) -> Set[object]:
        return set(self.__token_val_to_token.keys())

    def __get_zscore_for(self, token: Token, score: float):
        token_val = token.val
        if token_val not in self.__token_val_to_token:
            return 0
        token_scores: List[float] = list(self.__token_val_to_token[token_val].window_to_score.values())
        average_window_score: float = sum(token_scores) / len(token_scores)
        try:
            return (score - average_window_score) / stdev(token_scores)
        except ZeroDivisionError:
            raise NonNormalDistributionError(
                f"Cannot calculate z score for distribution, all values are {token_scores[0]}"
            )

    def __finalize_historical_data(self,
                                   earliest_window: Window,
                                   latest_window: Window,
                                   ) -> None:

        datetime_pointer: datetime = datetime.fromtimestamp(earliest_window.window_start_timestamp)

        while datetime_pointer < latest_window.window_start:
            current_window = Window(datetime_pointer, self.__window_size_seconds)

            for token in self.__token_val_to_token.values():
                if datetime_pointer not in self.__datetime_to_window and not self.__should_ignore_empty_windows:
                    token.window_to_score[current_window] = 0
                elif datetime_pointer in self.__datetime_to_window and current_window not in token.window_to_score:
                    token.window_to_score[current_window] = 0

            datetime_pointer += timedelta(seconds=self.__granularity_seconds)
        self.__is_historical_data_finalized = True

    def __add_historical_document(self, obj: object) -> None:
        self.historical_data_finalized = False
        document: Document = self.__get_document_from_object(obj)

        windows: List[Window] = self.__get_chronological_windows_containing_datetime(document.time)

        if self.__earliest_window is None:
            self.__earliest_window = windows[0]
        if self.__latest_window is None:
            self.__latest_window = windows[-1]

        self.__earliest_window = min(self.__earliest_window, windows[0])
        self.__latest_window = max(self.__latest_window, windows[-1])

        for token_val in document.tokens:

            if token_val not in self.__token_val_to_token:
                self.__token_val_to_token[token_val] = Token(token_val)

            token_weight = document.supported_document_type.weight_scale(document, token_val)
            for window in windows:
                self.__token_val_to_token[token_val].window_to_score[window] += token_weight

    def __get_chronological_windows_containing_datetime(self, time: datetime) -> List[Window]:
        timestamp: float = time.timestamp()
        closest_window: Window = self.__get_nearest_window(timestamp)

        windows: List[Window] = [closest_window]

        current_window_start = closest_window.window_start_timestamp - self.__granularity_seconds
        while self.__window_size_seconds >= abs(current_window_start - timestamp):
            current_window_start_datetime: datetime = datetime.fromtimestamp(current_window_start)
            if current_window_start_datetime not in self.__datetime_to_window:
                self.__datetime_to_window[current_window_start_datetime] = Window(
                    current_window_start_datetime,
                    self.__window_size_seconds,
                )
            windows.append(self.__datetime_to_window[current_window_start_datetime])
            current_window_start -= self.__granularity_seconds

        return windows

    def __get_document_from_object(self, o: object):
        o_type: type = type(o)
        self.__ensure_type_supported(o_type)
        supported_document_type: SupportedDocumentType = self.__supported_types_dict[o_type]
        time, tokens = supported_document_type.interpreter(o)
        return Document(time=time, tokens=tokens, supported_document_type=supported_document_type)

    def __ensure_type_supported(self, t: type):
        if t not in self.__supported_types_dict:
            raise TypeError(f"No type support for {t}!")

    def __get_nearest_window(self, timestamp: float) -> Window:
        window_start_timestamp: float = timestamp - (timestamp % self.__granularity_seconds)
        window_start_datetime: datetime = datetime.fromtimestamp(window_start_timestamp)
        return Window(window_start_datetime, self.__window_size_seconds)

    @staticmethod
    def __find_closest_divisible_number(target, divisor):
        return target - (target % divisor)
