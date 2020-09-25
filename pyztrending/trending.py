from statistics import stdev
from typing import Optional, Dict, Callable, List, Iterable, Set, Tuple

from pyztrending.exceptions import NonNormalDistributionError, DocumentTimeError
from pyztrending.models import SupportedDocumentType, Window, Token, Document, TokenStore


class Trending:

    def __init__(self,
                 window_size_seconds: int,
                 granularity_seconds: int,
                 should_ignore_empty_windows: bool = False,
                 ):

        if not isinstance(window_size_seconds, int) or not isinstance(granularity_seconds, int):
            raise TypeError("window_size_seconds and granularity_seconds need to be both type 'int'!")
        elif window_size_seconds <= 0 or granularity_seconds <= 0:
            raise ValueError("window_size_seconds and granularity_seconds need to be both a positive integer!")
        elif granularity_seconds > window_size_seconds:
            raise ValueError("Can't have step size be greater than window size!")

        self.__window_size_seconds: int = window_size_seconds
        self.__granularity_seconds: int = granularity_seconds
        self.__should_ignore_empty_windows: bool = should_ignore_empty_windows

        self.__earliest_window: Optional[Window] = None
        self.__latest_window: Optional[Window] = None

        self.__supported_types_dict: Dict[type, SupportedDocumentType] = {}
        self.__timestamp_to_window: Dict[int, Window] = {}
        self.__token_store: TokenStore() = TokenStore()

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
        self.__token_store.clear()
        self.add_historical_documents(objects)
        return trending

    def get_trending(self, objects: List) -> List:

        # Need to think about how to handle current data that exists in more than one window

        if not self.__is_historical_data_finalized:
            self.__finalize_historical_data(
                self.__earliest_window,
                self.__latest_window,
            )

        current_token_store: TokenStore = TokenStore()
        trending_by_window: Dict[Tuple[int, int], float] = []

        for document in [self.__get_document_from_object(obj) for obj in objects]:
            if document.timestamp < self.__latest_window.start_timestamp:
                raise DocumentTimeError("Provided document in trending data set that is older than historical data!")
            current_window: Window = self.__get_nearest_window(document.timestamp)
            for token_val in filter(lambda t: self.__token_store.contains(t), document.tokens):
                if not current_token_store.contains(token_val):
                    Trending.__move_token(self.__token_store, current_token_store, token_val)
                token: Token = current_token_store.get(token_val)
                token.add_document_to_window(current_window, document)

        for token in current_token_store.tokens():
            for window, score in token.get_scores_by_window():
                trending_by_window[(window.window_start_timestamp, window.window_end_timestamp)] = \
                    self.__get_zscore_for(token, score)

        return trending_by_window

    @property
    def _tokens(self) -> Set[Token]:
        return set(self.__token_store.values())

    def __get_zscore_for(self, token: Token, score: float):
        token_scores: List[float] = token.get_window_scores(
            should_ignore_empty_windows=self.__should_ignore_empty_windows
        )
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

        current_window: Window = Window(
            start_timestamp=earliest_window.start_timestamp,
            window_size_seconds=self.__window_size_seconds,
        )

        while current_window < latest_window:

            for token in self._tokens:

                is_current_window_empty: bool = not self.__are_any_tokens_in_window(current_window)

                is_this_token_not_present_but_others_are: bool = not is_current_window_empty and \
                    token.is_token_mentioned_in_window(current_window)

                if (is_current_window_empty and not self.__should_ignore_empty_windows) or \
                        is_this_token_not_present_but_others_are:
                    token.empty_windows.append(current_window)
                    token.window_to_score[current_window] = 0

            current_window: Window = Window(
                start_timestamp=earliest_window.start_timestamp + self.__granularity_seconds,
                window_size_seconds=self.__window_size_seconds,
            )

        self.__is_historical_data_finalized = True

    def __add_historical_document(self, obj: object) -> None:
        self.historical_data_finalized = False
        document: Document = self.__get_document_from_object(obj)

        windows: List[Window] = self.__get_chronological_windows_containing_timestamp(document.timestamp)

        if self.__earliest_window is None:
            self.__earliest_window = windows[0]
        if self.__latest_window is None:
            self.__latest_window = windows[-1]

        self.__earliest_window = min(self.__earliest_window, windows[0])
        self.__latest_window = max(self.__latest_window, windows[-1])

        for token_val in document.tokens:

            if not self.__token_store.contains(token_val):
                self.__token_store.add(token_val)
            token: Token = self.__token_store.get(token_val)
            for window in windows:
                token.add_document_to_window(window, document)

    def __get_chronological_windows_containing_timestamp(self, timestamp: int) -> List[Window]:
        closest_window: Window = self.__get_nearest_window(timestamp)

        windows: List[Window] = [closest_window]

        current_window_start = closest_window.start_timestamp - self.__granularity_seconds
        while self.__window_size_seconds >= abs(current_window_start - timestamp):
            if current_window_start not in self.__timestamp_to_window:
                self.__timestamp_to_window[current_window_start] = Window(
                    current_window_start,
                    self.__window_size_seconds,
                )
            windows.append(self.__timestamp_to_window[current_window_start])
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

    def __get_nearest_window(self, timestamp: int) -> Window:
        window_start_timestamp: float = timestamp - (timestamp % self.__granularity_seconds)
        return Window(window_start_timestamp, self.__window_size_seconds)

    def __are_any_tokens_in_window(self, window: Window) -> bool:
        return window.start_timestamp not in self.__timestamp_to_window

    @staticmethod
    def __move_token(from_token_store: TokenStore, to_token_store: TokenStore, token_val: object):
        if to_token_store.contains(token_val):
            raise ValueError("Trying to move token to TokenStore that already contains it!")
        to_token_store.token_dict[token_val] = from_token_store.token_dict[token_val]
