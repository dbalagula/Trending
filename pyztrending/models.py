from typing import Callable, List
from collections import defaultdict
from datetime import datetime, timedelta


class SupportedDocumentType:

    def __init__(self, t: type(type), interpreter: Callable, weight_function: Callable):
        self.type = t
        self.interpreter: Callable = interpreter
        self.weight_scale: Callable = weight_function


class Window:

    def __init__(self, window_start: datetime, window_size_seconds: int):
        self.window_start: datetime = window_start
        self.window_end: datetime = window_start + timedelta(seconds=window_size_seconds)

        self.window_start_timestamp = self.window_start.timestamp()
        self.window_end_timestamp = self.window_end.timestamp()

    def __eq__(self, other):
        if type(other) == Window:
            return self.window_start == other.window_start
        return False

    def __hash__(self):
        return int(self.window_start_timestamp)

    def __lt__(self, other):
        return self.window_start < other.window_start

    def __str__(self):
        return f"{self.window_start_timestamp}, {self.window_end_timestamp}"


class Token:

    def __init__(self, val):
        self.val = val
        self.window_to_score: defaultdict = defaultdict(float)
        self.empty_windows: List[Window] = []

    def __eq__(self, other):
        if type(other) == Token:
            return self.val == other.val

    def __hash__(self):
        return self.val.__hash__()

    def is_token_mentioned_in_window(self, window: Window) -> bool:
        return window in self.window_to_score

    def get_window_scores(self, should_ignore_empty_windows: bool) -> List[float]:
        num_empty_windows: int = 0 if should_ignore_empty_windows else len(self.empty_windows)
        return list(self.window_to_score.values()) + [0] * num_empty_windows


class Document:

    def __init__(self, time: datetime, tokens: List, supported_document_type: SupportedDocumentType):
        self.time = time
        self.tokens = tokens
        self.supported_document_type = supported_document_type
