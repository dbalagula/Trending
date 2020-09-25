from datetime import datetime
from typing import Callable, List, Dict
from collections import defaultdict


class SupportedDocumentType:

    def __init__(self, t: type(type), interpreter: Callable, weight_function: Callable):
        self.type = t
        self.interpreter: Callable = interpreter
        self.weight_scale: Callable = weight_function


class Window:

    def __init__(self, start_timestamp: int, window_size_seconds: int):
        self.start_timestamp = start_timestamp
        self.end_timestamp = start_timestamp + window_size_seconds

    def __eq__(self, other):
        if type(other) == Window:
            return self.start_timestamp == other.start_timestamp
        return False

    def __hash__(self):
        return int(self.start_timestamp)

    def __lt__(self, other):
        return self.start_timestamp < other.start_timestamp

    def __str__(self):
        return f"{self.start_timestamp}, {self.end_timestamp}"


class Document:

    def __init__(self, time: datetime, tokens: List, supported_document_type: SupportedDocumentType):
        self.timestamp = time.timestamp()
        self.tokens = tokens
        self.supported_document_type = supported_document_type


class Token:

    def __init__(self, val):
        self.val = val
        self.window_to_score: defaultdict = defaultdict(float)
        self.empty_windows: List[Window] = []

    def add_document_to_window(self, window: Window, document: Document):
        self.window_to_score[window] += document.supported_document_type.weight_scale(
            document,
            self.val,
        )

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

    def get_scores_by_window(self) -> Dict[Window, float]:
        return self.window_to_score


class TokenStore:

    def __init__(self):
        self.token_dict: Dict[object, Token] = {}

    def values(self) -> List[object]:
        return list(self.token_dict.keys())

    def tokens(self) -> List[object]:
        return list(self.token_dict.values())

    def add(self, token_val: object):
        self.token_dict[token_val] = Token(token_val)

    def get(self, token_val: object) -> Token:
        return self.token_dict[token_val]

    def contains(self, token: object) -> bool:
        return token in self.token_dict

    def clear(self):
        self.token_dict = {}
