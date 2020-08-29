from typing import Callable, List
from datetime import datetime, timedelta
from collections import defaultdict


class SupportedDocumentType:

    def __init__(self, t: type(type)):
        self.type = t
        self.interpreter: Callable = None
        self.weight_scale: Callable = None


class Window:

    def __init__(self, window_start: datetime, window_size_seconds: int):
        self.window_start: datetime = window_start
        self.window_start_timestamp = self.window_start.timestamp()
        self.window_end: datetime = window_start + timedelta(seconds=window_size_seconds)

    def __eq__(self, other):
        if type(other) == Window:
            return self.window_start == other.window_start
        return False

    def __hash__(self):
        return int(self.window_start_timestamp)

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
