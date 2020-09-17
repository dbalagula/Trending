import unittest
from datetime import datetime

from pyztrending import Trending


class MySupportedType:

    def __init__(self, val: int, time: datetime):
        self.val = val
        self.time = time


class MyUnsupportedType:

    def __init__(self, val: int, time: datetime):
        self.val = val
        self.time = time


def my_supported_class_interpreter(obj: MySupportedType):
    return obj.time, [obj.val]


def my_supported_class_weight_scale(obj: MySupportedType, val: int):
    return 1


class TestPyZTrending(unittest.TestCase):

    DEFAULT_WINDOW_SIZE_SECONDS: int = 60
    DEFAULT_STEP_SIZE_SECONDS: int = 15
    DEFAULT_SHOULD_IGNORE_EMPTY_WINDOWS: bool = False

    def test_add_class_support(self):

        trending = Trending(
            granularity_seconds=TestPyZTrending.DEFAULT_STEP_SIZE_SECONDS,
            window_size_seconds=TestPyZTrending.DEFAULT_WINDOW_SIZE_SECONDS,
            should_ignore_empty_windows=TestPyZTrending.DEFAULT_SHOULD_IGNORE_EMPTY_WINDOWS,
        )

        trending.add_type_support(
            MySupportedType,
            my_supported_class_interpreter,
            my_supported_class_weight_scale
        )

        example_val: int = 5
        example_time: datetime = datetime.now()
        example_my_supported_class_object: MySupportedType = MySupportedType(example_val, example_time)

        # Try adding an object of a supported type
        trending.add_historical_documents([example_my_supported_class_object])

        example_my_unsupported_class_object: MyUnsupportedType = MyUnsupportedType(example_val, example_time)
        try:
            # Try adding an object of an unsupported type, an exception should be thrown
            trending.add_historical_documents([example_my_unsupported_class_object])
        except TypeError:
            pass
        else:
            self.fail("No TypeError thrown for unsupported type!")

    def test_tokens(self):

        trending = Trending(
            granularity_seconds=TestPyZTrending.DEFAULT_STEP_SIZE_SECONDS,
            window_size_seconds=TestPyZTrending.DEFAULT_WINDOW_SIZE_SECONDS,
            should_ignore_empty_windows=TestPyZTrending.DEFAULT_SHOULD_IGNORE_EMPTY_WINDOWS,
        )

        trending.add_type_support(
            MySupportedType,
            my_supported_class_interpreter,
            my_supported_class_weight_scale
        )

        example_val: int = 5
        example_time: datetime = datetime.now()
        example_my_supported_class_object: MySupportedType = MySupportedType(example_val, example_time)

        trending.add_historical_documents([example_my_supported_class_object])

        self.assertEqual(
            trending.tokens,
            {example_val},
        )
