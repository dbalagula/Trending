import unittest
from datetime import datetime

from pyztrending import Trending
from pyztrending.exceptions import NonNormalDistributionError, DocumentTimeError


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


example_val: int = 5
example_time: datetime = datetime.now()
example_my_supported_class_object: MySupportedType = MySupportedType(example_val, example_time)
example_my_unsupported_class_object: MyUnsupportedType = MyUnsupportedType(example_val, example_time)


class TestPyZTrending(unittest.TestCase):

    WINDOW_SIZE_SIXTY_SECONDS: int = 60
    STEP_SIZE_FIFTEEN_SECONDS: int = 15
    SHOULD_IGNORE_EMPTY_WINDOWS_FALSE: bool = False

    def test_add_class_support(self):

        trending = Trending(
            granularity_seconds=TestPyZTrending.STEP_SIZE_FIFTEEN_SECONDS,
            window_size_seconds=TestPyZTrending.WINDOW_SIZE_SIXTY_SECONDS,
            should_ignore_empty_windows=TestPyZTrending.SHOULD_IGNORE_EMPTY_WINDOWS_FALSE,
        )

        trending.add_type_support(
            MySupportedType,
            my_supported_class_interpreter,
            my_supported_class_weight_scale
        )

        # Try adding an object of a supported type
        trending.add_historical_documents([example_my_supported_class_object])

        try:
            # Try adding an object of an unsupported type, an exception should be thrown
            trending.add_historical_documents([example_my_unsupported_class_object])
        except TypeError:
            pass
        else:
            self.fail("No TypeError thrown for unsupported type!")

    def test_tokens(self):

        trending = Trending(
            granularity_seconds=TestPyZTrending.STEP_SIZE_FIFTEEN_SECONDS,
            window_size_seconds=TestPyZTrending.WINDOW_SIZE_SIXTY_SECONDS,
            should_ignore_empty_windows=TestPyZTrending.SHOULD_IGNORE_EMPTY_WINDOWS_FALSE,
        )

        trending.add_type_support(
            MySupportedType,
            my_supported_class_interpreter,
            my_supported_class_weight_scale
        )

        trending.add_historical_documents([example_my_supported_class_object])

        self.assertEqual(
            trending._tokens,
            {example_val},
        )

    def test_enforce_granularity_parameter(self):
        try:
            Trending(
                granularity_seconds=None,
                window_size_seconds=TestPyZTrending.WINDOW_SIZE_SIXTY_SECONDS,
                should_ignore_empty_windows=TestPyZTrending.SHOULD_IGNORE_EMPTY_WINDOWS_FALSE,
            )
        except TypeError:
            pass
        else:
            self.fail("Expected TypeError for incorrect granularity type!")

        try:
            Trending(
                granularity_seconds=0,
                window_size_seconds=TestPyZTrending.SHOULD_IGNORE_EMPTY_WINDOWS_FALSE,
                should_ignore_empty_windows=TestPyZTrending.SHOULD_IGNORE_EMPTY_WINDOWS_FALSE,
            )
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError for when granularity is not a positive integer!")

        try:
            Trending(
                granularity_seconds=TestPyZTrending.SHOULD_IGNORE_EMPTY_WINDOWS_FALSE + 1,
                window_size_seconds=TestPyZTrending.SHOULD_IGNORE_EMPTY_WINDOWS_FALSE,
                should_ignore_empty_windows=TestPyZTrending.SHOULD_IGNORE_EMPTY_WINDOWS_FALSE,
            )
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError for when granularity is greater than window size!")

    def test_enforce_window_size_parameter(self):
        try:
            Trending(
                granularity_seconds=TestPyZTrending.STEP_SIZE_FIFTEEN_SECONDS,
                window_size_seconds=None,
                should_ignore_empty_windows=TestPyZTrending.SHOULD_IGNORE_EMPTY_WINDOWS_FALSE,
            )
        except TypeError:
            pass
        else:
            self.fail("Expected TypeError for incorrect window size type!")

        try:
            Trending(
                granularity_seconds=TestPyZTrending.STEP_SIZE_FIFTEEN_SECONDS,
                window_size_seconds=0,
                should_ignore_empty_windows=TestPyZTrending.SHOULD_IGNORE_EMPTY_WINDOWS_FALSE,
            )
        except ValueError:
            pass
        else:
            self.fail("Expected ValueError for when window size is not a positive integer!")

    def test_non_normal_distribution_throws_exception(self):

        trending = Trending(
            granularity_seconds=TestPyZTrending.STEP_SIZE_FIFTEEN_SECONDS,
            window_size_seconds=TestPyZTrending.WINDOW_SIZE_SIXTY_SECONDS,
            should_ignore_empty_windows=TestPyZTrending.SHOULD_IGNORE_EMPTY_WINDOWS_FALSE,
        )

        trending.add_type_support(
            MySupportedType,
            my_supported_class_interpreter,
            my_supported_class_weight_scale,
        )

        trending.add_historical_documents([
            example_my_supported_class_object,
            example_my_supported_class_object,
        ])

        arbitrary_future_time_in_seconds: int = 10000
        later_time: datetime = datetime.fromtimestamp(datetime.now().timestamp() + arbitrary_future_time_in_seconds)
        later_time_my_supported_class_object: MySupportedType = MySupportedType(
            example_val,
            later_time,
        )

        try:
            trending.get_trending([
                later_time_my_supported_class_object,
                later_time_my_supported_class_object,
            ])
        except NonNormalDistributionError:
            pass
        else:
            self.fail("NonNormalDistributionError was not thrown when it was expected!")

    def test_older_document_throws_exception(self):

        trending = Trending(
            granularity_seconds=TestPyZTrending.STEP_SIZE_FIFTEEN_SECONDS,
            window_size_seconds=TestPyZTrending.WINDOW_SIZE_SIXTY_SECONDS,
            should_ignore_empty_windows=TestPyZTrending.SHOULD_IGNORE_EMPTY_WINDOWS_FALSE,
        )

        trending.add_type_support(
            MySupportedType,
            my_supported_class_interpreter,
            my_supported_class_weight_scale,
        )

        trending.add_historical_documents([
            example_my_supported_class_object,
            example_my_supported_class_object,
        ])

        arbitrary_past_time_in_seconds: int = 10000
        past_time: datetime = datetime.fromtimestamp(datetime.now().timestamp() - arbitrary_past_time_in_seconds)
        past_time_my_supported_class_object: MySupportedType = MySupportedType(
            example_val,
            past_time,
        )

        try:
            trending.get_trending([
                past_time_my_supported_class_object,
            ])
        except DocumentTimeError:
            pass
        else:
            self.fail("DocumentTimeError was not thrown when it was expected!")
