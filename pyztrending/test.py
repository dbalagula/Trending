import pyztrending
from datetime import datetime


class MyObj:

    def __init__(self, time: datetime, val: int):
        self.time = time
        self.val = val


def my_obj_interpreter(obj: MyObj):
    return obj.time, [obj.val]


def my_obj_weight_scale(obj: MyObj, val: int):
    return 1


t = pyztrending.Trending(window_size_seconds=15 * 60, granularity_seconds=2 * 60, ignore_empty_windows=False)


t.add_interpreter(MyObj, my_obj_interpreter, my_obj_weight_scale)

t.add_historical_document(MyObj(datetime.fromtimestamp(1000000000), 5))
t.add_historical_document(MyObj(datetime.fromtimestamp(1000000000), 5))
t.add_historical_document(MyObj(datetime.fromtimestamp(1000000000), 5))
t.add_historical_document(MyObj(datetime.fromtimestamp(1000000000), 5))
t.add_historical_document(MyObj(datetime.fromtimestamp(1000000000+7*60), 7))

t.finalize_historical_data(t.earliest_window, pyztrending.Window(datetime.fromtimestamp(1000000000 + 1500 * 60), t.window_size_seconds))

print(t.get_trending([
    MyObj(datetime.fromtimestamp(1000000000), 5),
    MyObj(datetime.fromtimestamp(1000000000), 5),
    MyObj(datetime.fromtimestamp(1000000000), 5),
    MyObj(datetime.fromtimestamp(1000000000), 5),
    MyObj(datetime.fromtimestamp(1000000000), 5),
    MyObj(datetime.fromtimestamp(1000000000), 5),
    MyObj(datetime.fromtimestamp(1000000000), 5),
    MyObj(datetime.fromtimestamp(1000000000), 5),

    MyObj(datetime.fromtimestamp(1000000000), 7),
    MyObj(datetime.fromtimestamp(1000000000), 7),
    MyObj(datetime.fromtimestamp(1000000000), 7),

]))
