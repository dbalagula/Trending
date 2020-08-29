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


t.add_class_support(MyObj, my_obj_interpreter, my_obj_weight_scale)

t.add_historical_documents([
    MyObj(datetime.fromtimestamp(1000000000), 5),
    MyObj(datetime.fromtimestamp(1000000000), 5),
    MyObj(datetime.fromtimestamp(1000000000), 5),
    MyObj(datetime.fromtimestamp(1000000000), 5),
    MyObj(datetime.fromtimestamp(1000000000+7*60), 7),
])

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
