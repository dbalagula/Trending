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


window_size_seconds: int = 4 * 60
step_size_seconds: int = 60

t = pyztrending.Trending(
    window_size_seconds=window_size_seconds,
    granularity_seconds=step_size_seconds,
)

t.add_class_support(MyObj, my_obj_interpreter, my_obj_weight_scale)

t.add_historical_documents([
    MyObj(datetime.fromtimestamp(1000000000), 5),
])

# for window, score in t._Trending__token_val_to_token[5].window_to_score.items():
#     print(
#         window,
#         score,
#     )

print(t.get_trending([
    MyObj(datetime.fromtimestamp(1000000000 + 2*step_size_seconds), 5),
    MyObj(datetime.fromtimestamp(1000000000 + 2*step_size_seconds), 5),
]))
