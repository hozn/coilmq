import threading
import time

from coilmq.util.concurrency import CoilThreadingTimer


class TestCoilThreadingTimer:
    def setup_method(self, method):
        class CountedCallback:
            def __init__(self):
                self.lock = threading.Lock()
                self.n_called = 0

            def __call__(self, *args, **kwargs):
                with self.lock:
                    self.n_called += 1

        self.counter = CountedCallback

    def test_periodic_callback(self):
        period = 0.1
        factor = 10
        counter = self.counter()
        timer = CoilThreadingTimer()
        timer.schedule(period, counter)
        with timer:
            time.sleep(period * factor)
        assert abs(counter.n_called - factor) < factor * 0.5, (
            "Should provide 50% accuracy"
        )
