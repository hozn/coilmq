import time
from unittest.mock import Mock

from coilmq.util.concurrency import CoilThreadingTimer


class TestCoilThreadingTimer:
    def test_periodic_callback(self):
        period = 0.1
        factor = 10
        counter = Mock()
        timer = CoilThreadingTimer()
        timer.schedule(period, counter)
        with timer:
            time.sleep(period * factor)
        assert abs(counter.call_count - factor) < factor * 0.5, (
            "Should provide 50% accuracy"
        )
