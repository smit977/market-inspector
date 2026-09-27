"""Fail-stop supervision for Kafka calls that can stall during host suspend/rebalance."""
import os
import threading
import time
import structlog

class ProgressWatchdog:
    """The WAL/outbox makes abrupt restart safe; no input offset is advanced here."""
    def __init__(self,timeout_seconds=90,clock=time.monotonic,terminate=os._exit):
        self.timeout=timeout_seconds;self.clock=clock;self.terminate=terminate
        self.last_progress=clock();self.stopped=threading.Event();self.thread=None
    def touch(self): self.last_progress=self.clock()
    def check(self):
        if self.clock()-self.last_progress>=self.timeout:
            structlog.get_logger().error('alert_worker_stalled',timeout_seconds=self.timeout,action='restart_from_durable_checkpoint')
            self.terminate(1)
            return False
        return True
    def start(self):
        def monitor():
            while not self.stopped.wait(1):
                if not self.check(): break
        self.thread=threading.Thread(target=monitor,name='alert-progress-watchdog',daemon=True)
        self.thread.start()
    def close(self):
        self.stopped.set()
        if self.thread:self.thread.join(timeout=2)
