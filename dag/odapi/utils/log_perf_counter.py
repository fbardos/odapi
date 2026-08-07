from contextlib import contextmanager
from time import perf_counter

from dagster import AssetExecutionContext


class LogTime:
    def __init__(self, context: AssetExecutionContext):
        self.context = context
        self.logger = context.log
        self.timings = {}

    @contextmanager
    def step(self, name: str):
        start = perf_counter()
        try:
            yield
        finally:
            elapsed = perf_counter() - start
            self.timings[name] = elapsed
            self.logger.info(f"{name}: {elapsed:.3f}s")
            self.context.add_output_metadata({f'time_sec__{name}': elapsed})
