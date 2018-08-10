import logging
import time
from typing import List

from dask import delayed
import numpy as np

from cml_pipelines import Pipeline
from cml_pipelines.log import LogConsumerThread


class MyPipeline(Pipeline):
    def __init__(self, log_address: str):
        self.pipeline_id = "MyPipeline"
        self.log_address = log_address

    @property
    def logger(self):
        return self.get_logger(self.log_address)

    @delayed
    def load_data(self) -> np.ndarray:
        sleep_time = np.random.uniform(0.1, 2)
        self.logger.info("Sleeping for %f s...", sleep_time)
        time.sleep(sleep_time)
        return np.random.normal(size=(100, 100))

    @delayed
    def compute_mean(self, data: List[np.ndarray]) -> np.ndarray:
        self.logger.info("Computing mean...")
        return np.mean(data)

    def build(self):
        data = [self.load_data() for _ in range(100)]
        return self.compute_mean(data)


if __name__ == "__main__":
    logger = logging.getLogger("cml")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(name)s:%(levelname)s:%(asctime)s] %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    address = "tcp://127.0.0.1:9777"

    with LogConsumerThread(address=address) as log_consumer:
        pipeline = MyPipeline(address)
        pipeline.run()
