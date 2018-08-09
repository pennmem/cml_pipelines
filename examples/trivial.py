from functools import reduce
import operator
import random

from dask import delayed

from cml_pipelines import Pipeline


class MyPipeline(Pipeline):
    # Define a task using the delayed decorator
    @delayed
    def generate_datapoint(self):
        """Generate a single data point."""
        return random.random()

    def build(self):
        """Build the pipeline."""
        data = [self.generate_datapoint() for _ in range(1000)]

        # inline the delayed function to make an existing function into a task
        total = delayed(reduce)(operator.add, data)

        # return the final Delayed instance
        return total


# run the pipeline
pipeline = MyPipeline()
result = pipeline.run()
print(result)
