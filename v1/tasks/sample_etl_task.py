import pandas as pd
from sklearn.datasets import fetch_california_housing
from v1.common import Task


class SampleETLTask(Task):
    def _write_data(self):
        print ("Working??")

    def launch(self):
        self.logger.info("Launching sample ETL task")
        self._write_data()
        self.logger.info("Sample ETL task finished!")

# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    task = SampleETLTask()
    task.launch()

# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == '__main__':
    entrypoint()
