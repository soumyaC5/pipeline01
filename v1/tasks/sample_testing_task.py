from v1.common import Task

class SampleTestTask(Task):
  def _write_data(self,a,b):
        print (a+b)

  def launch(self):
        a,b = 5,6
        self.logger.info("Launching sample testing task")
        self._write_data(a,b)
        self.logger.info("Sample tesing task finished!")

# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    task = SampleTestTask()
    task.launch()

# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == '__main__':
    entrypoint()