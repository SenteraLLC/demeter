import os
import logging

from datetime import datetime

did_setup = False

def setupLogger(output_dir: str = "/tmp/") -> bool:
  t = datetime.now().strftime("%m-%d %H:%M:%S")

  f = os.path.join(output_dir, "demeter.explorer.{t}.log")

  logging.basicConfig(filename=f,
                      filemode='w',
                      format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                      datefmt='%H:%M:%S',
                      level=logging.DEBUG
                     )
  return True


def getLogger(name : str = "demeter_explorer") -> logging.Logger:
  if not did_setup:
    setupLogger()
  return logging.getLogger(name)
