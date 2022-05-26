class InvalidRowException(Exception):
  pass

class NotNullViolationException(InvalidRowException):
  pass

class BadGeometryException(InvalidRowException):
  pass

