# Account
class Account(object):
  id = ''
  name = ''

# Property
class Property(object):
  id = ''
  name = ''
  account = Account()

# View
class View(object):
  id = ''
  name = ''
  account = Account()
  property = Property()

class View_Template(object):
  ids = ""
  start_date = object()
  end_date = object()
  metrics = ""
  dimensions = ""
  orderBys = ""
  filters = ""
  

