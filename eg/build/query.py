from lib.common import base

query = base.QUERY
query.DEBUG = 0

query.init(excel_name='excel_name')

sql = f"xxx"
query.get(name="sheet_name", sql=sql)

query.save()
