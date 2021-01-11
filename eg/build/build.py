from lib.common.base import APP

db = APP.mysqlDb

"""
seg words
"""
from lib.tool.table_ddl import seg_words

table = seg_words.Table(table_name='xxxx', content_name="x_content_name", keyid='x_keyid')
table.DDLTable.add_string(name='y1')
table.build(db=db)

"""
split to words
"""
from lib.tool.table_ddl import split_to_words

table = split_to_words.Table(table_name='xxxx', content_name='x_content_name', keyid='x_keyid')
table.DDLTable.add_string(name='y1')
table.build(db=db)

"""
tag insert
"""
from lib.tool.table_ddl import tag_insert

table = tag_insert.Table(table_name='xxxx', keyid='x_keyid', tag_name='x_tag_name', tags=['x1', 'x2'])
table.DDLRule.add_string(name='y1')
table.build(db=db)

"""
tag update
"""
from lib.tool.table_ddl import tag_update

table = tag_update.Table(table_name='xxxx', tag_name='x_tag_name', tags=['x1', 'x2'])
table.build(db=db)
