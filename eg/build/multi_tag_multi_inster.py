from lib.common import base

db = base.APP.mysqlDb

"""
multi tag multi insert
"""

# build table
from lib.tool.tag_table import tag_insert

tag_list = ['b', 'c', 'd']
for tag_name in tag_list:
    tmi = tag_insert.TagTable(source_table_name='xxxx', keyid='content_id', tag_name=tag_name)
    tmi.DDLTag.add_string(name='x1', length=10)
    tmi.DDLTag.add_id(name='x2')
    tmi.build(db=db)

# tag
from lib.workflow.flow import tag_insert

tags_name = {
    'b': 'content',
    'c': 'content',
    'd': 'content',
}

tag_insert.TagFlow.flow_multi_tag(db=db, table_name='xxxx', tag_items=tags_name, id_name='content_id',
                                        keyid='content_id', addition_fields=['x1', 'x2'])
