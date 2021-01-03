from lib.common import base

db = base.APP.mysqlDb

"""
tag multi insert
"""
from lib.tool.tag_table import tag_insert

# build table
tmi = tag_insert.TagTable(source_table_name='xxxx', keyid='content_id', tag_name='a', tags=['a1', 'a2', 'a3'])
tmi.build(db=db)

from lib.workflow.flow import tag_insert
from lib.workflow.rule import tag_multi

# tag
rule = tag_multi.TagRule(db=db, table_name='rule_xx', keyword_name='xx_keyword', tags_name=['a1', 'a2', 'a3'])

tag_insert.TagFlow.flow(db=db, table_name='xxxx', id_name='content_id', keyid='content_id', tag_name='a',
                              tag_content_name='content', tag_rule=rule, addition_fields=['y1'])
