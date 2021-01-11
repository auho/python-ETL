from lib.common.base import APP
from lib.workflow.flow import tag_insert, tag_update
from lib.workflow.rule import tag_sole

db = APP.mysqlDb

"""
tag insert
"""
from lib.workflow.func.seg_words import FuncSegWords

func = FuncSegWords(key='x_content_name', table_name='x_content_words')
tag_insert.TagFlow.flow(db=db, table_name='xxxx', id_name='x_id', addition_fields=['x_keyid'], func=func)

from lib.workflow.func.split_to_words import FuncSplitToWords

func = FuncSplitToWords(key='x_content_name', sep=',', table_name='x_content_words')
tag_insert.TagFlow.flow(db=db, table_name='xxxx', id_name='x_id', addition_fields=['x_keyid'], func=func)

from lib.workflow.func.tag import FuncTagInsert

func = FuncTagInsert(key='x_content_name', rule=tag_sole.TagRule(db=db, name='x_rule_name'), table_name='x_tag_table_name')
tag_insert.TagFlow.flow(db=db, table_name='xxxx', id_name='x_id', addition_fields=['x_keyid'], func=func)

"""
tag update
"""
from lib.workflow.func.tag import FuncTagUpdate

source_func = FuncTagUpdate(key='x_content_name', rules=[tag_sole.TagRule(db=db, name='x_rule_name')])
content_func = FuncTagUpdate(
    key='x_content_name',
    rules=[
        tag_sole.TagRule(db=db, name='x_rule_name'),
        tag_sole.TagRule(db=db, name='x_rule_name')
    ]
)

tag_update.TagFlow.flow(db=db, table_name='xxxx', id_name='x_id', funcs=[source_func, content_func])
