from lib.common import base

db = base.APP.mysqlDb

"""
seg words
"""
from lib.tool.tag_table import seg_words

# build table
tw = seg_words.TagTable(source_table_name='xxxx', content_name="content", keyid='content_id')
tw.DDLWords.add_string(name='y1')
tw.build(db=db)

from lib.workflow.flow import seg_words

# seg word
seg_words.TagFlow.flow(db=db, table_name='xxxx', id_name='content_id', keyid='content_id', tag_content_name='title',
                       addition_fields=['y1'])
