from lib.dataflow import process
from lib.dataflow import mysql
from ..action import seg_words

"""
seg_words.TagFlow.flow(id_name='id_name', keyid='keyid', content_name='content_name', db=db, table_name='table_name')
"""


class TagFlow:
    @staticmethod
    def flow(db, table_name, id_name, keyid, tag_content_name, seg_rule=None, database_name=None, addition_fields=None):
        """

        :param db:
        :param table_name:
        :param id_name:
        :param keyid:
        :param tag_content_name:
        :param seg_rule:
        :param database_name:
        :param addition_fields:
        :return:
        """
        action = seg_words.Action(keyid=keyid, content_name=tag_content_name, db=db, table_name=table_name + '_words', database_name=database_name,
                                  addition_fields=addition_fields)
        action.add_rule(seg_rule=seg_rule)
        action.set_do_func_name(func_name='do_flag')

        fields = [id_name, keyid, tag_content_name]
        if addition_fields:
            fields = fields + addition_fields

        fields = list(set(fields))

        dp = mysql.DataProvide(db=db, table_name=table_name, id_name=id_name, database_name=database_name, fields=fields, read_page_size=1000,
                               last_id=0)

        process.DispatchCenter.dispatch(dp=dp, actions=[action])
