import re


def symbol_underline_fun(keyword, gap=5):
    return keyword.replace('_', '.{0,%s}' % gap)


def strict_en_fun(keyword):
    """
    英文名称需要正则保证前后无英文字母

    :param keyword:
    :return:
    """

    pattern_all_en = re.compile(r'[^\w+._\s()]', re.A)
    items = re.findall(pattern_all_en, keyword)

    is_all_en = True
    if len(items) > 0:
        is_all_en = False

    keyword_regex = keyword
    if is_all_en:
        keyword_regex = '^' + keyword + '(?=[^A-Za-z-])' + '|' \
                        + '(?<=[^0-9A-Za-z-])' + keyword + '(?=[^A-Za-z-])' + '|' + \
                        '(?<=[^0-9A-Za-z-])' + keyword + '$'

    return is_all_en, keyword_regex


def filter_regex_syntax_fun(keyword):
    # bad_strings = ['.', '*', '+', '?', '|', '[', ']', '(', ')']
    # bad_regexp = ''.join([f'\{x}' for x in bad_strings])
    #
    # keyword = re.sub(f'({bad_regexp})', '\\g<1>', keyword)

    keyword = keyword.replace('*', '\*')
    keyword = keyword.replace('[', '\[')
    keyword = keyword.replace(']', '\]')
    keyword = keyword.replace('(', '\(')
    keyword = keyword.replace(')', '\)')
    keyword = keyword.replace('?', '\?')
    keyword = keyword.replace('+', '\+')
    keyword = keyword.replace('.', '\.')
    keyword = keyword.replace('|', '\|')

    return keyword


class TagRule:
    """
    单字段打标签
    """

    def __init__(self, db, name=None, complete_keyword_name=None, complete_tags_name=None, complete_table_name=None, alias=None,
                 fixed_tags=None, exclude_tags=None, keyword_fun_list=None, database_name=None):
        """

        :param db: 数据库
        :param name:
        :param complete_table_name: rule list 表
        :param complete_keyword_name: 匹配的关键词
        :param complete_tags_name: 标签名称 ['tag1', 'tag2']
        :param alias: 标签别名 {'name':'alias'} name -> keyword table name； alias -> content table name
        :param fixed_tags: {name: vale}
        :param exclude_tags:
        :param database_name:
        """

        self._db = db
        self._name = name
        self._tableName = complete_table_name
        self._keywordName = complete_keyword_name
        self._tagsName = complete_tags_name  # type: list
        self._alias = alias  # type: dict
        self._fixed_tags = fixed_tags  # type: dict
        self._exclude_tags = exclude_tags  # type:list
        self._keywordFunList = keyword_fun_list  # type: list
        self._databaseName = database_name

        self._fieldsAlias = {}  # type: dict
        self._tagRuleList = None  # type: list
        self._regexString = ''
        self._pattern = None
        self._tagsDict = {}
        self._regexGroupName = '_rEgEx_'
        self._badKeywordDict = {}
        self._fixedName = []  # type: list

        self.main()

    def _check(self):
        if self._name:
            self._tableName = 'rule_' + self._name
            self._keywordName = self._name + '_keyword'

            columns = self._db.get_table_columns(table_name=self._tableName, database_name=self._databaseName)
            if not columns:
                raise Exception('rule table columns is error!')

            self._tagsName = [x for x in columns if x.find(self._name) == 0 and x != self._keywordName]

        if not self._tableName:
            raise Exception("table name IS ERROR!")

        if not self._keywordName:
            raise Exception("keyword filed name IS ERROR!")

        if not self._keywordFunList:
            self._keywordFunList = []

        if self._databaseName:
            self._tableName = f"`{self._databaseName}`.`{self._tableName}`"
        else:
            self._tableName = f"`{self._tableName}`"

        if not self._alias:
            self._alias = {}

        if not self._fixed_tags:
            self._fixed_tags = {}

        self._fieldsAlias[self._keywordName] = self._get_alias(self._keywordName)
        self._keywordName = self._fieldsAlias[self._keywordName]

        for index, tag_name in enumerate(self._tagsName):
            if self._exclude_tags and tag_name.replace(self._name + '_', '') in self._exclude_tags:
                self._tagsName.pop(index)
                continue

            self._fieldsAlias[tag_name] = self._get_alias(tag_name)
            self._tagsName[index] = self._fieldsAlias[tag_name]

        for name in self._fixed_tags.keys():
            self._fixedName.append(name)

        for tag_name in self._tagsName:
            self._tagsDict[tag_name] = {}

    def _get_tag_rule_list(self):
        """
        获取规则数据
        :return:
        """
        field_list_select_string = ','.join([f"`{t}` AS `{ta}`" for t, ta in self._fieldsAlias.items()])

        sql = f'SELECT {field_list_select_string} FROM {self._tableName} ORDER BY `keyword_len` DESC, `id` ASC'
        result = self._db.get_all(sql)
        self._tagRuleList = result

    def _generate_regex(self):
        """
        获取 tag rule 规则数据，转换为正则表达式
        如果是全英文字符规则，加上前后限定（前后不能为数字、英文，也就是完成匹配英文）

        :return:
        """

        regex_list = []
        group_regex_list = []
        for tag_rule in self._tagRuleList:
            keyword = tag_rule[self._keywordName].strip()
            if not keyword:
                continue

            is_all_en, keyword_regex = strict_en_fun(keyword=keyword)

            keyword_regex = filter_regex_syntax_fun(keyword)

            if not is_all_en:
                if self._keywordFunList:
                    for keyword_fun in self._keywordFunList:
                        res = keyword_fun(keyword_regex)
                        if res:
                            keyword_regex = res

            if keyword_regex == keyword:
                regex_list.append(keyword_regex)
            else:
                if is_all_en:
                    keyword = self._store_english_bad_keyword(keyword=keyword)
                else:
                    keyword = self._store_chinese_bad_keyword(keyword=keyword)

                keyword_regex = f'(?P<{keyword}>{keyword_regex})'
                group_regex_list.append(keyword_regex)

        if regex_list:
            group_regex_list.append(f'(?P<{self._regexGroupName}>' + '|'.join(regex_list) + ')')

        self._regexString = '|'.join(group_regex_list)
        self._pattern = re.compile(rf'{self._regexString}')

    def _generate_tags_dict(self):
        """
        把规则变成字典，方便后续根据 keyword 查找 tag
        :return:
        """

        if not self._tagsName:
            return

        assert_tag_name = self._tagsName[0]

        for tag_rule in self._tagRuleList:
            tag_keyword = tag_rule[self._keywordName]

            if tag_keyword not in self._tagsDict[assert_tag_name]:
                for tag_name in self._tagsName:
                    self._tagsDict[tag_name][tag_keyword] = tag_rule[tag_name]

    def _get_alias(self, tag_name):
        if tag_name in self._alias:
            return self._alias[tag_name]
        else:
            return tag_name

    def _add_fixed_tags_values(self, item):
        fixed = tuple()
        for value in self._fixed_tags.keys():
            fixed = fixed + (value,)

        if fixed:
            return item + fixed
        else:
            return item

    def _get_insert_fields(self):
        return self._tagsName + self._fixedName

    def _tag(self, content):
        keywords = self._tag_content(content=content)
        if not keywords:
            return None

        return self._generate_frequency(keywords=keywords)

    def _tag_update(self, content):
        keywords_frequency = self._tag(content=content)
        if not keywords_frequency:
            return False

        keyword = self._decide_sole_keyword(keywords_frequency=keywords_frequency)

        return self._generate_keyword_update_item(keyword=keyword)

    def _tag_insert(self, content):
        keywords_frequency = self._tag(content=content)
        if not keywords_frequency:
            return False

        keyword = self._decide_sole_keyword(keywords_frequency=keywords_frequency)
        item = [self._generate_keyword_insert_item(keyword=keyword)]

        return item

    def _tag_multi_insert(self, content):
        """
        返回多条
        """
        keywords_frequency = self._tag(content=content)
        if not keywords_frequency:
            return False

        items = []
        for keyword, num in keywords_frequency.items():
            item = self._generate_keyword_num_insert_item(keyword=keyword, num=num)
            items.append(item)

        return items

    def _tag_content(self, content):
        """
        根据内容进行正则匹配，返回所有 keyword

        :param content: 内容
        :return: keywords
        """
        if not content:
            return False

        keywords = []
        match_items = self._pattern.finditer(content)
        for match_item in match_items:
            for group_name, group_match in match_item.groupdict().items():
                if group_match is None:
                    continue

                if group_name == self._regexGroupName:
                    group_name = group_match

                group_name = self._restore_bad_keyword(good_keyword=group_name)

                keywords.append(group_name)

        if len(keywords) <= 0:
            return False

        return keywords

    def _generate_keyword_num_insert_item(self, keyword, num):
        tag_item = (keyword, num)
        for tag_name in self._tagsName:
            tag_item = tag_item + (self._tagsDict[tag_name][keyword],)

        return self._add_fixed_tags_values(item=tag_item)

    def _generate_keyword_insert_item(self, keyword):
        tag_item = (keyword,)
        for tag_name in self._tagsName:
            tag_item = tag_item + (self._tagsDict[tag_name][keyword],)

        return self._add_fixed_tags_values(item=tag_item)

    def _generate_keyword_update_item(self, keyword):
        update_item = dict()
        update_item[self._keywordName] = keyword
        for tag_name in self._tagsName:
            if keyword in self._tagsDict[tag_name]:
                update_item[tag_name] = self._tagsDict[tag_name][keyword]

        return update_item

    @staticmethod
    def _generate_frequency(keywords):
        keywords_frequency = {}

        for keyword in keywords:
            if keyword in keywords_frequency:
                keywords_frequency[keyword] = keywords_frequency[keyword] + 1
            else:
                keywords_frequency[keyword] = 1

        return keywords_frequency

    @staticmethod
    def _decide_multi_keywords(keywords_frequency):
        return keywords_frequency.keys()

    @staticmethod
    def _decide_sole_keyword(keywords_frequency):
        # 按照 key 进行排序
        # keyword_list = sorted(keyword_list.items(), key=lambda d: d[0], reverse=True)

        # 按照 value 进行排序
        keyword_list = sorted(keywords_frequency.items(), key=lambda d: d[1], reverse=True)
        keyword = keyword_list[0][0]

        return keyword

    def _store_chinese_bad_keyword(self, keyword):
        good_keyword = self._change_bad_keyword(keyword=keyword)
        good_keyword = re.sub('^([^a-zA-Z_])', '_\\1', good_keyword, 1)

        self._badKeywordDict[good_keyword] = keyword

        return good_keyword

    def _store_english_bad_keyword(self, keyword):
        good_keyword = self._change_bad_keyword(keyword=keyword)
        good_keyword = re.sub('^([^a-zA-Z_])', '_\\1', good_keyword, 1)
        self._badKeywordDict[good_keyword] = keyword

        return good_keyword

    def _restore_bad_keyword(self, good_keyword):
        if good_keyword in self._badKeywordDict:
            return self._badKeywordDict[good_keyword]

        return good_keyword

    @staticmethod
    def _change_bad_keyword(keyword):
        keyword = keyword.replace('.', 'A1A')
        keyword = keyword.replace(' ', 'A2A')
        keyword = keyword.replace('+', 'A3A')
        keyword = keyword.replace('(', 'A4A')
        keyword = keyword.replace(')', 'A5A')
        keyword = keyword.replace('-', 'A6A')
        keyword = keyword.replace('?', 'A7A')

        return keyword

    def _main(self):
        pass

    def main(self):
        self._check()

        self._main()

        self._get_tag_rule_list()
        self._generate_regex()
        self._generate_tags_dict()
