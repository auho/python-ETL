def get_content_from_item_by_keys(keys, item):
    content = ''
    if type(keys) == str:
        if keys in item:
            content = item[keys]
    elif type(keys) == list:

        for key in keys:
            if key in item:
                if item[key] is None or item[key] == '':
                    continue

                content += ' ' + item[key]
    else:
        print(keys)
        print(item)
        raise Exception('key is not in item!')

    return content
