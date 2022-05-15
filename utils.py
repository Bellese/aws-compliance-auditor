def flattenTags(tags: dict) -> dict:
    ret = {}

    for tag in tags:
        ret[tag['Key']] = tag['Value']

    return ret