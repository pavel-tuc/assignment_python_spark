import json
import re
import datetime
from dateutil.relativedelta import relativedelta

_REGEX_MONTH_ID = "^.*id=(\d+).*month=(\d{4}-\d{2}-\d{2}).*$"

full_paths = [
    "s3://my-bucket/xxx/yyy/zzz/abc/id=123/month=2019-01-01/2019-01-19T10:31:18.818Z.gz",
    "s3://my-bucket/xxx/yyy/zzz/abc/id=123/month=2019-02-01/2019-02-19T10:32:18.818Z.gz",
    "s3://my-bucket/xxx/yyy/zzz/abc/id=333/month=2019-03-01/2019-06-19T10:33:18.818Z.gz",
    "s3://my-bucket/xxx/yyy/zzz/def/id=123/month=2019-10-01/2019-10-19T10:34:18.818Z.gz",
    "s3://my-bucket/xxx/yyy/zzz/def/id=333/month=2019-11-01/2019-12-19T10:35:18.818Z.gz"
]


def get_all_keys(bucket: str, full_path: str) -> (int, datetime):
    """Returns a tuple

    Extracts id and month from full path to file in S3 bucket
    """
    object_id, month = re.search(_REGEX_MONTH_ID, bucket + full_path).groups()
    return object_id, month


def min_max_month(object_list: list, bucket: str) -> dict:
    """Returns a dict

    Calculates min and max month for each id and stores it in dict
    """
    result = {}
    for file in object_list:
        id, month = get_all_keys(bucket, file)
        if id not in result:
            result[id] = {"min_month": f'{month}', "max_month": f'{month}'}
        else:
            if month > result[id]['max_month']:
                result[id]['max_month'] = month
            if month < result[id]['min_month']:
                result[id]['min_month'] = month
    return result


def write_to_json(data: dict, file_name: str) -> None:
    """Returns

    Writes data in dict as json into output folder
    """
    json_object = json.dumps(data)
    with open(f"./output/{file_name}_{datetime.datetime.now().strftime('%Y%m%dT%H%M%SZ')}.json", "w") as file:
        file.write(json_object)


def date_range(object_list: list, bucket: str) -> dict:
    """Returns a dict

    Adds element missing_months to already existing dict of id's
    """
    result = min_max_month(object_list, bucket)
    for object_id, month in result.items():
        months = []
        start = datetime.datetime.strptime(month['min_month'], "%Y-%m-%d").date()
        end = datetime.datetime.strptime(month['max_month'], "%Y-%m-%d").date()
        while start < end:
            months.append(start.strftime("%Y-%m-%d"))
            start += relativedelta(months=1)
        result[object_id]["missing_months"] = months

    return result

def missing_months(object_list: list, bucket: str) -> dict:
    """Returns a dict

    Removes all existing records from dict element "missing_months"
    """
    dict_with_date_range = date_range(object_list, bucket)
    for file in object_list:
        object_id, month = get_all_keys(bucket, file)
        if month in dict_with_date_range[object_id]["missing_months"]:
            dict_with_date_range[object_id]["missing_months"].remove(month)
    return dict_with_date_range
if __name__ == '__main__':
    # A
    print(min_max_month(full_paths, "s3"))
    # B
    write_to_json(min_max_month(full_paths, "s3"), "result")
    # C
    write_to_json(missing_months(full_paths, "s3"), "result_enriched")
