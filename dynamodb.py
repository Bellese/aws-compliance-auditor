import boto3
import os
import logging
import utils

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    format="[%(asctime)s] [%(levelname)-8s] - %(message)s",
    level=getattr(logging, LOG_LEVEL),
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Establish boto3 client
dynamo_client = boto3.client('dynamodb')


def describeTables():
    return [dynamo_client.describe_table(TableName=table_name)['Table'] for table_name in
            dynamo_client.list_tables()['TableNames']]


def hasBackupsEnabled(backup_response: dict) -> bool:
    continuous_backups_status = backup_response['ContinuousBackupsDescription']['ContinuousBackupsStatus']
    pitr_status = backup_response['ContinuousBackupsDescription']['PointInTimeRecoveryDescription'][
        'PointInTimeRecoveryStatus']

    has_continuous_backups_enabled = continuous_backups_status == "ENABLED"
    has_pitr_enabled = pitr_status == "ENABLED"

    return has_continuous_backups_enabled and has_pitr_enabled


def auditDynamo():
    backup_data = {}

    tables = describeTables()

    for table in tables:

        table_name = table['TableName']
        tags = dynamo_client.list_tags_of_resource(ResourceArn=table['TableArn'])['Tags']

        continuous_backups_response = dynamo_client.describe_continuous_backups(TableName=table_name)

        backups_enabled = hasBackupsEnabled(continuous_backups_response)

        if backups_enabled:
            earliest_restore_point = \
                continuous_backups_response['ContinuousBackupsDescription']['PointInTimeRecoveryDescription'][
                    'EarliestRestorableDateTime'].strftime("%m/%d/%Y, %H:%M:%S")
            latest_restore_point = \
                continuous_backups_response['ContinuousBackupsDescription']['PointInTimeRecoveryDescription'][
                    'LatestRestorableDateTime'].strftime("%m/%d/%Y, %H:%M:%S")

            pitr_data = {
                "earliest_restore_point": earliest_restore_point,
                "latest_restore_point": latest_restore_point
            }

            backup_data[table_name] = {
                "backup_data": pitr_data,
                "backup_is_compliant": True,
                "tags": utils.flattenTags(tags)
            }
        else:
            backup_data[table_name] = {
                "backup_data": {},
                "backup_is_compliant": False,
                "tags": utils.flattenTags(tags)
            }

    return backup_data
