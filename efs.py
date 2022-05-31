import boto3
import os
import logging

import utils

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)-8s] - %(message)s",
    level=getattr(logging, LOG_LEVEL),
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

efs_client = None


def getEFSClient():
    global efs_client

    if efs_client is None:
        efs_client = boto3.client('efs')

    return efs_client


def auditEFS():
    backup_data = {}

    file_systems = getEFSClient().describe_file_systems()['FileSystems']

    for fs in file_systems:
        fs_id = fs['FileSystemId']
        fs_name = fs['Name']
        try:
            this_fs_backup_data = getEFSClient().describe_backup_policy(FileSystemId=fs_id)
            backup_data[fs_name] = {
                "backup_data": this_fs_backup_data['BackupPolicy']['Status'],
                "backup_is_compliant": this_fs_backup_data['BackupPolicy']['Status'] == "ENABLED",
                "tags": utils.flattenTags(fs['Tags'])
            }
        except:
            backup_data[fs_name] = {
                "backup_data": "NONE",
                "backup_is_compliant": False,
                "tags": utils.flattenTags(fs['Tags'])
            }

    return backup_data
