import datetime

import boto3
import os
import logging
import utils

from typing import List

import botocore

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)-8s] - %(message)s",
    level=getattr(logging, LOG_LEVEL),
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

efs_client = None
file_system_identifiers = []
efs_backup_data = {}


def getEFSClient():
    global efs_client

    if efs_client is None:
        efs_client = boto3.client('efs')

    return efs_client


def getFileSystemIdentifiers() -> List[str]:
    global file_system_identifiers

    if len(file_system_identifiers) == 0:
        file_systems = getEFSClient().describe_file_systems()['FileSystems']

        for fs in file_systems:
            this_file_system_id = fs['FileSystemId']
            file_system_identifiers.append(this_file_system_id)

    return file_system_identifiers


def getFileSystemBackups() -> dict:

    backup_data = {}

    for fs_id in getFileSystemIdentifiers():
        try:
            this_fs_backup_data = getEFSClient().describe_backup_policy(FileSystemId=fs_id)
            backup_data[fs_id] = {
                "backup_data": this_fs_backup_data,
                "backup_is_compliant": this_fs_backup_data['BackupPolicy']['Status'] == "ENABLED"
            }
        except:
            backup_data[fs_id] = {
                "backup_data": "NONE",
                "backup_is_compliant": False
            }

    return backup_data


def auditEFS():
    data = getFileSystemBackups()

    return data
