import datetime

import boto3
import os
import logging
import utils

from typing import List

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
rds_client = boto3.client('rds')

# https://us-east-1.console.aws.amazon.com/rds/home?region=us-east-1#database:id=analytics-backend-acr-bundle-micrdb-20220429163759925100000004;is-cluster=true


def getClusters() -> list:
    return rds_client.describe_db_clusters()['DBClusters']


def getInstances() -> list:
    return rds_client.describe_db_instances()['DBInstances']


def getLatestSnapshot(snapshots: List[dict]) -> dict:
    snapshots.reverse()

    latest = {}
    try:
        latest = snapshots[0]
    except IndexError:
        logger.debug("Cannot return snapshot data from empty list of snapshots")

    return latest


def auditRDS():

    clusters = getClusters()
    backup_data = {}

    analyzed_instances = []

    # Audit Clusters
    for cluster in clusters:
        cluster_id = cluster['DBClusterIdentifier']

        this_cluster_backup_data = rds_client.describe_db_cluster_snapshots(DBClusterIdentifier=cluster_id)['DBClusterSnapshots']
        latest_cluster_backup = getLatestSnapshot(this_cluster_backup_data)

        backup_data[cluster_id] = {
            "backup_data": latest_cluster_backup['SnapshotCreateTime'],
            "backup_is_compliant": backupIsCompliant(latest_cluster_backup),
            "tags": utils.flattenTags(cluster['TagList'])
        }

        # Track identifiers of instances that are members of the current cluster
        cluster_instance_identifiers = [member['DBInstanceIdentifier'] for member in cluster['DBClusterMembers']]
        analyzed_instances.extend(cluster_instance_identifiers)

    instances = getInstances()

    # Audit instances if the number of cluster-managed instances is different from the number
    #   of standalone instances
    if len(analyzed_instances) != len(instances):

        for instance in instances:
            instance_id = instance['DBInstanceIdentifier']

            if instance_id not in analyzed_instances:

                this_instance_backup_data = rds_client.describe_db_snapshots(DBInstanceIdentifier=instance_id)['DBSnapshots']
                this_instance_backup_data.reverse()

                latest_instance_backup = getLatestSnapshot(this_instance_backup_data)

                backup_data[instance_id] = {
                    "backup_data": latest_instance_backup['SnapshotCreateTime'],
                    "backup_is_compliant": backupIsCompliant(latest_instance_backup),
                    "tags": utils.flattenTags(instance['TagList'])
                }

                analyzed_instances.append(instance_id)

    return backup_data


def backupIsCompliant(backup_data: dict) -> bool:
    # TODO: determine RPO
    # TODO extend ruleset?

    try:
        snapshot_create_time = backup_data['SnapshotCreateTime']
    except KeyError:
        logger.debug("SnapshotCreateTime not detected, assuming backup is non-compliant")
        return False

    now = datetime.datetime.now(tz=None)

    snapshot_age = now - snapshot_create_time.replace(tzinfo=None)

    return snapshot_age < datetime.timedelta(hours=24)

