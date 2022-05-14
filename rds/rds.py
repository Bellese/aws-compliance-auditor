import datetime

import boto3
import os
import logging

from typing import List

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)-8s] - %(message)s",
    level=getattr(logging, LOG_LEVEL),
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

rds_client = None
cluster_data = None
instance_data = None

rds_backup_data = {}
analyzed_instances = []

# boto3 Data Fetching
def getRDSClient():
    global rds_client

    if rds_client is None:
        rds_client = boto3.client('rds')

    return rds_client


def getClusters() -> list:
    global cluster_data

    if cluster_data is None:
        cluster_data = getRDSClient().describe_db_clusters()['DBClusters']

    return cluster_data


def getClusterBackupData(cluster_id: str) -> dict:
    raw_cluster_backup_data = getRDSClient().describe_db_cluster_snapshots(DBClusterIdentifier=cluster_id)[
        'DBClusterSnapshots']
    target_data = [
        "SnapshotCreateTime",
        "ClusterCreateTime",
        "DBClusterSnapshotArn",
        "DBClusterSnapshotIdentifier"
    ]

    refined_cluster_backup_data = refineBackupData(raw_cluster_backup_data, target_data)[0]
    return refined_cluster_backup_data


def getInstances() -> list:
    global instance_data

    if instance_data is None:
        instance_data = getRDSClient().describe_db_instances()['DBInstances']

    return instance_data


def getInstanceBackupData(instance_id: str) -> dict:
    raw_instance_backup_data = getRDSClient().describe_db_snapshots(DBInstanceIdentifier=instance_id)['DBSnapshots']

    target_data = [
        "SnapshotCreateTime",
        "InstanceCreateTime",
        "DBSnapshotArn",
        "DBSnapshotIdentifier"
    ]

    refined_instance_backup_data = refineBackupData(raw_instance_backup_data, target_data)[0]
    return refined_instance_backup_data


# Snapshot Audit Helpers
def snapshotIsComplete(snapshot: dict) -> bool:
    return snapshot["PercentProgress"] == 100


def snapshotIsAutomated(snapshot: dict) -> bool:
    return snapshot["SnapshotType"] == "automated"


def refineBackupData(rawBackupData: dict, targetData: List[str]) -> List[dict]:
    refined = []

    for snapshot in rawBackupData:
        r = {}

        for index in targetData:
            r[index] = snapshot[index]

            r['complete'] = snapshotIsComplete(snapshot)
            r['automated'] = snapshotIsAutomated(snapshot)

        refined.append(r)

    refined.reverse()
    return refined


def markClusterMembersAnalyzed(members: list) -> None:
    global analyzed_instances

    for m in members:
        this_instance_id = m['DBInstanceIdentifier']
        analyzed_instances.append(this_instance_id)


def gaugeBackupCompliance(backup_data: dict) -> bool:
    # TODO: determine RPO; currently assuming 24 hours

    snapshot_create_time = backup_data['SnapshotCreateTime']

    now = datetime.datetime.now(tz=None)

    snapshot_age = now - snapshot_create_time.replace(tzinfo=None)

    return snapshot_age < datetime.timedelta(hours=24)


def auditRDSClusters() -> None:
    global rds_backup_data
    global analyzed_instances

    for cluster in getClusters():
        this_cluster_id = cluster['DBClusterIdentifier']
        this_cluster_backup_data = getClusterBackupData(this_cluster_id)

        rds_backup_data[this_cluster_id] = {
            "backup_data": this_cluster_backup_data,
            "backup_is_compliant": gaugeBackupCompliance(this_cluster_backup_data),
            "tags": flattenTags(cluster['TagList'])
        }

        markClusterMembersAnalyzed(cluster['DBClusterMembers'])


def auditRDSInstances() -> None:
    global rds_backup_data
    global analyzed_instances

    for instance in getInstances():
        this_instance_id = instance['DBInstanceIdentifier']

        if this_instance_id not in analyzed_instances:
            this_instance_backup_data = getInstanceBackupData(this_instance_id)
            rds_backup_data[this_instance_id] = {
                "backup_data": this_instance_backup_data,
                "backup_is_compliant": gaugeBackupCompliance(this_instance_backup_data),
                "tags": flattenTags(instance['TagList'])
            }
            analyzed_instances.append(this_instance_id)


def flattenTags(tags: dict) -> dict:
    ret = {}

    for tag in tags:
        ret[tag['Key']] = tag['Value']

    return ret


def gaugeAuditCoverage() -> bool:
    global analyzed_instances

    everything_is_audited = True

    if len(analyzed_instances) != len(getInstances()):
        everything_is_audited = False

    return everything_is_audited


def auditRDS() -> dict:
    global rds_backup_data
    global analyzed_instances

    auditRDSClusters()

    everything_is_audited = gaugeAuditCoverage()

    if not everything_is_audited:
        auditRDSInstances()

    return rds_backup_data
