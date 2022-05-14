
import datetime
import boto3
import logging
import os

from typing import List

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)-8s] - %(message)s",
    level=getattr(logging, LOG_LEVEL),
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

redshift_client = None
redshift_cluster_data = None
redshift_cluster_identifiers = []

redshift_backup_data = {}


def getRedshiftClient():
    global redshift_client

    if redshift_client is None:
        redshift_client = boto3.client('redshift')

    return redshift_client


def getClusterData():
    global redshift_cluster_data

    if redshift_cluster_data is None:
        redshift_cluster_data = getRedshiftClient().describe_clusters()['Clusters']

    return redshift_cluster_data


def getClusterIdentifiers():
    global redshift_cluster_identifiers

    if len(redshift_cluster_identifiers) == 0:
        raw_cluster_data = getClusterData()

        for cluster in raw_cluster_data:
            redshift_cluster_identifiers.append(cluster['ClusterIdentifier'])

    return redshift_cluster_identifiers

def snapshotIsAutomated(snapshot: dict) -> bool :
    return snapshot["SnapshotType"] == "automated"


def refineBackupData(rawBackupData: dict, targetData: List[str]) -> List[dict]:
    refined = []

    for snapshot in rawBackupData:
        r = {}

        for index in targetData:
            r[index] = snapshot[index]

            r['automated'] = snapshotIsAutomated(snapshot)

        refined.append(r)

    return refined


def backupIsCompliant(backup_data: dict) -> bool:
    # TODO: determine RPO; currently assuming 24 hours

    snapshot_create_time = backup_data['SnapshotCreateTime']

    now = datetime.datetime.now(tz=None)

    snapshot_age = now - snapshot_create_time.replace(tzinfo=None)

    return snapshot_age < datetime.timedelta(hours=24)


def auditClusterSnapshots() -> None:
    global redshift_backup_data

    cluster_ids = getClusterIdentifiers()

    target_data = [
        "SnapshotIdentifier",
        "ClusterIdentifier",
        "SnapshotCreateTime",
        "Encrypted",
        "DBName"
    ]

    for cid in cluster_ids:
        raw_data = getRedshiftClient().describe_cluster_snapshots(ClusterIdentifier=cid)["Snapshots"]
        refined_data = refineBackupData(raw_data, target_data)[0]
        redshift_backup_data[cid] = {
            "backup_data":refined_data,
            "backup_is_compliant": backupIsCompliant(refined_data)
        }


def auditRedshift():
    global redshift_backup_data

    auditClusterSnapshots()

    return redshift_backup_data
