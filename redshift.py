import datetime
import boto3
import logging
import os
import utils

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)-8s] - %(message)s",
    level=getattr(logging, LOG_LEVEL),
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

redshift_client = None


def getRedshiftClient():
    global redshift_client

    if redshift_client is None:
        redshift_client = boto3.client('redshift')

    return redshift_client


def backupIsCompliant(backup_data: dict) -> bool:
    # TODO: determine RPO; currently assuming 24 hours

    snapshot_create_time = backup_data['SnapshotCreateTime']

    now = datetime.datetime.now(tz=None)

    snapshot_age = now - snapshot_create_time.replace(tzinfo=None)

    return snapshot_age < datetime.timedelta(hours=24)


def auditRedshift():

    redshift_backup_data = {}

    clusters = getRedshiftClient().describe_clusters()['Clusters']

    for cluster in clusters:
        cid = cluster['ClusterIdentifier']

        cluster_snapshot_data = getRedshiftClient().describe_cluster_snapshots(ClusterIdentifier=cid)["Snapshots"]

        latest_snap = cluster_snapshot_data[0]

        redshift_backup_data[cid] = {
            "backup_data": latest_snap['SnapshotCreateTime'],
            "backup_is_compliant": backupIsCompliant(latest_snap),
            "tags": utils.flattenTags(cluster['Tags'])
        }

    return redshift_backup_data
