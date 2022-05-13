import boto3
import os
import logging
import json

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

backup_data = {}
instance_identifiers = []


def getRDSClient():
    global rds_client

    if rds_client is None:
        rds_client = boto3.client('rds')

    return rds_client


def getClusters():
    global cluster_data

    if cluster_data is None:
        cluster_data = getRDSClient().describe_db_clusters()['DBClusters']

    return cluster_data


def getInstances():
    global instance_data

    if instance_data is None:
        instance_data = getRDSClient().describe_db_instances()['DBInstances']

    return instance_data


def processClusterBackupData(raw):
    processed = []

    for element in raw:
        processed.append({
            'snapshot_create_time': element['SnapshotCreateTime'],
            'cluster_create_time': element['ClusterCreateTime'],
            'complete': element['PercentProgress'] == 100,
            'automated': element['SnapshotType'] == 'automated',
            'cluster_snapshot_arn': element['DBClusterSnapshotArn'],
            'cluster_snapshot_identifier': element['DBClusterSnapshotIdentifier']
        })

    processed.reverse()
    return processed

def processInstanceBackupData(raw):
    processed = []

    for s in raw:
        processed.append({
            'snapshot_create_time': s['SnapshotCreateTime'],
            'cluster_create_time': s['InstanceCreateTime'],
            'complete': s['PercentProgress'] == 100,
            'automated': s['SnapshotType'] == 'automated',
            'cluster_snapshot_arn': s['DBSnapshotArn'],
            'cluster_snapshot_identifier': s['DBSnapshotIdentifier']
        })

    return processed

def processAutomatedSnapshotData(raw):
    processed = {
        'restore_window': raw['RestoreWindow'],
        'instance_create_time': raw['InstanceCreateTime'],
        'automated_backup_arm': raw['DBInstanceAutomatedBackupsArn']
    }

    return processed


def auditClusters():
    global backup_data
    global instance_identifiers

    # TODO: include owner and POC from tag list in return data struct

    for cluster in getClusters():
        cluster_id = cluster['DBClusterIdentifier']
        logger.info(f"Auditing {cluster_id}")

        raw_cluster_backup_data = getRDSClient().describe_db_cluster_snapshots(DBClusterIdentifier=cluster_id)[
            'DBClusterSnapshots']

        backup_data[cluster_id] = {
            "backup_data": processClusterBackupData(raw_cluster_backup_data)
        }

        cluster_members = cluster['DBClusterMembers']

        for instance in cluster_members:
            instance_id = instance['DBInstanceIdentifier']
            instance_identifiers.append(instance_id)

def auditInstances():
    global backup_data
    global instance_data
    global instance_identifiers

    # TODO: include tag list in returned data struct
    for instance in instance_data:
        instance_id = instance['DBInstanceIdentifier']

        already_analyzed = instance_id in instance_identifiers

        if not already_analyzed:
            instance_identifiers.append(instance_id)

            snapshot_data = getRDSClient().describe_db_snapshots(DBInstanceIdentifier=instance_id)['DBSnapshots']
            automated_snapshot_data = getRDSClient().describe_db_instance_automated_backups(DBInstanceIdentifier=instance_id)['DBInstanceAutomatedBackups'][0]

            processed_snapshot_data = processInstanceBackupData(snapshot_data)
            processed_automated_snapshot_data = processAutomatedSnapshotData(automated_snapshot_data)

            backup_data[instance_id] = {
                "backup_data": {
                    "snapshots": processed_snapshot_data,
                    "automated_snapshot": processed_automated_snapshot_data
                }
            }

def auditRDS():
    global backup_data
    auditClusters()

    instances = getInstances()

    if len(instances) != len(instance_identifiers):
        logging.info("The number of standalone instances is different from the number of cluster-managed instances.")
        logging.info("Analyzing standalone instances...")
        auditInstances()

    logger.info(f"{len(instance_identifiers)} instances analyzed")

    return backup_data


def formatReturnData(data):
    ret = {}

    for index in data:
        _this = data[index]["backup_data"]

        latest_snapshot = None
        latest_automated_snapshot = None
        try:
            latest_snapshot = _this[0]
        except KeyError:
            # If a numeric index doesn't exist on _this, we know its shape must be
            # {"snapshots": [{}, {}, {}, ...], "automated_snapshots": [{}]}
            latest_snapshot = _this["snapshots"][0]
            latest_automated_snapshot = _this["automated_snapshot"]

        ret[index] = {
                "latest_snapshot": latest_snapshot,
                "latest_automated_snapshot": latest_automated_snapshot
        }

    return ret


def getRDSBackupData():
    global backup_data

    if len(backup_data.keys()) == 0:
        backup_data = auditRDS()
        backup_data = formatReturnData(backup_data)

    backup_data_json = json.dumps(backup_data, indent=4, sort_keys=False, default=str)

    return backup_data_json
