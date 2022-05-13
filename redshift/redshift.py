import boto3
import logging
import os

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



def getClusterSnapshots():
    cluster_ids = getClusterIdentifiers()

    snapshot_data = {}

    for id in cluster_ids:
        snapshot_data[id] = getRedshiftClient().describe_cluster_snapshots(ClusterIdentifier=id)

    logger.info(snapshot_data)