import redshift.redshift
import rds.rds as rds
import redshift.redshift as rs

import json
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


# TODO: only return DBs that DO NOT have auto-backup enabled; check SBX instances that Simon made
# TODO: add click options (ex: if --backup, only gauge backups; if --rds, only audit RDS resources, etc)

def main():

    redshift_status = rs.getClusterSnapshots()

    rds_status = rds.getRDSBackupData()


    # aggregated_backup_status_json = json.dumps(aggregated_backup_status, indent=4, sort_keys=False, default=str)
    # Write output
    account = os.getenv("AWS_PROFILE", "NULL_PROFILE")
    output_file = open(f'backup_status_{account}.json', 'w+')
    output_file.write(rds_status)
    output_file.close()


if __name__ == '__main__':
    main()
