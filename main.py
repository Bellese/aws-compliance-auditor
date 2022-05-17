import efs
import rds
import redshift as rs

import json
import logging
import os
import tabulate

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)-8s] - %(message)s",
    level=getattr(logging, LOG_LEVEL),
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


# TODO: add click options (ex: if --backup, only gauge backups; if --rds, only audit RDS resources, etc)

def buildTabulateInput(input_data: dict):
    compliant_table_headers = ["Name", "Latest Backup"]
    noncompliant_table_headers = ["Name", "Owner"]

    compliant_table_data = []
    noncompliant_table_data = []

    for aws_service_identifier in input_data:

        for resource_identifier in input_data[aws_service_identifier]:
            this_resource = input_data[aws_service_identifier][resource_identifier]

            backup_is_compliant = this_resource['backup_is_compliant']

            if backup_is_compliant:
                compliant_table_data.append([
                    resource_identifier,
                    this_resource["backup_data"]
                ])
            else:
                owner = "N/A"

                try:
                    owner = this_resource["tags"]["tech-poc-primary"]
                except KeyError:
                    logger.info(f"No Owner found for {resource_identifier}")

                noncompliant_table_data.append([
                    resource_identifier,
                    owner
                ])

    compliant_table = tabulate.tabulate(compliant_table_data, headers=compliant_table_headers, tablefmt="github")
    noncompliant_table = tabulate.tabulate(noncompliant_table_data, headers=noncompliant_table_headers, tablefmt="github")

    return compliant_table, noncompliant_table


def main():

    rds_status = rds.auditRDS()
    # redshift_status = rs.auditRedshift()
    # efs_status = efs.auditEFS()
    #
    aggregate_backup_status = {
        "rds": rds_status,
        # "redshift": redshift_status,
        # "efs": efs_status
    }

    c, nc = buildTabulateInput(aggregate_backup_status)

    # Write output
    account = os.getenv("AWS_PROFILE", "NULL_PROFILE")

    compliant_output_file = open(f'output/{account}_compliant_asses.md', 'w+')
    noncompliant_output_file = open(f'output/{account}_noncompliant_asses.md', 'w+')

    compliant_output_file.write(c)
    noncompliant_output_file.write(nc)

    compliant_output_file.close()
    noncompliant_output_file.close()


if __name__ == '__main__':
    main()
