import os

import efs
import rds
import redshift as rs
import dynamodb as ddb
import utils
import logging
import functools

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)-8s] - %(message)s",
    level=getattr(logging, LOG_LEVEL),
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# TODO: add click options (ex: if --backup, only gauge backups; if --rds, only audit RDS resources, etc)

def main():
    logger.info("Auditing DynamoDB resources")
    dynamo_status = ddb.auditDynamo()
    logger.info("Auditing RDS resources")
    rds_status = rds.auditRDS()
    logger.info("Auditing Redshift resources")
    redshift_status = rs.auditRedshift()
    logger.info("Auditing EFS resources")
    efs_status = efs.auditEFS()

    aggregate_backup_status = {
        "rds": rds_status,
        "redshift": redshift_status,
        "efs": efs_status,
        "dynamo": dynamo_status
    }

    logger.info("Building Markdown tables")

    compliant_markdown, noncompliant_markdown = utils.buildMarkdown(aggregate_backup_status)

    # Write output
    account = utils.getShortAccountName(os.getenv("AWS_PROFILE", "NULL_PROFILE"))

    output_file = open(f'output/{account}_audit_results.md', 'w')

    final_markdown = f"# HQR {account} BACKUP COMPLIANCE REPORT\n" + noncompliant_markdown + "\n\n" + compliant_markdown

    output_file.write(final_markdown)

    output_file.close()

    logger.info("Audit complete")




if __name__ == '__main__':
    main()


