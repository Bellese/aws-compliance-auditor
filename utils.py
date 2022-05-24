import os
import logging
import tabulate

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)-8s] - %(message)s",
    level=getattr(logging, LOG_LEVEL),
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def flattenTags(tags: dict) -> dict:
    ret = {}

    for tag in tags:
        ret[tag['Key']] = tag['Value']

    return ret


def defineCompliantHeaders(service_identifier):
    headers = ["Name", "Latest Backup"]

    if service_identifier == "efs":
        headers = ["Name", "Backup Policy Status"]

    if service_identifier == "dynamo":
        headers = ["Name", "Earliest Restore Point", "Latest Restore Point"]

    return headers


def buildTabulateInput(input_data: dict, service_identifier: str):
    compliant_table_headers = defineCompliantHeaders(service_identifier)
    noncompliant_table_headers = ["Name", "Owner"]

    compliant_table_data = []
    noncompliant_table_data = []

    for resource_identifier in input_data:
        this_resource = input_data[resource_identifier]
        backup_is_compliant = this_resource['backup_is_compliant']

        if backup_is_compliant:
            if service_identifier == "dynamo":
                compliant_table_data.append([
                    resource_identifier,
                    this_resource["backup_data"]["earliest_restore_point"],
                    this_resource["backup_data"]["latest_restore_point"]
                ])
            else:
                compliant_table_data.append([
                    resource_identifier,
                    this_resource["backup_data"]
                ])
        else:
            owner = "hqr-devops@bellese.io"

            try:
                owner = this_resource["tags"]["tech-poc-primary"]
            except KeyError:
                logger.info(f"No Owner found for resource {resource_identifier}. Dumping tags")

            noncompliant_table_data.append([
                resource_identifier,
                f"[{owner}](mailto:{owner})"
            ])

    compliant_table = tabulate.tabulate(compliant_table_data, headers=compliant_table_headers, tablefmt="github")
    noncompliant_table = tabulate.tabulate(noncompliant_table_data, headers=noncompliant_table_headers, tablefmt="github")

    return compliant_table, noncompliant_table


def buildMarkdown(aggregate_backup_status):
    compliant_table = "## COMPLIANT RESOURCES\n"
    noncompliant_table = "## NONCOMPLIANT RESOURCES\n"

    for x in aggregate_backup_status:
        logger.info(f"Building markdown for {x}")
        compliant_table += f"### {x.upper()} \n"
        noncompliant_table += f"### {x.upper()} \n"

        compliant_markdown, noncompliant_markdown = buildTabulateInput(aggregate_backup_status[x], x)

        compliant_table += f"{compliant_markdown}\n\n"
        noncompliant_table += f"{noncompliant_markdown}\n\n"

    return compliant_table, noncompliant_table


def getShortAccountName(account: str):
    name = "PROD"

    if "SBX" in account:
        name = "SBX"

    if "DEV" in account:
        name = "DEV"

    if "TEST" in account:
        name = "TEST"

    if "IMPL" in account:
        name = "IMPL"

    return name
