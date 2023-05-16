import urllib3
import os
import requests
import csv
import time
import datetime

# changes this
organization = "rpscclab"

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Set the token as an environment variable
os.environ["TOKEN"] = "3RX1fmYcSc2hKed-jrfCs8pplWM="


def fetch_nodes(todays_date):
    """
    Fetches nodes data from a Chef Automate API endpoint
    """
    # Define the API endpoint and headers
    url = "https://chef-automate.rpscc.hpecorp.local/api/v0/cfgmgmt/nodes"
    headers = {"api-token": os.environ["TOKEN"]}

    # Make the API request
    response = requests.get(url, headers=headers, verify=False)
    print "response"
    # print response.json()
    # Initialize variables for pagination
    nodes = []
    page_number = 1

    while True:
        # Make the API request with the current page number
        # full_url
        # response = requests.get(
        #     "{url}?pagination.page={page_number}&pagination.size=3500&filter=organization:{organization}".format(
        #         url=url, page_number=page_number, organization=organization
        #     ),
        #     headers=headers,
        #     verify=False,
        # )
        response = requests.get(
            url + "?pagination.page={}&pagination.size=3500&filter=organization:{}".format(
                page_number, organization
            ),
            headers=headers,
            verify=False,
        )
        print "response1"
        print response.json()

        # response = requests.get(
        #     f"{url}?pagination.page={page_number}&pagination.size=3500&filter=organization:{organization}",
        #     headers=headers,
        #     verify=False,
        # )
        # Check the response status code
        if response.status_code == 200:
            print "response.json()"
            print response
            if len(response.json()) != 0:
                print(
                    "fetching nodes pagination.page:{page_number} and pagination.size=3500".format(
                        page_number=page_number
                    )
                )
                # time.sleep(3)
                page_data = response.json()
                print "page_data"
                print page_data
                nodes.extend(page_data)
                page_number += 1
            else:
                break
        else:
            raise Exception("Request failed with status code {}".format(response.status_code))

    node_list = []
    for node_item in nodes:
        chef_tags = ",".join(node_item["chef_tags"]) if "chef_tags" in node_item else ""
        check_in_dates = node_item["checkin"].split("T")[0]
        if check_in_dates == todays_date:
            node_dict = {
                "id": node_item["id"],
                "report_date": todays_date,
                "checkin": node_item["checkin"],
                "chef_version": node_item["chef_version"],
                "chef_tags": chef_tags,
                "created_at": node_item["created_at"][:19].replace("T", " "),
                "environment": node_item["environment"],
                "ipaddress": node_item["ipaddress"],
                "hostname": node_item["hostname"],
                "platform": node_item["platform"],
                "platform_version": node_item["platform_version"],
                "status": node_item["status"],
            }
            node_list.append(node_dict)
    return node_list

def fetch_runs(node_id, yesterdays_time, todays_time):
    """
    Fetches all runs for a node in a given time range
    """
    print("pulling down last 24hrs run history for {}".format(node_id))
    # Define the API endpoint and headers for getting node runs
    node_runs_url = (
        "https://chef-automate.rpscc.hpecorp.local/api/v0/cfgmgmt/nodes/{}/runs"
    )
    node_each_runs_url = (
        "https://chef-automate.rpscc.hpecorp.local/api/v0/cfgmgmt/nodes/{}/runs/{}"
    )
    url_headers = {"api-token": os.environ["TOKEN"]}

    # Initialize variables for pagination and run data
    page = 1
    node_runs = []

    while True:
        # Make the API request for the current page
        node_runs_response = requests.get(
            node_runs_url.format(node_id),
            headers=url_headers,
            verify=False,
            params={
                "pagination.page": page,
                "start": yesterdays_time,
                "end": todays_time,
            },
        )

        # Check the response status code
        if node_runs_response.status_code == 200:
            # If the current page is empty, break out of the loop
            if len(node_runs_response.json()) != 0:
                # Get the runs from the response data
                runs = node_runs_response.json()

                # time.sleep(5)
                node_runs.extend(runs)
                # Increment the page number for the next API request
                page += 1
            else:
                break
        else:
            raise Exception(
                "API request failed with status code {}".format(node_runs_response.status_code)
            )

    # Get details for each run
    node_run_details = []
    for run in node_runs:
        run_response = requests.get(
            node_each_runs_url.format(node_id, run["id"]),
            headers=url_headers,
            verify=False,
        )
        if run_response.status_code == 200:
            run_details = run_response.json()
            node_run_details.append(run_details)
        else:
            raise Exception(
                "Request failed with status code {}".format(run_response.status_code)
            )
    return node_run_details

def generate_cookbook_csv_file(nodes, output_file_name):
    """
    Generates a CSV file from a list of nodes with their corresponding runs data
    """
    with open(output_file_name, "wb") as f:
        writer = csv.writer(f)

        # Write the header row
        writer.writerow(
            [
                "node_id",
                "checkin",
                "report_date",
                "chef_version",
                "chef_tags",
                "created_at",
                "environment",
                "ipaddress",
                "hostname",
                "platform",
                "platform_version",
                "run_id",
                "node_name",
                "total_execution_time_in_secs",
                "status",
                "updated_resource_count",
                "error_message",
                "versioned_cookbooks_name",
                "versioned_cookbooks_version",
            ]
        )

        # Write the data rows
        for node in nodes:
            node_id = node["id"]
            checkin = node["checkin"]
            report_date = node["report_date"]
            chef_version = node["chef_version"]
            chef_tags = node["chef_tags"]
            created_at = node["created_at"]
            environment = node["environment"]
            ipaddress = node["ipaddress"]
            hostname = node["hostname"]
            platform = node["platform"]
            platform_version = node["platform_version"]

            for run in node["run_details"]:
                run_id = run["id"]
                node_name = run["node_name"]
                start_time = run["start_time"]
                end_time = run["end_time"]
                start = int(time.mktime(time.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")))
                end = int(time.mktime(time.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ")))
                execution_time = end - start
                status = run["status"]
                updated_resource_count = run["updated_resource_count"]
                error_message = (
                    run["error"]["message"]
                    if "error" in run and "message" in run["error"]
                    else ""
                )
                for vc in run["versioned_cookbooks"]:
                    versioned_cookbooks_name = vc["name"]
                    versioned_cookbooks_version = vc["version"]

                    # Write the row for this run
                    writer.writerow(
                        [
                            node_id,
                            checkin,
                            report_date,
                            chef_version,
                            chef_tags,
                            created_at,
                            environment,
                            ipaddress,
                            hostname,
                            platform,
                            platform_version,
                            run_id,
                            node_name,
                            execution_time,
                            status,
                            updated_resource_count,
                            error_message,
                            versioned_cookbooks_name,
                            versioned_cookbooks_version,
                        ]
                    )

def generate_tag_csv_file(nodes, output_file_name):
    """
    Generates a csv file for tag report
    """
    with open(output_file_name, "wb") as f:
        writer = csv.writer(f)

        # Write the header row
        writer.writerow(
            [
                "node_id",
                "checkin",
                "report_date",
                "chef_version",
                "chef_tags",
                "created_at",
                "environment",
                "ipaddress",
                "hostname",
                "platform",
                "platform_version",
            ]
        )

        # Write the data rows
        for node in nodes:
            node_id = node["id"]
            checkin = node["checkin"]
            report_date = node["report_date"]
            chef_version = node["chef_version"]
            chef_tags = node["chef_tags"]
            created_at = node["created_at"]
            environment = node["environment"]
            ipaddress = node["ipaddress"]
            hostname = node["hostname"]
            platform = node["platform"]
            platform_version = node["platform_version"]

            # Write the row for this run
            writer.writerow(
                [
                    node_id,
                    checkin,
                    report_date,
                    chef_version,
                    chef_tags,
                    created_at,
                    environment,
                    ipaddress,
                    hostname,
                    platform,
                    platform_version,
                ]
            )

import datetime
import os
import csv
import time

def run():
    """
    Main function that orchestrates the entire workflow
    """
    # # database information
    # config = {
    #     "user": "root",
    #     "password": "Password123",
    #     "host": "15.154.252.92",
    #     "port": 3306,
    # }
    # tag_table = "test1"
    # cookbook_table = "test2"

    current_time = datetime.datetime.utcnow()
    todays_time = current_time.strftime("%Y-%m-%d")
    yesterdays_time = (current_time - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    script_path = "/var/log/chef/splunk_report"

    # Check if the directory exists, if not, create it
    if not os.path.exists(script_path):
        os.makedirs(script_path)

    # Fetch nodes data
    print "Generate all node information"
    new_nodes = fetch_nodes(todays_time)
    print new_nodes
    tag_output_file_name = os.path.join(script_path, "%s_tag_report.csv" % todays_time)
    print "\n\nGenerate all node information into a CSV"
    generate_tag_csv_file(new_nodes, tag_output_file_name)

    # # Call feeder to add the CSV data to the database
    # print "\n\nTransfer all data from CSV to the database"
    # config.update({"database": "tag1"})
    # tag_feeder(todays_time, config, tag_table)

    print "\n\nFetch runs data for each node and add it to the node dictionary"
    for node in new_nodes:
        node["run_details"] = fetch_runs(node["id"], yesterdays_time, todays_time)

    print "\n\nGenerate a CSV file from the nodes data"
    cookbook_output_file_name = "%s_cookbook_report.csv" % todays_time
    generate_cookbook_csv_file(new_nodes, os.path.join(script_path, cookbook_output_file_name))

    # print "\n\nCall feeder to add the CSV data to the database"
    # config.update({"database": "tag1"})
    # cookbook_feeder(todays_time, config, cookbook_table)

    # print "Output file %s has been generated successfully!" % cookbook_output_file_name


if __name__ == "__main__":
    run()
