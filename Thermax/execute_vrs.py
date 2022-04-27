from Modules import send_request
import logging
import json


def get_execute_etl():
    try:

        execute_batch_files_url = "http://localhost:50013/api/v1/vendor_recon/get_execute_batch_data/"

        headers = {
            "Content-Type": "application/json"
        }

        etl = send_request.SendRequest()

        payload = json.dumps({
            "tenantsId": 3,
            "groupsId": 3,
            "entityId": 4,
            "mProcessingLayerId": 6,
            "mProcessingSubLayerId": 6
        })

        execute_batch_files_response = etl.get_response(post_url=execute_batch_files_url, headers=headers, data=payload)

        print("execute_batch_files_response")
        print(execute_batch_files_response)

        return {"Status": "Success"}

    except Exception:
        logging.error("Error in Get Execute ETL!!!", exc_info=True)
        return {"Status": "Error"}


if __name__ == "__main__":
    get_execute_etl()
