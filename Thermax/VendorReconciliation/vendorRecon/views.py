import logging
import json
import re
from datetime import datetime
from django.views.decorators.csrf import csrf_exempt
from .models import  *
from django.http import JsonResponse
import requests
from django.db import connection
import pandas as pd
import os
from pathlib import Path
import shutil
from rest_framework import viewsets
from .serializers import *
import warnings
from .packages import read_file, send_email, write_vrs
# from django.utils import timezone

warnings.filterwarnings("ignore")

# Create your views here.
logger = logging.getLogger("vendor_reconciliation")

def execute_sql_query(query, object_type):
    try:
        with connection.cursor() as cursor:
            #logger.info("Executing SQL Query..")
            # logger.info(query)
            # print(query)
            cursor.execute(query)
            if object_type == "table":
                column_names = [col[0] for col in cursor.description]
                rows = dict_fetch_all(cursor)
                table_output = {"headers":column_names, "data":rows}
                output = json.dumps(table_output)
                return output
            elif object_type in ["data"]:
                column_names = [col[0] for col in cursor.description]
                rows = dict_fetch_all(cursor)
                table_output = {"headers": column_names, "data": rows}
                return table_output
            elif object_type == "Normal":
                return "Success"
            elif object_type in["update", "create"]:
                return None
            else:
                rows = cursor.fetchall()
                column_header = [col[0] for col in cursor.description]
                df = pd.DataFrame(rows)
                return [df, column_header]

    except Exception as e:
        logger.info("Error Executing SQL Query!!", exc_info=True)
        return None

def dict_fetch_all(cursor):
    "Return all rows from cursor as a dictionary"
    try:
        column_header = [col[0] for col in cursor.description]
        return [dict(zip(column_header, row)) for row in cursor.fetchall()]
    except Exception as e:
        logger.error("Error in converting cursor data to dictionary", exc_info=True)

def get_grid_transform(header, header_column):
    try:
        column_defs = []
        for header in header["headers"]:
            column_defs.append({
                "field": header
            })

        column_header_defs = []
        for header in header_column["headers"]:
            column_header_defs.append({
                "headerName": header
            })

        for i in range(0, len(column_defs)):
            column_defs[i]["headerName"] = column_header_defs[i]["headerName"]
            column_defs[i]["sortable"] = "true"

        return column_defs
    except Exception as e:
        logger.error("Error in Getting Grid Transformation!!!", exc_info=True)

class VendorMasterViewSet(viewsets.ModelViewSet):
    queryset = VendorMaster.objects.all()
    serializer_class = VendorMasterSerializer

class ReconFileUploadsViewSet(viewsets.ModelViewSet):
    queryset = ReconFileUploads.objects.all()
    serializer_class = ReconFileUploadsSerializer

class MasterMatchingCommentsViewSet(viewsets.ModelViewSet):
    queryset = MasterMatchingComments.objects.all()
    serializer_class = MasterMatchingCommentsSerializer

    def perform_create(self, serializer):
        serializer.save(created_date = str(datetime.today()), modified_date = str(datetime.today()))

def get_proper_file_name(file_name):
    try:
        file_name_extension = "." + file_name.split(".")[-1]
        file_name_without_extension = file_name.replace(file_name_extension, "")
        file_name_date = file_name_without_extension.replace(".", "") + "_" + str(datetime.now()).replace("-", "_").replace(" ", "_").replace(":", "_").replace(".","_") + file_name_extension
        file_name_proper = file_name_date.replace(" ", "_").replace("-", "_").replace("'", "").replace("#", "_No_").replace("&", "_").replace("(", "_").replace(")", "_")
        return file_name_proper
    except Exception:
        logger.error("Error in Getting Proper File Name!!!", exc_info=True)
        return "Error"

def get_proper_paths(input_path):
    try:
        file_location_to_data = input_path.split("Data/")[0] + "Data/"
        file_location_to_processing_layer_name = file_location_to_data + input_path.split("Data/")[1].split("/")[0]
        file_location_with_input = file_location_to_processing_layer_name + "/" + "input"
        return [file_location_to_processing_layer_name, file_location_with_input]
    except Exception:
        logger.error("Error in Getting Proper Paths!!!", exc_info=True)
        return "Error"

@csrf_exempt
def get_file_upload(request, *args, **kwargs):
    try:
        if request.method == "POST":

            tenant_id = request.POST.get("tenantId")
            group_id = request.POST.get("groupId")
            entity_id = request.POST.get("entityId")
            processing_layer_id = request.POST.get("processingLayerId")
            m_processing_layer_id = request.POST.get("mProcessingLayerId")
            m_processing_sub_layer_id = request.POST.get("mProcessingSubLayerId")
            user_id = request.POST.get("userId")
            file_uploaded = request.POST.get("fileUploaded")

            if int(tenant_id) > 0:
                if int(group_id) > 0:
                    if int(entity_id) > 0:
                        if int(m_processing_layer_id) > 0:
                            if int(m_processing_sub_layer_id) > 0:
                                if int(processing_layer_id) > 0:
                                    if int(user_id) > 0:
                                        if len(file_uploaded) > 0:
                                            post_url = "http://localhost:50003/source/get_processing_layer_def_list/"
                                            payload = json.dumps(
                                                {"tenant_id": tenant_id, "group_id": group_id,
                                                 "entity_id": entity_id, "processing_layer_id": processing_layer_id})
                                            headers = {
                                                "Content-Type": "application/json"
                                            }
                                            response = requests.get(post_url, data=payload, headers=headers)
                                            # print(response)
                                            if response.content:
                                                content_data = json.loads(response.content)
                                                if content_data["Status"] == "Success":
                                                    if file_uploaded == "BOTH":

                                                        file_locations = content_data["file_locations"]

                                                        internal_file_name = request.FILES["internalFileName"].name
                                                        external_file_name = request.FILES["externalFileName"].name

                                                        internal_file_name_proper = get_proper_file_name(internal_file_name)
                                                        external_file_name_proper = get_proper_file_name(external_file_name)

                                                        internal_file_location = ''
                                                        external_file_location = ''

                                                        for file_location in file_locations :
                                                            if file_location['side'] == "Internal" :
                                                                int_source_id = file_location['source_id']
                                                                internal_file_location = file_location['input_location']
                                                                int_processing_layer_name = file_location['processing_layer_name']
                                                            elif file_location['side'] == "External" :
                                                                external_file_location = file_location['input_location']
                                                                ext_source_id = file_location['source_id']
                                                                ext_processing_layer_name = file_location['processing_layer_name']

                                                        file_uploads_internal = ReconFileUploads.objects.filter(m_source_id = int_source_id, is_processed = 0)
                                                        internal_file_upload_ids = []
                                                        for internal_file in file_uploads_internal:
                                                            internal_file_upload_ids.append(internal_file.m_source_id)

                                                        file_uploads_external = ReconFileUploads.objects.filter(m_source_id = ext_source_id, is_processed = 0)
                                                        external_file_upload_ids = []
                                                        for external_file in file_uploads_external:
                                                            external_file_upload_ids.append(external_file.m_source_id)

                                                        if len(internal_file_upload_ids) == 0 and len(external_file_upload_ids) == 0:
                                                            if len(internal_file_location) > 0 and len(external_file_location) > 0:

                                                                internal_file_upload_path_name_date = internal_file_location + internal_file_name_proper
                                                                external_file_upload_path_name_date = external_file_location + external_file_name_proper

                                                                internal_file_paths = get_proper_paths(internal_file_location)
                                                                external_file_paths = get_proper_paths(external_file_location)

                                                                if not os.path.exists(internal_file_paths[0]):
                                                                    os.mkdir(internal_file_paths[0])
                                                                if not os.path.exists(internal_file_paths[1]):
                                                                    os.mkdir(internal_file_paths[1])

                                                                if not os.path.exists(external_file_paths[0]):
                                                                    os.mkdir(external_file_paths[0])
                                                                if not os.path.exists(external_file_paths[1]):
                                                                    os.mkdir(external_file_paths[1])

                                                                with open(internal_file_upload_path_name_date, 'wb+') as destination:
                                                                    for chunk in request.FILES["internalFileName"]:
                                                                        destination.write(chunk)
                                                                internal_file_size = Path(internal_file_upload_path_name_date).stat().st_size

                                                                with open(external_file_upload_path_name_date, 'wb+') as destination:
                                                                    for chunk in request.FILES["externalFileName"]:
                                                                        destination.write(chunk)
                                                                external_file_size = Path(external_file_upload_path_name_date).stat().st_size

                                                                # TODO : Add row_count also in the below Table

                                                                ReconFileUploads.objects.create(
                                                                    tenants_id = tenant_id,
                                                                    groups_id = group_id,
                                                                    entities_id = entity_id,
                                                                    processing_layer_id = processing_layer_id,
                                                                    processing_layer_name = int_processing_layer_name,
                                                                    m_source_id = int_source_id,
                                                                    m_processing_layer_id = m_processing_layer_id,
                                                                    m_processing_sub_layer_id = m_processing_sub_layer_id,
                                                                    source_type = 'FILE' ,
                                                                    extraction_type = "UPLOAD",
                                                                    file_name = internal_file_name_proper,
                                                                    file_size_bytes = internal_file_size,
                                                                    file_path = internal_file_upload_path_name_date,
                                                                    status = "BATCH",
                                                                    comments = "File in Batch!!!",
                                                                    is_processed = 0,
                                                                    is_processing = 0,
                                                                    is_active = 1,
                                                                    created_by = user_id,
                                                                    created_date = str(datetime.today()),
                                                                    modified_by = user_id,
                                                                    modified_date = str(datetime.today())
                                                                )

                                                                ReconFileUploads.objects.create(
                                                                    tenants_id = tenant_id,
                                                                    groups_id = group_id,
                                                                    entities_id = entity_id,
                                                                    processing_layer_id = processing_layer_id,
                                                                    processing_layer_name = ext_processing_layer_name,
                                                                    m_source_id = ext_source_id,
                                                                    m_processing_layer_id = m_processing_layer_id,
                                                                    m_processing_sub_layer_id = m_processing_sub_layer_id,
                                                                    source_type = 'FILE',
                                                                    extraction_type = "UPLOAD",
                                                                    file_name = external_file_name_proper,
                                                                    file_size_bytes = external_file_size,
                                                                    file_path = external_file_upload_path_name_date,
                                                                    status = "BATCH",
                                                                    comments = "File in Batch!!!",
                                                                    is_processed = 0,
                                                                    is_processing = 0,
                                                                    is_active = 1,
                                                                    created_by = user_id,
                                                                    created_date = str(datetime.today()),
                                                                    modified_by = user_id,
                                                                    modified_date = str(datetime.today())
                                                                )
                                                                return JsonResponse({"Status": "Success","Message": "File Uploaded Sucessfully!!!"})
                                                            else:
                                                                logger.error("Error in Getting the input and external file locations!!!")
                                                                return JsonResponse({"Status": "Error"})
                                                        else:
                                                            return JsonResponse({"Status": "File Exists", "Message": "Already File Exists with Choosen relationship!!!"})

                                                    elif file_uploaded == "INTERNAL":
                                                        file_locations = content_data["file_locations"]
                                                        internal_file_name = request.FILES["internalFileName"].name
                                                        internal_file_name_proper = get_proper_file_name(internal_file_name)

                                                        internal_file_location = ''

                                                        for file_location in file_locations :
                                                            if file_location['side'] == "Internal" :
                                                                int_source_id = file_location['source_id']
                                                                internal_file_location = file_location['input_location']
                                                                int_processing_layer_name = file_location['processing_layer_name']

                                                        file_uploads_internal = ReconFileUploads.objects.filter(m_source_id = int_source_id, is_processed = 0)
                                                        internal_file_upload_ids = []
                                                        for internal_file in file_uploads_internal:
                                                            internal_file_upload_ids.append(internal_file.m_source_id)

                                                        if len(internal_file_upload_ids) == 0:
                                                            if len(internal_file_location) > 0:
                                                                internal_file_upload_path_name_date = internal_file_location + internal_file_name_proper
                                                                internal_file_paths = get_proper_paths(internal_file_location)

                                                                if not os.path.exists(internal_file_paths[0]):
                                                                    os.mkdir(internal_file_paths[0])
                                                                if not os.path.exists(internal_file_paths[1]):
                                                                    os.mkdir(internal_file_paths[1])

                                                                with open(internal_file_upload_path_name_date, 'wb+') as destination:
                                                                    for chunk in request.FILES["internalFileName"]:
                                                                        destination.write(chunk)
                                                                internal_file_size = Path(internal_file_upload_path_name_date).stat().st_size

                                                                # TODO : Add row_count also in the below Table

                                                                ReconFileUploads.objects.create(
                                                                    tenants_id = tenant_id,
                                                                    groups_id = group_id,
                                                                    entities_id = entity_id,
                                                                    processing_layer_id = processing_layer_id,
                                                                    processing_layer_name = int_processing_layer_name,
                                                                    m_source_id = int_source_id,
                                                                    m_processing_layer_id = m_processing_layer_id,
                                                                    m_processing_sub_layer_id = m_processing_sub_layer_id,
                                                                    source_type = 'FILE' ,
                                                                    extraction_type = "UPLOAD",
                                                                    file_name = internal_file_name_proper,
                                                                    file_size_bytes = internal_file_size,
                                                                    file_path = internal_file_upload_path_name_date,
                                                                    status = "BATCH",
                                                                    comments = "File in Batch!!!",
                                                                    is_processed = 0,
                                                                    is_processing = 0,
                                                                    is_active = 1,
                                                                    created_by = user_id,
                                                                    created_date = str(datetime.today()),
                                                                    modified_by = user_id,
                                                                    modified_date = str(datetime.today())
                                                                )
                                                                return JsonResponse({"Status": "Success", "Message": "File Uploaded Sucessfully!!!"})
                                                            else:
                                                                logger.error("Error in Getting the Internal file locations!!!")
                                                                return JsonResponse({"Status": "Error"})
                                                        else:
                                                            return JsonResponse({"Status": "File Exists", "Message": "Already File Exists with Choosen relationship!!!"})

                                                    elif file_uploaded == "EXTERNAL":
                                                        file_locations = content_data["file_locations"]
                                                        external_file_name = request.FILES["externalFileName"].name

                                                        external_file_name_proper = get_proper_file_name(external_file_name)
                                                        external_file_location = ''

                                                        for file_location in file_locations :
                                                            if file_location['side'] == "External" :
                                                                external_file_location = file_location['input_location']
                                                                ext_source_id = file_location['source_id']
                                                                ext_processing_layer_name = file_location['processing_layer_name']

                                                        file_uploads_external = ReconFileUploads.objects.filter(m_source_id = ext_source_id, is_processed = 0)
                                                        external_file_upload_ids = []
                                                        for external_file in file_uploads_external:
                                                            external_file_upload_ids.append(external_file.m_source_id)

                                                        if len(external_file_upload_ids) == 0:
                                                            if len(external_file_location) > 0:
                                                                external_file_upload_path_name_date = external_file_location + external_file_name_proper
                                                                external_file_paths = get_proper_paths(external_file_location)

                                                                if not os.path.exists(external_file_paths[0]):
                                                                    os.mkdir(external_file_paths[0])
                                                                if not os.path.exists(external_file_paths[1]):
                                                                    os.mkdir(external_file_paths[1])

                                                                with open(external_file_upload_path_name_date, 'wb+') as destination:
                                                                    for chunk in request.FILES["externalFileName"]:
                                                                        destination.write(chunk)
                                                                external_file_size = Path(external_file_upload_path_name_date).stat().st_size

                                                                # TODO : Add row_count also in the below Table

                                                                ReconFileUploads.objects.create(
                                                                    tenants_id = tenant_id,
                                                                    groups_id = group_id,
                                                                    entities_id = entity_id,
                                                                    processing_layer_id = processing_layer_id,
                                                                    processing_layer_name = ext_processing_layer_name,
                                                                    m_source_id = ext_source_id,
                                                                    m_processing_layer_id = m_processing_layer_id,
                                                                    m_processing_sub_layer_id = m_processing_sub_layer_id,
                                                                    source_type = 'FILE',
                                                                    extraction_type = "UPLOAD",
                                                                    file_name = external_file_name_proper,
                                                                    file_size_bytes = external_file_size,
                                                                    file_path = external_file_upload_path_name_date,
                                                                    status = "BATCH",
                                                                    comments = "File in Batch!!!",
                                                                    is_processed = 0,
                                                                    is_processing = 0,
                                                                    is_active = 1,
                                                                    created_by = user_id,
                                                                    created_date = str(datetime.today()),
                                                                    modified_by = user_id,
                                                                    modified_date = str(datetime.today())
                                                                )
                                                                return JsonResponse({"Status": "Success", "Message": "File Uploaded Sucessfully!!!"})
                                                            else:
                                                                logger.error("Error in Getting the External file locations!!!")
                                                                return JsonResponse({"Status": "Error"})
                                                        else:
                                                            return JsonResponse({"Status": "File Exists", "Message": "Already File Exists with Choosen relationship!!!"})
                                                    else:
                                                        return JsonResponse({"Status": "Error", "Message": "Unknown File Upload Tye Found!!!"})
                                                elif content_data["Status"] == "Error":
                                                    logger.error("Error in Getting Processing Layer Definition List from Recon ETL Service!!!")
                                                    return JsonResponse({"Status": "Error"})
                                            else:
                                                return JsonResponse({"Status": "Error", "Message": "File Uploaded Not Found!!!"})
                                        else:
                                            return JsonResponse({"Status": "Error", "Message": "User Id Not Found!!!"})
                                    else:
                                        return JsonResponse({"Status": "Error", "Message": "Unmatched Status Not Found!!!"})
                                else:
                                    return JsonResponse({"Status": "Error", "Message": "Processing Layer Id Not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "M Processing Sub Layer Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "M Processing Layer Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id not Found!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in File Upload !!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_transaction_count(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            m_processing_layer_id = 0
            m_processing_sub_layer_id = 0
            processing_layer_id = 0

            for k,v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "m_processing_layer_id":
                    m_processing_layer_id = v
                if  k == "m_processing_sub_layer_id":
                    m_processing_sub_layer_id = v
                if k == "processing_layer_id":
                    processing_layer_id = v

            if int(tenant_id) > 0:
                if int(group_id) > 0:
                    if int(entity_id) > 0:
                        if int(m_processing_layer_id) > 0:
                            if int(m_processing_sub_layer_id) > 0:
                                if int(processing_layer_id) > 0:
                                    reco_settings_external = RecoSettings.objects.filter(setting_key = 'ext_count_all', is_active = 1, tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id)
                                    reco_settings_internal = RecoSettings.objects.filter(setting_key = 'int_count_all', is_active = 1, tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id)
                                    # reco_settings_external_not_closed = SettingQueries.objects.filter(setting_key = 'ext_count_all_not_closed', is_active = 1)
                                    # reco_settings_internal_not_closed = SettingQueries.objects.filter(setting_key = 'int_count_all_not_closed', is_active = 1)

                                    for setting in reco_settings_external:
                                        external_count = setting.setting_value

                                    for setting in reco_settings_internal:
                                        internal_count = setting.setting_value

                                    # for setting in reco_settings_external_not_closed:
                                    #     external_count_not_closed_query = setting.setting_value
                                    #
                                    # for setting in reco_settings_internal_not_closed:
                                    #     internal_count_not_closed_query = setting.setting_value

                                    external_count_proper = external_count.replace(
                                        "{tenants_id}", str(tenant_id)).replace("{groups_id}", str(group_id)).replace(
                                        "{entities_id}", str(entity_id)).replace(
                                        "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                        "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                        "{processing_layer_id}", str(processing_layer_id)).replace(
                                        "{conditions}", "")

                                    internal_count_proper = internal_count.replace(
                                        "{tenants_id}", str(tenant_id)).replace("{groups_id}", str(group_id)).replace(
                                        "{entities_id}", str(entity_id)).replace(
                                        "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                        "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                        "{processing_layer_id}", str(processing_layer_id)).replace(
                                        "{conditions}", "")

                                    # Matched
                                    matched_count_external_query = external_count_proper.replace("{processing_status_1}", "Matched")
                                    matched_count_internal_query = internal_count_proper.replace("{processing_status_1}", "Matched")
                                    matched_count_external_out = execute_sql_query(matched_count_external_query, object_type="data")
                                    matched_count_internal_out = execute_sql_query(matched_count_internal_query, object_type="data")
                                    matched_count_overall = int(matched_count_external_out["data"][0]["external_count"]) + int(matched_count_internal_out["data"][0]["internal_count"])

                                    # Unmatched
                                    unmatched_count_external_query = external_count_proper.replace("{processing_status_1}", "UnMatched")
                                    unmatched_count_internal_query = internal_count_proper.replace("{processing_status_1}", "UnMatched")
                                    unmatched_count_external_out = execute_sql_query(unmatched_count_external_query, object_type="data")
                                    unmatched_count_internal_out = execute_sql_query(unmatched_count_internal_query, object_type="data")
                                    unmatched_count_overall = int(unmatched_count_external_out["data"][0]["external_count"]) + int(unmatched_count_internal_out["data"][0]["internal_count"])

                                    # Group Matched
                                    grp_matched_count_external_query = external_count_proper.replace("{processing_status_1}", "GroupMatched")
                                    grp_matched_count_internal_query = internal_count_proper.replace("{processing_status_1}", "GroupMatched")
                                    grp_matched_count_external_out = execute_sql_query(grp_matched_count_external_query,object_type="data")
                                    grp_matched_count_internal_out = execute_sql_query(grp_matched_count_internal_query, object_type="data")
                                    grp_matched_count_overall = int(grp_matched_count_external_out["data"][0]["external_count"]) + int(grp_matched_count_internal_out["data"][0]["internal_count"])

                                    # Group Unmatched
                                    grp_unmatched_count_external_query = external_count_proper.replace("{processing_status_1}", "GroupUnMatched")
                                    grp_unmatched_count_internal_query = internal_count_proper.replace("{processing_status_1}", "GroupUnMatched")
                                    grp_unmatched_count_external_out = execute_sql_query(grp_unmatched_count_external_query,object_type="data")
                                    grp_unmatched_count_internal_out = execute_sql_query(grp_unmatched_count_internal_query, object_type="data")
                                    grp_unmatched_count_overall = int(grp_unmatched_count_external_out["data"][0]["external_count"]) + int(grp_unmatched_count_internal_out["data"][0]["internal_count"])

                                    # Contra
                                    contra_count_external_query = external_count_proper.replace("{processing_status_1}", "Contra")
                                    contra_count_internal_query = internal_count_proper.replace("{processing_status_1}", "Contra")
                                    contra_count_external_out = execute_sql_query(contra_count_external_query, object_type="data")
                                    contra_count_internal_out = execute_sql_query(contra_count_internal_query, object_type="data")
                                    contra_count_overall = int(contra_count_external_out["data"][0]["external_count"]) + int(contra_count_internal_out["data"][0]["internal_count"])

                                    # ALL
                                    # external_count_not_closed_query_proper = external_count_not_closed_query.replace("{tenants_id}", str(tenant_id)).\
                                    #     replace("{groups_id}", str(group_id)).replace("{entities_id}", str(entity_id)).replace("{m_processing_layer_id}", str(m_processing_layer_id)).\
                                    #     replace("{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace("{processing_layer_id}", str(processing_layer_id))
                                    # internal_count_not_closed_query_proper = internal_count_not_closed_query.replace("{tenants_id}", str(tenant_id)).\
                                    #     replace("{groups_id}", str(group_id)).replace("{entities_id}", str(entity_id)).replace("{m_processing_layer_id}", str(m_processing_layer_id)).\
                                    #     replace("{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace("{processing_layer_id}", str(processing_layer_id))
                                    # external_count_not_closed_query_output = execute_sql_query(external_count_not_closed_query_proper, object_type="data")
                                    # internal_count_not_closed_query_output = execute_sql_query(internal_count_not_closed_query_proper, object_type="data")
                                    # count_not_closed_overall = int(external_count_not_closed_query_output["data"][0]["external_count"]) + int(internal_count_not_closed_query_output["data"][0]["internal_count"])
                                    count_not_closed_overall = 0

                                    return  JsonResponse(
                                        {
                                            "label" : ["Matched", "UnMatched", "GroupMatched", "GroupUnMatched", "Contra"],
                                            "data" : [matched_count_overall, unmatched_count_overall, grp_matched_count_overall, grp_unmatched_count_overall, contra_count_overall, count_not_closed_overall]
                                            # "Matched": matched_count_overall,
                                            # "UnMatched": unmatched_count_overall,
                                            # "GroupMatched": grp_matched_count_overall,
                                            # "GroupUnMatched": grp_unmatched_count_overall,
                                            # "Contra": contra_count_overall
                                        }
                                    )
                                elif int(processing_layer_id) == 0:
                                    reco_settings_external = RecoSettings.objects.filter(setting_key='ext_count_all',is_active=1)
                                    reco_settings_internal = RecoSettings.objects.filter(setting_key='int_count_all',is_active=1)

                                    for setting in reco_settings_external:
                                        external_count = setting.setting_value

                                    for setting in reco_settings_internal:
                                        internal_count = setting.setting_value

                                    external_count_proper = external_count.replace(
                                        "{tenants_id}", str(tenant_id)).replace("{groups_id}", str(group_id)).replace(
                                        "{entities_id}", str(entity_id)).replace(
                                        "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                        "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                        "AND processing_layer_id = {processing_layer_id}", "").replace(
                                        "{conditions}", "")

                                    internal_count_proper = internal_count.replace(
                                        "{tenants_id}", str(tenant_id)).replace("{groups_id}", str(group_id)).replace(
                                        "{entities_id}", str(entity_id)).replace(
                                        "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                        "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                        "AND processing_layer_id = {processing_layer_id}", "").replace(
                                        "{conditions}", "")

                                    # Matched
                                    matched_count_external_query = external_count_proper.replace("{processing_status_1}", "Matched")
                                    matched_count_internal_query = internal_count_proper.replace("{processing_status_1}", "Matched")
                                    matched_count_external_out = execute_sql_query(matched_count_external_query,object_type="data")
                                    matched_count_internal_out = execute_sql_query(matched_count_internal_query,object_type="data")
                                    #print(matched_count_external_out)
                                    #print(matched_count_internal_out)
                                    matched_count_overall = int(matched_count_external_out["data"][0]["external_count"]) + int(matched_count_internal_out["data"][0]["internal_count"])

                                    # Unmatched
                                    unmatched_count_external_query = external_count_proper.replace("{processing_status_1}", "UnMatched")
                                    unmatched_count_internal_query = internal_count_proper.replace("{processing_status_1}", "UnMatched")
                                    unmatched_count_external_out = execute_sql_query(unmatched_count_external_query,object_type="data")
                                    unmatched_count_internal_out = execute_sql_query(unmatched_count_internal_query,object_type="data")
                                    unmatched_count_overall = int(unmatched_count_external_out["data"][0]["external_count"]) + int(unmatched_count_internal_out["data"][0]["internal_count"])

                                    # Group Matched
                                    grp_matched_count_external_query = external_count_proper.replace("{processing_status_1}", "GroupMatched")
                                    grp_matched_count_internal_query = internal_count_proper.replace("{processing_status_1}", "GroupMatched")
                                    grp_matched_count_external_out = execute_sql_query(grp_matched_count_external_query,object_type="data")
                                    grp_matched_count_internal_out = execute_sql_query(grp_matched_count_internal_query,object_type="data")
                                    grp_matched_count_overall = int(grp_matched_count_external_out["data"][0]["external_count"]) + int(grp_matched_count_internal_out["data"][0]["internal_count"])

                                    # Group Unmatched
                                    grp_unmatched_count_external_query = external_count_proper.replace("{processing_status_1}", "GroupUnMatched")
                                    grp_unmatched_count_internal_query = internal_count_proper.replace("{processing_status_1}", "GroupUnMatched")
                                    grp_unmatched_count_external_out = execute_sql_query(grp_unmatched_count_external_query, object_type="data")
                                    grp_unmatched_count_internal_out = execute_sql_query(grp_unmatched_count_internal_query, object_type="data")
                                    grp_unmatched_count_overall = int(grp_unmatched_count_external_out["data"][0]["external_count"]) + int(grp_unmatched_count_internal_out["data"][0]["internal_count"])

                                    # Contra
                                    contra_count_external_query = external_count_proper.replace("{processing_status_1}","Contra")
                                    contra_count_internal_query = internal_count_proper.replace("{processing_status_1}","Contra")
                                    contra_count_external_out = execute_sql_query(contra_count_external_query,object_type="data")
                                    contra_count_internal_out = execute_sql_query(contra_count_internal_query,object_type="data")
                                    contra_count_overall = int(contra_count_external_out["data"][0]["external_count"]) + int(contra_count_internal_out["data"][0]["internal_count"])

                                    return JsonResponse(
                                        {
                                            "label": ["Matched", "UnMatched", "GroupMatched", "GroupUnMatched","Contra"],
                                            "data": [matched_count_overall, unmatched_count_overall,grp_matched_count_overall, grp_unmatched_count_overall,contra_count_overall]
                                            # "Matched": matched_count_overall,
                                            # "UnMatched": unmatched_count_overall,
                                            # "GroupMatched": grp_matched_count_overall,
                                            # "GroupUnMatched": grp_unmatched_count_overall,
                                            # "Contra": contra_count_overall
                                        }
                                    )
                                else:
                                    return JsonResponse({"Status": "Error", "Message": "Processing Layer Id Not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "M Processing Sub Layer Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "M Processing Layer Id Not Found!!!"})
                    else:
                            return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                        return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                    return JsonResponse({"Status": "Error", "Message": "Tenant Id not Found!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in Getting Transaction Count!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_transaction_records(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            m_processing_layer_id = 0
            m_processing_sub_layer_id = 0
            processing_layer_id = 0
            record_status = ''

            for k,v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "m_processing_layer_id":
                    m_processing_layer_id = v
                if  k == "m_processing_sub_layer_id":
                    m_processing_sub_layer_id = v
                if k == "processing_layer_id":
                    processing_layer_id = v
                if k == "record_status":
                    record_status = v

            if int(tenant_id) > 0:
                if int(group_id) > 0:
                    if int(entity_id) > 0:
                        if int(m_processing_layer_id) > 0:
                            if int(m_processing_sub_layer_id) > 0:
                                if int(processing_layer_id) > 0:
                                    if record_status in ["Matched", "UnMatched", "Contra"]:
                                        reco_settings_external = RecoSettings.objects.filter(setting_key = 'ext_select_query_all', is_active = 1, tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id)
                                        reco_settings_internal = RecoSettings.objects.filter(setting_key = 'int_select_query_all', is_active = 1, tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id)

                                        setting_header_external = RecoSettings.objects.filter(setting_key = 'ext_header_all', is_active = 1, tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id)
                                        setting_header_internal = RecoSettings.objects.filter(setting_key = 'int_header_all', is_active = 1, tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id)

                                        for setting in reco_settings_external:
                                            external_select_query = setting.setting_value

                                        for setting in reco_settings_internal:
                                            internal_select_query = setting.setting_value

                                        for setting in setting_header_external:
                                            header_external = json.loads(setting.setting_value)

                                        for setting in setting_header_internal:
                                            header_internal = json.loads(setting.setting_value)

                                        external_select_query_proper = external_select_query.replace(
                                            "{tenants_id}", str(tenant_id)).replace("{groups_id}", str(group_id)).replace(
                                            "{entities_id}", str(entity_id)).replace(
                                            "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                            "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                            "{processing_layer_id}", str(processing_layer_id)).replace(
                                            "{processing_status_1}", record_status).replace("{conditions}", " ORDER BY ext_reference_date_time_1 ASC")
                                        # print(external_select_query_proper)
                                        internal_select_query_proper = internal_select_query.replace(
                                            "{tenants_id}", str(tenant_id)).replace("{groups_id}", str(group_id)).replace(
                                            "{entities_id}", str(entity_id)).replace(
                                            "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                            "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                            "{processing_layer_id}", str(processing_layer_id)).replace(
                                            "{processing_status_1}", record_status).replace("{conditions}", " ORDER BY int_reference_date_time_1 ASC")
                                        # print(internal_select_query_proper)
                                        external_query_out = json.loads(execute_sql_query(external_select_query_proper, object_type="table"))
                                        internal_query_out = json.loads(execute_sql_query(internal_select_query_proper, object_type="table"))

                                        external_query_out["headers"] = get_grid_transform(external_query_out, header_external)
                                        internal_query_out["headers"] = get_grid_transform(internal_query_out, header_internal)

                                        reco_settings = RecoSettings.objects.filter(
                                            tenants_id=tenant_id,
                                            groups_id=group_id,
                                            entities_id=entity_id,
                                            m_processing_layer_id=m_processing_layer_id,
                                            m_processing_sub_layer_id=m_processing_sub_layer_id,
                                            processing_layer_id=processing_layer_id,
                                            setting_key='amount_tolerance'
                                        )

                                        for setting in reco_settings:
                                            amount_tolerance = setting.setting_value

                                        return JsonResponse({
                                            "Status": "Success",
                                            "external_records" : external_query_out,
                                            "internal_records" : internal_query_out,
                                            "amount_tolerance" : amount_tolerance
                                        })
                                    elif record_status in ["GroupMatched"]:
                                        reco_settings_external = RecoSettings.objects.filter(setting_key='ext_select_query_group_matched', is_active = 1, tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id)
                                        reco_settings_internal = RecoSettings.objects.filter(setting_key='int_select_query_group_matched', is_active = 1, tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id)

                                        setting_header_external = RecoSettings.objects.filter(setting_key='ext_header_all', is_active = 1, tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id)
                                        setting_header_internal = RecoSettings.objects.filter(setting_key='int_header_all', is_active = 1, tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id)

                                        for setting in reco_settings_external:
                                            external_select_query = setting.setting_value

                                        for setting in reco_settings_internal:
                                            internal_select_query = setting.setting_value

                                        for setting in setting_header_external:
                                            header_external = json.loads(setting.setting_value)

                                        for setting in setting_header_internal:
                                            header_internal = json.loads(setting.setting_value)

                                        external_select_query_proper = external_select_query.replace(
                                            "{tenants_id}", str(tenant_id)).replace("{groups_id}",
                                                                                    str(group_id)).replace(
                                            "{entities_id}", str(entity_id)).replace(
                                            "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                            "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                            "{processing_layer_id}", str(processing_layer_id)).replace(
                                            "{processing_status_1}", record_status).replace("{conditions}", " ORDER BY ext_reference_date_time_1 ASC")

                                        internal_select_query_proper = internal_select_query.replace(
                                            "{tenants_id}", str(tenant_id)).replace("{groups_id}",
                                                                                    str(group_id)).replace(
                                            "{entities_id}", str(entity_id)).replace(
                                            "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                            "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                            "{processing_layer_id}", str(processing_layer_id)).replace(
                                            "{processing_status_1}", record_status).replace("{conditions}", " ORDER BY int_reference_date_time_1 ASC")

                                        external_query_out = json.loads(execute_sql_query(external_select_query_proper, object_type="table"))
                                        internal_query_out = json.loads(execute_sql_query(internal_select_query_proper, object_type="table"))

                                        external_query_out["headers"] = get_grid_transform(external_query_out, header_external)
                                        internal_query_out["headers"] = get_grid_transform(internal_query_out, header_internal)

                                        reco_settings = RecoSettings.objects.filter(
                                            tenants_id=tenant_id,
                                            groups_id=group_id,
                                            entities_id=entity_id,
                                            m_processing_layer_id=m_processing_layer_id,
                                            m_processing_sub_layer_id=m_processing_sub_layer_id,
                                            processing_layer_id=processing_layer_id,
                                            setting_key='amount_tolerance'
                                        )

                                        for setting in reco_settings:
                                            amount_tolerance = setting.setting_value

                                        return JsonResponse({
                                            "Status": "Success",
                                            "external_records": external_query_out,
                                            "internal_records": internal_query_out,
                                            "amount_tolerance": amount_tolerance
                                        })

                                    elif record_status in ["GroupUnMatched"]:
                                        reco_settings_external = RecoSettings.objects.filter(setting_key='ext_select_query_group_unmatched', is_active=1, tenants_id=tenant_id, groups_id=group_id, entities_id=entity_id, m_processing_layer_id=m_processing_layer_id, m_processing_sub_layer_id=m_processing_sub_layer_id, processing_layer_id=processing_layer_id)
                                        reco_settings_internal = RecoSettings.objects.filter(setting_key='int_select_query_group_unmatched', is_active=1, tenants_id=tenant_id, groups_id=group_id, entities_id=entity_id, m_processing_layer_id=m_processing_layer_id, m_processing_sub_layer_id=m_processing_sub_layer_id, processing_layer_id=processing_layer_id)

                                        setting_header_external = RecoSettings.objects.filter(setting_key='ext_header_all', is_active=1, tenants_id=tenant_id, groups_id=group_id, entities_id=entity_id, m_processing_layer_id=m_processing_layer_id, m_processing_sub_layer_id=m_processing_sub_layer_id, processing_layer_id=processing_layer_id)
                                        setting_header_internal = RecoSettings.objects.filter(setting_key='int_header_all', is_active=1, tenants_id=tenant_id, groups_id=group_id, entities_id=entity_id, m_processing_layer_id=m_processing_layer_id, m_processing_sub_layer_id=m_processing_sub_layer_id, processing_layer_id=processing_layer_id)

                                        for setting in reco_settings_external:
                                            external_select_query = setting.setting_value

                                        for setting in reco_settings_internal:
                                            internal_select_query = setting.setting_value

                                        for setting in setting_header_external:
                                            header_external = json.loads(setting.setting_value)

                                        for setting in setting_header_internal:
                                            header_internal = json.loads(setting.setting_value)

                                        external_select_query_proper = external_select_query.replace(
                                            "{tenants_id}", str(tenant_id)).replace("{groups_id}",
                                                                                    str(group_id)).replace(
                                            "{entities_id}", str(entity_id)).replace(
                                            "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                            "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                            "{processing_layer_id}", str(processing_layer_id)).replace(
                                            "{processing_status_1}", record_status).replace("{conditions}", " ORDER BY ext_reference_date_time_1 ASC")

                                        # print("external_select_query_proper")
                                        # print(external_select_query_proper)

                                        internal_select_query_proper = internal_select_query.replace(
                                            "{tenants_id}", str(tenant_id)).replace("{groups_id}",
                                                                                    str(group_id)).replace(
                                            "{entities_id}", str(entity_id)).replace(
                                            "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                            "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                            "{processing_layer_id}", str(processing_layer_id)).replace(
                                            "{processing_status_1}", record_status).replace("{conditions}", " ORDER BY int_reference_date_time_1 ASC")

                                        external_query_out = json.loads(execute_sql_query(external_select_query_proper, object_type="table"))
                                        internal_query_out = json.loads(execute_sql_query(internal_select_query_proper, object_type="table"))

                                        external_query_out["headers"] = get_grid_transform(external_query_out, header_external)
                                        internal_query_out["headers"] = get_grid_transform(internal_query_out, header_internal)

                                        reco_settings = RecoSettings.objects.filter(
                                            tenants_id=tenant_id,
                                            groups_id=group_id,
                                            entities_id=entity_id,
                                            m_processing_layer_id=m_processing_layer_id,
                                            m_processing_sub_layer_id=m_processing_sub_layer_id,
                                            processing_layer_id=processing_layer_id,
                                            setting_key='amount_tolerance'
                                        )

                                        for setting in reco_settings:
                                            amount_tolerance = setting.setting_value

                                        return JsonResponse({
                                            "Status": "Success",
                                            "external_records": external_query_out,
                                            "internal_records": internal_query_out,
                                            "amount_tolerance": amount_tolerance
                                        })

                                    elif record_status in ["All"]:
                                        reco_settings_external = RecoSettings.objects.filter(setting_key = 'ext_select_query_all_not_closed', tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, is_active = 1)
                                        reco_settings_internal = RecoSettings.objects.filter(setting_key = 'int_select_query_all_not_closed', tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, is_active = 1)

                                        setting_header_external = RecoSettings.objects.filter(setting_key='ext_header_all_not_closed', is_active=1, tenants_id=tenant_id, groups_id=group_id, entities_id=entity_id, m_processing_layer_id=m_processing_layer_id, m_processing_sub_layer_id=m_processing_sub_layer_id, processing_layer_id=processing_layer_id)
                                        setting_header_internal = RecoSettings.objects.filter(setting_key='int_header_all', is_active=1, tenants_id=tenant_id, groups_id=group_id, entities_id=entity_id, m_processing_layer_id=m_processing_layer_id, m_processing_sub_layer_id=m_processing_sub_layer_id, processing_layer_id=processing_layer_id)

                                        for setting in reco_settings_external:
                                            external_select_query = setting.setting_value

                                        for setting in reco_settings_internal:
                                            internal_select_query = setting.setting_value

                                        for setting in setting_header_external:
                                            header_external = json.loads(setting.setting_value)

                                        for setting in setting_header_internal:
                                            header_internal = json.loads(setting.setting_value)

                                        external_select_query_proper = external_select_query.replace(
                                            "{tenants_id}", str(tenant_id)).replace("{groups_id}",
                                                                                    str(group_id)).replace(
                                            "{entities_id}", str(entity_id)).replace(
                                            "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                            "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                            "{processing_layer_id}", str(processing_layer_id))

                                        internal_select_query_proper = internal_select_query.replace(
                                            "{tenants_id}", str(tenant_id)).replace("{groups_id}",
                                                                                    str(group_id)).replace(
                                            "{entities_id}", str(entity_id)).replace(
                                            "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                            "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                            "{processing_layer_id}", str(processing_layer_id))

                                        external_query_out = json.loads(execute_sql_query(external_select_query_proper, object_type="table"))
                                        internal_query_out = json.loads(execute_sql_query(internal_select_query_proper, object_type="table"))

                                        external_query_out["headers"] = get_grid_transform(external_query_out, header_external)
                                        internal_query_out["headers"] = get_grid_transform(internal_query_out, header_internal)

                                        return JsonResponse({
                                            "Status": "Success",
                                            "external_records": external_query_out,
                                            "internal_records": internal_query_out
                                        })

                                    else:
                                        return JsonResponse({"Status": "Error", "Message": "Proper Record Status not Found!!!"})
                                else:
                                    return JsonResponse({"Status": "Error", "Message": "Processing Layer Id Not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "M Processing Sub Layer Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "M Processing Layer Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id not Found!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in Getting Transaction Records!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_int_ext_transactions(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            m_processing_layer_id = 0
            m_processing_sub_layer_id = 0
            processing_layer_id = 0
            external_records_id = 0
            internal_records_id = 0
            external_group_id  = 0
            internal_group_id = 0
            external_contra_id = 0
            internal_contra_id = 0
            t_internal_records_id=0
            t_external_records_id=0
            reco_results_ext=None
            reco_results_ext_group=None
            reco_results_ext_contra=None
            reco_results_int_contra=None
            t_generated_number_1=0
            t_generated_number_2=0
            ext_condition=''
            int_condition=''
            ext_contra=[]
            int_contra = []

            for k, v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "m_processing_layer_id":
                    m_processing_layer_id = v
                if k == "m_processing_sub_layer_id":
                    m_processing_sub_layer_id = v
                if k == "processing_layer_id":
                    processing_layer_id = v
                if  k == "external_records_id":
                    external_records_id = v
                if k == "internal_records_id":
                    internal_records_id = v
                if  k == "external_group_id":
                    external_group_id = v
                if  k == "internal_group_id":
                    internal_group_id = v
                if  k == "external_contra_id":
                    external_contra_id = v
                if  k == "internal_contra_id":
                    internal_contra_id = v


            if int(tenant_id) > 0:
                if int(group_id) > 0:
                    if int(entity_id) > 0:
                        if int(m_processing_layer_id) > 0:
                            if int(m_processing_sub_layer_id) > 0:
                                if int(processing_layer_id) > 0:
                                    if int(external_records_id) > 0 :
                                        reco_results_ext = RecoResults.objects.get(t_external_records_id=external_records_id, is_active=1)
                                    elif int(internal_records_id) > 0:
                                        reco_results_ext = RecoResults.objects.get(t_internal_records_id=internal_records_id, is_active=1)
                                    elif int(external_group_id) > 0 :
                                        reco_results_ext_group = TransactionExternalRecords.objects.get(external_records_id=external_group_id, is_active=1)
                                        t_generated_number_1 =  reco_results_ext_group.ext_generated_number_1
                                    elif int(internal_group_id) > 0:
                                        reco_results_ext_group = TransactionInternalRecords.objects.get(internal_records_id=internal_group_id, is_active=1)
                                        t_generated_number_1 = reco_results_ext_group.int_generated_number_1
                                    if int(external_contra_id) > 0 :
                                        reco_results_ext_contra = TransactionExternalRecords.objects.get(external_records_id=external_contra_id, is_active=1)
                                        ext_contra.append(reco_results_ext_contra.external_records_id)
                                        ext_contra.append(reco_results_ext_contra.ext_contra_id)
                                    elif int(internal_contra_id) > 0:
                                        reco_results_int_contra = TransactionInternalRecords.objects.get(internal_records_id=internal_contra_id, is_Active=1)
                                        int_contra.append(reco_results_int_contra.internal_records_id)
                                        int_contra.append(reco_results_int_contra.int_contra_id)

                                    if(reco_results_ext is not None) :
                                        t_external_records_id = reco_results_ext.t_external_records_id
                                        t_internal_records_id = reco_results_ext.t_internal_records_id
                                        ext_condition = "AND external_records_id =" + str(t_external_records_id)
                                        int_condition = "AND internal_records_id =" + str(t_internal_records_id)
                                    elif(reco_results_ext_group is not None) :
                                        ext_condition = "AND ext_generated_number_1 =" + str(t_generated_number_1)
                                        int_condition = "AND int_generated_number_1 =" + str(t_generated_number_1)
                                    elif(reco_results_ext_contra is not None) :
                                        ext_condition = "AND external_records_id in "+str(ext_contra)
                                    elif(reco_results_int_contra is not None) :
                                        intcontrastr=''
                                        for intcontra in int_contra:
                                            intcontrastr=str(intcontra)+","
                                        intcontrastr=intcontrastr[:-1]
                                        int_condition = "AND internal_records_id in ("+intcontrastr+")"

                                    reco_settings_external = RecoSettings.objects.filter(setting_key='ext_select_query_all', is_active=1)
                                    reco_settings_internal = RecoSettings.objects.filter(setting_key='int_select_query_all', is_active=1)

                                    setting_header_external = RecoSettings.objects.filter(setting_key='ext_header_all', is_active=1)
                                    setting_header_internal = RecoSettings.objects.filter(setting_key='int_header_all', is_active=1)

                                    for setting in reco_settings_external:
                                        external_select_query = setting.setting_value

                                    for setting in reco_settings_internal:
                                        internal_select_query = setting.setting_value

                                    for setting in setting_header_external:
                                        header_external = json.loads(setting.setting_value)

                                    for setting in setting_header_internal:
                                        header_internal = json.loads(setting.setting_value)

                                    external_select_query_proper = external_select_query.replace(
                                        "{tenants_id}", str(tenant_id)).replace("{groups_id}", str(group_id)).replace(
                                        "{entities_id}", str(entity_id)).replace(
                                        "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                        "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                        "{processing_layer_id}", str(processing_layer_id)).replace(
                                        "AND ext_processing_status_1 = '{processing_status_1}'", "").replace(
                                        "{conditions}", ext_condition)
                                    # print(external_select_query_proper)

                                    internal_select_query_proper = internal_select_query.replace(
                                        "{tenants_id}", str(tenant_id)).replace("{groups_id}", str(group_id)).replace(
                                        "{entities_id}", str(entity_id)).replace(
                                        "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                        "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                        "{processing_layer_id}", str(processing_layer_id)).replace(
                                        "AND int_processing_status_1 = '{processing_status_1}'", "").replace(
                                        "{conditions}", int_condition)
                                    # print(internal_select_query_proper)
                                    external_query_out = json.loads(
                                        execute_sql_query(external_select_query_proper, object_type="table"))
                                    internal_query_out = json.loads(
                                        execute_sql_query(internal_select_query_proper, object_type="table"))

                                    external_query_out["headers"] = get_grid_transform(external_query_out, header_external)
                                    internal_query_out["headers"] = get_grid_transform(internal_query_out, header_internal)

                                    return JsonResponse({
                                        "Status": "Success",
                                        "external_records": external_query_out,
                                        "internal_records": internal_query_out
                                    })
                                else:
                                    return JsonResponse({"Status": "Error", "Message": "Processing Layer Id Not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "M Processing Sub Layer Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "M Processing Layer Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id not Found!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})
    except Exception:
        logger.error("Error in Getting Internal External Transaction Records!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_internal_external_headers(request, *args, **kwargs):
    try:
        body = request.body.decode('utf-8')
        data = json.loads(body)

        tenant_id = 0
        group_id = 0
        entity_id = 0
        m_processing_layer_id = 0
        m_processing_sub_layer_id = 0
        processing_layer_id = 0
        header_side = ''

        for k, v in data.items():
            if k == "tenantId":
                tenant_id = v
            if k == "groupId":
                group_id = v
            if k == "entityId":
                entity_id = v
            if k == "mProcessingLayerId":
                m_processing_layer_id = v
            if k == "mProcessingSubLayerId":
                m_processing_sub_layer_id = v
            if k == "processingLayerId":
                processing_layer_id = v
            if k == "headerSide":
                header_side = v

        if int(tenant_id) > 0:
            if int(group_id) > 0:
                if int(entity_id) > 0:
                    if int(m_processing_layer_id) > 0:
                        if int(m_processing_sub_layer_id) > 0:
                            if int(processing_layer_id) > 0:
                                if header_side == "External":
                                    reco_settings_external = RecoSettings.objects.filter(setting_key='ext_select_query_all', is_active=1, tenants_id=tenant_id, groups_id=group_id, entities_id=entity_id, m_processing_layer_id=m_processing_layer_id, m_processing_sub_layer_id=m_processing_sub_layer_id, processing_layer_id=processing_layer_id)
                                    setting_header_external = RecoSettings.objects.filter(setting_key='ext_header_all', is_active=1, tenants_id=tenant_id, groups_id=group_id, entities_id=entity_id, m_processing_layer_id=m_processing_layer_id, m_processing_sub_layer_id=m_processing_sub_layer_id, processing_layer_id=processing_layer_id)

                                    for setting in reco_settings_external:
                                        external_select_query = setting.setting_value

                                    for setting in setting_header_external:
                                        header_external = json.loads(setting.setting_value)

                                    external_select_query_proper = external_select_query.replace(
                                        "{tenants_id}", str(tenant_id)).replace("{groups_id}",
                                                                                str(group_id)).replace(
                                        "{entities_id}", str(entity_id)).replace(
                                        "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                        "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                        "{processing_layer_id}", str(processing_layer_id)).replace(
                                        "{processing_status_1}", "").replace(
                                        "{conditions}", "")
                                    # print(external_select_query_proper)
                                    external_query_out = json.loads(execute_sql_query(external_select_query_proper, object_type="table"))

                                    external_query_out["headers"] = get_grid_transform(external_query_out, header_external)

                                    return JsonResponse({"Status": "Success", "external_records": external_query_out})

                                elif header_side == "Internal":

                                    reco_settings_internal = RecoSettings.objects.filter(setting_key='int_select_query_all', is_active=1)
                                    setting_header_internal = RecoSettings.objects.filter(setting_key='int_header_all', is_active=1)

                                    for setting in reco_settings_internal:
                                        internal_select_query = setting.setting_value

                                    for setting in setting_header_internal:
                                        header_internal = json.loads(setting.setting_value)

                                    internal_select_query_proper = internal_select_query.replace(
                                        "{tenants_id}", str(tenant_id)).replace("{groups_id}",
                                                                                str(group_id)).replace(
                                        "{entities_id}", str(entity_id)).replace(
                                        "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                        "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                        "{processing_layer_id}", str(processing_layer_id)).replace(
                                        "{processing_status_1}", "").replace(
                                        "{conditions}", "")

                                    internal_query_out = json.loads(execute_sql_query(internal_select_query_proper, object_type="table"))

                                    internal_query_out["headers"] = get_grid_transform(internal_query_out, header_internal)

                                    return JsonResponse({"Status": "Success", "internal_records": internal_query_out})

                                else:
                                    return JsonResponse({"Status": "Error", "Message": "Header Side Not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "Processing Layer Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "M Processing Sub Layer Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "M Processing Layer Id Not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Entity Id Not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Group Id Not Found!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "Tenant Id Not Found!!!"})

    except Exception:
        logger.error("Error in getting Internal External Headers!!!", exc_info = True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_update_unmatched_matched(request, *args, **Kwargs):
    try:
        file_processing = ''
        file_uploads = ReconFileUploads.objects.filter(is_processing = 1)
        for file in file_uploads:
            file_processing = "FILE"

        if file_processing == "":

            body = request.body.decode('utf-8')
            data = json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            m_processing_layer_id = 0
            m_processing_sub_layer_id = 0
            processing_layer_id = 0
            user_id = 0
            external_record_id_list = None
            internal_record_id_list = None

            for k, v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "m_processing_layer_id":
                    m_processing_layer_id = v
                if k == "m_processing_sub_layer_id":
                    m_processing_sub_layer_id = v
                if k == "processing_layer_id":
                    processing_layer_id = v
                if k == "user_id":
                    user_id = v
                if k == "external_record_id_list":
                    external_record_id_list = v
                if  k == "internal_record_id_list":
                    internal_record_id_list = v
                if k == "matching_comment_id":
                    matching_comment_id = v
                if k == "matching_comment_description":
                    matching_comment_description = v

            if int(tenant_id) > 0:
                if int(group_id) > 0:
                    if int(entity_id) > 0:
                        if int(m_processing_layer_id) > 0:
                            if int(m_processing_sub_layer_id) > 0:
                                if int(processing_layer_id) > 0:
                                    if int(user_id) > 0:
                                        if external_record_id_list is not None:
                                            if internal_record_id_list is not None:

                                                reco_settings = RecoSettings.objects.filter(tenants_id=tenant_id, groups_id=group_id, entities_id=entity_id, m_processing_layer_id=m_processing_layer_id, m_processing_sub_layer_id=m_processing_sub_layer_id, processing_layer_id=processing_layer_id, setting_key='group_id')
                                                for setting in reco_settings:
                                                    group_id_value = setting.setting_value

                                                TransactionExternalRecords.objects.filter(external_records_id__in=external_record_id_list).update(ext_processing_status_1='GroupMatched', ext_match_type_1='USER-MATCHED', ext_record_status_1=1, ext_generated_number_1=group_id_value, ext_generated_number_3=matching_comment_id, ext_transaction_type_1=matching_comment_description, modified_by=user_id, modified_date=str(datetime.today()))
                                                TransactionInternalRecords.objects.filter(internal_records_id__in=internal_record_id_list).update(int_processing_status_1='GroupMatched', int_match_type_1='USER-MATCHED', int_record_status_1=1, int_generated_number_1=group_id_value, int_generated_number_3=matching_comment_id, int_transaction_type_1=matching_comment_description, modified_by=user_id, modified_date=str(datetime.today()))

                                                RecoResults.objects.create(
                                                    m_processing_layer_id=m_processing_layer_id,
                                                    m_processing_sub_layer_id=m_processing_sub_layer_id,
                                                    processing_layer_id=processing_layer_id,
                                                    generated_number_1=group_id_value,
                                                    reco_status="USER-MATCHED",
                                                    is_active=1,
                                                    created_by=user_id,
                                                    created_date=str(datetime.today()),
                                                    modified_by=user_id,
                                                    modified_date=str(datetime.today())
                                                )

                                                for setting in reco_settings:
                                                    setting.setting_value = str(int(group_id_value) + 1)
                                                    setting.save()

                                                return JsonResponse({"Status": "Success"})

                                            else:
                                                return JsonResponse({"Status" : "Error", "Message": "Internal Record Id List Not Found!!!"})
                                        else:
                                            return JsonResponse({"Status": "Error", "Message": "External Record Id List Not Found!!!"})
                                    else:
                                        return JsonResponse({"Status": "Error", "Message": "User Id Not Found!!!"})
                                else:
                                    return JsonResponse({"Status" : "Error", "Message": "Processing Layer Id Not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "M Processing Sub Layer Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "M Processing Layer Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id Not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id Not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id Not Found!!!"})
        else:
            return JsonResponse({"Status": "File", "Message": "File is Processing!!!"})
    except Exception:
        logger.error("Error in Updating to Matched!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_update_contra(request, *args, **kwargs):
    try:
        file_processing = ''
        file_uploads = ReconFileUploads.objects.filter(is_processing = 1)
        for file in file_uploads:
            file_processing = "FILE"

        if file_processing == "":

            body = request.body.decode('utf-8')
            data = json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            m_processing_layer_id = 0
            m_processing_sub_layer_id = 0
            processing_layer_id = 0
            user_id = 0
            external_contra_id_list = None
            internal_contra_id_list = None

            for k, v in data.items():
                if k == "tenantId":
                    tenant_id = v
                if k == "groupId":
                    group_id = v
                if k == "entityId":
                    entity_id = v
                if k == "mProcessingLayerId":
                    m_processing_layer_id = v
                if k == "mProcessingSubLayerId":
                    m_processing_sub_layer_id = v
                if k == "processingLayerId":
                    processing_layer_id = v
                if k == "userId":
                    user_id = v
                if k == "externalContraList":
                    external_contra_id_list = v
                if k == "internalContraList":
                    internal_contra_id_list = v
                if k == "matchingCommentId":
                    matching_comment_id = v
                if k == "matchingCommentDescription":
                    matching_comment_description = v

            if int(tenant_id) > 0:
                if int(group_id) > 0:
                    if int(entity_id) > 0:
                        if int(m_processing_layer_id) > 0:
                            if int(m_processing_sub_layer_id) > 0:
                                if int(processing_layer_id) > 0:
                                    if int(user_id) > 0:
                                        if len(external_contra_id_list) > 0:
                                            reco_settings = RecoSettings.objects.filter(tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, setting_key = 'ext_contra_id', is_active = 1)
                                            for setting in reco_settings:
                                                ext_contra_id = int(setting.setting_value)

                                            TransactionExternalRecords.objects.filter(external_records_id__in = external_contra_id_list).update(
                                                ext_match_type_1 = 'Contra',
                                                ext_match_type_2 = 'Contra',
                                                ext_processing_status_1 = 'Contra',
                                                ext_generated_number_3 = matching_comment_id,
                                                ext_transaction_type_1 = matching_comment_description,
                                                ext_contra_id = ext_contra_id,
                                                modified_by = user_id,
                                                modified_date = str(datetime.today())
                                            )

                                            for setting in reco_settings:
                                                setting.setting_value = str(ext_contra_id + 1)
                                                setting.save()

                                            return JsonResponse({"Status": "Success"})


                                        if len(internal_contra_id_list) > 0:
                                            reco_settings = RecoSettings.objects.filter(tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, setting_key = 'int_contra_id', is_active = 1)
                                            for setting in reco_settings:
                                                int_contra_id = int(setting.setting_value)

                                            TransactionInternalRecords.objects.filter(internal_records_id__in = internal_contra_id_list).update(
                                                int_match_type_1 = 'Contra',
                                                int_match_type_2 = 'Contra',
                                                int_processing_status_1 = 'Contra',
                                                int_generated_number_3 = matching_comment_id,
                                                int_transaction_type_1 = matching_comment_description,
                                                int_contra_id = int_contra_id,
                                                modified_by = user_id,
                                                modified_date = str(datetime.today())
                                            )

                                            for setting in reco_settings:
                                                setting.setting_value = str(int_contra_id + 1)
                                                setting.save()

                                            return JsonResponse({"Status": "Success"})
                                    else:
                                        return JsonResponse({"Status": "Error", "Message": "User Id Not Found!!!"})
                                else:
                                    return JsonResponse({"Status": "Error", "Message": "Processing Layer Id Not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "M Processing Sub Layer Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "M Processing Layer Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id Not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id Not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id Not Found!!!"})
        else:
            return JsonResponse({"Status": "File", "Message": "File is Processing!!!"})

    except Exception:
        logger.error("Error in Updating Contra Records!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_update_matched_unmatched(request, *args, **kwargs):
    try:

        file_processing = ''
        file_uploads = ReconFileUploads.objects.filter(is_processing = 1)
        for file in file_uploads:
            file_processing = "FILE"

        if file_processing == "":

            body = request.body.decode('utf-8')
            data = json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            m_processing_layer_id = 0
            m_processing_sub_layer_id = 0
            processing_layer_id = 0
            user_id = 0
            external_record_id = 0
            internal_record_id = 0

            for k, v in data.items():
                if k == "tenantId":
                    tenant_id = v
                if k == "groupId":
                    group_id = v
                if k == "entityId":
                    entity_id = v
                if k == "mProcessingLayerId":
                    m_processing_layer_id = v
                if k == "mProcessingSubLayerId":
                    m_processing_sub_layer_id = v
                if k == "processingLayerId":
                    processing_layer_id = v
                if k == "userId":
                    user_id = v
                if k == "externalRecordId":
                    external_record_id = v
                if  k == "internalRecordId":
                    internal_record_id = v

            if int(tenant_id) > 0:
                if int(group_id) > 0:
                    if int(entity_id) > 0:
                        if int(m_processing_layer_id) > 0:
                            if int(m_processing_sub_layer_id) > 0:
                                if int(processing_layer_id) > 0:
                                    if int(user_id) > 0:
                                        if int(internal_record_id) > 0:
                                            if int(external_record_id) > 0:
                                                reco_results = RecoResults.objects.filter(m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, t_external_records_id = external_record_id, t_internal_records_id = internal_record_id)

                                                if reco_results is not None:
                                                    TransactionExternalRecords.objects.filter(external_records_id = external_record_id).update(ext_processing_status_1 = 'UnMatched', ext_match_type_1 = 'USER-UNMATCHED', ext_match_type_2 = None, ext_record_status_1 = 0, ext_generated_number_1 = None, ext_generated_number_2 = None, ext_generated_number_3 = None, ext_transaction_type_1 = None, modified_by = user_id, modified_date = str(datetime.today()))
                                                    TransactionInternalRecords.objects.filter(internal_records_id = internal_record_id).update(int_processing_status_1 = 'UnMatched', int_match_type_1 = 'USER-UNMATCHED', int_match_type_2 = None, int_record_status_1 = 0, int_generated_number_1 = None, int_generated_number_2 = None, int_generated_number_3 = None, int_transaction_type_1 = None, modified_by = user_id, modified_date = str(datetime.today()))

                                                    for result in reco_results:
                                                        result.reco_status = 'USER-UNMATCHED'
                                                        result.is_active = 0
                                                        result.modified_by = user_id
                                                        result.modified_date = str(datetime.today())
                                                        result.save()

                                                    return JsonResponse({"Status": "Success"})
                                                else:
                                                    return JsonResponse({"Status" : "Error", "Message": "Response Content Not Found!!!"})
                                            else:
                                                return JsonResponse({"Status": "Error", "Message": "External Record Id Not Found!!!"})
                                        else:
                                            return JsonResponse({"Status": "Error", "Message": "Internal Record Id Not Found!!!"})
                                    else:
                                        return JsonResponse({"Status": "Error", "Message": "User Id Not Found!!!"})
                                else:
                                    return JsonResponse({"Status": "Error", "Message": "Processing Layer Id Not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "M Processing Sub Layer Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "M Processing Layer Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id Not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id Not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id Not Found!!!"})
        else:
            return JsonResponse({"Status": "File", "Message": "File is Processing!!!"})
    except Exception:
        logger.error("Error in Updating to UnMatched!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_group_id_transactions(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            m_processing_layer_id = 0
            m_processing_sub_layer_id = 0
            processing_layer_id = 0
            selected_group_id = 0

            for k, v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "m_processing_layer_id":
                    m_processing_layer_id = v
                if k == "m_processing_sub_layer_id":
                    m_processing_sub_layer_id = v
                if k == "processing_layer_id":
                    processing_layer_id = v
                if k == "selected_group_id":
                    selected_group_id = v

            if int(tenant_id) > 0:
                if int(group_id) > 0:
                    if int(entity_id) > 0:
                        if int(m_processing_layer_id) > 0:
                            if int(m_processing_sub_layer_id) > 0:
                                if int(processing_layer_id) > 0:
                                    if int(selected_group_id) > 0:

                                        reco_results = RecoResults.objects.filter(generated_number_1 = selected_group_id, is_active = 1)

                                        for result in reco_results:
                                            reco_result_id = result.t_reco_result_id

                                        # print(reco_result_id)
                                        if len(str(reco_result_id)) > 0:
                                            reco_settings_external = RecoSettings.objects.filter(setting_key='ext_select_query_all', tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id , processing_layer_id = processing_layer_id, is_active=1)
                                            reco_settings_internal = RecoSettings.objects.filter(setting_key='int_select_query_all', tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id , processing_layer_id = processing_layer_id, is_active=1)

                                            setting_header_external = RecoSettings.objects.filter(setting_key='ext_header_all', tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id , processing_layer_id = processing_layer_id, is_active=1)
                                            setting_header_internal = RecoSettings.objects.filter(setting_key='int_header_all', tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id , processing_layer_id = processing_layer_id, is_active=1)

                                            for setting in reco_settings_external:
                                                external_select_query = setting.setting_value

                                            for setting in reco_settings_internal:
                                                internal_select_query = setting.setting_value

                                            for setting in setting_header_external:
                                                header_external = json.loads(setting.setting_value)

                                            for setting in setting_header_internal:
                                                header_internal = json.loads(setting.setting_value)

                                            record_status = "GroupMatched"

                                            external_select_query_proper = external_select_query.replace(
                                                "{tenants_id}", str(tenant_id)).replace("{groups_id}",
                                                                                        str(group_id)).replace(
                                                "{entities_id}", str(entity_id)).replace(
                                                "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                                "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                                "{processing_layer_id}", str(processing_layer_id)).replace(
                                                "{processing_status_1}", record_status).replace("{conditions}", "AND ext_generated_number_1 = " + str(selected_group_id))
                                            # print(external_select_query_proper)
                                            internal_select_query_proper = internal_select_query.replace(
                                                "{tenants_id}", str(tenant_id)).replace("{groups_id}",
                                                                                        str(group_id)).replace(
                                                "{entities_id}", str(entity_id)).replace(
                                                "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                                "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                                "{processing_layer_id}", str(processing_layer_id)).replace(
                                                "{processing_status_1}", record_status).replace("{conditions}", "AND int_generated_number_1 = " + str(selected_group_id))
                                            # print(internal_select_query_proper)
                                            external_query_out = json.loads(execute_sql_query(external_select_query_proper, object_type="table"))
                                            internal_query_out = json.loads(execute_sql_query(internal_select_query_proper, object_type="table"))

                                            external_query_out["headers"] = get_grid_transform(external_query_out, header_external)
                                            internal_query_out["headers"] = get_grid_transform(internal_query_out, header_internal)

                                            return JsonResponse({
                                                "Status": "Success",
                                                "external_records": external_query_out,
                                                "internal_records": internal_query_out
                                            })

                                        return JsonResponse({"Status": "Success"})
                                    else:
                                        return JsonResponse({"Status": "Error", "Message": "Group Id Not Found!!!"})
                                else:
                                    return JsonResponse({"Status": "Error", "Message": "Processing Layer Id Not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "M Processing Sub layer Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "M Processing Layer Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id Not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id Not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id Not Found!!!"})
    except Exception:
        logger.error("Error in Getting Group Id Transactions!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_update_group_records_unmatched(request, *args, **kwargs):
    try:

        file_processing = ''
        file_uploads = ReconFileUploads.objects.filter(is_processing = 1)
        for file in file_uploads:
            file_processing = "FILE"

        if file_processing == "":

            body = request.body.decode('utf-8')
            data = json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            m_processing_layer_id = 0
            m_processing_sub_layer_id = 0
            processing_layer_id = 0
            user_id = 0

            for k, v in data.items():
                if k == "tenantId":
                    tenant_id = v
                if k == "groupId":
                    group_id = v
                if k == "entityId":
                    entity_id = v
                if k == "mProcessingLayerId":
                    m_processing_layer_id = v
                if k == "mProcessingSubLayerId":
                    m_processing_sub_layer_id = v
                if k == "processingLayerId":
                    processing_layer_id = v
                if k == "userId":
                    user_id = v
                if k == "selectedGroupId":
                    selected_group_id = v

            if int(tenant_id) > 0:
                if int(group_id) > 0:
                    if int(entity_id) > 0:
                        if int(m_processing_layer_id) > 0:
                            if int(m_processing_sub_layer_id) > 0:
                                if int(processing_layer_id) > 0:
                                    if int(user_id) > 0:
                                        if int(selected_group_id) > 0:
                                            reco_results = RecoResults.objects.filter(generated_number_1 = selected_group_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id, is_active = 1)

                                            if reco_results is not None:
                                                TransactionExternalRecords.objects.filter(
                                                    ext_generated_number_1 = selected_group_id,
                                                    tenants_id = tenant_id,
                                                    groups_id = group_id,
                                                    entities_id = entity_id,
                                                    m_processing_layer_id = m_processing_layer_id,
                                                    m_processing_sub_layer_id = m_processing_sub_layer_id,
                                                    processing_layer_id = processing_layer_id,
                                                    is_active = 1
                                                ).update(
                                                    ext_processing_status_1='UnMatched',
                                                    ext_match_type_1='USER-UNMATCHED',
                                                    ext_match_type_2=None,
                                                    ext_record_status_1=0,
                                                    ext_generated_number_1=None,
                                                    ext_generated_number_2=None,
                                                    ext_generated_number_3=None,
                                                    ext_transaction_type_1=None,
                                                    modified_by=user_id,
                                                    modified_date=str(datetime.today())
                                                )

                                                TransactionInternalRecords.objects.filter(
                                                    int_generated_number_1 = selected_group_id,
                                                    tenants_id=tenant_id,
                                                    groups_id=group_id,
                                                    entities_id=entity_id,
                                                    m_processing_layer_id=m_processing_layer_id,
                                                    m_processing_sub_layer_id=m_processing_sub_layer_id,
                                                    processing_layer_id=processing_layer_id,
                                                    is_active=1
                                                ).update(
                                                    int_processing_status_1='UnMatched',
                                                    int_match_type_1='USER-UNMATCHED',
                                                    int_match_type_2=None,
                                                    int_record_status_1=0,
                                                    int_generated_number_1=None,
                                                    int_generated_number_2=None,
                                                    int_generated_number_3=None,
                                                    int_transaction_type_1=None,
                                                    modified_by=user_id,
                                                    modified_date=str(datetime.today())
                                                )

                                                for result in reco_results:
                                                    result.reco_status = 'USER-UNMATCHED'
                                                    result.is_active = 0
                                                    result.modified_by = user_id
                                                    result.modified_date = str(datetime.today())
                                                    result.save()

                                                return JsonResponse({"Status": "Success"})

                                            else:
                                                return JsonResponse({"Status": "Error", "Message": "Reco Result is None!!!"})
                                        else:
                                            return JsonResponse({"Status": "Error", "Message": "Selected Group Id Not Found!!!"})
                                    else:
                                        return JsonResponse({"Status": "Error", "Message": "User Id Not Found!!!"})
                                else:
                                    return JsonResponse({"Status": "Error", "Message": "Processing Id Not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "M Processing Sub Layer Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "M Processing Layer Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id Not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id Not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id Not Found!!!"})
        else:
            return JsonResponse({"Status": "File", "Message": "File is Processing!!!"})
    except Exception:
        logger.error("Error in Updating Group Id Unmatched to Matched !!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_selected_contra_records(request, *args, **kwargs):
    try:
        body = request.body.decode('utf-8')
        data = json.loads(body)

        tenant_id = 0
        group_id = 0
        entity_id = 0
        m_processing_layer_id = 0
        m_processing_sub_layer_id = 0
        processing_layer_id = 0
        user_id = 0
        external_contra_id = 0
        internal_contra_id = 0

        for k, v in data.items():
            if k == "tenantId":
                tenant_id = v
            if k == "groupId":
                group_id = v
            if k == "entityId":
                entity_id = v
            if k == "mProcessingLayerId":
                m_processing_layer_id = v
            if k == "mProcessingSubLayerId":
                m_processing_sub_layer_id = v
            if k == "processingLayerId":
                processing_layer_id = v
            if k == "userId":
                user_id = v
            if k == "externalContraId":
                external_contra_id = v
            if k == "internalContraId":
                internal_contra_id = v

        if int(tenant_id) > 0:
            if int(group_id) > 0:
                if int(entity_id) > 0:
                    if int(m_processing_layer_id) > 0:
                        if int(m_processing_sub_layer_id) > 0:
                            if int(processing_layer_id) > 0:
                                if int(user_id) > 0:
                                    if int(external_contra_id) > 0:
                                        reco_settings_external = RecoSettings.objects.filter(setting_key = 'ext_select_query_all', is_active = 1, tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id)
                                        setting_header_external = RecoSettings.objects.filter(setting_key = 'ext_header_all', is_active = 1, tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id)

                                        for setting in reco_settings_external:
                                            external_select_query = setting.setting_value

                                        for setting in setting_header_external:
                                            header_external = json.loads(setting.setting_value)

                                        ext_condition = "AND ext_contra_id = " + str(external_contra_id)

                                        external_select_query_proper = external_select_query.replace(
                                            "{tenants_id}", str(tenant_id)).replace("{groups_id}",
                                                                                    str(group_id)).replace(
                                            "{entities_id}", str(entity_id)).replace(
                                            "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                            "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                            "{processing_layer_id}", str(processing_layer_id)).replace(
                                            "{processing_status_1}", "Contra").replace(
                                            "{conditions}", ext_condition)

                                        external_query_out = json.loads(execute_sql_query(external_select_query_proper, object_type="table"))
                                        external_query_out["headers"] = get_grid_transform(external_query_out, header_external)

                                        return JsonResponse({"Status": "Success", "external_records": external_query_out})

                                    elif int(internal_contra_id) > 0:
                                        reco_settings_internal = RecoSettings.objects.filter(setting_key = 'int_select_query_all', is_active = 1, tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id)
                                        setting_header_internal = RecoSettings.objects.filter(setting_key = 'int_header_all', is_active = 1, tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id, processing_layer_id = processing_layer_id)

                                        for setting in reco_settings_internal:
                                            internal_select_query = setting.setting_value

                                        for setting in setting_header_internal:
                                            header_internal = json.loads(setting.setting_value)

                                        int_condition = "AND int_contra_id = " + str(internal_contra_id)

                                        internal_select_query_proper = internal_select_query.replace(
                                            "{tenants_id}", str(tenant_id)).replace("{groups_id}",
                                                                                    str(group_id)).replace(
                                            "{entities_id}", str(entity_id)).replace(
                                            "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                            "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                            "{processing_layer_id}", str(processing_layer_id)).replace(
                                            "{processing_status_1}", "Contra").replace(
                                            "{conditions}", int_condition)

                                        internal_query_out = json.loads(execute_sql_query(internal_select_query_proper, object_type="table"))
                                        internal_query_out["headers"] = get_grid_transform(internal_query_out, header_internal)

                                        return JsonResponse({"Status": "Success", "internal_records": internal_query_out})
                                else:
                                    return JsonResponse({"Status": "Error", "Message": "User Id Not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "Processing Layer Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "M Processing SUb Layer Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "M Processing Layer Id Not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Entity Id Not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Group Id Not Found!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "Tenant Id Not Found!!!"})

    except Exception:
        logger.error("Error in Getting Selected Contra Records!!!", exc_info = True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_unmatch_matched_contra(request, *args, **kwargs):
    try:
        file_processing = ''
        file_uploads = ReconFileUploads.objects.filter(is_processing = 1)
        for file in file_uploads:
            file_processing = "FILE"

        if file_processing == "":

            body = request.body.decode('utf-8')
            data = json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            m_processing_layer_id = 0
            m_processing_sub_layer_id = 0
            processing_layer_id = 0
            user_id = 0
            contra_side = ''
            contra_id = 0

            for k, v in data.items():
                if k == "tenantId":
                    tenant_id = v
                if k == "groupId":
                    group_id = v
                if k == "entityId":
                    entity_id = v
                if k == "mProcessingLayerId":
                    m_processing_layer_id = v
                if k == "mProcessingSubLayerId":
                    m_processing_sub_layer_id = v
                if k == "processingLayerId":
                    processing_layer_id = v
                if k == "userId":
                    user_id = v
                if k == "contraSide":
                    contra_side = v
                if k == "contraId":
                    contra_id = v

            if int(tenant_id) > 0:
                if int(group_id) > 0:
                    if int(entity_id) > 0:
                        if int(m_processing_layer_id) > 0:
                            if int(m_processing_sub_layer_id) > 0:
                                if int(processing_layer_id) > 0:
                                    if int(user_id) > 0:
                                        if len(contra_side) > 0:
                                            if int(contra_id) > 0:
                                                if contra_side == "External":
                                                    TransactionExternalRecords.objects.filter(
                                                        ext_contra_id = contra_id,
                                                        tenants_id=tenant_id,
                                                        groups_id=group_id,
                                                        entities_id=entity_id,
                                                        m_processing_layer_id=m_processing_layer_id,
                                                        m_processing_sub_layer_id=m_processing_sub_layer_id,
                                                        processing_layer_id=processing_layer_id,
                                                        is_active=1
                                                    ).update(
                                                        ext_processing_status_1='UnMatched',
                                                        ext_match_type_1='USER-UNMATCHED',
                                                        ext_match_type_2=None,
                                                        ext_record_status_1=0,
                                                        ext_contra_id=None,
                                                        ext_generated_number_1=None,
                                                        ext_generated_number_2=None,
                                                        ext_generated_number_3=None,
                                                        ext_transaction_type_1=None,
                                                        modified_by=user_id,
                                                        modified_date=str(datetime.today())
                                                    )
                                                    return JsonResponse({"Status": "Success"})

                                                if contra_side == "Internal":
                                                    TransactionInternalRecords.objects.filter(
                                                        int_contra_id=contra_id,
                                                        tenants_id=tenant_id,
                                                        groups_id=group_id,
                                                        entities_id=entity_id,
                                                        m_processing_layer_id=m_processing_layer_id,
                                                        m_processing_sub_layer_id=m_processing_sub_layer_id,
                                                        processing_layer_id=processing_layer_id,
                                                        is_active=1
                                                    ).update(
                                                        int_processing_status_1='UnMatched',
                                                        int_match_type_1='USER-UNMATCHED',
                                                        int_match_type_2=None,
                                                        int_record_status_1=0,
                                                        int_contra_id=None,
                                                        int_generated_number_1=None,
                                                        int_generated_number_2=None,
                                                        int_generated_number_3=None,
                                                        int_transaction_type_1=None,
                                                        modified_by=user_id,
                                                        modified_date=str(datetime.today())
                                                    )
                                                    return JsonResponse({"Status": "Success"})
                                            else:
                                                return JsonResponse({"Status": "Error", "Message": "Contra Id List Not FOund!!!"})
                                        else:
                                            return JsonResponse({"Status": "Error", "Message": "Contra Side Not Found!!!"})
                                    else:
                                        return JsonResponse({"Status": "Error", "Message": "User Id Not Found!!!"})
                                else:
                                    return JsonResponse({"Status": "Error", "Message": "Processing Id Not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "M Processing Sub Layer Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "M Processing Layer Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id Not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id Not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id Not Found!!!"})
        else:
            return JsonResponse({"Status": "File", "Message": "File is Processing!!!"})
    except Exception:
        logger.error("Error in Unmatching matched Contra!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_grouped_unmatch_transactions(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            m_processing_layer_id = 0
            m_processing_sub_layer_id = 0
            processing_layer_id = 0
            selected_group_id = 0

            for k, v in data.items():
                if k == "tenant_id":
                    tenant_id = v
                if k == "group_id":
                    group_id = v
                if k == "entity_id":
                    entity_id = v
                if k == "m_processing_layer_id":
                    m_processing_layer_id = v
                if k == "m_processing_sub_layer_id":
                    m_processing_sub_layer_id = v
                if k == "processing_layer_id":
                    processing_layer_id = v
                if k == "selected_group_id":
                    selected_group_id = v

            if int(tenant_id) > 0:
                if int(group_id) > 0:
                    if int(entity_id) > 0:
                        if int(m_processing_layer_id) > 0:
                            if int(m_processing_sub_layer_id) > 0:
                                if int(processing_layer_id) > 0:
                                    if int(selected_group_id) > 0:

                                        reco_results = RecoResults.objects.filter(generated_number_2 = selected_group_id, is_active = 1)

                                        for result in reco_results:
                                            reco_result_id = result.t_reco_result_id

                                        # print(reco_result_id)
                                        if len(str(reco_result_id)) > 0:
                                            reco_settings_external = RecoSettings.objects.filter(setting_key='ext_select_query_all', tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id , processing_layer_id = processing_layer_id, is_active=1)
                                            reco_settings_internal = RecoSettings.objects.filter(setting_key='int_select_query_all', tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id , processing_layer_id = processing_layer_id, is_active=1)

                                            setting_header_external = RecoSettings.objects.filter(setting_key='ext_header_all', tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id , processing_layer_id = processing_layer_id, is_active=1)
                                            setting_header_internal = RecoSettings.objects.filter(setting_key='int_header_all', tenants_id = tenant_id, groups_id = group_id, entities_id = entity_id, m_processing_layer_id = m_processing_layer_id, m_processing_sub_layer_id = m_processing_sub_layer_id , processing_layer_id = processing_layer_id, is_active=1)

                                            for setting in reco_settings_external:
                                                external_select_query = setting.setting_value

                                            for setting in reco_settings_internal:
                                                internal_select_query = setting.setting_value

                                            for setting in setting_header_external:
                                                header_external = json.loads(setting.setting_value)

                                            for setting in setting_header_internal:
                                                header_internal = json.loads(setting.setting_value)

                                            record_status = "GroupUnMatched"

                                            external_select_query_proper = external_select_query.replace(
                                                "{tenants_id}", str(tenant_id)).replace("{groups_id}",
                                                                                        str(group_id)).replace(
                                                "{entities_id}", str(entity_id)).replace(
                                                "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                                "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                                "{processing_layer_id}", str(processing_layer_id)).replace(
                                                "{processing_status_1}", record_status).replace("{conditions}", "AND ext_generated_number_2 = " + str(selected_group_id))
                                            # print("external_select_query_proper", external_select_query_proper)
                                            internal_select_query_proper = internal_select_query.replace(
                                                "{tenants_id}", str(tenant_id)).replace("{groups_id}",
                                                                                        str(group_id)).replace(
                                                "{entities_id}", str(entity_id)).replace(
                                                "{m_processing_layer_id}", str(m_processing_layer_id)).replace(
                                                "{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace(
                                                "{processing_layer_id}", str(processing_layer_id)).replace(
                                                "{processing_status_1}", record_status).replace("{conditions}", "AND int_generated_number_2 = " + str(selected_group_id))
                                            # print("internal_select_query_proper", internal_select_query_proper)
                                            external_query_out = json.loads(execute_sql_query(external_select_query_proper, object_type="table"))
                                            internal_query_out = json.loads(execute_sql_query(internal_select_query_proper, object_type="table"))

                                            external_query_out["headers"] = get_grid_transform(external_query_out, header_external)
                                            internal_query_out["headers"] = get_grid_transform(internal_query_out, header_internal)

                                            return JsonResponse({
                                                "Status": "Success",
                                                "external_records": external_query_out,
                                                "internal_records": internal_query_out
                                            })

                                        return JsonResponse({"Status": "Success"})
                                    else:
                                        return JsonResponse({"Status": "Error", "Message": "Group Id Not Found!!!"})
                                else:
                                    return JsonResponse({"Status": "Error", "Message": "Processing Layer Id Not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "M Processing Sub layer Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "M Processing Layer Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id Not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id Not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id Not Found!!!"})
    except Exception:
        logger.error("Error in Getting Grouped UnMatch Transactions!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_unmatch_grouped_unmatched_transactions(request, *args, **kwargs):
    try:
        file_processing = ''
        file_uploads = ReconFileUploads.objects.filter(is_processing = 1)
        for file in file_uploads:
            file_processing = "FILE"

        if file_processing == "":

            body = request.body.decode('utf-8')
            data = json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            m_processing_layer_id = 0
            m_processing_sub_layer_id = 0
            processing_layer_id = 0
            user_id = 0
            external_records_list = list()
            internal_records_list = list()

            for k, v in data.items():
                if k == "tenantId":
                    tenant_id = v
                if k == "groupId":
                    group_id = v
                if k == "entityId":
                    entity_id = v
                if k == "mProcessingLayerId":
                    m_processing_layer_id = v
                if k == "mProcessingSubLayerId":
                    m_processing_sub_layer_id = v
                if k == "processingLayerId":
                    processing_layer_id = v
                if k == "userId":
                    user_id = v
                if k == "externalRecordsList":
                    external_records_list = v
                if  k == "internalRecordsList":
                    internal_records_list = v

            if int(tenant_id) > 0:
                if int(group_id) > 0:
                    if int(entity_id) > 0:
                        if int(m_processing_layer_id) > 0:
                            if int(m_processing_sub_layer_id) > 0:
                                if int(processing_layer_id) > 0:
                                    if int(user_id) > 0:
                                        if len(external_records_list) > 0:
                                            if len(internal_records_list) > 0:

                                                external_records_id = external_records_list[0]["external_records_id"]
                                                t_external_records = TransactionExternalRecords.objects.filter(external_records_id = external_records_id)

                                                generated_unmatched_sequence = 0

                                                for record in t_external_records:
                                                    generated_unmatched_sequence = record.ext_generated_number_2

                                                if generated_unmatched_sequence != 0:

                                                    TransactionExternalRecords.objects.filter(
                                                        ext_generated_number_2=generated_unmatched_sequence,
                                                        tenants_id=tenant_id,
                                                        groups_id=group_id,
                                                        entities_id=entity_id,
                                                        m_processing_layer_id=m_processing_layer_id,
                                                        m_processing_sub_layer_id=m_processing_sub_layer_id,
                                                        processing_layer_id=processing_layer_id
                                                    ).update(
                                                        ext_processing_status_1='UnMatched',
                                                        ext_match_type_1='USER-UNMATCHED',
                                                        ext_match_type_2=None,
                                                        ext_record_status_1=0,
                                                        ext_generated_number_1=None,
                                                        ext_generated_number_2=None,
                                                        ext_generated_number_3=None,
                                                        ext_transaction_type_1=None,
                                                        modified_by=user_id,
                                                        modified_date=str(datetime.today())
                                                    )

                                                    TransactionInternalRecords.objects.filter(
                                                        int_generated_number_2=generated_unmatched_sequence,
                                                        tenants_id=tenant_id,
                                                        groups_id=group_id,
                                                        entities_id=entity_id,
                                                        m_processing_layer_id=m_processing_layer_id,
                                                        m_processing_sub_layer_id=m_processing_sub_layer_id,
                                                        processing_layer_id=processing_layer_id
                                                    ).update(
                                                        int_processing_status_1='UnMatched',
                                                        int_match_type_1='USER-UNMATCHED',
                                                        int_match_type_2=None,
                                                        int_record_status_1=0,
                                                        int_generated_number_1=None,
                                                        int_generated_number_2=None,
                                                        int_generated_number_3=None,
                                                        int_transaction_type_1=None,
                                                        modified_by=user_id,
                                                        modified_date=str(datetime.today())
                                                    )

                                                    RecoResults.objects.filter(
                                                        generated_number_2=generated_unmatched_sequence,
                                                        is_active=1,
                                                        m_processing_layer_id=m_processing_layer_id,
                                                        m_processing_sub_layer_id=m_processing_sub_layer_id,
                                                        processing_layer_id=processing_layer_id
                                                    ).update(
                                                        reco_status='USER-UNMATCHED',
                                                        is_active=0,
                                                        modified_by=user_id,
                                                        modified_date=str(datetime.today())
                                                    )

                                                    return JsonResponse({"Status": "Success", "Message": "Records Updated Successfully!!!"})
                                            else:
                                                return JsonResponse({"Status": "Error", "Message": "Internal Records List Not Found!!!"})
                                        else:
                                            return JsonResponse({"Status": "Error", "Message": "External Records List Not Found!!!"})
                                    else:
                                        return JsonResponse({"Status": "Error", "Message": "User Id Not Found!!!"})
                                else:
                                    return JsonResponse({"Status": "Error", "Message": "Processing Layer Id Not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "M Processing Sub Layer Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "M Processing Layer Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id Not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id Not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id Not Found!!!"})
        else:
            return JsonResponse({"Status": "File", "Message": "File is Processing!!!"})
    except Exception:
        logger.error("Error in Updating UnMatched Group UnMatched Transactions!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_match_grouped_unmatched_transactions(request, *args, **kwargs):
    try:

        file_processing = ''
        file_uploads = ReconFileUploads.objects.filter(is_processing = 1)
        for file in file_uploads:
            file_processing = "FILE"

        if file_processing == "":

            body = request.body.decode('utf-8')
            data = json.loads(body)

            tenant_id = 0
            group_id = 0
            entity_id = 0
            m_processing_layer_id = 0
            m_processing_sub_layer_id = 0
            processing_layer_id = 0
            user_id = 0
            external_records_list = list()
            internal_records_list = list()

            for k, v in data.items():
                if k == "tenantId":
                    tenant_id = v
                if k == "groupId":
                    group_id = v
                if k == "entityId":
                    entity_id = v
                if k == "mProcessingLayerId":
                    m_processing_layer_id = v
                if k == "mProcessingSubLayerId":
                    m_processing_sub_layer_id = v
                if k == "processingLayerId":
                    processing_layer_id = v
                if k == "userId":
                    user_id = v
                if k == "externalRecordsList":
                    external_records_list = v
                if  k == "internalRecordsList":
                    internal_records_list = v
                if k == "matchingCommentId":
                    matching_comment_id = v
                if k == "matchingCommentDescription":
                    matching_comment_description = v

            if int(tenant_id) > 0:
                if int(group_id) > 0:
                    if int(entity_id) > 0:
                        if int(m_processing_layer_id) > 0:
                            if int(m_processing_sub_layer_id) > 0:
                                if int(processing_layer_id) > 0:
                                    if int(user_id) > 0:
                                        if len(external_records_list) > 0:
                                            if len(internal_records_list) > 0:

                                                external_records_id = external_records_list[0]["external_records_id"]
                                                t_external_records = TransactionExternalRecords.objects.filter(external_records_id = external_records_id)

                                                generated_unmatched_sequence = 0

                                                for record in t_external_records:
                                                    generated_unmatched_sequence = record.ext_generated_number_2

                                                external_records_ids_list = list()
                                                internal_records_ids_list = list()

                                                for external_record in external_records_list:
                                                    external_records_ids_list.append(external_record["external_records_id"])

                                                for internal_record in internal_records_list:
                                                    internal_records_ids_list.append(internal_record["internal_records_id"])

                                                reco_settings = RecoSettings.objects.filter(
                                                    tenants_id = tenant_id,
                                                    groups_id = group_id,
                                                    entities_id = entity_id,
                                                    m_processing_layer_id = m_processing_layer_id,
                                                    m_processing_sub_layer_id = m_processing_sub_layer_id,
                                                    processing_layer_id = processing_layer_id,
                                                    setting_key = 'group_id'
                                                )

                                                group_sequence = 0
                                                for setting in reco_settings:
                                                    group_sequence = setting.setting_value

                                                if group_sequence != 0:

                                                    TransactionExternalRecords.objects.filter(
                                                        external_records_id__in=external_records_ids_list
                                                    ).update(
                                                        ext_processing_status_1='GroupMatched',
                                                        ext_match_type_1='USER-MATCHED',
                                                        ext_record_status_1=1,
                                                        ext_generated_number_1=group_sequence,
                                                        ext_generated_number_2=None,
                                                        ext_generated_number_3=matching_comment_id,
                                                        ext_transaction_type_1=matching_comment_description,
                                                        modified_by=user_id,
                                                        modified_date=str(datetime.today())
                                                    )

                                                    TransactionInternalRecords.objects.filter(
                                                        internal_records_id__in=internal_records_ids_list
                                                    ).update(
                                                        int_processing_status_1='GroupMatched',
                                                        int_match_type_1='USER-MATCHED',
                                                        int_record_status_1=1,
                                                        int_generated_number_1=group_sequence,
                                                        int_generated_number_2=None,
                                                        int_generated_number_3=matching_comment_id,
                                                        int_transaction_type_1=matching_comment_description,
                                                        modified_by=user_id,
                                                        modified_date=str(datetime.today())
                                                    )

                                                    RecoResults.objects.create(
                                                        m_processing_layer_id=m_processing_layer_id,
                                                        m_processing_sub_layer_id=m_processing_sub_layer_id,
                                                        processing_layer_id=processing_layer_id,
                                                        generated_number_1=group_sequence,
                                                        reco_status="USER-MATCHED",
                                                        is_active=1,
                                                        created_by=user_id,
                                                        created_date=str(datetime.today()),
                                                        modified_by=user_id,
                                                        modified_date=str(datetime.today())
                                                    )

                                                    for setting in reco_settings:
                                                        setting.setting_value = str(int(group_sequence) + 1)
                                                        setting.save()

                                                    TransactionExternalRecords.objects.filter(
                                                        ext_generated_number_2=generated_unmatched_sequence,
                                                        tenants_id=tenant_id,
                                                        groups_id=group_id,
                                                        entities_id=entity_id,
                                                        m_processing_layer_id=m_processing_layer_id,
                                                        m_processing_sub_layer_id=m_processing_sub_layer_id,
                                                        processing_layer_id=processing_layer_id
                                                    ).update(
                                                        ext_processing_status_1='UnMatched',
                                                        ext_match_type_1='USER-UNMATCHED',
                                                        ext_match_type_2=None,
                                                        ext_record_status_1=0,
                                                        ext_generated_number_1=None,
                                                        ext_generated_number_2=None,
                                                        ext_generated_number_3=None,
                                                        ext_transaction_type_1=None,
                                                        modified_by=user_id,
                                                        modified_date=str(datetime.today())
                                                    )

                                                    TransactionInternalRecords.objects.filter(
                                                        int_generated_number_2=generated_unmatched_sequence,
                                                        tenants_id=tenant_id,
                                                        groups_id=group_id,
                                                        entities_id=entity_id,
                                                        m_processing_layer_id=m_processing_layer_id,
                                                        m_processing_sub_layer_id=m_processing_sub_layer_id,
                                                        processing_layer_id=processing_layer_id
                                                    ).update(
                                                        int_processing_status_1='UnMatched',
                                                        int_match_type_1='USER-UNMATCHED',
                                                        int_match_type_2=None,
                                                        int_record_status_1=0,
                                                        int_generated_number_1=None,
                                                        int_generated_number_2=None,
                                                        int_generated_number_3=None,
                                                        int_transaction_type_1=None,
                                                        modified_by=user_id,
                                                        modified_date=str(datetime.today())
                                                    )

                                                    RecoResults.objects.filter(
                                                        generated_number_2=generated_unmatched_sequence,
                                                        is_active=1,
                                                        m_processing_layer_id=m_processing_layer_id,
                                                        m_processing_sub_layer_id=m_processing_sub_layer_id,
                                                        processing_layer_id=processing_layer_id
                                                    ).update(
                                                        reco_status='USER-UNMATCHED',
                                                        is_active=0,
                                                        modified_by=user_id,
                                                        modified_date=str(datetime.today())
                                                    )

                                                    return JsonResponse({"Status": "Success"})
                                                else:
                                                    return JsonResponse({"Status": "Error"})
                                            else:
                                                return JsonResponse({"Status": "Error", "Message": "Internal Records List Not Found!!!"})
                                        else:
                                            return JsonResponse({"Status": "Error", "Message": "External Records List Not Found!!!"})
                                    else:
                                        return JsonResponse({"Status": "Error", "Message": "User Id Not Found!!!"})
                                else:
                                    return JsonResponse({"Status": "Error", "Message": "Processing Layer Id Not Found!!!"})
                            else:
                                return JsonResponse({"Status": "Error", "Message": "M Processing Sub Layer Id Not Found!!!"})
                        else:
                            return JsonResponse({"Status": "Error", "Message": "M Processing Layer Id Not Found!!!"})
                    else:
                        return JsonResponse({"Status": "Error", "Message": "Entity Id Not Found!!!"})
                else:
                    return JsonResponse({"Status": "Error", "Message": "Group Id Not Found!!!"})
            else:
                return JsonResponse({"Status": "Error", "Message": "Tenant Id Not Found!!!"})
        else:
            return JsonResponse({"Status": "File", "Message": "File is Processing!!!"})
    except Exception:
        logger.error("Error in Updating Matched Group UnMatched Transactions!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

@csrf_exempt
def get_update_duplicates(request, *args, **kwargs):
    try:
        if request.method == "POST":
            file_processing = ''
            file_uploads = ReconFileUploads.objects.filter(is_processing=1)
            for file in file_uploads:
                file_processing = "FILE"

            if file_processing == "":
                body = request.body.decode('utf-8')
                data = json.loads(body)

                for k,v in data.items():
                    if k == "tenantsId":
                        tenant_id = v
                    if k == "groupsId":
                        group_id = v
                    if k == "entityId":
                        entity_id = v
                    if k == "mProcessingLayerId":
                        m_processing_layer_id = v
                    if k == "mProcessingSubLayerId":
                        m_processing_sub_layer_id = v
                    if k == "processingLayerId":
                        processing_layer_id = v
                    if k == "userId":
                        user_id = v
                    if k == "externalRecords":
                        external_records_list = v
                    if k == "internalRecords":
                        internal_records_list = v

                external_records_ids_list = []
                for external_records in external_records_list:
                    external_records_ids_list.append(external_records["external_records_id"])

                internal_records_ids_list = []
                for internal_records in internal_records_list:
                    internal_records_ids_list.append(internal_records["internal_records_id"])

                if len(external_records_ids_list) > 0:
                    TransactionExternalRecords.objects.filter(
                        tenants_id = tenant_id,
                        groups_id = group_id,
                        entities_id = entity_id,
                        m_processing_layer_id = m_processing_layer_id,
                        m_processing_sub_layer_id = m_processing_sub_layer_id,
                        processing_layer_id = processing_layer_id,
                        external_records_id__in = external_records_ids_list
                    ).update(
                        ext_processing_status_1 = 'Duplicate',
                        modified_by = user_id,
                        is_active=False,
                        modified_date = str(datetime.today())
                    )

                if len(internal_records_ids_list) > 0:
                    TransactionInternalRecords.objects.filter(
                        tenants_id=tenant_id,
                        groups_id=group_id,
                        entities_id=entity_id,
                        m_processing_layer_id=m_processing_layer_id,
                        m_processing_sub_layer_id=m_processing_sub_layer_id,
                        processing_layer_id=processing_layer_id,
                        internal_records_id__in = internal_records_ids_list
                    ).update(
                        int_processing_status_1 = 'Duplicate',
                        modified_by = user_id,
                        is_active=False,
                        modified_date = str(datetime.today())
                    )
            else:
                return JsonResponse({"Status": "File", "Message": "File is Processing!!!"})
        else:
            return JsonResponse({"Status": "Error"})
    except Exception:
        logger.error("Error in Get Update Duplicates Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

def get_execute_batch_data(request, *args, **kwargs):
    try:
        if request.method == "GET":

            body = request.body.decode('utf-8')
            data = json.loads(body)

            for k,v in data.items():
                if k == "tenantsId":
                    tenants_id = v
                if k == "groupsId":
                    groups_id = v
                if k == "entityId":
                    entity_id = v
                if k == "mProcessingLayerId":
                    m_processing_layer_id = v
                if k == "mProcessingSubLayerId":
                    m_processing_sub_layer_id = v

            source_queries_url = "http://localhost:50003/source/get_source_insert_queries/"

            headers = {
                "Content-Type": "application/json"
            }

            file_uploads_batch_all = ReconFileUploads.objects.filter(
                tenants_id = tenants_id,
                groups_id = groups_id,
                entities_id = entity_id,
                m_processing_layer_id = m_processing_layer_id,
                m_processing_sub_layer_id = m_processing_sub_layer_id,
                status = 'BATCH',
                is_active = 1
            )

            if file_uploads_batch_all:

                processing_layer_ids_list = []

                for file in file_uploads_batch_all:
                    processing_layer_ids_list.append(file.processing_layer_id)

                processing_layer_ids_distinct_list = list(set(processing_layer_ids_list))

                for processing_layer_id in processing_layer_ids_distinct_list:
                    file_uploads_sources = ReconFileUploads.objects.filter(
                        tenants_id = tenants_id,
                        groups_id = groups_id,
                        entities_id = entity_id,
                        m_processing_layer_id = m_processing_layer_id,
                        m_processing_sub_layer_id = m_processing_sub_layer_id,
                        processing_layer_id = processing_layer_id,
                        status = 'BATCH',
                        is_active=1
                    )

                    source_ids_list = []
                    for source in file_uploads_sources:
                        # print(source.m_source_id)
                        source_ids_list.append({
                            "source_id": source.m_source_id,
                            "file_path": source.file_path,
                            "id": source.id,
                            "created_by": source.created_by,
                            "modified_by": source.modified_by
                        })

                    file_run = 0
                    for source_ids in source_ids_list:
                        file_run = file_run + 1
                        payload_insert = json.dumps(
                            {
                                "tenants_id": tenants_id,
                                "groups_id": groups_id,
                                "entities_id": entity_id,
                                "m_source_id": source_ids["source_id"]
                            }
                        )

                        response_insert = requests.get(source_queries_url, data=payload_insert, headers=headers)

                        if response_insert.content:
                            content_data_insert = json.loads(response_insert.content)

                            if content_data_insert["Status"] == "Success":

                                insert_query = content_data_insert["insert_query"]
                                attribute_name_list = content_data_insert["attribute_name_list"]
                                attribute_data_types_list = content_data_insert["attribute_data_types_list"]
                                unique_list = content_data_insert["unique_list"]
                                source_extension = content_data_insert["source_extension"]
                                column_start_row = content_data_insert["column_start_row"]
                                m_source_name = content_data_insert["m_source_name"]

                                read_file_output = read_file.get_data_from_file(
                                    file_path = source_ids["file_path"],
                                    sheet_name = "",
                                    source_extension = source_extension,
                                    attribute_list = attribute_name_list,
                                    column_start_row = column_start_row,
                                    password_protected = "",
                                    source_password = "",
                                    attribute_data_types_list = attribute_data_types_list,
                                    unique_list = unique_list,
                                    date_key_word = ''
                                )
                                if read_file_output["Status"] == "Success":
                                    data = read_file_output["data"]["data"]

                                    data_load_db = get_load_data_to_database(
                                        data_frame= data,
                                        insert_query= insert_query,
                                        tenants_id= tenants_id,
                                        groups_id= groups_id,
                                        entities_id= entity_id,
                                        file_uploads_id= source_ids["id"],
                                        m_source_id= source_ids["source_id"],
                                        m_source_name= m_source_name,
                                        m_processing_layer_id= m_processing_layer_id,
                                        m_processing_sub_layer_id= m_processing_sub_layer_id,
                                        processing_layer_id= processing_layer_id,
                                        created_by= source_ids["created_by"],
                                        modified_by= source_ids["modified_by"]
                                    )

                                    # print("data_load_db")
                                    # print(data_load_db)

                                    if data_load_db["Status"] == "Success":
                                        continue
                                    elif data_load_db["Status"] == "Error":
                                        if file_run != len(source_ids_list):
                                            update_file_status_data = {
                                                "request_type": "patch",
                                                "file_uploads_id": source_ids["id"],
                                                "message": "Error in Loading Data!!!",
                                                "file_status": "ERROR",
                                                "is_processed": 1,
                                                "is_processing": 0,
                                                "system_comments": "Error in Loading the data to DB Table!!!"
                                            }
                                            get_update_file_status(data=update_file_status_data)
                                            continue

                                        elif file_run == len(source_ids_list):
                                            update_file_status_data = {
                                                "request_type": "patch",
                                                "file_uploads_id": source_ids["id"],
                                                "message": "Error in Loading Data!!!",
                                                "file_status": "ERROR",
                                                "is_processed": 1,
                                                "is_processing": 0,
                                                "system_comments": "Error in Loading the data to DB Table!!!"
                                            }
                                            get_update_file_status(data=update_file_status_data)
                                            return JsonResponse({"Status": "Error", "Message": "Error in Loading the data to DB Table!!!"})

                                elif read_file_output["Status"] == "Error":
                                    update_file_status_data = {
                                        "request_type": "patch",
                                        "file_uploads_id": source_ids["id"],
                                        "message": "Error in Loading Data!!!",
                                        "file_status": "ERROR",
                                        "is_processed": 1,
                                        "is_processing": 0,
                                        "system_comments": "Content Status Error from sources Module!!!"
                                    }
                                    get_update_file_status(data=update_file_status_data)
                                    return JsonResponse({"Status": "Error", "Message": "Error in Getting Data From Reading File Package!!!"})
                                elif read_file_output["Status"] == "DataLength":
                                    update_file_status_data = {
                                        "request_type": "patch",
                                        "file_uploads_id": source_ids["id"],
                                        "message": "No Records Found!!!",
                                        "file_status": "SUCCESS",
                                        "is_processed": 1,
                                        "is_processing": 0,
                                        "system_comments": "Length of data is equals to Zero!!!"
                                    }
                                    get_update_file_status(data=update_file_status_data)
                                    return JsonResponse({"Status": "Success", "Message": "Length of data is equals to Zero!!!"})
                            else:
                                update_file_status_data = {
                                    "request_type": "patch",
                                    "file_uploads_id": source_ids["id"],
                                    "message": "Error in Loading Data!!!",
                                    "file_status": "ERROR",
                                    "is_processed": 1,
                                    "is_processing": 0,
                                    "system_comments": "Content Status Error from sources Module!!!"
                                }
                                get_update_file_status(data=update_file_status_data)
                                return JsonResponse({"Status": "Error", "Message": "Error in Source Queries Response !!!"})
                        else:
                            update_file_status_data = {
                                "request_type": "patch",
                                "file_uploads_id": source_ids["id"],
                                "message": "Error in Loading Data!!!",
                                "file_status": "ERROR",
                                "is_processed": 1,
                                "is_processing": 0,
                                "system_comments": "Content Not Received from Sources Module!!!"
                            }
                            get_update_file_status(data = update_file_status_data)
                            return JsonResponse({"Status": "Error", "Message": "Source Queries Response Content not Received!!!"})

                    for source in file_uploads_sources:
                        update_file_status_data = {
                            "request_type": "patch",
                            "file_uploads_id": source.id,
                            "message": "Processing the Data!!!",
                            "file_status": "PROCESSING",
                            "is_processed": 0,
                            "is_processing": 1,
                            "system_comments": None
                        }
                        get_update_file_status(data=update_file_status_data)

                    # Processing the Processing Layer
                    business_logic = get_execute_procedures(
                        tenants_id = tenants_id,
                        groups_id = groups_id,
                        entities_id = entity_id,
                        m_processing_layer_id = m_processing_layer_id,
                        m_processing_sub_layer_id = m_processing_sub_layer_id,
                        processing_layer_id = processing_layer_id,
                        user_id = 0
                    )

                    # print("business_logic")
                    # print(business_logic)

                    if business_logic["Status"] == "Success":
                        for source in file_uploads_sources:
                            update_file_status_data = {
                                "request_type": "patch",
                                "file_uploads_id": source.id,
                                "message": "Data Processing Completed Successfully!!!",
                                "file_status": "COMPLETED",
                                "is_processed": 1,
                                "is_processing": 0,
                                "system_comments": None
                            }
                            get_update_file_status(data=update_file_status_data)
                        return JsonResponse({"Status": "Success", "Message": "Data Executed Successfully!!!"})
                    elif business_logic["Status"] == "Error":
                        for source in file_uploads_sources:
                            update_file_status_data = {
                                "request_type": "patch",
                                "file_uploads_id": source.id,
                                "message": "ERROR in Processing Data!!!",
                                "file_status": "ERROR",
                                "is_processed": 1,
                                "is_processing": 0,
                                "system_comments": "Procedure Returned None!!!"
                            }
                            get_update_file_status(data=update_file_status_data)
                        return JsonResponse({"Status": "Error", "Message": "Error in Data Execution!!!"})

            else:
                return JsonResponse({"Status": "Success", "Message": "No records found in BATCH!!!"})
        else:
            return JsonResponse({"Status": "Error", "Message": "GET Method Not Received!!!"})
    except Exception:
        logger.error("Error in Get Execute Batch Data Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

def get_load_data_to_database(data_frame, insert_query, tenants_id, groups_id, entities_id, file_uploads_id, m_source_id, m_source_name, m_processing_layer_id,
                              m_processing_sub_layer_id, processing_layer_id, created_by, modified_by):
    try:
        if data_frame is not None:
            data_rows_list = []
            for index, rows in data_frame.iterrows():
                # create a list for the current row
                data_list = [rows[column] for column in data_frame.columns]
                data_rows_list.append(data_list)

            # Adding Common Necessary Fields to the rows
            for row in data_rows_list:
                row.append(tenants_id)  # Tenants Id
                row.append(groups_id)  # Groups Id
                row.append(entities_id)  # Entities Id
                row.append(file_uploads_id)  # File Uploads Id
                row.append(m_source_id)  # M Source Id
                row.append(m_source_name)  # M Source Name
                row.append("New")  # processing_status
                row.append(m_processing_layer_id)  # M Processing Layer Id
                row.append(m_processing_sub_layer_id)  # M Processing Sub Layer Id
                row.append(processing_layer_id)  # Processing Layer Id
                row.append(str(datetime.today()))  # Processing Date Time
                row.append(1)  # Is Active
                row.append(created_by)  # Created By
                row.append(str(datetime.today()))  # Created Date
                row.append(modified_by)  # Modified By
                row.append(str(datetime.today()))  # Modified Date
                row.append(0) # Record Status 1
                row.append(0) # Record Status 2
                row.append(0) # Record Status 3
                row.append(0) # Record Status 4
                row.append(0) # Record Status 5
                row.append(0) # Record Status 6
                row.append(0) # Record Status 7
                row.append(0) # Record Status 8
                row.append(0) # Record Status 9
                row.append(0) # Record Status 10

            # Create a insert string from the list
            records = []
            for record_lists in data_rows_list:
                record_string = ''
                for record_list in record_lists:
                    record_string = record_string + "'" + str(record_list) + "', "
                record_proper = "(" + record_string[:-2] + "),"
                records.append(record_proper)

            insert_value_string = ''
            for record in records:
                insert_value_string = insert_value_string + record

            final_query = insert_query.replace("{source_values}", insert_value_string[:-1])
            # Loading to the Proper Table
            load_output = execute_sql_query(final_query, object_type="Normal")

            # print("load_output")
            # print(load_output)

            # Updating the status of the file upload table
            if load_output == "Success":
                return {"Status": "Success"}
            else:
                return {"Status": "Error"}

    except Exception:
        logger.error("Error in Get Load Data to Database!!!", exc_info=True)
        return {"Status": "Error"}

def get_execute_procedures(tenants_id, groups_id, entities_id, m_processing_layer_id, m_processing_sub_layer_id, processing_layer_id, user_id):
    try:

        reco_execution_tasks = RecoExecutionTasks.objects.filter(
            tenants_id = tenants_id,
            groups_id = groups_id,
            entities_id = entities_id,
            m_processing_layer_id = m_processing_layer_id,
            m_processing_sub_layer_id = m_processing_sub_layer_id,
            processing_layer_id = processing_layer_id,
            is_active = 1
        ).order_by('execution_sequence')

        procedure_list = []
        for procedure in reco_execution_tasks:
            procedure_list.append(procedure.procedure_name)

        for procedure in procedure_list:
            final_procedure = procedure.replace("{params}", str(tenants_id) + ", " + str(groups_id) + ", " + str(entities_id) + ", " + str(m_processing_layer_id) + ", " + str(m_processing_sub_layer_id) + ", " + str(processing_layer_id) + ", " + str(user_id) + ", " + "@vReturn")
            # print(final_procedure)
            final_procedure_output = execute_sql_query(final_procedure, object_type="Normal")
            # print("final_procedure_output")
            # print(final_procedure_output)
            if final_procedure_output is not None:
                continue
            elif final_procedure_output is None:
                return {"Status": "Error"}

        return {"Status": "Success"}
    except Exception:
        logger.error("Error in Get Execute Procedures!!!", exc_info=True)
        return {"Status": "Error"}

def get_update_file_status(data):
    try:
        for k,v in data.items():
            if k == "request_type":
                request_type = v
            if k == "message":
                message = v
            if k == "file_uploads_id":
                file_uploads_id = v
            if k == "file_status":
                file_status = v
            if k == "is_processed":
                is_processed = v
            if k == "is_processing":
                is_processing = v
            if k == "system_comments":
                system_comments = v

        # print("file_uploads_id")
        # print(file_uploads_id)

        file_uploads_url = "http://localhost:50013/api/v1/vendor_recon/file_uploads/{file_uploads_id}/"
        file_uploads_url_proper = file_uploads_url.replace("{file_uploads_id}", str(file_uploads_id))

        headers = {
            "Content-Type": "application/json"
        }

        payload = json.dumps(
            {
                "status" : file_status,
                "comments": message,
                "is_processed": is_processed,
                "is_processing": is_processing,
                "modified_by": 0,
                "system_comments": system_comments
            }
        )

        if request_type == "patch":
            response_patch = requests.patch(file_uploads_url_proper, data=payload, headers=headers)
            if response_patch.content:
                content_data_patch = json.loads(response_patch.content)
                # print(content_data_patch)
                if content_data_patch["id"] == file_uploads_id:
                    return {"Status": "Success"}
                else:
                    return {"Status": "Error"}
            else:
                return {"Status": "Error", "Message": "Content Not Found from Patch Response!!!"}
        else:
            return {"Status": "Error", "Message": "Unknown Request Type!!!"}
    except Exception:
        logger.error("Error in Get Update FIle Status!!!", exc_info=True)
        return {"Status": "Error"}

@csrf_exempt
def get_send_mail(request, *args, **kwargs):
    try:
        if send_email.send_mail_to_vendor(receiver_email = "keerthana@adventbizsolutions.com"):
            return JsonResponse({"Status": "Success"})
        else:
            return JsonResponse({"Status": "Error"})
    except Exception:
        logger.error("Error in Sending Mail!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

def get_write_vrs_report(data):
    try:
        write_brs_output = write_vrs.write_vrs_file(data)
        if write_brs_output["Status"] == "Success":
            return {"Status" : "Success", "file_generated": write_brs_output["file_generated"]}
        elif write_brs_output["Status"] == "Error":
            logger.info("Error in Getting Write BRS Report!!!")
            logger.info(write_brs_output["Message"])
            return {"Status": "Error"}
    except Exception:
        logger.error("Error in Writing BRS Report!!!", exc_info=True)
        return {"Status": "Error"}

@csrf_exempt
def get_vrs_report(request, *args, **kwargs):
    try:
        if request.method == "POST":
            report_generation = ReportGeneration.objects.filter(is_report_generating = False)
            if report_generation:

                body = request.body.decode('utf-8')
                data = json.loads(body)

                for k,v in data.items():
                    if k == "tenantId":
                        tenant_id = v
                    if k == "groupId":
                        group_id = v
                    if k == "entityId":
                        entity_id = v
                    if k == "mProcessingLayerId":
                        m_processing_layer_id = v
                    if k == "mProcessingSubLayerId":
                        m_processing_sub_layer_id = v
                    if k == "processingLayerId":
                        processing_layer_id = v
                    if k == "reportFromDate":
                        report_from_date = v
                    if k == "reportToDate":
                        report_to_date = v

                vendor_matching_details = VendorMatchingDetails.objects.filter(
                    tenants_id = tenant_id,
                    groups_id = group_id,
                    entities_id = entity_id,
                    m_processing_layer_id = m_processing_layer_id,
                    m_processing_sub_layer_id = m_processing_sub_layer_id,
                    processing_layer_id = processing_layer_id
                )

                if vendor_matching_details:

                    report_generation_1 = ReportGeneration.objects.filter(id=1)

                    for report in report_generation_1:
                        report.is_report_generating = 1
                        report.save()


                    for vendor in vendor_matching_details:
                        vendor_code = vendor.vendor_code
                        vendor_name = vendor.vendor_name
                        vendor_site_code = vendor.vendor_site_code
                        vendor_category = vendor.vendor_category
                        liability_account = vendor.liability_account
                        division = vendor.division
                        pan_number = vendor.pan_number
                        gst_number = vendor.gst_number

                    reco_settings_rep_gen = RecoSettings.objects.filter(
                        tenants_id=tenant_id,
                        groups_id=group_id,
                        entities_id=entity_id,
                        m_processing_layer_id=m_processing_layer_id,
                        m_processing_sub_layer_id=m_processing_sub_layer_id,
                        processing_layer_id=processing_layer_id,
                        setting_key='report_generation'
                    )

                    for setting in reco_settings_rep_gen:
                        report_generation_count = setting.setting_value

                    reco_settings_tmx_dr_cr = RecoSettings.objects.filter(
                        tenants_id=tenant_id,
                        groups_id=group_id,
                        entities_id=entity_id,
                        m_processing_layer_id=m_processing_layer_id,
                        m_processing_sub_layer_id=m_processing_sub_layer_id,
                        processing_layer_id=processing_layer_id,
                        setting_key = 'vrs_rep_tmx_dr_cr'
                    )

                    for setting in reco_settings_tmx_dr_cr:
                        vrs_rep_tmx_dr_cr_query = setting.setting_value

                    reco_settings_vendor_dr_cr = RecoSettings.objects.filter(
                        tenants_id=tenant_id,
                        groups_id=group_id,
                        entities_id=entity_id,
                        m_processing_layer_id=m_processing_layer_id,
                        m_processing_sub_layer_id=m_processing_sub_layer_id,
                        processing_layer_id=processing_layer_id,
                        setting_key = 'vrs_rep_vendor_dr_cr'
                    )

                    for setting in reco_settings_vendor_dr_cr:
                        vrs_rep_vendor_dr_cr_query = setting.setting_value

                    reco_settings_vrs_rep_vendor_all = RecoSettings.objects.filter(
                        tenants_id=tenant_id,
                        groups_id=group_id,
                        entities_id=entity_id,
                        m_processing_layer_id=m_processing_layer_id,
                        m_processing_sub_layer_id=m_processing_sub_layer_id,
                        processing_layer_id=processing_layer_id,
                        setting_key = 'vrs_rep_vendor_all'
                    )

                    for setting in reco_settings_vrs_rep_vendor_all:
                        vrs_rep_vendor_all_query = setting.setting_value

                    reco_settings_vrs_rep_tmx_all = RecoSettings.objects.filter(
                        tenants_id=tenant_id,
                        groups_id=group_id,
                        entities_id=entity_id,
                        m_processing_layer_id=m_processing_layer_id,
                        m_processing_sub_layer_id=m_processing_sub_layer_id,
                        processing_layer_id=processing_layer_id,
                        setting_key = 'vrs_rep_tmx_all'
                    )

                    for setting in reco_settings_vrs_rep_tmx_all:
                        vrs_rep_tmx_all_query = setting.setting_value

                    vrs_rep_tmx_dr_cr_query_proper = vrs_rep_tmx_dr_cr_query.replace("{tenants_id}", str(tenant_id)).replace("{groups_id}", str(group_id)).\
                        replace("{entities_id}", str(entity_id)).replace("{m_processing_layer_id}", str(m_processing_layer_id)).\
                        replace("{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace("{processing_layer_id}", str(processing_layer_id)).\
                        replace("{from_date}",report_from_date).replace("{to_date}", report_to_date)

                    vrs_rep_vendor_dr_cr_query_proper = vrs_rep_vendor_dr_cr_query.replace("{tenants_id}", str(tenant_id)).replace("{groups_id}", str(group_id)).\
                        replace("{entities_id}", str(entity_id)).replace("{m_processing_layer_id}", str(m_processing_layer_id)).\
                        replace("{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace("{processing_layer_id}", str(processing_layer_id)).\
                        replace("{from_date}",report_from_date).replace("{to_date}", report_to_date)

                    vrs_rep_vendor_all_query_proper = vrs_rep_vendor_all_query.replace("{tenants_id}", str(tenant_id)).replace("{groups_id}", str(group_id)).\
                        replace("{entities_id}", str(entity_id)).replace("{m_processing_layer_id}", str(m_processing_layer_id)).\
                        replace("{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace("{processing_layer_id}", str(processing_layer_id)).\
                        replace("{from_date}",report_from_date).replace("{to_date}", report_to_date)

                    vrs_rep_tmx_all_query_proper = vrs_rep_tmx_all_query.replace("{tenants_id}", str(tenant_id)).replace("{groups_id}", str(group_id)).\
                        replace("{entities_id}", str(entity_id)).replace("{m_processing_layer_id}", str(m_processing_layer_id)).\
                        replace("{m_processing_sub_layer_id}", str(m_processing_sub_layer_id)).replace("{processing_layer_id}", str(processing_layer_id)).\
                        replace("{from_date}",report_from_date).replace("{to_date}", report_to_date)

                    vrs_rep_tmx_dr_cr_query_output = execute_sql_query(vrs_rep_tmx_dr_cr_query_proper, object_type="")[0]
                    vrs_rep_vendor_dr_cr_query_output = execute_sql_query(vrs_rep_vendor_dr_cr_query_proper, object_type="")[0]
                    vrs_rep_vendor_all_query_output = execute_sql_query(vrs_rep_vendor_all_query_proper, object_type="")[0]
                    vrs_rep_tmx_all_query_output = execute_sql_query(vrs_rep_tmx_all_query_proper, object_type="")[0]

                    data = {
                        "vendor_code": vendor_code,
                        "vendor_name": vendor_name,
                        "vendor_site_code": vendor_site_code,
                        "vendor_category": vendor_category,
                        "liability_account": liability_account,
                        "division": division,
                        "pan_number": pan_number,
                        "gst_number": gst_number,
                        "report_generation_date": str(datetime.today()),
                        "report_from_date": report_from_date,
                        "report_to_date": report_to_date,
                        "report_generation_count": report_generation_count,
                        "vrs_rep_tmx_dr_cr_query_output": vrs_rep_tmx_dr_cr_query_output,
                        "vrs_rep_vendor_dr_cr_query_output": vrs_rep_vendor_dr_cr_query_output,
                        "vrs_rep_vendor_all_query_output": vrs_rep_vendor_all_query_output,
                        "vrs_rep_tmx_all_query_output": vrs_rep_tmx_all_query_output
                    }

                    vrs_report_output = get_write_vrs_report(data)

                    if vrs_report_output["Status"] == "Success":
                        for setting in reco_settings_rep_gen:
                            setting.setting_value = str(int(report_generation_count) + 1)
                            setting.save()

                        report_generation_1 = ReportGeneration.objects.filter(id=1)

                        for report in report_generation_1:
                            report.is_report_generating = 0
                            report.save()

                        return JsonResponse({"Status": "Success", "file_generated": vrs_report_output["file_generated"]})
                    else:
                        report_generation_1 = ReportGeneration.objects.filter(id=1)

                        for report in report_generation_1:
                            report.is_report_generating = 0
                            report.save()

                        return JsonResponse({"Status": "Error"})
                else:

                    report_generation_1 = ReportGeneration.objects.filter(id=1)

                    for report in report_generation_1:
                        report.is_report_generating = 0
                        report.save()

                    return JsonResponse({"Status": "VNR", "Message": "Vendor Not Registered!!!"})
            else:
                return JsonResponse({"Status": "Report Generating", "Message": "User is Currently Generating Report!!!"})

        else:
            return JsonResponse({"Status": "Error", "Message": "POST Method Not Received!!!"})

    except Exception:
        report_generation_1 = ReportGeneration.objects.filter(id=1)

        for report in report_generation_1:
            report.is_report_generating = 0
            report.save()

        logger.error("Error in Get VRS Report Function!!!", exc_info=True)
        return JsonResponse({"Status": "Error"})

