from django.urls import path, include
from . import views
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register('vendor_master', views.VendorMasterViewSet, basename='vendor_master_view')
router.register('file_uploads', views.ReconFileUploadsViewSet, basename="file_uploads_view")
router.register('m_matching_comments', views.MasterMatchingCommentsViewSet, basename="m_matching_comments_view")

urlpatterns = [
    path('get_file_upload/', views.get_file_upload, name="get_file_upload"),
    path('get_transaction_count/', views.get_transaction_count, name="get_transaction_count"),
    path('get_transaction_records/', views.get_transaction_records, name="get_transaction_records"),
    path('get_int_ext_transactions/', views.get_int_ext_transactions, name="get_int_ext_transactions"),
    path('get_internal_external_headers/', views.get_internal_external_headers, name="get_internal_external_headers"),
    path('get_update_unmatched_matched/', views.get_update_unmatched_matched, name="get_update_unmatched_matched"),
    path('get_update_contra/', views.get_update_contra, name='get_update_contra'),
    path('get_update_matched_unmatched/', views.get_update_matched_unmatched, name="get_update_matched_unmatched"),
    path('get_group_id_transactions/', views.get_group_id_transactions, name="get_group_id_transactions"),
    path('get_update_group_records_unmatched/', views.get_update_group_records_unmatched, name="get_update_group_records_unmatched"),
    path('get_selected_contra_records/', views.get_selected_contra_records, name="get_selected_contra_records"),
    path('get_unmatch_matched_contra/', views.get_unmatch_matched_contra, name="get_unmatch_matched_contra"),
    path('get_grouped_unmatch_transactions/', views.get_grouped_unmatch_transactions, name="get_grouped_unmatch_transactions"),
    path('get_unmatch_grouped_unmatched_transactions/', views.get_unmatch_grouped_unmatched_transactions, name="get_unmatch_grouped_unmatched_transactions"),
    path('get_match_grouped_unmatched_transactions/', views.get_match_grouped_unmatched_transactions, name="get_match_grouped_unmatched_transactions"),
    path('get_update_duplicates/', views.get_update_duplicates, name="get_update_duplicates"),
    path('get_execute_batch_data/', views.get_execute_batch_data, name="get_execute_batch_data"),
    path('get_send_mail/', views.get_send_mail, name="get_send_mail"),
    path('get_vrs_report/', views.get_vrs_report, name="get_vrs_report")
]

# For View sets
urlpatterns += [
    path('', include(router.urls))
]