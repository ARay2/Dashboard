# -*- coding: utf-8 -*-
"""
Created on Mon Apr  8 14:52:47 2019

@author: VIPL140
"""

from __future__ import print_function
import os
import httplib2

from apiclient import discovery
from oauth2client import tools

# If PyCharm shows error saying reference not found, ignore
# PyCharm unable to detect
from apiclient.http import MediaFileUpload

try:
    import argparse

    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# importing classes
import auth
import download

# If modifying these scopes, delete previously saved credentials
# at ~/.credentials/drive-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'credentials.json'
APPLICATION_NAME = 'Drive API Python Quickstart'

auth_inst = auth.Auth(SCOPES, CLIENT_SECRET_FILE, APPLICATION_NAME)
credentials = auth_inst.get_credentials()

http = credentials.authorize(httplib2.Http())
drive_service = discovery.build('drive', 'v3', http=http)

# SQL queries
dau_sql_query = "with fd as (Select ga.userid, min(ga.timestampist) as fdate from clickstream_growthapp as ga group by ga.userid) Select  case when (g.timestampist::date - fd.fdate::date) <= 35 then (g.timestampist::date - fd.fdate::date)::text else '30+' end As D_Diff, substring(date_trunc('day',g.timestampist)::text,6,5) as Session_date, app.studentinfo_grade as user_grade, count(distinct(fd.userid)) as Users from clickstream_growthapp as g Left outer join fd ON fd.userid=g.userid left join ( select id, email,studentinfo_grade from public.user) app on app.id = g.userid where g.timestampist::date > current_date - 35 and (date_trunc('day',g.timestampist))-(date_trunc('day',fd.fdate)) is not null group by 1,2,3 order by 1 asc, 2 desc;"

content_pdf_sql_query = "select v1, ga.grade, count((case when (ga.eventlabel='app_study_chapter_click') then ga.userid end)) AS Opens, count(distinct(case when (ga.eventlabel='app_study_chapter_click') then ga.userid end)) AS Users from clickstream_growthApp as ga where ga.timestampist::date = current_date - 1 and ga.eventlabel='app_study_chapter_click' group by 1,2 order by 4 DESC;"

adoption_sectionwise_sql_query = """
select 
     case when (clickstream_growthApp.timestampist::date - clickstream_growthApp.firstopened_datetime::date) <= 30 then (clickstream_growthApp.timestampist::date - clickstream_growthApp.firstopened_datetime::date)::text
         else '30+' end As D_Diff,
    substring(date_trunc('day',clickstream_growthApp.timestampist)::text,6,5) As Usage_Date,
    /* Below two columns for the tabular structure to work on Excel */
    app.studentinfo_grade as user_grade,
    case   when clickstream_growthApp.eventlabel in ('app_study_chapter_click','app_study_pdf_item_clicked') then 'pdf'
           when clickstream_growthApp.eventlabel ilike 'app_cr_content_click' and v1 ilike 'ncert_pdf' then 'ncert_pdf'
           when clickstream_growthApp.eventlabel ilike 'app_cr_content_click' and v1 ilike 'imp_questions' then 'imp_questions'
           when clickstream_growthApp.eventlabel='app_doubt_submit' then 'doubt'
           when clickstream_growthApp.eventlabel in ('app_classroom_video_clicked','app_classroom_thumbnail_clicked','app_classroom_videodetail_video_clicked','app_classroom_livevideo_popup_clicked','app_classroom_video_click','app_classroom_button_click','app_classroom_chat_tab_click','app_classroom_chat_submit','app_classroom_player_click','app_classroom_live_overlay_click','app_classroom_video_click','app_classroom_button_click','app_classroom_chat_tab_click','app_classroom_chat_submit','app_classroom_player_click','app_classroom_live_overlay_click','app_classroom_video_loaded','app_classroom_video_seen')  then 'video'
           when clickstream_growthApp.eventlabel ilike 'app_cr_ncert_video_click' or (clickstream_growthApp.eventlabel ilike 'app_cr_content_click' and v1 ilike 'ncert_video') then 'ncert_video'
           when clickstream_growthApp.eventlabel in ('app_test_ongoing_click','app_test_normal_click', 'app_home_test_click') or (clickstream_growthApp.eventlabel ilike 'app_cr_content_click' and v1 ilike 'tests') or (clickstream_growthApp.eventlabel ilike 'app_hamburger_test_open' and v1 ilike 'ongoing') then 'test'
           when clickstream_growthApp.eventlabel in ('app_cr_class_click','app_hp_live_class_click') and v1 ilike 'live' then 'Live Classes'
           when clickstream_growthApp.eventlabel ilike 'app_home%' or clickstream_growthApp.eventlabel ilike 'app_hp%' then 'home'
        end as FA_Type,
    count(distinct(clickstream_growthApp.userid)) as users
         
from clickstream_growthApp
    left join ( select id, email,studentinfo_grade from public."user"
                ) app on app.id = clickstream_growthApp.userid

where (clickstream_growthApp.eventlabel in ('app_study_chapter_click','app_study_pdf_item_clicked','app_cr_content_click','app_doubt_submit','app_classroom_video_clicked','app_classroom_thumbnail_clicked','app_classroom_videodetail_video_clicked','app_classroom_livevideo_popup_clicked','app_classroom_video_click','app_classroom_button_click','app_classroom_chat_tab_click','app_classroom_chat_submit','app_classroom_player_click','app_classroom_live_overlay_click','app_classroom_video_click','app_classroom_button_click','app_classroom_chat_tab_click','app_classroom_chat_submit','app_classroom_player_click','app_classroom_live_overlay_click','app_classroom_video_loaded','app_classroom_video_seen','app_cr_ncert_video_click','app_test_ongoing_click','app_test_normal_click', 'app_home_test_click','app_hamburger_test_open','app_cr_class_click','app_hp_live_class_click')
        or clickstream_growthApp.eventlabel ilike '%app_home%'  or clickstream_growthApp.eventlabel ilike 'app_hp%')
    and clickstream_growthApp.timestampist::date > current_date - 30
    and (clickstream_growthApp.timestampist::date - clickstream_growthApp.firstopened_datetime::date) is not null

group by 1,
    date_trunc('day',clickstream_growthApp.timestampist),
    3,4

order by 1 asc,
    date_trunc('day',clickstream_growthApp.timestampist) desc;
"""

adoption_overall_sql_query = """
select 
    case when (clickstream_growthApp.timestampist::date - clickstream_growthApp.firstopened_datetime::date) <= 30 then (clickstream_growthApp.timestampist::date - clickstream_growthApp.firstopened_datetime::date)::text
         else '30+' end As D_Diff,

    substring(date_trunc('day',clickstream_growthApp.timestampist)::text,6,5) As Usage_Date,
    /* Below two columns for the tabular structure to work on Excel */
    app.studentinfo_grade as user_grade,
    'total' as FA_Type,
    count(distinct(clickstream_growthApp.userid)) as DataType

from clickstream_growthApp
    left join ( select id, email,studentinfo_grade from public."user"
                ) app on app.id = clickstream_growthApp.userid

where (clickstream_growthApp.eventlabel in ('app_study_chapter_click','app_study_pdf_item_clicked','app_cr_content_click','app_doubt_submit','app_classroom_video_clicked','app_classroom_thumbnail_clicked','app_classroom_videodetail_video_clicked','app_classroom_livevideo_popup_clicked','app_classroom_video_click','app_classroom_button_click','app_classroom_chat_tab_click','app_classroom_chat_submit','app_classroom_player_click','app_classroom_live_overlay_click','app_classroom_video_click','app_classroom_button_click','app_classroom_chat_tab_click','app_classroom_chat_submit','app_classroom_player_click','app_classroom_live_overlay_click','app_classroom_video_loaded','app_classroom_video_seen','app_cr_ncert_video_click','app_test_ongoing_click','app_test_normal_click', 'app_home_test_click','app_hamburger_test_open','app_cr_class_click','app_hp_live_class_click')
        or clickstream_growthApp.eventlabel ilike 'app_home%'   or clickstream_growthApp.eventlabel ilike 'app_hp%')
    and clickstream_growthApp.timestampist::date > current_date - 30
    and (clickstream_growthApp.timestampist::date - clickstream_growthApp.firstopened_datetime::date) is not null

group by 1,
    date_trunc('day',clickstream_growthApp.timestampist),
    3,4

order by 1 asc,
    date_trunc('day',clickstream_growthApp.timestampist) desc;
"""

content_video_sql_query = """with actc as 
    (select 
        count(case when clickstream_growthapp.eventlabel='app_classroom_videodetail_item_clicked' and v2='like' then v2 end) as "Like",
        count(case when clickstream_growthapp.eventlabel='app_classroom_videodetail_item_clicked' and v2='share' then v2 end) as "Share",
        count(case when clickstream_growthapp.eventlabel='app_classroom_videodetail_item_clicked' and v2='save' then v2 end) as "Saved",
        v1 as "Video_ID",
        grade as "Grade"

    from clickstream_growthApp
    where (trunc(timestamp 'epoch'+(timestamp+19800000)/1000*interval '1 second')) = current_date-1
        and clickstream_growthApp.eventlabel in ('app_classroom_thumbnail_clicked','app_classroom_video_clicked','app_classroom_videodetail_video_clicked','app_classroom_livevideo_popup_clicked','app_classroom_videodetail_item_clicked')
    group by 4,5)

select 'total' as "Date",
    cmds_video.t_subjects,
    cmds_video.title,
    cmds_video.input_topics,
    clickstream_growthApp.grade,
    cmds_video.duration/60 as mins,
    count(distinct(case when (clickstream_growthApp.eventlabel='app_classroom_video_clicked' or clickstream_growthApp.eventlabel='app_classroom_thumbnail_clicked' or clickstream_growthApp.eventlabel='app_classroom_videodetail_video_clicked' or clickstream_growthApp.eventlabel='app_classroom_livevideo_popup_clicked') then clickstream_growthApp.userid end)) AS "Users",
    count(distinct(case when (clickstream_growthApp.eventlabel='app_classroom_video_loaded' and v2 >= '25') then clickstream_growthApp.userid end)) AS ">25",
    count(distinct(case when (clickstream_growthApp.eventlabel='app_classroom_video_loaded' and v2 >= '50') then clickstream_growthApp.userid end)) AS ">50",
    count(distinct(case when (clickstream_growthApp.eventlabel='app_classroom_video_loaded' and v2 >= '75') then clickstream_growthApp.userid end)) AS ">75",
    count(distinct(case when (clickstream_growthApp.eventlabel='app_classroom_video_loaded' and v2 = '100') then clickstream_growthApp.userid end)) AS "100",
    actc.Like as "Like",
    actc.Share as "Share",
    actc.Saved as "Saved"

from clickstream_growthApp
    inner join cmds_video on clickstream_growthApp.v1 = cmds_video.id and clickstream_growthApp.eventlabel in ('app_classroom_video_clicked','app_classroom_thumbnail_clicked','app_classroom_videodetail_video_clicked','app_classroom_livevideo_popup_clicked','app_classroom_video_loaded')
    inner join actc on cmds_video.id = actc.Video_ID and clickstream_growthApp.grade = actc.grade

where (trunc(timestamp 'epoch'+(timestamp+19800000)/1000*interval '1 second')) = current_date-1

group by 1,2,3,4,5,6,12,13,14
having users >= 5
order by 7 DESC;"""

installs_sql_query = """
Select substring((trunc(timestamp 'epoch'+(timestamp+19800000)/1000*interval '1 second'))::text,6,5) as "Date",
    count(distinct(branchdata.id)) as "Total_install",
        count(distinct(case when ("last_attributed_touch_data__~channel" is null or "last_attributed_touch_data__~channel" = '') then branchdata.id end)) as "Organic",
        count(distinct(case when "last_attributed_touch_data__~channel" ilike '%mweb%' then branchdata.id end)) as "SEO",
        count(distinct(case when "last_attributed_touch_data__~channel" = 'youtube' then branchdata.id end)) as "youtube",
        count(distinct(case when "last_attributed_touch_data__~channel" = 'telegram_migration' then branchdata.id end)) as "Telegram",
        count(distinct(case when "last_attributed_touch_data__~channel" = 'app' then branchdata.id end)) as "Referral",
        count(distinct(case when "last_attributed_touch_data__~campaign" ilike '%[Essence]_%' then branchdata.id end)) as "UAC_Agency",
        count(distinct(case when "last_attributed_touch_data__~channel" in ('facebook','Facebook') then branchdata.id end)) as "Facebook",
        count(distinct(case when "last_attributed_touch_data__~campaign" ilike '%UAC%' then branchdata.id end)) as "UAC",
        count(distinct(case when "last_attributed_touch_data__~channel" = 'downloaded_pdf' then branchdata.id end)) as "Down_PDF"


from branchdatarealtime."data" as branchdata
where branchdata.name='INSTALL'
and (trunc(timestamp 'epoch'+(timestamp+19800000)/1000*interval '1 second')) >= current_date-30
group by 1
order by 1 DESC;
"""

overall_adoption_cohorts_sql_query = """
select 
    case when (clickstream_growthApp.timestampist::date - clickstream_growthApp.firstopened_datetime::date) <= 30 then (clickstream_growthApp.timestampist::date - clickstream_growthApp.firstopened_datetime::date)::text
         else '30+' end As D_Diff,

    substring(date_trunc('day',clickstream_growthApp.timestampist)::text,6,5) As Usage_Date,
    /* Below two columns for the tabular structure to work on Excel */
    'total' as user_grade,
    'total' as FA_Type,
    count(distinct(clickstream_growthApp.userid)) as DataType

from clickstream_growthApp
    left join (select userid,user_grade from user_app_segments) app on app.userid = clickstream_growthApp.userid

where (clickstream_growthApp.eventlabel in ('app_study_chapter_click','app_study_pdf_item_clicked','app_cr_content_click','app_doubt_submit','app_classroom_video_clicked','app_classroom_thumbnail_clicked','app_classroom_videodetail_video_clicked','app_classroom_livevideo_popup_clicked','app_classroom_video_click','app_classroom_button_click','app_classroom_chat_tab_click','app_classroom_chat_submit','app_classroom_player_click','app_classroom_live_overlay_click','app_classroom_video_click','app_classroom_button_click','app_classroom_chat_tab_click','app_classroom_chat_submit','app_classroom_player_click','app_classroom_live_overlay_click','app_classroom_video_loaded','app_classroom_video_seen','app_cr_ncert_video_click','app_test_ongoing_click','app_test_normal_click', 'app_home_test_click','app_hamburger_test_open','app_cr_class_click','app_hp_live_class_click')
        or clickstream_growthApp.eventlabel ilike 'app_home%'   or clickstream_growthApp.eventlabel ilike 'app_hp%')
    and clickstream_growthApp.timestampist::date > current_date - 30
    and (clickstream_growthApp.timestampist::date - clickstream_growthApp.firstopened_datetime::date) is not null

group by 1,
    date_trunc('day',clickstream_growthApp.timestampist),
    3,4

order by 1 asc,
    date_trunc('day',clickstream_growthApp.timestampist) desc;
"""

tvc_gs_sql_query = """
select
        count(CASE WHEN clickstream.APPNAME = 'VEDANTU' THEN clickstream.userid END) AS "WEBSITE TRAFFIC",
        count(CASE WHEN clickstream.APPNAME = 'GROWTHAPP' THEN clickstream.userid END) AS "App TRAFFIC",
        count(DISTINCT(CASE WHEN clickstream.APPNAME = 'VEDANTU' THEN u.id END)) AS "App Signups",
        count(DISTINCT(CASE WHEN clickstream.APPNAME = 'GROWTHAPP' THEN u.id END)) AS "WEBSITE Signups",
        TIMESTAMP WITH TIME ZONE 'epoch' + INTERVAL '1 second' * round(extract('epoch' from clickstream.timestampist) / 300) * 300 as timestamp1,
        substring(date_trunc('day',(TIMESTAMP WITH TIME ZONE 'epoch' + INTERVAL '1 second' * round(extract('epoch' from clickstream.timestampist) / 300) * 300))::text,1,10) as "Day",
        substring(date_trunc('hour',(TIMESTAMP WITH TIME ZONE 'epoch' + INTERVAL '1 second' * round(extract('epoch' from clickstream.timestampist) / 300) * 300))::text,12,2) as "Hour",
        substring(date_trunc('minute',(TIMESTAMP WITH TIME ZONE 'epoch' + INTERVAL '1 second' * round(extract('epoch' from clickstream.timestampist) / 300) * 300))::text,15,2) as "Minute"

from clickstream
        
    join public.user u on clickstream.userid = u.id 

where   clickstream.timestampist >= current_date - 30
        
  group BY 5
  ORDER BY 5; 
"""

# Function lists all the files present in the google drive (Can be edited to include size)
def listfiles():
    results = drive_service.files().list(
        pageSize=10,
        fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])
    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            print('{0} ({1})'.format(item['name'], item['id']))


def upload_file_to_google_drive(filename, filepath, mimetype, file_id):
    file_metadata = {'name': filename}
    media = MediaFileUpload(filepath,
                            mimetype=mimetype)
    file = drive_service.files().update(body=file_metadata,
                                        media_body=media,
                                        fileId=file_id).execute()
    os.remove(filepath)
    print('File ID uploaded: %s' % file.get('id'))


def lambda_handler(event, context):
    # listfiles()

    download_instance = download.Download()
    # data = input("Please enter your choice of which sheets to update. Please enter 'all' to update all sheets. "
    #              "Else enter the name of the sheet you want to update.")
    # data = 'all'

    cwd_dir = os.getcwd()

    download_instance.generate_and_save_csv_file(os.path.join(cwd_dir,
                                                              '/tmp/Adoption-Sectionwise.csv'),
                                                 adoption_sectionwise_sql_query)
    download_instance.generate_and_save_csv_file(os.path.join(cwd_dir,
                                                              '/tmp/Installs.csv'),
                                                 installs_sql_query)
    download_instance.generate_and_save_csv_file(os.path.join(cwd_dir,
                                                              '/tmp/DAU.csv'),
                                                 dau_sql_query)
    download_instance.generate_and_save_csv_file(os.path.join(cwd_dir,
                                                              '/tmp/Adoption_Overall.csv'),
                                                 adoption_overall_sql_query)
    download_instance.generate_and_save_csv_file(os.path.join(cwd_dir,
                                                              '/tmp/ContentVideo.csv'),
                                                 content_video_sql_query)
    download_instance.generate_and_save_csv_file(os.path.join(cwd_dir,
                                                              '/tmp/ContentPDF.csv'),
                                                 content_pdf_sql_query)
    download_instance.generate_and_save_csv_file(os.path.join(cwd_dir,
                                                              '/tmp/AdoptionCohorts.csv'),
                                                 overall_adoption_cohorts_sql_query)
	download_instance.generate_and_save_csv_file(os.path.join(cwd_dir,
                                                              '/tmp/TVC_GS.csv'),
                                                 tvc_gs_sql_query)

    # Uploads begin now
    upload_file_to_google_drive('Installs.csv',
                                '/tmp/Installs.csv',
                                'text/csv',
                                '1WSg3pASXGB2YGt2oVJyl6IED3YHPJYucKPEGLC3DvjA')
    upload_file_to_google_drive('Adoption-Sectionwise.csv',
                                '/tmp/Adoption-Sectionwise.csv',
                                'text/csv',
                                '1zrMuXGPA9kXxlbMiPD8nWuPcjZgZ8kcaeuqgWmB06nE')
    upload_file_to_google_drive('DAU.csv',
                                '/tmp/DAU.csv',
                                'text/csv',
                                '1CdxbeJaXT2-Zn-c4LMhxDdflycv92ISLVnH0bDkyax4')
    upload_file_to_google_drive('Adoption_Overall.csv',
                                '/tmp/Adoption_Overall.csv',
                                'text/csv',
                                '1bDVCB6eJpj2-JpjrdkXy3PH1jRz8yWtlblb-NU2dhhM')
    upload_file_to_google_drive('ContentVideo.csv',
                                '/tmp/ContentVideo.csv',
                                'text/csv',
                                '18TnN3EFt37HDhoVdVzxIIgXV2J94PCBptDsjKIOxwvU')
    upload_file_to_google_drive('ContentPDF.csv',
                                '/tmp/ContentPDF.csv',
                                'text/csv',
                                '1yrqB_kYI-3uypYn171C6L7UNJnW4zVqCNRWhNAJ91is')
    upload_file_to_google_drive('AdoptionCohorts.csv',
                                '/tmp/AdoptionCohorts.csv',
                                'text/csv',
                                '1CMiJe7DqtHdCfYQRvj_MROVqu1rHAdJuDXfV4BPQzdo')
	upload_file_to_google_drive('TVC_GS.csv',
                                '/tmp/TVC_GS.csv',
                                'text/csv',
                                '10kDlIg6VzgWK0hLv8DmsORV5LOxeFnDUfGtELdORBuA')

    # Below both are ONLY workable on Harkirat's account
    # upload_file_to_google_drive('ContentPDF.csv',
    #                             'Files_for_upload/ContentPDF.csv',
    #                             'text/csv',
    #                             '1MDAl40_92A2MQ1xYdmq1Xc6eYXqoQ50K')

    # Test
    # upload_file_to_google_drive('Test spreadsheet.csv',
    #                             '/tmp/DAU.csv',
    #                             'text/csv',
    #                             '1PueY0ul-B7V9-tLLPFIKdhkEPYfDuwt1MUrsAT934Xc')

# if __name__ == '__main__':
#     lambda_handler(None, None)
