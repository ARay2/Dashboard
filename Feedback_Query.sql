with response_time as
(
select doubts.closedby_email as "response_email",
       DATEDIFF(seconds,doubts.creationtime_istdatetime,min(doubtchat_message.creationtime_istdatetime)) as "Post_response_time",
       case when doubtpaidstate ilike 'PAID' then DATEDIFF(seconds,doubts.creationtime_istdatetime,min(doubtchat_message.creationtime_istdatetime)) end as "Post_paid_response_time",
       case when doubtpaidstate ilike 'UNPAID' then DATEDIFF(seconds,doubts.creationtime_istdatetime,min(doubtchat_message.creationtime_istdatetime)) end as "Post_unpaid_response_time"

from doubts
left outer join doubtchat_message on doubts.id = doubtchat_message.doubt_id
where doubtchat_message.from_role = 'TEACHER'
[[and {{creationtime}}]] 
[[and {{closedtime}}]]
[[and {{paid_state}}]]
[[and doubts.closedby_email = {{email}}]]
group by doubts.creationtime_istdatetime, doubts.closedby_email,doubts.doubtpaidstate
),
response as(
select avg(response_time.Post_response_time/60.0) as "Avg_resp_time",
avg(response_time.Post_paid_response_time/60.0) as "Avg_paid_resp_time",
avg(response_time.Post_unpaid_response_time/60.0) as "Avg_unpaid_resp_time",
response_time.response_email as "Email"

from response_time
group by response_time.response_email
)
select  distinct(closedby_email) as "Dex",
        doubt_state as Teacher_Pool,
        u.user_teacherinfo_dexinfo_dextype,
        u.contactnumber,
        avg(duration/60000.0) as Avg_Resolution_min,
        response.Avg_resp_time as "Average First Response Time",
        response.Avg_paid_resp_time as "Average paid FRT",
        response.Avg_unpaid_resp_time as "Average unpaid FRT",
        avg(case when doubts.doubtpaidstate ilike 'PAID' then DATEDIFF(seconds,doubts.creationtime_istdatetime,doubts.closed_time_istdatetime)/60.0 end) as "Average_Paid_Actual_Solving_Time",
        avg(case when doubts.doubtpaidstate ilike 'UNPAID' then DATEDIFF(seconds,doubts.creationtime_istdatetime,doubts.closed_time_istdatetime)/60.0 end) as "Average_Unpaid_Actual_Solving_Time",
        avg(DATEDIFF(seconds,doubts.creationtime_istdatetime,doubts.closed_time_istdatetime)/60.0) as "Average Actual Solving Time",
        count(closedby_email) as "Doubts_Solved",
        count(case when doubtpaidstate ilike 'PAID' then closedby_email end) as "Paid Doubts",
        count(case when doubtpaidstate ilike 'UNPAID' then closedby_email end) as "UnPaid Doubts",
        count(case when doubtpaidstate ilike 'FOS' then closedby_email end) as "FOS Doubts",
        count(case when user_doubt_feedback in('NOT_SOLVED')then closedby_email end) as "FEEDBACK_NO",
        count(case when user_doubt_feedback in('SOLVED') then closedby_email end) as "FEEDBACK_YES",
        count(case when user_doubt_feedback is Null then closedby_email end)as "FEEDBACK_BLANK",
        
        
        avg(case when t_grades='13' then (duration/60000.0) end) as Avg_Resolution_min_13,
        count(case when t_grades='13' then closedby_email end) as "Doubts_Solved_13",
        count(case when t_grades='13' and user_doubt_feedback in('NOT_SOLVED')then closedby_email end) as "FEEDBACK_NO_13",
        count(case when t_grades='13' and user_doubt_feedback in('SOLVED') then closedby_email end) as "FEEDBACK_YES_13",
        count(case when t_grades='13' and user_doubt_feedback is Null then closedby_email end)as "FEEDBACK_BLANK_13",
        
        avg(case when t_grades='12' then (duration/60000.0) end) as Avg_Resolution_min_12,
        count(case when t_grades='12' then closedby_email end) as "Doubts_Solved_12",
        count(case when t_grades='12' and user_doubt_feedback in('NOT_SOLVED')then closedby_email end) as "FEEDBACK_NO_12",
        count(case when t_grades='12' and user_doubt_feedback in('SOLVED') then closedby_email end) as "FEEDBACK_YES_12",
        count(case when t_grades='12' and user_doubt_feedback is Null then closedby_email end)as "FEEDBACK_BLANK_12",

        avg(case when t_grades='11' then (duration/60000.0) end) as Avg_Resolution_min_11,
        count(case when t_grades='11' then closedby_email end) as "Doubts_Solved_11",
        count(case when t_grades='11' and user_doubt_feedback in('NOT_SOLVED')then closedby_email end) as "FEEDBACK_NO_11",
        count(case when t_grades='11' and user_doubt_feedback in('SOLVED') then closedby_email end) as "FEEDBACK_YES_11",
        count(case when t_grades='11' and user_doubt_feedback is Null then closedby_email end)as "FEEDBACK_BLANK_11",
        
        
        avg(case when t_grades='10' then (duration/60000.0) end) as Avg_Resolution_min_10,
        count(case when t_grades='10' then closedby_email end) as "Doubts_Solved_10",
        count(case when t_grades='10' and user_doubt_feedback in('NOT_SOLVED')then closedby_email end) as "FEEDBACK_NO_10",
        count(case when t_grades='10' and user_doubt_feedback in('SOLVED') then closedby_email end) as "FEEDBACK_YES_10",
        count(case when t_grades='10' and user_doubt_feedback is Null then closedby_email end)as "FEEDBACK_BLANK_10",
        
        
        avg(case when t_grades='9' then (duration/60000.0) end) as Avg_Resolution_min_9,
        count(case when t_grades='9' then closedby_email end) as "Doubts_Solved_9",
        count(case when t_grades='9' and user_doubt_feedback in('NOT_SOLVED')then closedby_email end) as "FEEDBACK_NO_9",
        count(case when t_grades='9' and user_doubt_feedback in('SOLVED') then closedby_email end) as "FEEDBACK_YES_9",
        count(case when t_grades='9' and user_doubt_feedback is Null then closedby_email end)as "FEEDBACK_BLANK_9",
        
        
        avg(case when t_grades='8' then (duration/60000.0) end) as Avg_Resolution_min_8,
        count(case when t_grades='8' then closedby_email end) as "Doubts_Solved_8",
        count(case when t_grades='8' and user_doubt_feedback in('NOT_SOLVED')then closedby_email end) as "FEEDBACK_NO_8",
        count(case when t_grades='8' and user_doubt_feedback in('SOLVED') then closedby_email end) as "FEEDBACK_YES_8",
        count(case when t_grades='8' and user_doubt_feedback is Null then closedby_email end)as "FEEDBACK_BLANK_8",
        
        avg(case when t_grades='7' then (duration/60000.0) end) as Avg_Resolution_min_7,
        count(case when t_grades='7' then closedby_email end) as "Doubts_Solved_7",
        count(case when t_grades='7' and user_doubt_feedback in('NOT_SOLVED') then closedby_email end) as "FEEDBACK_NO_7",
        count(case when t_grades='7' and user_doubt_feedback in('SOLVED') then closedby_email end) as "FEEDBACK_YES_7",
        count(case when t_grades='7' and user_doubt_feedback is Null then closedby_email end)as "FEEDBACK_BLANK_7",
        
        avg(case when t_grades='6' then (duration/60000.0) end) as Avg_Resolution_min_6,
        count(case when t_grades='6' then closedby_email end) as "Doubts_Solved_6",
        count(case when t_grades='6' and user_doubt_feedback in('NOT_SOLVED') then closedby_email end) as "FEEDBACK_NO_6",
        count(case when t_grades='6' and user_doubt_feedback in('SOLVED') then closedby_email end) as "FEEDBACK_YES_6",
        count(case when t_grades='6' and user_doubt_feedback is Null then closedby_email end)as "FEEDBACK_BLANK_6",
        
        avg(case when t_grades='5' then (duration/60000.0) end) as Avg_Resolution_min_5,
        count(case when t_grades='5' then closedby_email end) as "Doubts_Solved_5",
        count(case when t_grades='5' and user_doubt_feedback in('NOT_SOLVED') then closedby_email end) as "FEEDBACK_NO_5",
        count(case when t_grades='5' and user_doubt_feedback in('SOLVED') then closedby_email end) as "FEEDBACK_YES_5",
        count(case when t_grades='5' and user_doubt_feedback is Null then closedby_email end)as "FEEDBACK_BLANK_5",
        
        avg(case when t_grades is null or t_grades in ('4','3','2','1') then (duration/60000.0) end) as Avg_Resolution_min_blank,
        --count(case when t_grades is null or t_grades in ('4','3','2','1') then closedby_email end) as "Doubts_Solved_blank",
        count(case when t_grades is null or t_grades in ('4','3','2','1') and user_doubt_feedback in('NOT_SOLVED') then closedby_email end) as "FEEDBACK_NO_blank",
        count(case when t_grades is null or t_grades in ('4','3','2','1') and user_doubt_feedback in('SOLVED') then closedby_email end) as "FEEDBACK_YES_blank",
        count(case when t_grades is null or t_grades in ('4','3','2','1') and user_doubt_feedback is Null then closedby_email end)as "FEEDBACK_BLANK_blank",
        FEEDBACK_NO_blank+FEEDBACK_YES_blank+FEEDBACK_BLANK_blank as "Doubts_Solved_blank",
        
        u.user_teacherinfo_dexinfo_accountname,
        u.user_teacherinfo_dexinfo_alternateemail,
        u.user_teacherinfo_dexinfo_accountnumber,
        u.user_teacherinfo_dexinfo_bankname,
        u.user_teacherinfo_dexinfo_branchname,
        u.user_teacherinfo_dexinfo_ifsc,
        u.user_teacherinfo_dexinfo_pannumber,
        u.user_teacherinfo_dexinfo_canceledchequeurl,
        u.blockedat_istdatetime
        
        
from doubts
    left outer join (select *
                     from (select u1.email,
        u1.user_teacherinfo_dexinfo_accountname,
        u1.user_teacherinfo_dexinfo_alternateemail,
        u1.user_teacherinfo_dexinfo_accountnumber,
        u1.user_teacherinfo_dexinfo_bankname,
        u1.user_teacherinfo_dexinfo_branchname,
        u1.user_teacherinfo_dexinfo_ifsc,
        u1.user_teacherinfo_dexinfo_pannumber,
        u1.user_teacherinfo_dexinfo_canceledchequeurl,
        u1.blockedat_istdatetime,
        u1.creationtime_istdatetime,
        u1.user_teacherinfo_dexinfo_dextype,
        u1.contactnumber,
        u1.role,
        rank() over (partition by u1.email order by u1.creationtime_istdatetime desc)
        
        from public.user u1)
        where rank = 1) as u on closedby_email = u.email
    inner join response on response.Email = doubts.closedby_email
    
where doubt_state in ('DOUBT_T1_SOLVED','DOUBT_T2_SOLVED')
    [[and {{creationtime}}]]
    and u.role = 'TEACHER'
    /*and u.user_teacherinfo_dexinfo_accountname is not null*/
    [[and {{grade}}]]
    [[and {{subject}}]]
    [[and {{closedtime}}]]
    [[and {{paid_state}}]]
    [[and doubts.closedby_email = {{email}}]]
group by 1,2,3,4,6,7,8,69,70,71,72,73,74,75,76,77
order by 6 desc