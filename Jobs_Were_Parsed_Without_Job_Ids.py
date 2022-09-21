from email.mime.base import MIMEBase
from email import encoders
import MySQLdb
import smtplib, ssl
import sys
import csv
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime
from collections import OrderedDict
from datetime import datetime

db = MySQLdb.connect(host="vcs-mdb1.vacancysoft.com",    # your host, usually localhost
                     user="parser_py",         # your username
                     passwd="qMJ3%2QF",  # your password
                     db="vcs_monitoring")        # name of the data base

# you must create a Cursor object. It will let
#  you execute all the queries you need
cur = db.cursor()

# Use all the SQL you like
cur.execute("select ra.company_id, c.company, case when ra.agent_id = 0 then 'cd' when ra.agent_id = 1 then 'ats' when ra.agent_id = 3 then 'table parser' when ra.agent_id = 9 then 'aggs' end as parsing_agent, ra.job_id,	ra.external_ref_id, ra.url, ra.location	from vcs_audit.raw_audit ra inner join vcs.companies c on ra.company_id = c.id where 	(ra.job_id is null or ra.job_id = '' or ra.job_id = ' ')	and ra.tstamp > subdate(now(), interval 1 day)	group by ra.company_id;")

myallData = cur.fetchall()
all_company_id = []
all_company = []
all_parsing_agent = []
all_job_id  = []
all_external_ref_id = []
all_url = []
all_location = []
for company_id, company,parsing_agent,job_id,external_ref_id,url,location in myallData:
    all_company_id.append(company_id)
    all_company.append(company)
    all_parsing_agent.append(parsing_agent)
    all_job_id.append(job_id)
    all_external_ref_id.append(external_ref_id)
    all_url.append(url)
    all_location.append(location)

dic = {'Company_ID ': all_company_id, 'Company ': all_company,'Parsing_Agent ': all_parsing_agent,'Job_ID': all_job_id,'External_Ref_Id':all_external_ref_id, 'Url': all_url,'Location' : all_location}

df = pd.DataFrame(dic)
df_csv = df.to_csv('Jobs_Were_Parsed_Without_Job_Ids.csv',sep=';',index=False)

# set your email and password
sender_email = "system-stats@vacancysoft.com"
password = "Ul2kVXoCFIZ7o1Zvi6No-Q"
receiver_email = 'royce.hakim@vacancysoft.com','iryna.khashchina@vacancysoft.com'

today = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
msg = MIMEMultipart()
msg['Subject'] = 'ALERT - Jobs were parsed without job ids  ' + str(today)
msg['From'] = sender_email
msg['To'] = ','.join(receiver_email)

part = MIMEBase('application', 'octet-steam')
part.set_payload(open('Jobs_Were_Parsed_Without_Job_Ids.csv','rb').read())
encoders.encode_base64(part)
part.add_header('Content-Disposition','attachment; filename = "Jobs_Were_Parsed_Without_Job_Ids.csv"')
body = "Invalid Combination - Jobs were parsed without job ids"
msg.attach(MIMEText((body)))
msg.attach(part)

s = smtplib.SMTP(host = 'smtp.mandrillapp.com', port = 587)
s.login(user=sender_email, password=password)
s.sendmail(sender_email,receiver_email,msg.as_string())


