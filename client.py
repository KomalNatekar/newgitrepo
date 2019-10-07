import gearman
import json
import random
import pymongo
from pymongo import MongoClient
import redis
import ast

def check_request_status(job_request):
    if job_request.complete:
        print("Job ",job_request.job.unique," is finished!")    
    elif job_request.timed_out:
        print("Job ",job_request.unique," timed out!" )
    
    d=json.loads(job_request.result)
    return d
    
gm_client = gearman.GearmanClient(['localhost:4730'])

data="she is dancing"
qid=random.randint(1,101)
new_dict={'qid_'+str(qid):data}
uid=random.randint(1,101)
r=redis.StrictRedis(host='localhost',port=6379,password=None,db=12)
r.set('uid_'+str(uid),new_dict)
print('Data inserted in Redis')

print('Fetching data!!')
key=input('Enter uid: ')
output=r.get(key)
ndict=ast.literal_eval(output.decode('utf-8')) 

list_of_jobs = [dict(task="lemma", data=json.dumps(ndict)), dict(task="postags", data=json.dumps(ndict)),
dict(task="stem", data=json.dumps(ndict)),dict(task="reverse", data=json.dumps(ndict))]
submitted_requests = gm_client.submit_multiple_jobs(list_of_jobs, background=False, wait_until_complete=False)

rlist=[]
completed_requests = gm_client.wait_until_jobs_completed(submitted_requests, poll_timeout=5.0)
for completed_job_request in completed_requests:
    d=check_request_status(completed_job_request)
    rlist.append(d)

olist=['lemma','pos_tags','stem','reverse']
fdict=dict(zip(olist,rlist))

sdict={'uid':key,'data':new_dict,'result':fdict}
arr=[{'data':new_dict,'result':fdict}]
#arr={'data':new_dict,'result':fdict}
client=MongoClient(host='localhost',port=27017)
database=client.mdb101

if database.new1.find_one({'uid':sdict['uid']}):
    g=database.new1.find_one({'uid':sdict['uid']})
    m=[]
    for i in g:
        m.append(g)
    v=[]
    for i in m :
        for j in i['x']:
            for k in j['data'].values():
                v.append(list(j['data'].values()))
                for l in v:
                    if l[0]==data:
                        pass
                    else:
                        database.new1.update_one({'uid':key},{'$push':{'x':arr[0]}})
    
    print('if')          
else:
    database.new1.insert_one({'uid':key,'x':[{'data':new_dict,'result':fdict}]})
    
    print('else')

print('Data inserted in Mongo!!')
