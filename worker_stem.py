import gearman
import json
from nltk.stem import PorterStemmer 
from nltk.tokenize import word_tokenize 

gm_worker = gearman.GearmanWorker(['localhost:4730'])

def task_stem(gearman_worker, gearman_job):
    x=[]
    y=[]
    ps = PorterStemmer() 
    jobdata=json.loads(gearman_job.data)
    qid=list(jobdata.keys())[0]
    words = word_tokenize(jobdata[qid]) 
    for w in words:
        x.append(w)
        y.append(ps.stem(w))
    s=dict(zip(x,y))
    return json.dumps(s)


gm_worker.set_client_id('python-worker')
gm_worker.register_task('stem',task_stem)
gm_worker.work()