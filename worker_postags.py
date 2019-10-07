import gearman
import json
from nltk import word_tokenize, pos_tag

gm_worker = gearman.GearmanWorker(['localhost:4730'])

def task_pos(gearman_worker,gearman_job):
    x=[]
    y=[]
    jobdata=json.loads(gearman_job.data)
    qid=list(jobdata.keys())[0]
    text = word_tokenize(jobdata[qid]) 
    tagged = pos_tag(text)
    for w in text:
        x.append(w)
    for i in tagged:
        y.append(i[1])
    k=dict(zip(x,y))

    return json.dumps(k)
    
gm_worker.register_task('postags',task_pos)
gm_worker.work()