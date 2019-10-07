import gearman
import json


gm_worker = gearman.GearmanWorker(['localhost:4730'])

def task_reverse(gearman_worker, gearman_job):
    
    jobdata=json.loads(gearman_job.data)
    qid=list(jobdata.keys())[0]
    print('Reversing String: ',jobdata[qid])
    result={'result':jobdata[qid][::-1]} 
    return json.dumps(result)  

gm_worker.register_task('reverse',task_reverse)
gm_worker.work()