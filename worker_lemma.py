import gearman
import json
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize

gm_worker = gearman.GearmanWorker(['localhost:4730'])

def task_lemma(gearman_worker, gearman_job):
    x=[]
    y=[]
    lemmatizer = WordNetLemmatizer()  
    jobdata=json.loads(gearman_job.data)
    qid=list(jobdata.keys())[0]
    words = word_tokenize(jobdata[qid]) 
    for w in words:
        x.append(w)
        y.append(lemmatizer.lemmatize(w))
    l=dict(zip(x,y))
    return json.dumps(l)

gm_worker.register_task('lemma',task_lemma)
gm_worker.work()