import hadoopy as hy
from pyspark import SparkContext, SparkConf
import random, os
import os.path
import xml.etree.ElementTree as ET

def relgradedqrel(sc, qid, qrelf, parNum=24):
    cwidlabel= sc.textFile(qrelf, parNum) \
            .map(lambda line : line.split(' ')) \
            .filter(lambda cols : len(cols) == 4) \
            .filter(lambda cols : int(cols[0]) == qid) \
            .map(lambda cols : (cols[2], int(cols[-1])))\
            .collect()
    cwidGrad = dict()
    # take the largest grade among all subtopics/facets
    # for diversity qrel
    for cwid, label in cwidlabel:
        # remove junk
        if label < 0:
            continue
        if label > 1 and label != 4:
            label = 2
        cwidGrad[cwid] = label
    return cwidGrad
def readcwidurl(sc, qid, urlfile, parNum=24):
    cwidurls= sc.textFile(urlfile + "/" + str(qid) + "/part*", parNum) \
            .map(lambda line : line.split(' ')) \
            .filter(lambda cols : len(cols) == 2) \
            .map(lambda cols : (cols[0], cols[-1].replace(",","/")))\
            .collect()
    return dict(cwidurls)
def readDid4ttsa(dictfile):
    qidCwiddocid=dict()
    for line in open(dictfile):
        cols=line.split(' ')
        qidcwid = cols[1].split('/')
        q = int(qidcwid[0])
        if q not in qidCwiddocid:
            qidCwiddocid[q]=dict()
        qidCwiddocid[q][qidcwid[1].rstrip()]=cols[0]
    return qidCwiddocid

def readInQuery(queryfile):
    queries = sc.textFile(queryfile, 1).collect()
    queries='\n'.join(queries)
    root = ET.fromstring(queries)
    qidQuery=dict()
    for query in root:
        qid = int(query.attrib['number'])
        qtype = query.attrib['type']
        shorttxt = ' '.join(query.findall('query')[0].text.replace('\n', '').replace(',', ' ').title().split())
        descrip = ' '.join(query.findall('description')[0].text.replace('\n', '').replace(',', ' ').split())
        subqueries = query.findall('subtopic')
        subquery=dict()
        for s in subqueries:
            sid = int(s.attrib['number'])
            stype = s.attrib['type']
            sshorttxt = ' '.join(s.text.replace('\n', '').split())
            subquery[sid]=(stype, sshorttxt)
        qidQuery[qid]=(qtype, shorttxt, descrip)
    return qidQuery

def createTriplet(labelDocs): 
    doc4jud=list()
    doc4test=list()
    labels = list(labelDocs.keys())
    random.shuffle(labels)
    mlnum = 25
    for l in labels:
        cwids = [d for d in labelDocs[l]]
        if len(cwids) < 4:
            continue
        random.shuffle(cwids)
        doc4jud.extend(cwids[:4])
        if len(cwids[4:]) < mlnum:
            mlnum = len(cwids[4:])
        if len(doc4jud) >= 12:
            break
    for l in labels:
        count = 0
        cwids = [d for d in labelDocs[l]]
        random.shuffle(cwids)
        for cwid in cwids:
            if cwid not in doc4jud:
                doc4test.append(cwid)
                count += 1
                if count >= mlnum:
                    break 
    return doc4jud, doc4test


# create triplets for judgment collection
def createTriplets(N=12,duplicate=1):
    outdir="/GW/D5data-2/khui/EmpiricalStudy/triplets"
    prefout=outdir+"/pref_" + str(N) + ".triplet"
    gradedout=outdir+"/grad_" + str(N) + ".triplet"
    testout=outdir+"/test_" + str(N) + ".triplet"
    # the triplet selection for preference judgment
    idxes=list(range(N))
    vs2pair=dict()
    allTriplets=list()
    allPairs=list()
    prefTriplets=list()
    def updateGain(maxtid, pidCoverage, tid2covgain, tid2pid, pid2tid):
        p1, p2, p3 = tid2pid[maxtid]
        for p in tid2pid[maxtid]:
            if pidCoverage[p] > 0:
                pidCoverage[p] -= 1
                for t in pid2tid[p]:
                    tid2covgain[t]-=1
    for i in range(N):
        if i not in vs2pair:
            vs2pair[i]=dict()
        for j in range(i+1, N):
            if j not in vs2pair:
                vs2pair[j]=dict()
            vs2pair[i][j]=len(allPairs)
            vs2pair[j][i]=len(allPairs)
            allPairs.append((i,j))
            for k in range(j+1, N):
                allTriplets.append((i,j,k))
    pidCoverage=dict(zip(list(range(len(allPairs))), [duplicate]*len(allPairs)))
    tid2pid = dict()
    pid2tid =dict()
    tid2covgain = dict()
    for tid in range(len(allTriplets)):
        i, j, k = allTriplets[tid]
        p1, p2, p3 = vs2pair[i][j], vs2pair[i][k], vs2pair[j][k]
        tid2pid[tid]=(p1,p2,p3)
        for p in tid2pid[tid]:
            if p not in pid2tid:
                pid2tid[p]=list()
            pid2tid[p].append(tid)
        tid2covgain[tid]=duplicate*3

    selectedtid=list()
    maxgain=duplicate*3
    while(True):
        maxtid, maxgain = max(tid2covgain.items(), key=lambda x:x[1])
        if maxgain==0:
            break
        updateGain(maxtid, pidCoverage, tid2covgain, tid2pid, pid2tid)
        selectedtid.append(maxtid)
        prefTriplets.append(allTriplets[maxtid])
    # select for test triplet
    # no overlap of pair
    idxLabel=dict()
    labellen=N/3
    for i in range(N):
        l = i/labellen
        if l > 3:
            l=3
        idxLabel[i]=l
    selectedpairs = list()
    testTriplets = list()
    random.shuffle(selectedtid)
    filteredtids=list()
    for t in selectedtid:
        d0, d1, d2 = allTriplets[t]
        l0, l1, l2 = idxLabel[d0], idxLabel[d1], idxLabel[d2]
        if l0 != l1 and l1 != l2:
            filteredtids.append(t)
    
    for t in filteredtids:
        p1, p2, p3 = tid2pid[t]
        if p1 not in selectedpairs and p2 not in selectedpairs and p3 not in selectedpairs:
            testTriplets.append(allTriplets[t])
            selectedpairs.extend(tid2pid[t])
            
    # the triplet selection for graded judgment
    gradedTriplets=list()
    for count in range(duplicate):
        random.shuffle(idxes)
        for i in range(len(idxes)):
            if i % 3 == 2:
                gradedTriplets.append((idxes[i], idxes[i-1], idxes[i-2]))
    #store the assignment
    with open(gradedout, 'w') as f:
        for t in gradedTriplets:
            f.write(str(t[0]) + " " + str(t[1]) + " " + str(t[2]) + "\n")
    with open(prefout, 'w') as f:
        for t in prefTriplets:
            f.write(str(t[0]) + " " + str(t[1]) + " " + str(t[2]) + "\n")
    with open(testout, 'w') as f:
        for t in testTriplets:
            f.write(str(t[0]) + " " + str(t[1]) + " " + str(t[2]) + "\n")

# create triplets based on ids
def triplets4Jud(doc4jud):
    N=len(doc4jud)
    iddir="/GW/D5data-2/khui/EmpiricalStudy/triplets"
    prefout=iddir+"/pref_" + str(N) + ".triplet"
    gradedout=iddir+"/grad_" + str(N) + ".triplet"
    prefCwids=list()
    gradCwids=list()
    if not os.path.isfile(prefout):
        createTriplets(N=N,duplicate=1)
    with open(prefout) as f:
        for l in f:
            cols=l.split(' ')
            idx0, idx1, idx2 = int(cols[0]), int(cols[1]), int(cols[2])
            prefCwids.append((doc4jud[idx0], doc4jud[idx1], doc4jud[idx2]))
    with open(gradedout) as f:
        for l in f:
            cols=l.split(' ')
            idx0, idx1, idx2 = int(cols[0]), int(cols[1]), int(cols[2])
            gradCwids.append((doc4jud[idx0], doc4jud[idx1], doc4jud[idx2]))
    return prefCwids, gradCwids
def triplets4Test(doc4jud, cwidGrad):
    N=len(doc4jud)
    iddir="/GW/D5data-2/khui/EmpiricalStudy/triplets"
    gradedout=iddir+"/grad_" + str(N) + ".triplet"
    testout=iddir+"/test_" + str(N) + ".triplet"
    prefCwids=list()
    gradCwids=list()
    if not os.path.isfile(gradedout):
        createTriplets(N=N,duplicate=1)
    with open(gradedout) as f:
        cwids=set()
        for l in f:
            cols=l.split(' ')
            idx0, idx1, idx2 = int(cols[0]), int(cols[1]), int(cols[2])
            d0, d1, d2 = doc4jud[idx0], doc4jud[idx1], doc4jud[idx2]
            l0, l1, l2 = cwidGrad[d0], cwidGrad[d1], cwidGrad[d2]
            if d0 not in cwids and d1 not in cwids and d2 not in cwids:
                gradCwids.append((d0, d1, d2))
                cwids.add(d0)
                cwids.add(d1)
                cwids.add(d2)
    with open(testout) as f:
        for l in f:
            cols=l.split(' ')
            idx0, idx1, idx2 = int(cols[0]), int(cols[1]), int(cols[2])
            d0, d1, d2 = doc4jud[idx0], doc4jud[idx1], doc4jud[idx2]
            l0, l1, l2 = cwidGrad[d0], cwidGrad[d1], cwidGrad[d2]
            if l1 != l2 and l0 != l1  and l0 != l2: 
                prefCwids.append((d0, d1, d2))
    return prefCwids, gradCwids

# output data for graded judgment
def writeCFGradedData(qid, query, description, cwiddocid, validcwidurls, outdir,
                      gradCwids, tgradCwids):
    def createGold(grade):
        if grade == 1 or grade == 2:
            gold = "\"" + "1\n2" + "\""
        else:
            gold =  "\"" + str(grade) + "\""
        return gold
    def createUrl(docid):
        url = "http://ttsa.mpi-inf.mpg.de/cwserver/converter?docid="+docid
        return url
    def createOurl(cwid):
        return validcwidurls[cwid] if cwid in validcwidurls else "-"
    lines=list()
    gradgoldreason="Judged by trained editors"
    headline = ','.join(["qid", "query","description",
                    "_golden",
                    "d1_cwid", "d1_did", "d1_ourl", "d1_url",
                        "d1_gjud_gold","d1_gjud_gold_reason",
                    "d2_cwid", "d2_did", "d2_ourl", "d2_url", 
                        "d2_gjud_gold","d2_gjud_gold_reason",
                    "d3_cwid", "d3_did", "d3_ourl", "d3_url",
                        "d3_gjud_gold","d3_gjud_gold_reason"])
    lines.append(headline)
    for doctuple in gradCwids:
        doclist = list(doctuple)
        random.shuffle(doclist)
        cwid1, cwid2, cwid3 = doclist
        line = ','.join([str(qid), query, description,
                        "False", 
                        cwid1, str(cwiddocid[cwid1]), createOurl(cwid1),createUrl(cwiddocid[cwid1]),
                         createGold(cwidGrad[cwid1]),gradgoldreason,
                        cwid2, str(cwiddocid[cwid2]), createOurl(cwid2),createUrl(cwiddocid[cwid2]), 
                         createGold(cwidGrad[cwid2]), gradgoldreason,
                        cwid3, str(cwiddocid[cwid3]), createOurl(cwid3),createUrl(cwiddocid[cwid3]),
                         createGold(cwidGrad[cwid3]),gradgoldreason])
        lines.append(line)
    random.shuffle(tgradCwids)
    t4test=tgradCwids[:len(gradCwids)+2]
    for doctuple in t4test:
        doclist = list(doctuple)
        random.shuffle(doclist)
        cwid1, cwid2, cwid3 = doclist
        line = ','.join([str(qid), query, description,
                        "True", 
                        cwid1, str(cwiddocid[cwid1]), createOurl(cwid1),createUrl(cwiddocid[cwid1]),
                         createGold(cwidGrad[cwid1]),gradgoldreason,
                        cwid2, str(cwiddocid[cwid2]), createOurl(cwid2),createUrl(cwiddocid[cwid2]), 
                         createGold(cwidGrad[cwid2]), gradgoldreason,
                        cwid3, str(cwiddocid[cwid3]), createOurl(cwid3),createUrl(cwiddocid[cwid3]), 
                         createGold(cwidGrad[cwid3]),gradgoldreason])
        lines.append(line)
    outdir = outdir + "/grad"
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    outfile=outdir+"/"+str(qid) + ".csv"
    with open(outfile, "w+") as f:
        for l in lines:
            f.write(l + "\n")
    return len(gradCwids), len(t4test)

# output data for preference judgment
def writeCFPrefData(qid, query, description, cwiddocid, validcwidurls, outdir,
                      prefCwids, tprefCwids):
    def getPrefLabel(l1, l2):
        
        if abs(l1-l2)<2:
            if l1 > l2:
                prefl="\"" + "1" + "\""
            else:
                prefl="\"" + "-1" + "\""
        else:
            if l1 > l2:
                prefl="\"" + "1\n0" + "\""
            elif l1 < l2:
                prefl="\"" + "-1\n0" + "\""
            else:
                prefl="\"" + "0" + "\"" 
        return prefl
    def createUrl(docid):
        url = "http://ttsa.mpi-inf.mpg.de/cwserver/converter?docid="+docid
        return url
    def createOurl(cwid):
        return validcwidurls[cwid] if cwid in validcwidurls else "-"
    lines=list()
    prefgoldreason="Judged by trained editors"
    headline = ','.join(["qid", "query","description",
                    "_golden",
                    "d1_cwid", "d1_did", "d1_ourl","d1_url", "d1_gjud_trec", 
                    "d2_cwid", "d2_did", "d2_ourl", "d2_url", "d2_gjud_trec",
                    "d3_cwid", "d3_did", "d3_ourl", "d3_url",  "d3_gjud_trec",
                    "d1_d2_pjud_gold", "d1_d2_pjud_gold_reason",
                    "d2_d3_pjud_gold", "d2_d3_pjud_gold_reason",
                    "d1_d3_pjud_gold","d1_d3_pjud_gold_reason"])
    lines.append(headline)
    for doctuple in prefCwids:
        doclist = list(doctuple)
        random.shuffle(doclist)
        cwid1, cwid2, cwid3 = doclist
        line = ','.join([str(qid), query, description,
                        "False", 
                        cwid1, str(cwiddocid[cwid1]), createOurl(cwid1),createUrl(cwiddocid[cwid1]),str(cwidGrad[cwid1]),
                        cwid2, str(cwiddocid[cwid2]), createOurl(cwid2),createUrl(cwiddocid[cwid2]), str(cwidGrad[cwid2]), 
                        cwid3, str(cwiddocid[cwid3]), createOurl(cwid3),createUrl(cwiddocid[cwid3]),str(cwidGrad[cwid3]),
                        getPrefLabel(cwidGrad[cwid1], cwidGrad[cwid2]), prefgoldreason,
                        getPrefLabel(cwidGrad[cwid2], cwidGrad[cwid3]),prefgoldreason,
                        getPrefLabel(cwidGrad[cwid1], cwidGrad[cwid3]), prefgoldreason])
        lines.append(line)
    random.shuffle(tprefCwids)
    t4test=tprefCwids[:len(prefCwids)+2]
    for doctuple in t4test:
        doclist = list(doctuple)
        random.shuffle(doclist)
        cwid1, cwid2, cwid3 = doclist
        line = ','.join([str(qid), query, description,
                        "True", 
                        cwid1, str(cwiddocid[cwid1]), createOurl(cwid1),createUrl(cwiddocid[cwid1]),str(cwidGrad[cwid1]),
                        cwid2, str(cwiddocid[cwid2]), createOurl(cwid2),createUrl(cwiddocid[cwid2]), str(cwidGrad[cwid2]), 
                        cwid3, str(cwiddocid[cwid3]), createOurl(cwid3),createUrl(cwiddocid[cwid3]),str(cwidGrad[cwid3]),
                        getPrefLabel(cwidGrad[cwid1], cwidGrad[cwid2]), prefgoldreason,
                        getPrefLabel(cwidGrad[cwid2], cwidGrad[cwid3]), prefgoldreason,
                        getPrefLabel(cwidGrad[cwid1], cwidGrad[cwid3]), prefgoldreason])
        lines.append(line)
    outdir = outdir + "/pref"
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    outfile=outdir+"/"+str(qid) + ".csv"
    with open(outfile, "w+") as f:
        for l in lines:
            f.write(l + "\n")
    return len(prefCwids), len(t4test)

def getLdocs(cwidGrad):
    labelDocs=dict()
    for d, l in list(cwidGrad.items()):
        if l not in labelDocs:
            labelDocs[l]=list()
        labelDocs[l].append(d)
    return labelDocs


conf = (SparkConf()
        .setAppName("empirical_study")
        .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        .set("spark.local.dir","/GW/D5data-2/khui/cw-docvector-termdf/tmp")
        .set("spark.kryoserializer.buffer.mb","128")
        .set("spark.driver.maxResultSize","2g"))
sc = SparkContext(conf = conf)

dictfile="/GW/D5data-2/khui/Clueweb12/html/cw4y/dictionary/did2docurl.dict"
urlfile="/user/khui/data/ExtractCwdocs/cwidurl/cw4y"
queryfile="/user/khui/data/query/wtff.xml"
outdir="/GW/D5data-2/khui/EmpiricalStudy/crowdFlower/inputdata"

qidQuery=readInQuery(queryfile)
qidCwiddocid = readDid4ttsa(dictfile)
qidyear=dict(zip(list(range(101, 301)), ['wt11']*50+['wt12']*50+['wt13']*50+['wt14']*50))
qids=[268,280]
#list(range(255,301)) + list(range(201,251))
for qid in qids:
    year=qidyear[qid]
    qrelf="/user/khui/data/qrel/qrels.adhoc." + year
    cwidGrad=relgradedqrel(sc, qid, qrelf, parNum=24)
    labelDocs = getLdocs(cwidGrad)
    doc4jud, doc4test = createTriplet(labelDocs)
    cwidurls = readcwidurl(sc, qid, urlfile, parNum=24)
    qtype, query, descrip = qidQuery[qid]
    prefCwids, gradCwids = triplets4Jud(doc4jud)
    tprefCwids, tgradCwids = triplets4Test(doc4test, cwidGrad)
    actgt4j, actgt4t=\
    writeCFGradedData(qid, query, descrip, qidCwiddocid[qid], cwidurls, outdir, gradCwids, tgradCwids)
    actpt4j, actpt4t=\
    writeCFPrefData(qid, query, descrip, qidCwiddocid[qid], cwidurls, outdir, prefCwids, tprefCwids)
    print(' '.join(["INFO",str(qid), 'dnum4j:%d'%len(doc4jud), 'dnum4t:%d'%len(doc4test),\
            'ptnum4j:%d'%len(prefCwids), 'ptnum4t:%d'%len(tprefCwids), \
            'actpt4j:%d'%actpt4j, 'actpt4t:%d'%actpt4t, \
            'gtnum4j:%d'%len(gradCwids), 'gtnum4j:%d'%len(tgradCwids),\
            'actgt4j:%d'%actgt4j, 'actgt4j:%d'%actgt4t]))
