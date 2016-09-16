import hadoopy as hy
from pyspark import SparkContext, SparkConf
import random, os, math
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
    qidCwiddocid = dict()
    for line in open(dictfile):
        cols=line.split(' ')
        qidcwid = cols[1].split('/')
        q = int(qidcwid[0])
        if q not in qidCwiddocid:
            qidCwiddocid[q]=dict()
        qidCwiddocid[q][qidcwid[1].rstrip()]=cols[0]
    return qidCwiddocid

def labelDocs(cwidGrad):
    ldocs=dict()
    for d, l in list(cwidGrad.items()):
        if l not in ldocs:
            ldocs[l]=list()
        ldocs[l].append(d) 
    return ldocs
def readInQuery(queryfile):
    queries = sc.textFile(queryfile, 1).collect()
    queries='\n'.join(queries)
    root = ET.fromstring(queries)
    qidQuery=dict()
    for query in root:
        qid = int(query.attrib['number'])
        qtype = query.attrib['type']
        shorttxt = ' '.join(query.findall('query')[0].text.replace('\n', '').\
                replace(',', ' ').title().split())
        descrip = ' '.join(query.findall('description')[0].text.replace('\n', '').\
                replace(',', ' ').split())
        subqueries = query.findall('subtopic')
        subquery=dict()
        for s in subqueries:
            sid = int(s.attrib['number'])
            stype = s.attrib['type']
            sshorttxt = ' '.join(s.text.replace('\n', '').split())
            subquery[sid]=(stype, sshorttxt)
        qidQuery[qid]=(qtype, shorttxt, descrip)
    return qidQuery
def selectDocs(labelDocs, cwidGrad, judnum=12): 
    doc4jud=list()
    doc4test=list()
    labels = list(labelDocs.keys())
    docnumperL = judnum / len(labels)
    for l in labels:
        cwids = labelDocs[l]
        if len(cwids) < 3:
            continue
        random.shuffle(cwids)
        doc4jud.extend(cwids[:docnumperL])
    allcwids = list(cwidGrad.keys())
    random.shuffle(allcwids)
    for cwid in allcwids:
        if len(doc4jud) >= judnum:
            break;
        if cwid not in doc4jud:
            doc4jud.append(cwid)
    doc4test=[cwid for cwid in allcwids if cwid not in doc4jud]
    return doc4jud, doc4test
def createJudPairs(doc4jud):
    return [(doc4jud[i], doc4jud[j]) for i in range(len(doc4jud)) for j in range(i+1, len(doc4jud))]
def createTestPairs(doc4test, cwidGrad):
    testpairs=list()
    for i in range(len(doc4test)):
        for j in range(i+1, len(doc4test)):
            d1, d2 = doc4test[i], doc4test[j]
            if cwidGrad[d1] != cwidGrad[d2]:
                testpairs.append((doc4test[i], doc4test[j]))
    return testpairs
def createTestDoc(ldocs, alldocs2consider, testnum):
    labledocs = dict(ldocs)
    testdocs=list()
    grads = list(labledocs.keys())
    labelidx=dict(zip(grads, [0]*len(grads)))
    i=len(grads)-1
    while len(testdocs) < testnum:
        l = grads[i % len(grads)]
        i+=1
        currentidx = labelidx[l]
        for j in range(currentidx, len(labledocs[l])):
            doc2consider = labledocs[l][j]
            if doc2consider in alldocs2consider:
                testdocs.append(doc2consider)
                labelidx[l]=j+1
                break
    return testdocs
def writeCFPrefData(qid, query, description, cwiddocid, cwidourls, 
                    outdir, pair4jud, pair4test):
    def getPrefLabel(l1, l2):
        if abs(l1-l2)==2:
            if l1 > l2:
                prefl="\"" + "1" + "\""
            elif l1 < l2:
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
        return cwidourls[cwid] if cwid in cwidourls else "-"
    lines=list()
    prefgoldreason="Judged by trained editors."
    headline = ','.join(["lid", "qid", "query","description",
                    "_golden",
                    "d1_cwid", "d1_did", "d1_ourl","d1_url", "d1_gjud_trec", 
                    "d2_cwid", "d2_did", "d2_ourl", "d2_url", "d2_gjud_trec",
                    "d1_d2_pjud_gold", "d1_d2_pjud_gold_reason"])
    #lines.append(headline)
    for docpair in pair4jud:
        doclist = list(docpair)
        random.shuffle(doclist)
        cwid1, cwid2 = docpair
        lineid = "PJ" + str(qid) + "%03d"%(len(lines))
        line = ','.join([lineid, str(qid), query, description,"False",\
                cwid1, str(cwiddocid[cwid1]), createOurl(cwid1),\
                createUrl(cwiddocid[cwid1]),str(cwidGrad[cwid1]),\
                cwid2, str(cwiddocid[cwid2]), createOurl(cwid2),\
                createUrl(cwiddocid[cwid2]), str(cwidGrad[cwid2]),\
                getPrefLabel(cwidGrad[cwid1], cwidGrad[cwid2]), prefgoldreason])
        lines.append(line)
    for docpair in pair4test:
        doclist = list(docpair)
        random.shuffle(doclist)
        lineid = "PT" + str(qid) + "%03d"%(len(lines))
        cwid1, cwid2 = docpair
        line = ','.join([lineid, str(qid), query, description,"True",\
                cwid1, str(cwiddocid[cwid1]), createOurl(cwid1),\
                createUrl(cwiddocid[cwid1]),str(cwidGrad[cwid1]),\
                cwid2, str(cwiddocid[cwid2]), createOurl(cwid2),\
                createUrl(cwiddocid[cwid2]), str(cwidGrad[cwid2]),\
                getPrefLabel(cwidGrad[cwid1], cwidGrad[cwid2]), prefgoldreason])
        lines.append(line)
    outdir = outdir + "/pref"
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    outfile=outdir+"/"+str(qid) + ".csv"
    with open(outfile, "w+") as f:
        for l in lines:
            f.write(l + "\n")
    return headline

def writeCFTitle(rootoutdir, prefTitle, gradTitle):
    outdir = rootoutdir + "/pref"
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    outfile=outdir+"/title.csv"
    with open(outfile, "w+") as f:
        f.write(prefTitle)
    outdir = rootoutdir + "/grad"
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    outfile=outdir+"/title.csv"
    with open(outfile, "w+") as f:
        f.write(gradTitle)

def writeCFGradedData(qid, query, description, 
                      cwiddocid, cwidourls, outdir,
                      doc4jud, doc4test):
    def createGold(grade):
        if grade == 1 or grade == 2:
            gold = "\"" + "1\n2" + "\""
        else:
            gold =  "\"" + str(grade) + "\""
        return gold
    def createUrl(docid):
        url = "http://ttsa.mpi-inf.mpg.de/cwserver/converter?docid="+docid
        return url
            
    lines=list()
    gradgoldreason="Judged by trained editors"
    headline = ','.join(["lid", "qid", "query","description",
                    "_golden",
                    "d_cwid", "d_did", "d_ourl", "d_url",
                        "d_gjud_gold","d_gjud_gold_reason"])
    #lines.append(headline)
    for cwid in doc4jud:
        lineid = "GJ" + str(qid) + "%03d"%(len(lines))
        line = ','.join([lineid, str(qid), query, description,"False",\
                cwid, str(cwiddocid[cwid]), cwidourls[cwid],createUrl(cwiddocid[cwid]),\
                createGold(cwidGrad[cwid]),gradgoldreason])
        lines.append(line)

    for cwid in doc4test:
        lineid = "GT" + str(qid) + "%03d"%(len(lines))
        line = ','.join([lineid, str(qid), query, description,\
                        "True", cwid, str(cwiddocid[cwid]), \
                        cwidourls[cwid],createUrl(cwiddocid[cwid]),\
                        createGold(cwidGrad[cwid]),gradgoldreason])
        lines.append(line)
    outdir = outdir + "/grad"
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    outfile=outdir+"/"+str(qid) + ".csv"
    with open(outfile, "w+") as f:
        for l in lines:
            f.write(l + "\n")
    return headline

def writeTriplet(qid, judtriplet, testtriplet, outdir):
    lines=list()
    tid=0
    for dtup in judtriplet:
        d1, d2, d3 = sorted(list(dtup))
        lines.append(' '.join([str(tid), 'jud', d1, d2, d3]))
        tid+=1
    for dtup in testtriplet:
        d1, d2, d3 = sorted(list(dtup))
        lines.append(' '.join([str(tid), 'test', d1, d2, d3]))
        tid+=1
    outdir = outdir + "/triplet"
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    outfile=outdir+"/"+str(qid) + ".triplet"
    with open(outfile, "w+") as f:
        for l in lines:
            f.write(l + "\n")

def createTriplet(cwids):
    doctriplet=list()
    docnum = len(cwids)
    cwids.sort()
    for i in range(docnum):
        for j in range(i+1, docnum):
            for k in range(j+1, docnum):
                doctriplet.append((cwids[i], cwids[j], cwids[k]))
    return doctriplet


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
outdir="/GW/D5data-2/khui/EmpiricalStudy/crowdFlower/data4jud"

qidyear=dict(zip(list(range(101, 301)), ['wt11']*50+['wt12']*50+['wt13']*50+['wt14']*50))
dictfile="/GW/D5data-2/khui/Clueweb12/html/cw4y/dictionary/did2docurl.dict"
qidCwiddocid = readDid4ttsa(dictfile)
queryfile="/user/khui/data/query/wtff.xml"
qidQuery=readInQuery(queryfile)
titleWrote=False
qids=list(range(251,301)) + list(range(201,251))
for qid in qids:
    year=qidyear[qid]
    qrelf="/user/khui/data/qrel/qrels.adhoc." + year
    qtype, query, descrip = qidQuery[qid]
    cwidGrad=relgradedqrel(sc, qid, qrelf, parNum=24)
    ldocs = labelDocs(cwidGrad)
    cwidurls = readcwidurl(sc, qid, urlfile, parNum=24)
    doc4jud, remaindocs = selectDocs(ldocs, cwidGrad, judnum=12)
    doc4test = createTestDoc(ldocs, remaindocs, testnum=12)
    pair4jud=createJudPairs(doc4jud)
    pair4test=createTestPairs(doc4test, cwidGrad)
    judtriplet, testtriplet = createTriplet(doc4jud), createTriplet(doc4test)
    writeTriplet(qid, judtriplet, testtriplet, outdir)
    prefTitle = writeCFPrefData(qid, query, descrip, qidCwiddocid[qid], cwidurls, outdir, pair4jud, pair4test)   
    gradTitle = writeCFGradedData(qid, query, descrip, qidCwiddocid[qid], cwidurls, outdir,  doc4jud, doc4test)    
    if not titleWrote:
        writeCFTitle(outdir, prefTitle, gradTitle)
        titleWrote=True
    print(' '.join(["INFO",str(qid), 'dnum4j:%d'%len(doc4jud), 'dnum4t:%d'%len(doc4test),\
                'ptnum4j:%d'%len(pair4jud), 'ptnum4t:%d'%len(pair4test)]))
