host = "localhost"
port = 10000
db = "sigma_impl"


conn = hive.connect(host=host, port=port)
cur = conn.cursor()
cur.execute("SHOW TABLES FROM %s"%db)
tbl_lis = cur.fetchall()

meta_lis = []

for tbl in tbl_lis:
    print(tbl[0])
    cur.execute("DESC %s.%s" %(db, tbl[0]))
    meta = cur.fetchall()
    
    
    for m in meta:
        lis = list(m)
    
        lis.insert(0, tbl[0])
        
        meta_lis.append(lis)
    
    
df = pd.DataFrame(meta_lis)
df.to_csv("../meta_data.tsv",sep='\t',index=None,header=None)
    
