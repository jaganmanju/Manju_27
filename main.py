import pandas as pd
import pymongo
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["repo"]
mydb2 = myclient["master_repo"]
mycol2 = mydb2["master_collection"]
mycol = mydb["golden_repo"]
mycol1=mydb["guid_repo"]
cur=mycol1.find()
list_cur=list(cur)
print(list_cur)
guid_df=pd.DataFrame(list_cur)
def getfilepath():
    a=mycol2.find({},{'domain_id':1,'IBUCKETPATH':1})
    path=[]
    for data in a:
        c=((data['IBUCKETPATH']))
        path1=' '.join(map(str, path))
        print(type(path1))
        return c
a=getfilepath()
print(a)
def inputid(a):
    input_df = pd.read_csv(a)
    print(input_df)
    a = input_df['GUID'].tolist()
    print(a)
    golden_df = pd.DataFrame(list_cur)
    # print(golden_df)
    b = golden_df['GUID'].tolist()
    print(b)
    # c = [int(x) for x in b]
    #Comparing two lists#
    merge_guid = []
    for i in a:
        if i in b:
            print('Matched records with GR which needs to be unmerged', (i))
            merge_guid.append(str(i))
        else:
            print('Records which doesnot match', (i))
    print(merge_guid)
    return merge_guid
b=inputid(a)
class unmerge:
    def __init__(self,id):
        self.id=id

    #Function for adding the flag field  for the GUIDS unmerge
    def unmerge_golden(self):
        for item in self.id:
            filter, update = {"GUID": item}, {"$set": {"unmerge": "yes"}}
            mycol.update_many(filter, update)
            print("Unmerged Record {} Updated in Mongodb".format(item))
#
    def unmerge_remove_ind_guid(self):
        # col_name = 'Consolidation_Ind'
        for item in self.id:
            filter, update = {"GUID": item}, {'$unset': {'Consolidation_Ind': " "}}
            mycol1.update_many(filter, update)
            print("Removed the consolidationindicatorof {} and Updated in Mongodb".format(item))
#
    def unmerge_guidrepo_update_oldguid(self):
        for item in self.id:
            filter, update = {"GUID": item}, {"$set": {"OLDGUID": item}}
            mycol1.update_many(filter, update)
            print("Updated oldguid {} in Mongodb".format(item))

    def unmerge_empty_guid(self):
        for item in self.id:
            filter, update = {"GUID": item}, {"$set": {"GUID": " "}}
            mycol1.update_many(filter, update)
            print("Removed the oldguid {} and Updated in Mongodb".format(item))

    import smtplib

    def send_trigger_mail():
        sender = 'manjunath.jagadeesan@gmail.com'
        receiver = "manjudataengineer@gmail.com"
        password = "zljo kvqs uvkh epac"  # Gmail App Password

        message = """Subject: GUID matched results

    GUID matching results

    #1234 = matched
    #4567 = unmatched
    """

        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, receiver, message)
        server.quit()

        print("Mail sent successfully")

    send_trigger_mail()


#
#
def main():
    b = unmerge(inputid(getfilepath()))
    b.unmerge_golden()
    b.unmerge_remove_ind_guid()
    b.unmerge_guidrepo_update_oldguid()
    b.unmerge_empty_guid()

main()

