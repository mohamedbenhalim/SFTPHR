import os

import config as cf
import pysftp
import csv
import shutil
import pyodbc as bd
import datetime
import zipfile


x = {}
xs = []
xs2 = []
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None
ServerFiles = []
LocalFiles = []

cnxn = bd.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + cf.localserver + ';DATABASE=' + cf.localdatabase + ';UID=' + cf.localusername + ';PWD=' + cf.localpassword, )
cur = cnxn.cursor()

sql = 'select distinct file_name from HR_files'
cur.execute(sql)
for each in cur:
    LocalFiles.append(str(each.file_name).strip())
    ServerFiles.append(str(each.file_name).strip())


for (dirpath, dirnames, filenames) in os.walk(cf.infolder):
    LocalFiles.extend(filenames)
for (dirpath, dirnames, filenames) in os.walk(cf.outfolder):
    LocalFiles.extend(filenames)

latest = 0
latestfile = None
sftp = pysftp.Connection(host=cf.sftpurl, username=cf.sftpuser, password=cf.sftppwd, private_key=".ppk", cnopts=cnopts)
directory_structure = sftp.listdir_attr(remotepath=cf.sftpfromfolder)
for attr in directory_structure:
    if attr.filename.startswith('Employee') or attr.filename.startswith('Leave') or attr.filename.startswith('Payroll')\
            or attr.filename.startswith('Balance'):
        print(attr.filename, attr)
        if attr.filename not in LocalFiles:
            sftp.get(cf.sftpfromfolder + attr.filename, cf.infolder + attr.filename)




def preparedata(filename):
    xs2=[]
    file = open(cf.infolder + filename, 'r', encoding='UTF-8')
    csv_file = csv.DictReader(file, delimiter=',')
    for row in csv_file:
        xs = []
        for k, v in row.items():
            temp = v
            xs.append(temp)
        xs2.append(xs)
    print (xs2)
    file.close()
    return xs2

x=os.listdir(cf.infolder)
for each in x:
    if each.split(".")[-1:][0]=='zip':
        z = zipfile.ZipFile(cf.infolder+"/"+each)
        z.setpassword(cf.zippassword.encode())
        z.extractall(cf.infolder)
        z.close()
        shutil.move(cf.infolder+each, cf.zipfolder+each,)

x=os.listdir(cf.infolder)
for each in x:
    try :
        xs2=preparedata(each)
        if len(xs2)>10:
            if each.startswith('Employee'):
                cur = cnxn.cursor()
                cur.execute("truncate table [UAT_ATIB002].[dbo].[HR_employee] ")
                cur.executemany('''insert into HR_employee (id,firstName,middleName,lastName,preferredName,birthDate,nationalID,
                Gender,martialStatus,address1,address2,city,zip,personalEmail,workEmail,workPhone,homePhone,Mobile,hireData,
                activeState,status,lastDay,exitMeeting,requestedBy,Reason,Notice,noticePeriod,commentsBoarding,userGroup,
                affiliateID,stateId,lastAccessDate,lastActivityDate,Language,workMail,modifiedBy,modifiedOn,userAccess,
                employeeCode,employeeSSN,passportNumber,driverLicence,OffBoardingDate,departmentID,departmentName,affiliateName,
                employeeTitle)
                 VALUES (?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,
                    ?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?)''', xs2)

                cur.execute("insert into HR_files (file_name,proccessing_date,status) VALUES (?,	?,	?) ",[each,datetime.datetime.now(),"done"])
                cnxn.commit()

                shutil.move(cf.infolder + each, cf.outfolder + each)
            if each.startswith('Leave'):
                cur = cnxn.cursor()
                cur.execute("truncate table [UAT_ATIB002].[dbo].[HR_leaves] ")
                cur.executemany(
                '''insert into HR_leaves (emp_id,	lastName,	leaveType,	startDate,	endDate,	startTime,	endTime,	deductionDays,
                    durationDays,	durationMinutes,	requestState)values (?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?)''', xs2)
                cur.execute("insert into HR_files (file_name,proccessing_date,status) VALUES (?,	?,	?) ",[each,datetime.datetime.now(),"done"])
                cnxn.commit()
                shutil.move(cf.infolder + each, cf.outfolder + each)
            if each.startswith('Payroll'):
                if each not in ServerFiles:
                    cur = cnxn.cursor()
                    cur.executemany('''insert into HR_payroll  (Start_date,End_date,Serial,Employee_Name,
                        Gross_Pay,Employee_WithHoldings,Company_WithHoldings,Net_Pay,Account,
                Base_Gross_Salary,Adjustment,Gross_Allownaces,Deductions_,Delay,Absence,Bonus,Additional_Salary_13,
                [حصة جهة العمل],[حصة المضمون],[ضريبة جهاز التضامن],[ضريبة الجهاد],[ضريبة الدخل],[خصم النقابة],[دمغة ضريبة الدخل])
                         VALUES (?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?,	?
                            )''', xs2)
                    cur.execute("insert into HR_files (file_name,proccessing_date,status) VALUES (?,	?,	?) ",
                                [each, datetime.datetime.now(), "done"])

                cnxn.commit()
            if each.startswith('Balance'):
                cur = cnxn.cursor()
                cur.execute("truncate table [UAT_ATIB002].[dbo].[HR_leave_balance] ")
                cur.executemany('''insert into HR_leave_balance (emp_id,lastName,Leave,Scheduled,Taken,Balance)
                            VALUES (?,	?,	?,	?,	?,	?)''',
                                xs2)

                cur.execute("insert into HR_files (file_name,proccessing_date,status) VALUES (?,	?,	?) ",
                            [each, datetime.datetime.now(), "done"])
                cnxn.commit()

                shutil.move(cf.infolder + each, cf.outfolder + each)
    except:
        shutil.move(cf.infolder + each, cf.errorfolder + each)



cnxn.close()
