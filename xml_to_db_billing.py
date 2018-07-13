#!/usr/bin/python

import MySQLdb
import xml.etree.ElementTree as ET
import time
import os
from ftplib import FTP

ftp_host = '192.168.1.13'
# ftp_host = '192.168.1.11'
ftp_user = 'lamjaya'
ftp_pass = 'kalong.1'
ftp_local_dir = os.path.join(os.path.dirname(__file__), "file/input/")
ftp_remote_dir = 'Upload'
# ftp_remote_dir = ''

db_host = '192.168.1.235'
db_user = 'lamsolusi'
db_pass = '4rfv!@#$'
db_name = 'assarent_lamjaya'
db_row_size = 500

try:
    db = MySQLdb.connect(db_host, db_user, db_pass, db_name)
except Exception as e:
    print "Failed to connect to database server : " + str(e)
    exit()

# TODO : parsing yg di failed
# parse_file(f)

try:
    ftp = FTP(ftp_host)
    ftp.login(ftp_user, ftp_pass)
except Exception as e:
    print "Failed to connect to ftp server : " + str(e)
    exit()

db_cursor = db.cursor()

# list dulu file di local sebagai acuan file mana yg belum terdownload
local_files = os.listdir(ftp_local_dir)
remote_files = ftp.nlst(ftp_remote_dir)

# TODO: download file dulu. parsing belakangan

for f in remote_files:
    # kalau belum ada di local file dan extensi .xml proses file
    if f not in local_files and ".xml" in f:
        # tentukan table mana
        if "FPLT" in f:
            sql = "REPLACE INTO MF_CONTRACT_BILLING_PLAN (CHR_SalesDocument,CHR_ItemSD,CHR_BillingPlanNumber,CHR_ItemBilling,DAT_SettlemetDateDeadline,DAT_SettlementDate,DAT_BillingDateIndex,INT_BIllingValue,CHR_BillingBlock,CHR_BillingStatus) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        elif "VBAK" in f:
            sql = "REPLACE INTO MF_CONTRACT_HEADER (CHR_SalesDocument,CHR_SoldToParty,DAT_CreatedOn,TIM_Time,CHR_CreatedBy,DAT_ChangeOn,DAT_DocumentDate,CHR_PurchaseOrderNo,DAT_PurchaseOrderDate,CHR_OrderReason,CHR_CollectiveNumber,INT_NetValue,CHR_Currency,CHR_SalesOrganization,CHR_DocConditionNo,CHR_Description,CHR_SalesDocumentType,CHR_Billing_block,CHR_MasterContract,DAT_ContractStartDate,DAT_ContractEndDate,DAT_ValidFromDate,DAT_ValidToDate,CHR_Up,CHR_CustomerAssignGroup,CHR_Payment_term) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        elif "VBAP" in f:
            sql = "REPLACE INTO MF_CONTRACT_ITEM (CHR_SalesDocument,CHR_DocumentItem,CHR_Material,CHR_Description,INT_qty,INT_NetValue,CHR_plant,CHR_billing_block,CHR_ReasonRejection,CHR_AccountAssigmentGroup,CHR_Equipment,CHR_LicensePlatNumber) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        elif "VBPA" in f:
            sql = "REPLACE INTO MF_CONTRACT_PARTNER (CHR_SalesDocument,CHR_item,CHR_PartnerFunction,CHR_CustomerId,CHR_ContactPerson,CHR_Address,CHR_CountryKey,CHR_AddressIndicator,CHR_PersonNumber,CHR_Name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        else:
            print "File " + f + " will not being downloaded."
            continue

        # download file
        with open(ftp_local_dir + f.replace(ftp_remote_dir, ""), 'wb') as file:
            print "Downloading " + f + "..."
            try:
                ftp.retrbinary('RETR %s' % f, file.write)
                print "Download completed. Moving files to SUCCESS folder"
                ftp.rename(f, ftp_remote_dir + "/SUCCESS" + f.replace(ftp_remote_dir, ""))
            except Exception as e:
                print "Failed to download " + f
                continue

        data_length = 0
        data_temp = []

        print "Parsing file " + f + "..."

        try:
            tree = ET.parse(ftp_local_dir + f.replace(ftp_remote_dir, ""))
            root = tree.getroot()
        except Exception as e:
            print f + " : Failed to load xml file: " + str(e)
            os.rename(ftp_local_dir + f.replace(ftp_remote_dir, ""), ftp_local_dir + 'failed' + f.replace(ftp_remote_dir, ""))
            continue

        for i in root.iter('DataLength'):
            data_length = i.text

        if data_length == 0:
            print "No DataLength tag found!"
            continue

        for i, s in enumerate(root.iter('string')):
            i += 1
            data = s.text.split('|')

            # balik value kalau DAT_SettlemetDateDeadline < DAT_SettlementDate
            if "FPLT" in f and int(data[4]) < int(data[5]):
                print "Swap value"
                data[4], data[5] = data[5], data[4]

            data_temp.append(data)

            if len(data_temp) == db_row_size or i == int(data_length):
                try:
                    db_cursor.executemany(sql, data_temp)
                    db.commit()
                    print str(i) + ' of ' + str(data_length) + ' [OK]'
                except Exception as e:
                    db.rollback()
                    print str(i) + ' of ' + str(data_length) + ' [FAILED] ' + str(e)
                    # TODO: move to failed dir. perlu?

                data_temp = []

        print "Parsing file " + f + " completed!"
        os.rename(ftp_local_dir + f.replace(ftp_remote_dir, ""), ftp_local_dir + 'success' + f.replace(ftp_remote_dir, ""))

ftp.close()
db.close()
