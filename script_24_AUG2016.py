# -*- coding: utf-8 -*-
"""
Created on Tue Jun 14 13:29:53 2016

@author: ssitaramachandran
"""
import os
import fnmatch
import errno
import os.path
import subprocess
import shutil
import datetime
import time
import cx_Oracle
import json
from os import listdir
from os.path import isfile, join
import re
import csv

def find_between( s, first, last ):
    try:
        start = s.index( first ) + len( first ) +1
        end = s.index(last)
        return s[start:end]
    except ValueError:
        return ""

def main():
"""
Interesting part for JEFF
Reads file from directory
"""
    try:        
        mybasepath = os.path.dirname(__file__)        
        input_path = (os.path.join(mybasepath,'Input'))
        input_archive_path = (os.path.join(mybasepath,'Input_Archive'))
        output_path = (os.path.join(mybasepath,'Output'))
        output_archive_path = (os.path.join(mybasepath,'Output_Archive'))
        log_path= (os.path.join(mybasepath,'Log'))
        daily_log_path = (os.path.join(log_path,'Log_'+ datetime.date.today().strftime("%d-%m-%Y")))
        resonpse_file = os.path.join(output_path,'AWS_response.txt')
        final_file = os.path.join(output_path,'Merged_AWS_response.txt')
        Log_success = os.path.join(daily_log_path,'Log_success.txt')
        Log_failed = os.path.join(daily_log_path,'Log_failed.txt')
        try:
            os.mkdir(daily_log_path)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise exc
            pass
        daily_input_archive_path = (os.path.join(input_archive_path,'IParchive_'+ datetime.date.today().strftime("%d-%m-%Y")))
        try:
            os.mkdir(daily_input_archive_path)
        except OSError as exc:
            if exc.errno != errno.EEXIST:                
                with open(Log_failed,'a') as io_log:
                    io_log.write('TIMESTAMP:'+datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")+'\n')
                    io_log.write('ERROR: Problem in daily input archive.\n'+str(exc))
                    io_log.write('-----------------------------------------------------------------------------------------------------------------\n')
                raise exc
            pass
        daily_output_archive_path = (os.path.join(output_archive_path,'OPArchive_'+ datetime.date.today().strftime("%d-%m-%Y")))
        try:
            os.mkdir(daily_output_archive_path)
        except OSError as exc:
            if exc.errno != errno.EEXIST:                
                with open(Log_failed,'a') as io_log:
                    io_log.write('TIMESTAMP:'+datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")+'\n')
                    io_log.write('ERROR: Problem in daily input archive.\n'+str(exc))
                    io_log.write('-----------------------------------------------------------------------------------------------------------------\n')
                raise exc
            pass        
        #Get all files in the input directory in an array
        inputfiles =[]
        inputfiles = [f for f in listdir(input_path) if fnmatch.fnmatch(f, '*.json') if isfile(join(input_path, f))]
        print inputfiles
        print len(inputfiles)
        #remove response_file if exits 
        if(os.path.exists(resonpse_file)):
            os.remove(resonpse_file)        
        #remove final_file if exits     
        if(os.path.exists(final_file)):
            os.remove(final_file)    
        # To quit the loop if there is an error and prevent unnecessary calls to AWS.
        _MAX_FILE_COUNTER = 0
"""
Interesting part for JEFF
"""		
"""
Interesting part for JEFF Processing the files
"""				
            for i in range(len(inputfiles)):                 
                print inputfiles[i]                
                final_input_path = os.path.join(input_path,inputfiles[i]) #loop each file
                
                
"""
Interesting part for JEFF  Oracle loading part starts
"""				                    
            con = cx_Oracle.connect('eris/eris@gmtest')
            cursor = con.cursor() 
            insert = """
             INSERT INTO EDIRECT_AWS_Response (file_name,status,error_type,eris_id_failed,error_msg,error_file_linenumber,adds,deletes,upload_date,upload_timestamp,orginal_msg)
               VALUES (:1, :2, :3, :4, :5, :6, :7, :8,to_date(:9,'DD.MM.YYYY'), :10, :11)"""
            # Initialize list that will serve as a container for bind values
            response_list = []
            try:
                with open(final_file)  as textfile:
                    oracle_table = csv.reader(textfile, delimiter='|')       
                    for row in oracle_table:
                        response_list.append(tuple(row))
            except IOError:
                with open(Log_failed,'a') as io_log:
                    io_log.write('ERROR_TIMESTAMP:'+datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")+'\n')
                    io_log.write('ERROR: Final_File not avaliable error.\nError_Msg: Final Output file does not exits. Check log of AWS Response error or for No Input error.\n')        
                    io_log.write('-----------------------------------------------------------------------------------------------------------------\n')
            # prepare insert statement
            cursor.prepare(insert)
            # execute insert with executemany
            cursor.executemany(None, response_list)
            # report number of inserted rows
            if(cursor.rowcount > 0):
                with open(Log_success,'a') as log:
                        log.write('TIMESTAMP:'+datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")+'\n')
                        log.write('Sucess: Values inserted into oracle table.\nMsg Inserted: ' + str(cursor.rowcount) + ' rows.\n')
                        log.write('-----------------------------------------------------------------------------------------------------------------\n')
                print 'Inserted: ' + str(cursor.rowcount) + ' rows.'
            else:
                with open(Log_failed,'a') as io_log:
                        io_log.write('TIMESTAMP:'+datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")+'\n')
                        io_log.write('ERROR: NO Values inserted into oracle table.\nMsg Inserted: ' + str(cursor.rowcount) + ' rows.\n')
                        io_log.write('-----------------------------------------------------------------------------------------------------------------\n')
                print 'Inserted: ' + str(cursor.rowcount) + ' rows.'        
            # commit
            con.commit()
            #close cursor and connection
            cursor.close()
            con.close()
"""
Interesting part for JEFF  Oracle part ends
"""				          			
        else:
            with open(Log_failed,'a') as io_log:
                    io_log.write('TIMESTAMP:'+datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")+'\n')
                    io_log.write('ERROR: No Input found.\nMsg: No input file. Check oracle table if it has values.\n')
                    io_log.write('-----------------------------------------------------------------------------------------------------------------\n')
"""
Interesting part for JEFF  On success moves the files to archive starts  and logs it
"""				 					
        input_allfiles = [f for f in listdir(input_path) if isfile(join(input_path, f))]
        for i in range(len(input_allfiles)):
            input_src_file = os.path.join(input_path,input_allfiles[i]) #loop each file
            input_dst_file = os.path.join(daily_input_archive_path,input_allfiles[i]) #loop each file    
            try:
                shutil.move(input_src_file,input_dst_file)
            except Exception as Inputmove_ERR:        
                pass
        #Get all files in the output directory in an array
        outputfiles =[]
        outputfiles = [f for f in listdir(output_path) if isfile(join(output_path, f))]    
        for j in range(len(outputfiles)):    
            output_src_file = os.path.join(output_path,outputfiles[j]) #loop each file
            output_dst_file = os.path.join(daily_output_archive_path,outputfiles[j]) #loop each file    
            try:
                shutil.move(output_src_file,output_dst_file)
            except Exception as Inputmove_ERR:        
                pass
        with open(Log_success,'a') as log:
            log.write('TIMESTAMP:'+datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")+'\n')
            log.write('Sucess: JSON Input files moved to archive.\nMsg : No of JSON files moved to archive ' + str(len(inputfiles)) + ' .\n')
            log.write('Sucess: TOTAL Input files moved to archive.\nMsg : TOTAL_FILES files moved to archive ' + str(len(input_allfiles)) + ' .\n')
            log.write('-----------------------------------------------------------------------------------------------------------------\n')
    except Exception  as e:
        with open(Log_success,'a') as log:
            log.write('TIMESTAMP:'+datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")+'\n')
            log.write('-----------------------------------------------------------------------------------------------------------------\n')
            log.write('-----------------------------------------------------------------------------------------------------------------\n')
            log.write('Unexpected failure check' +str(e)+ ' .\n')            
            log.write('-----------------------------------------------------------------------------------------------------------------\n')
            log.write('-----------------------------------------------------------------------------------------------------------------\n')
"""
Interesting part for JEFF  On success moves the files to archive   and logs it
"""		        
if __name__ == "__main__":
    main()        
    
