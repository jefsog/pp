import cx_Oracle

class Database(object):
    
    def __init__(self):
        raise Exception("Not Instantiable!")

    



    def drop_table(self, table_name):
        query = 'drop table ' + table_name
        try: 

            self.cursor.execute(query)

        except cx_Oracle.DatabaseError as e:        
            print query
            print e
            

    def create_table(self, table_name, lst_column, lst_data_type):     
        
        query1 = 'create table ' + table_name + ' ('
        #print table_name
        query2 = ''
        count = -1

        if len(lst_column) == len(lst_data_type) :
            count = len(lst_column)
        else:
            print table_name + ' has ' + str(len(lst_column)) + 'columns'
            print ', but some records have ' + str(len(lst_column_length)) + ' fields'
            print ', delimiting was not sucessful.'
        for i in range(0, count):
            if len(lst_column[i]) == 0:
                lst_column[i] = 'field' + str(i)
            query2 = query2 + lst_column[i] + ' ' + lst_data_type[i] 
            if i < count-1:
                query2 = query2 + ','
        query3 = ')'    
        query = query1+query2+query3
        
        
        try:
            
            self.cursor.execute(query)
            
        except cx_Oracle.DatabaseError as e:        
            print table_name
            print e
            print query
            raise cx_Oracle.DatabaseError(table_name+"|"+query)
            

    def insert_one_row(self, table_name, lst_field):
        query1 = 'insert into ' + table_name + ' values ('
        query2 = ''
        lst_length = len(lst_field)
        for i in range(0, lst_length):
            query2 = query2 + "'" + lst_field[i] + "'"
            if i < lst_length-1:
                query2 = query2 + ','
        query3 = ')'
        query = query1 + query2 + query3
        #print query
        
        self.cursor.execute(query)
        self.cursor.execute('commit')
        


    def insert_rows(self, table_name, lst_lst_field, int_start_row, int_len_column):
        query1 = 'insert into ' + table_name + ' values ('
        
        query3 = ')'
        try:
            
            count = 0
            for i in range(int_start_row, len(lst_lst_field)):
                row = lst_lst_field[i]
                if len(row) != 0:  # empty record
                    
                    query2 = ''
                    if len(row)<int_len_column: # row could have less fields than what columns indicate
                        for i in range(int_len_column-len(row)):
                            row.append('')
                    for j in range(len(row)):
                        query2 = query2 + self.prepare_field(row[j]) 
                        if j < len(row)-1:
                            query2 = query2 + ','
                    query = query1 + query2 + query3                
                    self.cursor.execute(query)
                    count = count + 1
                    if count%10000 == 0:
                        self.cursor.execute('commit')
                        print count
            self.cursor.execute('commit')
            
            print 'row_inserted: ' + str(count)
            return count
        except cx_Oracle.DatabaseError as e:
            raise cx_Oracle.DatabaseError(table_name+"|"+query)


    def prepare_field(self, str_string):
        string_returned = ''
        if len(str_string)<=4000:
            if str_string.find("'") == -1:
                string_returned = "'" + str_string + "'"
            else:
                string_returned = "'" + self.escape_single_quotation(str_string) + "'"
        else:
            if str_string.find("'") == -1:
                string_returned = self.prepare_clob_field(str_string)
            else:
                string_returned = self.prepare_clob_field(self.escape_single_quotation(str_string))
        return string_returned

    # if a field contains a single quotation mark, escapte it with another single quotation mark
    def escape_single_quotation(self, str_string):
        str_string = str_string.replace("'", "''") 
        return str_string

    #string with length over 4000 can not be transmitted into Oracle 
    #so it is sliced first, then wrap with to_clob, finally concatnated 
    def prepare_clob_field(self, str_field):
        int_step = 4000
        i = 0
        str_code = ''
        # it is better to delimit in the space, rather than in the middle of a word
        while i < len(str_field):
            
            j = i+int_step
            if j < len(str_field):
                while j>=i:
                    
                    if str_field[j-1] == ' ':
                        
                        str_code = str_code  + "to_clob('"+str_field[i:j]+"')"
                        i = j

                        break
                    else:
                        j += -1
            else:
                str_code = str_code  + "to_clob('"+str_field[i:j]+"')"
                break
                

            #i = i + int_step
            if i<len(str_field):
                str_code = str_code + '||'
        return str_code
    
    def close(self):
        self.cursor.close()
        self.cnx.close()

class US_database(Database):
    
    def __init__(self):        
        self.cnx = cx_Oracle.connect('eris_us_load/eris@GMPROD')
        self.cursor = self.cnx.cursor()
        
class Test_database(Database):
    def __init__(self):
        self.cnx = cx_Oracle.connect('eris/eris@gmtest')
        self.cursor = self.cnx.cursor()
        
class CA_database(Database):
    
    def __init__(self):        
        self.cnx = cx_Oracle.connect('eris_ca_load/eris@GMPROD')
        self.cursor = self.cnx.cursor()        
        
if __name__ == '__main__':
    db = Test_database()
    #db.drop_table('JEFF1_FORMERLY_USED_DEFENSE_S')
    
    str_field = '"Prior to Sept, 2004 data translation this spill Lead_DEC Field was  O\'CONNELL     Tank level loss of 600gal.    2/1/01 Met with Leon Paretsky and Joe Floryshak to examine vaults installed around feeder leak.  Vault has concrete sides, sand/soil floor- also open to soil where feeders enter and exit vault.  Requested 4 sample locations for benzene and dielectric fluid.  Middle feeder leaked.  Will sample 2 from floor and one from each end.  Floor samples will be hand augured through sand into native soil. (JHO)    3/6/01 Leon Paretsky forwarded an updated copy of e2mis report #129975 with summary of data.    3/7/01 Spoke to Leon Paretsky.  TPH levels are very high in 3 places (25300, 12100, 26300ppm).  He will arrange contractor with vac truck to remove more soil. (JHO)    3/25/03 Sent email to Leon Paretsky requesting information on status. (KMF)    3/26/03 Spoke to L.Paretsky.  He claims that Con Ed needed to get HASP from vendor and ran into trouble.  Paretsky admitted to dropping the ball. He states that he will pick up where is left off.  I did not propose a timeframe. (KMF)    6/2/05: e-mail to Mike Pillig, Con Ed S&TO:   We discussed this spill briefly during our meeting last month.  The last information we have in the file is that on 3/7/01, Leon agreed to bring in a contractor to remove additional contaminated soil from the field constructed manhole.  On 3/26/03, Kerry Foley followed up with Leon, who promised to complete the requested excavation.  As of today, there has been no additional excavation.     Please arrange for a contractor to visit the site and complete the required soil removal within the next month. Please note that the previous soil samples which exhibited high levels of TPH were up to 2 feet into the soil, so at least 2 additional feet of soil needs to be removed from the floor, and at least 6 inches must be removed from the north side of the excavation.       Post excavation samples should be collected and submitted for analysis.  Forward results to Sam Arakhan for review.  (JHO)    Update 11/22/05  Spill closed based on report in eDocs link. SKA    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  e2mis no. 129975:    14-Feb-2000 @ 09:40 hrs. - Fdr. 38B11 S/S Pumphouse #1 tank level loss of 600 gallons. Leak rate of 4 gallons per hour. Feeder is currently out of service for unrelated work. Feeder runs between the Bensonhurst S/S and the Greenwood S/S. Leak committee conveined and Underground Transmission and Gas Corrosion inspecting manholes and subsurface along feeder run. Substation Operations inspecting pumphouses and all associated equipment.     DEC# 99-12939. 2 TO crews sent out to check 9 manholes. TO crew sent to perform drop checks on 3  lines. PFT drum being mixed. At 14:30 all manholes were checked - no leak found. Greenwood 3  lines drop checked - no loss. Bensonhurst 3  line drop checked - no loss. Drop check performed on feeder 30 psi drop in 1 hour. Feeder returned to reduced pressure. On 3-11 2/14 & 11-7 2/15 TO crews performed subsurface inspections between the 2 substations - nothing found. Leak rate at reduced pressure (80 psi) is approx 2 gph. At normal pressure (275 psi) leak rate is approx 8 gph.     On 7-3 shift 2/15 drop checks performed on 38B11T leg at Bensonhurst. PFT truck being set up at MH 66209. At 14:00 drop checks completed on 38B11T in Bensonhurst show that leak is on B phase. 70 psi drop on feeder in under 5 minutes. Feeder put back on reduced pressure. PFT equipment dismantled at MH 66209 and brought to B phase riser at Transformer #1. PFT injection to be done at bypass piping between 38B11 and 38B11T in Transformer vault #1. At 16:15 feeder  brought back to normal pressure. At 17:00 circulation valving was completed and PFT injection was started on 38B11T B phase only. At 18:00 circulation was stopped and PFT was stopped. Feeder was put on reduced pressure. On 11-7 shift 2/16 3/4  barholes were drilled every 5\' through building.     PFT van sent out on 2/16 to take barhole sampl\'es along pipe run. No PFT found in barholes. On 2/17 more samples taken - no PFT found. Samples taken from far end of feeder - no PFT found. Circulation was probably not complete when it was shut down of 2/15. On 2/17 PFT was injected into feeder until 400 gallons of fluid had passed (entire B phase).     05:30 2/18 high PFT readings found in barholes in Bensonhurst #2 S/S. Chem lab pinpointed highest reading. Contractor mobilized and set up wooden dam sealed to the floor. Wet saw used to cut through concrete floor and hand excavation used to go into soil. Wet soil found in excavation. Allstate called to supply vac truck and Cusco. Chem lab called for samples. Soil being removed by Cusco.    At 03:45 2/19 temp clamp installed. Leak is on transition weld between the copper and stainless steel. Temp clamp not holding completely. Bucket under leak containing fluid. Safeway continues to cleanup and remove soil for repair. Chem lab reports 00-01542 <1.00ppm PCBs. B phase valving closed to stop leak. Leak exposed - no water can enter pipe. Excavation continues to make room for repairs. On 2/19 Allstate removed 1500 gallons of oil/water from trench. Weld repairs attempted on 2/19 - because of dielectric fluid in pipe cannot heat up copper enough to get weld metal to penetrate. Crack opens back up as soon as pipe cools down. USI representative on location on 2/19 - permanent type of repair clamp to be fabricated.     Excavation to be enlarged and field poured manhole to be built around couplings on all 3 phases. On 2/23 USI met with S&TE to discuss preliminary design of clamp. On 2/24 USI was given  permission to fabricate clamps for all 3 couplings.     On 3/10/00 USi demonstrated a prototype clamp in their Milford Ct. lab. The clamp held 500 psi for 1 hour and 250 psi for 72 hours. S&TE requested minor changes and USI agreed. On 3/17/00 USI stated that 3 clamps had been fabricated and would be shipped for delivery to Trans Ops on Monday 3/20/00. Clamps installed on all three couplings in excavation between 3/23. Reduced pressure put back on feeder in steps. Potheads were bled on 3/24. Feeder returned to normal pressure. Pads placed under clamp. On 3/25 clamps inspected. 1/2  diameter spot of fluid on pad  under clamp. Feeder energized 13:43 on 3/26. Clamp wiped down and pads replaced. On 3/27 clamp inspected again. No more leakage found. Contractor began fabricating box for manhole. Chem lab reported 00-01544 .17ppm Benzene and <0.0046ppm benzene.     Clean Harbors notified to respond and remove dumpster. On 4/05/00 Clean Harbors removed 4 cubic yards of spill material from dumpster under CTF 0900914 and 15 cubic yards of material under CTF0900915 in a dumpster.     On 2/13/01 Chem lab reported to location and took samples of soil in box as per Leon Partesky. Chem lab reported 01-01551 TCLP benzene at 4 locations (all less than 0.0046ppm) and TRPH at 4 locations (highest was 2310ppm). Chem lab reported 01-01551 TPH 8100 at 4 locations.     On Thurs 02/01/01 Leon Paretsky (TO), Joe Floryshak and Rich Perusse (EH&S-Remediation) met with Jane O\'Connell (NYSDEC) to inspect the manhole that was constructed around the leak location. The amount of dielectric fluid released, waste management of the contaminated soil, and sampling was discussed.   (a) Dielectric fluid released: 1600 gal of dielectric fluid was released.    (b) Waste Management  The follow table summarizes the disposal of the waste from the site.  Document No. Date Vendor Waste Quantity  APV00607 02/19/00 All-State oil/water mixture 1500 gal  CTF0900914 04/05/00 Clean Harbors oily debris 4 cyds  CTF0900915 04/05/00 Clean Harbors oily debris 15 cyds    (c) Sampling: The manhole appeared to have a sandy floor, and there were openings around the pipe penetrations through the manhole walls. It was agreed to take 4 core soil samples in the excavation: two in the floor and two at the side walls, as follows: The samples were obtained on 02/13/01 by a representative from Con Edison\'s Chem Lab, accompanied by Rich Perusse (EH&S Remediation). Samples were to be analyzed for TCLP benzene and for dielectric fluid via  EPA method 8100.     Analyses were conducted by ETL, Inc. The results are tabulated below.     Location             Dielectric fluid         Petroleum TRPH                           By 8100* (ppm)             (ppm)    1X- bottom north           25300                   25300   about ,2 ft into soil  2X- bottom south           12100                   12100   about 2 ft into soil  3X- south side               464                     464   about 1 ft into soil  4X- north side             26300                   26300   about 6 inches into soil  Note: * identified as Sun 6 dielectric fluid.    "'
    #str_field = 'hi wor sep me '
    print db.prepare_field(str_field)
    db.close
    
