import pandas as pd
import sqlite3
import sys
import csv
import os

# =============================================================================
# # setting the current working directory
# os.chdir("C:\\Users\\ay552lg\\Documents\\work")
# =============================================================================

def query(_c, _query ):
    _c.execute(_query)
    print(_c.fetchall())
    return;
   
conn = sqlite3.connect("test.sqlite", timeout=10)

conn.execute("DROP TABLE if exists Data")
conn.execute("""CREATE TABLE if not exists Data (FMID INTEGER,
                                                 MarketName TEXT,
                                                 Website TEXT,
                                                 Facebook TEXT,
                                                 Twitter TEXT,
                                                 Youtube TEXT,
                                                 OtherMedia TEXT,
                                                 street TEXT,
                                                 city TEXT,
                                                 County TEXT,
                                                 State TEXT,
                                                 zip TEXT,
                                                 Season1Date TEXT,
                                                 Season1StartDate TEXT,
                                                 Season1EndDate TEXT,
                                                 "1Sunday" TEXT,
                                                 "1Monday" TEXT,
                                                 "1Tuesday" TEXT,
                                                 "1Wednesday" TEXT,
                                                 "1Thursday" TEXT,
                                                 "1Friday" TEXT,
                                                 "1Saturday" TEXT,
                                                 Season2Date TEXT,
                                                 Season2StartDate TEXT,
                                                 Season2EndDate TEXT,
                                                 "2Sunday" TEXT,
                                                 "2Monday" TEXT,
                                                 "2Tuesday" TEXT,
                                                 "2Wednesday" TEXT,
                                                 "2Thursday" TEXT,
                                                 "2Friday" TEXT,
                                                 "2Saturday" TEXT,
                                                 Season3Date TEXT,
                                                 Season3StartDate TEXT,
                                                 Season3EndDate TEXT,
                                                 "3Monday" TEXT,
                                                 "3Tuesday" TEXT,
                                                 "3Wednesday" TEXT,
                                                 "3Thursday" TEXT,
                                                 "3Friday" TEXT,
                                                 "3Sunday" TEXT,
                                                 "3Saturday" TEXT,
                                                 Season4Date TEXT,
                                                 Season4StartDate TEXT,
                                                 Season4EndDate TEXT,
                                                 "4Wednesday" TEXT,
                                                 "4Thursday" TEXT,
                                                 "4Saturday" TEXT,
                                                 x NUMERIC,
                                                 y NUMERIC,
                                                 Location TEXT,
                                                 Credit TEXT,
                                                 WIC TEXT,
                                                 WICcash TEXT,
                                                 SFMNP TEXT,
                                                 SNAP TEXT,
                                                 Organic TEXT,
                                                 Bakedgoods TEXT,
                                                 Cheese TEXT,
                                                 Crafts TEXT,
                                                 Flowers TEXT,
                                                 Eggs TEXT,
                                                 Seafood TEXT,
                                                 Herbs TEXT,
                                                 Vegetables TEXT,
                                                 Honey TEXT,
                                                 Jams TEXT,
                                                 Maple TEXT,
                                                 Meat TEXT,
                                                 Nursery TEXT,
                                                 Nuts TEXT,
                                                 Plants TEXT,
                                                 Poultry TEXT,
                                                 Prepared TEXT,
                                                 Soap TEXT,
                                                 Trees TEXT,
                                                 Wine TEXT,
                                                 Coffee TEXT,
                                                 Beans TEXT,
                                                 Fruits TEXT,
                                                 Grains TEXT,
                                                 Juices TEXT,
                                                 Mushrooms TEXT,
                                                 PetFood TEXT,
                                                 Tofu TEXT,
                                                 WildHarvested TEXT,
                                                 updateTime TIMESTAMP)""")

df = pd.read_csv("final_project_sql-csv-final.csv", encoding='ISO-8859-1')
df.to_sql("Data", conn, if_exists='append', index=False)

c = conn.cursor()

# Check for FMID duplication
c.execute("""SELECT a.FMID 
             FROM Data a
             INNER JOIN (SELECT FMID, COUNT(*) FROM data GROUP BY 1 HAVING COUNT(*) > 1) b
             ON a.FMID = b.FMID""")
if c.fetchall():
    print("ERROR:FMID has duplicated value. Workflow Terminated!")
    conn.close()
    sys.exit()

# 1.Check for Website format error
query(c, """UPDATE Data
            SET Website = null
            WHERE SUBSTR(Website,1,8) != 'https://' 
            AND SUBSTR(Website,1,7) != 'http://'""")

# 2.Check for Facebook format error
query(c, """UPDATE Data
            SET Facebook = null
            WHERE LOWER(Facebook) NOT LIKE '%facebook.com%'
            OR Facebook LIKE '% %'""")

# 3.Check for Twitter format error
query(c, """UPDATE Data
            SET Twitter = CASE WHEN Twitter LIKE '%@%'
                               THEN 'https://twitter.com/' || SUBSTR(Twitter,INSTR(Twitter,'@'))
                               WHEN Twitter LIKE '% %' OR Twitter NOT LIKE '%twitter.com%'
                               THEN null
                               ELSE Twitter
                          END""")

# 4.Check for Youtube format error
query(c, """UPDATE Data
            SET Youtube = null
            WHERE LOWER(Youtube) NOT LIKE '%youtube.com%'
            OR Youtube LIKE '% %'""")

# 5.Check for Zipcode format error
query(c, """UPDATE Data
            SET zip = null
            WHERE zip > '99999' OR zip < '00000'""")

# 5.Check for x,y format error
query(c, """UPDATE Data
            SET x = null,
                y = null
            WHERE x IS null OR y IS null""")

# 6.Check for item columns format error
query(c, """UPDATE Data
            SET Credit = CASE WHEN Credit = 'Y' THEN TRUE ELSE FALSE END,
            WIC = CASE WHEN WIC = 'Y' THEN TRUE ELSE FALSE END,
            WICcash = CASE WHEN WICcash = 'Y' THEN TRUE ELSE FALSE END,
            SFMNP = CASE WHEN SFMNP = 'Y' THEN TRUE ELSE FALSE END,
            SNAP = CASE WHEN SNAP = 'Y' THEN TRUE ELSE FALSE END,
            Organic = CASE WHEN Organic = 'Y' THEN TRUE ELSE FALSE END,
            Bakedgoods = CASE WHEN Bakedgoods = 'Y' THEN TRUE ELSE FALSE END,
            Cheese = CASE WHEN Cheese = 'Y' THEN TRUE ELSE FALSE END,
            Crafts = CASE WHEN Crafts = 'Y' THEN TRUE ELSE FALSE END,
            Flowers = CASE WHEN Flowers = 'Y' THEN TRUE ELSE FALSE END,
            Eggs = CASE WHEN Eggs = 'Y' THEN TRUE ELSE FALSE END,
            Seafood = CASE WHEN Seafood = 'Y' THEN TRUE ELSE FALSE END,
            Herbs = CASE WHEN Herbs = 'Y' THEN TRUE ELSE FALSE END,
            Vegetables = CASE WHEN Vegetables = 'Y' THEN TRUE ELSE FALSE END,
            Honey = CASE WHEN Honey = 'Y' THEN TRUE ELSE FALSE END,
            Jams = CASE WHEN Jams = 'Y' THEN TRUE ELSE FALSE END,
            Maple = CASE WHEN Maple = 'Y' THEN TRUE ELSE FALSE END,
            Meat = CASE WHEN Meat = 'Y' THEN TRUE ELSE FALSE END,
            Nursery = CASE WHEN Nursery = 'Y' THEN TRUE ELSE FALSE END,
            Nuts = CASE WHEN Nuts = 'Y' THEN TRUE ELSE FALSE END,
            Plants = CASE WHEN Plants = 'Y' THEN TRUE ELSE FALSE END,
            Poultry = CASE WHEN Poultry = 'Y' THEN TRUE ELSE FALSE END,
            Prepared = CASE WHEN Prepared = 'Y' THEN TRUE ELSE FALSE END,
            Soap = CASE WHEN Soap = 'Y' THEN TRUE ELSE FALSE END,
            Trees = CASE WHEN Trees = 'Y' THEN TRUE ELSE FALSE END,
            Wine = CASE WHEN Wine = 'Y' THEN TRUE ELSE FALSE END,
            Coffee = CASE WHEN Coffee = 'Y' THEN TRUE ELSE FALSE END,
            Beans = CASE WHEN Beans = 'Y' THEN TRUE ELSE FALSE END,
            Fruits = CASE WHEN Fruits = 'Y' THEN TRUE ELSE FALSE END,
            Grains = CASE WHEN Grains = 'Y' THEN TRUE ELSE FALSE END,
            Juices = CASE WHEN Juices = 'Y' THEN TRUE ELSE FALSE END,
            Mushrooms = CASE WHEN Mushrooms = 'Y' THEN TRUE ELSE FALSE END,
            PetFood = CASE WHEN PetFood = 'Y' THEN TRUE ELSE FALSE END,
            Tofu = CASE WHEN Tofu = 'Y' THEN TRUE ELSE FALSE END,
            WildHarvested = CASE WHEN WildHarvested = 'Y' THEN TRUE ELSE FALSE END""")

# 7.Check for item columns format error
query(c,"""SELECT '0000-' || CASE WHEN 'january' LIKE SUBSTR(LOWER(TRIM(Season1StartDate)),1,INSTR(LOWER(TRIM(Season1StartDate)),' ')-1) || '%'
                                            THEN '01'
                                            WHEN 'february' LIKE SUBSTR(LOWER(TRIM(Season1StartDate)),1,INSTR(LOWER(TRIM(Season1StartDate)),' ')-1) || '%'
                                            THEN '02'
                                            WHEN 'march' LIKE SUBSTR(LOWER(TRIM(Season1StartDate)),1,INSTR(LOWER(TRIM(Season1StartDate)),' ')-1) || '%'
                                            THEN '03'
                                            WHEN 'april' LIKE SUBSTR(LOWER(TRIM(Season1StartDate)),1,INSTR(LOWER(TRIM(Season1StartDate)),' ')-1) || '%'
                                            THEN '04'
                                            WHEN 'may' LIKE SUBSTR(LOWER(TRIM(Season1StartDate)),1,INSTR(LOWER(TRIM(Season1StartDate)),' ')-1) || '%'
                                            THEN '05'
                                            WHEN 'june' LIKE SUBSTR(LOWER(TRIM(Season1StartDate)),1,INSTR(LOWER(TRIM(Season1StartDate)),' ')-1) || '%'
                                            THEN '06'
                                            WHEN 'july' LIKE SUBSTR(LOWER(TRIM(Season1StartDate)),1,INSTR(LOWER(TRIM(Season1StartDate)),' ')-1) || '%'
                                            THEN '07'
                                            WHEN 'august' LIKE SUBSTR(LOWER(TRIM(Season1StartDate)),1,INSTR(LOWER(TRIM(Season1StartDate)),' ')-1) || '%'
                                            THEN '08'
                                            WHEN 'september' LIKE SUBSTR(LOWER(TRIM(Season1StartDate)),1,INSTR(LOWER(TRIM(Season1StartDate)),' ')-1) || '%'
                                            THEN '09'
                                            WHEN 'october' LIKE SUBSTR(LOWER(TRIM(Season1StartDate)),1,INSTR(LOWER(TRIM(Season1StartDate)),' ')-1) || '%'
                                            THEN '10'
                                            WHEN 'november' LIKE SUBSTR(LOWER(TRIM(Season1StartDate)),1,INSTR(LOWER(TRIM(Season1StartDate)),' ')-1) || '%'
                                            THEN '11'
                                            WHEN 'december' LIKE SUBSTR(LOWER(TRIM(Season1StartDate)),1,INSTR(LOWER(TRIM(Season1StartDate)),' ')-1) || '%'
                                            THEN '12'
                                       END  || '-' || SUBSTR(LOWER(TRIM(Season1StartDate)),INSTR(LOWER(TRIM(Season1StartDate)),' '))
      FROM Data WHERE LENGTH(Season1StartDate) < 10 LIMIT 1""")

# 7.Check for season info format error
def seasonfix(_c,_col):
    query(_c,"""UPDATE Data
                SET """ + _col + """ = CASE WHEN LENGTH(""" + _col + """) > 9
                                            THEN """ + _col + """
                                            WHEN INSTR(LOWER(TRIM(""" + _col + """)), ' ') > 0
                                            THEN '0000-' || CASE WHEN 'january' LIKE SUBSTR(LOWER(TRIM(""" + _col + """)),1,INSTR(LOWER(TRIM(""" + _col + """)),' ')-1) || '%'
                                                                 THEN '01'
                                                                 WHEN 'february' LIKE SUBSTR(LOWER(TRIM(""" + _col + """)),1,INSTR(LOWER(TRIM(""" + _col + """)),' ')-1) || '%'
                                                                 THEN '02'
                                                                 WHEN 'march' LIKE SUBSTR(LOWER(TRIM(""" + _col + """)),1,INSTR(LOWER(TRIM(""" + _col + """)),' ')-1) || '%'
                                                                 THEN '03'
                                                                 WHEN 'april' LIKE SUBSTR(LOWER(TRIM(""" + _col + """)),1,INSTR(LOWER(TRIM(""" + _col + """)),' ')-1) || '%'
                                                                 THEN '04'
                                                                 WHEN 'may' LIKE SUBSTR(LOWER(TRIM(""" + _col + """)),1,INSTR(LOWER(TRIM(""" + _col + """)),' ')-1) || '%'
                                                                 THEN '05'
                                                                 WHEN 'june' LIKE SUBSTR(LOWER(TRIM(""" + _col + """)),1,INSTR(LOWER(TRIM(""" + _col + """)),' ')-1) || '%'
                                                                 THEN '06'
                                                                 WHEN 'july' LIKE SUBSTR(LOWER(TRIM(""" + _col + """)),1,INSTR(LOWER(TRIM(""" + _col + """)),' ')-1) || '%'
                                                                 THEN '07'
                                                                 WHEN 'august' LIKE SUBSTR(LOWER(TRIM(""" + _col + """)),1,INSTR(LOWER(TRIM(""" + _col + """)),' ')-1) || '%'
                                                                 THEN '08'
                                                                 WHEN 'september' LIKE SUBSTR(LOWER(TRIM(""" + _col + """)),1,INSTR(LOWER(TRIM(""" + _col + """)),' ')-1) || '%'
                                                                 THEN '09'
                                                                 WHEN 'october' LIKE SUBSTR(LOWER(TRIM(""" + _col + """)),1,INSTR(LOWER(TRIM(""" + _col + """)),' ')-1) || '%'
                                                                 THEN '10'
                                                                 WHEN 'november' LIKE SUBSTR(LOWER(TRIM(""" + _col + """)),1,INSTR(LOWER(TRIM(""" + _col + """)),' ')-1) || '%'
                                                                 THEN '11'
                                                                 WHEN 'december' LIKE SUBSTR(LOWER(TRIM(""" + _col + """)),1,INSTR(LOWER(TRIM(""" + _col + """)),' ')-1) || '%'
                                                                 THEN '12'
                                                            END  || '-' || TRIM(SUBSTR(LOWER(TRIM(""" + _col + """)),INSTR(LOWER(TRIM(""" + _col + """)),' '))) || 'T00:00:00Z'
                                             WHEN """ + _col + """ IS NOT null
                                             THEN '0000-' || CASE WHEN 'january' LIKE LOWER(TRIM(""" + _col + """)) || '%'
                                                                 THEN '01'
                                                                 WHEN 'february' LIKE LOWER(TRIM(""" + _col + """)) || '%'
                                                                 THEN '02'
                                                                 WHEN 'march' LIKE LOWER(TRIM(""" + _col + """)) || '%'
                                                                 THEN '03'
                                                                 WHEN 'april' LIKE LOWER(TRIM(""" + _col + """)) || '%'
                                                                 THEN '04'
                                                                 WHEN 'may' LIKE LOWER(TRIM(""" + _col + """)) || '%'
                                                                 THEN '05'
                                                                 WHEN 'june' LIKE LOWER(TRIM(""" + _col + """)) || '%'
                                                                 THEN '06'
                                                                 WHEN 'july' LIKE LOWER(TRIM(""" + _col + """)) || '%'
                                                                 THEN '07'
                                                                 WHEN 'august' LIKE LOWER(TRIM(""" + _col + """)) || '%'
                                                                 THEN '08'
                                                                 WHEN 'september' LIKE LOWER(TRIM(""" + _col + """)) || '%'
                                                                 THEN '09'
                                                                 WHEN 'october' LIKE LOWER(TRIM(""" + _col + """)) || '%'
                                                                 THEN '10'
                                                                 WHEN 'november' LIKE LOWER(TRIM(""" + _col + """)) || '%'
                                                                 THEN '11'
                                                                 WHEN 'december' LIKE LOWER(TRIM(""" + _col + """)) || '%'
                                                                 THEN '12'
                                                            END || '-00T00:00:00Z'
                                            END""")
    return;

seasons = ['Season1StartDate','Season1EndDate','Season2StartDate','Season2EndDate','Season3StartDate','Season3EndDate','Season4StartDate','Season4EndDate']    
for s in seasons:
    seasonfix(c,s)

dows = ['"1Sunday"','"1Monday"','"1Tuesday"','"1Wednesday"','"1Thursday"','"1Friday"','"1Saturday"','"2Sunday"','"2Monday"','"2Tuesday"','"2Wednesday"','"2Thursday"','"2Friday"','"2Saturday"','"3Monday"','"3Tuesday"','"3Wednesday"','"3Thursday"','"3Friday"','"3Sunday"','"3Saturday"','"4Wednesday"','"4Thursday"','"4Saturday"']
for d in dows:
    query(c, """UPDATE Data
                SET """ + d + """ = UPPER(""" + d + """)""")

# Export)
data = c.execute("SELECT * FROM Data")

with open('final_project_sql.csv', 'wt+', newline='', encoding='ISO-8859-1') as f:
    writer = csv.writer(f)
    writer.writerow(('FMID',
                    'MarketName',
                    'Website',
                    'Facebook',
                    'Twitter',
                    'Youtube',
                    'OtherMedia',
                    'street',
                    'city',
                    'County',
                    'State',
                    'zip',
                    'Season1Date',
                    'Season1StartDate',
                    'Season1EndDate',
                    '1Sunday',
                    '1Monday',
                    '1Tuesday',
                    '1Wednesday',
                    '1Thursday',
                    '1Friday',
                    '1Saturday',
                    'Season2Date',
                    'Season2StartDate',
                    'Season2EndDate',
                    '2Sunday',
                    '2Monday',
                    '2Tuesday',
                    '2Wednesday',
                    '2Thursday',
                    '2Friday',
                    '2Saturday',
                    'Season3Date',
                    'Season3StartDate',
                    'Season3EndDate',
                    '3Monday',
                    '3Tuesday',
                    '3Wednesday',
                    '3Thursday',
                    '3Friday',
                    '3Sunday',
                    '3Saturday',
                    'Season4Date',
                    'Season4StartDate',
                    'Season4EndDate',
                    '4Wednesday',
                    '4Thursday',
                    '4Saturday',
                    'x',
                    'y',
                    'Location',
                    'Credit',
                    'WIC',
                    'WICcash',
                    'SFMNP',
                    'SNAP',
                    'Organic',
                    'Bakedgoods',
                    'Cheese',
                    'Crafts',
                    'Flowers',
                    'Eggs',
                    'Seafood',
                    'Herbs',
                    'Vegetables',
                    'Honey',
                    'Jams',
                    'Maple',
                    'Meat',
                    'Nursery',
                    'Nuts',
                    'Plants',
                    'Poultry',
                    'Prepared',
                    'Soap',
                    'Trees',
                    'Wine',
                    'Coffee',
                    'Beans',
                    'Fruits',
                    'Grains',
                    'Juices',
                    'Mushrooms',
                    'PetFood',
                    'Tofu',
                    'WildHarvested',
                    'updateTime'))
    writer.writerows(data)

c.close()
conn.commit()
conn.close()

