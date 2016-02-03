#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Converts a dalite SQL dump to CSV format
Creates a separate CSV output file for each non-empty table

Jan 22, 2016

@author: A.Ang
"""

import csv
import sys
import string

def main():
    '''
    assumes folder 'csv' for writing output csv files to
    '''

    fileNameIn = 'sql/database.sql-20160114'
    # fileNameIn = sys.argv[1]
    dirOut = 'csv'


    newTable = False 
    with open(fileNameIn, 'r') as fileIn:
        for line in fileIn:
            # detect table start
            if line.startswith('CREATE TABLE'):
                tableName = line.split(' ')[2].strip("`")
                colNames = []
                newTable = True # indicate creation of new table on next insert statement

            # get table column names
            elif newTable and line.startswith("  `"):
                fn = line.split()[0].strip("`")
                colNames.append(fn)

            # get table data from insert statements
            elif line.startswith('INSERT INTO') :
                if newTable:
                    # create output csv"
                    print "Detected table {0}".format(tableName)
                    fileNameOut = "{0}/{1}.csv".format(dirOut,tableName)
                    csvfile = open(fileNameOut,'w')
                    writer = csv.writer(csvfile)
                    writer.writerow(colNames)
                    n = len(colNames)
                    newTable = False
                    
                records = separateAndParseRecords(line)
                for record in records:
                    # check parsed record length against number of column names
                    if len(record)!= n:
                        print "Length mismatch: {0}".format(', '.join(record)[:500])
                    else:
                        writer.writerow(record)


def cleanText(text):
    '''
    Replace NULL value with blank
    Replace escaped quote characters with unescaped version
    Replace newline (\r\n) with a space

    '''
    if text == 'NULL':
        return ''
    text.replace(text,"\'")
    charMap = {r"\'":"'",r'\"':'"',r'\r\n':' ',"’":"'"}
    for oldchar, newchar in charMap.items():
        text = string.replace(text,oldchar,newchar)
    return text


def separateAndParseRecords(line):
    '''
    Separates parentheses-enclosed records in an insert statement
    Returns list of lists, where each inner list is a record
    '''
    records = []
    recordItem = ''
    insideQuotes = False
    insideParen = False
    prevchar = None
    prevprevchar = None
    MAX_CHARS = 32000

    for char in line:
        if char=="(" and not insideParen:
            insideParen = True
            record = []
            recordItem = ''
            
        elif char==")" and insideParen and not insideQuotes:
            insideParen = False
            recordItem = cleanText(recordItem)
            record.append(recordItem[:MAX_CHARS])
            records.append(record)

        elif char == "'" and (prevchar != '\\' or (prevchar=='\\' and prevprevchar=='\\')):
            insideQuotes = not insideQuotes

        elif char == "," and insideParen and not insideQuotes:
            recordItem = cleanText(recordItem)
            record.append(recordItem[:MAX_CHARS])
            recordItem = ''

        else:
            recordItem += char

        prevprevchar = prevchar
        prevchar = char

    return records

if __name__ == '__main__':
    main()

