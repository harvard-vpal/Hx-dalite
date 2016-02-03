#!/usr/bin/env python
"""
Joins dalite csv and log files to create an output csv file containing student response info

Feb 1, 2016

@author: A.Ang
"""

import pandas as pd
import json
import os
from collections import defaultdict

def main():
    '''
    assumes 2 folders present in current directory:
         'csv': contains csv file outputs from sql2csv.py
         'logs': contains daily log files
    '''
    csv_directory = 'csv'
    logs_directory = 'logs'
    outFileName = 'output_new.csv'

    # load csv files
    print "loading csv files"
    df_answers = getAnswerTable('{0}/peerinst_answer.csv'.format(csv_directory))
    df_answerchoices = getAnswerChoicesTable('{0}/peerinst_answerchoice.csv'.format(csv_directory))
    df_users = getUsersTable('{0}/auth_user.csv'.format(csv_directory))
    df_questions = getQuestionsTable('{0}/peerinst_question.csv'.format(csv_directory))

    # load logs
    print "loading and processing log files"
    
    df_loginfo = getLogInfoTable(logs_directory)

    # join everything
    print "merging tables"
    df_answerslogs = joinTables(df_loginfo, df_answers, df_questions, df_answerchoices, df_users)

    # reorder columns
    df_answerslogs = (
        df_answerslogs[[
            'user_hash_id','course_id','course_axis_url_name','assignment_id','question_id','question_text',
            'first_answer_choice','first_answer_label','rationale_id','rationale',
            'second_answer_choice','second_answer_label','chosen_rationale_id','chosen_rationale_text',
            'rationales','rationale_algorithm_name','rationale_algorithm_version',
            'problem_show_time','first_check_time','second_check_time',
        ]]
    )

    # formatting (e.g. course_axis_url_name or course_id)
    applyOutputFormatting(df_answerslogs)

    # do some custom filtering (e.g. filter out rows with no username info)
    filterOutputTable(df_answerslogs)

    # write to csv
    df_answerslogs.to_csv(outFileName,index=False)
    print "wrote to {0}".format(outFileName)



def groupLogs(logs):
    '''
    go from flat logs array to dict that has (user, question_id) as key, and array of info records as value
    '''
    print "...grouping logs"
    log_groups = defaultdict(list)

    for log in logs:
        username = log['username']
        question_id = log['event']['question_id']
        course_id = log['context']['course_id']
        
        record = {
            'username': username,
            'question_id': question_id,
            'course_id': course_id,
            'course_axis_url_name': log['context']['module']['usage_key'],
            'event_type': log['event_type'],
            'time': log['time'],
            'problem_part': None,
            'rationales': None,
            'rationale_algorithm_name':None,
            'rationale_algorithm_version':None,
        }
        if log['event_type'] == 'problem_check' and 'first_answer_choice' in log['event']:
            record['problem_part'] = 1
            try: 
                record['course_axis_url_name'] = log['context']['module']['usage_key'].split('@')[-1]
            except AttributeError:
                # if lti module is blank
                pass
        elif log['event_type'] == 'problem_check' and 'second_answer_choice' in log['event']:
            record['problem_part'] = 2
            record['rationales'] = [rationale['id'] for rationale in log['event']['rationales']]
            record['rationale_algorithm_name'] = log['event']['rationale_algorithm']['name']
            record['rationale_algorithm_version'] = log['event']['rationale_algorithm']['version']
    #     record['log'] = json.dumps(log)
    #     record_groups[(log[username],logs[question_id])] = []
        
        log_groups[(username,question_id,course_id)].append(record)

    return log_groups


def filterLogGroups(log_groups):
    '''
    filter for problem_check duplicates and flatten into single dict
    takes as input a dict of log groups, with keys (username,question_id,course_id) and value = list of dict records
    returns a list of dict records with each dict summarizing info from a log group
    '''
    print "...filtering log groups"
    log_groups_filtered = []
    for log_group in log_groups.values():
        group_info = {
            'username':log_group[0]['username'],
            'question_id':log_group[0]['question_id'],
            'course_id':log_group[0]['course_id'],
            'course_axis_url_name':log_group[0]['course_axis_url_name'],
            'rationales':None,
            'problem_show_time':None,
            'first_check_time':None,
            'second_check_time':None,
            'rationale_algorithm_name':None,
            'rationale_algortihm_version':None,
        }
        second_check_found = False
        first_check_found = False
        problem_show_found = False
        
        for i in range(len(log_group),0,-1):
            log = log_group[i-1]
            # second check
            if not second_check_found:
                if log['event_type']=='problem_check' and log['problem_part']==2:
                    group_info['second_check_time'] = log['time']
                    group_info['rationales'] = log['rationales']
                    group_info['rationale_algorithm_name'] = log['rationale_algorithm_name']
                    group_info['rationale_algorithm_version'] = log['rationale_algorithm_version']
                    second_check_found = True    
            # first check
            if not first_check_found:
                if log['event_type']=='problem_check' and log['problem_part']==1:
                    group_info['first_check_time'] = log['time']
                    first_check_found = True          
            # problem_show
            elif not problem_show_found:
                if log['event_type']=='problem_show':
                    group_info['problem_show_time'] = log['time']
                    
        log_groups_filtered.append(group_info)
    return log_groups_filtered


def loadLogsFromFiles(directory):
    '''
    load log files into one list
    '''
    print "...loading log files"
    # load all log files into list
    logs = []
    for filename in os.listdir(directory):
        if not filename.startswith('student.log'):
            continue
        with open("{0}/{1}".format(directory,filename)) as f:
            for log in f:
                logs.append(json.loads(log))
    return logs


def getLogInfoTable(directory):
    '''
    load logs from file, do some filtering/summarization, and return a pandas df
    '''
    # load logs from file
    logs = loadLogsFromFiles(directory)

    # do some log filtering before moving into pandas df
    log_groups = groupLogs(logs)
    log_groups_filtered = filterLogGroups(log_groups)

    # load list of log info dicts into pandas
    df_loginfo = pd.DataFrame(log_groups_filtered)

    # convert time columns to timestamp datatype
    for col in ['problem_show_time','first_check_time','second_check_time']:
        df_loginfo[col] = pd.to_datetime(df_loginfo[col])

    # rename column (so it matches the corresponding col in answers table)
    df_loginfo = df_loginfo.rename(columns={'username':'user_token'})

    return df_loginfo


def getAnswerTable(filename):
    '''
    load answers csv into pandas dataframe and do some formatting
    '''
    # create df from answer table
    df_answers = pd.read_csv(filename)
    # just keep the necessary columns
    #TODO if other columns (e.g. upvotes, expert) get used, add them back in here
    df_answers = (
        df_answers
            # rename id column
            .rename(columns={'id':'rationale_id'})
            # subset columns
            [['rationale_id','first_answer_choice','rationale','second_answer_choice',
              'user_token','chosen_rationale_id','question_id','assignment_id'
            ]]
        )
    # explicitly fill in chosen_rationale_id for those who stuck with their own rationales
    null_idx = df_answers[df_answers.chosen_rationale_id.isnull()].index
    df_answers.ix[null_idx,'chosen_rationale_id'] = df_answers.ix[null_idx,'rationale_id']

    return df_answers


def getAnswerChoicesTable(filename):
    '''
    load answer choices table into pandas df
    '''
    df_answerchoices = pd.read_csv(filename)

    # add numeric code for answer choice {1,2} or {1,2,3}
    def add_choiceint(x):
        x['answer_choice'] = range(1,len(x)+1)
        return x

    df_answerchoices = (
        df_answerchoices
            [['text','question_id']]
            .groupby('question_id')
            .apply(add_choiceint)
        )

    return df_answerchoices


def getUsersTable(filename):
    '''
    load users csv file into pandas df
    '''

    df_users = pd.read_csv(filename)

    df_users = (
        df_users
            # rename lti internal username col for consistency
            .rename(columns={'username':'user_token'})
            # strip @localhost from email to get the edx hash_id
            .assign(user_hash_id = lambda x: x.email.apply(lambda x: x.replace('@localhost','')))
            # subset columns
            [['user_token','user_hash_id']]
        )
    

    return df_users


def getQuestionsTable(filename):
    '''load questions csv file into pandas df'''
    df_questions = pd.read_csv(filename)
    df_questions = (df_questions
                        # formatting
                        .assign(question_text=lambda x: x.text.apply(lambda x: x.replace('<b>','').replace('</b>','')))
                        # rename column
                        .rename(columns={'id':'question_id'})
                    )
    return df_questions


def getEdxUserTable(filename):
    '''
    get edx mapping table for hash_id to username
    '''
    ## something like
    # df_user_id_map = pd.read_csv('csv/user_id_map.csv')
    # df_user_id_map = df_user_id_map[['hash_id','username']]
    pass

    
def joinTables(df_loginfo, df_answers, df_questions, df_answerchoices, df_users):
    '''
    join all the df tables
    '''
    return (
        df_answers
            # join in info from logs (event times, rationales)
            .merge(
                df_loginfo,
                how='left',
                on=['user_token','question_id'],
            )
            # join in question text
            .merge(
                df_questions[['question_id','question_text']],
                how='left',
                on='question_id',
            )
            # join in label for first answer
            .merge(
                df_answerchoices.rename(columns={'answer_choice':'first_answer_choice','text':'first_answer_label'}),
                how='left',
                on=['question_id','first_answer_choice']
            )
            # join in label for second answer
            .merge(
                df_answerchoices.rename(columns={'answer_choice':'second_answer_choice','text':'second_answer_label'}),
                how='left',
                on=['question_id','second_answer_choice']
            )
            # self join to get chosen_rationale text
            .merge(
                df_answers[['rationale_id','rationale']].rename(columns={'rationale_id':'chosen_rationale_id','rationale':'chosen_rationale_text'}),
                how='left',
                on='chosen_rationale_id',
            )
            # join in edX username
            .merge(
                df_users.rename(columns={'username':'user_token'}),
                how='left',
                on='user_token',
            )

            ## could also incorporate edX username by joining on hash_id 

    )


def formatCourseAxisUrlName(s):
    ''' 
    format module id / course axis url name
    assumes something like "block-v1:HarvardX+TST-DALITE-NG-1+now+type@lti+block@609e562e13c74b85912f30fafeeca774" and takes the last part after the '@'
    '''
    if s: return s.split('@')[-1]
    

def formatCourseId(s):
    '''
    format course id, assumes something like "course-v1:HarvardX+ER22.1x+2015T3"
    '''
    if s: return s.replace('course-v1:','').replace('+','/')


def applyOutputFormatting(df_answerslogs):
    '''
    apply some formatting rules to relevant columns of the output dataframe before writing to csv
    '''
    idx = df_answerslogs.course_axis_url_name.notnull()
    df_answerslogs.ix[idx,'course_axis_url_name'] = df_answerslogs.ix[idx,'course_axis_url_name'].apply(formatCourseAxisUrlName)

    idx = df_answerslogs.course_id.notnull()
    df_answerslogs.ix[idx,'course_id'] = df_answerslogs.ix[idx,'course_axis_url_name'].apply(formatCourseId)


def filterOutputTable(df_answerslogs):
    '''
    define custom filtering
    '''

    # custom filtering and write to csv
    return (
        df_answerslogs
            # drop rows with no user info
            .dropna(subset=['user_hash_id'])
            # question_id<9 looks like it was used for platform testing
            .query('question_id>9')
     )


if __name__ == '__main__':
    main()

