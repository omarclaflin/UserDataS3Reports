# Omar's Python Tools for querying S3 and pulling results to local system


def listAllBuckets(s3):
    import boto3
    for bucket in s3.buckets.all():
        print(bucket.name)

def listSubFolders(bucket, parent):
#listSubFolders('aws-athena-query-results-XXXXXXXXXXXXXXXXXX','Unsaved/2018/01/')
    import boto3
    client = boto3.client('s3')
    result = client.list_objects(Bucket = bucket, 
                                 Prefix=parent, Delimiter='/')
    for o in result.get('CommonPrefixes'):
        print('sub folder : ', o.get('Prefix'))

def listContents(bucket, parent):
#    parent = "folderone/foldertwo/"
    print('Looking for file. ')
    import boto3
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(name=bucket)
    FilesNotFound = True
    for obj in bucket.objects.filter(Prefix=parent):
         print('{0}:{1}'.format(bucket.name, obj.key))
         FilesNotFound = False
    if FilesNotFound:
         print("ALERT", "No file in {0}/{1}".format(bucket, prefix))
        
def readContents(bucket,parent,key):
    import boto3
    s3 = boto3.resource('s3')
    s3bucket = s3.Bucket(name=bucket)
    actualkey = parent+'/'+key
    path =key
#    print(actualkey)
    FilesNotFound = True
    for obj in s3bucket.objects.filter(Prefix=parent):
#        print(obj.key)
        if obj.key == actualkey:
            print("Found it! Good job on getting all those numbers and letter in the right order.")
            FilesNotFound=False
            
            print("tryna download " +actualkey+ " to "+path +"\n")
            if not AWSLambda:
                s3.Bucket(bucket).download_file(actualkey, path)
            else:
                s3.Bucket(bucket).download_file(actualkey, '/tmp/'+path)
            print("download complete")
            
    if FilesNotFound:
         print("ALERT", "Searched: {0} and found NO file named {1}/{2}".format(bucket, parent, key))


def run_query(query,database,s3_output):
    import boto3
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
            }
        ,
        ResultConfiguration={
            'OutputLocation': s3_output,
            }
        )
    print('Execution ID: ' + response['QueryExecutionId'])
    return response

def checkQueries(is_queries_running,queryStore,oldresponses,repeatTime):
    import boto3
    while sum(is_queries_running) != 0: #len(is_queries_running):
        for qq in range(len(is_queries_running)):
            if is_queries_running[qq]:
                response = client.batch_get_query_execution(
                    QueryExecutionIds=[queryStore[query_num]])
                #store full response, and just completion status
                responses[qq] = response 
                is_queries_running[qq] = response['QueryExecutions'][0]['Status']['State']!='SUCCEEDED'
                if response['QueryExecutions'][0]['Status']['State']=='FAILED':
                    print('Failed because ' +response['QueryExecutions'][0]['Status']['StateChangeReason'])
        #pause for some arbitrary amount of time
    #    print('%i queries completed out of %i /n' %query_params.sum() %len(query_params))
        #if repeat set to 0, then break out of this whole darn thing
        import time
        if repeatTime == 0:
            break
        else:
            time.sleep(repeatTime)
        print([str(sum([not i for i in is_queries_running])) +' queries completed out of ' + str(len(is_queries_running)) + ' queries'])
        return responses    

def fetchGameNames(override):
    import os.path
    if AWSLambda:
        if not os.path.isfile('/tmp/Games.dat'):
            print('Games.dat doesnt exist -- fetching it now. \n')
            getGameTable()
        else:#if it exists
            if not override:
                import datetime
                print('Games.dat already exists, last updated at ' 
                      + str(datetime.datetime.fromtimestamp(os.path.getmtime('/tmp/Games.dat'))) +
                      '. Skipping  Games fetch step. Change DOWNLOAD_OVERIDE to True '
                      'to override this and force an update. \n')
            else:
                import os
                os.remove('/tmp/Games.dat')
                getGameTable()
    else:
        if not os.path.isfile('Games.dat'):
            print('Games.dat doesnt exist -- fetching it now. \n')
            getGameTable()
        else:#if it exists
            if not override:
                import datetime
                print('Games.dat already exists, last updated at ' 
                      + str(datetime.datetime.fromtimestamp(os.path.getmtime('Games.dat'))) +
                      '. Skipping  Games fetch step. Change DOWNLOAD_OVERIDE to True '
                      'to override this and force an update. \n')
            else:
                import os
                os.remove('Games.dat')
                getGameTable()

def getGameTable():
    import snowflake.connector
    import numpy as np
    import pandas as pd
    import os

    if SnowflakeFlag:
        ctx = snowflake.connector.connect(
            user='XXXXXXXXX',
            password=keycode(),
            account='labs.us-east-1'
            )
        cs = ctx.cursor()
        print('here')

        NUMBER_OF_ROWS_LIMIT= 1000000

        #store all responses for all queries    
        print('\nQUERY (THROUGH SNOWFLAKE) REPORT: \n')
        import time
        start = time.time()
        query1 = "select id,name,updated_at,lpi_release_date,lpi_game_version from .games "

        try:
            print("Running query: %i" %(1))
            cs = ctx.cursor()
            cur=cs.execute(query1)
            grab_rows = cs.fetchmany(NUMBER_OF_ROWS_LIMIT)
            print('rows fetched: ', np.shape(grab_rows))
            filenamerino = 'GameTableQuery'+str(1)+'.csv'
            # write out
            df = pd.DataFrame.from_records(grab_rows, columns=[x[0].lower() for x in cur.description])
            import os
            if AWSLambda:
                df.to_csv('/tmp/'+filenamerino)
            else:
                df.to_csv(filenamerino)
            print(filenamerino+' captured in local file store.... ')

            cs.close()
        finally:
            ctx.close()

        print('done')
        print(str(time.time()-start) + " seconds to run queries.")

        #rename file after download
        import os
        if AWSLambda:
            os.rename('/tmp/'+filenamerino, '/tmp/'+'Games.dat')
        else:
            os.rename(filenamerino, 'Games.dat')
        print(filenamerino+' shall now be known as.... '+'Games.dat.')


    else:
        #probably just make this a permanent local data store
        bucketrino = 'aws-athena-query-results-XXXXXXXXXXX' 
        locationrino = 'Unsaved/AcrossGamesAnalysis'
        database = ""
        import boto3
        s3 = boto3.resource('s3')
        s3_output = 's3://'+bucketrino+'/'+locationrino+'/'
        #low level client representing Athena, to check on queries
        client = boto3.client('athena')
        queryStore =[]
        query_1 = "select id,name,updated_at,lpi_release_date,lpi_game_version from .games "

        #query/ies here
        res = run_query(query_1,database,s3_output)
        filenamerino = res['QueryExecutionId']+'.csv'
        filerino = locationrino+'/'+filenamerino
        queryStore.append(res['QueryExecutionId'])
        print("Running  Game Table query. ")

        #pause for some arbitrary amount of time
        import time
        time.sleep(5)

        is_queries_running=[]
        response = client.batch_get_query_execution(
            QueryExecutionIds=[queryStore[0]])
        #store completion status
        is_queries_running.append(response['QueryExecutions'][0]['Status']['State']!='SUCCEEDED')

        print('Queries running?'+str(is_queries_running))
        print([str(sum([not i for i in is_queries_running])) +' queries completed out of ' 
               + str(len(is_queries_running)) + ' queries'])

        #check and wait if running
        while (response['QueryExecutions'][0]['Status']['State']=='RUNNING'):
            #responses=[]
            is_queries_running=[]
            #can fetch layers of info about query
            response = client.batch_get_query_execution(
                QueryExecutionIds=[queryStore[0]])
            #store full response, and just completion status
            #responses.append(response) 
            is_queries_running.append(response['QueryExecutions'][0]['Status']['State']!='SUCCEEDED')

        print('Queries running?'+str(is_queries_running))
        print([str(sum([not i for i in is_queries_running])) +' queries completed out of ' 
               + str(len(is_queries_running)) + ' queries'])
        print('Current Status:' + response['QueryExecutions'][0]['Status']['State'])


        #If there, get it
        if response['QueryExecutions'][0]['Status']['State'] == 'SUCCEEDED':
            #check to see if query is there
            listContents(bucketrino,filerino)
            #download it
            readContents(bucketrino,locationrino,filenamerino)
            #rename file after download
            import os
            if AWSLambda:
                os.rename('/tmp/'+filenamerino, '/tmp/'+'Games.dat')
            else:
                os.rename(filenamerino, 'Games.dat')
            print(filenamerino+' shall now be known as.... '+'Games.dat.')
    

def cleanData(NUMMISSINGITEMS, MINPERFORMANCE, NUMSESSIONS,OUTLIERFILTER, SESSION_FILTER,USER_TRUE_SESSION,filenamerino):
    #This removes all rows/entries that fail the following:
    #Gameplay session has less than NUMITEMS (did they quit halfway?)
    #Gameplay session has less than MINPERFORMANCE correct (did they just set it down?)
    #Number of total sessions in this dataset by user is less than NUMSESSIONS (did this user only play once or twice?)
    
    import pandas as pd
    if AWSLambda:
        data = pd.read_csv('/tmp/'+ filenamerino)
    else:
        data = pd.read_csv(filenamerino)
    import numpy as np
    
    #Sort data AND RESET INDEX
    data=data.sort_values(['user_id','event_created_at'])

    
    #NUMMISSINGITEMS = -1 # -1 to include all; 0 to include only game_sessions with max items; 1 to include game sessions only missing 1, etc
    print('\n' + str(np.shape(data)[0]) + ' is the total number of sessions fetched by this query.')
    
    #drop empty data
    trimdata=data.dropna(axis=1, how='all') #drop all columns num_tries/num_trials if empty
    trimdata=trimdata.dropna(axis=0, how='any') #drop any events that are empty
    
    print('Initial size is: ' + str(data.shape[0]) + ' and final size is: ' + str(trimdata.shape[0]) + ' after dropping empties.')
    data = trimdata
    
    #sigh... check for bad typed entries
    if (((pd.to_numeric(data['numtries'], downcast='integer', errors='coerce').isnull().sum()) > 0) 
        or (pd.to_numeric(data['numcorrect'], downcast='integer', errors='coerce').isnull().sum()) > 0):
        print('\nType errors (probably not integer) found for [numtries]: ' 
        + str(pd.to_numeric(data['numtries'], downcast='integer', errors='coerce').isnull().sum())
        + ' and for [numcorrect]: '
        + str(pd.to_numeric(data['numcorrect'], downcast='integer', errors='coerce').isnull().sum()))
        
        #now get rid of bad events that refuse to convert
        data['numcorrect'] = data[~pd.to_numeric(data['numcorrect'], downcast='integer', errors='coerce').isnull()]['numcorrect']
        data['numtries'] = data[~pd.to_numeric(data['numtries'], downcast='integer', errors='coerce').isnull()]['numtries']
    else:
        print('No type errors found in data.')
    
    #now force others into integers
    data['numtries'] = pd.to_numeric(data['numtries'], downcast='integer', errors='coerce')
    data['numcorrect'] = pd.to_numeric(data['numcorrect'], downcast='integer', errors='coerce')
        
    
    #downsample time played to seconds from milliseconds
    print("Downsampling playtime to seconds from ms.")
    data['playtime'] = data['playtime']/1000     
    data['playtime'] = pd.to_numeric(data['playtime'], downcast='integer', errors='coerce')

    #if Missing Item Filter is on (ie. set to a positive number)
    if NUMMISSINGITEMS > 0:
        print('Maximum items [numtries] found in data set is: ' + str(data['numtries'].max()))
        print('Number of sessions with ', str((data['numtries'].max() - NUMMISSINGITEMS)) 
              + ' trials or less: '+ str(np.sum(data['numtries'] < (data['numtries'].max() -NUMMISSINGITEMS))) ) 
        trimdata = data[~((data['numtries'] < (data['numtries'].max() -NUMMISSINGITEMS) ))]
        print('Size of sessions before: ' + str(np.shape(data)[0]) + ' and size after: '
              + str(np.shape(trimdata)[0]))
        data=trimdata
    else:
        print('Skipping item filtering step.')

    #If min performance filter is on
    if MINPERFORMANCE > 0:
        print('Number of sessions with ' + str(MINPERFORMANCE) + ' performance or less: ' + str(np.sum(data['numcorrect'] <= MINPERFORMANCE)) )
        trimdata = data[~((data['numcorrect'] <= MINPERFORMANCE))]
        print('Size of sessions before: '  + str(np.shape(data)[0]) + ' and size after: ' + str(np.shape(trimdata[~((trimdata['numcorrect'] <= MINPERFORMANCE))])[0]))
        data=trimdata
    else:
        print('Skipping minimum performance filtering step.')

    #if outlier filter is on (ie set to a positive number, get rid of that many sigmas)
    if OUTLIERFILTER > 0:
        print(str(np.sum(data['numcorrect'] > (data['numcorrect'].mean() + data['numcorrect'].std()*OUTLIERFILTER)))
              + ' number of sessions with higher performance than: ' 
              + str(data['numcorrect'].mean() + data['numcorrect'].std()*OUTLIERFILTER)
              + ' and '
              + str(np.sum(data['numcorrect'] < (data['numcorrect'].mean() - data['numcorrect'].std()*OUTLIERFILTER)))
              + ' sessions with lower performance than: '
              + str(data['numcorrect'].mean() - data['numcorrect'].std()*OUTLIERFILTER)
              + ' num items correct.')
        data = data[~(data['numcorrect'] < (data['numcorrect'].mean() - data['numcorrect'].std()*OUTLIERFILTER) )]
        data = data[~(data['numcorrect'] > (data['numcorrect'].mean() + data['numcorrect'].std()*OUTLIERFILTER) )]
    else:
        print('Skipping filtering of outliers.')
      
    #if SESSION_FILTER is on
    if SESSION_FILTER >=0:
        print('Data size before Session Filtering: ' + str(np.shape(data)[0]) + ' sessions. ')
        print('Session levels: ' 
              + str(data.groupby(['session_level']).count().index[data.groupby(['session_level']).count()['user_id'] < SESSION_FILTER].values)
              + ' have only ' 
              + str(data.groupby(['session_level']).count()['user_id'][data.groupby(['session_level']).count()['user_id'] < SESSION_FILTER].values)
              + ' gameplay events and will be excluded.')
        for slExclude in data.groupby(['session_level']).count().index[data.groupby(['session_level']).count()['user_id'] < SESSION_FILTER].values:
            data = data[~(data['session_level'] == slExclude)]
        print('Data size after: ' + str(np.shape(data)[0]) + ' sessions. ')
    else:
        print('Skipping sparse data session filtering.')
        
    #Remove low session users (after exclusion of low item sessions)
    #    Add a counter column first
    if USER_TRUE_SESSION:
        if 'nth' in data.columns.values:
            data.rename(columns ={'nth':'session_count'}, inplace=True)
            print('Renaming query count (nth) into session #s...')
        else:
            data['session_count'] = data.groupby(['user_id']).cumcount()+1
            print('No nth col found...extrapolating session #s from event creation dates...')
    else:
        data['session_count'] = data.groupby(['user_id']).cumcount()+1
        print('Extrapolating session #s from event creation dates...')
        
#     print('OMAR DEBUG: num tries len: ' + str(len(data['numtries']>0)))
#     print('OMAR DEBUG: num correct len: ' + str(len(data['numcorrect']>0)))
    print('OMAR DEBUG: num tries len: ' + str(data['numtries'].max()))
    print('OMAR DEBUG: num correct len: ' + str(data['numcorrect'].max()))
    
    
    if data['numtries'].max() < 1:
        print("numtries is missing or set to 0. Deleting from data to skip tries-related plots.")
        data=data.drop('numtries',axis=1)
        try:
            print('numtries still present. max numtries after dropping: ' + str(data['numtries'].max()))
        except:
            print('numtries removed')
    
    
    print('Attempting to filter all users with not enough session data...')
    #data = data[~(data['session_count'] <= NUMSESSIONS)]
    datatrimmed = data.groupby('user_id').filter(lambda x: len(x) >= NUMSESSIONS).sort_values(['user_id','session_count'])
    print(str(np.shape(data)[0] - np.shape(datatrimmed)[0]) + ' sessions excluded because user played ' + str(NUMSESSIONS) + ' or less (after all other filtering, too).')
    data=datatrimmed
    print(str(np.shape(data)[0]) + ' is the final size of the session matrix.\n')
    
    if AWSLambda:
        data.to_csv('/tmp/'+ filenamerino)
    else:
        data.to_csv(filenamerino)

def keycode():
    s = [1,2,3,4,5,6,7,8,9,10]
    return ''.join([chr(c) for c in s])

#def kewlplot():
def kewlPlot(x, y, yerr, ax, color):

    import matplotlib.pyplot as plt
    import numpy as np
    yerr=yerr.fillna(0)

    #---basic plot but with error bars!!!
    ax.errorbar(x, y, ms=20, c=color, lw=5,  yerr = yerr, fmt='o')
#    ax.plot(x, y, 'o', color, markersize=8,
#            markeredgewidth=1, markeredgecolor=color, markerfacecolor='None')
    #ax.legend()
    #ax.grid()

    modelshitworking = False
    if modelshitworking:
        import scipy.stats as stats

        # Modeling with Numpy
        p, cov = np.polyfit(x, y, 1, cov=True)        # parameters and covariance from of the fit
        y_model = np.polyval(p, x)                    # model using the fit parameters; NOTE: parameters here are coefficients

        # Statistics
        n = y.size                              # number of observations
        m = p.size                                    # number of parameters
        DF = n - m                                    # degrees of freedom
        t = stats.t.ppf(0.95, n - m)                  # used for CI and PI bands
        var = np.var(y)

        # Estimates of Error in Data/Model
        resid = y - y_model                           
        #chi2 = np.sum((resid/y_model)**2)             # chi-squared; estimates error in data
        chi2 = np.sum((resid**2)/(var**2))            # chi-squared; weighted sums of squared deviations: estimates error in data
        chi2_red = chi2/(DF)                          # reduced chi-squared; chi sq per DoF; measures goodness of fit
        rsd_err = np.sqrt(np.sum(resid**2)/(DF))        #residual standard deviation of the error

        # Plotting --------------------------------------------------------------------

        # Data
        # Fit
        ax.plot(x, y_model, '-', color='0.1', linewidth='2', alpha=0.5, label='Fit')  

        x2 = np.linspace(np.min(x), np.max(x), 100)
        y2 = np.linspace(np.min(y_model), np.max(y_model), 100)

        # Prediction Interval
        PI = t*rsd_err*np.sqrt(1/n +(x2-np.mean(x))**2/np.sum((x-np.mean(x))**2))
        ax.fill_between(x2, y2+PI, y2-PI, color='#b9cfe7', edgecolor='')

        '''Minor hack for labeling CI fill_between()'''
        ax.plot(x2, y2+PI, '-', color='#b9cfe7', label='95% Prediction Limits')

        # Confidence Interval
        CI = t*rsd_err*np.sqrt(1+1/n+(x2-np.mean(x))**2/np.sum((x-np.mean(x))**2))   
        ax.fill_between(x2, y2+CI, y2-CI, color='None', linestyle='--')
        ax.plot(x2, y2-CI, '--', color='0.5', label='95% Confidence Limits')
        ax.plot(x2, y2+CI, '--', color='0.5')

def plot_mean_and_CI(indx, mean, stderr, ax, color_mean=None, color_shading=None, label=None):
    # plot the shaded range of the confidence intervals
    ax.fill_between(indx, mean-stderr, mean+stderr,
                     color=color_shading, alpha=.5)
    # plot the mean on top
    ax.plot(indx, mean, color_mean, label=label)
    ax.legend()
    ax.grid()
              
def cleanUp(filenamerino):
    import os
    if AWSLambda:
        os.remove('/tmp/'+filenamerino)
    else:
        os.remove(filenamerino)
    print("File " +filenamerino+ " cleaned up.")
    
def cleanOld():
    import os
    for file in os.listdir("."):
        if file.endswith(".csv"):
            os.remove(file)
            print('Removed: ' + str(file) +'\n')
            
def distplot(TEXT_SIZE, data1, data2, color1, color2, TITLE, gamename1, gamename2, XLABEL, YLABEL):
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import matplotlib
    import textwrap
    if not AWSLambda:
        from scipy import stats
    import datetime

    print("\n\n" + TITLE +".")
    fig,ax = plt.subplots(nrows = 1,figsize=(40,10), ncols = 3) #ADD MORE HERE
    matplotlib.rcParams.update({'font.size': TEXT_SIZE})
    matplotlib.rc('xtick', labelsize=32) 
    matplotlib.rc('ytick', labelsize=32) 
    # print(int(np.max([data1.max(),data2.max()]) - np.min([data1.min(),data2.min()])))
    # print(data1.min())
    # print(data1.max())
    # print(data2.min())
    # print(data2.max())
    # print((np.min([data2.min(),data1.min()]), np.max([data2.max(),data1.max()])))
    
    if int(np.max([data1.max(),data2.max()]) - np.min([data1.min(),data2.min()])) + 1 < 30:
        if int(np.max([data1.max(),data2.max()]) - np.min([data1.min(),data2.min()])) + 1 < 20:
            BINSIZE=10
        else:
            BINSIZE=20
    else:
        BINSIZE = 30
        
    ax[0].hist(data1,
               range=(data1.min(),data1.max()),
               bins = BINSIZE,
#                bins=int(data1.max()-data1.min()+1), 
               color=color1, alpha = 0.5);
    ax[0].set_title("\n".join(textwrap.wrap(TITLE + " " 
                                            + gamename1,38)))
    ax[0].set_xlabel(XLABEL)
    ax[0].set_ylabel(YLABEL)

    ax[1].hist(data2, 
               range=(data2.min(),data2.max()),
               bins = BINSIZE,
#                bins=int(data2.max()-data2.min()+1), 
               color=color2, alpha = 0.5);
    ax[1].set_title("\n".join(textwrap.wrap(TITLE + " " + gamename2,38)))
    ax[1].set_xlabel(XLABEL)
    ax[1].set_ylabel(YLABEL)

    ax[2].hist(data1,
               range=(np.min([data2.min(),data1.min()]), np.max([data2.max(),data1.max()])),
               bins=BINSIZE, color=color1, alpha = 0.5, density=True);
    ax[2].hist(data2, 
                  range=(np.min([data2.min(),data1.min()]), np.max([data2.max(),data1.max()])),
                  bins=BINSIZE, color=color2, alpha = 0.5, density=True);
    ax[2].set_title("\n".join(textwrap.wrap(TITLE+" (Both)",38)))
    ax[2].set_xlabel(XLABEL)
    ax[2].set_ylabel(YLABEL)

    plt.show()

    if not AWSLambda:
        try:
            #Kolmogorov-Smirnoff test for distrib            
            wordsyword=['NOT' if stats.ks_2samp(data1,data2)[1]<0.05 else 'TOTALLY']
            from sklearn.preprocessing import normalize
            wordsyword=['NOT' if stats.ks_2samp(normalize(data1[:,np.newaxis],axis=0).ravel(),
                                                normalize(data2[:,np.newaxis],axis=0).ravel())[1]<0.05 
                        else 'TOTALLY']
            print('These distributions are ' + wordsyword[0] + 
                    ' the same. (Kolmogorov-Smirnov test p value is ' 
                    + str(stats.ks_2samp(data1,data2)[1]) + ').  '
                    + 'These distribution shapes are ' + wordsyword[0] + 
                    ' the same. (Kolmogorov-Smirnov test p value following normalization is ' 
                    + str(stats.ks_2samp(normalize(data1[:,np.newaxis],axis=0).ravel(),
                                        normalize(data2[:,np.newaxis],axis=0).ravel())[1])
                    + ').')
        except:
            print("Something about the KS test broke.")
    return fig

    
            
def fullReport(filenamerinos, reportName, pdf, FIT_TEST, TIME_FILTER, NUM_SESSIONS_FOR_CONVERGENCE, DISTRIB_FILTER, PLOT_OUTLIER_FILTER, EARLY_LEARNING_FILTER, LABELS, DOWNSAMPLE, game_ids):
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import matplotlib
    import textwrap
    if not AWSLambda:
        from scipy import stats
    import datetime
    from matplotlib.backends.backend_pdf import PdfPages
    matplotlib.rcParams.update({'font.size': 32})
    matplotlib.rc('xtick', labelsize=32) 
    matplotlib.rc('ytick', labelsize=32) 

    if not FIT_TEST:
        print("Fit test trigger is on, will exclude irrelevant graphs.\n")

    #Get game names for plot titles and Pdf metadata    
    print('Retrieving game names from  games database..')
    if AWSLambda:
        data1 = pd.read_csv('/tmp/'+filenamerinos[0])
        data2 = pd.read_csv('/tmp/'+filenamerinos[1])
        data = pd.read_csv('/tmp/'+'Games.dat')
    else:
        data1 = pd.read_csv(filenamerinos[0])
        data2 = pd.read_csv(filenamerinos[1])
        data = pd.read_csv('Games.dat')
    gamename1 = LABELS[0]+" "+ data[data['id']==game_ids[0]]['name'].item() + ' #' +str(game_ids[0]) #name, lpi_release_date, updated_at, lpi_game_version
    gamename2 = LABELS[1]+" "+ data[data['id']==game_ids[1]]['name'].item() + ' #' +str(game_ids[1]) #name, lpi_release_date, updated_at, lpi_game_version


    # Set the file's metadata via the PdfPages object:
    d = pdf.infodict()
    d['Title'] = 'Game Comparison Report'
    d['Author'] = 'Pythena Porting Tools -- Omar'
    d['Subject'] = 'Analytical Comparison between ' + gamename1 + ' and ' + gamename2
    #d['Keywords'] = ' '
    d['CreationDate'] = datetime.datetime.now()
    #d['ModDate'] = datetime.datetime.today()

    #PLOTCOUNT =0
    #COL = 0
    color1='b'
    color2='g'
    color3 = 'c'
    color4 =  'm'
    TEXT_SIZE=22
        
    #COL = gamenum
    label1='Game #'+str(game_ids[0])
    label2='Game #'+str(game_ids[1])
    
    
    if data1.shape[0]==0:
        print('No data in game id '+game_id[0] +'! Skipping.')    
    elif data2.shape[0]==0:
        print('No data in game id '+game_id[1] +'! Skipping.')    
    else:


        print("Analysis of User Behavior, User Progression, User Performance, Leveling Systems, Scoring Systems: \n"
                + '\n\nUSER BEHAVIOR: ')

        #Distrib of Total Sessions played
        if ('user_id' in data1.columns) and ('session_count' in data1.columns) and  ('user_id' in data2.columns) and ('session_count' in data2.columns):

            numplays1  = data1.groupby('user_id')['session_count'].max()
            numplays2  = data2.groupby('user_id')['session_count'].max()
            
            TITLE = "Distribution of Total Sessions across Users "
            XLABEL = "Number of total plays"
            YLABEL = "Total users"
            fig = distplot(TEXT_SIZE, numplays1, numplays2, color1, color2, TITLE, gamename1, gamename2, XLABEL, YLABEL)

            pdf.savefig(fig)
    
            if int(DISTRIB_FILTER)>0:
                OEnumplays1 = numplays1[numplays1<=DISTRIB_FILTER]
                OEnumplays2 = numplays2[numplays2<=DISTRIB_FILTER]

                print('Users (after filtering) on '  + gamename1 + ' played anywhere from ' +str(numplays1.min()) + ' plays to ' 
                      +str(numplays1.max()) 
                      + ' plays. Outlier exclusion for this set is set at ' +str(DISTRIB_FILTER) + ' plays which '
                      +' excludes ' + str(np.shape(numplays1)[0] - np.shape(OEnumplays1)[0]) + ' users out of ' 
                      + str(np.shape(numplays1)[0]) + ' users.  '
                      + 'Users (after filtering) on '  + gamename2 + ' played anywhere from ' +str(numplays2.min()) + ' plays to ' 
                      +str(numplays2.max()) 
                      + ' plays. Outlier exclusion for this set is set at ' +str(DISTRIB_FILTER) + ' plays which '
                      +' excludes ' + str(np.shape(numplays2)[0] - np.shape(OEnumplays2)[0]) + ' users out of ' 
                      + str(np.shape(numplays2)[0]) + ' users.')

                TITLE = "Distribution of Total Sessions across Users (excluding >"+str(DISTRIB_FILTER)+" plays) across Users "
                XLABEL = "Number of total plays"
                YLABEL = "Total users"
                fig = distplot(TEXT_SIZE, OEnumplays1, OEnumplays2, color1, color2, TITLE, gamename1, gamename2, XLABEL, YLABEL)

                pdf.savefig(fig)       
                
        else:
            print('No user id or session count data.')
            
        
         #Distrib of Time played
        if ('user_id' in data1.columns) and ('playtime' in data1.columns) and  ('user_id' in data2.columns) and ('playtime' in data2.columns):
            print('\n\nDistribution of Maximum Time played.')

            numplays1  = data1.groupby('user_id')['playtime'].max()
            numplays2  = data2.groupby('user_id')['playtime'].max()

            if TIME_FILTER[0]>0:
                print('Filtering outliers... \n')
                OEMultiplier = TIME_FILTER[0]
                numplaysOutlierCuttoff1 = numplays1.mean() + numplays1.std()*OEMultiplier
                numplays1 = numplays1[numplays1<=numplaysOutlierCuttoff1]
            if TIME_FILTER[1]>0:
                print('Filtering outliers... \n')
                OEMultiplier = TIME_FILTER[1]
                numplaysOutlierCuttoff2 = numplays2.mean() + numplays2.std()*OEMultiplier
                numplays2 = numplays2[numplays2<=numplaysOutlierCuttoff2]

            TITLE = "Distribution of Maximum Time Spent across Users "
            XLABEL = "Amount of Time"
            YLABEL = "Total users"
            fig = distplot(TEXT_SIZE, numplays1, numplays2, color1, color2, TITLE, gamename1, gamename2, XLABEL, YLABEL)

            pdf.savefig(fig)

            
         #Distrib of Time played
        if ('user_id' in data1.columns) and ('playtime' in data1.columns) and  ('user_id' in data2.columns) and ('playtime' in data2.columns):
            print('\n\nDistribution of Maximum Time played.')

            numplays1  = data1.groupby('user_id')['playtime'].min()
            numplays2  = data2.groupby('user_id')['playtime'].min()
            
            if TIME_FILTER[0]>0:
                print('Filtering outliers... \n')
                OEMultiplier = TIME_FILTER[0]
                numplaysOutlierCuttoff1 = numplays1.mean() + numplays1.std()*OEMultiplier
                numplays1 = numplays1[numplays1<=numplaysOutlierCuttoff1]                
            if TIME_FILTER[1]>0:
                print('Filtering outliers... \n')
                OEMultiplier = TIME_FILTER[1]
                numplaysOutlierCuttoff2 = numplays2.mean() + numplays2.std()*OEMultiplier
                numplays2 = numplays2[numplays2<=numplaysOutlierCuttoff2]

            TITLE = "Distribution of Minimum Time Spent across Users "
            XLABEL = "Amount of Time"
            YLABEL = "Total users"
            fig = distplot(TEXT_SIZE, numplays1, numplays2, color1, color2, TITLE, gamename1, gamename2, XLABEL, YLABEL)
                
            pdf.savefig(fig)
            

        
        print('\nUSER PROGRESSION: ')
        #sessionLevel vs session_count
        print('\nNOTE: KNEE BENDS MAY BE INDUCED BY USER FILTERING (e.g. including users who play only 20 sessions ' 
              'or more may induce a knee bend in Learning/Familiarization curves at level 20/21.)' )

         #Distrib of Session Levels
        if ('session_level' in data1.columns) and  ('session_level' in data2.columns):
            print('\n\nDistribution of Session Levels across ALL PLAYS.')

            numplays1  = data1['session_level']
            numplays2  = data2['session_level']

            TITLE = "Distribution of Session Levels across ALL PLAYS "
            XLABEL = "Session Level"
            YLABEL = "Total plays"
            fig = distplot(TEXT_SIZE, numplays1, numplays2, color1, color2, TITLE, gamename1, gamename2, XLABEL, YLABEL)

            pdf.savefig(fig)


         #Distrib of User Levels
        if ('user_level' in data1.columns) and  ('user_level' in data2.columns):
            print('\n\nDistribution of Session Levels across ALL PLAYS.')

            numplays1  = data1['user_level']
            numplays2  = data2['user_level']

            TITLE = "Distribution of User Levels Across ALL PLAYS "
            XLABEL = "User Level"
            YLABEL = "Total Plays"
            fig = distplot(TEXT_SIZE, numplays1, numplays2, color1, color2, TITLE, gamename1, gamename2, XLABEL, YLABEL)

            pdf.savefig(fig)


        if (FIT_TEST and ('session_level' in data1.columns) and ('session_count' in data1.columns) and
           ('session_level' in data2.columns) and ('session_count' in data2.columns)):
            #LOOKS ACROSS ALL SESSIONS
            slProg1 = data1.groupby('session_count')['session_level'].mean() 
            slProgStderr1 = (data1.groupby('session_count')['session_level'].std()
                            /np.sqrt(data1.groupby('session_count')['session_level'].count()) )

            slProg2 = data2.groupby('session_count')['session_level'].mean() 
            slProgStderr2 = (data2.groupby('session_count')['session_level'].std()
                            /np.sqrt(data2.groupby('session_count')['session_level'].count()) )
            
            print('\n\nSession count vs session Level across all sessions.')
            fig,ax = plt.subplots(nrows = 1,figsize=(40,10), ncols = 3) #ADD MORE HERE
            matplotlib.rcParams.update({'font.size': TEXT_SIZE})
            matplotlib.rc('xtick', labelsize=32) 
            matplotlib.rc('ytick', labelsize=32) 
            
            kewlPlot(slProg1.index.values, slProg1, slProgStderr1, ax[0], color1)  
            kewlPlot(slProg2.index.values, slProg2, slProgStderr2, ax[1], color2)  
            ax[0].set_title("\n".join(textwrap.wrap("Average Level across Session Plays for " + gamename1,38)))
            ax[1].set_title("\n".join(textwrap.wrap("Average Level across Session Plays for " + gamename2,38)))
            ax[2].set_title("\n".join(textwrap.wrap("Average Level across Session Plays for Both",38)))
            ax[0].set_xlabel('Session Number')
            ax[0].set_ylabel('Average Level')
            ax[1].set_xlabel('Session Number')
            ax[1].set_ylabel('Average Level')
            ax[2].set_xlabel('Session Number')
            ax[2].set_ylabel('Average Level')
            #ax[PLOTCOUNT][COL].set_yticks(np.arange(slProg.min(),slProg.max()))
            kewlPlot(slProg1.index.values, slProg1, slProgStderr1, ax[2], color1)  
            kewlPlot(slProg2.index.values, slProg2, slProgStderr2, ax[2], color2)  
            pdf.savefig(fig)
            plt.show()
            
            print('Session# only accounts for %' 
                 + str(100*np.corrcoef(slProg1.index.values,slProg1)[0][1]**2)
                 + ' variance accounted for, for the users level (assuming a crappy linear model)'
                 + ' in ' + gamename1 + '.')
            print('Session# only accounts for %' 
                 + str(100*np.corrcoef(slProg2.index.values,slProg2)[0][1]**2)
                 + ' variance accounted for, for the users level (assuming a crappy linear model)'
                 + ' in ' + gamename2 + '.')
            
            print('Could put some stats test to tell you whether these are different.')
            

            #LOOKS ACROSS ONLY FIRST X SESSIONS
            slProg1counts=data1.groupby('session_count')['session_level'].count()
            slProg2counts=data2.groupby('session_count')['session_level'].count()
            #EARLY_LEARNING_FILTER = [.05, 20]
            print("\n\nSome automated exclusion filters below b/c some outlier players play a large number of sessions.")
            for iters in EARLY_LEARNING_FILTER:
                fig,ax = plt.subplots(nrows = 1,figsize=(40,10), ncols = 3) #ADD MORE HERE
                matplotlib.rcParams.update({'font.size': TEXT_SIZE})
                matplotlib.rc('xtick', labelsize=32) 
                matplotlib.rc('ytick', labelsize=32) 
                if iters <= 1:
                    #Find "knee in the bend" or just early learning behavior:
                    slProgRange1 = slProg1counts.cumsum()<slProg1counts.cumsum()[-1:].values[0]*(1-iters)
                    slProgRange2 = slProg2counts.cumsum()<slProg2counts.cumsum()[-1:].values[0]*(1-iters)
                    print('\n\nEXCLUDING %' + str(100*iters) + ' of sessions (to filter out tail after kneebend), to visualize early learning/familiarization behavior:')
                    tslProg1=slProg1[slProgRange1]
                    tslProg2=slProg2[slProgRange2]
                    tslProgStderr1=slProgStderr1[slProgRange1]
                    tslProgStderr2=slProgStderr2[slProgRange2]
                    ax[0].set_title("\n".join(textwrap.wrap("Average Level across Session Plays, excluding %"+str(iters*100) + " for " + gamename1,38)))
                    ax[1].set_title("\n".join(textwrap.wrap("Average Level across Session Plays, excluding %"+str(iters*100) + " for " + gamename2,38)))
                    ax[2].set_title("\n".join(textwrap.wrap("Average Level across Session Plays, excluding %"+str(iters*100) + " for Both",38)))

                elif iters > 1:
                    #just do session number inclusion
                    tslProg1=slProg1[0:int(iters)]
                    tslProg2=slProg2[0:int(iters)]
                    tslProgStderr1=slProgStderr1[0:int(iters)]
                    tslProgStderr2=slProgStderr2[0:int(iters)]
                    print('\n\nINCLUDING ONLY ' + str(int(iters)) + ' sessions, to visualize early learning/familiarization behavior:')
                    ax[0].set_title("\n".join(textwrap.wrap("Average Level across first " + str(iters) + " Session Plays for " + gamename1,38)))
                    ax[1].set_title("\n".join(textwrap.wrap("Average Level across first " + str(iters) + " Session Plays for " + gamename2,38)))
                    ax[2].set_title("\n".join(textwrap.wrap("Average Level across first " + str(iters) + " Session Plays for Both",38)))


                kewlPlot(tslProg1.index.values, tslProg1, tslProgStderr1, ax[0], color1)  
                kewlPlot(tslProg2.index.values, tslProg2, tslProgStderr2, ax[1], color2)  

                ax[0].set_xlabel('Session Number')
                ax[0].set_ylabel('Average Level')
                ax[1].set_xlabel('Session Number')
                ax[1].set_ylabel('Average Level')
                ax[2].set_xlabel('Session Number')
                ax[2].set_ylabel('Average Level')

                kewlPlot(tslProg1.index.values, tslProg1, tslProgStderr1, ax[2], color1)  
                kewlPlot(tslProg2.index.values, tslProg2, tslProgStderr2, ax[2], color2)  
                plt.show()
                pdf.savefig(fig)

                print('Session# only accounts for %' 
                     + str(100*np.corrcoef(tslProg1.index.values,tslProg1)[0][1]**2)
                     + ' variance accounted for, for the users level (assuming a crappy linear model)'
                     + ' in ' + gamename1 + '.')
                print('Session# only accounts for %' 
                     + str(100*np.corrcoef(tslProg2.index.values,tslProg2)[0][1]**2)
                     + ' variance accounted for, for the users level (assuming a crappy linear model)'
                     + ' in ' + gamename2 + '.')

            #plot_mean_and_CI(slProg.index.values, slProgStderr, slProgStderr, ax[PLOTCOUNT][2], 
            #                 color_mean=color, color_shading=color, label=label)
            #pdf.savefig(fig)            
        else:
            print('No session level or session_count data.')

#ADD conditional logic, ADD double variables, ADD title resizing, ADD descriptive/hypotest/etc

#         #INDIVIDUAL sessionLevel vs session_count for each user (colorplot) (for segmentation)
        if (FIT_TEST and ('session_level' in data1.columns) and ('session_count' in data1.columns)  and ('user_id' in data1.columns) and 
            ('session_level' in data2.columns) and ('session_count' in data2.columns)  and ('user_id' in data2.columns)):
            print('\n\nSegmentation of users by progression: level by play#')
            # data1['sessionNumber']=data1.groupby('user_id').cumcount()
            # data2['sessionNumber']=data2.groupby('user_id').cumcount()
            userLevelsOverTime1=data1.pivot_table(values='session_level', index = 'user_id', columns='session_count', 
                                                   aggfunc=np.nanmax).reset_index()
            userLevelsOverTime1=userLevelsOverTime1.iloc[:,1:NUM_SESSIONS_FOR_CONVERGENCE+1].dropna(axis=0, how='any')
            userLevelsOverTime1['mean'] = userLevelsOverTime1.mean(axis=1)
            userLevelsOverTime1=userLevelsOverTime1.sort_values(by=userLevelsOverTime1.columns.values.tolist())
            userLevelsOverTime1 = userLevelsOverTime1.drop(['mean'], axis=1)

            userLevelsOverTime2=data2.pivot_table(values='session_level', index = 'user_id', columns='session_count', 
                                                   aggfunc=np.nanmax).reset_index()
            userLevelsOverTime2=userLevelsOverTime2.iloc[:,1:NUM_SESSIONS_FOR_CONVERGENCE+1].dropna(axis=0, how='any')
            userLevelsOverTime2['mean'] = userLevelsOverTime2.mean(axis=1)
            userLevelsOverTime2=userLevelsOverTime2.sort_values(by=userLevelsOverTime1.columns.values.tolist())
            userLevelsOverTime2 = userLevelsOverTime2.drop(['mean'], axis=1)

            #First reduce user footprint to meaningful visualizeable range (500?)
            #DOWNSAMPLE = 1000
            sampletag1=""
            if np.shape(userLevelsOverTime1)[0] > DOWNSAMPLE:
                print("Downsampling for user segmentation graph.")
                shrunkULOT1 = np.zeros([DOWNSAMPLE, np.shape(userLevelsOverTime1)[1]]);
                downsamplejumps = round(np.shape(userLevelsOverTime1)[0]/DOWNSAMPLE)
                sampletag1 = "Downsampled"
                if downsamplejumps == 1:
                    sampletag1 = "Top"
                    userLevelsOverTime1=userLevelsOverTime1.iloc[list(range(np.shape(userLevelsOverTime1)[0] - DOWNSAMPLE,np.shape(userLevelsOverTime1)[0])),:]
                k = 0
                for i in range(downsamplejumps,np.shape(userLevelsOverTime1)[0],downsamplejumps):
                    if k < np.shape(shrunkULOT1)[0]-1:
                        shrunkULOT1[k,:] = userLevelsOverTime1.iloc[i,:].values
                    k+=1
                userLevelsOverTime1 = shrunkULOT1
            else:
                userLevelsOverTime1 = userLevelsOverTime1.values                    

            sampletag2=""
            if np.shape(userLevelsOverTime2)[0] > DOWNSAMPLE:
                print("Downsampling for user segmentation graph.")
                shrunkULOT2 = np.zeros([DOWNSAMPLE, np.shape(userLevelsOverTime2)[1]]);            
                downsamplejumps = round(np.shape(userLevelsOverTime2)[0]/DOWNSAMPLE)
                sampletag2 = "Downsampled"
                if downsamplejumps == 1:
                    sampletag2 = "Top"
                    userLevelsOverTime2=userLevelsOverTime2.iloc[list(range(np.shape(userLevelsOverTime2)[0] - DOWNSAMPLE,np.shape(userLevelsOverTime2)[0])),:]
                k=0
                for i in range(downsamplejumps,np.shape(userLevelsOverTime2)[0],downsamplejumps):
                    if k < np.shape(shrunkULOT2)[0]-1:
                        shrunkULOT2[k,:] = userLevelsOverTime2.iloc[i,:].values
                    k+=1
                userLevelsOverTime2 = shrunkULOT2
            else:
                userLevelsOverTime2 = userLevelsOverTime2.values

            EXPANDFACTOR1 = round(np.shape(userLevelsOverTime1)[0]/np.shape(userLevelsOverTime1)[1])
            EXPANDFACTOR2 = round(np.shape(userLevelsOverTime2)[0]/np.shape(userLevelsOverTime2)[1])

            expandedULOT1 = np.zeros([np.shape(userLevelsOverTime1)[0], EXPANDFACTOR1*np.shape(userLevelsOverTime1)[1]]);
            expandedULOT2 = np.zeros([np.shape(userLevelsOverTime2)[0], EXPANDFACTOR2*np.shape(userLevelsOverTime2)[1]]);

            k=0
            for i in range(np.shape(userLevelsOverTime1)[1]):
                for j in range(EXPANDFACTOR1):
                    expandedULOT1[:,k] = userLevelsOverTime1[:,i]
                    k+=1
            k=0
            for i in range(np.shape(userLevelsOverTime2)[1]):
                for j in range(EXPANDFACTOR2):
                    expandedULOT2[:,k] = userLevelsOverTime2[:,i]
                    k+=1

            #Session count vs session Level
            import matplotlib.patches as mpatches
            fig,ax = plt.subplots(nrows = 1,figsize=(40,10), ncols = 3) #ADD MORE HERE
            matplotlib.rcParams.update({'font.size': TEXT_SIZE})
            matplotlib.rc('xtick', labelsize=32) 
            matplotlib.rc('ytick', labelsize=32) 

            #ax[PLOTCOUNT][COL].imshow(userLevelsOverTime)
            im1 = ax[0].imshow(expandedULOT1, interpolation='none')    
            ax[0].set_xticks(np.arange(1,NUM_SESSIONS_FOR_CONVERGENCE,2))
            im2 = ax[1].imshow(expandedULOT2, interpolation='none')
            ax[1].set_xticks(np.arange(1,NUM_SESSIONS_FOR_CONVERGENCE,2))

            #attempt at color bar legend
            im1 = ax[0].imshow(expandedULOT1, interpolation='none')
            im2 = ax[1].imshow(expandedULOT2, interpolation='none')
           # im3 = ax[2].imshow(expandedULOT2-expandedULOT1, interpolation='none')
        #    ax[2].imshow(expandedULOT1-expandedULOT2, interpolation='none')

            values1 = np.append(np.unique(expandedULOT1.ravel()), np.unique(expandedULOT1.ravel()))
            values1 = np.sort(values1)
            values1 = np.linspace(values1[0],values1[-1:],12)
            #values1 = [i for i in range(int(values1[0]),int(values1[-1:]),int( (values1[-1:]-values1[0])/(16)))]
            colors1 = np.asarray([ im1.cmap(im1.norm(value)) for value in values1])
            colors1=colors1.reshape(-1,4)
            print(np.shape(values1),np.shape(colors1))
            patches1 = [ mpatches.Patch(color=colors1[i], label="Level {l}".format(l=values1[i]) ) for i in range(len(values1)) ]
            plt.legend(handles=patches1, bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0. )

            ax[0].set_title("\n".join(textwrap.wrap(sampletag1 + " "+ str(np.shape(userLevelsOverTime1)[0]) 
                                                    + " User's Levels across "+ str(NUM_SESSIONS_FOR_CONVERGENCE)
                                                    +"  Plays for " + gamename1,38)))
            ax[0].set_xlabel('Session Number')
            ax[0].set_ylabel('Users')

            ax[1].set_title("\n".join(textwrap.wrap(sampletag2 + " "+ str(np.shape(userLevelsOverTime2)[0]) 
                            + " User's Levels across "+ str(NUM_SESSIONS_FOR_CONVERGENCE)
                                                    +"  Plays for " + gamename2,38)))
            ax[1].set_xlabel('Session Number')
            ax[1].set_ylabel('Users')

           #ax[2].set_title("\n".join(textwrap.wrap("Difference Map in User Segementation between Both Games",40)))


            plt.show()
            
            #kewlPlot(slProg.index.values, slProg, slProgStderr, ax[PLOTCOUNT][2], color)  
            #plot_mean_and_CI(slProg.index.values, slProgStderr, slProgStderr, ax[PLOTCOUNT][2], 
            #                 color_mean=color, color_shading=color, label=label)
            pdf.savefig(fig)            
        else:
            print('No session level or session_count data or user_id data.')
            
            
        print('\nUSER PERFORMANCE: ')
        #NUM CORRECT
        #Distrib of Num Correct by users
        if (('user_id' in data1.columns) and ('numcorrect' in data1.columns) and 
            ('user_id' in data2.columns) and ('numcorrect' in data2.columns)):
            numcorr1  = data1.groupby('user_id')['numcorrect'].mean()
            numcorr2  = data2.groupby('user_id')['numcorrect'].mean()
            
            
            if PLOT_OUTLIER_FILTER > 0:
                OEMultiplier = PLOT_OUTLIER_FILTER
                numcorrOutlierCuttoff1 = numcorr1.std()*OEMultiplier
                numcorrOutlierCuttoff2 = numcorr2.std()*OEMultiplier
                OEnumcorr1 = numcorr1[numcorr1 < numcorrOutlierCuttoff1]
                OEnumcorr2 = numcorr2[numcorr2 < numcorrOutlierCuttoff2]
                print('\n\nNUM CORRECT: Distribution of number correct')

                print('Users (after filtering) got anywhere from ' 
                      +str(numcorr1.min()) + ' trials correct to ' 
                      +str(numcorr1.max()) 
                      + ' trials correct. Outlier exclusion for this set is set at ' 
                      + str(numcorrOutlierCuttoff1) 
                      + ' correct trials which '
                      +' excludes ' 
                      #test
                      + str(np.shape(numcorr1)[0] - np.shape(OEnumcorr1)[0]) 
                      + ' users out of ' 
                      + str(np.shape(numcorr1)[0]) + ' users.')
                
                print('Users (after filtering) got anywhere from ' 
                  +str(numcorr2.min()) + ' trials correct to ' 
                  +str(numcorr2.max()) 
                  + ' trials correct. Outlier exclusion for this set is set at ' 
                  + str(numcorrOutlierCuttoff2) 
                  + ' correct trials which '
                  +' excludes ' 
                  + str(np.shape(numcorr2)[0] - np.shape(OEnumcorr2)[0]) + ' users out of ' 
                  + str(np.shape(numcorr2)[0]) + ' users.')

            else:
                OEnumcorr1 = numcorr1
                OEnumcorr2 = numcorr2
                
            TITLE = "Distribution of Number Correct across Users "
            XLABEL = "Average Number Correct"
            YLABEL = "Total users"
            fig = distplot(TEXT_SIZE, OEnumcorr1, OEnumcorr2, color1, color2, TITLE, gamename1, gamename2, XLABEL, YLABEL)
                
            pdf.savefig(fig)

        else:
            print('No user id or num correct data.')

        #NUM CORRECT
        #Distrib of Num Correct% by users
        if (('user_id' in data1.columns) and ('numcorrect' in data1.columns) and ('numtries' in data1.columns) and
            ('user_id' in data2.columns) and ('numcorrect' in data2.columns) and ('numtries' in data2.columns)):
            numcorr1  = round(100*data1.groupby('user_id')['numcorrect'].mean()/data1.groupby('user_id')['numtries'].mean())
            numcorr2  = round(100*data2.groupby('user_id')['numcorrect'].mean()/data2.groupby('user_id')['numtries'].mean())
            #get rid of infinities created by 0 num_tries:
            if np.sum(numcorr1==float('inf')) > 0:
                print(str(np.sum(numcorr1==float('inf'))) + ' infinities found in query#1, likely bc number of tries = 0 for some users.')
            if np.sum(numcorr2==float('inf')) > 0:
                print(str(np.sum(numcorr2==float('inf'))) + ' infinities found in query#2, likely bc number of tries = 0 for some users.')

            numcorr1[numcorr1==float('inf')]=0
            numcorr2[numcorr2==float('inf')]=0

            print('\n\nNUM CORRECT %: num_correct/num_tries')
            
            
            if PLOT_OUTLIER_FILTER > 0:
                OEMultiplier = PLOT_OUTLIER_FILTER
                numcorrOutlierCuttoff1 = numcorr1.std()*OEMultiplier
                numcorrOutlierCuttoff2 = numcorr2.std()*OEMultiplier
            
                OEnumcorr1 = numcorr1[numcorr1 < numcorrOutlierCuttoff1]
                OEnumcorr2 = numcorr2[numcorr2 < numcorrOutlierCuttoff2]

                print('Users (after filtering) got anywhere from %' 
                      + str(numcorr1.min()) + ' trials correct to %' 
                      + str(numcorr1.max()) 
                      + ' trials correct. Outlier exclusion for this set is set at %' 
                      +str(numcorrOutlierCuttoff1) 
                      + ' correct trials which '
                      +' excludes ' 
                      + str(np.shape(numcorr1)[0] - np.shape(OEnumcorr1)[0]) 
                      + ' users out of ' 
                      + str(np.shape(numcorr1)[0]) + ' users.')

                print('Users (after filtering) got anywhere from %' 
                      +str(numcorr2.min()) + ' trials correct to %' +str(numcorr2.max()) 
                      + ' trials correct. Outlier exclusion for this set is set at %' 
                      +str(numcorrOutlierCuttoff2) 
                      + ' correct trials which '
                      +' excludes ' 
                      + str(np.shape(numcorr2)[0] - np.shape(OEnumcorr2)[0]) + ' users out of ' 
                      + str(np.shape(numcorr2)[0]) + ' users.')
            else:
                OEnumcorr1 = numcorr1
                OEnumcorr2 = numcorr2
            

            if OEnumcorr1.min() == OEnumcorr1.max() or OEnumcorr2.min() == OEnumcorr2.max():
                print('Skipping Distribution of Num %Correct plot because all users scored the same (minimum of %correct = maximum of %correct')
                print('Users min: and max: ' +str(OEnumcorr1.min()) + ' ' + str(OEnumcorr1.max()))
                print('Users min: and max: ' +str(OEnumcorr2.min()) + ' ' + str(OEnumcorr2.max()))
            else:
                print('Users min: and max: ' +str(OEnumcorr1.min()) + ' ' + str(OEnumcorr1.max()))
                print('Users min: and max: ' +str(OEnumcorr2.min()) + ' ' + str(OEnumcorr2.max()))
    
                TITLE = "Distribution of User's Average % Performance (number correct/num tries)" 
                XLABEL = 'Average % Correct (number correct/num tries)'
                YLABEL = "Total users"
                fig = distplot(TEXT_SIZE, OEnumcorr1, OEnumcorr2, color1, color2, TITLE, gamename1, gamename2, XLABEL, YLABEL)
                    
            pdf.savefig(fig)
        else:
            print('No numtries data to calculate Performance per level.')
            
        #Number correct & Number % Correct by level
        if (('session_level' in data1.columns) and ('numcorrect' in data1.columns) and
           ('session_level' in data2.columns) and ('numcorrect' in data2.columns)):
            slPerf1 = ( (data1.groupby('session_level')['numcorrect'].sum()
                        /data1.groupby('session_level')['numcorrect'].count()).dropna() )
            slPerfStderr1 = (data1.groupby('session_level')['numcorrect'].std()
                            /np.sqrt(data1.groupby('session_level')['numcorrect'].count()) )

            slPerf2 = ( (data2.groupby('session_level')['numcorrect'].sum()
                        /data2.groupby('session_level')['numcorrect'].count()).dropna() )
            slPerfStderr2 = (data2.groupby('session_level')['numcorrect'].std()
                            /np.sqrt(data2.groupby('session_level')['numcorrect'].count()) )
            
            #level vs num correct
            print('\n\nAverage number correct vs level: numcorrect vs session_level.')
            matplotlib.rcParams.update({'font.size': 22})
            matplotlib.rc('xtick', labelsize=22) 
            matplotlib.rc('ytick', labelsize=22) 
            fig,ax = plt.subplots(nrows = 1,figsize=(40,10), ncols = 3) 
            
            #Overlay % correct        
            if (('numtries' in data1.columns) and ('numtries' in data2.columns)):
                matplotlib.rcParams.update({'font.size': 22})
                matplotlib.rc('xtick', labelsize=22) 
                matplotlib.rc('ytick', labelsize=22) 
            else:
                matplotlib.rcParams.update({'font.size': TEXT_SIZE})
                matplotlib.rc('xtick', labelsize=32) 
                matplotlib.rc('ytick', labelsize=32) 
                

            #plot each in the correct column: #ax[0][col 1-3].plot(x,y)
            kewlPlot(slPerf1.index.values, slPerf1, slPerfStderr1, ax[0], color1)  
            kewlPlot(slPerf2.index.values, slPerf2, slPerfStderr2, ax[1], color2)  
            ax[0].set_title("\n".join(textwrap.wrap("Average Number Correct across Session Levels for " + gamename1,38)))
            ax[0].set_xlabel('Session Levels')
            ax[0].set_ylabel('Average Number Correct')
            ax[1].set_title("\n".join(textwrap.wrap("Average Number Correct across Session Levels for " + gamename2,38)))
            ax[1].set_xlabel('Session Levels')
            ax[1].set_ylabel('Average Number Correct')
            ax[2].set_title("\n".join(textwrap.wrap("Average Number Correct across Session Levels for Both",38)))
            ax[2].set_xlabel('Session Levels')
            ax[2].set_ylabel('Average Number Correct')
            print('Approximate variance accounted for in Level to Average Num Correct relationship: %' 
                  + str(100*np.corrcoef(slPerf1.index.values,slPerf1)[0][1]**2))
            #Overlay % correct        
            if (('numtries' in data1.columns) and ('numtries' in data2.columns)):
                ax[0].set_title("\n".join(textwrap.wrap("Average Number Correct [" + color1 + "] (and % Correct ["+color3+"]) across Session Levels for " + gamename1,38)))
                ax[1].set_title("\n".join(textwrap.wrap("Average Number Correct ["+color2+"] (and % Correct ["+color4+"]) across Session Levels for " + gamename2,38)))
                
                print('\nAverage number correct: numcorrect/numtries.')
                data1['slPerf'] = data1['numcorrect']/data1['numtries']*100
                data2['slPerf'] = data2['numcorrect']/data2['numtries']*100

        
                #Infinity logic for ppl who tried 0 times
                if np.sum(data1['slPerf']==float('inf')) > 0:
                    print(str(np.sum(data1['slPerf']==float('inf'))) + ' infinities found in query#1, likely bc number of tries = 0 for some users.')
                    data1['slPerf'][data1['slPerf']==float('inf')]=0
                    print('These have been set to 0.')
                if np.sum(data2['slPerf']==float('inf')) > 0:
                    print(str(np.sum(data2['slPerf']==float('inf'))) + ' infinities found in query#2, likely bc number of tries = 0 for some users.')
                    data2['slPerf'][data2['slPerf']==float('inf')]=0
                    print('These have been set to 0.')


                slPercentPerf1 = data1.groupby('session_level')['slPerf'].mean()
                slPercentPerfStderr1 = (data1.groupby('session_level')['slPerf'].std()
                                       /np.sqrt(data1.groupby('session_level')['slPerf'].count()))
                twinax = ax[0].twinx()
                twinax.set_ylabel('Average % Correct')
                kewlPlot(slPercentPerf1.index.values, slPercentPerf1, slPercentPerfStderr1, twinax, color=color3)
                print('Approximate variance accounted for in Level to Percent Average Correct Trials relationship: %' 
                      + str(100*np.corrcoef(slPercentPerf1.index.values,slPercentPerf1)[0][1]**2))

                slPercentPerf2 = data2.groupby('session_level')['slPerf'].mean()
                slPercentPerfStderr2 = (data2.groupby('session_level')['slPerf'].std()
                                       /np.sqrt(data2.groupby('session_level')['slPerf'].count()))
                twinax = ax[1].twinx()
                twinax.set_ylabel('Average % Correct')
                kewlPlot(slPercentPerf2.index.values, slPercentPerf2, slPercentPerfStderr2, twinax, color=color4)

                print('Approximate variance accounted for in Level to Percent Average Correct Trials relationship: %' 
                      + str(100*np.corrcoef(slPercentPerf2.index.values,slPercentPerf2)[0][1]**2))
                
                twinax = ax[2].twinx()
                plot_mean_and_CI(slPercentPerf1.index.values, slPercentPerf1, slPercentPerfStderr1, twinax, 
                                 color_mean=color1, color_shading=color3, label=label1)
                plot_mean_and_CI(slPercentPerf2.index.values, slPercentPerf2, slPercentPerfStderr2, twinax, 
                                 color_mean=color2, color_shading=color4, label=label2)
                
            else:
                print('No numtries data to calculate Performance per level.')
                
            plot_mean_and_CI(slPerf1.index.values, slPerf1, slPerfStderr1, ax[2], 
                             color_mean=color1, color_shading=color1, label=label1)
            plot_mean_and_CI(slPerf2.index.values, slPerf2, slPerfStderr2, ax[2], 
                             color_mean=color2, color_shading=color2, label=label2)
            plt.show()
            pdf.savefig(fig)
            
        else:
            print('No session level or numcorrect data.')
            

        print('LEVELING SYSTEMS: ')
        #Number correct & Number % Correct by USER level (extrapolated level by game ssytem)
        if (FIT_TEST and ('user_level' in data1.columns) and ('numcorrect' in data1.columns) and
            ('user_level' in data2.columns) and ('numcorrect' in data2.columns)):
            slPerf1 = ( (data1.groupby('user_level')['numcorrect'].sum()
                        /data1.groupby('user_level')['numcorrect'].count()).dropna() )
            slPerfStderr1 = (data1.groupby('user_level')['numcorrect'].std()
                            /np.sqrt(data1.groupby('user_level')['numcorrect'].count()) )

            slPerf2 = ( (data2.groupby('user_level')['numcorrect'].sum()
                        /data2.groupby('user_level')['numcorrect'].count()).dropna() )
            slPerfStderr2 = (data2.groupby('user_level')['numcorrect'].std()
                            /np.sqrt(data2.groupby('user_level')['numcorrect'].count()) )
            print('\n\nNumber correct & Number % Correct by USER level (extrapolated level by game system).')
            fig,ax = plt.subplots(nrows = 1,figsize=(40,10), ncols = 3) 
            #Overlay % correct        
            if (('numtries' in data1.columns) and ('numtries' in data2.columns)):
                matplotlib.rcParams.update({'font.size': 22})
                matplotlib.rc('xtick', labelsize=22) 
                matplotlib.rc('ytick', labelsize=22) 
            else:
                matplotlib.rcParams.update({'font.size': 24})
                matplotlib.rc('xtick', labelsize=32) 
                matplotlib.rc('ytick', labelsize=32) 
                

            #level vs num correct 
            #plot each in the correct column: #ax[0][col 1-3].plot(x,y)
            kewlPlot(slPerf1.index.values, slPerf1, slPerfStderr1, ax[0], color=color1)  
            kewlPlot(slPerf2.index.values, slPerf2, slPerfStderr2, ax[1], color=color2)  
            kewlPlot(slPerf1.index.values, slPerf1, slPerfStderr1, ax[2], color=color1)  
            kewlPlot(slPerf2.index.values, slPerf2, slPerfStderr2, ax[2], color=color2)  
            ax[0].set_title("\n".join(textwrap.wrap("Average Number Correct across User Levels for " 
                                                   + gamename1,38)))
            ax[0].set_xlabel('User Levels')
            ax[0].set_ylabel('Average Number Correct')
            ax[1].set_title("\n".join(textwrap.wrap("Average Number Correct across User Levels for " 
                                                   + gamename2,38)))
            ax[1].set_xlabel('User Levels')
            ax[1].set_ylabel('Average Number Correct')
            ax[2].set_title("\n".join(textwrap.wrap("Average Number Correct across User Levels for Both",38)))
            print('Approximate variance accounted for in NEXT Level to Average Correct Trials relationship: %' 
                  + str(100*np.corrcoef(slPerf1.index.values,slPerf1)[0][1]**2))
            print('Approximate variance accounted for in NEXT Level to Average Correct Trials relationship: %' 
                  + str(100*np.corrcoef(slPerf2.index.values,slPerf2)[0][1]**2))
            #Overlay % correct        
            if (('numtries' in data1.columns) and ('numtries' in data2.columns)):
                ax[0].set_title("\n".join(textwrap.wrap("Average Number Correct ["+color1+"] (and % Correct ["+color3+"]) across User Levels for " 
                                                       + gamename1,38)))
                ax[1].set_title("\n".join(textwrap.wrap("Average Number Correct ["+color2+"] (and % Correct ["+color4+"]) across User Levels for " 
                                                       + gamename2,38)))

                data1['slPerf'] = data1['numcorrect']/data1['numtries']*100
                slPercentPerf1 = data1.groupby('user_level')['slPerf'].mean()
                slPercentPerfStderr1 = (data1.groupby('user_level')['slPerf'].std()
                                       /np.sqrt(data1.groupby('user_level')['slPerf'].count()))
                twinax = ax[0].twinx()
                twinax.set_ylabel('Average % Correct')
                kewlPlot(slPercentPerf1.index.values, slPercentPerf1, slPercentPerfStderr1, twinax, color=color3)
                print('Approximate variance accounted for in NEXT Level to PERCENT (%) Average Correct Trials relationship: %' 
                      + str(100*np.corrcoef(slPercentPerf1.index.values,slPercentPerf1)[0][1]**2))

                data2['slPerf'] = data2['numcorrect']/data2['numtries']*100
                slPercentPerf2 = data2.groupby('user_level')['slPerf'].mean()
                slPercentPerfStderr2 = (data2.groupby('user_level')['slPerf'].std()
                                       /np.sqrt(data2.groupby('user_level')['slPerf'].count()))
                twinax = ax[1].twinx()
                twinax.set_ylabel('Average % Correct')
                kewlPlot(slPercentPerf2.index.values, slPercentPerf2, slPercentPerfStderr2, twinax, color=color4)
                print('Approximate variance accounted for in NEXT Level to PERCENT (%) Average Correct Trials relationship: %' 
                      + str(100*np.corrcoef(slPercentPerf2.index.values,slPercentPerf2)[0][1]**2))
                
                twinax = ax[2].twinx()
                plot_mean_and_CI(slPercentPerf1.index.values, slPercentPerf1, slPercentPerfStderr1, twinax, 
                                 color_mean=color1, color_shading=color3, label=label1)
                plot_mean_and_CI(slPercentPerf2.index.values, slPercentPerf2, slPercentPerfStderr2, twinax, 
                                 color_mean=color2, color_shading=color4, label=label2)                
            else:
                print('No numtries data to calculate % Performance per level.')

            plot_mean_and_CI(slPerf1.index.values, slPerf1, slPerfStderr1, ax[2], 
                             color_mean=color1, color_shading=color1, label=label1)
            plot_mean_and_CI(slPerf2.index.values, slPerf2, slPerfStderr2, ax[2], 
                             color_mean=color2, color_shading=color2, label=label2)
            pdf.savefig(fig)
            plt.show()
        else:
            print('No user level or numcorrect data.')


        if (FIT_TEST and ('session_level' in data1.columns) and ('user_level' in data1.columns) and
            ('session_level' in data2.columns) and ('user_level' in data2.columns)):
            print('\n\nConvergence variability: Level jumps vs Session Levels')
            ul1 =( (data1.groupby('session_level')['user_level'].mean() - data1.groupby('session_level')['session_level'].mean()))
            data1['levelJump'] =data1['user_level'] - data1['session_level']
            ulPerfStderr1 = (data1.groupby('session_level')['levelJump'].std()
                            /np.sqrt(data1.groupby('session_level')['levelJump'].count()) )

            fig,ax = plt.subplots(nrows = 1,figsize=(40,10), ncols = 3) 
            matplotlib.rcParams.update({'font.size': TEXT_SIZE})
            matplotlib.rc('xtick', labelsize=32) 
            matplotlib.rc('ytick', labelsize=32) 

            kewlPlot(ul1.index.values, ul1, ulPerfStderr1, ax[0], color=color1)
            ax[0].set_title("\n".join(textwrap.wrap("Average Level Jumps across Session Levels for " +gamename1,38)))
            ax[0].set_xlabel('Session Levels')
            ax[0].set_ylabel('Average Level Jumps')
            ulvar1 = data1.groupby('session_level')['levelJump'].mean().dropna()
            print('Approximate change in variance of level jumping (convergence) relationship: %' 
                  + str( 100*np.corrcoef(ulvar1.index.values,ulvar1)[0][1]**2) 
                  + ' variance accounted for (in variance of converging behavior)')
            plot_mean_and_CI(ul1.index.values, ul1, ulPerfStderr1, ax[2], 
                             color_mean=color1, color_shading=color1, label=label1)

            ul2 =( (data2.groupby('session_level')['user_level'].mean() - data2.groupby('session_level')['session_level'].mean()))
            data2['levelJump'] =data2['user_level'] - data2['session_level']
            ulPerfStderr2 = (data2.groupby('session_level')['levelJump'].std()
                            /np.sqrt(data2.groupby('session_level')['levelJump'].count()) )
            kewlPlot(ul2.index.values, ul2, ulPerfStderr2, ax[1], color=color2)
            ax[1].set_title("\n".join(textwrap.wrap("Average Level Jumps across Session Levels for " +gamename2,38)))
            ax[1].set_xlabel('Session Levels')
            ax[1].set_ylabel('Average Level Jumps')
            ulvar2 = data2.groupby('session_level')['levelJump'].mean().dropna()
            print('Approximate change in variance of level jumping (convergence) relationship: %' 
                  + str( 100*np.corrcoef(ulvar2.index.values,ulvar2)[0][1]**2) 
                  + ' variance accounted for (in variance of converging behavior)')
            plot_mean_and_CI(ul2.index.values, ul2, ulPerfStderr2, ax[2], 
                             color_mean=color2, color_shading=color2, label=label2)

            ax[2].set_title("\n".join(textwrap.wrap("Average Level Jumps across Session Levels for Both",38)))
            ax[2].set_xlabel('Session Levels')
            ax[2].set_ylabel('Average Level Jumps')
            plt.show()
            pdf.savefig(fig)
            
        else:
            print('No session level or user level data.')
            
        if (FIT_TEST and ('user_id' in data1.columns) and ('event_created_at' in data1.columns) 
            and ('session_level' in data1.columns) and ('user_level' in data1.columns) and
            ('user_id' in data2.columns) and ('event_created_at' in data2.columns) 
            and ('session_level' in data2.columns) and ('user_level' in data2.columns)):            
            print('\n\nLevel jumps vs Time played: convergence behavior:')
            #Add session counts
            data1=data1.sort_values(by=['user_id','event_created_at'])
            data1['sessionNumber']=data1.groupby('user_id').cumcount()
            data2=data2.sort_values(by=['user_id','event_created_at'])
            data2['sessionNumber']=data2.groupby('user_id').cumcount()
            #Add level jumps (in case not done arleady)
            #data['levelJump'] =data['user_level'] - data['session_level']
            #Remove lower than X sessions, trim off higher session
            trimdata1 = data1[~((data1['sessionNumber'] >= NUM_SESSIONS_FOR_CONVERGENCE))]
            trimdata2 = data2[~((data2['sessionNumber'] >= NUM_SESSIONS_FOR_CONVERGENCE))]
            trimdata1=trimdata1.sort_values(by=['user_id','event_created_at'])
            trimdata2=trimdata2.sort_values(by=['user_id','event_created_at'])
            trimdata1['levelJump']=trimdata1['user_level']-trimdata1['session_level']
            trimdata2['levelJump']=trimdata2['user_level']-trimdata2['session_level']

            userLevelJumpOverTime1=trimdata1.pivot_table(values='levelJump', index = 'user_id', columns='sessionNumber', 
                                                   aggfunc=np.nanmean).reset_index()
            userLevelJumpOverTimeMean1=userLevelJumpOverTime1.iloc[:,1:].mean(axis=0,skipna=True)
            userLevelJumpOverTimeMeanStderr1 = (userLevelJumpOverTime1.iloc[:,1:].std(axis=0,skipna=True)/
                                               np.sqrt(userLevelJumpOverTime1.iloc[:,1:].count(axis=0,numeric_only=True)))

            userLevelJumpOverTime2=trimdata2.pivot_table(values='levelJump', index = 'user_id', columns='sessionNumber', 
                                                   aggfunc=np.nanmean).reset_index()
            userLevelJumpOverTimeMean2=userLevelJumpOverTime2.iloc[:,1:].mean(axis=0,skipna=True)
            userLevelJumpOverTimeMeanStderr2 = (userLevelJumpOverTime2.iloc[:,1:].std(axis=0,skipna=True)/
                                               np.sqrt(userLevelJumpOverTime2.iloc[:,1:].count(axis=0,numeric_only=True)))

            fig,ax = plt.subplots(nrows = 1,figsize=(40,10), ncols = 3)             
            matplotlib.rcParams.update({'font.size': TEXT_SIZE})
            matplotlib.rc('xtick', labelsize=32) 
            matplotlib.rc('ytick', labelsize=32) 

            kewlPlot(userLevelJumpOverTimeMean1.index.values.astype(int), userLevelJumpOverTimeMean1, 
                     userLevelJumpOverTimeMeanStderr1, ax[0], color=color1)
            kewlPlot(userLevelJumpOverTimeMean2.index.values.astype(int), userLevelJumpOverTimeMean2, 
                     userLevelJumpOverTimeMeanStderr2, ax[1], color=color2)
            ax[0].set_title("\n".join(textwrap.wrap("Average Level Jumps across Time for "+gamename1,38)))
            ax[0].set_xlabel('Time Played')
            ax[0].set_ylabel('Average Level Jump')
            ax[1].set_title("\n".join(textwrap.wrap("Average Level Jumps across Time for "+gamename2,38)))
            ax[1].set_xlabel('Session # Played')
            ax[1].set_ylabel('Average Level Jump')
            print('Users jump a minimum of ' 
                  + str(userLevelJumpOverTime1.iloc[:,1:].min().min()) + ' and a max of '
                  + str(userLevelJumpOverTime1.iloc[:,1:].max().max()) + ' session level jumps. ')
            plot_mean_and_CI(userLevelJumpOverTimeMean1.index.values.astype(int), userLevelJumpOverTimeMean1, 
                  userLevelJumpOverTimeMeanStderr1, ax[2], color_mean=color1, color_shading=color1, label=label1)
            print('Users jump a minimum of ' 
                  + str(userLevelJumpOverTime2.iloc[:,1:].min().min()) + ' and a max of '
                  + str(userLevelJumpOverTime2.iloc[:,1:].max().max()) + ' session level jumps. ')
            plot_mean_and_CI(userLevelJumpOverTimeMean2.index.values.astype(int), userLevelJumpOverTimeMean2, 
                  userLevelJumpOverTimeMeanStderr2, ax[2], color_mean=color2, color_shading=color2, label=label2)
            ax[2].set_title("\n".join(textwrap.wrap("Average Level Jumps across Time for Both",38)))
            plt.show()
            pdf.savefig(fig)
            
        else:
            print('No user_id or event_created_at or session_level or user_level data.')

            
        #Distrib of Level Changes across sessions
        if (FIT_TEST and ('levelJump' in data1.columns) and ('user_id' in data1.columns) and
            ('levelJump' in data2.columns) and ('user_id' in data2.columns)):
            levelJump1  = data1.groupby('user_id')['levelJump'].mean()
            levelJump2  = data2.groupby('user_id')['levelJump'].mean()
            print("\n\nDistribution of Level changes across sessions.\n")
            
            if PLOT_OUTLIER_FILTER > 0:
                OEMultiplier = PLOT_OUTLIER_FILTER
                levelJumpOutlierCuttoff1 = levelJump1.std()*OEMultiplier
                OElevelJump1 = levelJump1[levelJump1 < levelJumpOutlierCuttoff1]
                levelJumpOutlierCuttoff2 = levelJump2.std()*OEMultiplier
                OElevelJump2 = levelJump2[levelJump2 < levelJumpOutlierCuttoff2]
                print('Users (after filtering) jumped anywhere from ' +str(levelJump1.min()) + ' levels to ' 
                      +str(levelJump1.max()) 
                      + ' levels for an average jump of '+ str(levelJump1.mean())
                      +' levels. Outlier exclusion for this set is set at a level jump of ' +str(levelJumpOutlierCuttoff1) 
                      + ' levels which '
                      +' excludes ' + str(np.shape(levelJump1)[0] - np.shape(OElevelJump1)[0]) + ' users out of ' 
                      + str(np.shape(levelJump1)[0]) + ' users.')
                print('Users (after filtering) jumped anywhere from ' +str(levelJump2.min()) + ' levels to ' 
                      +str(levelJump2.max()) 
                      + ' levels for an average jump of '+ str(levelJump2.mean())
                      +' levels. Outlier exclusion for this set is set at a level jump of ' +str(levelJumpOutlierCuttoff2) 
                      + ' levels which '
                      +' excludes ' + str(np.shape(levelJump2)[0] - np.shape(OElevelJump2)[0]) + ' users out of ' 
                      + str(np.shape(levelJump2)[0]) + ' users.')
            else:
                OElevelJump1 = levelJump1
                OElevelJump2 = levelJump2
                
            TITLE = "Distribution of Average Level Changes across Users "
            XLABEL = 'Average level change for that User'
            YLABEL = "Total users"
            fig = distplot(TEXT_SIZE, OElevelJump1, OElevelJump2, color1, color2, TITLE, gamename1, gamename2, XLABEL, YLABEL)
                
            pdf.savefig(fig)
        else:
            print('No user id or level Jump data.')
        
        
        print('SCORING SYSTEMS: ')
        #Distrib of Scores across users
        if (('score' in data1.columns) and ('user_id' in data1.columns) and
            ('score' in data2.columns) and ('user_id' in data2.columns)):
            score1  = data1.groupby('user_id')['score'].mean()
            score2  = data2.groupby('user_id')['score'].mean()
            if PLOT_OUTLIER_FILTER > 0:
                OEMultiplier = PLOT_OUTLIER_FILTER
                #OEMultiplier = 4
                scoreOutlierCuttoff1 = score1.std()*OEMultiplier
                OEscore1 = score1[score1 < scoreOutlierCuttoff1]
                scoreOutlierCuttoff2 = score2.std()*OEMultiplier
                OEscore2 = score2[score2 < scoreOutlierCuttoff2]
                print('Users (after filtering) scored anywhere from ' +str(score1.min()) + ' points to ' 
                      +str(score1.max()) 
                      + ' points. Outlier exclusion for this set is set at a score of ' +str(scoreOutlierCuttoff1) 
                      + ' points which '
                      +' excludes ' + str(np.shape(score1)[0] - np.shape(OEscore1)[0]) + ' users out of ' 
                      + str(np.shape(score1)[0]) + ' users.')
                print('Users (after filtering) scored anywhere from ' +str(score2.min()) + ' points to ' 
                      +str(score2.max()) 
                      + ' points. Outlier exclusion for this set is set at a score of ' +str(scoreOutlierCuttoff2) 
                      + ' points which '
                      +' excludes ' + str(np.shape(score2)[0] - np.shape(OEscore2)[0]) + ' users out of ' 
                      + str(np.shape(score2)[0]) + ' users.')
            else:
                OEscore1 = score1
                OEscore2 = score2
            
            TITLE = "Distribution of Average Score across Users "
            XLABEL = 'Average Score'
            YLABEL = "Total users"
            fig = distplot(TEXT_SIZE, OEscore1, OEscore2, color1, color2, TITLE, gamename1, gamename2, XLABEL, YLABEL)
                
            pdf.savefig(fig)
        else:
            print('No user_id or score data.')

        #Score by level
        if (('session_level' in data1.columns) and ('score' in data1.columns) and
            ('session_level' in data2.columns) and ('score' in data2.columns)):
            print("\n\nScore by level.")
            slPerf1 = ( (data1.groupby('session_level')['score'].sum()
                        /data1.groupby('session_level')['score'].count()).dropna() )
            slPerfStderr1 = (data1.groupby('session_level')['score'].std()
                            /np.sqrt(data1.groupby('session_level')['score'].count()) )
            slPerf2 = ( (data2.groupby('session_level')['score'].sum()
                        /data2.groupby('session_level')['score'].count()).dropna() )
            slPerfStderr2 = (data2.groupby('session_level')['score'].std()
                            /np.sqrt(data2.groupby('session_level')['score'].count()) )

            #score vs level 
            fig,ax = plt.subplots(nrows = 1,figsize=(40,10), ncols = 3)             
            matplotlib.rcParams.update({'font.size': TEXT_SIZE})
            matplotlib.rc('xtick', labelsize=32) 
            matplotlib.rc('ytick', labelsize=32) 

            kewlPlot(slPerf1.index.values, slPerf1, slPerfStderr1, ax[0], color=color1)  
            kewlPlot(slPerf2.index.values, slPerf2, slPerfStderr2, ax[1], color=color2)  
            ax[0].set_title("\n".join(textwrap.wrap("Average Score across Session Levels for " + gamename1,38)))
            ax[0].set_xlabel('Session Levels')
            ax[0].set_ylabel('Average Score')
            ax[1].set_title("\n".join(textwrap.wrap("Average Score across Session Levels for " + gamename2,38)))
            ax[1].set_xlabel('Session Levels')
            ax[1].set_ylabel('Average Score')
            print('Approximate variance accounted for in Level to Score relationship: %' 
                  + str(100*np.corrcoef(slPerf1.index.values,slPerf1)[0][1]**2))
            plot_mean_and_CI(slPerf1.index.values, slPerf1, slPerfStderr1, ax[2], color_mean=color1, color_shading=color1, label=label1)
            print('Approximate variance accounted for in Level to Score relationship: %' 
                  + str(100*np.corrcoef(slPerf2.index.values,slPerf2)[0][1]**2))
            plot_mean_and_CI(slPerf2.index.values, slPerf2, slPerfStderr2, ax[2], color_mean=color2, color_shading=color2, label=label2)
            ax[2].set_title("\n".join(textwrap.wrap("Average Score across Session Levels for Both",38)))
            plt.show()
            pdf.savefig(fig)
        else:
            print('No session level or score data.')

        #Score by Session Number (session_count)
        if (FIT_TEST and ('session_count' in data1.columns) and ('score' in data1.columns) and
            ('session_count' in data2.columns) and ('score' in data2.columns)):
            print('\n\nScore by session number.')
            slPerf1 = ( (data1.groupby('session_count')['score'].sum()
                        /data1.groupby('session_count')['score'].count()).dropna() )
            slPerfStderr1 = (data1.groupby('session_count')['score'].std()
                            /np.sqrt(data1.groupby('session_count')['score'].count()) )
            slPerf2 = ( (data2.groupby('session_count')['score'].sum()
                        /data2.groupby('session_count')['score'].count()).dropna() )
            slPerfStderr2 = (data2.groupby('session_count')['score'].std()
                            /np.sqrt(data2.groupby('session_count')['score'].count()) )

            #score vs level 
            fig,ax = plt.subplots(nrows = 1,figsize=(40,10), ncols = 3)             
            matplotlib.rcParams.update({'font.size': TEXT_SIZE})
            matplotlib.rc('xtick', labelsize=32) 
            matplotlib.rc('ytick', labelsize=32) 

            kewlPlot(slPerf1.index.values, slPerf1, slPerfStderr1, ax[0], color=color1)  
            kewlPlot(slPerf2.index.values, slPerf2, slPerfStderr2, ax[1], color=color2)  
            ax[0].set_title("\n".join(textwrap.wrap("Average Score across Session Number for " + gamename1,38)))
            ax[0].set_xlabel('Session Number')
            ax[0].set_ylabel('Average Score')
            ax[1].set_title("\n".join(textwrap.wrap("Average Score across Session Number for " + gamename2,38)))
            ax[1].set_xlabel('Session Number')
            ax[1].set_ylabel('Average Score')
            #plot_mean_and_CI(slPerf.index.values, slPerf, slPerfStderr, ax[PLOTCOUNT][2], color_mean=color, color_shading=color, label=label)
            kewlPlot(slPerf1.index.values, slPerf1, slPerfStderr1, ax[2], color=color1)  
            kewlPlot(slPerf2.index.values, slPerf2, slPerfStderr2, ax[2], color=color2)  
            print('Approximate variance accounted for in Session Number to Score relationship: %' 
                  + str(100*np.corrcoef(slPerf1.index.values,slPerf1)[0][1]**2))
            print('Approximate variance accounted for in Session Number to Score relationship: %' 
                  + str(100*np.corrcoef(slPerf2.index.values,slPerf2)[0][1]**2))
            ax[2].set_title("\n".join(textwrap.wrap("Average Score across Session Number for Both",38)))
            plt.show()
            pdf.savefig(fig)
           
            #LOOKS ACROSS ONLY FIRST X SESSIONS
            slPerf1counts=data1.groupby('session_count')['score'].count()
            slPerf2counts=data2.groupby('session_count')['score'].count()
            #EARLY_LEARNING_FILTER = [.05, 20]
            print("\n\nSome automated exclusion filters below b/c some outlier players play a large number of sessions.")
            for iters in EARLY_LEARNING_FILTER:
                fig,ax = plt.subplots(nrows = 1,figsize=(40,10), ncols = 3) #ADD MORE HERE
                matplotlib.rcParams.update({'font.size': 24})
                matplotlib.rc('xtick', labelsize=32) 
                matplotlib.rc('ytick', labelsize=32) 
                
                if iters <= 1:
                    #Find "knee in the bend" or just early learning behavior:
                    slPerfRange1 = slPerf1counts.cumsum()<slPerf1counts.cumsum()[-1:].values[0]*(1-iters)
                    slPerfRange2 = slPerf2counts.cumsum()<slPerf2counts.cumsum()[-1:].values[0]*(1-iters)
                    print('\n\nEXCLUDING %' + str(100*iters) + ' of sessions (to filter out tail after kneebend), to visualize early learning/familiarization behavior:')
                    tslPerf1=slPerf1[slPerfRange1]
                    tslPerf2=slPerf2[slPerfRange2]
                    tslPerfStderr1=slPerfStderr1[slPerfRange1]
                    tslPerfStderr2=slPerfStderr2[slPerfRange2]

                    ax[0].set_title("\n".join(textwrap.wrap("Average Score across Session Plays, excluding %"+str(iters*100) + " for " + gamename1,38)))
                    ax[1].set_title("\n".join(textwrap.wrap("Average Score across Session Plays, excluding %"+str(iters*100) + " for " + gamename2,38)))
                    ax[2].set_title("\n".join(textwrap.wrap("Average Score across Session Plays, excluding %"+str(iters*100) + " for Both",38)))

                elif iters > 1:
                    ax[0].set_title("\n".join(textwrap.wrap("Average Score across first " + str(iters) + " Session Plays for " + gamename1,38)))
                    ax[1].set_title("\n".join(textwrap.wrap("Average Score across first " + str(iters) + " Session Plays for " + gamename2,38)))
                    ax[2].set_title("\n".join(textwrap.wrap("Average Score across first " + str(iters) + " Session Plays for Both",38)))
                    #just do session number inclusion
                    tslPerf1=slPerf1[0:int(iters)]
                    tslPerf2=slPerf2[0:int(iters)]
                    tslPerfStderr1=slPerfStderr1[0:int(iters)]
                    tslPerfStderr2=slPerfStderr2[0:int(iters)]
                    print('\n\nINCLUDING ONLY ' + str(int(iters)) + ' sessions, to visualize early learning/familiarization behavior:')


                kewlPlot(tslPerf1.index.values, tslPerf1, tslPerfStderr1, ax[0], color1)  
                kewlPlot(tslPerf2.index.values, tslPerf2, tslPerfStderr2, ax[1], color2)  

                ax[0].set_xlabel('Session Number')
                ax[0].set_ylabel('Average Score')
                ax[1].set_xlabel('Session Number')
                ax[1].set_ylabel('Average Score')
                ax[2].set_xlabel('Session Number')
                ax[2].set_ylabel('Average Score')

                kewlPlot(tslPerf1.index.values, tslPerf1, tslPerfStderr1, ax[2], color1)  
                kewlPlot(tslPerf2.index.values, tslPerf2, tslPerfStderr2, ax[2], color2)  

                print('Session# only accounts for %' 
                     + str(100*np.corrcoef(tslPerf1.index.values,tslPerf1)[0][1]**2)
                     + ' variance accounted for, for the users level (assuming a crappy linear model)'
                     + ' in ' + gamename1 + '.')
                print('Session# only accounts for %' 
                     + str(100*np.corrcoef(tslPerf2.index.values,tslPerf2)[0][1]**2)
                     + ' variance accounted for, for the users level (assuming a crappy linear model)'
                     + ' in ' + gamename2 + '.')
            
                plt.show()
                pdf.savefig(fig)
        else:
            print('No session count or score data.')
                       
#MAIN BODY STARTS HERE:
#DEFINE PARAMETERS HERE FOR QUERY TOOL:
#query_params_game_ids =[139, 142, 143, 147, 148, 149, 150, 151, 152, 154, 155, 158, 159, 160, 163, 165, 55, 68, 70, 71, 72, 73, 81, 82, 98, 102, 113, 126]
#query_params_game_ids =[139, 142, 143, 166, 176]
#query_params_game_ids =[166, 176]
#query_params_game_ids =[180, 181]
#query_params_game_ids =[139, 142, 143, 147]
def sql_generator(QUERY_WINDOW,USER_TRUE_SESSION,QUERY_AMOUNT,QUOTES,query_params_user_plays,
                        QUERY_AMOUNT_LOWER_LIMIT,query_params_game_ids,
                        CORRECT_LABEL,TRIES_LABEL,PLAY_TIME,SERVER_QUERY,MONTH_COMPARE,VERSION,YEAR,USER_EXCLUSION,SESSION_EXCLUSION, SELECTED_LEVEL, SNOWFLAKE_FLAG):
            #DEFINE QUERY HERE:
        #query_temp1 = ("SELECT event_id, user_id, session_level, user_level, score, json_extract(metadata, '$.num_tries')"
        #               " AS num_tries FROM VIEWS.GAMESAVE WHERE game_id IN (")
        #query_temp1 = ("SELECT user_id, session_level, score, json_extract(metadata, '$.num_tries') AS num_tries "
        #               "FROM VIEWS.GAMESAVE WHERE game_id IN (")
        #query_temp2 = ") AND yyyy=2018 AND mm=2"
        if not QUERY_WINDOW: #if this is empty
            if USER_TRUE_SESSION:
                query1_temp0 = "SELECT * FROM (Select game_result_id,"
                query2_temp0 = "SELECT * FROM (Select game_result_id,"
                query1_temp6 = ")"
                query2_temp6 = ")"
            else:
                query1_temp0 = "Select "
                query2_temp0 = "Select "
                query1_temp6 = ""
                query2_temp6 = ""
            if VERSION[0]:
                query1_temp6 =  " WHERE version = "+str(VERSION[0]) + query1_temp6
            if VERSION[1]:
                query2_temp6 =  " WHERE version = "+str(VERSION[1]) + query2_temp6

            query1_temp1b = ""
            query2_temp1b = ""
        else: #otherwise use parameters
            if USER_TRUE_SESSION[0]:
                query1_temp0 = "SELECT * FROM (SELECT * FROM (Select game_result_id,"
                if SNOWFLAKE_FLAG:
                    query1_temp1b = ", datediff('"+QUERY_WINDOW+"',created_at, current_date) AS TIME_OLD "
                else:
                    query1_temp1b = ", date_diff('"+QUERY_WINDOW+"',created_at, current_date) AS TIME_OLD "
                query1_temp6 = (") WHERE TIME_OLD <=" +str(QUERY_AMOUNT[0]) + " AND TIME_OLD >= " +str(QUERY_AMOUNT_LOWER_LIMIT[0]) 
                                +") INNER JOIN (SELECT id, rank() OVER (PARTITION BY game_id, user_id ORDER BY id) AS nth FROM gameresults.game_results WHERE game_id ="+ str(query_params_game_ids[0]) +") game_num ON game_result_id = game_num.id")
                if SESSION_EXCLUSION[0]:
                    query1_temp6 = (") WHERE TIME_OLD <=" +str(QUERY_AMOUNT[0]) + " AND TIME_OLD >= " +str(QUERY_AMOUNT_LOWER_LIMIT[0]) 
                                    +") INNER JOIN (SELECT id, rank() OVER (PARTITION BY user_id ORDER BY id) AS nth FROM gameresults.game_results WHERE game_id ="+ str(query_params_game_ids[0]) + " OR game_id ="+ str(query_params_game_ids[1]) +") game_num ON game_result_id = game_num.id")
            else:
                query1_temp0 = "SELECT * FROM (Select "
                if SNOWFLAKE_FLAG:
                    query1_temp1b = ", datediff('"+QUERY_WINDOW+"',created_at, current_date) AS TIME_OLD "
                else:
                    query1_temp1b = ", date_diff('"+QUERY_WINDOW+"',created_at, current_date) AS TIME_OLD "
                query1_temp6 = (") WHERE TIME_OLD <=" +str(QUERY_AMOUNT[0]) + " AND TIME_OLD >= " +str(QUERY_AMOUNT_LOWER_LIMIT[0]))

            if USER_TRUE_SESSION[1]:
                query2_temp0 = "SELECT * FROM (SELECT * FROM (Select  game_result_id,"
                if SNOWFLAKE_FLAG:
                    query2_temp1b = ", datediff('"+QUERY_WINDOW+"',created_at, current_date) AS TIME_OLD "
                else:
                    query2_temp1b = ", date_diff('"+QUERY_WINDOW+"',created_at, current_date) AS TIME_OLD "
                query2_temp6 = (") WHERE TIME_OLD <=" +str(QUERY_AMOUNT[1])  + " AND TIME_OLD >= " +str(QUERY_AMOUNT_LOWER_LIMIT[1])
                               +") INNER JOIN (SELECT id, rank() OVER (PARTITION BY game_id, user_id ORDER BY id) AS nth FROM gameresults.game_results WHERE game_id = "+str(query_params_game_ids[1]) +") game_num ON game_result_id = game_num.id")
                if SESSION_EXCLUSION[1]:
                    query2_temp6 = (") WHERE TIME_OLD <=" +str(QUERY_AMOUNT[1])  + " AND TIME_OLD >= " +str(QUERY_AMOUNT_LOWER_LIMIT[1])
                                   +") INNER JOIN (SELECT id, rank() OVER (PARTITION BY user_id ORDER BY id) AS nth FROM gameresults.game_results WHERE game_id = "+str(query_params_game_ids[0]) + " OR game_id ="+ str(query_params_game_ids[1]) +") game_num ON game_result_id = game_num.id")
            else:
                query2_temp0 = "SELECT * FROM (Select "
                if SNOWFLAKE_FLAG:
                    query2_temp1b = ", datediff('"+QUERY_WINDOW+"',created_at, current_date) AS TIME_OLD "
                else:
                    query2_temp1b = ", date_diff('"+QUERY_WINDOW+"',created_at, current_date) AS TIME_OLD "
                query2_temp6 = (") WHERE TIME_OLD <=" +str(QUERY_AMOUNT[1])  + " AND TIME_OLD >= " +str(QUERY_AMOUNT_LOWER_LIMIT[1]))

            if VERSION[0]:
                query1_temp6 = query1_temp6[:7] + " version = "+str(VERSION[0]) + " AND " +  query1_temp6[7:] 
            if VERSION[1]:
                query2_temp6 = query2_temp6[:7] + " version = "+str(VERSION[1]) + " AND " +  query2_temp6[7:]


        if SNOWFLAKE_FLAG:
            if SELECTED_LEVEL:
                query1_temp1 = ("user_id, event_created_at, user_level, "
                                + "GET_PATH(PARSE_JSON(metadata), 'selected_level') AS session_level, score, " +
                               "GET_PATH(PARSE_JSON(metadata), '"
                                +CORRECT_LABEL[1]+
                                "') AS numcorrect, "
                               "GET_PATH(PARSE_JSON(metadata), '"
                                +TRIES_LABEL[1]+
                                "') AS numtries, "+
                               "GET_PATH(PARSE_JSON(metadata), '"
                               +PLAY_TIME[1]+
                               "') AS playtime")
            else:
                query1_temp1 = ("user_id, event_created_at, user_level, session_level, score, " 
                               "GET_PATH(PARSE_JSON(metadata), '"
                                +CORRECT_LABEL[1]+
                                "') AS numcorrect, "
                               "GET_PATH(PARSE_JSON(metadata), '"
                                +TRIES_LABEL[1]+
                                "') AS numtries, "+
                               "GET_PATH(PARSE_JSON(metadata), '"
                               +PLAY_TIME[1]+
                               "') AS playtime")
        else:
            if SELECTED_LEVEL:
                query1_temp1 = ("user_id, event_created_at, user_level, json_extract(metadata, '$.selected_level') as session_level, score, " 
                               "json_extract(metadata, '$."
                                +CORRECT_LABEL[1]+
                                "') AS numcorrect, "
                               "json_extract(metadata, '$."
                                +TRIES_LABEL[1]+
                                "') AS numtries, "+
                               "json_extract(metadata, '$." 
                               +PLAY_TIME[1]+
                               "') AS playtime")
            else:
                query1_temp1 = ("user_id, event_created_at, user_level, session_level, score, " 
                               "json_extract(metadata, '$."
                                +CORRECT_LABEL[1]+
                                "') AS numcorrect, "
                               "json_extract(metadata, '$."
                                +TRIES_LABEL[1]+
                                "') AS numtries, "+
                               "json_extract(metadata, '$." 
                               +PLAY_TIME[1]+
                               "') AS playtime")




        if VERSION[0]:
            if SNOWFLAKE_FLAG:
                query1_temp1c = (", AS_INTEGER(GET_PATH(PARSE_JSON(metadata) 'version')) AS version"
                + " FROM " + SERVER_QUERY + " "
                           "WHERE game_id= ")
            else:
                query1_temp1c = (", TRY_CAST(json_extract(metadata, '$.version') AS integer) AS version"
                + " FROM " + SERVER_QUERY + " "
                           "WHERE game_id= ")
        else:
            query1_temp1c = (" FROM " + SERVER_QUERY + " "
                       "WHERE game_id= ")
        if not MONTH_COMPARE:
            if YEAR[0] == 'all':
                query1_temp2 =  ("  ")
                query1_temp4 =  ("  " + "GROUP BY user_id HAVING COUNT (user_id) >= ")
            else:
                if not QUOTES:
                    if SNOWFLAKE_FLAG:
                        query1_temp2 =  (" AND ("+ "YEAR(CREATED_AT)=" + " OR  YEAR(CREATED_AT)=".join([str(yy) for yy in YEAR]) +") ")
                        query1_temp4 =  (" AND ("+ "YEAR(CREATED_AT)=" + " OR  YEAR(CREATED_AT)=".join([str(yy) for yy in YEAR]) +") "
                                     + "GROUP BY user_id HAVING COUNT (user_id) >= ")
                    else:
                        query1_temp2 =  (" AND ("+ "yyyy=" + " OR  yyyy=".join([str(yy) for yy in YEAR]) +") ")
                        query1_temp4 =  (" AND ("+ "yyyy=" + " OR  yyyy=".join([str(yy) for yy in YEAR]) +") "
                                     + "GROUP BY user_id HAVING COUNT (user_id) >= ")
                else:
                    if SNOWFLAKE_FLAG:
                        query1_temp2 =  (" AND "+ "YEAR(CREATED_AT)='" + "' OR  YEAR(CREATED_AT)='".join([str(yy) for yy in YEAR]) +"' ")
                        query1_temp4 =  (" AND ("+ "YEAR(CREATED_AT)='" + "' OR  YEAR(CREATED_AT)='".join([str(yy) for yy in YEAR]) +"') "
                                     + "GROUP BY user_id HAVING COUNT (user_id) >= ")
                    else:
                        query1_temp2 =  (" AND "+ "yyyy='" + "' OR  yyyy='".join([str(yy) for yy in YEAR]) +"' ")
                        query1_temp4 =  (" AND ("+ "yyyy='" + "' OR  yyyy='".join([str(yy) for yy in YEAR]) +"') "
                                     + "GROUP BY user_id HAVING COUNT (user_id) >= ")
        else:
            if YEAR[0] == 'all':
                if not QUOTES:
                    if SNOWFLAKE_FLAG:
                        query1_temp2 = ( " AND MONTH(CREATED_AT) = " 
                                    + str(MONTH_COMPARE[1]) + " ")
                        query1_temp4 =  (" AND (MONTH(CREATED_AT)=" 
                                     + str(MONTH_COMPARE[1])  + " " +
                                     ") GROUP BY user_id HAVING COUNT (user_id) >= ")
                    else:
                        query1_temp2 = ( " AND mm = " 
                                    + str(MONTH_COMPARE[1]) + " ")
                        query1_temp4 =  (" AND (mm=" 
                                     + str(MONTH_COMPARE[1])  + " " +
                                     ") GROUP BY user_id HAVING COUNT (user_id) >= ")
                else:
                    if SNOWFLAKE_FLAG:
                        query1_temp2 = ( " AND MONTH(CREATED_AT) = '" 
                                + str(MONTH_COMPARE[1]) + "' ")
                        query1_temp4 =  (" AND (MONTH(CREATED_AT)='" 
                                 + str(MONTH_COMPARE[1])  + "' " +
                                 ") GROUP BY user_id HAVING COUNT (user_id) >= ")   
                    else:
                        query1_temp2 = ( " AND mm = '" 
                                    + str(MONTH_COMPARE[1]) + "' ")
                        query1_temp4 =  (" AND (mm='" 
                                     + str(MONTH_COMPARE[1])  + "' " +
                                     ") GROUP BY user_id HAVING COUNT (user_id) >= ")   
            else:
                if not QUOTES:
                    if SNOWFLAKE_FLAG:
                        query1_temp2 = ( " AND "+ "(YEAR(CREATED_AT)=" + " OR  YEAR(CREATED_AT)=".join([str(yy) for yy in YEAR]) +") AND MONTH(CREATED_AT) = " 
                                        + str(MONTH_COMPARE[0]) + " ")
                        query1_temp4 = ( " AND "+ "(YEAR(CREATED_AT)=" + " OR  YEAR(CREATED_AT)=".join([str(yy) for yy in YEAR]) +") AND MONTH(CREATED_AT) = " 
                                        + str(MONTH_COMPARE[0]) + ") GROUP BY user_id HAVING COUNT (user_id) >= ")
                    else:
                        query1_temp2 = ( " AND "+ "(yyyy=" + " OR  yyyy=".join([str(yy) for yy in YEAR]) +") AND mm = " 
                                    + str(MONTH_COMPARE[0]) + " ")
                        query1_temp4 = ( " AND "+ "(yyyy=" + " OR  yyyy=".join([str(yy) for yy in YEAR]) +") AND mm = " 
                                    + str(MONTH_COMPARE[0]) + ") GROUP BY user_id HAVING COUNT (user_id) >= ")
                else:
                    if SNOWFLAKE_FLAG:
                        query1_temp2 = ( " AND ("+ "YEAR(CREATED_AT)='" + "' OR  YEAR(CREATED_AT)='".join([str(yy) for yy in YEAR]) +"') AND MONTH(CREATED_AT) = '" 
                                    + str(MONTH_COMPARE[0]) + "' ")
                        query1_temp4 =  (" AND ("+ "YEAR(CREATED_AT)='" + "' OR  YEAR(CREATED_AT)='".join([str(yy) for yy in YEAR]) +"') AND (MONTH(CREATED_AT)='" 
                                     + str(MONTH_COMPARE[0])  + "' " +
                                     ") GROUP BY user_id HAVING COUNT (user_id) >= ")
                    else:
                        query1_temp2 = ( " AND ("+ "yyyy='" + "' OR  yyyy='".join([str(yy) for yy in YEAR]) +"') AND mm = '" 
                                    + str(MONTH_COMPARE[0]) + "' ")
                        query1_temp4 =  (" AND ("+ "yyyy='" + "' OR  yyyy='".join([str(yy) for yy in YEAR]) +"') AND (mm='" 
                                     + str(MONTH_COMPARE[0])  + "' " +
                                     ") GROUP BY user_id HAVING COUNT (user_id) >= ")                   
        if USER_EXCLUSION[0]:
            query1_temp3 =  ("AND user_id NOT IN (SELECT DISTINCT(user_id) FROM " +SERVER_QUERY 
                             + " WHERE game_id = " + str(query_params_game_ids[1]) + ") AND "
                             + " user_id IN "
                             + "(SELECT user_id FROM " + SERVER_QUERY + " " 
                             + "WHERE game_id= ")
        else:
            query1_temp3 =  ("AND user_id IN "
                       "(SELECT user_id FROM " + SERVER_QUERY + " "
                       "WHERE game_id= ")

        query1_temp5 =  (")")



        if SNOWFLAKE_FLAG:
            if SELECTED_LEVEL:
                query2_temp1 = ("user_id, event_created_at, user_level, "
                                + "GET_PATH(PARSE_JSON(metadata), 'selected_level') AS session_level, score, " +
                               "GET_PATH(PARSE_JSON(metadata), '"
                                +CORRECT_LABEL[1]+
                                "') AS numcorrect, "
                               "GET_PATH(PARSE_JSON(metadata), '"
                                +TRIES_LABEL[1]+
                                "') AS numtries, "+
                               "GET_PATH(PARSE_JSON(metadata), '"
                               +PLAY_TIME[1]+
                               "') AS playtime")
            else:
                query2_temp1 = ("user_id, event_created_at, user_level, session_level, score, " 
                               "GET_PATH(PARSE_JSON(metadata), '"
                                +CORRECT_LABEL[1]+
                                "') AS numcorrect, "
                               "GET_PATH(PARSE_JSON(metadata), '"
                                +TRIES_LABEL[1]+
                                "') AS numtries, "+
                               "GET_PATH(PARSE_JSON(metadata), '"
                               +PLAY_TIME[1]+
                               "') AS playtime")
        else:
            if SELECTED_LEVEL:
                query2_temp1 = ("user_id, event_created_at, user_level, json_extract(metadata, '$.selected_level') as session_level, score, " 
                               "json_extract(metadata, '$."
                                +CORRECT_LABEL[1]+
                                "') AS numcorrect, "
                               "json_extract(metadata, '$."
                                +TRIES_LABEL[1]+
                                "') AS numtries, "+
                               "json_extract(metadata, '$." 
                               +PLAY_TIME[1]+
                               "') AS playtime")
            else:
                query2_temp1 = ("user_id, event_created_at, user_level, session_level, score, " 
                               "json_extract(metadata, '$."
                                +CORRECT_LABEL[1]+
                                "') AS numcorrect, "
                               "json_extract(metadata, '$."
                                +TRIES_LABEL[1]+
                                "') AS numtries, "+
                               "json_extract(metadata, '$." 
                               +PLAY_TIME[1]+
                               "') AS playtime")


        if VERSION[1]:
            if SNOWFLAKE_FLAG:
                query2_temp1c = (", AS_INTEGER(GET_PATH(PARSE_JSON(metadata) 'version')) AS version"
                + " FROM " + SERVER_QUERY + " "
                           "WHERE game_id= ")
            else:
                query2_temp1c = (", TRY_CAST(json_extract(metadata, '$.version') AS integer) AS version"
                + " FROM " + SERVER_QUERY + " "
                           "WHERE game_id= ")
        else:
            query2_temp1c = (" FROM " + SERVER_QUERY + " "
                       "WHERE game_id= ")
        if not MONTH_COMPARE:
            if YEAR[0] == 'all':
                query2_temp2 =  ("  ")
                query2_temp4 =  ("  " + "GROUP BY user_id HAVING COUNT (user_id) >= ")
            else:
                if not QUOTES:
                    if SNOWFLAKE_FLAG:
                        query2_temp2 =  (" AND ("+ "YEAR(CREATED_AT)=" + " OR  YEAR(CREATED_AT)=".join([str(yy) for yy in YEAR]) +") ")
                        query2_temp4 =  (" AND ("+ "YEAR(CREATED_AT)=" + " OR  YEAR(CREATED_AT)=".join([str(yy) for yy in YEAR]) +") "
                                         + "GROUP BY user_id HAVING COUNT (user_id) >= ")
                    else:
                        query2_temp2 =  (" AND ("+ "yyyy=" + " OR  yyyy=".join([str(yy) for yy in YEAR]) +") ")
                        query2_temp4 =  (" AND ("+ "yyyy=" + " OR  yyyy=".join([str(yy) for yy in YEAR]) +") "
                                     + "GROUP BY user_id HAVING COUNT (user_id) >= ")
                else:
                    if SNOWFLAKE_FLAG:
                        query2_temp2 =  (" AND "+ "(YEAR(CREATED_AT)='" + "' OR  YEAR(CREATED_AT)='".join([str(yy) for yy in YEAR]) +"' )")
                        query2_temp4 =  (" AND ("+ "YEAR(CREATED_AT)='" + "' OR  YEAR(CREATED_AT)='".join([str(yy) for yy in YEAR]) +"') "
                                         + "GROUP BY user_id HAVING COUNT (user_id) >= ")
                    else:
                        query2_temp2 =  (" AND "+ "(yyyy='" + "' OR  yyyy='".join([str(yy) for yy in YEAR]) +"' )")
                        query2_temp4 =  (" AND ("+ "yyyy='" + "' OR  yyyy='".join([str(yy) for yy in YEAR]) +"') "
                                     + "GROUP BY user_id HAVING COUNT (user_id) >= ")
        else:
            if YEAR[0] == 'all':
                if not QUOTES:
                    query2_temp2 = ( " AND mm = " 
                                + str(MONTH_COMPARE[1]) + " ")
                    query2_temp4 =  (" AND (mm=" 
                                 + str(MONTH_COMPARE[1])  + " " +
                                 ") GROUP BY user_id HAVING COUNT (user_id) >= ")
                else:
                    query2_temp2 = ( " AND mm = '" 
                                + str(MONTH_COMPARE[1]) + "' ")
                    query2_temp4 =  (" AND (mm='" 
                                 + str(MONTH_COMPARE[1])  + "' " +
                                 ") GROUP BY user_id HAVING COUNT (user_id) >= ")                    
            else:
                if not QUOTES:
                    if SNOWFLAKE_FLAG:
                        query2_temp2 = ( " AND "+ "(YEAR(CREATED_AT)=" + " OR  YEAR(CREATED_AT)=".join([str(yy) for yy in YEAR]) +") AND MONTH(CREATED_AT) = " 
                                        + str(MONTH_COMPARE[1]) + " ")
                        query2_temp4 =  (" AND ("+ "YEAR(CREATED_AT)=" + " OR  YEAR(CREATED_AT)=".join([str(yy) for yy in YEAR]) +") AND (MONTH(CREATED_AT)=" 
                                         + str(MONTH_COMPARE[1])  + " " +
                                         ") GROUP BY user_id HAVING COUNT (user_id) >= ")
                    else:
                        query2_temp2 = ( " AND "+ "(yyyy=" + " OR  yyyy=".join([str(yy) for yy in YEAR]) +") AND mm = " 
                                        + str(MONTH_COMPARE[1]) + " ")
                        query2_temp4 =  (" AND ("+ "yyyy=" + " OR  yyyy=".join([str(yy) for yy in YEAR]) +") AND (mm=" 
                                         + str(MONTH_COMPARE[1])  + " " +
                                         ") GROUP BY user_id HAVING COUNT (user_id) >= ")
                else:
                    if SNOWFLAKE_FLAG:
                        query2_temp2 = ( " AND "+ "(YEAR(CREATED_AT)='" + "' OR  YEAR(CREATED_AT)='".join([str(yy) for yy in YEAR]) +"') AND MONTH(CREATED_AT) = '" 
                                        + str(MONTH_COMPARE[1]) + "' ")
                        query2_temp4 =  (" AND ("+ "YEAR(CREATED_AT)='" + "' OR  YEAR(CREATED_AT)='".join([str(yy) for yy in YEAR]) +"') AND (MONTH(CREATED_AT)='" 
                                         + str(MONTH_COMPARE[1])  + "' " +
                                         ") GROUP BY user_id HAVING COUNT (user_id) >= ")
                    else:
                        query2_temp2 = ( " AND "+ "(yyyy='" + "' OR  yyyy='".join([str(yy) for yy in YEAR]) +"') AND mm = '" 
                                        + str(MONTH_COMPARE[1]) + "' ")
                        query2_temp4 =  (" AND ("+ "yyyy='" + "' OR  yyyy='".join([str(yy) for yy in YEAR]) +"') AND (mm='" 
                                         + str(MONTH_COMPARE[1])  + "' " +
                                         ") GROUP BY user_id HAVING COUNT (user_id) >= ")
        if USER_EXCLUSION[1]:
            query2_temp3 =  ("AND user_id NOT IN (SELECT DISTINCT(user_id) FROM " + SERVER_QUERY 
                             + " WHERE game_id = " + str(query_params_game_ids[0]) + ") AND "
                             + " user_id IN "
                             + "(SELECT user_id FROM " + SERVER_QUERY + " " 
                             + "WHERE game_id= ")
        else:
            query2_temp3 =  ("AND user_id IN "
                       "(SELECT user_id FROM " + SERVER_QUERY + " "
                       "WHERE game_id= ")
        query2_temp5 =  (")")


        #make query
        if QUOTES:
            punctuation = "'" #DEFUNCT TAG, was for when serve was temporarily outputting strings, WOULD HAVE TO UPDATE
        else:
            punctuation = ""
        query1 = (query1_temp0 + query1_temp1+ query1_temp1b+ query1_temp1c
                 +punctuation+str(query_params_game_ids[0]) + punctuation
                 +query1_temp2 +query1_temp3
                 +punctuation+str(query_params_game_ids[0]) + punctuation
                 +query1_temp4
                 +str(query_params_user_plays) + query1_temp5 + query1_temp6)
        query2 = (query2_temp0 + query2_temp1+ query2_temp1b+ query2_temp1c
                 +punctuation+str(query_params_game_ids[1]) + punctuation
                 +query2_temp2 +query2_temp3
                 +punctuation+str(query_params_game_ids[1]) + punctuation
                 +query2_temp4
                 +str(query_params_user_plays) + query2_temp5 + query2_temp6)
        return query1,query2

def runPythena(query_params_game_ids =[180, 181],
               FIT_TEST =False,
               reportName = 'defaultName',
               TIME_FILTER = [0,0], #set this to standard deviation exclusion; 2 means >2 stdevs will be excluded on Time plots
               MONTH_COMPARE = [], #compare different months (usually to test on same game_id)
               VERSION = [None, None],
               query_params_user_plays = 20,
               #Remove play sessions with very low number of words, remove single session users
               NUMSESSIONS = 20,
               NUM_SESSIONS_FOR_CONVERGENCE = 3,
               EARLY_LEARNING_FILTER = [0.05, 20],
               NUMMISSINGITEMS = -1, # -1 to include all; 0 to include only game_sessions with max items; 1 to include game sessions only missing 1, etc
               MINPERFORMANCE = -1,
               OUTLIERFILTER = 0, #simple filter; for now, excludes anything outside +/- 3 standard deviations; set to 0 to turn off
               #parameter for convergence examination (should be less or equal to sessions filter above)
               SESSION_FILTER = 5, #Less than this many sessions for that session level will negate datapoint for that session (for debug modes or fake data)
               #Fetches  game_id table regardless if its on the local store already -- (i.e. force an update)
               #otherwise, if false, it will only query, fetch query, and download if its missing
               PLOT_OUTLIER_FILTER = 3,
               DISTRIB_FILTER = 20,
               DOWNLOAD_OVERRIDE = True,
               DOWNSAMPLE = 1000,
               CORRECT_LABEL =['num_correct','num_correct'], #summary (not header) label in metadata for correct
               TRIES_LABEL = ['num_tries','num_tries'], #summary label for tries in metadata
               PLAY_TIME = ['playTime','playTime'], #default labels for 'playTime' in metadata
               QUERY_WINDOW = '', #day, week, month, second
               QUERY_AMOUNT = [1,1], #default is 0, which is 'last day, week, etc'; 1 means last 2, 2 last 3, etc
               QUERY_AMOUNT_LOWER_LIMIT = [0,0],  #default is 0, which is everything newer or as new as 0 (most recent)
               YEAR = ['all','all'], #[0,0] to query this year for each query, [2017, 2018] for 2017 + 2018 etc
               TUTORIAL_PLOTS = True,
               USER_EXCLUSION = [False, False], #set to True to exclude all users that have played both game_ids listed
               SESSION_EXCLUSION = [True, True], #set to True to exclude sessions from each other that have played both game_ids listed
               SERVER_QUERY = 'views.gamesave', #SERVER_QUERY = 'views.gamesaves',
               QUOTES = False,   #
               LABELS = ['',''],
               USER_TRUE_SESSION = [False, False],
               SNOWFLAKE_FLAG = True, #enables SNOWFLAKE query generator and server access
               SELECTED_LEVEL = False, #enable to replace 'session level' with 'selected level'
               AWS_LAMBDA = False   #will try to implement unified code (putting logic statements to exclude imports, changing directory paths, etc, required for AWS Lambda)
              ): 

    #MAIN CODE STARTS HERE:
    
    print('Importing the libraries\n')
    import textwrap
    import pandas as pd
    import datetime 
    import numpy as np
    if SNOWFLAKE_FLAG:
        import snowflake.connector
    else:
        import boto3
    
    #deprecated parameter, logic fix here
    if YEAR == None:
        YEAR = ['all','all']
    if YEAR[0] == 0:
        YEAR[0] = datetime.datetime.now().year
    if YEAR[1] == 0:
        YEAR[1] = datetime.datetime.now().year
    if isinstance(USER_TRUE_SESSION, bool): #if set to True --> set to [True, True]
        USER_TRUE_SESSION = [USER_TRUE_SESSION,USER_TRUE_SESSION]
    
    if SESSION_EXCLUSION[0]:
        #set user true session on if using user exclusion
        #user exclusion counts sessions across both games, True session counts session# by events table
        USER_TRUE_SESSION[0] = True
    if SESSION_EXCLUSION[1]:
        USER_TRUE_SESSION[1] = True
            
    #can't be a function parameter and be declared global
    global AWSLambda
    AWSLambda = AWS_LAMBDA
    global SnowflakeFlag
    SnowflakeFlag = SNOWFLAKE_FLAG

    #too lazy to fix, flip it here
    FIT_TEST = not FIT_TEST

    # Create the PdfPages object to which we will save the pages:
    # The with statement makes sure that the PdfPages object is closed properly at
    # the end of the block, even if an Exception occurs.
    from matplotlib.backends.backend_pdf import PdfPages
    if AWSLambda:
        path_addon = '/tmp/'
    else:
        path_addon = ''

    with PdfPages(path_addon + reportName + '.pdf') as pdf:
        def addTopText(text,fig):
            #add text box to current figure
            axExtra = fig.add_axes((.1,.9,.8,1))
            axExtra.set_axis_off()
            fig.text(.1, .95, 
                text,
                fontsize=30)

        def textBox(text):
            #separate text box
            LINE_LENGTH = 100 #line wrap char length
#             text = text.replace('\n', ' '*LINE_LENGTH) 
            import textwrap
            import matplotlib.pyplot as plt
            text="\n".join(textwrap.wrap(text,LINE_LENGTH, drop_whitespace=False,
                                         replace_whitespace=False))
            text=text.replace("$","\$")
            fig = plt.figure(figsize=(40,40))
            ax = fig.add_axes([0,0,1,1])
            ax.text(0.1, 0.95, text,
                horizontalalignment='left',
                verticalalignment='top',
                fontsize=40, color='black',
                transform=ax.transAxes)
            pdf.savefig(fig)

        fetchGameNames(DOWNLOAD_OVERRIDE) #set to True if you want to override and update local store of  Games

        #define database to query, and s3 location for query dump
        database = "views"
        bucketrino = 'aws-athena-query-results-XXXXXXXXXXX' 
        locationrino = 'Unsaved/AcrossGamesAnalysis'


        if SnowflakeFlag:
            import snowflake.connector
            ctx = snowflake.connector.connect(
                user='datascience_etl',
                password=keycode(),
                account='labs.us-east-1'
                )
            cs = ctx.cursor()
        elif not SnowflakeFlag:
            #low level client representing Athena, to check on queries
            client = boto3.client('athena')
            queryStore =[]
            filenamerinos=[]
            filerinos=[]

        import boto3
        s3 = boto3.resource('s3')
        s3_output = 's3://'+bucketrino+'/'+locationrino+'/'

        #generate SQl queries
        query1,query2 = sql_generator(QUERY_WINDOW,USER_TRUE_SESSION,QUERY_AMOUNT,QUOTES,query_params_user_plays,
                        QUERY_AMOUNT_LOWER_LIMIT,query_params_game_ids,
                        CORRECT_LABEL,TRIES_LABEL,PLAY_TIME,SERVER_QUERY,MONTH_COMPARE,VERSION,YEAR,
                        USER_EXCLUSION,SESSION_EXCLUSION,SELECTED_LEVEL,SNOWFLAKE_FLAG)
        
        #attempt ot add query to metadata page (first page)
        if SnowflakeFlag:
            querySystemString = "(FOR REPRODUCTION IN SNOWFLAKE)"
        if not SnowflakeFlag:
            querySystemString = "(FOR REPRODUCTION IN ATHENA)"

        textBox("PYTHENA GAME FITNESS REPORT METADATA"
               + "\n\nREPORT: " + str(reportName)
               + "\n\nTIMESTAMP OF REPORT GENERATION: " + str(datetime.datetime.now())
               + "\n\n\n GENERATED SQL QUERY #1 "+ querySystemString + ": \n" + query1  
               + "\n\n GENERATED SQL QUERY #2 " + querySystemString + ": \n" + query2
               + "\n\n REPORT PARAMTERS USED: \n" 
                #normally, would import "inspect" module, but AWS Lambda limits, I"ll just list manually
               +" query_params_game_ids = " + str(query_params_game_ids)
               +", FIT_TEST = " +str(FIT_TEST)
               +", reportName = "  + str(reportName)
               +", TIME_FILTER = " + str(TIME_FILTER)
               +", MONTH_COMPARE = " + str(MONTH_COMPARE)
               +", VERSION = " + str(VERSION)
               +", query_params_user_plays = " + str(query_params_user_plays)
               +", NUMSESSIONS = " + str(NUMSESSIONS)
               +", NUM_SESSIONS_FOR_CONVERGENCE = " +str(NUM_SESSIONS_FOR_CONVERGENCE)
               +", EARLY_LEARNING_FILTER = " +str(EARLY_LEARNING_FILTER)
               +", NUMMISSINGITEMS = " + str(NUMMISSINGITEMS)
               +", MINPERFORMANCE = " + str(MINPERFORMANCE)
               +", OUTLIERFILTER = " + str(OUTLIERFILTER)
               +", SESSION_FILTER = " + str(SESSION_FILTER)
               +", PLOT_OUTLIER_FILTER = " + str(PLOT_OUTLIER_FILTER)
               +", DISTRIB_FILTER = " + str(DISTRIB_FILTER)
               +", DOWNLOAD_OVERRIDE = " + str(DOWNLOAD_OVERRIDE)
               +", DOWNSAMPLE = " + str(DOWNSAMPLE)
               +", CORRECT_LABEL = " + str(CORRECT_LABEL)
               +", TRIES_LABEL = " + str(TRIES_LABEL)
               +", PLAY_TIME = " + str(PLAY_TIME)
               +", QUERY_WINDOW = " + str(QUERY_WINDOW)
               +", QUERY_AMOUNT = " + str(QUERY_AMOUNT)
               +", QUERY_AMOUNT_LOWER_LIMIT = " + str(QUERY_AMOUNT_LOWER_LIMIT)
               +", YEAR = " + str(YEAR)
               +", TUTORIAL_PLOTS = " + str(TUTORIAL_PLOTS)
               +", USER_EXCLUSION = " + str(USER_EXCLUSION)
               +", SESSION_EXCLUSION = " + str(SESSION_EXCLUSION)
               +", SERVER_QUERY = " + str(SERVER_QUERY)
               +", QUOTES = " + str(QUOTES)
               +", LABELS = " + str(LABELS)
               +", USER_TRUE_SESSION = " + str(USER_TRUE_SESSION)
               +", SELECTED_LEVEL = " + str(SELECTED_LEVEL)
               +", AWS_LAMBDA = " + str(AWS_LAMBDA)
               )

        #attempt query or queries
        print("Query #1: " + query1 + " \nQuery #2: " + query2)
        

        #old AWS Athena query sytem
        if not SnowflakeFlag:
            print('\nQUERY (THROUGH ATHENA) REPORT: \n')
            import time
            start = time.time()
            print("Running query: %i" %(1))
            res = run_query(query1,database,s3_output)
            filenamerino = res['QueryExecutionId']+'.csv'
            filenamerinos.append(filenamerino)
            filerino = locationrino+'/'+filenamerino
            filerinos.append(filerino)
            queryStore.append(res['QueryExecutionId'])

            print("Running query: %i" %(2))
            res = run_query(query2,database,s3_output)
            filenamerino = res['QueryExecutionId']+'.csv'
            filenamerinos.append(filenamerino)
            filerino = locationrino+'/'+filenamerino
            filerinos.append(filerino)
            queryStore.append(res['QueryExecutionId'])

            #pause for some arbitrary amount of time
            import time
            time.sleep(5)


            #store all responses for all queries    
            responses=[]
            is_queries_running=[]
            #can fetch layers of info about query
            for query_num in range(len(query_params_game_ids)):
                response = client.batch_get_query_execution(
                    QueryExecutionIds=[queryStore[query_num]])
                #store full response, and just completion status
                responses.append(response) 
                is_queries_running.append(response['QueryExecutions'][0]['Status']['State']=='RUNNING')

            print('Queries running?'+str(is_queries_running) + '  \n' +
                  str(np.sum([not i for i in is_queries_running])) +' queries completed out of ' + str(len(is_queries_running)) + ' queries')
            #response = checkQueries(is_queries_running,queryStore,responses,0)

            #is_queries_finished=[]
            #for elem in responses:
            #    is_queries_finished.append(elem['QueryExecutions'][0]['Status']['State']=='SUCCEEDED')

            print("Waiting for queries to finish... \n")
            sleepytime=5
            while (np.sum([elem['QueryExecutions'][0]['Status']['State']=='RUNNING' for elem in responses]) > 0):
                sleepytime += sleepytime
                print('Waiting... ')
                time.sleep(sleepytime)
                is_queries_running=[]
                responses=[]    #blank this, so we can update responses with server check
                #can fetch layers of info about query
                for query_num in range(len(query_params_game_ids)):
                    response = client.batch_get_query_execution(
                        QueryExecutionIds=[queryStore[query_num]])
                    #store full response, and just completion status
                    responses.append(response) 
                    is_queries_running.append(response['QueryExecutions'][0]['Status']['State']!='SUCCEEDED')

            print('Queries running?'+str(is_queries_running))
            print([str(sum([not i for i in is_queries_running])) +' queries completed out of ' + str(len(is_queries_running)) + ' queries'])
            print('Current Status:' + str([elem['QueryExecutions'][0]['Status']['State'] for elem in responses]) + '    \n')
            print(str(time.time()-start) + " seconds to run queries.")


            print('DATA FETCH (FROM S3) REPORT: \n')
            #If there, get it
            for query_num in range(len(query_params_game_ids)):
                if response['QueryExecutions'][0]['Status']['State'] == 'SUCCEEDED':
                    print('Now fetching query #' + str(query_num+1) + ' which is game_id #'
                          +str(query_params_game_ids[query_num]))
                    #check to see if query is there
                    listContents(bucketrino,filerinos[query_num])

                    #download it
                    readContents(bucketrino,locationrino,filenamerinos[query_num])

            print('\nDATA CLEANING REPORT: \n')
            for query_num in range(len(query_params_game_ids)):
                if response['QueryExecutions'][0]['Status']['State'] == 'SUCCEEDED':
                    print('Now fetching, downloading, cleaning query #' + str(query_num+1) + ' which is game_id #'
                          +str(query_params_game_ids[query_num]))

                    #Remove play sessions with very low number of words, remove single session users
                    cleanData(NUMMISSINGITEMS, MINPERFORMANCE, NUMSESSIONS, OUTLIERFILTER, SESSION_FILTER, USER_TRUE_SESSION, filenamerinos[query_num])




        #submit through Snowflake system
        if SNOWFLAKE_FLAG:
            #!/usr/bin/env python
            import snowflake.connector
            import numpy as np
            import csv as csv
            import pandas as pd

            # Gets the version
            ctx = snowflake.connector.connect(
                user='datascience_etl',
                password=keycode(),
                account='labs.us-east-1'
                )
            NUMBER_OF_ROWS_LIMIT= 1000000

            #store all responses for all queries    
            print('\nQUERY (THROUGH SNOWFLAKE) REPORT: \n')
            import time
            start = time.time()

            try:
                filenamerinos=[]
                filerinos=[]
                print("Running query: %i" %(1))
                cs = ctx.cursor()
                cur=cs.execute(query1)
                grab_rows = cs.fetchmany(NUMBER_OF_ROWS_LIMIT)
                print('rows fetched: ', np.shape(grab_rows))
                filenamerino = 'QueryExecutionId'+str(1)+'.csv'
                filenamerinos.append(filenamerino)
                filerino = locationrino+'/'+filenamerino
                filerinos.append(filerino)
                # write out
                df = pd.DataFrame.from_records(grab_rows, columns=[x[0].lower() for x in cur.description])
                if AWS_LAMBDA:
                    print('writing out...')
                    df.to_csv('/tmp/' + filenamerino)
                else:
                    print('writing out...')
                    df.to_csv(filenamerino)
                cs.close()

                print("Running query: %i" %(2))
                cs = ctx.cursor()
                cur=cs.execute(query2)
                grab_rows = cs.fetchmany(NUMBER_OF_ROWS_LIMIT)
                print('rows fetched: ', np.shape(grab_rows))
                filenamerino = 'QueryExecutionId'+str(2)+'.csv'
                filenamerinos.append(filenamerino)
                filerino = locationrino+'/'+filenamerino
                filerinos.append(filerino)
                # write out
                df = pd.DataFrame.from_records(grab_rows, columns=[x[0].lower() for x in cur.description])
                if AWS_LAMBDA:
                    print('writing out...')
                    df.to_csv('/tmp/' + filenamerino)
                else:
                    print('writing out...')
                    df.to_csv(filenamerino)
                cs.close()
            finally:
                ctx.close()

            print('done')

            print(str(time.time()-start) + " seconds to run queries.")


            print('\nDATA CLEANING REPORT: \n')
            for query_num in range(len(query_params_game_ids)):
                print('Now cleaning query #' + str(query_num+1) + ' which is game_id #'
                      +str(query_params_game_ids[query_num]))

                #Remove play sessions with very low number of words, remove single session users
                cleanData(NUMMISSINGITEMS, MINPERFORMANCE, NUMSESSIONS, OUTLIERFILTER, SESSION_FILTER, USER_TRUE_SESSION, filenamerinos[query_num])


        #query end
        print('\n\n\n\n Analysis and descriptive report of game data. \n')

        if TUTORIAL_PLOTS:
            #plot sample overlaps for stats noobs
            import matplotlib.pyplot as plt
            import matplotlib
            import textwrap
            print('Below is fake data to demonstrate examples of similar and disimilar data (overlap, tolerances, etc):\n')
            fig,ax = plt.subplots(nrows = 3,figsize=(40,30), ncols = 3) #ADD MORE HERE
            matplotlib.rcParams.update({'font.size': 32})
            matplotlib.rc('xtick', labelsize=32) 
            matplotlib.rc('ytick', labelsize=32) 
            row = 0
            for i in [0,0.5,2]:
                EXTRA = i
                # generate 3 sets of random means and confidence intervals to plot
                indx=list(range(50))
                mean0 = np.sort(np.random.random(50))
                ub0 = np.random.random(50)

                mean1 = np.sort(np.random.random(50)) + EXTRA
                ub1 = np.random.random(50)

                # plot the data
                plot_mean_and_CI(indx, mean0, ub0, ax[row][0], color_mean='g', color_shading='g', label='Game 1')
                plot_mean_and_CI(indx, mean1, ub1, ax[row][1], color_mean='b', color_shading='b', label='Game 2') #'g'
                plot_mean_and_CI(indx, mean0, ub0, ax[row][2], color_mean='g', color_shading='g', label='Game 1')
                plot_mean_and_CI(indx, mean1, ub1, ax[row][2], color_mean='b', color_shading='b', label='Game 2') #'g'
                ax[row][0].set_title('Fake data example #1')
                ax[row][1].set_title('Fake data example #2')

                if i == 0:
                    ax[row][2].set_title("\n".join(textwrap.wrap('Overlapping -- these are the same',40)))
                elif i ==2:
                    ax[row][2].set_title('These are NOT the same')
                elif i ==0.5:
                    ax[row][2].set_title("\n".join(textwrap.wrap('These are similar -- some levels within tolerance, overall difference',40)))
                row+=1


            plt.show()
            pdf.savefig(fig)

        print('Retrieving game names from  games database..')
        if AWSLambda:
            data1 = pd.read_csv('/tmp/'+filenamerinos[0])
            data2 = pd.read_csv('/tmp/'+filenamerinos[1])
            data = pd.read_csv('/tmp/'+'Games.dat')
        else:
            data1 = pd.read_csv(filenamerinos[0])
            data2 = pd.read_csv(filenamerinos[1])
            data = pd.read_csv('Games.dat')
        gamename1 = data[data['id']==query_params_game_ids[0]]['name'].item() #name, lpi_release_date, updated_at, lpi_game_version
        gamename2 = data[data['id']==query_params_game_ids[1]]['name'].item() #name, lpi_release_date, updated_at, lpi_game_version

        print('\nDESCRIPTIVE AND GRAPHICAL REPORT:\n')
        #15 rows of plots: usr beh, usr-game beh, usr perf, score sys, level sys
        #col 1, game 1, col2 game 2, col 3 overlap
    #    fig,ax = plt.subplots(nrows = 15*len(query_params_game_ids),figsize=(30,5*20), ncols = 3)

        #for query_num in range(len(query_params_game_ids)):
        #plot each in the correct column: #ax[0][col 1-3].plot(x,y)
        fullReport(filenamerinos, reportName, pdf, FIT_TEST, TIME_FILTER, NUM_SESSIONS_FOR_CONVERGENCE, DISTRIB_FILTER, PLOT_OUTLIER_FILTER,
                   EARLY_LEARNING_FILTER, LABELS, DOWNSAMPLE,
                   query_params_game_ids)


        for query_num in range(len(query_params_game_ids)):
            #clean-up
            cleanUp(filenamerinos[query_num])

    #end pdf Report -- writes out locally here
    
    writeS3report(gamename1, gamename2, reportName = reportName+'.pdf') #this writes to S3

def writeS3report(gamename1, gamename2, reportName = 'defaultReport.pdf', database = "views", 
             bucket = 'pythena-reports',
             location1 = 'Pythena_Reports', location2 = 'Pythena_Reports/PythenaReportsBackup'
            ):            
            #  bucket = 'aws-athena-query-results-447374670232-us-east-1',
            #  location1 = 'Unsaved/PythenaReportsBackup/CurrentReports', location2 = 'Unsaved/PythenaReportsBackup'

    #paramters to write out report to S3
    import boto3
    import numpy as np
    import time
    #define database to query, and s3 location for query dump
    s3 = boto3.resource('s3')

    #define subfolder name
    if gamename1 == gamename2:
        subfolder_name = gamename1
    else:
        subfolder_name = gamename1 + 'VS' + gamename2
    subfolder_name = subfolder_name.replace(" ", "") #get rid of spaces for AWS S3 Location

    if not AWSLambda: #if I'm running it locally on my machine, just write a backup report on S3 -- don't replace view copy
        #write out report
        print('Writing out to S3.\n')
        objectKey = location2 + '/'  + subfolder_name + '/' + reportName[:-4] + '_' + str(round(time.time()))+ reportName[-4:]
        s3.Bucket(bucket).put_object(Key= objectKey, Body=open(reportName, 'rb'))
        #Make readable by teams, by changing it to public
        object_acl = s3.Object(bucket,objectKey)
        object_acl.Acl().put(ACL='public-read')
        print('Making S3 object available at ' +  objectKey + '.\n')
    else:
        #write out backup report
        objectKey = location2 + '/' + subfolder_name + '/' + reportName[:-4] + '_' + str(round(time.time()))+ reportName[-4:]
        print('Writing out '+ objectKey +' (timestamped) to S3.\n')
        s3.Bucket(bucket).put_object(Key= objectKey, Body=open('/tmp/' + reportName, 'rb'))
        try:
            #Make readable by teams, by changing it to public
            object_acl = s3.Object(bucket,objectKey)
            object_acl.Acl().put(ACL='public-read')
            print('Making S3 object available at ' +  objectKey + '.\n')
        except:
            print('Object ACL fetch or put failed.')
        
        #write out report
        objectKey = location1 + '/' +  reportName
        print('Writing out '+ objectKey +' to S3.\n')
        s3.Bucket(bucket).put_object(Key= objectKey, Body=open('/tmp/' + reportName, 'rb'))
        try:
            #Make readable by teams, by changing it to public
            object_acl = s3.Object(bucket,objectKey)
            object_acl.Acl().put(ACL='public-read')
            print('Making S3 object available at ' +  objectKey + '.\n')
        except:
            print('Object ACL fetch or put failed.')
        

