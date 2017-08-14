from google import auth
from google.google_analytics.ga_model import Property, View

# Define the auth scopes to request.
scope = ['https://www.googleapis.com/auth/analytics.readonly']

# Authenticate and construct service.
service = auth.get_service('analytics', 'v4')

def get_GA_results(config, pageToken=0):
    # Use the Analytics Service Object to query the Core Reporting API
    # for the number of sessions in the past seven days.
    if config.filters == '':

        data = service.reports().batchGet(
                 body={
                   'reportRequests':[
                   {
                       'viewId': config.ids,
                       'dateRanges': [{'startDate': config.start_date, 'endDate': config.end_date}],
                       'metrics': config.metrics,
                       'dimensions': config.dimensions,
                       'orderBys': config.orderBys,
                       'samplingLevel': 'LARGE',
                       'pageSize': 10000,
                       'pageToken': str(pageToken)
                   }]
                 }
               ).execute()
    else:
        data = service.reports().batchGet(
                 body={
                   'reportRequests':[
                   {
                       'viewId': config.ids,
                       'dateRanges': [{'startDate': config.start_date, 'endDate': config.end_date}],
                       'metrics': config.metrics,
                       'dimensions': config.dimensions,
                       'orderBys': config.orderBys,
                       'samplingLevel': 'LARGE',
                       'pageSize': 10000,
                       'pageToken': str(pageToken)
                   }]
                 }
               ).execute()
    return data

def dumpData(View):
    BulkDATA = list()
    nextPageToken = 0
    while nextPageToken != None:
        DATA = get_GA_results(View, nextPageToken)
        try:
            nextPageToken = DATA.get('reports').get('nextPageToken')
        except:
            nextPageToken = None
        BulkDATA.append(DATA)
    return BulkDATA
