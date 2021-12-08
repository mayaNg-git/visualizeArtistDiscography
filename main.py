import requests
import base64
import pandas as pd
import cx_Oracle
import matplotlib.pyplot as plt
import seaborn as sns

authHeader = {}
authData = {}
clientID = "3420ab2affe9452e95dc6481b4795d00"
clientSecret = "b31f3c67b63246d3bd3a597c05fdc3e8"
authUrl = "https://accounts.spotify.com/api/token"
BASE_URL = 'https://api.spotify.com/v1/'


# Base64 encoded Client ID and Client Secret
def getAccessToken(clientID, clientSecret):
    message = f"{clientID}:{clientSecret}"
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    authHeader['Authorization'] = "Basic " + base64_message
    authData['grant_type'] = "client_credentials"
    res = requests.post(authUrl, headers=authHeader, data=authData)

    responseObject = res.json()
    accessToken = responseObject['access_token']

    return accessToken


def getAllArtistTracks(token, artist_id):
    headers = {
        "Authorization": "Bearer " + token
    }

    # pull all artists albums
    r = requests.get(BASE_URL + 'artists/' + artist_id + '/albums',
                     headers=headers,
                     params={'include_groups': 'album', 'limit': 50})
    d = r.json()

    albums = []  # to keep track of duplicates

    # loop over albums and get all tracks
    for album in d['items']:
        album_name = album['name']

        # here's a hacky way to skip over albums we've already grabbed
        trim_name = album_name.split('(')[0].strip()
        if trim_name.upper() in albums or int(album['release_date'][:4]) > 1983:
            continue
        albums.append(trim_name.upper())  # use upper() to standardize

        # this takes a few seconds so let's keep track of progress

        # pull all tracks from this album
        r = requests.get(BASE_URL + 'albums/' + album['id'] + '/tracks',
                         headers=headers)
        tracks = r.json()['items']

        for track in tracks:
            audioFeature = getAudioFeatures(token, track, album)
            # storing to Oracle
            insertToDatabase(audioFeature)


def getAudioFeatures(token, trackInfo, albumInfo):
    headers = {
        "Authorization": "Bearer " + token
    }

    res = requests.get(BASE_URL + 'audio-features/' + trackInfo['id'], headers=headers)
    jsonData = res.json()
    jsonData['audio_mode'] = jsonData.pop('mode')
    # combine with album info
    jsonData.update({
        'track_name': trackInfo['name'],
        'album_name': albumInfo['name'],
        'short_album_name': albumInfo['name'].split('(')[0].strip(),
        'release_date': albumInfo['release_date'],
        'album_id': albumInfo['id']
    })

    return jsonData


def insertToDatabase(data):
    keys = tuple(data)
    dictsize = len(data)
    sql = ''
    headers = '('
    for i in range(dictsize):
        headers += keys[i]
        if i < dictsize - 1:
            headers += ', '

    headers += ')'

    for i in range(dictsize):
        if type(data[keys[i]]).__name__ == 'str':
            sql += '\'' + str(data[keys[i]]).replace("'", "") + '\''
        else:
            sql += str(data[keys[i]]).replace("'", "")
        if i < dictsize - 1:
            sql += ', '

    query = "INSERT INTO SPOTIFY_AUDIO_FEATURES" + str(headers) + " values (" + sql + ")"
    print(query)
    try:
        with conn.cursor() as cursor:
            # execute the insert statement
            cursor.execute(query)
            # commit work
            conn.commit()
            print('Inserted successfully')
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)


# Create Oracle dababase connection
dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='oracle')
conn = cx_Oracle.connect(user='system', password='nyOracle123', dsn=dsn_tns)

cursor = conn.cursor()
# API requests
token = getAccessToken(clientID, clientSecret)

getAllArtistTracks(token, '36QJpDe2go2KgaRleHCDTp')


# Retrieve json data from Oracle and pass it to DataFrame
query = 'SELECT * FROM SPOTIFY_AUDIO_FEATURES'
df = pd.read_sql(query, con=conn)

# Visualize Data

plt.figure(figsize=(10, 10))
ax = sns.scatterplot(data=df, x='VALENCE', y='ACOUSTICNESS',
                     hue='SHORT_ALBUM_NAME', palette='rainbow',
                     size='DURATION_MS', sizes=(10, 200),
                     alpha=0.7)

h, labs = ax.get_legend_handles_labels()
ax.legend(h[1:10], labs[1:10], loc='best', title=None)
plt.show()
