import requests, os
from requests.auth import HTTPDigestAuth
for i in range(1,90):
    url = 'https://downloads.flickr.com/d/data_42999161_438d80197d9c8ecbaf254b6001dc0635ae2024e3c2f00aa8837c4580b05b35e8_' + str(i) + '.zip'
    # url = 'https://s3.amazonaws.com/flickr-metadump-us-east-1/72157721465842094_ee435dbbde22_part1.zip'
    print('Downloading from: ' + url)
    response = requests.get(url, auth=HTTPDigestAuth('andrewcottam36@yahoo.com','thargaL88$'))
    filename = "flickr-photos-part-" + str(i) + ".zip" 
    with open(filename, "wb") as f:
        f.write(response.content)
    print('Moving to Google Cloud Storage')
    os.system('gcloud storage cp /Users/andrewcottam/Documents/GitHub/gcloud-library/' + filename + ' gs://andrewcottam-default-flickr-photos/' + filename)
    break