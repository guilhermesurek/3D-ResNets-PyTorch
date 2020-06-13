import urllib3, json

url = "http://ec2-52-25-205-214.us-west-2.compute.amazonaws.com/files/activity_net.v1-3.min.json"
response = urllib3.PoolManager().request('GET', url).data
data = json.loads(response)

with open('activity_net.v1-3.min.json', 'w') as outfile:
    json.dump(data, outfile)