import csv
import json
import time
import tweepy


# You must use Python 2.7.x
# Rate limit chart for Twitter REST API - https://dev.twitter.com/rest/public/rate-limits

def loadKeys(key_file):
    # TODO: put your keys and tokens in the keys.json file,
    #       then implement this method for loading access keys and token from keys.json
    # rtype: str <api_key>, str <api_secret>, str <token>, str <token_secret>

    # Load keys here and replace the empty strings in the return statement with those keys
	with open('keys.json') as file:
		key = json.load(file)
	api_key = str(key["api_key"])
	api_secret = str(key["api_secret"])
	token = str(key["token"])
	token_secret = str(key["token_secret"])
	file.close()
	return api_key,api_secret,token,token_secret

# Q1.b.(i) - 5 points
def getPrimaryFriends(api, root_user, no_of_friends):
    # TODO: implement the method for fetching 'no_of_friends' primary friends of 'root_user'
    # rtype: list containing entries in the form of a tuple (root_user, friend)
	primary_friends = []
    # Add code here to populate primary_friends
	user = api.get_user(root_user)
	try:
		friend = user.friends()
	except tweepy.error.RateLimitError:
		print("Rate Limit Error happens. Sleep 15 minutes.")
		time.sleep(60 * 15)
		friend = user.friends()
	except tweepy.error.TweepError:
		print("Tweep Error happens. Can't get primary friends.")
	length = min(no_of_friends, len(friend))
	for i in range(length):
		temp = (root_user, friend[i].screen_name)
		primary_friends.append(temp)
	return primary_friends

# Q1.b.(ii) - 7 points
def getNextLevelFriends(api, friends_list, no_of_friends):
    # TODO: implement the method for fetching 'no_of_friends' friends for each entry in friends_list
    # rtype: list containing entries in the form of a tuple (friends_list[i], friend)
    next_level_friends = []
    # Add code here to populate next_level_friends
    for friend in friends_list:
    	temp = getPrimaryFriends(api, friend[1], no_of_friends)
    	next_level_friends.extend(temp)
    return next_level_friends

# Q1.b.(iii) - 7 points
def getNextLevelFollowers(api, followers_list, no_of_followers):
    # TODO: implement the method for fetching 'no_of_followers' followers for each entry in followers_list
    # rtype: list containing entries in the form of a tuple (follower, followers_list[i])    
    next_level_followers = []
    # Add code here to populate next_level_followers
    for follower in followers_list:
    	temp = getPrimaryFollowers(api, follower[1], no_of_followers)
    	next_level_followers.extend(temp)
    return next_level_followers

def getPrimaryFollowers(api, root_user, no_of_friends):
	primary_followers = []
	#while True:
	try:
		follower = api.followers(root_user)
	except tweepy.error.RateLimitError:
		print("Rate Limit Error happens. Sleep 15 minutes.")
		time.sleep(60 * 15)
		follower = api.followers(root_user)
	except tweepy.error.TweepError:
		print("Tweep Error happens. Can't get followers.")
	length = min(no_of_friends, len(follower))	
	for i in range(length):
		temp = (follower[i].screen_name, root_user)
		primary_followers.append(temp)
	return primary_followers
	
# Q1.b.(i),(ii),(iii) - 4 points
def GatherAllEdges(api, root_user, no_of_neighbours):
    # TODO:  implement this method for calling the methods getPrimaryFriends, getNextLevelFriends
    #        and getNextLevelFollowers. Use no_of_neighbours to specify the no_of_friends/no_of_followers parameter.
    #        NOT using the no_of_neighbours parameter may cause the autograder to FAIL.
    #        Accumulate the return values from all these methods.
    # rtype: list containing entries in the form of a tuple (Source, Target). Refer to the "Note(s)" in the 
    #        Question doc to know what Source node and Target node of an edge is in the case of Followers and Friends. 
    all_edges = [] 
    #Add code here to populate all_edges
    primary_friends = getPrimaryFriends(api, root_user, no_of_neighbours)
    all_edges.extend(primary_friends)
    all_edges.extend(getNextLevelFriends(api, primary_friends, no_of_neighbours))
    all_edges.extend(getNextLevelFollowers(api, primary_friends, no_of_neighbours))
    return all_edges


# Q1.b.(i),(ii),(iii) - 5 Marks
def writeToFile(data, output_file):
    # write data to output_file
    # rtype: None
    output = open(output_file,"w")
    for i in data:
        output.write(",".join(i))
        output.write("\n")
    output.close()




"""
NOTE ON GRADING:

We will import the above functions
and use testSubmission() as below
to automatically grade your code.

You may modify testSubmission()
for your testing purposes
but it will not be graded.

It is highly recommended that
you DO NOT put any code outside testSubmission()
as it will break the auto-grader.

Note that your code should work as expected
for any value of ROOT_USER.
"""

def testSubmission():
    KEY_FILE = 'keys.json'
    OUTPUT_FILE_GRAPH = 'graph.csv'
    NO_OF_NEIGHBOURS = 20
    ROOT_USER = 'PoloChau'

    api_key, api_secret, token, token_secret = loadKeys(KEY_FILE)

    auth = tweepy.OAuthHandler(api_key, api_secret)
    auth.set_access_token(token, token_secret)
    api = tweepy.API(auth)

    edges = GatherAllEdges(api, ROOT_USER, NO_OF_NEIGHBOURS)

    writeToFile(edges, OUTPUT_FILE_GRAPH)
    

if __name__ == '__main__':
    testSubmission()

