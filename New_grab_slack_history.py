# MIT License

# Copyright (c) 2016 Chandler Abraham

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from slacker import Slacker
import slacker
import json
import argparse
import os
import sys
import pandas as pd

# This script finds all channels, private channels and direct messages
# that your user participates in, downloads the complete history for
# those converations and writes each conversation out to seperate json files.
#
# This user centric history gathering is nice because the official slack data exporter
# only exports public channels.
#
# PS, this only works if your slack team has a paid account which allows for unlimited history.
#
# PPS, this use of the API is blessed by Slack.
# https://get.slack.help/hc/en-us/articles/204897248
# " If you want to export the contents of your own private groups and direct messages
# please see our API documentation."
#
# get your slack user token at the bottom of this page
# https://api.slack.com/web
#
# dependencies:
#  pip install slacker # https://github.com/os/slacker
#
# usage examples
#  python slack_history.py --token='123token'
#  python slack_history.py --token='123token' --dryRun=True
#  python slack_history.py --token='123token' --skipDirectMessages
#  python slack_history.py --token='123token' --skipDirectMessages --skipPrivateChannels


# fetches the complete message history for a channel/group/im
#
# pageableObject could be:
# slack.channel
# slack.groups
# slack.im
#
# channelId is the id of the channel/group/im you want to download history for.

def getHistory(pageableObject, channelId, pageSize=100):
    messages = []
    lastTimestamp = None

    while (True):
        response = pageableObject.history(
            channel=channelId,
            latest=lastTimestamp,
            oldest=0
               ).body

        messages.extend(response['messages'])

        if (response['has_more'] == True):
            lastTimestamp = messages[-1]['ts']  # -1 means last element in a list
        else:
            break
    return messages


def mkdir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


# fetch and write history for all public channels
def getChannels(path, token1,token2, dryRun):
    channels = token2.conversations.list().body['channels']
    overall_channels = 0
    overall_messages = 0
    s2=token1
    slack = token2

    print("\nfound channels: ")
    for channel in channels:
        print(channel['name'])

    if not dryRun:
        parentDir = path+"/channels"
        mkdir(parentDir)
        for channel in channels:
            channel_name = channel['name'].encode('utf-8').strip()
            channel_name = channel_name.decode('utf-8')
            print("getting history for channel {0}".format(channel_name))
            fileName = "{parent}/{file}.json".format(parent=parentDir, file=channel_name)
            messages = getHistory(s2.conversations, channel['id'])
            channelInfo = slack.conversations.info(channel['id']).body['channel']
            members_array = []
            try:
            	members_array =  slack.conversations.members(channel['id']).body['members']
            except slacker.Error as e:
            	print(e)
            	members_array = [repr(e)]
            members = {"members":members_array}
            channelInfo.update(members)
            with open(fileName, 'w') as outFile:
                print("writing {0} records to {1}".format(len(messages), fileName))
                overall_messages = overall_messages + len(messages)
                overall_channels = overall_channels + 1
                json.dump({'channel_info': channelInfo, 'messages': messages}, outFile, indent=4)

    print(("{om} messages in {oc} channels").format(om=overall_messages, oc=overall_channels))


# fetch and write history for all direct message conversations
# also known as IMs in the slack API.
def getDirectMessages(path, slack, ownerId, userIdNameMap, dryRun):
    dms = slack.im.list().body['ims']

    print("\nfound direct messages (1:1) with the following users:")
    for dm in dms:
        print(userIdNameMap.get(dm['user'], dm['user'] + " (name unknown)"))

    if not dryRun:
        parentDir = path+"/direct_messages"
        mkdir(parentDir)
        for dm in dms:
            name = userIdNameMap.get(dm['user'], dm['user'] + " (name unknown)")
            print("getting history for direct messages with {0}".format(name))
            fileName = "{parent}/{file}.json".format(parent=parentDir, file=name)
            messages = getHistory(slack.im, dm['id'])
            channelInfo = {'members': [dm['user'], ownerId]}
            with open(fileName, 'w') as outFile:
                print("writing {0} records to {1}".format(len(messages), fileName))
                json.dump({'channel_info': channelInfo, 'messages': messages}, outFile, indent=4)


# fetch and write history for all private channels
# also known as groups in the slack API.
def getPrivateChannels(path, slack, dryRun):
    groups = slack.users.conversations

    print("\nfound private channels:")
    for group in groups:
        print("{0}: ({1} members)".format(group['name'], len(group['members'])))

    if not dryRun:
        parentDir = path+"/channels"
        mkdir(parentDir)

        for group in groups:
            messages = []
            print("getting history for private channel {0} with id {1}".format(group['name'], group['id']))
            fileName = "{parent}/{file}.json".format(parent=parentDir, file=group['name'])
            messages = getHistory(slack.groups, group['id'])
            channelInfo = slack.groups.info(group['id']).body['group']
            with open(fileName, 'w') as outFile:
                print("writing {0} records to {1}".format(len(messages), fileName))
                json.dump({'channel_info': channelInfo, 'messages': messages}, outFile, indent=4)


# fetch all users for the channel and return a map userId -> userName
def getUserMap(path, slack):
    # get all users in the slack organization
    users = slack.users.list().body['members']
    userIdNameMap = {} 
    
    userColumns = ['id', 'name', 'real-name', 'email']
    usersDf = pd.DataFrame(columns = userColumns)

    for user in users:
        email = 'none'
        try:
            email = str(user['profile']['email'])
            newRow = pd.Series([user['id'], user['name'], user['profile']['real_name'], email], index=userColumns)
            usersDf = usersDf.append(newRow, ignore_index=True)
        except KeyError as e:
            print ('no email for ' + user['name'])

        if 'email' in user['profile'].keys():
        	print(user['profile']['email'])

        userIdNameMap[user['id']] = user['name']

    usersDf.to_csv(path+'/users.csv', sep=',', index=False, encoding='utf-8')

    print("found {0} users ".format(len(users)))

    return userIdNameMap


# get basic info about the slack channel to ensure the authentication token works
def doTestAuth(slack):
    testAuth = slack.auth.test().body
    teamName = testAuth['team']
    currentUser = testAuth['user']
    print("Successfully authenticated for team {0} and user {1} ".format(teamName, currentUser))
    return testAuth


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='download slack history')

    parser.add_argument(
        '--workspace',
        action='store',
        default=False,
        help="select slack workspace")


    parser.add_argument(
	    '--tokenUser',
	    action='store',
	    default=False,
	    help="give User TOken")


    parser.add_argument(
	    '--tokenBot',
	    action='store',
	    default=False,
	    help="Give bot Token")

    parser.add_argument(
        '--dryRun',
        action='store_true',
        default=False,
        help="if dryRun is true, don't fetch/write history only get channel names")

    parser.add_argument(
        '--skipPrivateChannels',
        action='store_true',
        default=True,
        help="skip fetching history for private channels")

    parser.add_argument(
        '--skipChannels',
        action='store_true',
        default=False,
        help="skip fetching history for channels")
    
    args = parser.parse_args()

    workspace = args.workspace
    tokenUser = args.tokenUser
    tokenBot = args.tokenBot

    os.mkdir(workspace) 

    if workspace == workspace:
        path = workspace
        token = ''
        no_workspace = False

        token1 = Slacker(tokenUser)
        token2 = Slacker(tokenBot)

        if not no_workspace:
            testAuth = doTestAuth(token1)
            testAuth = doTestAuth(token2)


            userIdNameMap = getUserMap(path, token1)

            dryRun = args.dryRun

            if not dryRun:
                with open(path+'/metadata.json', 'w') as outFile:
                    print("writing metadata")
                    metadata = {
                        'auth_info': testAuth,
                        'users': userIdNameMap
                    }
                    json.dump(metadata, outFile, indent=4)

            if not args.skipChannels:
                getChannels(path, token1,token2, dryRun)

            if not args.skipPrivateChannels:
                getPrivateChannels(path, token1, dryRun)

    else:
        print('Failed')
