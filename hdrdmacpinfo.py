#!/usr/bin/env python3

import os
import sys
import zmq
import json
import time
import datetime


MAX_WAIT_TIME = 5.0  # sec

# Check that host was supplied and print Usage statement if not
if len(sys.argv) < 2:
	print('\nUsage:\n\thdrdmacpinfo host [port]\n\n')
	sys.exit()

# Get HOST and PORT from command line
HOST = sys.argv[1]
PORT = 10471
if( len(sys.argv) >= 3 ): PORT = int(sys.argv[2])


# Connect to host
print('Connecting to ' + HOST + ':' + str(PORT) + ' ...')

# Create socket
try:
	context = zmq.Context()
	socket = context.socket(zmq.SUB)
	socket.connect('tcp://' + HOST + ':' + str(PORT))
	socket.setsockopt_string(zmq.SUBSCRIBE, '') # Accept all messages from publisher
except:
	print('Error connecting to host or subscribing to service')
	print(sys.exc_info()[0])
	sys.exit()

print('Subscribed to hdrdmacp server for status info. Waiting ....')

start_time = time.time()
while True:
	try:
		string = socket.recv_string(flags=zmq.NOBLOCK)
		print('Received string:\n' + string + '\n')
		break
		#myinfo = json.loads( string )
		#myinfo['received'] = time.time()

		#app.Add_hdrdmacp_HostInfo( myinfo )

	except zmq.Again as e:
		if( (time.time() - start_time) >=  MAX_WAIT_TIME ):
			print('No messages received in %f seconds. Giving up.' % MAX_WAIT_TIME)
			sys.exit()
		time.sleep(1)
