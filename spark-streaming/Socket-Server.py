# import the socket library
import socket

# next create a socket object
s = socket.socket()
print("Socket successfully created")

# reserve a port on your computer in our case it is 9001
port = 9001

# Next bind to the port
# we have not typed any ip in the ip field
# instead we have inputted an empty string
# this makes the server listen to requests
# coming from other computers on the network
s.bind(('', port))
print("socket binded to %s" %(port))

# put the socket into listening mode
s.listen(5)
print("socket is listening")

# a forever loop until we interrupt it or
# an error occurs
# with open('RandomData.txt','rb') as f:
with open('TweetData.txt', 'rb') as f:

    while True:
        data1=f.readline()
        # Establish connection with client.
        c, addr = s.accept()
        print('Got connection from', addr)

        # send data to client
        c.sendall(data1)
        # c.sendall(''.join(format(data1,'b'))) # not working
        # c.send('Thank you for connecting')

        # Close the connection with the client
        c.close()
