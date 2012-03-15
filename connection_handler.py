"""
Friendly Python SSH2 interface.
Code mostly by Zeth (http://zeth.net/source/)
and Justin Riley for StarCluster
License: LGPL or something
"""

import os
import tempfile
import paramiko

class Connection(object):
    """Connects and logs into the specified hostname. 
    Arguments that are not given are guessed from the environment.""" 

    def __init__(self,
                 host,
                 username = None,
                 private_key = None,
                 private_key_pass = None,
                 password = None,
                 port = 22,
                 ):
        self._sftp_live = False
        self._sftp = None
        if not username:
            username = os.environ['LOGNAME']

        # Begin the SSH transport.
        self._transport = paramiko.Transport((host, port))
        self._tranport_live = True
        # Authenticate the transport.
        if password:
            # Using Password.
            try:
                self._transport.connect(username = username, password = password)
            except paramiko.AuthenticationException:
                print "The password totally botched"
        elif private_key:
            # Use Private Key.
            pkey = None
            if private_key.endswith('rsa') or private_key.count('rsa'):
                pkey = self._load_rsa_key(private_key, private_key_pass)
            elif private_key.endswith('dsa') or private_key.count('dsa'):
                pkey = self._load_dsa_key(private_key, private_key_pass)
            else:
                pkey = self._load_rsa_key(private_key, private_key_pass)
                if pkey is None:
                    pkey = self._load_dsa_key(private_key, private_key_pass)
            try:
                self._transport.connect(username = username, pkey = pkey)
            except paramiko.AuthenticationException:
                raise exception.SSHAuthException(username, host)
        elif private_key is None:
            pkey = self._load_dsa_key("~/.ssh/id_dsa", private_key_pass)
            if pkey is None:
                pkey = self._load_rsa_key("~/.ssh/id_rsa", private_key_pass)
            try:
                self._transport.connect(username = username, pkey = pkey)
            except paramiko.AuthenticationException:
                raise exception.SSHAuthException(username, host)
        else:
            print "Failure squad, no credentials, can't login!"
    
    def _sftp_connect(self):
        """Establish the SFTP connection."""
        if not self._sftp_live:
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
            self._sftp_live = True

    def get(self, remotepath, localpath = None):
        """Copies a file between the remote host and the local host."""
        if not localpath:
            localpath = os.path.split(remotepath)[1]
        self._sftp_connect()
        self._sftp.get(remotepath, localpath)

    def put(self, localpath, remotepath = None):
        """Copies a file between the local host and the remote host."""
        if not remotepath:
            remotepath = os.path.split(localpath)[1]
        self._sftp_connect()
        self._sftp.put(localpath, remotepath)

    def execute(self, command, silent = True, only_printable = False,
                ignore_exit_status=False):
        """
        Execute a remote command and return stdout/stderr
 
        NOTE: this function blocks until the process finishes
 
        kwargs:
        silent - do not print output
        only_printable - filter the command's output to allow only printable
                        characters
        returns List of output lines
        """
        channel = self._transport.open_session()
        channel.exec_command(command)
        stdout = channel.makefile('rb', -1)
        stderr = channel.makefile_stderr('rb', -1)
        output = []
        line = None
        if silent:
            output = stdout.readlines() + stderr.readlines()
        else:
            while line != '':
                line = stdout.readline()
                if only_printable:
                    line = ''.join(char for char in line if char in string.printable) 
                if line != '':
                    output.append(line)
                    print line,
 
            for line in stderr.readlines():
                output.append(line)
                print line;
        output = [ line.strip() for line in output ]
        #TODO: warn about command failures
        exit_status = channel.recv_exit_status()
        if exit_status != 0:
            if not ignore_exit_status:
                print("command %s failed with status %d" % (command,
                                                                exit_status))
            else:
                print("command %s failed with status %d" % (command,
                                                                exit_status))
        return output

    def close(self):
        """Closes the connection and cleans up."""
        # Close SFTP Connection.
        if self._sftp_live:
            self._sftp.close()
            self._sftp_live = False
        # Close the SSH Transport.
        if self._tranport_live:
            self._transport.close()
            self._tranport_live = False

    def __del__(self):
        """Attempt to clean up if not explicitly closed."""
        self.close()

    def _load_rsa_key(self, private_key, private_key_pass=None):
        private_key_file = os.path.expanduser(private_key)
        try:
            rsa_key = paramiko.RSAKey.from_private_key_file(private_key_file, private_key_pass)
            #print("Using private key %s (rsa)" % private_key)
            return rsa_key
        except paramiko.SSHException,e:
            print 'invalid rsa key or password specified'
 
    def _load_dsa_key(self, private_key, private_key_pass=None):
        private_key_file = os.path.expanduser(private_key)
        try:
            dsa_key = paramiko.DSSKey.from_private_key_file(private_key_file, private_key_pass)
            #print("Using private key %s (dsa)" % private_key)
            return dsa_key
        except paramiko.SSHException,e:
            print 'invalid dsa key or password specified'

if __name__ == "__main__":
    # Set the servername and username to your own
    myssh = Connection("yourcluster.ca","ceing")
    print myssh.execute("ls -al")
    #myssh.put('ssh.py')
    myssh.close()
