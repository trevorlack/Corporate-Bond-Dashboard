import sys 
import win32com.client 

MySite = win32com.client.Dispatch('CuteFTPPro.TEConnection') 

MySite.Protocol = 'FTP' 
MySite.Host = 'sftp://(username):(password)@edx.standardandpoors.com'
MySite.Login = 'profund'
MySite.Password = 'Te8fun'
#MySite.UseProxy = 'BOTH'
MySite.Connect() 

if not MySite.IsConnected: 
    print('Could not connect to: %s Aborting!' % MySite.Host)
    sys.exit(1)
else: 
    print('You are now connected to: %s' % MySite.Host)

MySite.LocalFolder = 'c:/Users/tlack/Desktop/GARBAGE'
MySite.RemoteFolder = '/Inbox'
MySite.Download('*SP5MAIG*')


MySite.Disconnect()
MySite.TECommand('exit')
print(MySite.Status)