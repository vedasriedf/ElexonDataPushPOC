import logging
import time
import sys
import os
import stomp
import ssl
import datetime
from datetime import datetime
import xmltodict
from azure.storage.fileshare import ShareFileClient

connstr="DefaultEndpointsProtocol=https;AccountName=adrelexonstorage;AccountKey=C9hF4obA4dzqgpAoBebp43sRdxs5ODA1VCv09K6nMfW6BkGZWOj2kk3jCQ/vO/ZEzY1rOI30l2Bp+AStonWOzg==;EndpointSuffix=core.windows.net"
class MyListener(stomp.ConnectionListener):
    def on_error(self, frame):
        f = open(r"D:\Applications\Elexon\PushPOC\logs\Error_"+datetime.now().strftime("%Y%D%m")+".txt", mode="a",encoding="UTF-8")
        f.write('"%s"' % frame.body)
        f.close()
        #print('received an error "%s"' % frame.body)

    def on_message(self, frame):
        message = xmltodict.parse(frame.body)
        if 'msgGrp' in message :
            key=message['msgGrp']['flow']
            
        #else:
        #    key='GL_MarketDocument'
        #    t=message['GL_MarketDocument']
            if key in ['MEL','MELS','FPN','PN']:
                fileName="Elexon_"+datetime.now().strftime("%Y%m%d%H%M%S%f")+"_"+key+".txt"
                file_client = ShareFileClient.from_connection_string(conn_str=connstr, share_name="adrelexonfileshare", file_path=fileName)
                file_client.upload_file('"%s"' % frame.body)
                #f = open(r"D:\Applications\Elexon\PushPOC\Files\Elexon_"+datetime.now().strftime("%Y%m%d%H%M%S%f")+"_"+key+".txt", mode="w",encoding="UTF-8")
                #f.write('"%s"' % frame.body)
                #f.close()
        #print('received a message "%s"' % frame.body)

if __name__ == '__main__':
    # --- Start of connection details
    host = "api.bmreports.com"
    port=61613
    apikey = "svk5coa0imeb589"
    clientId = "ADRTest"
    topicName = "bmrsTopic"
    SUBSCRIPTIONID = "<YOUR SUBSCRIPTION ID GOES HERE>"
    # --- End of connection details

    # Set up logging
    logging.basicConfig(level=logging.INFO)

    # Create STOMP coonection
    conn = stomp.Connection12([(host, port)])
    try:
        
        

        conn.set_ssl(for_hosts=[(host, port)], ssl_version=ssl.PROTOCOL_TLS)
        conn.set_listener('', MyListener())
        conn.connect(apikey, apikey, wait=True)
        conn.subscribe(destination='/topic/bmrsTopic', id=clientId, ack='auto')
        # Keep the main thread alive to receive messages
        while conn.is_connected():
            time.sleep(1)
    except KeyboardInterrupt:
        conn.close_connection()
        sys.exit(0)
