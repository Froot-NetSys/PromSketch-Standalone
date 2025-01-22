# pip install pyshark
# sudo apt install tshark -y

import pyshark, struct, socket

output_name = "caida_sourceip.txt"
output_file = open(output_name, "a")

def ip_to_int(ip_string):
    """Converts an IP string to an integer."""
    return struct.unpack('!I', socket.inet_aton(ip_string))[0]

filenames = ["equinix-nyc.dirA.20190117-125910.UTC.anon.pcap"] # , "equinix-nyc.dirA.20190117-130000.UTC.anon.pcap"]
for filename in filenames:
    cap = pyshark.FileCapture(filename)
    for packet in cap:
        if 'IP' in packet:
            sourceip = ip_to_int(packet.ip.src) 
            output_file.write(str(sourceip) + "\n")
    cap.close()

output_file.close()