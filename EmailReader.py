import ipwhois, sys, csv, os, re
import olefile as OleFile
import xlsxwriter
# Import the email modules we'll need
from email.parser import Parser as EmailParser
from email import policy
from pprint import pprint
from collections import OrderedDict


logfilename = str(datetime.now()).replace(":","_")+".log"
logging.basicConfig(filename=logfilename,
                    filemode='a',
                    format='%(asctime)s,  %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)



def windowsUnicode(string):
    if string is None:
        return None
    if sys.version_info[0] >= 3:  # Python 3
        return str(string, 'utf_16_le')
    else:  # Python 2
        return unicode(string, 'utf_16_le')


def atoi(text):
    return int(text) if text.isdigit() else text

def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    '''
    return [ atoi(c) for c in re.split('(\d+)', text) ]



class Message(OleFile.OleFileIO):
	def __init__(self, filename):
		OleFile.OleFileIO.__init__(self, filename)

	def _getStream(self, filename):
		if self.exists(filename):
			stream = self.openstream(filename)
			return stream.read()
		else:
			return None

	def _getStringStream(self, filename, prefer='unicode'):
		"""Gets a string representation of the requested filename.
		Checks for both ASCII and Unicode representations and returns
		a value if possible.  If there are both ASCII and Unicode
		versions, then the parameter /prefer/ specifies which will be
		returned.
		"""

		if isinstance(filename, list):
			# Join with slashes to make it easier to append the type
			filename = "/".join(filename)

		asciiVersion = self._getStream(filename + '001E')
		unicodeVersion = windowsUnicode(self._getStream(filename + '001F'))
		if asciiVersion is None:
			return unicodeVersion
		elif unicodeVersion is None:
			return asciiVersion
		else:
			if prefer == 'unicode':
				return unicodeVersion
			else:
				return asciiVersion
		
	@property
	def header(self):
		try:
			return self._header
		except Exception:
			headerText = self._getStringStream('__substg1.0_007D')
			if headerText is not None:
				self._header = EmailParser().parsestr(headerText)
			else:
				self._header = None
			return self._header



class LookupEntry:
	def __init__(self, ipaddress, dns_name=None):
		self.ipaddress = ipaddress
		self.email = ""
		self.address = ""
		self.name = ""
		self.description = ""
		self.predefined_dns_name = dns_name
	
	def resolveIP(self):
		try:
			obj = ipwhois.IPWhois(self.ipaddress).lookup_rdap()

			try:
				self.description =  obj['network']['remarks'][0]['description']
			except TypeError:
				self.description = "-"
				
			self.country = obj['network']['country']

			for key, val in obj['objects'].items():

				if isinstance(val, dict):
					if val['contact']['address']:
						try:
							self.address = val['contact']['address'][0]['value'].\
							replace("\n", " ")
						except UnicodeEncodeError:
							self.address = "encoding error"
							handleUnicodeError()
							continue
					if val['contact']['email']:
						try:
							self.email = val['contact']['email'][0]['value']
						except UnicodeEncodeError:
							self.email = "encoding error"
							handleUnicodeError()
							continue
					if val['contact']['name']:
						try:
							self.name = val['contact']['name']
						except UnicodeEncodeError:
							self.name = "encoding error"
							handleUnicodeError()
							continue
		except ipwhois.exceptions.IPDefinedError: 
			self.address ="Private IP address"
			self.name = "Private IP address"
			self.country = "Private IP address"
			self.description = "Private IP address"
		except ipwhois.exceptions.HTTPLookupError:
			self.address = "HTTP Lookup Error"
			self.name =  "HTTP Lookup Error"
			self.country =  "HTTP Lookup Error"
			self.description = "HTTP Lookup Error"
			
	def appendToCSVfile(self, csvfile):
		with open(csvfile, "w") as csvwriter:
			writer = csv.writer(csvwriter, delimiter="|")
			print ("Writing data ", self.ipaddress)
			writer.writerow([self.ipaddress, self.name, self.country,
			 self.email, self.address, self.description, self.predefined_dns_name])
	
	def get_resolved_ip(self):
		cached_IPs[self.ipaddress] = self.name, self.country, self.email, self.address, self.description, self.predefined_dns_name
		
		return (self.name, self.country,
			 self.email, self.address, self.description, self.predefined_dns_name)
			 
	
def writeToXLSXfile(xlsxfile, table_data):
		
	#logger.info("writing to xlsx file{}".format(xlsxfile)
	workbook = xlsxwriter.Workbook(xlsxfile)
	worksheet = workbook.add_worksheet("extracted data")
	worksheet.write_row(0, 0, ("file", "From", "To","Delivered To",  "Date", "Ip", "characterization", "received_from_date", "provider", 
								"country", "provider email", "provider address", "description", "predefined dns name"))
	for row_idx, (message_file, sender, receiver, delivered_to, sent_date,
	               ip, received_from_date, name, country, email, address, description, 
	       predefined_dns_name, ip_source) in enumerate(table_data):
		
		worksheet.write_row(row_idx+1, 0, (message_file, sender, 
				receiver, delivered_to, sent_date, 
			ip, ip_source, received_from_date, name, country, email, address, description, 
				predefined_dns_name))
	workbook.close()
      
                
        


def resolveIPs(paths):
	for path, val in paths.items():

		for (dns, ips) in val:
			for ipaddress in ips:
				if ipaddress != "127.0.0.1":
					lookupentry = LookupEntry(ipaddress, dns)
					lookupentry.resolveIP()
					lookupentry.appendToCSVfile(path+".csv")


def readfilegen(txtfile):
	with open(txtfile, "r") as txtfile:
		for line in txtfile.readlines():
			yield preprocess(line)
		


def preprocess(ipaddress):
	return ipaddress.replace("\n", "")

def handleUnicodeError():
	logger.warning("encoding error with address")


class MailProcessor:
	
	def __init__(self):
		self.received = r"(Received: )from(.+)"
		self.IP = r"[^\w|^\.]([0-2]?\d{1,2}\.[0-2]?\d{1,2}\.[0-2]?\d{1,2}\.[0-2]?\d{1,3})"
		self.DNS_Name_from = r"from\s*([\w+\-+\.+]+)\s+" #
		self.DNS_Name_by = r"by(\s+(\w+\-*\w+\.+\w+)+\s)"
		self.sender_regex = r"From:(\s+\w+\S+)"
		self.receiver_regex = r"To:(\s+\w+\S+)"
		self.IP6 = r"(\w{0,4}\:{1,2}\w{0,4}\:{1,2}\w{0,4}\:{1,2}\w{0,4}\:{1,2}\w{1,4})"
		self.date_sent_regex = r";(.+)"
		self.x_originating_ip_regex = r"X-Originating-IP:(.+)([1-9]\d{1,2}\.\d{1,3}\.\d{1,3}\.\d{1,3})"
		self.delivered_to_regex = r"Delivered-To:(\s+\w+\S+)"
		self.date_regex = r"Date:(\s+.+)"
		
		

	def reader(self, folder):
		for root, dirs, message_files in os.walk(folder):
			dirs.sort()
			for message_file in sorted(message_files, key=natural_keys):
				with open(os.path.join(root, message_file), 'r') as fp:
					yield fp, message_file, root
	
	def read_header(self, msg_header):
		header = ""
		for line in msg_header.read():
			header +=  line
		return header
		
		
class MailHeader:
	
	def __init__(self, header):
		self.header = header
		self.ips = OrderedDict()
		self.ips6 = OrderedDict()
		self.dates_time_sent = OrderedDict()
		
		self.date_time_sent = ""
			
	def extract_hops(self, mail_processor):
		
		self.hops = []
		first_hit = False 
			
		for idx, val in enumerate(re.finditer(mail_processor.received, self.header)):
		
			self.hops.append(val.group(2))
	
	def extract_x_originating_ip(self, mail_processor):
		for idx, val in enumerate(re.finditer(mail_processor.x_originating_ip_regex, self.header)):
			return val.group(2)		
	
	def extract_sender(self, mail_processor):
		for idx, val in enumerate(re.finditer(mail_processor.sender_regex, self.header)):
			return val.group(1)
			
	def extract_receiver(self, mail_processor):
		receivers = [val.group(1) for idx, val in enumerate(re.finditer(mail_processor.receiver_regex, self.header))]
		if receivers:
			return receivers[-1]
	
	def extract_delivered_to(self, mail_processor):
		for idx, val in enumerate(re.finditer(mail_processor.delivered_to_regex, self.header)):
			return val.group(1)
	
	def extract_date(self, mail_processor):
		for idx, val in enumerate(re.finditer(mail_processor.date_regex, self.header)):
			return val.group(1)
			
	def process_hops(self, mail_processor):

		self.hops.reverse()
		for idx, hop in enumerate(self.hops):
			
			self.dns_from = re.search(mail_processor.DNS_Name_from, hop)
			self.dns_by = re.search(mail_processor.DNS_Name_by, hop)
			self.ips[idx]= re.findall(mail_processor.IP, hop)
			self.ips6[idx] = re.findall(mail_processor.IP6, hop)
			self.dates_time_sent[idx] = re.findall(mail_processor.date_sent_regex, hop)
							
			if self.dns_from:
				self.dns_from = self.dns_from.group(1)
			
			if self.dns_by:
			
				self.dns_by = self.dns_by.group(0)[3:]
			

	def get_received_from_ip(self):
		"""returns: received_from_ip
		       
		"""
		
		if self.ips:
		
			for idx, ips in self.ips.items():

				if ips:
					ips.reverse()
					for ip in ips:
						if not ip.startswith("127.") and not ip.startswith("10.") and not ip.startswith("192."):
							
							return idx, ip
				
		return None, None
					
	def get_received_from_ip6(self):
		"""returns: received_from_ip
		       
		"""

		if self.ips6:
			for idx, ips6 in self.ips6.items():
				if ips6:
					for ip6 in ips6:
						if not ip6.startswith("fe80"):
							return idx, ip6
				
				
		return None, None

	def get_received_from_date(self, loop_idx):
		try:
			return self.dates_time_sent[loop_idx][0]
		except IndexError:
			return None


def readmsgFiles(folder):
	# If the e-mail headers are in a file, uncomment these two lines:
	
	for messagefile in os.listdir(folder):
		print ("processing", messagefile)
		with open(os.path.join(folder, messagefile), 'rb') as fp:
			msg = Message(fp)
			with open (os.path.join(os.curdir, "msgs", messagefile.split(".")[0]+"_header.txt"), 'w') as fw:
				fw.write(str(msg.header))
			    
				for k, val in msg.header.items():
					if k == "Received":
					
						dns_from = re.search(DNS_Name_from, val)
						dns_by = re.search(DNS_Name_by, val)
						ips = re.findall(IP, val)
						
						if dns_from:
							dns_from = dns_from.group(0)[5:]
							
						if dns_by:
							dns_by = dns_by.group(0)[3:]			
							
						hops.append((dns_by,ips, dns_from))
		paths[messagefile.split(".")[0]] = hops
		hops = []
	return paths
	

def visualizePaths(message_file, hops, dir=None):
	
	with open('paths.txt', 'a') as pathwriter:
		
		pathwriter.write(dir+"/"+message_file+": ")
		for idx, (dns_by, ips, dns_from, ips6) in enumerate(hops[::-1]):
			
			ip_by = "---"
			ip_from = "---"
		
			if idx < len(hops)-1:
				delimiter = " -------------------->  "
			else:
				delimiter = ""
			
			if not dns_by:
				dns_by = "---"
			
			if not dns_from:
				dns_from = "---"
			
			if ips6:
				try:
					ip_by = ips6[1]
				except IndexError:
					pass
				
				try:
					ip_from = ips6[0]
				except:
					pass
					
			if ips:
				try:
					ip_from = ips[0][1]
				except IndexError:
					pass
				
				try:
					ip_by = ips[1][1]
				except IndexError:
					pass
			
	
			pathwriter.write(dns_by+" receiver having IP "+ip_by+" reports that the message originated from mail relay server with IP "+
					 ip_from +" which corresponds to "+dns_from+delimiter)
				
		pathwriter.write("\n\n")


def process_ip(sender_provider_ip):
	if sender_provider_ip not in cached_IPs.keys():
		lookupentry = LookupEntry(sender_provider_ip)
		lookupentry.resolveIP()
		return lookupentry.get_resolved_ip()
	else: 
		print ("using cached entry for IP {}".format(sender_provider_ip))
		return cached_IPs[sender_provider_ip]
	
	
def process_file(mail_header, message_file, file_path=None):
	print ("reading {0} at {1}".format(message_file, file_path))
	header = mail_processor.read_header(mail_header)
	
	mail_header = MailHeader(header)
	mail_header.extract_hops(mail_processor)
	mail_header.process_hops(mail_processor)
	
	sender = mail_header.extract_sender(mail_processor)
	receiver = mail_header.extract_receiver(mail_processor)
	delivered_to = mail_header.extract_delivered_to(mail_processor)
	
	x_originating_ip = mail_header.extract_x_originating_ip(mail_processor)
	sent_date = mail_header.extract_date(mail_processor)
	
	nof_loop, received_from_ip = mail_header.get_received_from_ip()

	nof_loop6, received_from_ip6 = mail_header.get_received_from_ip6()
	
	if nof_loop and nof_loop6 and nof_loop > nof_loop6:
		received_from_ip = received_from_ip6
		nof_loop = nof_loop6
	
	
	if x_originating_ip:
		print ("performing X Originating IP lookup {} ".format(x_originating_ip))
		name, country, email, address, description,predefined_dns_name  = process_ip(x_originating_ip)
		
		return (message_file, sender, receiver, delivered_to, sent_date,
	               x_originating_ip, "", name, country, email, address, description, 
	       predefined_dns_name, "Source X Originating IP")
		
	elif received_from_ip:
		received_from_sent_date = mail_header.get_received_from_date(nof_loop)
		print ("performing Received from IP lookup {} hop {}".format(received_from_ip, nof_loop))
		name, country, email, address, description,predefined_dns_name  = process_ip(received_from_ip)
		print (name, country, email, address, description,predefined_dns_name )

		return (message_file, sender, receiver, delivered_to, sent_date,
	               str(received_from_ip), received_from_sent_date, name, country, email, address, description, 
	       predefined_dns_name, "Received from: hop number: {}".format(nof_loop+1))
	
	
	else:
		print ("no IP found")
		return (message_file, sender, receiver, delivered_to, sent_date, "","","",
		"","","", "", "", "")



if __name__ == "__main__":
	
	try:
		os.remove('paths.txt')
	except FileNotFoundError:
		pass
		
	mail_processor = MailProcessor()
	base_dir = os.getcwd()
	table_data = []
	
	cached_IPs = {}
	
	for idx, (mail_header, message_file, file_path) in enumerate(mail_processor.reader(sys.argv[1])):
		(message_file, sender, receiver, delivered_to, sent_date,
	               provider_ip, received_from_date, name, country, email, address, description, 
	       predefined_dns_name, ip_source)  = process_file(mail_header, message_file, file_path) 
		table_data.append((message_file, sender, receiver, delivered_to, sent_date,
	               provider_ip, received_from_date, name, country, email, address, description, 
	       predefined_dns_name, ip_source))
	
		if idx == 10:
			break


	
	if os.path.isfile(sys.argv[1]):
		with open(sys.argv[1], 'r') as fp:
		
			(message_file, sender, receiver, delivered_to, sent_date,
	               ip, received_from_date, name, country, email, address, description, 
	       predefined_dns_name, ip_source) = process_file(fp, sys.argv[1])
			table_data.append((message_file, sender, receiver, delivered_to, sent_date,
	               ip, received_from_date, name, country, email, address, description, 
	       predefined_dns_name, ip_source))
	
	writeToXLSXfile("results.xlsx", table_data)
	
	
		


