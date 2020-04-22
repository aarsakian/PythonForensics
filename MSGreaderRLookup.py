import ipwhois, sys, csv, os, re
import olefile as OleFile

# Import the email modules we'll need
from email.parser import Parser as EmailParser
from email import policy


def windowsUnicode(string):
    if string is None:
        return None
    if sys.version_info[0] >= 3:  # Python 3
        return str(string, 'utf_16_le')
    else:  # Python 2
        return unicode(string, 'utf_16_le')


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
			#if self.ipaddress == "51.254.76.129":print (obj)
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
			
	def appendToCSVfile(self, csvfile):
		with open(csvfile, "w") as csvwriter:
			writer = csv.writer(csvwriter, delimiter="|")
			print ("Writing data ", self.ipaddress)
			writer.writerow([self.ipaddress, self.name, self.country,
			 self.email, self.address, self.description, self.predefined_dns_name])


def resolveIPs(paths):
	for path, val in paths.items():

		for (dns, ips) in val:
			for ipaddress in ips:
				if ipaddress != "127.0.0.1":
					lookupentry = LookupEntry(ipaddress, dns)
					lookupentry.resolveIP()
					lookupentry.appendToCSVfile(path+".csv")


def readfilegen(txtfile):
	with open(txtfile, "r", encoding='utf-8') as txtfile:
		for line in txtfile.readlines():
			yield preprocess(line)
		

           # except exceptions.HTTPLookupError:
             #    writer.writerow([logline.accept_date, logline.client_ip, logline.http_request_method,
              #               logline.http_request_path, "lookupError", "-", "-"])
              #   logger.warning("error in looking IP address {0}".format(logline.client_ip))
              #   continue

def preprocess(ipaddress):
	return ipaddress.replace("\n", "")

def handleUnicodeError():
	logger.warning("encoding error with address")


class MailProcessor:
	
	def __init__(self):
		self.received = r"(Received: )from|(\+|\-\d{4})"
		self.IP = r"([^\w|^\.]([1-9]\d{1,2}\.\d{1,3}\.\d{1,3}\.\d{1,3}))"
		self.DNS_Name_from = r"from\s*([\w+\-+\.+]+)\s+" #
		self.DNS_Name_by = r"by(\s+(\w+\-*\w+\.+\w+)+\s)"
		self.IP6 = r"(\w{0,4}\:{1,2}\w{0,4}\:{1,2}\w{0,4}\:{1,2}\w{0,4}\:{1,2}\w{1,4})"
		

	def reader(self, folder):
		for root, dirs, message_files in os.walk(folder):
			dirs.sort()
			for message_file in sorted(message_files):
				with open(os.path.join(root, message_file), 'r', encoding='utf-8') as fp:
					yield fp, message_file, root
			
	def extract_hops(self, msg_header, message_file):
			
		header = ""
		
		self.hops = []
		first_hit = False 
		
		for line in msg_header.read():
			header +=  line
			
		for idx, val in enumerate(re.finditer(self.received, header)):
	
			if val.group(0)== "Received: from":
				start = val.start()
				first_hit = True
			elif first_hit:
				end = val.end()
				self.hops.append(header[start:end])
				
				first_hit = False
	
	def process_hops(self):
		processed_hops = []
		
		for idx, hop in enumerate(self.hops):
			self.dns_from = re.search(self.DNS_Name_from, hop)
			self.dns_by = re.search(self.DNS_Name_by, hop)
			self.ips = re.findall(self.IP, hop)
			self.ips6 = re.findall(self.IP6, hop)
				
			if self.dns_from:
#				print (hop, "    ",self.dns_from)
				self.dns_from = self.dns_from.group(1)
			
			if self.dns_by:
				self.dns_by = self.dns_by.group(0)[3:]
				
			if self.dns_by or self.dns_from or self.ips or self.ips6:
				processed_hops.append((self.dns_by, self.ips, self.dns_from, self.ips6))
				#print ('inserting at', self.dns_from ,"HOP", hop)

		return processed_hops
		
	def get_sender_provider_ip(self):
		ips = re.findall(self.IP, self.hops[-1])
		if ips:
			return ips[0][1]
		ips = re.findall(self.IP6, self.hops[-1])
		if ips:
			return ips[0]


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
					#	print (val)
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
	
	with open('paths.txt', 'a', encoding='utf-8') as pathwriter:
		
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
			
		#	print ("hop", idx+1, dns_by, ip_by, ip_from, dns_from)
			pathwriter.write(dns_by+" receiver having IP "+ip_by+" reports that the message originated from mail relay server with IP "+
					 ip_from +" which corresponds to "+dns_from+delimiter)
				
		pathwriter.write("\n\n")


if __name__ == "__main__":
	#for ipaddess in readfilegen(sys.argv[1]):
	#	lookupentry = LookupEntry(ipaddess)
	#	lookupentry.resolveIP()
	#	lookupentry.appendToCSVfile("resolve.csv")
	try:
		os.remove('paths.txt')
	except FileNotFoundError:
		pass
		
	mail_processor = MailProcessor()
	base_dir = os.getcwd()
	for (mail_header, message_file, file_path) in mail_processor.reader(sys.argv[1]):
		
			print ("parsing {0} at {1}".format(message_file, file_path))

			mail_processor.extract_hops(mail_header, message_file)
				
			processed_hops = mail_processor.process_hops()
			sender_provider_ip = mail_processor.get_sender_provider_ip()
			print ("visualizing {0}".format(message_file))
			visualizePaths(message_file, processed_hops, file_path)
			if sender_provider_ip:
				lookupentry = LookupEntry(sender_provider_ip)
				lookupentry.resolveIP()
				
				try:
					
					os.makedirs(os.path.join('csv', file_path))
				except OSError as e:
					
					pass
				
				os.chdir(os.path.join('csv', file_path))
				lookupentry.appendToCSVfile(message_file.split(".")[0]+".csv")
			os.chdir(base_dir)


