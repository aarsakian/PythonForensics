import vobject, sys, xlsxwriter, os, codecs
from jinja2 import Template
from functools import partial

class Entry:
    def __init__(self, id , val, children=None):
        self.id = id
        self.val = val
        self.children=children


def openxls(xlsname):
    workbook = xlsxwriter.Workbook(xlsname)
    return workbook

def write2xls(worksheet, rowNum, colNum, data):
    border= workbook.add_format({'border': True})
    worksheet.write(rowNum ,colNum, data, border)


def readVcard(fname):
    with open(fname) as f:
        data= f.read()
        vdata = vobject.readOne(data).contents
    return vdata

def readFiles(path):
    files = os.listdir(path)
    fi = open("vcard.log", "w")
    for file in files:
        vdata = readVcard(os.path.join(path, file))

        parseVcard(vdata, file)

        msg= "Parsed successfully file{0}\n".format(file)
        sys.stdout.write(msg)
        fi.write(str(msg))
    fi.close()


def openhtmltemplate():
    f=open("templates/vcard.html")
    template = Template(f.read())
    f.close()
    return template

def createhtml(filename):
    f=codecs.open(filename, 'w', encoding='utf -8')
    return f


def rendertemplate(f, outerlist, filename):
    template = openhtmltemplate()
    t = template.render(my_list=outerlist, filename=filename)
    writehtml(f ,t)


def writehtml(f, t):
    f.write(t)

def closehtml(f):
    f.close()




def parseVcard(vdata, file):
    worksheet = workbook.add_worksheet(file)
    nofcols = 0
    outerlist = []
    for colNum, (id, data) in enumerate(vdata.items()):
        if isinstance(data, list):

            for rowNum, item in enumerate(data):

                val = item.value
                if not isinstance(val,(str, unicode, list)):
                    startcol = nofcols
                    innerlist=[]
                    for num,(attr, valattr) in enumerate(vars(val).items()):


                        write2xls(worksheet, rowNum+1,nofcols, attr)
                        write2xls(worksheet, rowNum+2, nofcols,  valattr)
                        innerlist.append(Entry(attr, valattr))

                        nofcols+=1
                    merge_format = workbook.add_format({
                        'bold':     True,
                        'border':   3,
                        'align':    'center',
                        'valign':   'vcenter',

                    })

                    worksheet.merge_range(rowNum, startcol, rowNum, startcol+len(vars(val).keys())-1, id,
                                          merge_format)
                 #   outerlist.append(innerlist)
                else:

                    write2xls(worksheet, 0, nofcols, id)
                    if isinstance(val, list):
                       val = val[0]

                    write2xls(worksheet, 1, nofcols, val)

                    nofcols+=1
                    outerlist.append(Entry(id, val))
            if innerlist:
                outerlist.append(Entry(id, "", innerlist))
            innerlist=[]


        else:

            write2xls(worksheet,1, colNum,  data)

    newfunc(outerlist, file)

def closexls(workbook):
    workbook.close()


if __name__ == "__main__":
    if len(sys.argv) > 3:

        workbook = openxls(sys.argv[2])
        fhandler = createhtml(sys.argv[3])

        newfunc = partial(rendertemplate, fhandler)
        readFiles(sys.argv[1])
        closehtml(fhandler)
        closexls(workbook)

    else:
        sys.stderr.write("Usage : python %s inputfolder output XLSX file output HTML file \n" % sys.argv[0])