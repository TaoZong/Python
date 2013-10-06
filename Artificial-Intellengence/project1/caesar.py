import cPickle as pickle
import sys
### Caesar cipher

### function that takes as input a filehandle and an integer to shift,
## reads the contents of the file, and shifts each character by the
## appropriate amount. The resulting string should be
## returned. Punctuation and other non-alphabetic characters can be
## ignored. 
## 

def caesar(infile, shift=3) :
    try :
        inputFile = open(infile)
    except :
        print "Sorry, input"
    newline = "\nThe new encrypted string is:\n"
    line = inputFile.readline()
    while line :
        for char in line :
            if char.isalpha() or char.isdigit() :
                char = chr(ord(char) + int(shift))
            newline += char
        line = inputFile.readline()
    return newline

### Usage: caesar {--shift=n} file
## main should print the result of caesar.

if __name__ == '__main__' :
    if len(sys.argv) == 2 :
        outString = caesar(sys.argv[1], sys.argv[2])
    else:
        infile = raw_input("Please input a file name as input file\n")
        shift = raw_input("Please input a number as shift amount\n")
        if infile != "" and shift != "" :
            outString = caesar(infile, shift)
        elif infile != "" and shift == "" :
            outString = caesar(infile)
        else :
            print "Sorry, input error!"
    print outString