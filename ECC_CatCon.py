# -*- python -*-
#
# Copyright (C) 2010 SAS Institute Inc.
# All rights reserved
#
# Confidential and Proprietary Information
#

import sys
import socket
import re
import string
import types
import time
import datetime

from StringIO import StringIO

"""
Python client for SAS Content Categorization Server.
"""

############################################################
############################################################

nb_results_regex = re.compile("NB_RESULTS:\s+(\d+)$")
record_regex = re.compile("RECORD\s+(\d+)")
language_regex = re.compile("\s+LANGUAGE:\s+(.*)")
encoding_regex = re.compile("\s+CHAR_ENCODING\s+(.*)")
banner_regex = re.compile("TERAGRAM\s+categories/concepts\s+server")
version_regex = re.compile("Protocol\s+version\s+3\.(\d+)")
nb_mcat_projects_regex = re.compile("NB_MCAT_PROJECTS:\s+(\d+)$")
current_mcat_project_regex = re.compile("CURRENT_MCAT_PROJECT:\s+(\d+)$")
nb_concepts_projects_regex = re.compile("NB_CONCEPTS_PROJECTS:\s+(\d+)$")
current_liti_project_regex = re.compile("CURRENT_LITI_PROJECT:\s+(\d+)$")
nb_liti_projects_regex = re.compile("NB_LITI_PROJECTS:\s+(\d+)$")
li_char_pos_regex = re.compile("\tCHAR_POS:\s+(\d+)-(\d+)")
li_word_pos_regex = re.compile("\tWORD_POS:\s+(\d+)-(\d+)")
li_fact_regex = re.compile("\tFACT:\s+(.*)")
li_match_regex = re.compile("\tMATCH:\s+(.*)")
li_context_regex = re.compile("\tCONTEXT:\s+(.*)")
li_nb_args_regex =re.compile("NB_ARGS:\s+(\d+)")
li_arg_type_regex = re.compile("\tARG_TYPE:\s+(.*)")
li_arg_string_regex = re.compile("\tARG_STRING:\s+(.*)")
li_fullpath_regex = re.compile("\tFULLPATH:\s+(.*)")
li_info_regex = re.compile("\tINFO:\s+(.*)")
li_canonical_regex = re.compile("\tCANONICAL:\s+(.*)")
current_concepts_project_regex = re.compile("CURRENT_CONCEPTS_PROJECT:\s+(\d+)$")
project_regex = re.compile("\d+\.\s+(.*)")
nb_categories_regex = re.compile("NB_CATEGORIES:\s+(\d+)")
category_regex = re.compile(".*?\s+(.*)")
nb_concepts_regex = re.compile("NB_CONCEPTS:\s+(\d+)")
nb_liti_predicates_regex = re.compile("NB_PREDICATES:\s+(\d+)")
liti_predicate_regex = re.compile("\d+\.\s+(.*)")
v0_regex = re.compile("(.*?)\s+\[(.*)\]\s+(.*)")
v1_regex = re.compile("(.*?)\s+\[(.*)\]\s+\[(.*)\]\s+(.*)")
v2_regex = re.compile("(.*?)\s+\[([0-9\.]*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+([0-9\-,]*)")
v3_regex = re.compile("(.*?)\s+\[([0-9\.]*),([0|1])\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+([0-9\-,]*)")
v4_regex = re.compile("(.*?)\s+\[([0-9\.]*),([0|1])\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+([0-9\-,]*)")
## 1. cat_name 
v0_cat_with_meta = re.compile("\d+\.\s+(.*?)\s+(.*)\s+")
## 1. cat_name [metadata] 
v1_cat_with_meta = re.compile("\d+\.\s+(.*?)\s+\[(.*)\]\s+")
## 1. cat_name [metadata] [unique_id] [comments] [links] [author] [cdate] [mdate]
v2_cat_with_meta = re.compile("\d+\.\s+(.*?)\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+")
## 1. cat_name [metadata] [unique_id] [comments] [links] [author] [cdate] [mdate] [rulestatus]
v4_cat_with_meta = re.compile("\d+\.\s+(.*?)\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+")

match_regex = re.compile("(\d+)-(\d+)")
nb_categories_regex = re.compile("NB_CATEGORIES:\s+(\d+)")
type_path_regex = re.compile("\s+TYPE:\s+(.+)/([^/]+)\s*$")
type_regex = re.compile("\s+TYPE:\s+(.+)")
info_regex = re.compile("\s+INFO:\s+(.+)")
orig_term_regex = re.compile("\s+ORIGINAL_TERM:\s+(.+)")
start_pos_regex = re.compile("\s+START_POS:\s+(\d+)")
end_pos_regex = re.compile("\s+END_POS:\s+(\d+)")
concept_rel_regex = re.compile("\s+RELEVANCY:\s([0-9\.]+)\s\(([0|1])\)")
concept_metadata_regex = re.compile("\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]\s+\[(.*)\]")

syn_encoding_regex = re.compile("ENCODING:\s+(.+)")
syn_text_regex = re.compile("SYNONYM_TEXT:\s+(.+)")

end_error = "END_MSG\n"

# Sentinel term used by synonym replacement
# Defined in Java API, _catcon_server.c, and here
NO_ORIG_TERM = "__TG_ZQX__"

def _getContents(data):
    try:
        return data.read()
    except AttributeError:
        return str(data)

def _timeDiff(t1, t2):
    diff = t2-t1;
    return diff.seconds + diff.microseconds/1000000.0


def _hex2char(match):
    if(match):
        return chr(int(match.group(1),16))

def _unescapeString(str):
    """
    Undo the string escaping that we applied to the metadata in CC Studio
    """
    if str == "":
        return str

    new_str = str.replace('\\t', '\t')
    new_str = new_str.replace('\\n', '\n')
    new_str = new_str.replace('\\r', '\r')
    new_str = new_str.replace('\\f', '\f')
    new_str = new_str.replace('\\"', '\"')
    new_str = new_str.replace("\\'", "\'")
    new_str = new_str.replace('\\\\', '\\')
        
    new_str = re.sub(r"\\x([\dA-F]{2})",_hex2char, new_str)
    return new_str


############################################################
############################################################

class CatConError( Exception ):
    """Exception class for error messages generated by the API."""

    def __init__( self, message='' ):

        Exception.__init__( self )
        self.msg = message

    def __str__(self):
        return repr( self.msg )

class CatConServerError( Exception ):
    """
    Exception class for error messages generated by SAS Content Categorization
    Server.
    """

    def __init__( self, message='' ):

        Exception.__init__( self )
        self.msg = message

    def __str__(self):
        return repr( self.msg )

class LanguageIdResult:
    """
    This class represents the results from performing language and encoding
    identification on a text.
    """
    
    def __init__(self, language, encoding):
        self.language = language
        self.encoding = encoding

    def getLanguage(self):
        """
        Returns the language that the text is in.
        """
        
        return self.language

    def getEncoding(self):
        """
        Returns the encoding that the text is in.
        """
        
        return self.encoding

class CatConMatch:
    """
    This class represents an individual result ('match') from performing
    categorization or concept extraction on a text.
    """

    def __init__(self, parent, start, end, orig_term):
        self.parent = parent
        self.start = start
        self.end = end
        self.orig_term = orig_term

    def getStart(self):
        """
        Gets the starting offset of this match within the source text.
        """
        
        return self.start

    def getEnd(self):
        """
        Gets the ending offset of this match within the source text.
        """
        
        return self.end

    def getMatchPhrase(self):
        """
        Returns the phrase that triggered this match.
        """

        return self.parent._getDocument()[self.start:self.end + 1]

    def getOriginalTerm(self):
        """
        Returns the original phrase that was replaced via synonym processing.
        Requires SAS Content Categorization Server 12.1 or higher.  This method
        has no effect when used with previous versions of the server.
        """
        if(self.orig_term == NO_ORIG_TERM):
            return ""
        
        return _unescapeString(self.orig_term)

class CategorizationResult:
    """
    This class represents a single category that was identified
    in the source text. This category may have multiple matches
    ('hits') within the source text. Returned with the category's
    name is:
    * Its relevance (i.e. how good of a match it was)
    * Its parent document
    * The list of CatConMatch hits that comprise this result
    """

    def __init__(self, document, name, relevance, above_relevancy_cutoff, metadata):
        self.document = document
        self.matches = []
        self.name = name
        self.relevance = relevance
        self.above_relevancy_cutoff = above_relevancy_cutoff
        self.metadata = metadata

    def _addMatch(self, start, end):
        self.matches.append(CatConMatch(self, start, end, ""))

    def _addOrigTerm(self, orig_term, i):
        self.matches[i].orig_term = _unescapeString(orig_term)

    def getName(self):
        """
        Gets this category's name, e.g. Top/Foo/Bar/Baz.
        """
        
        return self.name

    def getRelevance(self):
        """
        Gets this category match's relevance.
        """
        
        return self.relevance

    def getIsAboveRelCutoff(self):
        """
        Return 1 if this document is above the relevancy cutoff, 0 otherwise.
        If the relevancy cutoff is enabled on the server (i.e. no documents
        below the cutoff will be included in the results) or the protocol
        version is < 3.3, this function always returns 1 and is essentially
        a no-op.
        """

        return self.above_relevancy_cutoff
    
    def _getDocument(self):
        """
        Gets the source document that we categorized against
        """
        
        return self.document

    def getMatches(self):
        """
        Returns the list of CatConMatch matches for this category.
        """
        
        return self.matches

    def getMetadata(self):
        """
        Returns the description metadata for this category.
        """
        if(len(self.metadata) > 0):
            return _unescapeString(self.metadata[0])
        else:
            return ""

    def getUniqueIDMetadata(self):
        """
        Returns the unique ID metadata for this category.
        """
        if(len(self.metadata) > 1):
            return _unescapeString(self.metadata[1])
        else:
            return ""

    def getCommentsMetadata(self):
        """
        Returns the comments metadata for this category.
        """
        if(len(self.metadata) > 2):
            return _unescapeString(self.metadata[2])
        else:
            return ""

    def getRelatedLinksMetadata(self):
        """
        Returns the related links metadata for this category.
        """
        if(len(self.metadata) > 3):
            return _unescapeString(self.metadata[3])
        else:
            return ""

    def getAuthorMetadata(self):
        """
        Returns the author metadata for this category.
        """
        if(len(self.metadata) > 4):
            return _unescapeString(self.metadata[4])
        else:
            return ""

    def getCreationDateMetadata(self):
        """
        Returns the creation date metadata for this category.
        """
        if(len(self.metadata) > 5):
            return _unescapeString(self.metadata[5])
        else:
            return ""

    def getModificationDateMetadata(self):
        """
        Returns the modification date metadata for this category.
        """
        if(len(self.metadata) > 6):
            return _unescapeString(self.metadata[6])
        else:
            return ""

    def getRuleStatusMetadata(self):
        """
        Returns the rule status metadata for this category (either
        \"COMPLETED\", \"PENDING\", or \"UNKNOWN\").
        """
        if(len(self.metadata) > 7):
            status=_unescapeString(self.metadata[7])

            if(status == "1"):
                return "COMPLETED"
            elif(status == "2"):
                return "PENDING"
            else:
                return "UNKNOWN"
        else:
            return ""
    

class FactExtractionResult:
    """
    This class represents a single fact that was identified in the source text.
    It contains the following members:

    FactType: The name of the SAS Content Categorization Studio concept that
    matched, e.g. EXECUTIVE_INFO.
    
    FactString: The string from the document that caused this fact to match,
    e.g. \"Dr. Jim Goodnight is the CEO of SAS Institute, Inc.\".

    Context: The fact string with appropriate concordance information before
    and after the match.

    CharStart: The character start position of the fact in the document.

    CharEnd: The character end position of the fact in the document.

    WordStart: The word start position of the fact in the document.

    WordEnd: The word end position of the fact in the document.

    NumberOfArgs: The number of arguments for the fact match.

    Args: The arguments for the fact.  This consists of a list of tuples: the
    first element of each tuple is the literal matching string for the
    argument, e.g. \"Dr. Jim Goodnight\", and the second element is the type of
    the argument, e.g. \"PERSON\".
    """
    def __init__(self, fact_string, type, char_start, char_end, word_start, word_end):
        self.FactString = fact_string
        self.FactType = type
        self.FactTypePath = ""
        self.CharStart = char_start
        self.CharEnd = char_end
        self.WordStart = word_start
        self.WordEnd = word_end
        self.Args = []
        self.NumberOfArgs = 0
        self.Context = ""

    def _addArgs(self, arg_str, arg_type):
        arg_array = []
        arg_array.append(arg_str)
        arg_array.append(arg_type)
        self.Args.append(arg_array)
        self.NumberOfArgs += 1




class ConceptExtractionResult:
    """
    This class represents a single concept that was identified
    in the source text. This concept may have multiple matches
    ('hits') within the source text. Returned with the concept's
    name is:
    * Its 'info'
    * Its parent document
    * The list of CatConMatch hits that comprise this result
    """

    def __init__(self, document, type, type_path, info, rel = 0.0, above_cutoff = 1, metadata = []):
        self.document = document
        self.matches = []
        self.type = type
        self.type_path = type_path
        self.info = info
        self.relevance = rel
        self.above_cutoff = above_cutoff
        self.metadata = metadata
        self.canonical_form=""
        self.canonical_char_start=0;
        self.canonical_char_end=0;


    def _addMatch(self, start, end, orig_term):
        self.matches.append(CatConMatch(self, start, end, orig_term))


    def _addCanonicalForm(self, canonical_form, char_start=0,char_end=0):
        self.canonical_form = canonical_form
        self.canonical_char_start = char_start
        self.canonical_char_end = char_end

    def getType(self):
        """
        Gets the name of the concept that contains the match string.
        """
        
        return self.type

    def getTypePath(self):
        """
        Gets the full path of the concept that contains the match string.
        """
        return self.type_path 

    def getName(self):
        """
        Synonymous with getType().
        """

        return self.getType()

    def getInfo(self):
        """
        Gets this concept's info, i.e. the info string associated with the
        match string in SAS Content Categorization Studio.  If there is no info
        string associated with the match string, an empty string is returned.
        """
        return self.info

    def getCanonicalForm(self):
        """
        Gets this concept's canonical form, i.e. the canonical form string
        associated with the match string in SAS Content Categorization Studio.
        If there is no canonical form string associated with the match string,
        the match string itself is returned.

        Applies only to LITI contextual extraction results that use coreference resolution.
        """
        return self.canonical_form
    
    def getCanonicalStart(self):
        """
        Gets this concept's canonical form byte offset.

        Applies only to LITI contextual extraction results that use coreference resolution.
        """
        return self.canonical_char_start

    def getCanonicalEnd(self):
        """
        Gets this concept's canonical form byte offset.

        Applies only to LITI contextual extraction results that use coreference resolution.
        """
        return self.canonical_char_end


    def _getDocument(self):
        """
        Gets the source document that we extracted concepts from
        """
        return self.document

    def getMatches(self):
        """
        Returns the list of CatConMatch matches for this concept.
        """
        
        return self.matches

    def getRelevance(self):
        """
        Returns the relevance of the concept.

        Does not apply to LITI contextual extraction results.
        """
        return self.relevance

    def getIsAboveRelCutoff(self):
        """
        Returns whether the relevance is above the cutoff for that concept.

        Does not apply to LITI contextual extraction results.
        """
        return self.above_cutoff

    def getMetadata(self):
        """
        Returns the description metadata for this concept.
        Requires version 3.10 or greater of SAS Content Categorization Server.

        Does not apply to LITI contextual extraction results.
        """
        if(len(self.metadata) > 0):
            return _unescapeString(self.metadata[0])
        else:
            return ""

    def getUniqueIDMetadata(self):
        """
        Returns the unique ID metadata for this concept.
        Requires version 3.10 or greater of SAS Content Categorization Server.

        Does not apply to LITI contextual extraction results.
        """
        if(len(self.metadata) > 1):
            return _unescapeString(self.metadata[1])
        else:
            return ""

    def getCommentsMetadata(self):
        """
        Returns the comments metadata for this concept.
        Requires version 3.10 or greater of SAS Content Categorization Server.

        Does not apply to LITI contextual extraction results.
        """
        if(len(self.metadata) > 2):
            return _unescapeString(self.metadata[2])
        else:
            return ""

    def getRelatedLinksMetadata(self):
        """
        Returns the related links metadata for this concept.
        Requires version 3.10 or greater of SAS Content Categorization Server.

        Does not apply to LITI contextual extraction results.
        """
        if(len(self.metadata) > 3):
            return _unescapeString(self.metadata[3])
        else:
            return ""

    def getAuthorMetadata(self):
        """
        Returns the author metadata for this concept.
        Requires version 3.10 or greater of SAS Content Categorization Server.

        Does not apply to LITI contextual extraction results.
        """
        if(len(self.metadata) > 4):
            return _unescapeString(self.metadata[4])
        else:
            return ""

    def getCreationDateMetadata(self):
        """
        Returns the creation date metadata for this concept.
        Requires version 3.10 or greater of SAS Content Categorization Server.

        Does not apply to LITI contextual extraction results.
        """
        if(len(self.metadata) > 5):
            return _unescapeString(self.metadata[5])
        else:
            return ""

    def getModificationDateMetadata(self):
        """
        Returns the modification date metadata for this concept.
        Requires version 3.10 or greater of SAS Content Categorization Server.

        Does not apply to LITI contextual extraction results.
        """
        if(len(self.metadata) > 6):
            return _unescapeString(self.metadata[6])
        else:
            return ""

    def getRuleStatusMetadata(self):
        """
        Returns the rule status metadata for this concept (either
        \"COMPLETED\", \"PENDING\", or \"UNKNOWN\").
        Requires version 3.10 or greater of SAS Content Categorization Server.
        
        Does not apply to LITI contextual extraction results.
        """
        if(len(self.metadata) > 7):
            status=_unescapeString(self.metadata[7])

            if(status == "1"):
                return "COMPLETED"
            elif(status == "2"):
                return "PENDING"
            else:
                return "UNKNOWN"
        else:
            return ""
    
############################################################
############################################################

class CatConClient:
    """
    This class is responsible for communicating with the SAS Content
    Categorization Server and performing categorization, language
    identification, concept extraction, and contextual extraction.
    """

    def __init__(self, timeout = None):
        """
        timeout: Specifies the number of seconds to wait before timing out the
        connection.
        """
        self.hosts = []
        self.ports = []
        self.server_number = 0

        self.socket_to_server = None
        self.s = None

        self.mcat_projects = []
        self.current_mcat_project = -1

        self.liti_projects = []
        self.current_liti_project = -1


        self.concepts_projects = []
        self.current_concepts_project = -1

        self.timeout = timeout

        self.ver_major = self.ver_minor = -1

        self.use_persistent_connection = 0
        self.wait_time = 0.0
        

    def __del__(self):
        if self.socket_to_server != None:
            self._close()

    def _close(self):
        if self.socket_to_server != None:
            self.socket_to_server.close()
            self.socket_to_server = None
        if self.s != None:
            self.s.close()
            self.s = None

    def addServer(self, host, port):
        """
        Adds a server and port that you wish to try to connect to.
        The CatConClient will attempt to do failover and round-robin
        load balancing between these servers.

        --------

        host: The host to connect to.

        port: The port on which SAS Content Categorization Server is running
        on that host.
        """

        self.hosts.append(host)
        self.ports.append(port)

    def usePersistentConnection(self, value):
        """
        Set whether or not to use persistent connections with SAS Content
        Categorization Server.

        --------

        value: True to use persistent connections, False otherwise.  By
        default, persistent connections are not used.
        """
        self.use_persistent_connection = value
    
    def _connect(self):
        first_server_number = self.server_number
        

        while(1):
            try:
                self.server_number = (self.server_number + 1) % len(self.hosts)
                self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # this method is apparently only available + working in python >= 2.3
                if sys.hexversion >= 0x02030000:
                    self.s.settimeout(self.timeout)
                self.s.connect((self.hosts[self.server_number],
                                self.ports[self.server_number]))                
                self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.socket_to_server = self.s.makefile('r+')
                #s.close()
                break
            except:
                if self.server_number == first_server_number:
                    raise CatConError("Couldn't connect to any servers")

    def _acquireConnection(self):
        try:
            if self.socket_to_server != None and not self.socket_to_server.closed:
                return
        except:
            pass

        for host in range(0, len(self.hosts)):
            self._connect()  # throws it's own exception

            try:
                
                try:
                    banner_regex.match(self._readLine())
                except:
                    raise("Server is not a SAS Content Categorization Server")
                    self._close()
                    continue
        
                try:
                    match = version_regex.match(self._readLine())
                except:
                    raise("Wrong protocol version")
                    self._close()
                    continue
                
                self.ver_major = 3
                self.ver_minor = int(match.group(1))
                
                # OK
                self._readLine()

                # Reset all the project lists
                self.mcat_projects = []
                self.current_mcat_project = -1
                self.concepts_projects = []
                self.current_concepts_project = -1

                self.liti_projects = []
                self.current_liti_project = -1

                if self.ver_major >=3 and self.ver_minor >= 7:
                    self._write("FEEDBACK N\nLIST_MCAT_PROJECTS\nLIST_CONCEPTS_PROJECTS\nLIST_LITI_PROJECTS\n")
                else:
                    self._write("FEEDBACK N\nLIST_MCAT_PROJECTS\nLIST_CONCEPTS_PROJECTS\n")

                nb_mcat_projects = 0
                match = nb_mcat_projects_regex.match(self._readLine())
                if match:
                    nb_mcat_projects = int(match.group(1))
                    for i in range(0, nb_mcat_projects):
                        match = project_regex.match(self._readLine())
                        if match:
                            self.mcat_projects.append(match.group(1))

                if nb_mcat_projects > 0:
                    match = current_mcat_project_regex.match(self._readLine())
                    if match:
                        self.current_mcat_project = int(match.group(1))

                nb_concepts_projects = 0
                match = nb_concepts_projects_regex.match(self._readLine())
                if match:
                    nb_concepts_projects = int(match.group(1))
                    for i in range(0, nb_concepts_projects):
                        match = project_regex.match(self._readLine())
                        if match:
                            self.concepts_projects.append(match.group(1))

                if nb_concepts_projects > 0:
                    match = current_concepts_project_regex.match(self._readLine())
                    if match:
                        self.current_concepts_project = int(match.group(1))

                if self.ver_major >=3 and self.ver_minor >= 7:
                    nb_liti_projects = 0
                    match = nb_liti_projects_regex.match(self._readLine())
                    if match:
                        nb_liti_projects = int(match.group(1))
                        for i in range(0, nb_liti_projects):
                            match = project_regex.match(self._readLine())
                            if match:
                                self.liti_projects.append(match.group(1))
                                
                    if nb_liti_projects > 0:
                        match = current_liti_project_regex.match(self._readLine())
                        if match:
                            self.current_liti_project = int(match.group(1))

                return
            except:
                self._close()
                pass

        raise CatConError( "failover failed" )

    def _releaseConnection(self):
        if not self.use_persistent_connection:
            self._close()

    def _write(self, message):
        try:
            self.socket_to_server.write(message)
            self.socket_to_server.flush()
        except:
            raise CatConError( "socket write failed" )

    def _readLine(self):
        line = None
        try:
            line = self.socket_to_server.readline()
        except:
            raise CatConError( "socket read failed" )

        return line

    def _waitForResponse(self, response):
        comment_regex = re.compile("#")
        
        while(True):
            line = self._readLine().rstrip('\n')

            if(comment_regex.match(line)):                
                continue
            else:
                return (line == response)
            

    def _formatDocument(self, document):
        return "SET_DOCUMENT_ENCODING UTF-8\nDOCUMENT$ %s\n%s\n" % (len(document)+1, document)

    def getProtocolVersionMajor(self):
        """
        Get the major protocol version number as reported by the server.
        """
        try:
            self._acquireConnection()
            self._releaseConnection()
        except Exception, e:
            self._close()
            raise e

        return self.ver_major

    
    def getProtocolVersionMinor(self):
        """
        Get the minor protocol version number as reported by the server.
        """
        try:
            self._acquireConnection()
            self._releaseConnection()
        except Exception, e:
            self._close()
            raise e

        return self.ver_minor
        

    def reloadProjects(self):
        """
        Reload all projects on the Catcon server.
        """

        try:
            self._acquireConnection()
            self._write("RELOAD_PROJECTS\n")
            if self._getErrorMessage():
                raise CatConServerError(self.getLatestErrorMessage())
            self._releaseConnection()
        except Exception, e:
            self._close()
            raise e
            

    def _categoryProjectToId(self, project):
        for i in range(0, len(self.mcat_projects)):
            if self.mcat_projects[i] == project:
                return i+1

        return -1

    def _conceptProjectToId(self, project):
        for i in range(0, len(self.concepts_projects)):
            if self.concepts_projects[i] == project:
                return i+1

        return -1

    def _litiProjectToId(self, project):
        for i in range(0, len(self.liti_projects)):
            if self.liti_projects[i] == project:
                return i + 1
        return -1

    def _readAvailableCategoriesWithMetadata(self):
        available_categories_with_meta = []

        match = nb_categories_regex.match(self._readLine())
        if match:
            for i in range(0, int(match.group(1))):
                category = {}
                category["name"] = ""
                category["metadata"] = ""
                category["unique_id"] = ""
                category["comments"] = ""
                category["links"] = ""
                category["author"] = ""
                category["cdate"] = ""
                category["mdate"] = ""
                category["rulestatus"] = ""
                if self.ver_major >=3 :
                    category_with_metadata_regex = None
                    if(self.ver_minor == 0):
                        category_with_metadata_regex = v0_cat_with_meta
                        match = category_with_metadata_regex.match(self._readLine())
                        if match:
                            category = {}
                            category["name"] = match.group(1)
                    elif (self.ver_minor == 1):
                        category_with_metadata_regex = v1_cat_with_meta
                        match = category_with_metadata_regex.match(self._readLine())
                        if match:
                            category = {}
                            category["name"] = match.group(1)
                            category["metadata"] = match.group(2)

                    elif (self.ver_minor == 2 or self.ver_minor == 3):
                        category_with_metadata_regex = v2_cat_with_meta
                        match = category_with_metadata_regex.match(self._readLine())
                        if match:
                            category = {}
                            category["name"] = match.group(1)
                            category["metadata"] = match.group(2)
                            category["unique_id"] = match.group(3)
                            category["comments"] = match.group(4)
                            category["links"] = match.group(5)
                            category["author"] = match.group(6)
                            category["cdate"] = match.group(7)
                            category["mdate"] =match.group(8)
                    elif (self.ver_minor >=4 ):
                        category_with_metadata_regex = v4_cat_with_meta
                        match = category_with_metadata_regex.match(self._readLine())
                        if match:
                            category = {}
                            category["name"] = _unescapeString(match.group(1))
                            category["metadata"] = _unescapeString(match.group(2))
                            category["unique_id"] = _unescapeString(match.group(3))
                            category["comments"] = _unescapeString(match.group(4))
                            category["links"] = _unescapeString(match.group(5))
                            category["author"] = _unescapeString(match.group(6))
                            category["cdate"] = _unescapeString(match.group(7))
                            category["mdate"] = _unescapeString(match.group(8))
                            category["rulestatus"] = _unescapeString(match.group(9))
                    available_categories_with_meta.append(category)

        return available_categories_with_meta

                    

    def _readAvailableCategories(self):
        available_categories = []

        match = nb_categories_regex.match(self._readLine())
        if match:
            for i in range(0, int(match.group(1))):
                match = category_regex.match(self._readLine())
                if match:                    
                    available_categories.append(match.group(1))
                    
        return available_categories

    def _getErrorMessage(self):
        if self.ver_major >= 3 and self.ver_minor >= 5:
            self._write("GET_ERROR\n");
            line = ""
            self.error_msg = ""
            error = 0
            while(line <> end_error):
                self.error_msg += line
                line = self._readLine()
                if line <> end_error:
                    error = 1

            return error
        else:
            return 0
        
    def getLatestErrorMessage(self):
        """
        Return the last error message generated by SAS Content Categorization
        Server.
        """
        if self.ver_major >= 3 and self.ver_minor >= 5:
            return self.error_msg.rstrip().replace('\n', ';')
        else:
            return "CatConClient:getLatestErrorMessage only works with SAS Content Categorization Server protocol version 3.5 and above\n"


    def listAvailableCategories(self, projects=[]):
        """
        Returns a map of project name -> list of categories available in that
        project.

        --------

        projects: An optional parameter listing the projects that you want to
        list individual categories from.  If unspecified, the current
        categorization project is used.
        """

        self._acquireConnection()

        if self.current_mcat_project == -1:
            self._close()
            raise CatConError("No categorization projects loaded")

        project_to_category_map = {}

        projects_copy = projects[0:len(projects)]
        if len(projects_copy) == 0:
            projects_copy.append(self.mcat_projects[self.current_mcat_project - 1])

        try:
            for project in projects_copy:
                id = self._categoryProjectToId(project)
                if id != -1:
                    self._write(("SET_MCAT_PROJECT %d\nLIST_CATEGORIES\n" % id))
                    project_to_category_map[project] = self._readAvailableCategories()

            if self._getErrorMessage():
                raise CatConServerError(self.getLatestErrorMessage())
            
            self._releaseConnection()
        except Exception, e:
            self._close()
            project_to_catresult_map = {}
            raise e
        
        return project_to_category_map

    def listAvailableCategoriesWithMetadata(self, projects=[]):
        """
        Returns a map of project name -> category hashes.  The category hashes
        contain all the metadata for each category.  Key values for the hashes
        are as follows.

        * name: The name of the category
        * metadata: The description for the category
        * unique_id: The unique ID for the category
        * comments: The comments for the category
        * links: The related links for the category
        * author: The author of the category
        * cdate: The creation date for the category
        * mdate: The modification date for the category
        * rulestatus: The status of the category (1 = Completed, 2 = Pending)

        --------

        projects: An optional parameter listing the projects that you want to
        list individual categories from.  If unspecified, the current
        categorization project is used.
        """
        self._acquireConnection()

        if self.current_mcat_project == -1:
            self._close()
            raise CatConError("No categorization projects loaded")

        project_to_category_map = {}

        projects_copy = projects[0:len(projects)]
        if len(projects_copy) == 0:
            projects_copy.append(self.mcat_projects[self.current_mcat_project - 1])

        try:
            for project in projects_copy:
                id = self._categoryProjectToId(project)
                if id != -1:
                    list_command = "SET_MCAT_PROJECT %d\n" % (id)
                    if self.ver_major >= 3 :
                        list_command += "SET_GET_MCAT_METADATA Y\n"

                    if self.ver_major >= 3 and self.ver_minor > 0 :
                        list_command += "SET_GET_MCAT_UNIQUEID_METADATA Y\n"
                    if self.ver_major >= 3 and self.ver_minor > 1:
                        list_command += "SET_GET_MCAT_COMMENTS_METADATA Y\nSET_GET_MCAT_LINKS_METADATA Y\nSET_GET_MCAT_AUTHOR_METADATA Y\nSET_GET_MCAT_CDATE_METADATA Y\nSET_GET_MCAT_MDATE_METADATA Y\n"
                    if self.ver_major >= 3 and self.ver_minor > 3:
                        list_command += "SET_GET_MCAT_RULESTATUS_METADATA Y\n"
                    list_command += "LIST_CATEGORIES_WITH_METADATA\n" 
                    self._write(list_command)
                    project_to_category_map[project] = self._readAvailableCategoriesWithMetadata()

            if self._getErrorMessage():
                raise CatConServerError(self.getLatestErrorMessage())
            
            self._releaseConnection()
        except Exception, e:
            self._close()
            project_to_catresult_map = {}
            raise e
        
        return project_to_category_map


    def listAvailableCategorizationProjects(self):
        """
        Lists the available categorization projects available on the CatCon
        server.
        """

        self._acquireConnection()
        self._releaseConnection()
        
        return self.mcat_projects[0:len(self.mcat_projects)]

    def listAvailableLiExtractionProjects(self):
        """
        Lists the available liti extraction projects on the CatCon server
        """        
        self._acquireConnection()
        self._releaseConnection()

        if self.ver_major < 3 or (self.ver_major == 3 and self.ver_minor < 7):
            raise CatConError("listAvailableLiExtractionProjects only works with SAS Content Categorization Server version 3.7 and above")
        
        return self.liti_projects[0:len(self.liti_projects)]


    def listAvailableConceptExtractionProjects(self):
        """
        Lists the available concept extraction projects available on the CatCon
        server.
        """

        self._acquireConnection()
        self._releaseConnection()
        
        return self.concepts_projects[0:len(self.concepts_projects)]

    def _parseCategoryLine(self, document, line):
        metadata_list = []
        pos_str=""
        result=None
        above_relevancy_cutoff = 1

        if(self.ver_major == 3):
            if(self.ver_minor == 0):
                match=v0_regex.match(line)
                if match:
                    cat=match.group(1)
                    relevancy=float(match.group(2))
                    pos_str=match.group(3)
            elif(self.ver_minor == 1):
                match=v1_regex.match(line)
                if match:
                    cat=match.group(1)
                    relevancy=float(match.group(2))
                    metadata_list.append(match.group(3))
                    pos_str=match.group(4)
            elif(self.ver_minor == 2):
                match=v2_regex.match(line)
                if match:
                    cat=match.group(1)
                    relevancy=float(match.group(2))
                    metadata_list.append(match.group(3))
                    metadata_list.append(match.group(4))
                    metadata_list.append(match.group(5))
                    metadata_list.append(match.group(6))
                    metadata_list.append(match.group(7))
                    metadata_list.append(match.group(8))
                    metadata_list.append(match.group(9))
                    pos_str=match.group(10)
            elif(self.ver_minor == 3):
                match=v3_regex.match(line)
                if match:
                    cat=match.group(1)
                    relevancy=float(match.group(2))
                    above_relevancy_cutoff=int(match.group(3))
                    metadata_list.append(match.group(4))
                    metadata_list.append(match.group(5))
                    metadata_list.append(match.group(6))
                    metadata_list.append(match.group(7))
                    metadata_list.append(match.group(8))
                    metadata_list.append(match.group(9))
                    metadata_list.append(match.group(10))
                    pos_str=match.group(11)
            elif(self.ver_minor >= 4):
                match=v4_regex.match(line)
                if match:
                    cat=match.group(1)
                    relevancy=float(match.group(2))
                    above_relevancy_cutoff=int(match.group(3))
                    metadata_list.append(match.group(4))
                    metadata_list.append(match.group(5))
                    metadata_list.append(match.group(6))
                    metadata_list.append(match.group(7))
                    metadata_list.append(match.group(8))
                    metadata_list.append(match.group(9))
                    metadata_list.append(match.group(10))
                    metadata_list.append(match.group(11))
                    pos_str=match.group(12)

            if match:
                result = CategorizationResult(document, cat, relevancy, above_relevancy_cutoff, metadata_list)

                for hit in string.split(pos_str, ","):
                    hit_match = match_regex.match(hit)
                    if hit_match:
                        result._addMatch(int(hit_match.group(1)), int(hit_match.group(2)))

        return result

    def _parseCategoryBlock(self, document, line):
        metadata_list = []
        pos_str=""
        result=None
        above_relevancy_cutoff = 1

        # Category name
        cat = line.rstrip('\n')

        # Category relevance
        match = re.compile("([\d\.]*),(\d)").match(self._readLine().rstrip('\n'))
        if match:
            relevancy=float(match.group(1))
            above_relevancy_cutoff=int(match.group(2))

        # There are currently 8 metadata fields (description, unique ID, 
	# comments, related links, author, creation data, modification date,
	# and rule status)
        for i in range(0, 8):
            metadata_list.append(self._readLine().rstrip('\n'))

        # Match positions
        pos_str = self._readLine().rstrip('\n')

        result = CategorizationResult(document, cat, relevancy, above_relevancy_cutoff, metadata_list)

        nb_hits = 0
        for hit in string.split(pos_str, ","):
            hit_match = match_regex.match(hit)
            if hit_match:
                result._addMatch(int(hit_match.group(1)), int(hit_match.group(2)))
                nb_hits = nb_hits + 1

        # Synonym replacements
        if(self.ver_major > 3 or (self.ver_major == 3 and
                                  self.ver_minor >= 16)):
            orig_terms = self._readLine().rstrip('\n')
            i = 0
            for hit in string.split(orig_terms, "\t"):
                if i < nb_hits:
                    result._addOrigTerm(hit, i)
                    i = i + 1

        return result
                
    def _readCategoryResult(self, document):
        categorization_result_list = []
        count = 0

        t1 = datetime.datetime.now()

        match = nb_categories_regex.match(self._readLine())
        
        t2 = datetime.datetime.now()
        self.wait_time += _timeDiff(t1, t2)        

        if match:
            nb_cats = int(match.group(1))

            # Get synonym replacement text (if applicable)
            if((self.ver_major > 3 or (self.ver_major == 3 and
                                       self.ver_minor >= 16))
               and nb_cats > 0):
                encoding = self._readLine().rstrip('\n')
                
                if encoding != NO_ORIG_TERM:
                    new_doc_text = self._readLine().rstrip('\n')
                    try:
                        u = unicode(_unescapeString(new_doc_text), encoding)
                        document = u.encode("utf-8")
                    except UnicodeDecodeError:
                        raise CatConError("ERROR: synonym text is not in encoding format %s") % (encoding)

            for i in range(0, nb_cats):
                if(self.ver_major > 3 or
                   (self.ver_major >= 3 and self.ver_minor >= 14)):
                    result = self._parseCategoryBlock(document,self._readLine())
                else:
                    result = self._parseCategoryLine(document,self._readLine())
                count = count + 1
                categorization_result_list.append(result)

        return categorization_result_list

    def categorize(self, document,
                   format="text",
                   document_name="",
                   projects=[],
                   relevancy_type=2,
                   use_longest_match=0,
                   use_relevancy_cutoff=1,
                   default_relevancy_cutoff=0.0,
                   skip_qualifier_matches=False,
                   get_original_terms=False):
        """
        Categorizes a document and returns a map of project names -> list of
        CategorizationResults.

        Note that all arguments below are optional: if not specified, the
        default values shown above will be used.

        --------

        document: The text of the document to send to SAS Content
        Categorization Server.
        
        format: The document format, i.e. 'text' or 'xml'.  XML documents are
        processed on a per-field basis.

        document_name: The name of the source document.
        
        projects: A list of categorization projects that you want to match
        against.  If this is an empty list, the current categorization project
        (generally the first one listed in the SAS Content Categorization
        Server configuration file) is used.

        relevancy_type: Specifies the relevancy algorithm to use when
        computing relevancy for categories.  Valid values are
        1 (operator-based), 2 (frequency-based), or 3 (zone-based).

        use_longest_match: If set to 1, substring matches in category rules
        will not be returned.  For example, if your rule is (OR,'business',
        'business partner'), a document containing the phrase
        'business partner' will contain 1 match for the rule, not 2).

        use_relevancy_cutoff:  By default, category matches that fall
        below the relevancy cutoff will not be included in the category result
        set.  If this argument is set to 0, documents below the threshold will
        also be included in the result set.

        default_relevancy_cutoff: Override the relevancy cutoff from the
        MCO file with a value at run-time.  If this is 0, the value from the
        MCO file will be used.

        skip_qualifier_matches: If this is True, category rule qualifier terms
        (designated with the _Q suffix) will not be returned as matching terms
        in the category results.

        get_original_terms: If this is True, the terms in your document which
        were replaced by synonym list processing will be computed and returned.
        This requires SAS Content Categorization Server 12.1 or higher, and has
        no effect with older versions.
        """
        return self._categorizeInternal(document,
                                        format,
                                        document_name,
                                        projects,
                                        relevancy_type,
                                        use_longest_match,
                                        use_relevancy_cutoff,
                                        default_relevancy_cutoff,
                                        skip_qualifier_matches,
                                        get_original_terms,
                                        False
                                        )

    def _categorizeInternal(self, document,
                            format="text",
                            document_name="",
                            projects=[],
                            relevancy_type=2,
                            use_longest_match=0,
                            use_relevancy_cutoff=1,
                            default_relevancy_cutoff=0.0,
                            skip_qualifier_matches=False,
                            get_original_terms=False,
                            doingCatAndConcepts = False,
                            ):
        """
        The parameters for this are the same as for categorize(), except for
        the doingCatAndConcepts parameter.  doingCatAndConcepts is True if
        we're doing combined categorization and concept extraction, False
        otherwise.
        """

        project_to_catresult_map = {}
        try:
            self._acquireConnection()

            if self.current_mcat_project == -1:
                if(not doingCatAndConcepts):
                    self._releaseConnection()
                    raise CatConError("No Category Projects Loaded")
                else:
                    return project_to_catresult_map
        
            request = StringIO()
        
            request.write(("SET_DOCUMENT_FORMAT %s\n" % format))
            request.write("SET_GET_MATCH_POSITIONS Y\n")
        
            if(self.ver_major >= 3 and self.ver_minor >= 1):
                request.write("SET_GET_MCAT_METADATA Y\n")
            
            if(self.ver_major >= 3 and self.ver_minor >= 2):
                request.write("SET_GET_MCAT_UNIQUEID_METADATA Y\n")
                request.write("SET_GET_MCAT_COMMENTS_METADATA Y\n")
                request.write("SET_GET_MCAT_LINKS_METADATA Y\n")
                request.write("SET_GET_MCAT_AUTHOR_METADATA Y\n")
                request.write("SET_GET_MCAT_CDATE_METADATA Y\n")
                request.write("SET_GET_MCAT_MDATE_METADATA Y\n")
                request.write(("SET_MCAT_RELEVANCY_TYPE %s\n" % relevancy_type))
                if(use_longest_match):
                    request.write("SET_MCAT_LONGEST_MATCH Y\n")
                else:
                    request.write("SET_MCAT_LONGEST_MATCH N\n")

            if(self.ver_major >= 3 and self.ver_minor >= 3):
                if(use_relevancy_cutoff):
                    request.write("SET_USE_MCAT_RELEVANCY_CUTOFF Y\n")
                else:
                    request.write("SET_USE_MCAT_RELEVANCY_CUTOFF N\n")


            if(self.ver_major >= 3 and self.ver_minor >= 4):
                request.write("SET_GET_MCAT_RULESTATUS_METADATA Y\n")
                if(default_relevancy_cutoff > 0.0):
                    request.write("SET_MCAT_DEFAULT_RELEVANCY_CUTOFF %f \n" % default_relevancy_cutoff)

            if(self.ver_major >= 3 and self.ver_minor >= 5):
                if(skip_qualifier_matches):
                    request.write("SET_SKIP_QUALIFIER_MATCH_POSITIONS Y\n")
                else:
                    request.write("SET_SKIP_QUALIFIER_MATCH_POSITIONS N\n")

            if(self.ver_major > 3 or (self.ver_major == 3 and
                                          self.ver_minor >= 16)):
                if(get_original_terms):
                    request.write("SET_GET_MCAT_ORIGINAL_TERMS Y\n")
                else:
                    request.write("SET_GET_MCAT_ORIGINAL_TERMS N\n")
                    
            if document_name != "":
                request.write(("SET_DOCUMENT_NAME %s\n" % document_name))
            
            doc_contents = _getContents(document)
            request.write(self._formatDocument(doc_contents))
        
            self._write(request.getvalue())
        
            projects_copy = projects[0:len(projects)]
            if len(projects_copy) == 0:
                projects_copy.append(self.mcat_projects[self.current_mcat_project - 1])
                    
            for project in projects_copy:
                id = self._categoryProjectToId(project)
                if id != -1:
                    self._write(("SET_MCAT_PROJECT %d\nCATEGORIZE\n" % id))
                    project_to_catresult_map[project] = self._readCategoryResult(doc_contents)
            if self._getErrorMessage():
                raise CatConServerError(self.getLatestErrorMessage())

            # If we're going to do concept extraction afterwards, don't
            # release the connection, since we'll reuse the same connection
            if(not doingCatAndConcepts):
                self._releaseConnection()

        except Exception, e:
            self._close()
            project_to_catresult_map = {}
            raise e
                
        return project_to_catresult_map

    def _readConceptResult(self, document):
        concept_result_list = []

        t1 = datetime.datetime.now()

        nb_matches = 0
        match = nb_results_regex.match(self._readLine())
        
        t2 = datetime.datetime.now()
        self.wait_time += _timeDiff(t1, t2)

        if match:
            nb_matches = int(match.group(1))
            
            # Get synonym replacement text (if applicable)
            if((self.ver_major > 3 or (self.ver_major == 3 and
                                       self.ver_minor >= 16))
               and nb_matches > 0):
                encoding = NO_ORIG_TERM
                
                match = syn_encoding_regex.match( self._readLine())
                if match:
                    encoding = match.group(1)
            
                if encoding != NO_ORIG_TERM:
                    match = syn_text_regex.match(self._readLine())
                    if match:
                        new_doc_text = match.group(1)
                        try:
                            u = unicode(_unescapeString(new_doc_text), encoding)
                            document = u.encode("utf-8")
                        except UnicodeDecodeError:
                            raise CatConError("ERROR: synonym text is not in encoding format %s") % (encoding)

            for i in range(0, nb_matches):
                metadata_list = []
                
                # RESULT: 1
                self._readLine()

                type = ""
                type_path = ""
                type_line = self._readLine().rstrip()
                match = type_path_regex.match(type_line)
                if self.ver_major==3 and self.ver_minor >= 6 and match:
                    type_path = match.group(1) + '/' + match.group(2)
                    type = match.group(2) 
                else:
                    match = type_regex.match(type_line)
                    if match:
                        type_path = type = match.group(1)

                info = ""
                match = info_regex.match(self._readLine())
                if match:
                    info = match.group(1)

                start_pos = 0
                match = start_pos_regex.match(self._readLine())
                if match:
                    start_pos = int(match.group(1))

                end_pos = 0
                match = end_pos_regex.match(self._readLine())
                if match:
                    end_pos = int(match.group(1))

                rel = 0.0
                cutoff = 1
                if(self.ver_major == 3):
                    if(self.ver_minor >= 3):
                        line = self._readLine()
                        match = concept_rel_regex.match(line)
                        if match:
                            rel = float(match.group(1))
                            cutoff = int(match.group(2))

                # Original term from synonym replacement
                orig_term = ""
                if(self.ver_major > 3 or (self.ver_major == 3 and
                                          self.ver_minor >= 16)):
                    line = self._readLine()
                    match = orig_term_regex.match(line)
                    if match:
                        orig_term = match.group(1)

                if(self.ver_major > 3 or (self.ver_major == 3 and
                                          self.ver_minor >= 14)):
                    # There are currently 8 metadata fields (description,
                    # unique ID, comments, related links, author, creation
                    # date, modification date,and rule status)
                    for i in range(0, 8):
                        metadata_list.append(self._readLine().rstrip('\n'))
                elif(self.ver_major == 3 and self.ver_minor >= 10):
                    line = self._readLine()
                    match=concept_metadata_regex.match(line)
                    if match:
                        metadata_list.append(match.group(1))
                        metadata_list.append(match.group(2))
                        metadata_list.append(match.group(3))
                        metadata_list.append(match.group(4))
                        metadata_list.append(match.group(5))
                        metadata_list.append(match.group(6))
                        metadata_list.append(match.group(7))
                        metadata_list.append(match.group(8))

                result = ConceptExtractionResult(document, type, type_path, info, rel, cutoff, metadata_list)
                result._addMatch(start_pos, end_pos, orig_term)

                concept_result_list.append(result)

                # END
                line = self._readLine()

                
        return concept_result_list


    # This is obsolete used if protocol version is less than 3.15
    def _readLiConceptsResult_old(self, document):
        """
        used to read the concepts result for li.
        """
        concepts_result_list = []
        
        match = nb_results_regex.match(self._readLine())
        if match:
            for i in range(0, int(match.group(1))):
                self._readLine()
                char_start = 0
                char_end = 0
                match = li_char_pos_regex.match(self._readLine())
                if(match):
                    char_start = int(match.group(1))
                    char_end = int(match.group(2))
                    
                word_start = 0
                word_end = 0
                match = li_word_pos_regex.match(self._readLine())
                if(match):
                    word_start = int(match.group(1))
                    word_end = int(match.group(2))

                type = ""
                match = type_regex.match(self._readLine())
                if(match):
                    type = match.group(1)

                match_string = ""
                match = li_match_regex.match(self._readLine())
                if match:
                    match_string = _unescapeString(match.group(1))
                
                concept = ConceptExtractionResult(document, type, "", "");
                concept._addMatch(char_start, char_end)
                concepts_result_list.append(concept)


        return concepts_result_list

    # This is the latest version
    def _readLiConceptsResult(self, document):
        """
        used to read the concepts result for li.
        """
        concepts_result_list = []

        nb_matches = 0
        match = nb_results_regex.match(self._readLine())        
        if match:
            nb_matches = int(match.group(1))

            # Get synonym replacement text (if applicable)
            if((self.ver_major > 3 or (self.ver_major == 3 and
                                       self.ver_minor >= 16))
               and nb_matches > 0):
                encoding = NO_ORIG_TERM
                
                match = syn_encoding_regex.match( self._readLine())
                if match:
                    encoding = match.group(1)
            
                if encoding != NO_ORIG_TERM:
                    match = syn_text_regex.match(self._readLine())
                    if match:
                        new_doc_text = match.group(1)
                        try:
                            u = unicode(_unescapeString(new_doc_text), encoding)
                            document = u.encode("utf-8")
                        except UnicodeDecodeError:
                            raise CatConError("ERROR: synonym text is not in encoding format %s") % (encoding)
            
            for i in range(0, nb_matches):
                self._readLine()
                char_start = 0
                char_end = 0
                match = li_char_pos_regex.match(self._readLine())
                if(match):
                    char_start = int(match.group(1))
                    char_end = int(match.group(2))
                    
                word_start = 0
                word_end = 0
                match = li_word_pos_regex.match(self._readLine())
                if(match):
                    word_start = int(match.group(1))
                    word_end = int(match.group(2))

                type = ""
                match = type_regex.match(self._readLine())
                if(match):
                    type = match.group(1)

                match_string = ""
                match = li_match_regex.match(self._readLine())
                if match:
                    match_string = _unescapeString(match.group(1))

                canonical_string = ""
                canonical_word_start = 0
                canonical_word_end = 0
                canonical_char_start = 0
                canonical_char_end = 0

                line_str=self._readLine()
                match = li_canonical_regex.match(line_str)
                if match:
                    canonical_string = _unescapeString(match.group(1))
                    match = li_char_pos_regex.match(self._readLine())
                    if(match):
                        canonical_char_start = int(match.group(1))
                        canonical_char_end = int(match.group(2))
                    match = li_word_pos_regex.match(self._readLine())
                    if(match):
                        canonical_word_start = int(match.group(1))
                        canonical_word_end = int(match.group(2))
                    line_str=self._readLine()
                                        
                info_string = ""
                match = li_info_regex.match(line_str)
                if match:
                    info_string = _unescapeString(match.group(1))
                    
                fullpath_string = ""
                match = li_fullpath_regex.match(self._readLine())
                if match:
                    fullpath_string = _unescapeString(match.group(1))
                    fullpath_string = fullpath_string

                # Original term from synonym replacement
                orig_term = ""
                if(self.ver_major > 3 or (self.ver_major == 3 and
                                          self.ver_minor >= 16)):
                    line = self._readLine()
                    match = orig_term_regex.match(line)
                    if match:
                        orig_term = match.group(1)
                
                concept = ConceptExtractionResult(document, type, fullpath_string, info_string);
                concept._addMatch(char_start, char_end, orig_term)
                concept._addCanonicalForm(canonical_string,canonical_char_start,canonical_char_end)

                concepts_result_list.append(concept)


        return concepts_result_list


    # this is obsolete used if protocol is less than 3.15
    def _readLiFactResult_old(self, document):
        fact_result_list = []

        t1 = datetime.datetime.now()
        
        match = nb_results_regex.match(self._readLine())
        
        t2 = datetime.datetime.now()
        self.wait_time += _timeDiff(t1, t2)

        if match:
            for i in range(0, int(match.group(1))):

                self._readLine()
                
                char_start = 0
                char_end = 0
                match = li_char_pos_regex.match(self._readLine())
                if(match):
                    char_start = int(match.group(1))
                    char_end = int(match.group(2))
                    
                word_start = 0
                word_end = 0
                match = li_word_pos_regex.match(self._readLine())
                if(match):
                    word_start = int(match.group(1))
                    word_end = int(match.group(2))
                    
                fact = ""
                match = li_fact_regex.match(self._readLine())
                if(match):
                    fact = match.group(1)

                match_string = ""
                match = li_match_regex.match(self._readLine())
                if match:
                    match_string = match.group(1)
                
                context = ""
                match = li_context_regex.match(self._readLine())
                if(match):
                    context = match.group(1)
                self._readLine()  # read End
                nb_args = 0
                match = li_nb_args_regex.match(self._readLine())
                if match:
                    nb_args = int(match.group(1))
                    
                fact_result = FactExtractionResult(_unescapeString(match_string), fact, char_start, char_end, word_start, word_end)
                fact_result.Context = _unescapeString(context)
                
                for i in range(0, nb_args):
                    arg_type = ""
                    match = li_arg_type_regex.match(self._readLine())
                    if match:
                        arg_type = match.group(1)
                    arg_str = ""
                    match = li_arg_string_regex.match(self._readLine())
                    if match:
                        arg_str = _unescapeString(match.group(1))
                    fact_result._addArgs(arg_str, arg_type)

                #self._readLine()   
                fact_result_list.append(fact_result)

        return fact_result_list


#This is the latest version
    def _readLiFactResult(self, document):
        fact_result_list = []

        t1 = datetime.datetime.now()
        
        match = nb_results_regex.match(self._readLine())
        
        t2 = datetime.datetime.now()
        self.wait_time += _timeDiff(t1, t2)

        if match:
            for i in range(0, int(match.group(1))):

                self._readLine()
                
                char_start = 0
                char_end = 0
                match = li_char_pos_regex.match(self._readLine())
                if(match):
                    char_start = int(match.group(1))
                    char_end = int(match.group(2))
                    
                word_start = 0
                word_end = 0
                match = li_word_pos_regex.match(self._readLine())
                if(match):
                    word_start = int(match.group(1))
                    word_end = int(match.group(2))
                    
                fact = ""
                match = li_fact_regex.match(self._readLine())
                if(match):
                    fact = match.group(1)

                match_string = ""
                match = li_match_regex.match(self._readLine())
                if match:
                    match_string = match.group(1)
                
                context = ""
                match = li_context_regex.match(self._readLine())
                if(match):
                    context = match.group(1)

                fullpath = ""
                match = li_fullpath_regex.match(self._readLine())
                if(match):
                    fullpath = match.group(1)

                self._readLine()  # read End
                nb_args = 0
                match = li_nb_args_regex.match(self._readLine())
                if match:
                    nb_args = int(match.group(1))
                    
                fact_result = FactExtractionResult(_unescapeString(match_string), fact, char_start, char_end, word_start, word_end)
                fact_result.Context = _unescapeString(context)
                fact_result.FactTypePath = _unescapeString(fullpath)
                
                for i in range(0, nb_args):
                    arg_type = ""
                    match = li_arg_type_regex.match(self._readLine())
                    if match:
                        arg_type = match.group(1)
                    arg_str = ""
                    match = li_arg_string_regex.match(self._readLine())
                    if match:
                        arg_str = _unescapeString(match.group(1))
                    fact_result._addArgs(arg_str, arg_type)

                #self._readLine()   
                fact_result_list.append(fact_result)

        return fact_result_list



    def getWaitTime(self):
        """
        Return the amount of time that SAS Content Categorization Server has
        been idle since the last time this value was cleared.  Used primarily
        in performance testing.
        """
        return self.wait_time

    def clearWaitTime(self):
        """
        Clear the counter representing the idle time of SAS Content
        Categorization Server.
        """
        self.wait_time=0

    def extractConcepts(self,
                        document,
                        format = "text",
                        document_name = "",
                        projects = [],
                        relevancy_type = 0,
                        match_type = 1,
                        activate_all_concepts = False,
                        active_concepts = {},
                        concepts_fields = [],
                        concepts_inline_fields = {},
                        concept_relevancy_cutoff = {},
                        use_relevancy_cutoff = 0,
                        default_relevancy_cutoff=0.0,
                        active_subtrees_concepts = [],
                        get_original_terms = False):
        """
        Does concept extraction on a document and returns a map of project
        names -> list of ConceptExtractionResults.

        Note that all arguments below are optional: if not specified, the
        default values shown above will be used.

        --------

        document: The text of the document to send to SAS Content
        Categorization Server.
        
        format: The document format, i.e. 'text' or 'xml'.  XML documents are
        processed on a per-field basis.

        document_name: The name of the source document.
        
        projects: A list of concept extraction projects that you want to
        match against.  If this is an empty list, the current concept
        extraction project (generally the first one listed in the SAS Content
        Categorization Server configuration file) is used.

        relevancy_type: Specifies the relevancy algorithm to use when
        computing relevancy for concepts.  Valid values are
        1 (frequency-based) or 2 (zone-based).
                
        match_type: Controls how concepts are matched if one concept is a
        subset of another (e.g. George Washington, the person, vs. George
        Washington Bridge, the location).  Valid values are 0 (all matches),
        1 (longest match), or 2 (shortest match).

        activate_all_concepts: True to activate all concepts in the current
        project, False otherwise.  If this is False, individual concepts must
        be activated using the active_concepts map or active_subtree_concepts
        list, or you will not get any results.

        active_concepts: A map of concept names -> Boolean values indicating
        which individual concepts should be activated or deactivated.  Works in
        conjunction with the activate_all_concepts parameter.

        concepts_fields: A list of XML fields to use when searching for concept
        matches in documents.  Any matches not in these fields will not be
        returned.  Requires the document type to be 'xml'.

        concepts_inline_fields: A map of concept names to field names.
        Multiple field names can be specified for each concept, separated by
        commas.  This controls the inline tags that will be generated within
        the document.

        concept_relevancy_cutoff: A map of concept names to relevancy cutoff
        values.  Unlike with categories, individual concepts can specify their
        relevancy cutoff values at runtime.

        use_relevancy_cutoff:  By default, concept matches that fall
        below the relevancy cutoff will not be included in the concept result
        set.  If this argument is set to 0, documents below the threshold will
        also be included in the result set.

        default_relevancy_cutoff: Override the relevancy cutoff from the
        CONCEPTS file with a value at run-time.  If this is 0, the value from
        the CONCEPTS file will be used.

        active_subtree_concepts: A list of concepts whose subtrees should be
        activated or deactivated.  The list must consist of tuples in the form
        (<concept_name>, 0|1), where 1 indicates the subtree should be
        activated and 0 indicates it should be deactivated.  For each concept
        listed, that concept and all its children will be automatically
        activated or deactivated.  Note that for this parameter to have any
        effect, the activate_all_concepts parameter to this method must be set
        to False.

        get_original_terms: If this is True, the terms in your document which
        were replaced by synonym list processing will be computed and returned.
        This requires SAS Content Categorization Server 12.1 or higher, and has
        no effect with older versions.
        """
        return self._extractConceptsInternal(document,
                                             format,
                                             document_name,
                                             projects,
                                             relevancy_type,
                                             match_type,
                                             activate_all_concepts,
                                             active_concepts,
                                             concepts_fields,
                                             concepts_inline_fields,
                                             concept_relevancy_cutoff,
                                             use_relevancy_cutoff,
                                             default_relevancy_cutoff,
                                             active_subtrees_concepts,
                                             get_original_terms,
                                             False)
    

    def _extractConceptsInternal(self,
                                 document,
                                 format = "text",
                                 document_name = "",
                                 projects = [],
                                 relevancy_type = 0,
                                 match_type = 1,
                                 activate_all_concepts = False,
                                 active_concepts = {},
                                 concepts_fields = [],
                                 concepts_inline_fields = {},
                                 concept_relevancy_cutoff = {},
                                 use_relevancy_cutoff = 0,
                                 default_relevancy_cutoff = 0.0,
                                 active_subtrees_concepts = [],
                                 get_original_terms = False,
                                 doingCatAndConcepts = False):
        """
        The parameters for this are the same as for extractConcepts(), except
        for the doingCatAndConcepts parameter.  doingCatAndConcepts is True if
        we're doing combined categorization and concept extraction, False
        otherwise.
        """

        project_to_conceptresult_map = {}
        try:
            # If we already did categorization, the connection is already open
            if(not doingCatAndConcepts):
                self._acquireConnection()

            if self.current_concepts_project == -1:
                if(not doingCatAndConcepts):
                    self._releaseConnection()
                    raise CatConError("No Concept Projects Loaded")
                else:
                    return project_to_conceptresult_map

            if projects == 'all':
                projects = self.concepts_projects

            request = StringIO()
            request.write("SET_DOCUMENT_FORMAT %s\n" % format)

            if document_name != "":
                request.write("SET_DOCUMENT_NAME %s\n" % document_name)

            request.write("SET_CONCEPTS_RELEVANCY_TYPE %d\n" % relevancy_type)
        
            request.write("SET_CONCEPTS_MATCH_TYPE %d\n" % match_type)

            if activate_all_concepts:
                request.write("SET_CONCEPTS_ALL_ACTIVATED Y\n")
            else:
                if(len(active_concepts) > 0 or len(active_subtrees_concepts) > 0):
                    request.write("SET_CONCEPTS_ALL_ACTIVATED N\n")

            for active_subtree_setting in active_subtrees_concepts:
                request.write("SET_CONCEPT_SUBTREE_ACTIVE %s" % active_subtree_setting[0])
                if active_subtree_setting[1]:
                    request.write(" Y\n")
                else:
                    request.write(" N\n")

            
            for active_concept in active_concepts.keys():
                request.write(("SET_CONCEPT_ACTIVE %s" % active_concept))
                if active_concepts[active_concept]:
                    request.write(" Y\n")
                else:
                    request.write(" N\n")        

            if format == "xml":
                if len(concepts_fields) > 0:
                    request.write("SET_CONCEPTS_FIELD ")
                    request.write(",".join(concepts_fields))
                    request.write("\n")

                for concept in concepts_inline_fields.keys():
                    fields = concepts_inline_fields[concept]
                    request.write("SET_CONCEPTS_INLINE_FIELD ")
                    request.write(concept)
                    request.write(":")
                    #request.write(",".join(fields))
                    request.write(fields)
                    request.write("\n")

            for concept in concept_relevancy_cutoff.keys():
                request.write("SET_CONCEPT_RELEVANCY_CUTOFF %s:%f\n" % (concept, concept_relevancy_cutoff[concept]))

            if(self.ver_major >= 3 and self.ver_minor >= 3):
                if(use_relevancy_cutoff):
                    request.write("SET_USE_CONCEPTS_RELEVANCY_CUTOFF Y\n")
                if self.ver_minor >=4:
                    if(default_relevancy_cutoff > 0.0):
                        request.write("SET_CONCEPT_DEFAULT_RELEVANCY_CUTOFF %f \n" % (default_relevancy_cutoff))
                else:
                    request.write("SET_USE_CONCEPTS_RELEVANCY_CUTOFF N\n")

            if(self.ver_major > 3 or (self.ver_major == 3
                                      and self.ver_minor >= 10)):
               request.write("SET_GET_CONCEPTS_METADATA Y\n")
               request.write("SET_GET_CONCEPTS_UNIQUEID_METADATA Y\n")
               request.write("SET_GET_CONCEPTS_COMMENTS_METADATA Y\n")
               request.write("SET_GET_CONCEPTS_LINKS_METADATA Y\n")
               request.write("SET_GET_CONCEPTS_AUTHOR_METADATA Y\n");
               request.write("SET_GET_CONCEPTS_CDATE_METADATA Y\n");
               request.write("SET_GET_CONCEPTS_MDATE_METADATA Y\n");
               request.write("SET_GET_CONCEPTS_RULESTATUS_METADATA Y\n");

            if(self.ver_major > 3 or (self.ver_major == 3 and
                                      self.ver_minor >= 16)):
                if(get_original_terms):
                    request.write("SET_GET_CONCEPTS_ORIGINAL_TERMS Y\n")
                else:
                    request.write("SET_GET_CONCEPTS_ORIGINAL_TERMS N\n")

            # If we already did categorization, the document was already sent
            # to the server...
            # ZAWISZA 2009-12-15: unless we had no categorization projects
            # loaded
            doc_contents = _getContents(document)
            if(not doingCatAndConcepts):
                request.write(self._formatDocument(doc_contents))
            else:
                if self.current_mcat_project == -1:
                    request.write(self._formatDocument(doc_contents))

            self._write(request.getvalue())

            projects_copy = projects[0:len(projects)]
            if len(projects_copy) == 0:
                projects_copy.append(self.concepts_projects[self.current_concepts_project - 1])

            for project in projects_copy:
                id = self._conceptProjectToId(project)
                if id != -1:
                    self._write(("SET_CONCEPTS_PROJECT %d\nEXTRACT_CONCEPTS\n" % id))
                    project_to_conceptresult_map[project] = self._readConceptResult(doc_contents)
            if self._getErrorMessage():
                raise CatConServerError(self.getLatestErrorMessage())

            self._releaseConnection()

        except Exception, e:
            self._close()
            project_to_conceptresult_map = {}
            raise e

        return project_to_conceptresult_map


    def ExtractLiFacts(self,
                       document,
                       document_name = "",
                       format = "text",
                       projects = [],
                       li_word_concordance = 0,
                       li_char_concordance = 0,
                       li_match_type =0,
                       li_longest_match = True,
                       li_remove_duplicates = False,
                       li_with_concordance = False,
                       li_canonical_label = False,
                       get_original_terms = False):
        """
        Performs LITI contextual extraction on a document and returns a map
        of project names -> tuples.  The first element in the tuple is a list
        of FactExtractionResults, representing facts.  The second element is a
        list of ConceptExtractionResults, representing words and phrases from
        the document.

        Note that all arguments below are optional: if not specified, the
        default values shown above will be used.
 
        --------
 
        document: The text of the document to send to SAS Content
        Categorization Server.

        document_name: The name of the source document.
         
        format: The document format, i.e. 'text' or 'xml'.
 
        projects: A list of LITI contextual extraction projects that you want
        to match against.  If this is an empty list, the current contextual
        extraction project (generally the first one listed in the SAS Content
        Categorization Server configuration file) is used.

        li_word_concordance: The number of words before and after the matching
        string to display in the optional concordance result.  To use this,
        the li_char_concordance parameter should be 0, and li_with_concordance
        must be set to True.

        li_char_concordance: The number of characters before and after the
        matching string to display in the optional concordance result.  To use
        this, the li_word_concordance parameter should be 0, and
        li_with_concordance must be set to True.

        li_match_type: Controls how contextual extraction concepts are matched
        if one concept is a subset of another (e.g. George Washington, the
        person, vs. George Washington Bridge, the location) and your rules do
        not disambiguate the match.  Valid values are 0 (all matches returned),
        1 (the longest match is returned), and 2 (the best match, i.e. the one
        with the highest priority, is returned).

        li_longest_match: Only applicable when li_match_type is set to 1
        (longest match) or 2 (best match).  If this is True, all overlapping
        matches within a single concept will be returned; otherwise, only the
        first of the overlapping matches is returned.

        li_remove_duplicates: If set to True, facts that are duplicates of
        other facts will be removed from the list of fact matches.
        
        li_with_concordance: If set to True, concordance information about fact
        matches, i.e. information about the context in which those facts
        occurred in the document, will be returned.

        li_canonical_label: If set to True, the canonical form of concept
        matches using coreference resolution will be returned.

        get_original_terms: If this is True, the terms in your document which
        were replaced by synonym list processing will be computed and returned.
        This requires SAS Content Categorization Server 12.1 or higher, and has
        no effect with older versions.  Note that this only applies to
        concepts, not facts.
        """
        project_to_factresult_map = {}
        try:
            self._acquireConnection()
            if self.ver_major < 3 or (self.ver_major == 3 and self.ver_minor < 7):
                raise CatConError("ExtractLiFacts only works with SAS Content Categorization Server version 3.7 and above")

            if self.current_liti_project == -1:
                self._releaseConnection()
                raise CatConError("No LiTi projects Loaded.")

            if projects == 'all':
                projects = self.liti_projects

            request = StringIO()
            request.write("SET_DOCUMENT_FORMAT %s\n" % format)
            
            if document_name != "":
                request.write("SET_DOCUMENT_NAME %s\n" % document_name)

            request.write("SET_LITI_WORD_CONCORDANCE %d\n" % li_word_concordance)
            request.write("SET_LITI_CHAR_CONCORDANCE %d\n" % li_char_concordance)
            request.write("SET_LITI_MATCH_TYPE %d\n" % li_match_type)
            
            if li_longest_match:
                request.write("SET_LITI_LONGEST_MATCH_TYPE 1\n")
            else:
                request.write("SET_LITI_LONGEST_MATCH_TYPE 0\n")

            if li_remove_duplicates:
                request.write("SET_LITI_REMOVE_DUPLICATE_PREDICATES Y\n")
            else:
                request.write("SET_LITI_REMOVE_DUPLICATE_PREDICATES N\n")

            if li_with_concordance:
                request.write("SET_LITI_WITH_CONCORDANCE Y\n")
            else:
                request.write("SET_LITI_WITH_CONCORDANCE N\n")

            if self.ver_major > 3 or (self.ver_major == 3 and self.ver_minor >= 15):
                if li_canonical_label:
                    request.write("SET_LITI_OUTPUT_CANONICAL_LABEL Y\n")
                else:
                    request.write("SET_LITI_OUTPUT_CANONICAL_LABEL N\n")

            if(self.ver_major > 3 or (self.ver_major == 3 and
                                          self.ver_minor >= 16)):
                if(get_original_terms):
                    request.write("SET_GET_LITI_ORIGINAL_TERMS Y\n")
                else:
                    request.write("SET_GET_LITI_ORIGINAL_TERMS N\n")
            
            doc_contents = _getContents(document)
            request.write(self._formatDocument(doc_contents))
            
            self._write(request.getvalue())

            projects_copy = projects[0:len(projects)]
            if len(projects_copy) == 0:
                projects_copy.append(self.liti_projects[self.current_liti_project -1])

            for project in projects_copy:
                id = self._litiProjectToId(project)
                if id != -1:
                    self._write(("SET_LITI_PROJECT %d\nEXTRACT_LITI_FACTS\n" % id))
                    project_to_factresult_map[project] = []
                    
                    if self.ver_major > 3 or (self.ver_major == 3 and self.ver_minor >=15):
                           fact_list = self._readLiFactResult(doc_contents)
                           concepts_list = self._readLiConceptsResult(doc_contents)
                    else:
                           fact_list = self._readLiFactResult_old(doc_contents)
                           concepts_list = self._readLiConceptsResult_old(doc_contents)
                    
                    project_to_factresult_map[project].append(fact_list)
                    project_to_factresult_map[project].append(concepts_list)

            if self._getErrorMessage():
                raise CatConServerError(self.getLatestErrorMessage())

            self._releaseConnection()

        except Exception, e:
            self._close()
            project_to_factresult_map = {}
            raise e

        return project_to_factresult_map


    def _readLanguageIdResult(self):
        language_id_list = []

        line = self._readLine()
        match = nb_results_regex.match(line)
        if match:
            nb_results = int(match.group(1))

            for i in range(0, nb_results):
                # _readLine()
                language = self._readLine()
                if record_regex.match(language): language = self._readLine()
                encoding = self._readLine()

                match = language_regex.match(language)
                if match:
                    language = match.group(1)
                else:
                    language = ""

                match = encoding_regex.match(encoding)
                if match:
                    encoding = match.group(1)
                else:
                    encoding = ""

                language_id_list.append(LanguageIdResult(language, encoding))

        return language_id_list

    def identifyLanguages(self, document, document_name="", bilingual=0):
        """
        Given an input document, returns a list of LanguageIdResults.

        All parameters below are options.
        --------

        document_name: The name of the input document.

        bilingual: Whether the input document contains multiple languages.
        """

        self._acquireConnection()

        request = StringIO()
        if bilingual:
            request.write("SET_DOCUMENT_BILINGUAL Y\n")
        else:
            request.write("SET_DOCUMENT_BILINGUAL N\n")

        if document_name != "":
            request.write(("SET_DOCUMENT_NAME %s\n" % document_name))

        request.write(self._formatDocument(_getContents(document)))
        request.write("ID_LANGUAGE\n")
        
        self._write(request.getvalue())

        language_id_list = self._readLanguageIdResult()

        self._releaseConnection()
        
        return language_id_list

    def update_project(self,
                       filename,
                       user,
                       password,
                       project_name,
                       project_type):


        """
        Update a project loaded on the server.

        --------

        filename: The MCO or CONCEPTS binary to upload to SAS Content
        Categorization Server.

        user: The user name used to log on to SAS Content Categorization
        Server.

        password: The password used to log on to SAS Content Categorization
        Server.

        project_name: The name of the project to update on SAS Content
        Categorization Server.

        project_type: The type of the project (T_MCATOBJ for category projects,
        T_CONCEPTS for concepts projects, T_LITI for contextual extraction
        projects).
        """
        f = open(filename,"rb")
        data = None;
        if f:
            data = f.read()
            f.close()
        else:
            print "Can't open specfied file: %s \n" % (filename)
            return
        length = len(data)
        self._acquireConnection()

        if (project_type == "T_LITI" and
            (self.ver_major < 3 or (self.ver_major == 3 and self.ver_minor < 9))):
            raise CatConError("LITI uploading requires SAS Content Categorization Server version 3.9 or higher (current version %d.%d)" % (self.ver_major, self.ver_minor))

        # Need to turn on feedback for things to work properly
        self._write("FEEDBACK Y\n")
        self._waitForResponse("OK")

        self._write("UPDATE_PROJECT\n%s\n%s\n" % (user,password))
        status = self._waitForResponse("OK")
        if(status == False):
            if self._getErrorMessage():
                self._releaseConnection()
                raise CatConServerError(self.getLatestErrorMessage())

        self._write("%s\n%s\n" % (project_name,project_type))
        status = self._waitForResponse("OK")
        if(status == False):
            if self._getErrorMessage():
                self._releaseConnection()
                raise CatConServerError(self.getLatestErrorMessage())

        self._write("%d\n" % (length))
        status = self._waitForResponse("READY")
        if(status == False):
            if self._getErrorMessage():
                self._releaseConnection()
                raise CatConServerError(self.getLatestErrorMessage())
        
        self._write(data)
        status = self._waitForResponse("OK")
        if(status == False):        
            if self._getErrorMessage():
                self._releaseConnection()
                raise CatConServerError(self.getLatestErrorMessage())

        # Now that we're done, turn feedback back off
        self._write("FEEDBACK N\n")

        self._write("BYE\n");
        
        self._releaseConnection()

    def categorizeAndExtractConcepts(self,
                                     document,
                                     format="text",
                                     document_name="",
                                     cat_projects=[],
                                     cat_relevancy_type=2,
                                     cat_use_longest_match=0,
                                     cat_use_relevancy_cutoff=1,
                                     con_projects = [],
                                     con_relevancy_type = 0,
                                     con_match_type = 2,
                                     activate_all_concepts = True,
                                     active_concepts = {},
                                     concepts_fields = [],
                                     concepts_inline_fields = {},
                                     concept_relevancy_cutoff = {},
                                     con_use_relevancy_cutoff = 0,
                                     cat_default_relevancy_cutoff=0.0,
                                     con_default_relevancy_cutoff=0.0,
                                     skip_qualifier_matches = False,
                                     active_subtrees_concepts = [],
                                     get_original_terms = False):
        """
        Combines the existing categorize and extractConcepts functions, with
        the further optimization of only sending the specified document to the
        server once.  This saves a little time if you're doing categorization
        and concept extraction on the same document.
        
        Returns a pair of maps, one for project name -> list of
        CategorizationResults, one for project name -> list of
        ConceptExtractionResults.

        Note that all arguments below are optional: if not specified, the
        default values shown above will be used.

        --------

        document: The text of the document to send to SAS Content
        Categorization Server.
        
        format: The document format, i.e. 'text' or 'xml'.  XML documents are
        processed on a per-field basis.

        document_name: The name of the source document.
        
        cat_projects: A list of categorization projects that you want to match
        against.  If this is an empty list, the current categorization project
        (generally the first one listed in the SAS Content Categorization
        Server configuration file) is used.

        cat_relevancy_type: Specifies the relevancy algorithm to use when
        computing relevancy for categories.  Valid values are
        1 (operator-based), 2 (frequency-based), or 3 (zone-based).

        cat_use_longest_match: If set to 1, substring matches in category rules
        will not be returned.  For example, if your rule is (OR,'business',
        'business partner'), a document containing the phrase
        'business partner' will contain 1 match for the rule, not 2).

        cat_use_relevancy_cutoff:  By default, category matches that fall
        below the relevancy cutoff will not be included in the category result
        set.  If this argument is set to 0, documents below the threshold will
        also be included in the result set.

        con_projects: A list of concept extraction projects that you want to
        match against.  If this is an empty list, the current concept
        extraction project (generally the first one listed in the SAS Content
        Categorization Server configuration file) is used.

        con_relevancy_type: Specifies the relevancy algorithm to use when
        computing relevancy for concepts.  Valid values are
        1 (frequency-based) or 2 (zone-based).
                
        con_match_type: Controls how concepts are matched if one concept is a
        subset of another (e.g. George Washington, the person, vs. George
        Washington Bridge, the location).  Valid values are 0 (all matches),
        1 (longest match), or 2 (shortest match).

        activate_all_concepts: True to activate all concepts in the current
        project, False otherwise.  If this is False, individual concepts must
        be activated using the active_concepts map or active_subtree_concepts
        list, or you will not get any results.

        active_concepts: A map of concept names -> Boolean values indicating
        which individual concepts should be activated or deactivated.  Works in
        conjunction with the activate_all_concepts parameter.

        concepts_fields: A list of XML fields to use when searching for concept
        matches in documents.  Any matches not in these fields will not be
        returned.  Requires the document type to be 'xml'.

        concepts_inline_fields: A map of concept names to field names.
        Multiple field names can be specified for each concept, separated by
        commas.  This controls the inline tags that will be generated within
        the document.

        concept_relevancy_cutoff: A map of concept names to relevancy cutoff
        values.  Unlike with categories, individual concepts can specify their
        relevancy cutoff values at runtime.

        con_use_relevancy_cutoff:  By default, concept matches that fall
        below the relevancy cutoff will not be included in the concept result
        set.  If this argument is set to 0, documents below the threshold will
        also be included in the result set.

        cat_default_relevancy_cutoff: Override the relevancy cutoff from the
        MCO file with a value at run-time.  If this is 0, the value from the
        MCO file will be used.

        con_default_relevancy_cutoff: Override the relevancy cutoff from the
        CONCEPTS file with a value at run-time.  If this is 0, the value from
        the CONCEPTS file will be used.

        skip_qualifier_matches: If this is True, category rule qualifier terms
        (designated with the _Q suffix) will not be returned as matching terms
        in the category results.

        active_subtree_concepts: A list of concepts whose subtrees should be
        activated or deactivated.  The list must consist of tuples in the form
        (<concept_name>, 0|1), where 1 indicates the subtree should be
        activated and 0 indicates it should be deactivated.  For each concept
        listed, that concept and all its children will be automatically
        activated or deactivated.  Note that for this parameter to have any
        effect, the activate_all_concepts parameter to this method must be set
        to False.

        get_original_terms: If this is True, the terms in your document which
        were replaced by synonym list processing will be computed and returned.
        This requires SAS Content Categorization Server 12.1 or higher, and has
        no effect with older versions.
        """
        cat_result = {}
        con_result = {}
        
        try:
            cat_result = self._categorizeInternal(document,
                                                  format,
                                                  document_name,
                                                  cat_projects,
                                                  cat_relevancy_type,
                                                  cat_use_longest_match,
                                                  cat_use_relevancy_cutoff,
                                                  cat_default_relevancy_cutoff,
                                                  skip_qualifier_matches,
                                                  get_original_terms,
                                                  True)
        except Exception, e:
            raise e        

        try:
            con_result =  self._extractConceptsInternal(document,
                                                        format,
                                                        document_name,
                                                        con_projects,
                                                        con_relevancy_type,
                                                        con_match_type ,
                                                        activate_all_concepts,
                                                        active_concepts,
                                                        concepts_fields,
                                                        concepts_inline_fields,
                                                        concept_relevancy_cutoff,
                                                        con_use_relevancy_cutoff,
                                                        con_default_relevancy_cutoff,
                                                        active_subtrees_concepts,
                                                        get_original_terms,
                                                        True)
        except Exception, e:
            raise e

        return (cat_result, con_result)


    def listAvailableConcepts(self, projects=[]):
        """
        Returns a map of project name -> list of concepts available in that
        project.

        --------

        projects: An optional parameter listing the projects that you want to
        list individual concepts from.  If unspecified, the current
        concept extraction project is used.
        """

        self._acquireConnection()

        if self.ver_major < 3 or (self.ver_major == 3 and self.ver_minor < 10):
            self._close()
            raise CatConError("listAvailableConcepts method requires SAS Content Categorization Server version 3.10 or greater")
        
        if self.current_concepts_project == -1:
            self._close()
            raise CatConError("No concept extraction projects loaded")

        project_to_concept_map = {}

        projects_copy = projects[0:len(projects)]
        if len(projects_copy) == 0:
            projects_copy.append(self.concepts_projects[self.current_concepts_project - 1])

        try:
            for project in projects_copy:
                id = self._conceptProjectToId(project)
                if id != -1:
                    self._write(("SET_CONCEPTS_PROJECT %d\nLIST_CONCEPTS\n" % id))
                    project_to_concept_map[project] = self._readAvailableConcepts()

            if self._getErrorMessage():
                raise CatConServerError(self.getLatestErrorMessage())
            
            self._releaseConnection()
        except Exception, e:
            self._close()
            project_to_conceptresult_map = {}
            raise e
        
        return project_to_concept_map


    def listAvailableLitiConcepts(self, projects=[]):
        """
        Returns a map of project name -> list of liti concepts available in that
        project.

        --------

        projects: An optional parameter listing the projects that you want to
        list individual concepts from.  If unspecified, the current
        concept extraction project is used.
        """

        self._acquireConnection()

        if self.ver_major < 3 or (self.ver_major == 3 and self.ver_minor < 12):
            self._close()
            raise CatConError("listAvailableConcepts method requires SAS Content Categorization Server version 3.12 or greater")
        
        if self.current_liti_project == -1:
            self._close()
            raise CatConError("No LITI concept extraction projects loaded")

        project_to_concept_map = {}

        projects_copy = projects[0:len(projects)]
        if len(projects_copy) == 0:
            projects_copy.append(self.liti_projects[self.current_liti_project - 1])

        try:
            for project in projects_copy:
                id = self._litiProjectToId(project)
                if id != -1:
                    self._write(("SET_LITI_PROJECT %d\nLIST_LITI_CONCEPTS\n" % id))
                    project_to_concept_map[project] = self._readAvailableLitiConcepts()

            if self._getErrorMessage():
                raise CatConServerError(self.getLatestErrorMessage())
            
            self._releaseConnection()
        except Exception, e:
            self._close()
            project_to_conceptresult_map = {}
            raise e
        
        return project_to_concept_map


    def listAvailableLitiPredicates(self, projects=[]):
        """
        Returns a map of project name -> list of liti predicates available in that
        project.

        --------

        projects: An optional parameter listing the projects that you want to
        list individual concepts from.  If unspecified, the current
        concept extraction project is used.
        """

        self._acquireConnection()

        if self.ver_major < 3 or (self.ver_major == 3 and self.ver_minor < 12):
            self._close()
            raise CatConError("listAvailablePredicates method requires SAS Content Categorization Server version 3.12 or greater")
        
        if self.current_liti_project == -1:
            self._close()
            raise CatConError("No LITI projects loaded")

        project_to_predicates_map = {}

        projects_copy = projects[0:len(projects)]
        if len(projects_copy) == 0:
            projects_copy.append(self.liti_projects[self.current_liti_project - 1])

        try:
            for project in projects_copy:
                id = self._litiProjectToId(project)
                if id != -1:
                    self._write(("SET_LITI_PROJECT %d\nLIST_LITI_PREDICATES\n" % id))
                    project_to_predicates_map[project] = self._readAvailableLitiPredicates()

            if self._getErrorMessage():
                raise CatConServerError(self.getLatestErrorMessage())
            
            self._releaseConnection()
        except Exception, e:
            self._close()
            project_to_predicates_map = {}
            raise e
        
        return project_to_predicates_map



    def listAvailableConceptsWithMetadata(self, projects=[]):
        """
        Returns a map of project name -> concept hashes.  The concept hashes
        contain all the metadata for each concept.  Key values for the hashes
        are as follows.

        * name: The name of the concept
        * metadata: The description for the concept
        * unique_id: The unique ID for the concept
        * comments: The comments for the concept
        * links: The related links for the concept
        * author: The author of the concept
        * cdate: The creation date for the concept
        * mdate: The modification date for the concept
        * rulestatus: The status of the concept (1 = Completed, 2 = Pending)

        --------

        projects: An optional parameter listing the projects that you want to
        list individual concepts from.  If unspecified, the current
        concept extraction project is used.
        """
        self._acquireConnection()

        if self.ver_major < 3 or (self.ver_major == 3 and self.ver_minor < 10):
            self._close()
            raise CatConError("listAvailableConceptsWithMetadata method requires SAS Content Categorization Server version 3.10 or greater")

        if self.current_concepts_project == -1:
            self._close()
            raise CatConError("No concept extraction projects loaded")

        project_to_concept_map = {}

        projects_copy = projects[0:len(projects)]
        if len(projects_copy) == 0:
            projects_copy.append(self.concepts_projects[self.current_concepts_project - 1])

        try:
            for project in projects_copy:
                id = self._conceptProjectToId(project)
                if id != -1:
                    list_command = "SET_CONCEPTS_PROJECT %d\n" % (id)
                    list_command += "SET_GET_CONCEPTS_METADATA Y\n"
                    list_command += "SET_GET_CONCEPTS_UNIQUEID_METADATA Y\n"
                    list_command += "SET_GET_CONCEPTS_COMMENTS_METADATA Y\n"
                    list_command += "SET_GET_CONCEPTS_LINKS_METADATA Y\n"
                    list_command += "SET_GET_CONCEPTS_AUTHOR_METADATA Y\n"
                    list_command += "SET_GET_CONCEPTS_CDATE_METADATA Y\n"
                    list_command += "SET_GET_CONCEPTS_MDATE_METADATA Y\n"
                    list_command += "SET_GET_CONCEPTS_RULESTATUS_METADATA Y\n"
                    list_command += "LIST_CONCEPTS_WITH_METADATA\n" 
                    self._write(list_command)
                    project_to_concept_map[project] = self._readAvailableConceptsWithMetadata()

            if self._getErrorMessage():
                raise CatConServerError(self.getLatestErrorMessage())
            
            self._releaseConnection()
        except Exception, e:
            self._close()
            project_to_conceptresult_map = {}
            raise e
        
        return project_to_concept_map


    def _readAvailableConcepts(self):
        available_concepts = []

        match = nb_concepts_regex.match(self._readLine())
        if match:
            for i in range(0, int(match.group(1))):
                # Even though we're looking for concepts, the category_regex
                # regular expression will properly match concepts as well
                match = category_regex.match(self._readLine())
                if match:                    
                    available_concepts.append(match.group(1))
                    
        return available_concepts


    def _readAvailableLitiConcepts(self):
        available_concepts = []

         #nb_concepts_regex is fine for liti concepts also  
        match = nb_concepts_regex.match(self._readLine())
        if match:
            for i in range(0, int(match.group(1))):
                # Even though we're looking for concepts, the category_regex
                # regular expression will properly match concepts as well
                match = category_regex.match(self._readLine())
                if match:                    
                    available_concepts.append(match.group(1))
                    
        return available_concepts

    def _readAvailableLitiPredicates(self):
        available_predicates = []

        match = nb_liti_predicates_regex.match(self._readLine())
        if match:
            for i in range(0, int(match.group(1))):
                match = liti_predicate_regex.match(self._readLine())
                if match:                    
                    available_predicates.append(match.group(1))
                    
        return available_predicates


    def _readAvailableConceptsWithMetadata(self):
        available_concepts_with_meta = []

        match = nb_concepts_regex.match(self._readLine())
        if match:
            for i in range(0, int(match.group(1))):
                concept = {}
                concept["name"] = ""
                concept["metadata"] = ""
                concept["unique_id"] = ""
                concept["comments"] = ""
                concept["links"] = ""
                concept["author"] = ""
                concept["cdate"] = ""
                concept["mdate"] = ""
                concept["rulestatus"] = ""

                # The regex used here was designed to match category metadata,
                # but works fine for concept metadata as well
                match = v4_cat_with_meta.match(self._readLine())
                if match:
                    concept = {}
                    concept["name"] = match.group(1)
                    concept["metadata"] = match.group(2)
                    concept["unique_id"] = match.group(3)
                    concept["comments"] = match.group(4)
                    concept["links"] = match.group(5)
                    concept["author"] = match.group(6)
                    concept["cdate"] = match.group(7)
                    concept["mdate"] =match.group(8)
                    concept["rulestatus"] = match.group(9)
                    available_concepts_with_meta.append(concept)

        return available_concepts_with_meta
