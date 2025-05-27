#Import libraries
import os
import sys
import json
import difflib
from databricks import sql
from fnmatch import fnmatch
from timeit import default_timer as timer

#Constants
DBSC_CONFIG_FILE="dbsc-config.json"
SCHEMA_ARG_SEPARATOR="+"
SEPARATOR_ID="$SEP$"
MAGIC_TAG="# MAGIC"
NULL_COMMENT="(null)"

#Object ids
OBJECTID_TABLE     ="tabl"
OBJECTID_VIEW      ="view"
OBJECTID_SCALARFUNC="scfn"
OBJECTID_TABLEFUNC ="tbfn"

#Control chars escape sequences
ESCAPE_SEQUENCES=[[";","$$SEMCOL$$"],["(","$$BEGPAR$$"],[")","$$ENDPAR$$"],["'","$$QUOTE$$"],[" comment ","$$COMM$$"]]

#Object constants
OBJECTID_CONF = {
  OBJECTID_TABLE     :{"order":0,"description":"Tables"},
  OBJECTID_VIEW      :{"order":1,"description":"Views"},
  OBJECTID_SCALARFUNC:{"order":2,"description":"Scalar functions"},
  OBJECTID_TABLEFUNC :{"order":3,"description":"Table functions"}
}

#Equivalent data types
TYPE_TRANSLATION={
  "tinyint" :"byte",
  "smallint":"short",
  "integer" :"int",
  "bigint"  :"long"
}

#Variables to show progress
_LastMessage=""
_MessageCnt=0

#Global display progress flag
_ShowProgress=True

#----------------------------------------------------------------------------------------------------------------------
# Show help
#----------------------------------------------------------------------------------------------------------------------
def ShowHelp():
  print("Databricks schema compare tool - v1.0 - Diego Marin 2023")
  print("")
  print("Usage: python dbsc.py <source> <target> [--filter:<pattern>] [--sep] [--raw] [--np]")
  print("       python dbsc.py --dump:<source> [--filter:<pattern>] [--np]")
  print("")
  print("<source>           : Databricks source schema names, schema group or project folder")
  print("<target>           : Databricks target schema names, schema group or project folder")
  print("--dump:<source>    : No comparison just dump schema definition as json")
  print("--filter:<pattern> : Filter objects to compare using pattern")
  print("--sep              : Print separation line between objects in results")
  print("--raw              : Report results as raw list")
  print("--np               : No progress indicator")
  print("")
  print("Selected databricks instance: "+os.environ["AZURE_SELECTION"]+(" ("+os.environ["DATABRICKS_SERVER_HOSTNAME"]+")" if "DATABRICKS_SERVER_HOSTNAME" in os.environ else ""))
  print("")
  print("Notes:")
  print("Databricks instance is read by default from environment variable DATABRICKS_SERVER_HOSTNAME if it exists")
  print("Databricks http path is read from environment variable DATABRICKS_HTTP_PATH if it exists")
  print("Databricks access token is read by default from environment variable DATABRICKS_TOKEN if it exists")
  print("Schema names in source and target can be one or several (separated by "+SCHEMA_ARG_SEPARATOR+")")
  if "schema_groups" in _Config:
    print("Schema groups as defined in configuration file can be one of these: "+",".join(_Config["schema_groups"]))
  
#----------------------------------------------------------------------------------------------------------------------
# Get command line arguments
#----------------------------------------------------------------------------------------------------------------------
def GetCommandLineOptions(Options):

  #Default values for options
  SrcFolder=""
  TgtFolder=""
  SrcSchemas=""
  TgtSchemas=""
  PatternFilter="*"
  RawOutput=False
  SeparatorLine=False
  ShowProgress=True
  DumpMode=False

  #Not enough arguments given
  if len(sys.argv)<2:
    ShowHelp()
    return False
  
  #Get arguments
  elif len(sys.argv)>=2 and sys.argv[1].startswith("--dump:"):
    DumpMode=True
    Source=sys.argv[1].replace("--dump:","")
    Target=""
    for i in range(2,len(sys.argv)):
      item=sys.argv[i]
      if item.startswith("--filter:"):
        PatternFilter=item.replace("--filter:","")
      elif item=="--np":
        ShowProgress=False
      else:
        print("Invalid option: ",item)
        return False
  elif len(sys.argv)>=3: 
    Source=sys.argv[1]
    Target=sys.argv[2]
    for i in range(3,len(sys.argv)):
      item=sys.argv[i]
      if item.startswith("--filter:"):
        PatternFilter=item.replace("--filter:","")
      elif item=="--sep":
        SeparatorLine=True
      elif item=="--raw":
        RawOutput=True
      elif item=="--np":
        ShowProgress=False
      else:
        print("Invalid option: ",item)
        return False
  else:
    print("Invalid arguments")
    return False
  
  #Must specify source and target
  if len(Source)==0:
    print("Must provide source")
    return False
  if len(Target)==0 and DumpMode==False:
    print("Must provide target")
    return False

  #Check input is folders or schemas
  if os.path.exists(Source):
    SrcFolder=Source
  else:
    SrcSchemas=Source
  if os.path.exists(Target):
    TgtFolder=Target
  else:
    TgtSchemas=Target

  #Cannot compare two project folders (as we cannot provide schema selection)
  if len(SrcFolder)!=0 and len(TgtFolder)!=0:
    print("Source and target cannot be both folders")
    return False

  #Return arguments
  Options.append(SrcFolder)
  Options.append(SrcSchemas)
  Options.append(TgtFolder)
  Options.append(TgtSchemas)
  Options.append(PatternFilter)
  Options.append(SeparatorLine)
  Options.append(RawOutput)
  Options.append(ShowProgress)
  Options.append(DumpMode)

  #Return code
  return True

# ----------------------------------------------------------------------------------
# Loads JSON configuration file
# (the files can contain comments (started by //), since they are filtered before 
# ----------------------------------------------------------------------------------  
def JsonFileParser(FilePath):
  
  #Import data sources file
  try:
    File=open(FilePath,"r")
    FileContent=File.read()
    File.close()
    FileContent="\n".join([Line for Line in FileContent.split("\n") if Line.strip().startswith("//")==False])
    Json=json.loads(FileContent)
  except Exception as Ex:
    Message=f"Exception reading configuration file ({FilePath}): {str(Ex)}"
    return False,None,Message

  #Return result
  return True,Json,""

#----------------------------------------------------------------------------------------------------------------------
# String trim to take out all double spaces
#----------------------------------------------------------------------------------------------------------------------
def TrimDoubleSpaces(Str,KeepLitterals=False):
  if KeepLitterals==False:
    Result=Str.strip()
    while(Result.find("  ")!=-1):
      Result=Result.replace("  "," ")
  else:
    Result=""
    PrevChar=None
    LitteralMode=False
    for c in Str.strip():
      if c=="'" and PrevChar!="\\" and LitteralMode==False:
        LitteralMode=True
      elif c=="'" and PrevChar!="\\" and LitteralMode==True:
        LitteralMode=False
      if c==" ":
        if LitteralMode==True:
          Result+=c
        else:
          if PrevChar!=" ":
            Result+=c
      else:
        Result+=c
      PrevChar=c
    Result=Result.strip()
  return Result

#----------------------------------------------------------------------------------------------------------------------
# String make all lower case except litterals
#----------------------------------------------------------------------------------------------------------------------
def LowerCase(Str):
  Result=""
  LitteralMode=False
  for c in Str:
    if c=="'" and LitteralMode==False:
      LitteralMode=True
    elif c=="'" and LitteralMode==True:
      LitteralMode=False
    if LitteralMode==True:
      Result+=c
    else:
      Result+=c.lower()
  return Result

#----------------------------------------------------------------------------------------------------------------------
# Escape all control chars inside litterals
#----------------------------------------------------------------------------------------------------------------------
def EscapeControlChars(Str,EscapeSequences,Reverse=False):
  
  #Escape all control chars inside litterals
  if Reverse==False:
    SingleQuoteEscape=[s for s in EscapeSequences if s[0]=="'"][0][1]
    Result=""
    WorkStr=Str
    LitteralMode=False
    i=0
    while(i<len(WorkStr)):
      if WorkStr[i]=="'" and LitteralMode==False:
        LitteralMode=True
        Result+=SingleQuoteEscape
        i+=1
        continue
      elif WorkStr[i]=="'" and LitteralMode==True:
        LitteralMode=False
        Result+=SingleQuoteEscape
        i+=1
        continue
      if LitteralMode==True:
        Escaped=False
        for Item in EscapeSequences:
          if WorkStr[i:].startswith(Item[0]):
            Result+=Item[1]
            i+=len(Item[0])
            Escaped=True
            break
        if Escaped==False:
          Result+=WorkStr[i]
          i+=1
      else:
        Result+=WorkStr[i]
        i+=1
  #Unescape all control chars
  else:
    Result=Str
    for Item in EscapeSequences:
      Result=Result.replace(Item[1],Item[0])
  
  #Return result
  return Result

#----------------------------------------------------------------------------------------------------------------------
# Filter SQL comments on statement
#----------------------------------------------------------------------------------------------------------------------
def FilterSqlComments(SqlText):
  
  #Init loop
  Lines=[]
  SqlText=SqlText.strip(" ")
  
  #Process lines
  for Line in SqlText.split("\n"):
    
    #Trim spaces
    Line=Line.rstrip(" ")
    
    #Filter comment lines
    if Line.lstrip(" ").startswith("--"):
      Line=""
    
    #Filter inline comments
    LitteralMode=False
    InlineComment=-1
    for i,c in enumerate(Line):
      if c=="'" and LitteralMode==False:
        LitteralMode=True
      elif c=="'" and LitteralMode==True:
        LitteralMode=False
      if LitteralMode==False:
        if Line[i:].startswith("--")==True:
          InlineComment=i 
          break
    if InlineComment!=-1:
      Line=Line[:InlineComment].rstrip(" ")
    
    #Append filtered lines
    if len(Line)!=0:
      Lines.append(Line)
  
  #Return result
  return "\n".join(Lines)

#----------------------------------------------------------------------------------------------------------------------
# Sql parser
#----------------------------------------------------------------------------------------------------------------------
def SqlParse(Sentence):
  
  #Constants
  JOIN_STR="$STNJOIN$"
  DATABRICKS_OPERATORS=["!","!=","%","&","*","+","-","/","<","<=","<=>","<>","=","==",">",">=","^","|","||","~"]
  SPECIAL_CHARACTERS=[",",";","(",")","[","]","'","\"","\n","\\n","--"," "]

  #Build escape sequences for databricks operators and special characters
  #All sequences are built in the form $ESCSEQnn$
  EscapeSequences=[[x,"$ESCSEQ"+str(i).rjust(2,'0')+"$"] for i,x in enumerate(DATABRICKS_OPERATORS+SPECIAL_CHARACTERS)]

  #Get lines from sentence and filter comment lines
  WorkStn=Sentence.split("\n")
  Lines=[]
  for Stn in WorkStn:
    Line=Stn.rstrip(" ")
    if len(Line.strip(" "))!=0 and Line.strip(" ").startswith("--")==False:
      Lines.append(Line)
  
  #Parse tokens from every line and filter inline comments
  SqlTokens=[]
  for Line in Lines:
    WorkLine=Line.strip(" ")
    WorkLine=EscapeControlChars(WorkLine,EscapeSequences)
    WorkLine=(WorkLine[:WorkLine.find("--")].strip(" ") if WorkLine.find("--")!=-1 else WorkLine)
    for Seq in EscapeSequences:
      if Seq[0]!=" ":
        WorkLine=WorkLine.replace(Seq[0]," "+Seq[0]+" ")
    WorkLine=TrimDoubleSpaces(WorkLine)
    WorkLine=JOIN_STR.join(WorkLine.split(" "))
    WorkLine=EscapeControlChars(WorkLine,EscapeSequences,Reverse=True)
    Tokens=WorkLine.split(JOIN_STR)
    SqlTokens.extend(Tokens)
  
  #Return result
  return SqlTokens

#----------------------------------------------------------------------------------------------------------------------
# Check token list starts with 
#----------------------------------------------------------------------------------------------------------------------
def TokenListStartsWith(Tokens,StartTokens,StartPosition=0):
  ListTokens=Tokens[StartPosition:]
  StartTokens=[x.lower() for x in StartTokens.split(" ")]
  if len(ListTokens)<len(StartTokens):
    return False
  else:
    LowerCaseTokens=[x.lower() for x in ListTokens[:len(StartTokens)]]
    if " ".join(LowerCaseTokens)==" ".join(StartTokens):
      return True
    else:
      return False

#----------------------------------------------------------------------------------------------------------------------
# Find token in parenthesys level zero
#----------------------------------------------------------------------------------------------------------------------
def FindZeroLevelToken(Tokens,TokensToFind,StartPos=0,FindMode="or",CaseSensitive=False):
  ListTokens=Tokens
  if isinstance(TokensToFind,str):
    FindTokens=[TokensToFind]
  else:
    FindTokens=TokensToFind
  if CaseSensitive==False:
    ListTokens=[x.lower() for x in ListTokens]
    FindTokens=[x.lower() for x in FindTokens]
  i=StartPos
  ParLevel=0
  while(i<len(ListTokens)):
    if ParLevel==0:
      if FindMode=="or" and len([x for x in FindTokens if x==ListTokens[i]])!=0:
        return i
      elif FindMode=="and" and i+len(FindTokens)<len(ListTokens) and ListTokens[i:len(FindTokens)]==FindTokens:
        return i
    if(ListTokens[i]==")"):
      ParLevel-=1
    elif(ListTokens[i]=="("):
      ParLevel+=1
    i+=1
  return -1

#----------------------------------------------------------------------------------------------------------------------
# Find substring in parenthesys level zero
#----------------------------------------------------------------------------------------------------------------------
def FindZeroLevelSubStr(Str,SubStr,StartPos=0,CaseSensitive=False):
  i=StartPos
  ParLevel=0
  while(i<len(Str)):
    if ParLevel==0:
      if CaseSensitive==True and Str[i:].startswith(SubStr):
        return i
      elif CaseSensitive==False and Str[i:].lower().startswith(SubStr.lower()):
        return i
    if(Str[i]==")"):
      ParLevel-=1
    elif(Str[i]=="("):
      ParLevel+=1
    i+=1
  return -1

#----------------------------------------------------------------------------------------------------------------------
# Get catalog, schema and name from fully qualified object name
#----------------------------------------------------------------------------------------------------------------------
def SplitObjectName(FullyQualifiedName):
  Parts=FullyQualifiedName.split(".")
  Catalog=(Parts[-3] if len(Parts)>=3 else "")
  Schema=(Parts[-2] if len(Parts)>=2 else "")
  Name=(Parts[-1] if len(Parts)>=1 else "")
  return Catalog,Schema,Name

#----------------------------------------------------------------------------------------------------------------------
# Schema name replacements
#----------------------------------------------------------------------------------------------------------------------
def SchemaNameReplacements(SchemaName):
  for Repl in _Config["schema_name_replacements"]:
    SchemaName=SchemaName.replace(Repl["substring"],Repl["replacement"])
  return SchemaName

#----------------------------------------------------------------------------------------------------------------------
# Calculate schema short names
#----------------------------------------------------------------------------------------------------------------------
def GetSchemaShortNames(SchemaNames):
  ShortNames={Name:Name.strip() for Name in SchemaNames}
  while(True):
    Shortened=False
    for c in ShortNames[SchemaNames[0]]:
      FoundInAll=True
      for SchemaName in ShortNames:
        if ShortNames[SchemaName][0]!=c:
          FoundInAll=False
          break
      if FoundInAll:
        ShortNames={Name:ShortNames[Name][1:] for Name in ShortNames}
        Shortened=True
    if Shortened==False:
      break  
  return ShortNames

#----------------------------------------------------------------------------------------------------------------------
# Check object is ignored for schema
#----------------------------------------------------------------------------------------------------------------------
def IsObjectIgnored(ObjectId):
  for NamePattern in _Config["ignored_objects_in_repo"]:
    if fnmatch(ObjectId,NamePattern)==True:
      return True
  return False

#----------------------------------------------------------------------------------------------------------------------
# Check object is ignored for schema
#----------------------------------------------------------------------------------------------------------------------
def StandardType(RawType):
  if RawType.lower() in TYPE_TRANSLATION:
    return TYPE_TRANSLATION[RawType.lower()]
  return RawType.lower()

#----------------------------------------------------------------------------------------------------------------------
# Progress message
#----------------------------------------------------------------------------------------------------------------------
def DisplayProgress(From,Index,Total,Object):
  global _LastMessage
  global _MessageCnt
  global _ShowProgress
  if _ShowProgress==False:
    return
  Wheel=['-','\\','|','/']
  if Index!=0 and Total!=0:
    BarLen=10
    Bar="["+("#"*(int(BarLen*Index/Total))+"."*BarLen)[:BarLen]+"]"
  if From=="CON":
    Message="["+Wheel[_MessageCnt%4]+"] Connecting to databricks ..."
  elif From=="LST":
    Message="["+Wheel[_MessageCnt%4]+"] Reading object list from schema "+f"{Index}/{Total} {Bar} ({Object}) ..."
  elif From=="SRC":
    Message="["+Wheel[_MessageCnt%4]+"] Reading objects from source "+f"{Index}/{Total} {Bar} ({Object}) ..."
  elif From=="TGT":
    Message="["+Wheel[_MessageCnt%4]+"] Reading objects from target "+f"{Index}/{Total} {Bar} ({Object}) ..."
  elif From=="CMP":
    Message="["+Wheel[_MessageCnt%4]+"] Comparing objects "+f"{Index}/{Total} {Bar} ({Object}) ..."
  elif From=="CLR":
    Message=" "*len(_LastMessage)
  if len(Message)<len(_LastMessage):
    print(" "*len(_LastMessage),end="\r")
  print(Message,end="\r")
  _LastMessage=Message
  _MessageCnt+=1

#----------------------------------------------------------------------------------------------------------------------
# Connect to data source
#----------------------------------------------------------------------------------------------------------------------
def Connect(ServerHostName,HttpPath,AccessToken):
  DisplayProgress("CON",0,0,"")
  try:
    Cursor=sql.connect(server_hostname=ServerHostName,http_path=HttpPath,access_token=AccessToken).cursor()
  except Exception as Ex:
    print("Unable to open connection to databricks: "+str(Ex))
    return False,None,0
  return True,Cursor

#----------------------------------------------------------------------------------------------------------------------
# Get object definition from SQL definition
#----------------------------------------------------------------------------------------------------------------------
def GetObjectDefinition(From,Command,SelectedSchemas,PatternFilter):
  
  #Calculate selected schemas with name replacements
  SelSchemas=list(set([SchemaNameReplacements(Name) for Name in SelectedSchemas]))

  #Return valid definition
  ReturnDefinition=False

  #Clean comments
  Tokens=SqlParse(Command)

  #Fetch table definition
  if TokenListStartsWith(Tokens,"create table") \
  or TokenListStartsWith(Tokens,"create or replace table"):

    #Get view name
    TableNameIndex=0
    TableNameIndex=(TableNameIndex+1 if TokenListStartsWith(Tokens,"create",TableNameIndex) else TableNameIndex)
    TableNameIndex=(TableNameIndex+2 if TokenListStartsWith(Tokens,"or replace",TableNameIndex) else TableNameIndex)
    TableNameIndex=(TableNameIndex+1 if TokenListStartsWith(Tokens,"table",TableNameIndex) else TableNameIndex)
    TableNameIndex=(TableNameIndex+3 if TokenListStartsWith(Tokens,"if not exists",TableNameIndex) else TableNameIndex)
    TableName=Tokens[TableNameIndex]

    #Get object id, shcema and name
    ObjectType=OBJECTID_TABLE
    CatalogName,SchemaName,ObjectName=SplitObjectName(TableName)
    SchemaName=SchemaNameReplacements(SchemaName)

    #Do not compare schema if is not in selection or object not selected
    if (SchemaName not in SelSchemas or fnmatch(ObjectName,PatternFilter)==False) and DumpMode==False:
      return True,"",None,None
     
    #Find parenthesys that define table fields
    BegParenIndex=FindZeroLevelToken(Tokens,"(",TableNameIndex+1)
    EndParenIndex=FindZeroLevelToken(Tokens,")",BegParenIndex+1)
    if BegParenIndex==-1 or EndParenIndex==-1:
      return False,f"Begining and ending parenthesys for column specification expected in definition of table {TableName})",None,None

    #Parse all column definitions
    i=BegParenIndex+1
    Columns={}
    while(True):
      FieldEndIndex=FindZeroLevelToken(Tokens,[",",")"],i)
      if FieldEndIndex==-1:
        return False,f"Comma or ending parenthesys expected in definition of table {TableName} after token {i}",None,None
      NotNullIndex=FindZeroLevelToken(Tokens,["not","null"],i)
      CommentIndex=FindZeroLevelToken(Tokens,"comment",i)
      ColumnName=(Tokens[i] if i<FieldEndIndex else None)
      ColumnName=ColumnName.replace("`","")
      ColumnType=(StandardType(Tokens[i+1]) if i+1<FieldEndIndex else None)
      ColumnNullable=(True if NotNullIndex!=-1 and NotNullIndex<FieldEndIndex else False)
      ColumnComment=(Tokens[CommentIndex+1] if CommentIndex!=-1 and CommentIndex<FieldEndIndex else NULL_COMMENT)
      if ColumnName==None or ColumnType==None:
        return False,f"Field name and type expected in definition of table {TableName} after token {i}",None,None
      Columns[ColumnName]={"type":ColumnType,"nullable":ColumnNullable,"comment":ColumnComment}
      if Tokens[FieldEndIndex]==")":
        break
      i=FieldEndIndex+1
      if i>=EndParenIndex:
        break
    
    #Get table comment
    CommentIndex=FindZeroLevelToken(Tokens,"comment",EndParenIndex+1)
    if CommentIndex!=-1 and CommentIndex+1<=len(Tokens)-1:
      ObjectComment=Tokens[CommentIndex+1]
    else:
      ObjectComment=NULL_COMMENT

    #Store table definition
    ObjectId=ObjectType+":"+SchemaName+"."+ObjectName
    FullyQualifiedName=SchemaName+"."+ObjectName
    ObjectDef={"fullname":FullyQualifiedName,"type":ObjectType,"text":"","comment":ObjectComment,"columns":Columns}
    ReturnDefinition=True

  #Fetch view definition
  if TokenListStartsWith(Tokens,"create view") \
  or TokenListStartsWith(Tokens,"create or replace view") \
  or TokenListStartsWith(Tokens,"create temporar view") \
  or TokenListStartsWith(Tokens,"create or replace temporary view"):
    
    #Get view name
    ViewNameIndex=0
    ViewNameIndex=(ViewNameIndex+1 if TokenListStartsWith(Tokens,"create",ViewNameIndex) else ViewNameIndex)
    ViewNameIndex=(ViewNameIndex+2 if TokenListStartsWith(Tokens,"or replace",ViewNameIndex) else ViewNameIndex)
    ViewNameIndex=(ViewNameIndex+1 if TokenListStartsWith(Tokens,"temporary",ViewNameIndex) else ViewNameIndex)
    ViewNameIndex=(ViewNameIndex+1 if TokenListStartsWith(Tokens,"view",ViewNameIndex) else ViewNameIndex)
    ViewNameIndex=(ViewNameIndex+3 if TokenListStartsWith(Tokens,"if not exists",ViewNameIndex) else ViewNameIndex)
    ViewName=Tokens[ViewNameIndex]

    #Get object id, shcema and name
    ObjectType=OBJECTID_VIEW
    CatalogName,SchemaName,ObjectName=SplitObjectName(ViewName)
    SchemaName=SchemaNameReplacements(SchemaName)

    #Do not compare schema if is not in selection
    if (SchemaName not in SelSchemas or fnmatch(ObjectName,PatternFilter)==False) and DumpMode==False:
      return True,"",None,None

    #Fetch view text
    Keyword="as"
    CleanCommand=FilterSqlComments(Command)
    Pos=FindZeroLevelSubStr(CleanCommand.replace("\n"," ").replace("\t"," ")," "+Keyword+" ")
    if Pos==-1:
      return False,f"Unable to find '{Keyword}' keyword in definition of view ({ViewName})",None,None
    ViewText=CleanCommand[Pos+len(Keyword)+2:].strip(" ")

    #Store table definition
    ObjectId=ObjectType+":"+SchemaName+"."+ObjectName
    FullyQualifiedName=SchemaName+"."+ObjectName
    ObjectDef={"fullname":FullyQualifiedName,"type":ObjectType,"text":ViewText,"comment":NULL_COMMENT,"columns":{}}
    ReturnDefinition=True

  #Fetch function definition
  if TokenListStartsWith(Tokens,"create function") \
  or TokenListStartsWith(Tokens,"create or replace function") \
  or TokenListStartsWith(Tokens,"create temporar function") \
  or TokenListStartsWith(Tokens,"create or replace temporary function"):
    
    #Get function name
    FunctionNameIndex=0
    FunctionNameIndex=(FunctionNameIndex+1 if TokenListStartsWith(Tokens,"create",FunctionNameIndex) else FunctionNameIndex)
    FunctionNameIndex=(FunctionNameIndex+2 if TokenListStartsWith(Tokens,"or replace",FunctionNameIndex) else FunctionNameIndex)
    FunctionNameIndex=(FunctionNameIndex+1 if TokenListStartsWith(Tokens,"temporary",FunctionNameIndex) else FunctionNameIndex)
    FunctionNameIndex=(FunctionNameIndex+1 if TokenListStartsWith(Tokens,"function",FunctionNameIndex) else FunctionNameIndex)
    FunctionNameIndex=(FunctionNameIndex+3 if TokenListStartsWith(Tokens,"if not exists",FunctionNameIndex) else FunctionNameIndex)
    FunctionName=Tokens[FunctionNameIndex]

    #Find parenthesys that define function parameters
    BegParenIndex=FindZeroLevelToken(Tokens,"(",FunctionNameIndex+1)
    EndParenIndex=FindZeroLevelToken(Tokens,")",BegParenIndex+1)
    if BegParenIndex==-1 or EndParenIndex==-1:
      return False,f"Begining and ending parenthesys for parameter specification expected in definition of function ({FunctionName})",None,None

    #Get object id, shcema and name
    ObjectType=(OBJECTID_TABLEFUNC if FindZeroLevelToken(Tokens,"returns table")!=-1 else OBJECTID_SCALARFUNC)
    CatalogName,SchemaName,ObjectName=SplitObjectName(FunctionName)
    SchemaName=SchemaNameReplacements(SchemaName)

    #Do not compare schema if is not in selection or object not selected
    if (SchemaName not in SelSchemas or fnmatch(ObjectName,PatternFilter)==False) and DumpMode==False:
      return True,"",None,None

    #Parse function parameters
    i=BegParenIndex+1
    Parms=[]
    while(True):
      ParmEndIndex=FindZeroLevelToken(Tokens,[",",")"],i)
      if ParmEndIndex==-1:
        return False,f"Comma or ending parenthesys expected in parameter definition of function {FunctionName} after token {i}",None,None
      ParmName=(Tokens[i] if i<ParmEndIndex else None)
      ParmType=(StandardType(Tokens[i+1]) if i+1<ParmEndIndex else None)
      if ParmName==None or ParmType==None:
        return False,f"Parameter name and type expected in definition of function {FunctionName} after token {i}",None,None
      Parms.append({"name":ParmName,"type":ParmType})
      if Tokens[ParmEndIndex]==")":
        break
      i=ParmEndIndex+1
      if i>=EndParenIndex:
        break

    #Get return type for scalar functions
    if ObjectType==OBJECTID_SCALARFUNC:
      ReturnsIndex=FindZeroLevelToken(Tokens,"returns")
      if ReturnsIndex==-1 or ReturnsIndex+1>len(Tokens)-1:
        return False,f"Return type expected in definition of function {FunctionName}",None,None
      ReturnType=StandardType(Tokens[ReturnsIndex+1])
    
    #Get return type for table functions
    elif ObjectType==OBJECTID_TABLEFUNC:
      ReturnsTableIndex=FindZeroLevelToken(Tokens,["returns","table","("])
      if ReturnsTableIndex==-1:
        return False,f"Table specification expected after returns table keywords in definition of function {FunctionName}",None,None
      RetTableBegParenIndex=ReturnsTableIndex+2
      RetTableEndParenIndex=FindZeroLevelToken(Tokens,")",RetTableBegParenIndex+1)
      if RetTableBegParenIndex==-1 or RetTableEndParenIndex==-1:
        return False,f"Begining and ending parenthesys for return table specification expected in definition of function ({FunctionName})",None,None
      i=ReturnsTableIndex+3
      ReturnType=[]
      while(True):
        FieldEndIndex=FindZeroLevelToken(Tokens,[",",")"],i)
        if FieldEndIndex==-1:
          return False,f"Comma or ending parenthesys expected in specification of return table in definition of function {FunctionName} after token {i}",None,None
        CommentIndex=FindZeroLevelToken(Tokens,"comment",i)
        ColumnName=(Tokens[i] if i<FieldEndIndex else None)
        ColumnType=(StandardType(Tokens[i+1]) if i+1<FieldEndIndex else None)
        ColumnComment=(Tokens[CommentIndex+1] if CommentIndex!=-1 and CommentIndex<FieldEndIndex else NULL_COMMENT)
        if ColumnName==None or ColumnType==None:
          return False,f"Field name and type expected in specification of return table in definition of function {FunctionName} after token {i}",None,None
        ReturnType.append({"name":ColumnName,"type":ColumnType,"comment":ColumnComment})
        i=FieldEndIndex+1
        if i>=RetTableEndParenIndex:
          break

    #Fetch function text
    Keyword="return"
    CleanCommand=FilterSqlComments(Command)
    Pos=FindZeroLevelSubStr(CleanCommand.replace("\n"," ").replace("\t"," ")," "+Keyword+" ")
    if Pos==-1:
      return False,f"Unable to find '{Keyword}' keyword in definition of function ({FunctionName})",None,None
    FunctionText=CleanCommand[Pos+len(Keyword)+2:].strip(" ")

    #Store table definition
    ObjectId=ObjectType+":"+SchemaName+"."+ObjectName
    FullyQualifiedName=SchemaName+"."+ObjectName
    ObjectDef={"fullname":FullyQualifiedName,"type":ObjectType,"returns":ReturnType,"text":FunctionText,"parameters":Parms}
    ReturnDefinition=True

  #Return object definition
  if ReturnDefinition==True:
    return True,"",ObjectId,ObjectDef
  else:
    return True,"",None,None

#----------------------------------------------------------------------------------------------------------------------
# Get schema definitions from repository folder
#----------------------------------------------------------------------------------------------------------------------
def GetSchemaFromProject(From,ProjFolder,SchemaNames,PatternFilter,DumpMode):
  
  #Initialize schema definition
  SchemaDef={}

  #Calculate selected schemas with environment replace
  SelectedSchemas=SchemaNames.split(SCHEMA_ARG_SEPARATOR)
  SelectedSchemas=list(set([SchemaNameReplacements(Schema) for Schema in SelectedSchemas]))

  #Get relevant files to read (only python files)
  Files=[]
  for DirPath,DirNames,FileNames in os.walk(ProjFolder):
    for FileName in FileNames:
      if FileName.endswith(".py"):
        FilePath=(DirPath+"\\"+FileName).replace("\\\\","\\")
        Files.append(FilePath)
  
  #Process all files
  Objects=[]
  for File in Files:

    #Read all file lines
    try:
      Handler=open(File,"r")
      FileLines=Handler.readlines()
      Handler.close()
    except Exception as Ex:
      Message="Error reading file "+File+". "+str(Ex)
      return False,Message,[]

    #Process all lines
    FetchCommand=False
    ProcessCommands=False
    CommandList=[]
    LastLine=len(FileLines)-1
    for i,FileLine in enumerate(FileLines):
      
      #Format lines
      FileLine=FileLine.strip(" ")
      
      #Get Sql commands
      if FileLine.startswith(MAGIC_TAG+r" %sql"):
        FetchCommand=True
        FileLine=""
      elif len(FileLine.replace("\n",""))==0:
        FetchCommand=False
        ProcessCommands=True
      if i==LastLine:
        ProcessCommands=True
      if FetchCommand==True:
        FileLine=FileLine.replace(MAGIC_TAG+" ","")
        FileLine=FileLine.replace(MAGIC_TAG,"")
        if len(FileLine.replace("\n","").strip(" "))!=0:
          CommandList.append(FileLine)

      #Store commands
      if ProcessCommands==True:
        JoinedCommands="".join(CommandList)
        CommandLines=JoinedCommands.split(";")
        CommandList=[]
        for Command in CommandLines:
          Objects.append(Command) 
        ProcessCommands=False
        CommandLines=[]

  #Parse all object definitions
  for i,Command in enumerate(Objects):
    Status,Message,ObjectId,ObjectDef=GetObjectDefinition(From,Command,SelectedSchemas,PatternFilter)
    if Status==False:
      return False,Message,{}
    if ObjectId!=None and ObjectDef!=None:
      SchemaDef[ObjectId]=ObjectDef
      DisplayProgress(From,i+1,len(Objects),ObjectId)

  #Return schema definition
  return True,"",SchemaDef

#----------------------------------------------------------------------------------------------------------------------
# Get schema info from databricks instance metastore
#----------------------------------------------------------------------------------------------------------------------
def GetSchemaFromMetastore(From,Cursor,SchemaNames,PatternFilter):

  #Query to get table,views and functions
  TBVW_LIST_QUERY="show tables in <schemaname> like '*'"         #Table/View list query
  TBVW_DETL_QUERY="show create table <tablename>"                #Table/View detail query
  FUNC_LIST_QUERY="show user functions in <schemaname> like '*'" #Function list query
  FUNC_DETL_QUERY="describe function extended <functionname>"    #Function detail query

  #Get object list
  ObjectList=[]
  SelSchemas=list(set(SchemaNames.split(SCHEMA_ARG_SEPARATOR)))
  for i,SchemaName in enumerate(SelSchemas):
    
    #Display progress
    DisplayProgress("LST",i+1,len(SelSchemas),SchemaName)

    #Get tables / Views
    Query=TBVW_LIST_QUERY.replace("<schemaname>",SchemaName)
    try:
      Cursor.execute(Query)
    except Exception as Ex:
      Message="Query error: "+str(Ex)+" (SQL: "+Query+")"
      return False,Message,[]
    for Row in Cursor.fetchall():
      if Row["isTemporary"]==True:
        continue
      Schema=Row["database"]
      Object=Row["tableName"]
      if fnmatch(Object,PatternFilter)==False:
        continue
      ObjectList.append({"kind":"TBVW","schema":Schema,"object":Object})
    
    #Get user functions
    Query=FUNC_LIST_QUERY.replace("<schemaname>",SchemaName)
    try:
      Cursor.execute(Query)
    except Exception as Ex:
      Message="Query error: "+str(Ex)+" (SQL: "+Query+")"
      return False,Message,[]
    for Row in Cursor.fetchall():
      FunctionName=Row["function"]
      Catalog,Schema,Object=SplitObjectName(FunctionName)
      if fnmatch(Object,PatternFilter)==False:
        continue
      ObjectList.append({"kind":"FUNC","schema":Schema,"object":Object})
  
  #Get object definitions
  SchemaDef={}
  for i,Object in enumerate(ObjectList):

    #Get object details
    Kind=Object["kind"]
    SchemaName=Object["schema"]
    ObjectName=Object["object"]

    #Get table/View definition
    if Kind=="TBVW":
      Query=TBVW_DETL_QUERY.replace("<tablename>",SchemaName+"."+ObjectName)
      try:
        Cursor.execute(Query)
      except Exception as Ex:
        Message="Query error: "+str(Ex)+" (SQL: "+Query+")"
        return False,Message,[]
      Command=""
      for Row in Cursor.fetchall():
        Command+=Row["createtab_stmt"]
      Status,Message,ObjectId,ObjectDef=GetObjectDefinition(From,Command,[SchemaName],"*")
      if Status==False:
        return False,Message,{}
      if ObjectId!=None and ObjectDef!=None:
        SchemaDef[ObjectId]=ObjectDef
        DisplayProgress(From,i+1,len(ObjectList),ObjectId)

    #Get function definition
    elif Kind=="FUNC":
      
      #Get function attributes
      Query=FUNC_DETL_QUERY.replace("<functionname>",SchemaName+"."+ObjectName)
      try:
        Cursor.execute(Query)
      except Exception as Ex:
        Message="Query error: "+str(Ex)+" (SQL: "+Query+")"
        return False,Message,[]
      FunctionParms=[]
      ReturnList=[]
      FetchParms=False
      FetchReturn=False
      for Row in Cursor.fetchall():
        Line=Row[0]
        if Line.startswith("Type: "):
          ObjectType=(OBJECTID_TABLEFUNC if TrimDoubleSpaces(Line.replace("Type: ",""))=="TABLE" else OBJECTID_SCALARFUNC)
        elif Line.startswith("Input: "):
          Line=Line.replace("Input: ","")
          FetchParms=True
        elif Line.startswith("Returns: "):
          FetchParms=False
          FetchReturn=True
          Line=Line.replace("Returns: ","")
        elif Line.startswith("Deterministic: "):
          FetchReturn=False
        elif Line.startswith("Body: "):
          FunctionText=Line[len("Body: "):].strip()
        if FetchParms==True:
          Parms=TrimDoubleSpaces(Line)
          ParmName=Parms.split(" ")[0]
          ParmType=StandardType(Parms.split(" ")[1])
          FunctionParms.append(ParmName+" "+ParmType)
        if FetchReturn==True:
          ReturnList.append(TrimDoubleSpaces(Line))
      if ObjectType==OBJECTID_SCALARFUNC:
        ReturnType=ReturnList[0]
      else:
        ReturnType=",".join(ReturnList)

      #Build definition and parse
      if ObjectType==OBJECTID_SCALARFUNC:
        Command=f"create function {SchemaName}.{ObjectName} ({','.join(FunctionParms)}) returns {ReturnType} return {FunctionText}"
      elif ObjectType==OBJECTID_TABLEFUNC:
        Command=f"create function {SchemaName}.{ObjectName} ({','.join(FunctionParms)}) returns table({ReturnType}) return {FunctionText}"
      Status,Message,ObjectId,ObjectDef=GetObjectDefinition(From,Command,[SchemaName],"*")
      if Status==False:
        return False,Message,{}
      if ObjectId!=None and ObjectDef!=None:
        SchemaDef[ObjectId]=ObjectDef
        DisplayProgress(From,i+1,len(ObjectList),ObjectId)

  #Return
  return True,"",SchemaDef

#----------------------------------------------------------------------------------------------------------------------
# Compare schemas
#----------------------------------------------------------------------------------------------------------------------
def CompareSchemas(SrcSchemaDef,TgtSchemaDef,SrcIsFolder,TgtIsFolder,SeparatorLine,RawOutput):
  
  #Init comparison
  FullObjectIds={}
  ComparisonTable=[]
  ComparisonList=[]
  Differences=0

  #Calculate short schema names
  SelectedSchemas=list(set([ObjectId.split(":")[1].split(".")[0] for ObjectId in SrcSchemaDef]+[ObjectId.split(":")[1].split(".")[0] for ObjectId in TgtSchemaDef]))
  ShortNames=GetSchemaShortNames(SelectedSchemas)
  for ObjectId in [ObjectId for ObjectId in SrcSchemaDef]:
    ObjectDef=SrcSchemaDef[ObjectId]
    ObjectType=ObjectId.split(":")[0]
    SchemaName=ObjectId.split(":")[1].split(".")[0]
    ObjectName=ObjectId.split(":")[1].split(".")[1]
    ShortSchema=ShortNames[SchemaName]
    ShortObjectId=ObjectType+":"+ShortSchema+("." if len(ShortSchema)!=0 else "")+ObjectName
    del SrcSchemaDef[ObjectId]
    SrcSchemaDef[ShortObjectId]=ObjectDef
    if not ShortObjectId in FullObjectIds:
      FullObjectIds[ShortObjectId]=ObjectId
  for ObjectId in [ObjectId for ObjectId in TgtSchemaDef]:
    ObjectDef=TgtSchemaDef[ObjectId]
    ObjectType=ObjectId.split(":")[0]
    SchemaName=ObjectId.split(":")[1].split(".")[0]
    ObjectName=ObjectId.split(":")[1].split(".")[1]
    ShortSchema=ShortNames[SchemaName]
    ShortObjectId=ObjectType+":"+ShortSchema+("." if len(ShortSchema)!=0 else "")+ObjectName
    del TgtSchemaDef[ObjectId]
    TgtSchemaDef[ShortObjectId]=ObjectDef
    if not ShortObjectId in FullObjectIds:
      FullObjectIds[ShortObjectId]=ObjectId

  #Get all different object names from both schemas
  Objects=list(set([(SrcSchemaDef[Name]["type"],Name) for Name in SrcSchemaDef]+[(TgtSchemaDef[Name]["type"],Name) for Name in TgtSchemaDef]))
  Objects.sort(key=lambda x:str(OBJECTID_CONF[x[0]]["order"])+":"+x[1])
  ObjectNames=[Obj[1] for Obj in Objects]

  #Loop through all object names
  for i,ObjectName in enumerate(ObjectNames):

    #Show progress
    DisplayProgress("CMP",i+1,len(ObjectNames),ObjectName)

    #Check all items missing in source schema
    if ObjectName in TgtSchemaDef and ObjectName not in SrcSchemaDef:
      if SrcIsFolder==False or (SrcIsFolder==True and IsObjectIgnored(FullObjectIds[ObjectName])==False):
        if RawOutput==False:
          ComparisonTable.append([ObjectName,"","","(object added)"])
        else:
          ComparisonList.append([ObjectName,["Object added in target"]])
        Differences+=1
    
    #Check all items missing in target schema
    elif ObjectName in SrcSchemaDef and ObjectName not in TgtSchemaDef:
      if TgtIsFolder==False or (TgtIsFolder==True and IsObjectIgnored(FullObjectIds[ObjectName])==False):
        if RawOutput==False:
          ComparisonTable.append([ObjectName,"","(object added)",""])
        else:
          ComparisonList.append([ObjectName,["Object added in source"]])
        Differences+=1

    #Check all items missing in second schema
    elif ObjectName in SrcSchemaDef and ObjectName in TgtSchemaDef:
      
      #ComparisonTable of table and view attsributes
      if SrcSchemaDef[ObjectName]["type"] in [OBJECTID_TABLE,OBJECTID_VIEW]:

        #Objects have different comment
        if SrcSchemaDef[ObjectName]["comment"]!=TgtSchemaDef[ObjectName]["comment"]:
          if RawOutput==False:
            ComparisonTable.append([ObjectName,"comment",SrcSchemaDef[ObjectName]["comment"],TgtSchemaDef[ObjectName]["comment"]])
          else:
            ComparisonList.append([ObjectName,["Object comment is different","Source object comment: "+SrcSchemaDef[ObjectName]["comment"],"Target object comment: "+TgtSchemaDef[ObjectName]["comment"]]])
          Differences+=1

      #ComparisonTable of function attributes
      if SrcSchemaDef[ObjectName]["type"] in [OBJECTID_SCALARFUNC,OBJECTID_TABLEFUNC]:

        #Objects have different return type
        SrcRetType=(",".join([Col["name"]+" "+Col["type"]+(" comment "+Col["comment"] if Col["comment"]!=NULL_COMMENT else "") for Col in SrcSchemaDef[ObjectName]["returns"]]) if SrcSchemaDef[ObjectName]["type"]==OBJECTID_TABLEFUNC else SrcSchemaDef[ObjectName]["returns"])
        TgtRetType=(",".join([Col["name"]+" "+Col["type"]+(" comment "+Col["comment"] if Col["comment"]!=NULL_COMMENT else "") for Col in TgtSchemaDef[ObjectName]["returns"]]) if TgtSchemaDef[ObjectName]["type"]==OBJECTID_TABLEFUNC else TgtSchemaDef[ObjectName]["returns"])
        if SrcRetType!=TgtRetType:
          if RawOutput==False:
            ComparisonTable.append([ObjectName,"returns",SrcRetType,TgtRetType])
          else:
            ComparisonList.append([ObjectName,["Function return type is different","Source return type: "+SrcRetType,"Target return type: "+TgtRetType]])          
          Differences+=1

        #Objects have different parameters
        SrcParmList=",".join([Parm["name"]+" "+Parm["type"] for Parm in SrcSchemaDef[ObjectName]["parameters"]])
        TgtParmList=",".join([Parm["name"]+" "+Parm["type"] for Parm in TgtSchemaDef[ObjectName]["parameters"]])
        if SrcParmList!=TgtParmList:
          if RawOutput==False:
            ComparisonTable.append([ObjectName,"parameters",SrcParmList,TgtParmList])
          else:
            ComparisonList.append([ObjectName,["Function parameters different","Source parameters: "+SrcParmList,"Target parameters: "+TgtParmList]])
          Differences+=1

      #ComparisonTable of view / function definitions
      if SrcSchemaDef[ObjectName]["type"] in [OBJECTID_VIEW,OBJECTID_SCALARFUNC,OBJECTID_TABLEFUNC]:
        SrcText=SchemaNameReplacements(SrcSchemaDef[ObjectName]["text"])
        TgtText=SchemaNameReplacements(TgtSchemaDef[ObjectName]["text"])
        if SrcText!=TgtText:
          SrcLines=[Line for Line in SrcText.split("\n")]
          TgtLines=[Line for Line in TgtText.split("\n")]
          SrcLineNr=0
          TgtLineNr=0
          TextComparison=[]
          if RawOutput==False:
            for Diff in difflib.unified_diff(SrcLines,TgtLines):
              Diff=str(Diff)
              if Diff.startswith("+++") or Diff.startswith("---"):
                continue
              if Diff.startswith("@@"):
                SrcLineNr=int(Diff.replace("@","").strip().split(" ")[0].split(",")[0].replace("-",""))
                TgtLineNr=int(Diff.replace("@","").strip().split(" ")[1].split(",")[0].replace("+",""))
                continue
              if Diff.startswith("+"):
                TextComparison.append(["","","",str(TgtLineNr).rjust(3)+": "+Diff[1:]])
                Differences+=1
                TgtLineNr+=1
              elif Diff.startswith("-"):
                TextComparison.append(["","",str(SrcLineNr).rjust(3)+": "+Diff[1:],""])
                Differences+=1
                SrcLineNr+=1
              else:
                TextComparison.append(["","",str(SrcLineNr).rjust(3)+": "+Diff[1:],str(TgtLineNr).rjust(3)+": "+Diff[1:]])
                SrcLineNr+=1
                TgtLineNr+=1
            if len(TextComparison)!=0:
              if len(ComparisonTable)!=0:
                if ComparisonTable[-1][0]!=ObjectName:  
                  TextComparison[0][0]=ObjectName
                  TextComparison[0][1]="definition"
              else:
                TextComparison[0][0]=ObjectName
                TextComparison[0][1]="definition"
              ComparisonTable.extend(TextComparison)
          else:
            DifferenceList=[]
            for Diff in difflib.unified_diff(SrcLines,TgtLines):
              if Diff.startswith("+++") or Diff.startswith("---"):
                continue
              if Diff.startswith("@@"):
                continue
              else:
                DifferenceList.append(str(Diff))
            ComparisonList.append([ObjectName,["Object definition is different","Differences:\n"+"\n".join(DifferenceList)]])

      #ComparisonTable of table and view columns
      if SrcSchemaDef[ObjectName]["type"] == OBJECTID_TABLE:

        #ComparisonTable of columns
        ColNames=list(set([Name for Name in SrcSchemaDef[ObjectName]["columns"]]+[Name for Name in TgtSchemaDef[ObjectName]["columns"]]))
        ColNames.sort()
        ColComparison=[]
        for ColName in ColNames:
          if ColName in TgtSchemaDef[ObjectName]["columns"] and ColName not in SrcSchemaDef[ObjectName]["columns"]:
            ColComparison.append(["","column:"+ColName,"","(column added)"])
            Differences+=1
          elif ColName in SrcSchemaDef[ObjectName]["columns"] and ColName not in TgtSchemaDef[ObjectName]["columns"]:
            ColComparison.append(["","column:"+ColName,"(column added)",""])
            Differences+=1
          elif ColName in SrcSchemaDef[ObjectName]["columns"] and ColName in TgtSchemaDef[ObjectName]["columns"]:
            if SrcSchemaDef[ObjectName]["columns"][ColName]["type"]!=TgtSchemaDef[ObjectName]["columns"][ColName]["type"]:
              ColComparison.append(["","column:"+ColName,"type:"+SrcSchemaDef[ObjectName]["columns"][ColName]["type"],"type:"+TgtSchemaDef[ObjectName]["columns"][ColName]["type"]])
              Differences+=1
            if SrcSchemaDef[ObjectName]["columns"][ColName]["nullable"]!=TgtSchemaDef[ObjectName]["columns"][ColName]["nullable"]:
              ColComparison.append(["","column:"+ColName,"nullable:"+str(SrcSchemaDef[ObjectName]["columns"][ColName]["nullable"]),"nullable:"+str(TgtSchemaDef[ObjectName]["columns"][ColName]["nullable"])])
              Differences+=1
            if SrcSchemaDef[ObjectName]["columns"][ColName]["comment"]!=TgtSchemaDef[ObjectName]["columns"][ColName]["comment"]:
              ColComparison.append(["","column:"+ColName,"comment:"+SrcSchemaDef[ObjectName]["columns"][ColName]["comment"],"comment:"+TgtSchemaDef[ObjectName]["columns"][ColName]["comment"]])
              Differences+=1
        if len(ColComparison)!=0:
          if RawOutput==False:
            if len(ComparisonTable)!=0:
              if ComparisonTable[-1][0]!=ObjectName:  
                ColComparison[0][0]=ObjectName
                ColComparison[0][1]="definition"
            else:
              ColComparison[0][0]=ObjectName
              ColComparison[0][1]="definition"
            ComparisonTable.extend(ColComparison)
          else:
            DifferenceList=[]
            for Difference in ColComparison:
              if Difference[2]=="" and Difference[3]=="(column added)":
                DifferenceList.append(Difference[1]+" is added in target")
              elif Difference[2]=="(column added)" and Difference[3]=="":
                DifferenceList.append(Difference[1]+" is added in source")
              else:
                DifferenceList.append(Difference[1]+", Source "+Difference[2]+", Target "+Difference[3])
            ComparisonList.append([ObjectName,DifferenceList])

  
  #Count different objects
  if RawOutput==False:
    Diffs=[[Diff[0]] for Diff in ComparisonTable]
  else:
    Diffs=[[Diff[0]] for Diff in ComparisonList]
  DiffObjects=0
  PrevObject=None
  for Diff in Diffs:
    if PrevObject!=Diff and (len(Diff)!=0 and PrevObject!=None):
      DiffObjects+=1
    PrevObject=Diff
  if len(Diffs)!=0:
    DiffObjects+=1

  #Prepare result for comparison table
  if RawOutput==False:

    #Insert separation lines between objects and count ojects different
    if SeparatorLine==True:
      Result=[]
      PrevObject=None
      for Diff in ComparisonTable:
        if PrevObject!=Diff[0] and (len(Diff[0])!=0 and PrevObject!=None):
          Result.append([SEPARATOR_ID,"","",""])
        Result.append(Diff)
        PrevObject=Diff[0]
    else:
      Result=ComparisonTable

  #Results for raw output
  else:
    Result=ComparisonList

  #Calculate compared objects
  ComparedObjects=len(ObjectNames)

  #Clear progress
  DisplayProgress("CLR",0,0,"")

  #Return comparison result
  return ComparedObjects,Differences,DiffObjects,Result

#----------------------------------------------------------------------------------------------------------------------
# PrintTable
#----------------------------------------------------------------------------------------------------------------------
def PrintTable(Heading,ColAttributes,Rows,MaxWidth):

  #Calculate data column widths
  Lengths=[0]*len(Rows[0])
  for Row in Rows:
    i=0
    for Field in Row:
      Lengths[i]=max(max(Lengths[i],len(str(Field))),len(Heading[i]))
      i+=1

  #Calculate max column to print according to data length and maximun width
  i=0
  TableWidth=1
  MaxColumn=0
  Truncated=False
  for Len in Lengths:
    TableWidth+=Len+1
    MaxColumn=i
    if(TableWidth>MaxWidth):
      Truncated=True
      MaxColumn-=1
      TableWidth-=(Len+1)
      break
    i+=1

  #Adjust lengths if table is truncated and has resizeable columns
  if Truncated==True and "".join(ColAttributes).find("W")!=-1:
    while(True):
      i=0
      Resized=False
      for ColHeader in Heading:
        if Lengths[i]>len(ColHeader) and ColAttributes[i].find("W")!=-1:
          Lengths[i]-=1
          Resized=True
        i+=1
      if Resized==False:
        break
      i=0
      TableWidth=1
      MaxColumn=0
      Truncated=False
      for Len in Lengths:
        TableWidth+=Len+1
        MaxColumn=i
        if(TableWidth>MaxWidth):
          Truncated=True
          MaxColumn-=1
          TableWidth-=(Len+1)
          break
        i+=1
      if Truncated==False:
        break

  #Separator line
  Separator="-"*TableWidth

  #Print column headings
  print(Separator)
  print("|",end="",flush=True)
  i=0
  for Col in Heading:
    print(Col.center(Lengths[i])+"|",end="",flush=True)
    if(i>=MaxColumn):
      break
    i+=1
  print("")
  i=0
  print(Separator)

  #Print data
  for Row in Rows:
    if Row[0]==SEPARATOR_ID:
      i=0
      print("|",end="",flush=True)
      for Field in Row:
        print("·"*Lengths[i]+"|",end="",flush=True)
        if(i>=MaxColumn):
          break
        i+=1
      print("")
    else:
      i=0
      print("|",end="",flush=True)
      for Field in Row:
        if ColAttributes[i].find("L")!=-1:
          print(str(Field)[:Lengths[i]].ljust(Lengths[i])+"|",end="",flush=True)
        elif ColAttributes[i].find("R")!=-1:
          print(str(Field)[:Lengths[i]].rjust(Lengths[i])+"|",end="",flush=True)
        elif ColAttributes[i].find("C")!=-1:
          print(str(Field)[:Lengths[i]].center(Lengths[i])+"|",end="",flush=True)
        if(i>=MaxColumn):
          break
        i+=1
      print("")
  print(Separator)

  #Column count warning
  if(MaxColumn<len(Lengths)-1):
    WarnMessage="Displaying {0} columns out of {1} columns due to console width".format(str(MaxColumn+1),str(len(Lengths)))
  else:
    WarnMessage=""

  #Warning
  if(len(WarnMessage)!=0):
    print(WarnMessage)    

#----------------------------------------------------------------------------------------------------------------------
# Print raw output
#----------------------------------------------------------------------------------------------------------------------
def PrintRawOutput(Comparison):
  PrevObjectName=""
  for Item in Comparison:
    ObjectName=Item[0]
    DifferenceList=Item[1]
    if ObjectName!=PrevObjectName:
      print("\n--- Object: "+ObjectName+" ---")
    for Diff in DifferenceList:
      print(Diff)
    PrevObjectName=ObjectName
  if len(Comparison)!=0:
    print("")

#----------------------------------------------------------------------------------------------------------------------
# Main
#----------------------------------------------------------------------------------------------------------------------

#Get configuration file if it exists
_Config={}
if os.path.exists(DBSC_CONFIG_FILE)==True:
  Status,_Config,Message=JsonFileParser(DBSC_CONFIG_FILE)
  if Status==False:
    print(Message)
    exit()

#Get command line arguments
Options=[]
if(GetCommandLineOptions(Options)):
  SrcFolder=Options[0]
  SrcSchemas=Options[1]
  TgtFolder=Options[2]
  TgtSchemas=Options[3]
  PatternFilter=Options[4]
  SeparatorLine=Options[5]
  RawOutput=Options[6]
  ShowProgress=Options[7]
  DumpMode=Options[8]
else:
  exit()

#Set global show progress flag
_ShowProgress=ShowProgress

#Get console size
if(sys.stdout.isatty()):
  Console=os.get_terminal_size()
  ConsoleWidth=Console.columns-1
else:
  ConsoleWidth=9999
  _ShowProgress=False

#Replace schema groups by actual selected schemas
if len(SrcSchemas)!=0:
  if SrcSchemas in _Config["schema_groups"]:
    SrcSchemas=_Config["schema_groups"][SrcSchemas]
if len(TgtSchemas)!=0:
  if TgtSchemas in _Config["schema_groups"]:
    TgtSchemas=_Config["schema_groups"][TgtSchemas]

#Get databricks instance details from environment variables
if len(SrcSchemas)!=0 or len(TgtSchemas)!=0:
  ServerHostName=(os.environ["DATABRICKS_SERVER_HOSTNAME"] if "DATABRICKS_SERVER_HOSTNAME" in os.environ else "")
  if len(ServerHostName)==0:
    print("Unable to get databricks server hostname from environment variable DATABRICKS_SERVER_HOSTNAME")
    exit()
  HttpPath=(os.environ["DATABRICKS_HTTP_PATH"] if "DATABRICKS_HTTP_PATH" in os.environ else "")
  if len(HttpPath)==0:
    print("Unable to get databricks server hostname from environment variable DATABRICKS_HTTP_PATH")
    exit()
  AccessToken=(os.environ["DATABRICKS_TOKEN"] if "DATABRICKS_TOKEN" in os.environ else "")
  if len(AccessToken)==0:
    print("Unable to get databricks server hostname from environment variable DATABRICKS_TOKEN")
    exit()

#Get start time
Start=timer()

#Get definitions from project folders
SrcIsFolder=False
TgtIsFolder=False
if len(SrcFolder)!=0:
  SrcIsFolder=True
  State,Message,SrcSchemaDef=GetSchemaFromProject("SRC",SrcFolder,TgtSchemas,PatternFilter,DumpMode)
  if State==False:
    print(Message)
    print("Error occured when retrieving definitions from folder "+SrcFolder)
    exit()
if len(TgtFolder)!=0:
  TgtIsFolder=True
  State,Message,TgtSchemaDef=GetSchemaFromProject("TGT",TgtFolder,SrcSchemas,PatternFilter,DumpMode)
  if State==False:
    print(Message)
    print("Error occured when retrieving definitions from folder "+TgtFolder)
    exit()

#Get definitions from databricks metastore
if len(SrcSchemas)!=0 or len(TgtSchemas)!=0:
  State,Cursor=Connect(ServerHostName,HttpPath,AccessToken)
  if State==False:
    exit()
  if len(SrcSchemas)!=0:
    State,Message,SrcSchemaDef=GetSchemaFromMetastore("SRC",Cursor,SrcSchemas,PatternFilter)
    if State==False:
      print(Message)
      print("Error occured when retrieving definition of schema "+SrcSchemas)
      exit()
  if len(TgtSchemas)!=0:
    State,Message,TgtSchemaDef=GetSchemaFromMetastore("TGT",Cursor,TgtSchemas,PatternFilter)
    if State==False:
      print(Message)
      print("Error occured when retrieving definition of schema "+TgtSchemas)
      exit()

#Dump mode (no comparison)
if DumpMode==True:
  print(json.dumps(SrcSchemaDef,indent=2))

#Schema comparison mode
else:

  #Compare schemas
  ComparedObjects,Differences,DiffObjects,Comparison=CompareSchemas(SrcSchemaDef,TgtSchemaDef,SrcIsFolder,TgtIsFolder,SeparatorLine,RawOutput)

  #Print schema comparison
  if len(Comparison)!=0:
    if RawOutput==True:
      PrintRawOutput(Comparison)
    else:
      PrintTable(["Object","Item",(SrcSchemas if len(SrcSchemas)!=0 else SrcFolder),(TgtSchemas if len(TgtSchemas)!=0 else TgtFolder)],["L","L","LW","LW"],Comparison,ConsoleWidth)
      print("Legend: "+", ".join([Id+"="+OBJECTID_CONF[Id]["description"] for Id in OBJECTID_CONF]))

  #Difference counter
  ElapsedTime=timer()-Start
  print(("[Ok]" if Differences==0 else "[Diff]")+f" Compared {ComparedObjects} object(s), found {DiffObjects} object(s) different and {Differences} difference(s) ["+f"{ElapsedTime:.2f}s"+"]")