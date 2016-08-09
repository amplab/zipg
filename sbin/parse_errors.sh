#!/bin/bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

LOGFILE=$sbin/../log/handler.log

NUM_SRC_CONTEXT_LINES=3

old_IFS=$IFS  # save the field separator           
IFS=$'\n'     # new field separator, the end of line           

for bt in `cat $LOGFILE | grep '\[bt\]'`; do
  IFS=$old_IFS     # restore default field separator 
  printf '\n'
  EXEC=`echo $bt | cut -d' ' -f3 | cut -d'(' -f1`  
  ADDR=`echo $bt | cut -d'[' -f3 | cut -d']' -f1`
  echo "BACKTRACE:  $EXEC $ADDR"
  A2L=`addr2line -a $ADDR -e $EXEC -pfC`
  #echo "A2L:        $A2L"

  FUNCTION=`echo $A2L | sed 's/\<at\>.*//' | cut -d' ' -f2-99`
  FILE_AND_LINE=`echo $A2L | sed 's/.* at //'`
  echo "FILE:       $FILE_AND_LINE"
  echo "FUNCTION:   $FUNCTION"

  # print offending source code
  SRCFILE=`echo $FILE_AND_LINE | cut -d':' -f1`
  LINENUM=`echo $FILE_AND_LINE | cut -d':' -f2`
  if ([ -f $SRCFILE ]); then
    cat -n $SRCFILE | grep -C $NUM_SRC_CONTEXT_LINES "^ *$LINENUM\>" | sed "s/ $LINENUM/*$LINENUM/"
  else
    echo "File not found: $SRCFILE"
  fi
  IFS=$'\n'     # new field separator, the end of line           
done

IFS=$old_IFS     # restore default field separator
